// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	// We dot-import fsm to use common names such as fsm.True/False.
	. "github.com/cockroachdb/cockroach/pkg/util/fsm"
)

var noRewindExpected = CmdPos(-1)

type testContext struct {
	manualClock *hlc.ManualClock
	clock       *hlc.Clock
	mockDB      *client.DB
	mon         mon.BytesMonitor
	tracer      opentracing.Tracer
	// ctx is mimicking the spirit of a client connection's context
	ctx      context.Context
	settings *cluster.Settings
}

func makeTestContext() testContext {
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	factory := client.TxnSenderFactoryFunc(func(client.TxnType) client.TxnSender {
		return client.TxnSenderFunc(
			func(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				return nil, nil
			},
		)
	})
	settings := cluster.MakeTestingClusterSettings()
	return testContext{
		manualClock: manual,
		clock:       clock,
		mockDB:      client.NewDB(factory, clock),
		mon: mon.MakeMonitor(
			"test root mon",
			mon.MemoryResource,
			nil,  /* curCount */
			nil,  /* maxHist */
			-1,   /* increment */
			1000, /* noteworthy */
			settings,
		),
		tracer:   tracing.NewTracer(),
		ctx:      context.TODO(),
		settings: settings,
	}
}

type retryIntentEnum int

const (
	retryIntentUnknown retryIntentEnum = iota
	retryIntentSet
	retryIntentNotSet
)

// createOpenState returns a txnState initialized with an open txn.
func (tc *testContext) createOpenState(
	typ txnType, retryIntentOpt retryIntentEnum,
) (State, *txnState2) {
	if retryIntentOpt == retryIntentUnknown {
		log.Fatalf(context.TODO(), "retryIntentOpt not specified")
	}

	sp := tc.tracer.StartSpan("createOpenState")
	ctx := opentracing.ContextWithSpan(tc.ctx, sp)
	ctx, cancel := context.WithCancel(ctx)

	txnStateMon := mon.MakeMonitor("test mon",
		mon.MemoryResource,
		nil,  /* curCount */
		nil,  /* maxHist */
		-1,   /* increment */
		1000, /* noteworthy */
		cluster.MakeTestingClusterSettings(),
	)
	txnStateMon.Start(tc.ctx, &tc.mon, mon.BoundAccount{})

	ts := txnState2{
		Ctx:           ctx,
		connCtx:       tc.ctx,
		sp:            sp,
		cancel:        cancel,
		sqlTimestamp:  timeutil.Now(),
		isolation:     enginepb.SERIALIZABLE,
		priority:      roachpb.NormalUserPriority,
		mon:           &txnStateMon,
		txnAbortCount: metric.NewCounter(MetaTxnAbort),
	}
	ts.mu.txn = client.NewTxn(tc.mockDB, roachpb.NodeID(1) /* gatewayNodeID */, client.RootTxn)

	state := stateOpen{
		ImplicitTxn: FromBool(typ == implicitTxn),
		RetryIntent: FromBool(retryIntentOpt == retryIntentSet),
	}
	return state, &ts
}

// createAbortedState returns a txnState initialized with an aborted txn.
func (tc *testContext) createAbortedState(retryIntentOpt retryIntentEnum) (State, *txnState2) {
	_, ts := tc.createOpenState(explicitTxn, retryIntentOpt)
	ts.mu.txn.CleanupOnError(ts.Ctx, errors.Errorf("dummy error"))
	s := stateAborted{RetryIntent: FromBool(retryIntentOpt == retryIntentSet)}
	return s, ts
}

func (tc *testContext) createRestartWaitState() (State, *txnState2) {
	_, ts := tc.createOpenState(
		explicitTxn,
		// retryIntent must had been set in order for the state to advance to
		// RestartWait.
		retryIntentSet)
	s := stateRestartWait{}
	return s, ts
}

func (tc *testContext) createCommitWaitState() (State, *txnState2, error) {
	_, ts := tc.createOpenState(explicitTxn, retryIntentSet)
	// Commit the KV txn, simulating what the execution layer is doing.
	if err := ts.mu.txn.Commit(ts.Ctx); err != nil {
		return nil, nil, err
	}
	s := stateCommitWait{}
	return s, ts, nil
}

func (tc *testContext) createNoTxnState() (State, *txnState2) {
	txnStateMon := mon.MakeMonitor("test mon",
		mon.MemoryResource,
		nil,  /* curCount */
		nil,  /* maxHist */
		-1,   /* increment */
		1000, /* noteworthy */
		cluster.MakeTestingClusterSettings(),
	)
	ts := txnState2{mon: &txnStateMon, connCtx: tc.ctx}
	return stateNoTxn{}, &ts
}

// checkAdv returns an error if adv does not match all the expected fields.
//
// Pass noRewindExpected for expRewPos if a rewind is not expected.
func checkAdv(adv advanceInfo, expCode advanceCode, expRewPos CmdPos, expEv txnEvent) error {
	if adv.code != expCode {
		return errors.Errorf("expected code: %s, but got: %s (%+v)", expCode, adv.code, adv)
	}
	if expRewPos == noRewindExpected {
		if adv.rewCap != (rewindCapability{}) {
			return errors.Errorf("expected not rewind, but got: %+v", adv)
		}
	} else {
		if adv.rewCap.rewindPos != expRewPos {
			return errors.Errorf("expected rewind to %d, but got: %+v", expRewPos, adv)
		}
	}
	if expEv != adv.txnEvent {
		return errors.Errorf("expected txnEvent: %s, got: %s", expEv, adv.txnEvent)
	}
	return nil
}

// expKVTxn is used with checkTxn to check that fields on a client.Txn
// correspond to expectations. Any field left nil will not be checked.
type expKVTxn struct {
	debugName    *string
	isolation    *enginepb.IsolationType
	userPriority *roachpb.UserPriority
	// For the timestamps we just check the physical part. The logical part is
	// incremented every time the clock is read and so it's unpredictable.
	tsNanos     *int64
	origTSNanos *int64
	maxTSNanos  *int64
	isFinalized *bool
}

func checkTxn(txn *client.Txn, exp expKVTxn) error {
	if txn == nil {
		return errors.Errorf("expected a KV txn but found an uninitialized txn")
	}
	if exp.debugName != nil && !strings.HasPrefix(txn.DebugName(), *exp.debugName+" (") {
		return errors.Errorf("expected DebugName: %s, but got: %s",
			*exp.debugName, txn.DebugName())
	}
	if exp.isolation != nil && *exp.isolation != txn.Isolation() {
		return errors.Errorf("expected isolation: %s, but got: %s",
			*exp.isolation, txn.Isolation())
	}
	if exp.userPriority != nil && *exp.userPriority != txn.UserPriority() {
		return errors.Errorf("expected UserPriority: %s, but got: %s",
			*exp.userPriority, txn.UserPriority())
	}
	if exp.tsNanos != nil && *exp.tsNanos != txn.Proto().Timestamp.WallTime {
		return errors.Errorf("expected Timestamp: %d, but got: %s",
			*exp.tsNanos, txn.Proto().Timestamp)
	}
	if origTimestamp := txn.OrigTimestamp(); exp.origTSNanos != nil &&
		*exp.origTSNanos != origTimestamp.WallTime {
		return errors.Errorf("expected OrigTimestamp: %d, but got: %s",
			*exp.origTSNanos, origTimestamp)
	}
	if exp.maxTSNanos != nil && *exp.maxTSNanos != txn.Proto().MaxTimestamp.WallTime {
		return errors.Errorf("expected MaxTimestamp: %d, but got: %s",
			*exp.maxTSNanos, txn.Proto().MaxTimestamp)
	}
	if exp.isFinalized != nil && *exp.isFinalized != txn.IsFinalized() {
		return errors.Errorf("expected finalized: %t but wasn't", *exp.isFinalized)
	}
	return nil
}

func TestTransitions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	dummyRewCap := rewindCapability{rewindPos: CmdPos(12)}
	testCon := makeTestContext()
	tranCtx := transitionCtx{
		db:             testCon.mockDB,
		nodeID:         roachpb.NodeID(5),
		clock:          testCon.clock,
		tracer:         tracing.NewTracer(),
		connMon:        &testCon.mon,
		sessionTracing: &SessionTracing{},
		settings:       testCon.settings,
	}

	type expAdvance struct {
		expCode advanceCode
		expEv   txnEvent
	}

	txnName := sqlTxnName
	now := testCon.clock.Now()
	iso := enginepb.SERIALIZABLE
	pri := roachpb.NormalUserPriority
	maxTS := testCon.clock.Now().Add(testCon.clock.MaxOffset().Nanoseconds(), 0 /* logical */)
	varTrue := true
	varFalse := false
	type test struct {
		name string

		// A function used to init the txnState to the desired state before the
		// transition. The returned State and txnState are to be used to initialize
		// a Machine.
		init func() (State, *txnState2, error)

		// The event to deliver to the state machine.
		ev Event
		// evPayload, if not nil, is the payload to be delivered with the event.
		evPayload EventPayload
		// evFun, if specified, replaces ev and allows a test to create an event
		// that depends on the transactionState.
		evFun func(ts *txnState2) (Event, EventPayload)

		// The expected state of the fsm after the transition.
		expState State

		// The expected advance instructions resulting from the transition.
		expAdv expAdvance

		// If nil, the kv txn is expected to be nil. Otherwise, the txn's fields are
		// compared.
		expTxn *expKVTxn
	}
	tests := []test{
		//
		// Tests starting from the NoTxn state.
		//
		{
			// Start an implicit txn from NoTxn.
			name: "NoTxn->Starting (implicit txn)",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createNoTxnState()
				return s, ts, nil
			},
			ev:        eventTxnStart{ImplicitTxn: True},
			evPayload: makeEventTxnStartPayload(iso, pri, tree.ReadWrite, timeutil.Now(), tranCtx),
			expState:  stateOpen{ImplicitTxn: True, RetryIntent: False},
			expAdv: expAdvance{
				// We expect to stayInPlace; upon starting a txn the statement is
				// executed again, this time in state Open.
				expCode: stayInPlace,
				expEv:   txnStart,
			},
			expTxn: &expKVTxn{
				debugName:    &txnName,
				isolation:    &iso,
				userPriority: &pri,
				tsNanos:      &now.WallTime,
				origTSNanos:  &now.WallTime,
				maxTSNanos:   &maxTS.WallTime,
				isFinalized:  &varFalse,
			},
		},
		{
			// Start an explicit txn from NoTxn.
			name: "NoTxn->Starting (explicit txn)",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createNoTxnState()
				return s, ts, nil
			},
			ev:        eventTxnStart{ImplicitTxn: False},
			evPayload: makeEventTxnStartPayload(iso, pri, tree.ReadWrite, timeutil.Now(), tranCtx),
			expState:  stateOpen{ImplicitTxn: False, RetryIntent: False},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnStart,
			},
			expTxn: &expKVTxn{
				debugName:    &txnName,
				isolation:    &iso,
				userPriority: &pri,
				tsNanos:      &now.WallTime,
				origTSNanos:  &now.WallTime,
				maxTSNanos:   &maxTS.WallTime,
				isFinalized:  &varFalse,
			},
		},
		//
		// Tests starting from the Open state.
		//
		{
			// Set the retry intent.
			name: "Open->Open + retry intent",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentNotSet)
				return s, ts, nil
			},
			ev:       eventRetryIntentSet{},
			expState: stateOpen{ImplicitTxn: False, RetryIntent: True},
			expAdv: expAdvance{
				expCode: advanceOne,
			},
			expTxn: &expKVTxn{},
		},
		{
			// Finish an implicit txn.
			name: "Open (implicit) -> NoTxn",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(implicitTxn, retryIntentNotSet)
				// We commit the KV transaction, as that's done by the layer below
				// txnState.
				if err := ts.mu.txn.Commit(ts.Ctx); err != nil {
					return nil, nil, err
				}
				return s, ts, nil
			},
			ev:        eventTxnFinish{},
			evPayload: eventTxnFinishPayload{commit: true},
			expState:  stateNoTxn{},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnCommit,
			},
			expTxn: nil,
		},
		{
			// Finish an explicit txn.
			name: "Open (explicit) -> NoTxn",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentNotSet)
				// We commit the KV transaction, as that's done by the layer below
				// txnState.
				if err := ts.mu.txn.Commit(ts.Ctx); err != nil {
					return nil, nil, err
				}
				return s, ts, nil
			},
			ev:        eventTxnFinish{},
			evPayload: eventTxnFinishPayload{commit: true},
			expState:  stateNoTxn{},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnCommit,
			},
			expTxn: nil,
		},
		{
			// Get a retriable error while we can auto-retry.
			name: "Open + auto-retry",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentNotSet)
				return s, ts, nil
			},
			evFun: func(ts *txnState2) (Event, EventPayload) {
				b := eventRetriableErrPayload{
					err: roachpb.NewHandledRetryableTxnError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Proto()),
					rewCap: dummyRewCap,
				}
				return eventRetriableErr{CanAutoRetry: True, IsCommit: False}, b
			},
			expState: stateOpen{ImplicitTxn: False, RetryIntent: False},
			expAdv: expAdvance{
				expCode: rewind,
				expEv:   txnRestart,
			},
			// Expect non-nil txn.
			expTxn: &expKVTxn{
				isFinalized: &varFalse,
			},
		},
		{
			// Like the above test - get a retriable error while we can auto-retry,
			// except this time the error is on a COMMIT. This shouldn't make any
			// difference; we should still auto-retry like the above.
			name: "Open + auto-retry (COMMIT)",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentNotSet)
				return s, ts, nil
			},
			evFun: func(ts *txnState2) (Event, EventPayload) {
				b := eventRetriableErrPayload{
					err: roachpb.NewHandledRetryableTxnError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Proto()),
					rewCap: dummyRewCap,
				}
				return eventRetriableErr{CanAutoRetry: True, IsCommit: True}, b
			},
			expState: stateOpen{ImplicitTxn: False, RetryIntent: False},
			expAdv: expAdvance{
				expCode: rewind,
				expEv:   txnRestart,
			},
			// Expect non-nil txn.
			expTxn: &expKVTxn{
				isFinalized: &varFalse,
			},
		},
		{
			// Get a retriable error when we can no longer auto-retry, but the client
			// is doing client-side retries.
			name: "Open + client retry",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentSet)
				return s, ts, nil
			},
			evFun: func(ts *txnState2) (Event, EventPayload) {
				b := eventRetriableErrPayload{
					err: roachpb.NewHandledRetryableTxnError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Proto()),
					rewCap: rewindCapability{},
				}
				return eventRetriableErr{CanAutoRetry: False, IsCommit: False}, b
			},
			expState: stateRestartWait{},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnRestart,
			},
			// Expect non-nil txn.
			expTxn: &expKVTxn{
				isFinalized: &varFalse,
			},
		},
		{
			// Like the above (a retriable error when we can no longer auto-retry, but
			// the client is doing client-side retries) except the retriable error
			// comes from a COMMIT statement. This means that the client didn't
			// properly respect the client-directed retries protocol (it should've
			// done a RELEASE such that COMMIT couldn't get retriable errors), and so
			// we can't go to RestartWait.
			name: "Open + client retry + error on COMMIT",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentSet)
				return s, ts, nil
			},
			evFun: func(ts *txnState2) (Event, EventPayload) {
				b := eventRetriableErrPayload{
					err: roachpb.NewHandledRetryableTxnError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Proto()),
					rewCap: rewindCapability{},
				}
				return eventRetriableErr{CanAutoRetry: False, IsCommit: True}, b
			},
			expState: stateNoTxn{},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnAborted,
			},
			// Expect nil txn.
			expTxn: nil,
		},
		{
			// An error on COMMIT leaves us in NoTxn, not in Aborted.
			name: "Open + non-retriable error on COMMIT",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentSet)
				return s, ts, nil
			},
			ev:        eventNonRetriableErr{IsCommit: True},
			evPayload: eventNonRetriableErrPayload{err: fmt.Errorf("test non-retriable err")},
			expState:  stateNoTxn{},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnAborted,
			},
			// Expect nil txn.
			expTxn: nil,
		},
		{
			// We get a retriable error, but we can't auto-retry and the client is not
			// doing client-directed retries.
			name: "Open + useless retriable error (explicit)",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentNotSet)
				return s, ts, nil
			},
			evFun: func(ts *txnState2) (Event, EventPayload) {
				b := eventRetriableErrPayload{
					err: roachpb.NewHandledRetryableTxnError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Proto()),
					rewCap: rewindCapability{},
				}
				return eventRetriableErr{CanAutoRetry: False, IsCommit: False}, b
			},
			expState: stateAborted{RetryIntent: False},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnAborted,
			},
			expTxn: &expKVTxn{
				isFinalized: &varTrue,
			},
		},
		{
			// Like the above, but this time with an implicit txn: we get a retriable
			// error, but we can't auto-retry. We expect to go to NoTxn.
			name: "Open + useless retriable error (implicit)",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(implicitTxn, retryIntentNotSet)
				return s, ts, nil
			},
			evFun: func(ts *txnState2) (Event, EventPayload) {
				b := eventRetriableErrPayload{
					err: roachpb.NewHandledRetryableTxnError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Proto()),
					rewCap: rewindCapability{},
				}
				return eventRetriableErr{CanAutoRetry: False, IsCommit: False}, b
			},
			expState: stateNoTxn{},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnAborted,
			},
			// Expect the txn to have been cleared.
			expTxn: nil,
		},
		{
			// We get a non-retriable error.
			name: "Open + non-retriable error",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentNotSet)
				return s, ts, nil
			},
			ev:        eventNonRetriableErr{IsCommit: False},
			evPayload: eventNonRetriableErrPayload{err: fmt.Errorf("test non-retriable err")},
			expState:  stateAborted{RetryIntent: False},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnAborted,
			},
			expTxn: &expKVTxn{
				isFinalized: &varTrue,
			},
		},
		{
			// We go to CommitWait (after a RELEASE SAVEPOINT).
			name: "Open->CommitWait",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentSet)
				// Simulate what execution does before generating this event.
				err := ts.mu.txn.Commit(ts.Ctx)
				return s, ts, err
			},
			ev:       eventTxnReleased{},
			expState: stateCommitWait{},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnCommit,
			},
			expTxn: &expKVTxn{
				isFinalized: &varTrue,
			},
		},
		{
			// Restarting from Open via ROLLBACK TO SAVEPOINT.
			name: "Open + restart",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentSet)
				return s, ts, nil
			},
			ev:       eventTxnRestart{},
			expState: stateOpen{ImplicitTxn: False, RetryIntent: True},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnRestart,
			},
			// We would like to test that the transaction's epoch bumped if the txn
			// performed any operations, but it's not easy to do the test.
			expTxn: &expKVTxn{},
		},
		//
		// Tests starting from the Aborted state.
		//
		{
			// The txn finished, such as after a ROLLBACK.
			name: "Aborted->NoTxn",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createAbortedState(retryIntentNotSet)
				return s, ts, nil
			},
			ev:        eventTxnFinish{},
			evPayload: eventTxnFinishPayload{commit: false},
			expState:  stateNoTxn{},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnAborted,
			},
			expTxn: nil,
		},
		{
			// The txn is starting again (e.g. ROLLBACK TO SAVEPOINT while in Aborted).
			name: "Aborted->Starting",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createAbortedState(retryIntentSet)
				return s, ts, nil
			},
			ev:        eventTxnStart{ImplicitTxn: False},
			evPayload: makeEventTxnStartPayload(iso, pri, tree.ReadWrite, timeutil.Now(), tranCtx),
			expState:  stateOpen{ImplicitTxn: False, RetryIntent: True},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   noEvent,
			},
			expTxn: &expKVTxn{
				isFinalized: &varFalse,
			},
		},
		//
		// Tests starting from the RestartWait state.
		//
		{
			// The txn got finished, such as after a ROLLBACK.
			name: "RestartWait->NoTxn",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createRestartWaitState()
				err := ts.mu.txn.Rollback(ts.Ctx)
				return s, ts, err
			},
			ev:        eventTxnFinish{},
			evPayload: eventTxnFinishPayload{commit: false},
			expState:  stateNoTxn{},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnAborted,
			},
			expTxn: nil,
		},
		{
			// The txn got restarted, through a ROLLBACK TO SAVEPOINT.
			name: "RestartWait->Open",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createRestartWaitState()
				return s, ts, nil
			},
			ev:       eventTxnRestart{},
			expState: stateOpen{ImplicitTxn: False, RetryIntent: True},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnRestart,
			},
			expTxn: &expKVTxn{},
		},
		{
			name: "RestartWait->Aborted",
			init: func() (State, *txnState2, error) {
				s, ts := testCon.createRestartWaitState()
				return s, ts, nil
			},
			ev:        eventNonRetriableErr{IsCommit: False},
			evPayload: eventNonRetriableErrPayload{err: fmt.Errorf("test non-retriable err")},
			expState:  stateAborted{RetryIntent: True},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnAborted,
			},
			expTxn: &expKVTxn{},
		},
		//
		// Tests starting from the CommitWait state.
		//
		{
			name: "CommitWait->NoTxn",
			init: func() (State, *txnState2, error) {
				return testCon.createCommitWaitState()
			},
			ev:        eventTxnFinish{},
			evPayload: eventTxnFinishPayload{commit: true},
			expState:  stateNoTxn{},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnCommit,
			},
			expTxn: nil,
		},
		{
			name: "CommitWait + err",
			init: func() (State, *txnState2, error) {
				return testCon.createCommitWaitState()
			},
			ev:        eventNonRetriableErr{IsCommit: False},
			evPayload: eventNonRetriableErrPayload{err: fmt.Errorf("test non-retriable err")},
			expState:  stateCommitWait{},
			expAdv: expAdvance{
				expCode: skipBatch,
			},
			expTxn: &expKVTxn{
				isFinalized: &varTrue,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Get the initial state.
			s, ts, err := tc.init()
			if err != nil {
				t.Fatal(err)
			}
			machine := MakeMachine(TxnStateTransitions, s, ts)

			// Perform the test's transition.
			ev := tc.ev
			payload := tc.evPayload
			if tc.evFun != nil {
				ev, payload = tc.evFun(ts)
			}
			if err := machine.ApplyWithPayload(ctx, ev, payload); err != nil {
				t.Fatal(err)
			}

			// Check that we moved to the right high-level state.
			if state := machine.CurState(); state != tc.expState {
				t.Fatalf("expected state %#v, got: %#v", tc.expState, state)
			}

			// Check the resulting advanceInfo.
			adv := ts.consumeAdvanceInfo()
			expRewPos := noRewindExpected
			if tc.expAdv.expCode == rewind {
				expRewPos = dummyRewCap.rewindPos
			}
			if err := checkAdv(
				adv, tc.expAdv.expCode, expRewPos, tc.expAdv.expEv,
			); err != nil {
				t.Fatal(err)
			}

			// Check that the KV txn is in the expected state.
			if tc.expTxn == nil {
				if ts.mu.txn != nil {
					t.Fatalf("expected no txn, got: %+v", ts.mu.txn)
				}
			} else {
				if err := checkTxn(ts.mu.txn, *tc.expTxn); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
