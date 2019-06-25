// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/gogo/protobuf/proto"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
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
	factory := client.MakeMockTxnSenderFactory(
		func(context.Context, *roachpb.Transaction, roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			return nil, nil
		})

	settings := cluster.MakeTestingClusterSettings()
	ambient := testutils.MakeAmbientCtx()
	return testContext{
		manualClock: manual,
		clock:       clock,
		mockDB:      client.NewDB(ambient, factory, clock),
		mon: mon.MakeMonitor(
			"test root mon",
			mon.MemoryResource,
			nil,  /* curCount */
			nil,  /* maxHist */
			-1,   /* increment */
			1000, /* noteworthy */
			settings,
		),
		tracer:   ambient.Tracer,
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
) (fsm.State, *txnState) {
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

	ts := txnState{
		Ctx:           ctx,
		connCtx:       tc.ctx,
		sp:            sp,
		cancel:        cancel,
		sqlTimestamp:  timeutil.Now(),
		priority:      roachpb.NormalUserPriority,
		mon:           &txnStateMon,
		txnAbortCount: metric.NewCounter(MetaTxnAbort),
	}
	ts.mu.txn = client.NewTxn(ctx, tc.mockDB, roachpb.NodeID(1) /* gatewayNodeID */, client.RootTxn)

	state := stateOpen{
		ImplicitTxn: fsm.FromBool(typ == implicitTxn),
		RetryIntent: fsm.FromBool(retryIntentOpt == retryIntentSet),
	}
	return state, &ts
}

// createAbortedState returns a txnState initialized with an aborted txn.
func (tc *testContext) createAbortedState(retryIntentOpt retryIntentEnum) (fsm.State, *txnState) {
	_, ts := tc.createOpenState(explicitTxn, retryIntentOpt)
	ts.mu.txn.CleanupOnError(ts.Ctx, errors.Errorf("dummy error"))
	s := stateAborted{RetryIntent: fsm.FromBool(retryIntentOpt == retryIntentSet)}
	return s, ts
}

func (tc *testContext) createRestartWaitState() (fsm.State, *txnState) {
	_, ts := tc.createOpenState(
		explicitTxn,
		// retryIntent must had been set in order for the state to advance to
		// RestartWait.
		retryIntentSet)
	s := stateRestartWait{}
	return s, ts
}

func (tc *testContext) createCommitWaitState() (fsm.State, *txnState, error) {
	_, ts := tc.createOpenState(explicitTxn, retryIntentSet)
	// Commit the KV txn, simulating what the execution layer is doing.
	if err := ts.mu.txn.Commit(ts.Ctx); err != nil {
		return nil, nil, err
	}
	s := stateCommitWait{}
	return s, ts, nil
}

func (tc *testContext) createNoTxnState() (fsm.State, *txnState) {
	txnStateMon := mon.MakeMonitor("test mon",
		mon.MemoryResource,
		nil,  /* curCount */
		nil,  /* maxHist */
		-1,   /* increment */
		1000, /* noteworthy */
		cluster.MakeTestingClusterSettings(),
	)
	ts := txnState{mon: &txnStateMon, connCtx: tc.ctx}
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
	userPriority *roachpb.UserPriority
	// For the timestamps we just check the physical part. The logical part is
	// incremented every time the clock is read and so it's unpredictable.
	tsNanos     *int64
	origTSNanos *int64
	maxTSNanos  *int64
}

func checkTxn(txn *client.Txn, exp expKVTxn) error {
	if txn == nil {
		return errors.Errorf("expected a KV txn but found an uninitialized txn")
	}
	if exp.debugName != nil && !strings.HasPrefix(txn.DebugName(), *exp.debugName+" (") {
		return errors.Errorf("expected DebugName: %s, but got: %s",
			*exp.debugName, txn.DebugName())
	}
	if exp.userPriority != nil && *exp.userPriority != txn.UserPriority() {
		return errors.Errorf("expected UserPriority: %s, but got: %s",
			*exp.userPriority, txn.UserPriority())
	}
	proto := txn.Serialize()
	if exp.tsNanos != nil && *exp.tsNanos != proto.Timestamp.WallTime {
		return errors.Errorf("expected Timestamp: %d, but got: %s",
			*exp.tsNanos, proto.Timestamp)
	}
	if origTimestamp := txn.OrigTimestamp(); exp.origTSNanos != nil &&
		*exp.origTSNanos != origTimestamp.WallTime {
		return errors.Errorf("expected OrigTimestamp: %d, but got: %s",
			*exp.origTSNanos, origTimestamp)
	}
	if exp.maxTSNanos != nil && *exp.maxTSNanos != proto.MaxTimestamp.WallTime {
		return errors.Errorf("expected MaxTimestamp: %d, but got: %s",
			*exp.maxTSNanos, proto.MaxTimestamp)
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
	pri := roachpb.NormalUserPriority
	maxTS := testCon.clock.Now().Add(testCon.clock.MaxOffset().Nanoseconds(), 0 /* logical */)
	type test struct {
		name string

		// A function used to init the txnState to the desired state before the
		// transition. The returned State and txnState are to be used to initialize
		// a Machine.
		init func() (fsm.State, *txnState, error)

		// The event to deliver to the state machine.
		ev fsm.Event
		// evPayload, if not nil, is the payload to be delivered with the event.
		evPayload fsm.EventPayload
		// evFun, if specified, replaces ev and allows a test to create an event
		// that depends on the transactionState.
		evFun func(ts *txnState) (fsm.Event, fsm.EventPayload)

		// The expected state of the fsm after the transition.
		expState fsm.State

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
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createNoTxnState()
				return s, ts, nil
			},
			ev: eventTxnStart{ImplicitTxn: fsm.True},
			evPayload: makeEventTxnStartPayload(pri, tree.ReadWrite, timeutil.Now(),
				nil /* historicalTimestamp */, tranCtx),
			expState: stateOpen{ImplicitTxn: fsm.True, RetryIntent: fsm.False},
			expAdv: expAdvance{
				// We expect to stayInPlace; upon starting a txn the statement is
				// executed again, this time in state Open.
				expCode: stayInPlace,
				expEv:   txnStart,
			},
			expTxn: &expKVTxn{
				debugName:    &txnName,
				userPriority: &pri,
				tsNanos:      &now.WallTime,
				origTSNanos:  &now.WallTime,
				maxTSNanos:   &maxTS.WallTime,
			},
		},
		{
			// Start an explicit txn from NoTxn.
			name: "NoTxn->Starting (explicit txn)",
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createNoTxnState()
				return s, ts, nil
			},
			ev: eventTxnStart{ImplicitTxn: fsm.False},
			evPayload: makeEventTxnStartPayload(pri, tree.ReadWrite, timeutil.Now(),
				nil /* historicalTimestamp */, tranCtx),
			expState: stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.False},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnStart,
			},
			expTxn: &expKVTxn{
				debugName:    &txnName,
				userPriority: &pri,
				tsNanos:      &now.WallTime,
				origTSNanos:  &now.WallTime,
				maxTSNanos:   &maxTS.WallTime,
			},
		},
		//
		// Tests starting from the Open state.
		//
		{
			// Set the retry intent.
			name: "Open->Open + retry intent",
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentNotSet)
				return s, ts, nil
			},
			ev:       eventRetryIntentSet{},
			expState: stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.True},
			expAdv: expAdvance{
				expCode: advanceOne,
			},
			expTxn: &expKVTxn{},
		},
		{
			// Finish an implicit txn.
			name: "Open (implicit) -> NoTxn",
			init: func() (fsm.State, *txnState, error) {
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
			init: func() (fsm.State, *txnState, error) {
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
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentNotSet)
				return s, ts, nil
			},
			evFun: func(ts *txnState) (fsm.Event, fsm.EventPayload) {
				b := eventRetriableErrPayload{
					err: roachpb.NewTransactionRetryWithProtoRefreshError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Serialize()),
					rewCap: dummyRewCap,
				}
				return eventRetriableErr{CanAutoRetry: fsm.True, IsCommit: fsm.False}, b
			},
			expState: stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.False},
			expAdv: expAdvance{
				expCode: rewind,
				expEv:   txnRestart,
			},
			// Expect non-nil txn.
			expTxn: &expKVTxn{},
		},
		{
			// Like the above test - get a retriable error while we can auto-retry,
			// except this time the error is on a COMMIT. This shouldn't make any
			// difference; we should still auto-retry like the above.
			name: "Open + auto-retry (COMMIT)",
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentNotSet)
				return s, ts, nil
			},
			evFun: func(ts *txnState) (fsm.Event, fsm.EventPayload) {
				b := eventRetriableErrPayload{
					err: roachpb.NewTransactionRetryWithProtoRefreshError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Serialize()),
					rewCap: dummyRewCap,
				}
				return eventRetriableErr{CanAutoRetry: fsm.True, IsCommit: fsm.True}, b
			},
			expState: stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.False},
			expAdv: expAdvance{
				expCode: rewind,
				expEv:   txnRestart,
			},
			// Expect non-nil txn.
			expTxn: &expKVTxn{},
		},
		{
			// Get a retriable error when we can no longer auto-retry, but the client
			// is doing client-side retries.
			name: "Open + client retry",
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentSet)
				return s, ts, nil
			},
			evFun: func(ts *txnState) (fsm.Event, fsm.EventPayload) {
				b := eventRetriableErrPayload{
					err: roachpb.NewTransactionRetryWithProtoRefreshError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Serialize()),
					rewCap: rewindCapability{},
				}
				return eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.False}, b
			},
			expState: stateRestartWait{},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnRestart,
			},
			// Expect non-nil txn.
			expTxn: &expKVTxn{},
		},
		{
			// Like the above (a retriable error when we can no longer auto-retry, but
			// the client is doing client-side retries) except the retriable error
			// comes from a COMMIT statement. This means that the client didn't
			// properly respect the client-directed retries protocol (it should've
			// done a RELEASE such that COMMIT couldn't get retriable errors), and so
			// we can't go to RestartWait.
			name: "Open + client retry + error on COMMIT",
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentSet)
				return s, ts, nil
			},
			evFun: func(ts *txnState) (fsm.Event, fsm.EventPayload) {
				b := eventRetriableErrPayload{
					err: roachpb.NewTransactionRetryWithProtoRefreshError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Serialize()),
					rewCap: rewindCapability{},
				}
				return eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.True}, b
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
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentSet)
				return s, ts, nil
			},
			ev:        eventNonRetriableErr{IsCommit: fsm.True},
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
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentNotSet)
				return s, ts, nil
			},
			evFun: func(ts *txnState) (fsm.Event, fsm.EventPayload) {
				b := eventRetriableErrPayload{
					err: roachpb.NewTransactionRetryWithProtoRefreshError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Serialize()),
					rewCap: rewindCapability{},
				}
				return eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.False}, b
			},
			expState: stateAborted{RetryIntent: fsm.False},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnAborted,
			},
			expTxn: &expKVTxn{},
		},
		{
			// Like the above, but this time with an implicit txn: we get a retriable
			// error, but we can't auto-retry. We expect to go to NoTxn.
			name: "Open + useless retriable error (implicit)",
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createOpenState(implicitTxn, retryIntentNotSet)
				return s, ts, nil
			},
			evFun: func(ts *txnState) (fsm.Event, fsm.EventPayload) {
				b := eventRetriableErrPayload{
					err: roachpb.NewTransactionRetryWithProtoRefreshError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Serialize()),
					rewCap: rewindCapability{},
				}
				return eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.False}, b
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
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentNotSet)
				return s, ts, nil
			},
			ev:        eventNonRetriableErr{IsCommit: fsm.False},
			evPayload: eventNonRetriableErrPayload{err: fmt.Errorf("test non-retriable err")},
			expState:  stateAborted{RetryIntent: fsm.False},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnAborted,
			},
			expTxn: &expKVTxn{},
		},
		{
			// We go to CommitWait (after a RELEASE SAVEPOINT).
			name: "Open->CommitWait",
			init: func() (fsm.State, *txnState, error) {
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
			expTxn: &expKVTxn{},
		},
		{
			// Restarting from Open via ROLLBACK TO SAVEPOINT.
			name: "Open + restart",
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createOpenState(explicitTxn, retryIntentSet)
				return s, ts, nil
			},
			ev:       eventTxnRestart{},
			expState: stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.True},
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
			init: func() (fsm.State, *txnState, error) {
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
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createAbortedState(retryIntentSet)
				return s, ts, nil
			},
			ev: eventTxnStart{ImplicitTxn: fsm.False},
			evPayload: makeEventTxnStartPayload(pri, tree.ReadWrite, timeutil.Now(),
				nil /* historicalTimestamp */, tranCtx),
			expState: stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.True},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   noEvent,
			},
			expTxn: &expKVTxn{},
		},
		{
			// The txn is starting again (e.g. ROLLBACK TO SAVEPOINT while in Aborted).
			// Verify that the historical timestamp from the evPayload is propagated
			// to the expTxn.
			name: "Aborted->Starting (historical)",
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createAbortedState(retryIntentSet)
				return s, ts, nil
			},
			ev: eventTxnStart{ImplicitTxn: fsm.False},
			evPayload: makeEventTxnStartPayload(pri, tree.ReadOnly, now.GoTime(),
				&now, tranCtx),
			expState: stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.True},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   noEvent,
			},
			expTxn: &expKVTxn{
				tsNanos: proto.Int64(now.WallTime),
			},
		},
		//
		// Tests starting from the RestartWait state.
		//
		{
			// The txn got finished, such as after a ROLLBACK.
			name: "RestartWait->NoTxn",
			init: func() (fsm.State, *txnState, error) {
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
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createRestartWaitState()
				return s, ts, nil
			},
			ev:       eventTxnRestart{},
			expState: stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.True},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnRestart,
			},
			expTxn: &expKVTxn{},
		},
		{
			name: "RestartWait->Aborted",
			init: func() (fsm.State, *txnState, error) {
				s, ts := testCon.createRestartWaitState()
				return s, ts, nil
			},
			ev:        eventNonRetriableErr{IsCommit: fsm.False},
			evPayload: eventNonRetriableErrPayload{err: fmt.Errorf("test non-retriable err")},
			expState:  stateAborted{RetryIntent: fsm.True},
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
			init: func() (fsm.State, *txnState, error) {
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
			init: func() (fsm.State, *txnState, error) {
				return testCon.createCommitWaitState()
			},
			ev:        eventNonRetriableErr{IsCommit: fsm.False},
			evPayload: eventNonRetriableErrPayload{err: fmt.Errorf("test non-retriable err")},
			expState:  stateCommitWait{},
			expAdv: expAdvance{
				expCode: skipBatch,
			},
			expTxn: &expKVTxn{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Get the initial state.
			s, ts, err := tc.init()
			if err != nil {
				t.Fatal(err)
			}
			machine := fsm.MakeMachine(TxnStateTransitions, s, ts)

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
