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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

var noRewindExpected = CmdPos(-1)
var emptyTxnID = uuid.UUID{}

type testContext struct {
	manualClock *hlc.ManualClock
	clock       *hlc.Clock
	mockDB      *kv.DB
	mon         *mon.BytesMonitor
	tracer      *tracing.Tracer
	// ctx is mimicking the spirit of a client connection's context
	ctx      context.Context
	settings *cluster.Settings
}

func makeTestContext(stopper *stop.Stopper) testContext {
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	factory := kv.MakeMockTxnSenderFactory(
		func(context.Context, *roachpb.Transaction, roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			return nil, nil
		})

	settings := cluster.MakeTestingClusterSettings()
	ambient := log.MakeTestingAmbientCtxWithNewTracer()
	return testContext{
		manualClock: manual,
		clock:       clock,
		mockDB:      kv.NewDB(ambient, factory, clock, stopper),
		mon: mon.NewMonitor(
			"test root mon",
			mon.MemoryResource,
			nil,  /* curCount */
			nil,  /* maxHist */
			-1,   /* increment */
			1000, /* noteworthy */
			settings,
		),
		tracer:   ambient.Tracer,
		ctx:      context.Background(),
		settings: settings,
	}
}

// createOpenState returns a txnState initialized with an open txn.
func (tc *testContext) createOpenState(typ txnType) (fsm.State, *txnState) {
	sp := tc.tracer.StartSpan("createOpenState")
	ctx := tracing.ContextWithSpan(tc.ctx, sp)
	ctx, cancel := context.WithCancel(ctx)

	txnStateMon := mon.NewMonitor("test mon",
		mon.MemoryResource,
		nil,  /* curCount */
		nil,  /* maxHist */
		-1,   /* increment */
		1000, /* noteworthy */
		cluster.MakeTestingClusterSettings(),
	)
	txnStateMon.Start(tc.ctx, tc.mon, mon.BoundAccount{})

	ts := txnState{
		Ctx:           ctx,
		connCtx:       tc.ctx,
		cancel:        cancel,
		sqlTimestamp:  timeutil.Now(),
		priority:      roachpb.NormalUserPriority,
		mon:           txnStateMon,
		txnAbortCount: metric.NewCounter(MetaTxnAbort),
	}
	ts.mu.txn = kv.NewTxn(ctx, tc.mockDB, roachpb.NodeID(1) /* gatewayNodeID */)

	state := stateOpen{
		ImplicitTxn: fsm.FromBool(typ == implicitTxn),
	}
	return state, &ts
}

// createAbortedState returns a txnState initialized with an aborted txn.
func (tc *testContext) createAbortedState() (fsm.State, *txnState) {
	_, ts := tc.createOpenState(explicitTxn)
	return stateAborted{}, ts
}

func (tc *testContext) createCommitWaitState() (fsm.State, *txnState, error) {
	_, ts := tc.createOpenState(explicitTxn)
	// Commit the KV txn, simulating what the execution layer is doing.
	if err := ts.mu.txn.Commit(ts.Ctx); err != nil {
		return nil, nil, err
	}
	s := stateCommitWait{}
	return s, ts, nil
}

func (tc *testContext) createNoTxnState() (fsm.State, *txnState) {
	txnStateMon := mon.NewMonitor("test mon",
		mon.MemoryResource,
		nil,  /* curCount */
		nil,  /* maxHist */
		-1,   /* increment */
		1000, /* noteworthy */
		cluster.MakeTestingClusterSettings(),
	)
	ts := txnState{mon: txnStateMon, connCtx: tc.ctx}
	return stateNoTxn{}, &ts
}

// checkAdv returns an error if adv does not match all the expected fields.
//
// Pass noRewindExpected for expRewPos if a rewind is not expected.
func checkAdv(
	adv advanceInfo, expCode advanceCode, expRewPos CmdPos, expEv txnEventType, expTxnID uuid.UUID,
) error {
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
	if expEv != adv.txnEvent.eventType {
		return errors.Errorf("expected txnEventType: %s, got: %s", expEv, adv.txnEvent.eventType)
	}

	// The txnID field in advanceInfo is only checked when either:
	// * txnStart: the txnID in this case should be not be empty.
	// * txnCommit, txnRollback: the txnID in this case should equal to the
	//                           transaction that just finished execution.
	switch expEv {
	case txnStart:
		if adv.txnEvent.txnID.Equal(emptyTxnID) {
			return errors.Errorf(
				"expected txnID not to be empty, but it is")
		}
	case txnCommit, txnRollback:
		if adv.txnEvent.txnID != expTxnID {
			return errors.Errorf(
				"expected txnID: %s, got: %s", expTxnID, adv.txnEvent.txnID)
		}
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
	writeTSNanos          *int64
	readTSNanos           *int64
	uncertaintyLimitNanos *int64
}

func checkTxn(txn *kv.Txn, exp expKVTxn) error {
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
	proto := txn.TestingCloneTxn()
	if exp.writeTSNanos != nil && *exp.writeTSNanos != proto.WriteTimestamp.WallTime {
		return errors.Errorf("expected Timestamp: %d, but got: %s",
			*exp.writeTSNanos, proto.WriteTimestamp)
	}
	if readTimestamp := txn.ReadTimestamp(); exp.readTSNanos != nil &&
		*exp.readTSNanos != readTimestamp.WallTime {
		return errors.Errorf("expected ReadTimestamp: %d, but got: %s",
			*exp.readTSNanos, readTimestamp)
	}
	if exp.uncertaintyLimitNanos != nil && *exp.uncertaintyLimitNanos != proto.GlobalUncertaintyLimit.WallTime {
		return errors.Errorf("expected GlobalUncertaintyLimit: %d, but got: %s",
			*exp.uncertaintyLimitNanos, proto.GlobalUncertaintyLimit)
	}
	return nil
}

func TestTransitions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	dummyRewCap := rewindCapability{rewindPos: CmdPos(12)}
	testCon := makeTestContext(stopper)
	tranCtx := transitionCtx{
		db:             testCon.mockDB,
		nodeIDOrZero:   roachpb.NodeID(5),
		clock:          testCon.clock,
		tracer:         tracing.NewTracer(),
		connMon:        testCon.mon,
		sessionTracing: &SessionTracing{},
		settings:       testCon.settings,
	}

	type expAdvance struct {
		expCode advanceCode
		expEv   txnEventType
	}

	txnName := sqlTxnName
	now := testCon.clock.Now()
	pri := roachpb.NormalUserPriority
	uncertaintyLimit := testCon.clock.Now().Add(testCon.clock.MaxOffset().Nanoseconds(), 0 /* logical */)
	type test struct {
		name string

		// A function used to init the txnState to the desired state before the
		// transition. The returned State and txnState are to be used to initialize
		// a Machine.
		// If the initialized txnState contains an active transaction, its
		// transaction ID is also returned. Else, emptyTxnID is returned.
		init func() (fsm.State, *txnState, uuid.UUID, error)

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
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createNoTxnState()
				return s, ts, emptyTxnID, nil
			},
			ev: eventTxnStart{ImplicitTxn: fsm.True},
			evPayload: makeEventTxnStartPayload(pri, tree.ReadWrite, timeutil.Now(),
				nil /* historicalTimestamp */, tranCtx, sessiondatapb.Normal),
			expState: stateOpen{ImplicitTxn: fsm.True},
			expAdv: expAdvance{
				// We expect to stayInPlace; upon starting a txn the statement is
				// executed again, this time in state Open.
				expCode: stayInPlace,
				expEv:   txnStart,
			},
			expTxn: &expKVTxn{
				debugName:             &txnName,
				userPriority:          &pri,
				writeTSNanos:          &now.WallTime,
				readTSNanos:           &now.WallTime,
				uncertaintyLimitNanos: &uncertaintyLimit.WallTime,
			},
		},
		{
			// Start an explicit txn from NoTxn.
			name: "NoTxn->Starting (explicit txn)",
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createNoTxnState()
				return s, ts, emptyTxnID, nil
			},
			ev: eventTxnStart{ImplicitTxn: fsm.False},
			evPayload: makeEventTxnStartPayload(pri, tree.ReadWrite, timeutil.Now(),
				nil /* historicalTimestamp */, tranCtx, sessiondatapb.Normal),
			expState: stateOpen{ImplicitTxn: fsm.False},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnStart,
			},
			expTxn: &expKVTxn{
				debugName:             &txnName,
				userPriority:          &pri,
				writeTSNanos:          &now.WallTime,
				readTSNanos:           &now.WallTime,
				uncertaintyLimitNanos: &uncertaintyLimit.WallTime,
			},
		},
		//
		// Tests starting from the Open state.
		//
		{
			// Finish an implicit txn.
			name: "Open (implicit) -> NoTxn",
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createOpenState(implicitTxn)
				// We commit the KV transaction, as that's done by the layer below
				// txnState.
				if err := ts.mu.txn.Commit(ts.Ctx); err != nil {
					return nil, nil, emptyTxnID, err
				}
				return s, ts, ts.mu.txn.ID(), nil
			},
			ev:        eventTxnFinishCommitted{},
			evPayload: nil,
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
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createOpenState(explicitTxn)
				// We commit the KV transaction, as that's done by the layer below
				// txnState.
				if err := ts.mu.txn.Commit(ts.Ctx); err != nil {
					return nil, nil, emptyTxnID, err
				}
				return s, ts, ts.mu.txn.ID(), nil
			},
			ev:        eventTxnFinishCommitted{},
			evPayload: nil,
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
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createOpenState(explicitTxn)
				return s, ts, ts.mu.txn.ID(), nil
			},
			evFun: func(ts *txnState) (fsm.Event, fsm.EventPayload) {
				b := eventRetriableErrPayload{
					err:    ts.mu.txn.PrepareRetryableError(ctx, "test retriable err"),
					rewCap: dummyRewCap,
				}
				return eventRetriableErr{CanAutoRetry: fsm.True, IsCommit: fsm.False}, b
			},
			expState: stateOpen{ImplicitTxn: fsm.False},
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
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createOpenState(explicitTxn)
				return s, ts, ts.mu.txn.ID(), nil
			},
			evFun: func(ts *txnState) (fsm.Event, fsm.EventPayload) {
				b := eventRetriableErrPayload{
					err:    ts.mu.txn.PrepareRetryableError(ctx, "test retriable err"),
					rewCap: dummyRewCap,
				}
				return eventRetriableErr{CanAutoRetry: fsm.True, IsCommit: fsm.True}, b
			},
			expState: stateOpen{ImplicitTxn: fsm.False},
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
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createOpenState(explicitTxn)
				return s, ts, ts.mu.txn.ID(), nil
			},
			evFun: func(ts *txnState) (fsm.Event, fsm.EventPayload) {
				b := eventRetriableErrPayload{
					err:    ts.mu.txn.PrepareRetryableError(ctx, "test retriable err"),
					rewCap: rewindCapability{},
				}
				return eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.False}, b
			},
			expState: stateAborted{},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   noEvent,
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
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createOpenState(explicitTxn)
				return s, ts, ts.mu.txn.ID(), nil
			},
			evFun: func(ts *txnState) (fsm.Event, fsm.EventPayload) {
				b := eventRetriableErrPayload{
					err:    ts.mu.txn.PrepareRetryableError(ctx, "test retriable err"),
					rewCap: rewindCapability{},
				}
				return eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.True}, b
			},
			expState: stateNoTxn{},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnRollback,
			},
			// Expect nil txn.
			expTxn: nil,
		},
		{
			// An error on COMMIT leaves us in NoTxn, not in Aborted.
			name: "Open + non-retriable error on COMMIT",
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createOpenState(explicitTxn)
				return s, ts, ts.mu.txn.ID(), nil
			},
			ev:        eventNonRetriableErr{IsCommit: fsm.True},
			evPayload: eventNonRetriableErrPayload{err: fmt.Errorf("test non-retriable err")},
			expState:  stateNoTxn{},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnRollback,
			},
			// Expect nil txn.
			expTxn: nil,
		},
		{
			// Like the above, but this time with an implicit txn: we get a retriable
			// error, but we can't auto-retry. We expect to go to NoTxn.
			name: "Open + useless retriable error (implicit)",
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createOpenState(implicitTxn)
				return s, ts, ts.mu.txn.ID(), nil
			},
			evFun: func(ts *txnState) (fsm.Event, fsm.EventPayload) {
				b := eventRetriableErrPayload{
					err:    ts.mu.txn.PrepareRetryableError(ctx, "test retriable err"),
					rewCap: rewindCapability{},
				}
				return eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.False}, b
			},
			expState: stateNoTxn{},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   txnRollback,
			},
			// Expect the txn to have been cleared.
			expTxn: nil,
		},
		{
			// We get a non-retriable error.
			name: "Open + non-retriable error",
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createOpenState(explicitTxn)
				return s, ts, ts.mu.txn.ID(), nil
			},
			ev:        eventNonRetriableErr{IsCommit: fsm.False},
			evPayload: eventNonRetriableErrPayload{err: fmt.Errorf("test non-retriable err")},
			expState:  stateAborted{},
			expAdv: expAdvance{
				expCode: skipBatch,
				expEv:   noEvent,
			},
			expTxn: &expKVTxn{},
		},
		{
			// We go to CommitWait (after a RELEASE SAVEPOINT).
			name: "Open->CommitWait",
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createOpenState(explicitTxn)
				// Simulate what execution does before generating this event.
				err := ts.mu.txn.Commit(ts.Ctx)
				return s, ts, ts.mu.txn.ID(), err
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
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createOpenState(explicitTxn)
				return s, ts, ts.mu.txn.ID(), nil
			},
			ev:       eventTxnRestart{},
			expState: stateOpen{ImplicitTxn: fsm.False},
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
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createAbortedState()
				return s, ts, ts.mu.txn.ID(), nil
			},
			ev:       eventTxnFinishAborted{},
			expState: stateNoTxn{},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnRollback,
			},
			expTxn: nil,
		},
		{
			// The txn is starting again (ROLLBACK TO SAVEPOINT <not cockroach_restart> while in Aborted).
			name: "Aborted->Open",
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createAbortedState()
				return s, ts, ts.mu.txn.ID(), nil
			},
			ev:       eventSavepointRollback{},
			expState: stateOpen{ImplicitTxn: fsm.False},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   noEvent,
			},
			expTxn: &expKVTxn{},
		},
		{
			// The txn is starting again (ROLLBACK TO SAVEPOINT cockroach_restart while in Aborted).
			name: "Aborted->Restart",
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createAbortedState()
				return s, ts, ts.mu.txn.ID(), nil
			},
			ev:       eventTxnRestart{},
			expState: stateOpen{ImplicitTxn: fsm.False},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnRestart,
			},
			expTxn: &expKVTxn{
				userPriority:          &pri,
				writeTSNanos:          &now.WallTime,
				readTSNanos:           &now.WallTime,
				uncertaintyLimitNanos: &uncertaintyLimit.WallTime,
			},
		},
		{
			// The txn is starting again (e.g. ROLLBACK TO SAVEPOINT while in Aborted).
			// Verify that the historical timestamp from the evPayload is propagated
			// to the expTxn.
			name: "Aborted->Starting (historical)",
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts := testCon.createAbortedState()
				return s, ts, ts.mu.txn.ID(), nil
			},
			ev:       eventTxnRestart{},
			expState: stateOpen{ImplicitTxn: fsm.False},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   txnRestart,
			},
			expTxn: &expKVTxn{
				writeTSNanos: proto.Int64(now.WallTime),
			},
		},
		//
		// Tests starting from the CommitWait state.
		//
		{
			name: "CommitWait->NoTxn",
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts, err := testCon.createCommitWaitState()
				if err != nil {
					return nil, nil, emptyTxnID, err
				}
				return s, ts, ts.mu.txn.ID(), nil
			},
			ev:       eventTxnFinishCommitted{},
			expState: stateNoTxn{},
			expAdv: expAdvance{
				expCode: advanceOne,
				expEv:   noEvent,
			},
			expTxn: nil,
		},
		{
			name: "CommitWait + err",
			init: func() (fsm.State, *txnState, uuid.UUID, error) {
				s, ts, err := testCon.createCommitWaitState()
				if err != nil {
					return nil, nil, emptyTxnID, err
				}
				return s, ts, ts.mu.txn.ID(), nil
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
			s, ts, expectedTxnID, err := tc.init()
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
				adv, tc.expAdv.expCode, expRewPos, tc.expAdv.expEv, expectedTxnID,
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
