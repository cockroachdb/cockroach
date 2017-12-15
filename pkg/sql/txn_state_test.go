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
	"fmt"
	"strings"
	"testing"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// mockRewinder is an implementation of rewindInterface that allows tests to
// assert that getRewindCapability() has not been called or what argument of the
// last setTxnStartPos() is.
// close() needs to be called for the assertions to fire.
type mockRewinder struct {
	// If failOnRewind is set, close() will report an error if
	// getRewindTxnCapability() has been called.
	failOnRewind bool
	err          error

	txnStartPos cursorPosition
	// nextToDeliver indicates the first statement for which results have _not_
	// been delivered to the client. So, a rewind to any position >= this will
	// be allowed.
	nextToDeliver cursorPosition

	// If != invalidPos, this will be compared to txnStartPos on Close().
	expTxnStartPos cursorPosition
}

var _ rewindInterface = &mockRewinder{}

type failOnRewindOpt bool

const (
	failOnRewind failOnRewindOpt = false
	// allowRewind means that rewinds will not cause test failures by themselves.
	// The mockRewinder still refuses to allow a rewind below the txnStartPos.
	allowRewind failOnRewindOpt = true
)

// If fow == failOnRewind, nextToDeliver doesn't matter; invalidPos can be
// passed.
func newMockRewinder(fow failOnRewindOpt, nextToDeliver cursorPosition) *mockRewinder {
	return &mockRewinder{
		failOnRewind:   fow == failOnRewind,
		expTxnStartPos: invalidPos,
		nextToDeliver:  nextToDeliver,
	}
}

func (m *mockRewinder) expectTxnStart(exp cursorPosition) {
	m.expTxnStartPos = exp
}

func (m *mockRewinder) close() error {
	if m.err != nil {
		return m.err
	}
	if m.expTxnStartPos != invalidPos && m.expTxnStartPos != m.txnStartPos {
		return errors.Errorf("expected TxnStartPos to have been set "+
			"to: %s but found: %s", m.expTxnStartPos, m.txnStartPos)
	}
	return nil
}

// getRewindCapability is part of rewindInterface.
func (m *mockRewinder) getRewindTxnCapability() (rewindCapability, bool) {
	if m.failOnRewind {
		m.err = errors.Errorf("unexpected getRewindTxnCapability() call")
		return rewindCapability{}, false
	}

	if m.nextToDeliver.compare(m.txnStartPos) <= 0 {
		// Can rewind. We're only going to partially initialize the
		// rewindCapability, for the purposes of tests using this.
		return rewindCapability{rewindPos: m.txnStartPos}, true
	}
	return rewindCapability{}, false
}

// setTxnStartPos is part of rewindInterface.
func (m *mockRewinder) setTxnStartPos(_ context.Context, pos cursorPosition) {
	m.txnStartPos = pos
}

const noFlush bool = false
const flushSet bool = true

var _ = noFlush

var invalidPos = cursorPosition{queryStrPos: -1, stmtIdx: -1}
var noRewind = cursorPosition{queryStrPos: -1, stmtIdx: -1}

type testContext struct {
	manualClock *hlc.ManualClock
	clock       *hlc.Clock
	mockDB      *client.DB
	mon         mon.BytesMonitor
	tracer      opentracing.Tracer
	// ctx is mimicking the spirit of a clientn connection's context
	ctx context.Context
}

func makeTestContext() testContext {
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	return testContext{
		manualClock: manual,
		clock:       clock,
		mockDB:      client.NewDB(nil /* sender */, clock),
		mon: mon.MakeMonitor(
			"test root mon",
			mon.MemoryResource,
			nil,  /* curCount */
			nil,  /* maxHist */
			-1,   /* increment */
			1000, /* noteworthy */
		),
		tracer: tracing.NewTracer(),
		ctx:    context.TODO(),
	}
}

// createOpenState returns a txnState initialized with an open txn.
//
// rew will be informed about the transaction starting at txnStartPos.
func (tc *testContext) createOpenState(
	retryIntentOpt retryIntentEnum, txnStartPos cursorPosition, rew rewindInterface,
) *txnState2 {
	if retryIntentOpt == retryIntentUnknown {
		log.Fatalf(context.TODO(), "retryIntentOpt not specified")
	}
	txn := client.NewTxn(tc.mockDB, roachpb.NodeID(1) /* gatewayNodeID */)

	sp := tc.tracer.StartSpan("createOpenState")
	ctx := opentracing.ContextWithSpan(tc.ctx, sp)
	ctx, cancel := context.WithCancel(ctx)

	txnStateMon := mon.MakeMonitor("test mon",
		mon.MemoryResource,
		nil,  /* curCount */
		nil,  /* maxHist */
		-1,   /* increment */
		1000, /* noteworthy */
	)
	txnStateMon.Start(tc.ctx, &tc.mon, mon.BoundAccount{})

	ts := txnState2{
		sts: stateMachineState{
			topLevelState: StateOpen,
			retryIntent:   retryIntentOpt == retryIntentSet,
		},
		txnType:      explicitTxn,
		Ctx:          ctx,
		sp:           sp,
		cancel:       cancel,
		sqlTimestamp: timeutil.Now(),
		isolation:    enginepb.SERIALIZABLE,
		priority:     roachpb.NormalUserPriority,
		mon:          &txnStateMon,
	}
	ts.mu.txn = txn
	rew.setTxnStartPos(ctx, txnStartPos)
	return &ts
}

// createAbortedState returns a txnState initialized with an aborted txn.
func (tc *testContext) createAbortedState(retryIntentOpt retryIntentEnum) *txnState2 {
	rew := newMockRewinder(failOnRewind, invalidPos)
	ts := tc.createOpenState(
		retryIntentOpt,       // the retryIntent doesn't matter
		cursorPosition{1, 1}, // txnStartPos doesn't matter
		rew)
	ts.mu.txn.CleanupOnError(ts.Ctx, errors.Errorf("dummy error"))
	ts.mu.txn = nil
	ts.sts.topLevelState = StateAborted
	return ts
}

func (tc *testContext) createRestartWaitState() *txnState2 {
	rew := newMockRewinder(failOnRewind, invalidPos)
	ts := tc.createOpenState(
		// retryIntent must had been set in order for the state to advance to
		// RestartWait.
		retryIntentSet,
		cursorPosition{1, 1}, // txnStartPos
		rew)
	ts.sts.topLevelState = StateRestartWait
	return ts
}

func (tc *testContext) createNoTxnState() *txnState2 {
	txnStateMon := mon.MakeMonitor("test mon",
		mon.MemoryResource,
		nil,  /* curCount */
		nil,  /* maxHist */
		-1,   /* increment */
		1000, /* noteworthy */
	)
	return &txnState2{
		sts: stateMachineState{
			topLevelState: StateNoTxn,
		},
		mon: &txnStateMon,
	}
}

// checkAdv returns an error if adv does not match all the expected fields.
//
// Pass noFlush/flushSet for expFlush. Pass noRewind for expRewPos if a rewind
// is not expected.
func checkAdv(adv advanceInfo, expCode advanceCode, expFlush bool, expRewPos cursorPosition) error {
	if adv.code != expCode {
		return errors.Errorf("expected code: %s, but got: %s (%+v)", expCode, adv.code, adv)
	}
	if adv.flush != expFlush {
		return errors.Errorf("expected flush: %t, but got: %+v", expFlush, adv)
	}
	if expRewPos == noRewind {
		if adv.rewCap != (rewindCapability{}) {
			return errors.Errorf("expected not rewind, but got: %+v", adv)
		}
	} else {
		if adv.rewCap.rewindPos != expRewPos {
			return errors.Errorf("expected rewind to %s, but got: %+v", expRewPos, adv)
		}
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
	if exp.debugName != nil && !strings.HasPrefix(txn.DebugName(), *exp.debugName) {
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
	if exp.origTSNanos != nil && *exp.origTSNanos != txn.OrigTimestamp().WallTime {
		return errors.Errorf("expected OrigTimestamp: %d, but got: %s",
			*exp.origTSNanos, txn.OrigTimestamp())
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
	firstStmtPos := cursorPosition{
		queryStrPos: 10,
		stmtIdx:     20,
	}
	testCon := makeTestContext()
	tranCtx := transitionCtx{
		db:                    testCon.mockDB,
		nodeID:                roachpb.NodeID(5),
		clock:                 testCon.clock,
		tracer:                tracing.NewTracer(),
		DefaultIsolationLevel: enginepb.SERIALIZABLE,
		connMon:               &testCon.mon,
	}

	type expAdvance struct {
		expCode advanceCode
		// Use noFlush/flushSet.
		expFlush bool
		// Use noRewind if a rewind is not expected.
		expRewPos cursorPosition
	}

	implicitTxnName := sqlImplicitTxnName
	explicitTxnName := sqlTxnName
	now := testCon.clock.Now()
	iso := enginepb.SERIALIZABLE
	pri := roachpb.NormalUserPriority
	maxTS := testCon.clock.Now().Add(testCon.clock.MaxOffset().Nanoseconds(), 0 /* logical */)
	varFalse := false
	varTrue := true
	type test struct {
		name string

		// A function used to init the txnState in the desired state before the
		// transition.
		init func(rew *mockRewinder) (*txnState2, error)

		// rew is the rewinder that the transition can query to see how far back it
		// can rewind. Tests can set some expectations on this as part of the test
		// case, and they can also add further expectations in the init() method.
		//
		// If not initialized, a mockRewinder that doesn't permit rewindes will be
		// used.
		rew *mockRewinder

		// The event to deliver to the state machine.
		ev inputEvent
		// evFun, if specified, replaces ev and allows a test to create an event
		// that depends on the transactionState.
		evFun func(ts *txnState2) inputEvent

		// The expected "top level state" after the transition.
		expTopLevelState TxnStateEnum2
		// If !nil, the txn's state retryIntent is compared to this expected value.
		expRetryIntent *bool

		// The expected advance instructions resulting from the transition.
		expAdv expAdvance

		// If nil, the kv txn is expected to be nil.
		expTxn *expKVTxn
	}
	tests := []test{
		//
		// Tests starting from the NoTxn state.
		//
		{
			// Start an implicit txn from NoTxn.
			name: "NoTxn->Starting (implicit txn)",
			init: func(rew *mockRewinder) (*txnState2, error) {
				ts := testCon.createNoTxnState()
				rew.expectTxnStart(firstStmtPos)
				return ts, nil
			},
			ev:               makeBeginTxnEvent(implicitTxn, firstStmtPos),
			expTopLevelState: StateOpen,
			expAdv: expAdvance{
				// We expect to stayInPlace; upon starting a txn the statement is
				// executed again, this time in state Open.
				expCode:   stayInPlace,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			expTxn: &expKVTxn{
				debugName:    &implicitTxnName,
				isolation:    &iso,
				userPriority: &pri,
				tsNanos:      &now.WallTime,
				origTSNanos:  &now.WallTime,
				maxTSNanos:   &maxTS.WallTime,
				isFinalized:  &varFalse,
			},
			expRetryIntent: &varFalse,
		},
		{
			// Start an explicit txn from NoTxn.
			name: "NoTxn->Starting (explicit txn)",
			init: func(rew *mockRewinder) (*txnState2, error) {
				ts := testCon.createNoTxnState()
				rew.expectTxnStart(firstStmtPos)
				return ts, nil
			},
			ev:               makeBeginTxnEvent(implicitTxn, firstStmtPos),
			expTopLevelState: StateOpen,
			expAdv: expAdvance{
				expCode:   stayInPlace,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			expTxn: &expKVTxn{
				debugName:    &explicitTxnName,
				isolation:    &iso,
				userPriority: &pri,
				tsNanos:      &now.WallTime,
				origTSNanos:  &now.WallTime,
				maxTSNanos:   &maxTS.WallTime,
				isFinalized:  &varFalse,
			},
			expRetryIntent: &varFalse,
		},
		//
		// Tests starting from the Open state.
		//
		{
			// Set the retry intent.
			name: "Open->Open + retry intent",
			init: func(rew *mockRewinder) (*txnState2, error) {
				ts := testCon.createOpenState(retryIntentNotSet, cursorPosition{1, 1}, rew)
				return ts, nil
			},
			ev:               makeNoTopLevelTransitionEvent(retryIntentSet, dontFlushResult),
			expTopLevelState: StateOpen,
			expAdv: expAdvance{
				expCode:   advanceOne,
				expFlush:  noFlush,
				expRewPos: noRewind,
			},
			expRetryIntent: &varTrue,
			expTxn:         &expKVTxn{},
		},
		{
			// Finish a txn.
			name: "Open->NoTxn",
			init: func(rew *mockRewinder) (*txnState2, error) {
				ts := testCon.createOpenState(retryIntentNotSet, cursorPosition{1, 1}, rew)
				// We commit the KV transaction, as that's done by the layer below
				// txnState.
				if err := ts.mu.txn.Commit(ts.Ctx); err != nil {
					return nil, err
				}
				return ts, nil
			},
			ev:               makeTxnFinishedEvent(),
			expTopLevelState: StateNoTxn,
			expAdv: expAdvance{
				expCode:   advanceOne,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			expTxn: nil,
		},
		{
			// Get a retriable error while we can auto-retry.
			name: "Open + auto-retry",
			init: func(rew *mockRewinder) (*txnState2, error) {
				return testCon.createOpenState(
					retryIntentNotSet,
					cursorPosition{1, 1}, // txnStartPos
					rew), nil
			},
			rew: newMockRewinder(
				allowRewind,
				// This position will allow a rewind to the beginning of the txn.
				cursorPosition{0, 0}),
			evFun: func(ts *txnState2) inputEvent {
				return makeErrEvent(
					roachpb.NewHandledRetryableTxnError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Proto(),
					),
					ts.mu.txn, &tree.Select{})
			},
			expTopLevelState: StateOpen,
			expAdv: expAdvance{
				expCode:  rewind,
				expFlush: noFlush,
				// We expect a rewind to the position used when creating the state above.
				expRewPos: cursorPosition{1, 1},
			},
			// Expect non-nil txn.
			expTxn: &expKVTxn{
				isFinalized: &varFalse,
			},
			expRetryIntent: &varFalse,
		},
		{
			// Get a retriable error when we can no longer auto-retry, but the client
			// is doing client-side retries.
			name: "Open + client retry",
			init: func(rew *mockRewinder) (*txnState2, error) {
				return testCon.createOpenState(
					retryIntentSet,       // client want to do client-side retries
					cursorPosition{1, 1}, // txnStartPos
					rew), nil
			},
			rew: newMockRewinder(
				allowRewind,
				// This position will not allow a rewind to the beginning of the txn.
				cursorPosition{1, 2}),
			evFun: func(ts *txnState2) inputEvent {
				return makeErrEvent(
					roachpb.NewHandledRetryableTxnError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Proto(),
					),
					ts.mu.txn, &tree.Select{})
			},
			expTopLevelState: StateRestartWait,
			expAdv: expAdvance{
				expCode:   skipQueryStr,
				expFlush:  flushSet,
				expRewPos: noRewind,
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
			init: func(rew *mockRewinder) (*txnState2, error) {
				return testCon.createOpenState(
					retryIntentSet,       // client want to do client-side retries
					cursorPosition{1, 1}, // txnStartPos
					rew), nil
			},
			rew: newMockRewinder(
				allowRewind,
				// This position will not allow a rewind to the beginning of the txn.
				cursorPosition{1, 2}),
			evFun: func(ts *txnState2) inputEvent {
				return makeErrEvent(
					roachpb.NewHandledRetryableTxnError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Proto(),
					),
					ts.mu.txn,
					// This is where this setup differs from the previous one.
					&tree.CommitTransaction{},
				)
			},
			expTopLevelState: StateNoTxn,
			expAdv: expAdvance{
				expCode:   skipQueryStr,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			// Expect nil txn.
			expTxn: nil,
		},
		{
			// An error on COMMIT leaves us in NoTxn, not in Aborted.
			name: "Open + non-retriable error on COMMIT",
			init: func(rew *mockRewinder) (*txnState2, error) {
				return testCon.createOpenState(
					retryIntentSet,       // client want to do client-side retries
					cursorPosition{1, 1}, // txnStartPos
					rew), nil
			},
			rew: newMockRewinder(
				allowRewind,
				// This position will not allow a rewind to the beginning of the txn.
				cursorPosition{1, 2}),
			evFun: func(ts *txnState2) inputEvent {
				return makeErrEvent(
					fmt.Errorf("test non-retriable err"), ts.mu.txn, &tree.CommitTransaction{})
			},
			expTopLevelState: StateNoTxn,
			expAdv: expAdvance{
				expCode:   skipQueryStr,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			// Expect nil txn.
			expTxn: nil,
		},
		{
			// We get a retriable error, but we can't auto-retry and the client is not
			// doing client-directed retries.
			name: "Open + useless retriable error",
			init: func(rew *mockRewinder) (*txnState2, error) {
				return testCon.createOpenState(
					retryIntentNotSet,    // the client isn't doing client-side retries
					cursorPosition{1, 1}, // txnStartPos
					rew), nil
			},
			rew: newMockRewinder(
				allowRewind,
				// This position will not allow a rewind to the beginning of the txn.
				cursorPosition{1, 2}),
			evFun: func(ts *txnState2) inputEvent {
				return makeErrEvent(
					roachpb.NewHandledRetryableTxnError(
						"test retriable err",
						ts.mu.txn.ID(),
						*ts.mu.txn.Proto(),
					),
					ts.mu.txn, &tree.Select{})
			},
			expTopLevelState: StateAborted,
			expAdv: expAdvance{
				expCode:   skipQueryStr,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			// Expect nil txn.
			expTxn: nil,
		},
		{
			// We get a non-retriable error.
			name: "Open + non-retriable error",
			init: func(rew *mockRewinder) (*txnState2, error) {
				return testCon.createOpenState(
					retryIntentNotSet,    // the client isn't doing client-side retries
					cursorPosition{1, 1}, // txnStartPos
					rew), nil
			},
			rew: newMockRewinder(
				allowRewind,
				// This position will not allow a rewind to the beginning of the txn.
				cursorPosition{1, 2}),
			evFun: func(ts *txnState2) inputEvent {
				return makeErrEvent(
					fmt.Errorf("test non-retriable err"), ts.mu.txn, &tree.Select{})
			},
			expTopLevelState: StateAborted,
			expAdv: expAdvance{
				expCode:   skipQueryStr,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			// Expect nil txn.
			expTxn: nil,
		},
		//
		// Tests starting from the Aborted state.
		//
		{
			// The txn got finished, such as after a ROLLBACK.
			name: "Aborted->NoTxn",
			init: func(rew *mockRewinder) (*txnState2, error) {
				return testCon.createAbortedState(retryIntentNotSet), nil
			},
			ev:               makeTxnFinishedEvent(),
			expTopLevelState: StateNoTxn,
			expAdv: expAdvance{
				expCode:   advanceOne,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			expTxn: nil,
		},
		{
			// The txn is starting again (e.g. ROLLBACK TO SAVEPOINT while in Aborted).
			name: "Aborted->Starting",
			init: func(rew *mockRewinder) (*txnState2, error) {
				rew.expectTxnStart(cursorPosition{1, 1})
				return testCon.createAbortedState(retryIntentSet), nil
			},
			ev: makeBeginTxnEvent(
				explicitTxn,
				cursorPosition{1, 1}, // firstStmtPos
			),
			expTopLevelState: StateOpen,
			expAdv: expAdvance{
				expCode:   advanceOne,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			expTxn: &expKVTxn{
				isFinalized: &varFalse,
			},
			// We expect the retry intent to be set as it was when we entered the
			// Aborted state.
			expRetryIntent: &varTrue,
		},
		//
		// Tests starting from the RestartWait state.
		//
		{
			// The txn got finished, such as after a ROLLBACK.
			name: "RestartWait->NoTxn",
			init: func(rew *mockRewinder) (*txnState2, error) {
				return testCon.createRestartWaitState(), nil
			},
			ev:               makeTxnFinishedEvent(),
			expTopLevelState: StateNoTxn,
			expAdv: expAdvance{
				expCode:   advanceOne,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			expTxn: nil,
		},
		{
			// The txn got restarted, through a ROLLBACK TO SAVEPOINT.
			name: "RestartWait->Open",
			init: func(rew *mockRewinder) (*txnState2, error) {
				rew.expTxnStartPos = cursorPosition{2, 5}
				return testCon.createRestartWaitState(), nil
			},
			ev:               makeTxnRestartEvent(cursorPosition{2, 5}),
			expTopLevelState: StateOpen,
			expAdv: expAdvance{
				expCode:   advanceOne,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			expTxn: &expKVTxn{},
		},
		{
			name: "RestartWait->Aborted",
			init: func(_ *mockRewinder) (*txnState2, error) {
				return testCon.createRestartWaitState(), nil
			},
			evFun: func(ts *txnState2) inputEvent {
				return makeErrEvent(
					fmt.Errorf("test non-retriable err"), ts.mu.txn, &tree.Select{})
			},
			expTopLevelState: StateAborted,
			expAdv: expAdvance{
				expCode:   skipQueryStr,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			expTxn: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.rew == nil {
				tc.rew = newMockRewinder(failOnRewind, invalidPos)
			}
			defer func() {
				if err := tc.rew.close(); err != nil {
					t.Fatal(err)
				}
			}()

			// Get the initial state.
			ts, err := tc.init(tc.rew)
			if err != nil {
				t.Fatal(err)
			}

			// Perform the test's transition.
			ev := tc.ev
			if tc.evFun != nil {
				ev = tc.evFun(ts)
			}
			adv, err := ts.performStateTransition(ctx, ev, tc.rew, firstStmtPos, tranCtx)
			if err != nil {
				t.Fatal(err)
			}

			// Check that we moved to the right high-level state.
			if ts.sts.topLevelState != tc.expTopLevelState {
				t.Fatalf("expected state %s, got: %s", tc.expTopLevelState, ts.sts)
			}

			// Check the resulting advanceInfo.
			if err := checkAdv(
				adv, tc.expAdv.expCode, tc.expAdv.expFlush, tc.expAdv.expRewPos,
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

			if tc.expRetryIntent != nil {
				if *tc.expRetryIntent != ts.sts.retryIntent {
					t.Fatalf(
						"expected retry intent: %t, but got the opposite", *tc.expRetryIntent)
				}
			}
		})
	}
}

// Test that ts.autoRetryCounter is maintained properly.
func TestAutoRetryCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	firstStmtPos := cursorPosition{
		queryStrPos: 10,
		stmtIdx:     20,
	}
	testCon := makeTestContext()
	tranCtx := transitionCtx{
		db:                    testCon.mockDB,
		nodeID:                roachpb.NodeID(5),
		clock:                 testCon.clock,
		tracer:                tracing.NewTracer(),
		DefaultIsolationLevel: enginepb.SERIALIZABLE,
		connMon:               &testCon.mon,
	}

	type expAdvance struct {
		expCode advanceCode
		// Use noFlush/flushSet.
		expFlush bool
		// Use noRewind if a rewind is not expected.
		expRewPos cursorPosition
	}

	// We'll allow rewinds.
	rew := newMockRewinder(allowRewind, firstStmtPos)
	defer func() {
		if err := rew.close(); err != nil {
			t.Fatal(err)
		}
	}()

	// We're going to start in state NoTxn and then perform a couple of automatic
	// retries and check that the counter is incremented.

	ts := testCon.createNoTxnState()
	if ts.autoRetryCounter != 0 {
		t.Fatalf("expected counter at 0, got: %d", ts.autoRetryCounter)
	}
	rew.expectTxnStart(firstStmtPos)
	beginEv := makeBeginTxnEvent(implicitTxn, firstStmtPos)
	expAdv := expAdvance{
		// We expect to stayInPlace; upon starting a txn the statement is
		// executed again, this time in state Open.
		expCode:   stayInPlace,
		expRewPos: noRewind,
		expFlush:  flushSet,
	}
	adv, err := ts.performStateTransition(ctx, beginEv, rew, firstStmtPos, tranCtx)
	if err != nil {
		t.Fatal(err)
	}
	// Check the resulting advanceInfo.
	if err := checkAdv(
		adv, expAdv.expCode, expAdv.expFlush, expAdv.expRewPos,
	); err != nil {
		t.Fatal(err)
	}
	if ts.autoRetryCounter != 0 {
		t.Fatalf("expected counter at 0, got: %d", ts.autoRetryCounter)
	}

	// Simulate an auto-retry and check that the counter is incremented.
	retryEv := makeErrEvent(
		roachpb.NewHandledRetryableTxnError(
			"test retriable err",
			ts.mu.txn.ID(),
			*ts.mu.txn.Proto(),
		),
		ts.mu.txn, &tree.Select{})
	expAdv = expAdvance{
		expCode:   rewind,
		expRewPos: firstStmtPos,
		expFlush:  noFlush,
	}
	adv, err = ts.performStateTransition(ctx, retryEv, rew, firstStmtPos, tranCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkAdv(
		adv, expAdv.expCode, expAdv.expFlush, expAdv.expRewPos,
	); err != nil {
		t.Fatal(err)
	}
	if ts.autoRetryCounter != 1 {
		t.Fatalf("expected counter at 1, got: %d", ts.autoRetryCounter)
	}

	// Simulate another auto-retry and check that the counter is incremented
	// again.
	adv, err = ts.performStateTransition(ctx, retryEv, rew, firstStmtPos, tranCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkAdv(
		adv, expAdv.expCode, expAdv.expFlush, expAdv.expRewPos,
	); err != nil {
		t.Fatal(err)
	}
	if ts.autoRetryCounter != 2 {
		t.Fatalf("expected counter at 2, got: %d", ts.autoRetryCounter)
	}

	// Check that running a neutral statement doesn't affect the counter.
	noopEv := makeNoTopLevelTransitionEvent(retryIntentSet, dontFlushResult)
	expAdv = expAdvance{
		expCode:   advanceOne,
		expRewPos: noRewind,
		expFlush:  noFlush,
	}
	adv, err = ts.performStateTransition(ctx, noopEv, rew, firstStmtPos, tranCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkAdv(
		adv, expAdv.expCode, expAdv.expFlush, expAdv.expRewPos,
	); err != nil {
		t.Fatal(err)
	}
	if ts.autoRetryCounter != 2 {
		t.Fatalf("expected counter at 1, got: %d", ts.autoRetryCounter)
	}

	// Check that finishing the txn resets the counter.
	finishEv := makeTxnFinishedEvent()
	expAdv = expAdvance{
		expCode:   advanceOne,
		expRewPos: noRewind,
		expFlush:  flushSet,
	}
	// Commit the txn so we can perform the transition.
	if err := ts.mu.txn.Commit(ts.Ctx); err != nil {
		t.Fatal(err)
	}
	adv, err = ts.performStateTransition(ctx, finishEv, rew, firstStmtPos, tranCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkAdv(
		adv, expAdv.expCode, expAdv.expFlush, expAdv.expRewPos,
	); err != nil {
		t.Fatal(err)
	}
	if ts.autoRetryCounter != 0 {
		t.Fatalf("expected counter at 0, got: %d", ts.autoRetryCounter)
	}
}
