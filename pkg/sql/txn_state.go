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

// This file contains txnState, the middle layer of the ConnExecutor. It
// implements a state machine: it encapsulates a connection's state and
// concentrates all the state mutations into one method -
// txnState.performStateTransition().
//
// txnState represents the middle layer of the ConnExecutor: it takes
// inputEvents bottom execution layer and it returns instructions to the top
// layer about what statement to execute next.
//
// The "state" that the txnState manages includes:
// - the high-level transaction state (i.e. NoTxn, Open, Aborted, RetryWait,
//   etc.)
// - the KV-level client.Txn
// - the tracing spans related to transactions
//
// For the state machine specification, see the comment about
// performStateTransition().

import (
	"fmt"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// txnType represents the type of a SQL transaction.
type txnType int

//go:generate stringer -type=txnType
const (
	txnTypeUnknown txnType = iota
	// implicitTxn means that the txn was created for a SQL statement executed
	// outside of a transaction.
	implicitTxn
	// explicitTxn means that the txn was explicitly started with a BEGIN
	// statement.
	explicitTxn
)

var _ = txnTypeUnknown

// TxnStateEnum2 represents the state of a SQL txn.
type TxnStateEnum2 int

//go:generate stringer -type=TxnStateEnum2
const (
	// No txn is in scope. Either there never was one, or it got committed/rolled
	// back. Note that this state will not be experienced outside of the Session
	// and Executor (i.e. it will not be observed by a running query) because the
	// Executor opens implicit transactions before executing non-transactional
	// queries.
	//
	// WIP(andrei): The "State" prefix from all these enum values will go away
	// once TxnStateEnum is replaced by TxnStateEnum2.
	StateNoTxn TxnStateEnum2 = iota

	// A txn is in scope.
	StateOpen

	// The txn has encountered a (non-retriable) error.
	// Statements will be rejected until a COMMIT/ROLLBACK is seen.
	StateAborted

	// The txn has encountered a retriable error.
	// Statements will be rejected until a RESTART_TRANSACTION is seen.
	StateRestartWait

	// The KV txn has been committed successfully through a RELEASE.
	// Statements are rejected until a COMMIT is seen.
	StateCommitWait
)

// WIP(andrei)
var _ = StateCommitWait

// stateMachineState contains the elements of txnState that, in conjunction with
// inputEvents, define the state transitions. In other words, a state transition
// is decided based exclusively on stateMachine + inputEvent.
type stateMachineState struct {
	// state represents the transactional state that the connection is currently
	// in.
	topLevelState TxnStateEnum2

	// If set, the user declared the intention to retry the txn in case of
	// retriable errors. The txn will enter a RestartWait state in case of such
	// errors.
	// This field is mostly used while in the Open state, but even when in other
	// states it sometimes maintains info about what the retry intention was while
	// we were in Open last.
	retryIntent bool
}

func (s stateMachineState) String() string {
	if s.topLevelState == StateOpen {
		return fmt.Sprintf("Open (retryIntent: %t)", s.retryIntent)
	}
	return s.topLevelState.String()
}

// txnState contains state associated with an ongoing SQL txn; it is associated
// with and stored inside a ConnExecutor. It contains the stateMachineState plus
// a long tail of peripheral fields that are mutated as side-effects of state
// transitions; notably the KV client.Txn is in this category.
//
// The ConnExecutor does not mutate the txnState directly; instead, the
// ConnExecutor produces inputEvents in response to statement execution and
// passes them to performStateTransition(), which mutates the state.
//
// Depending on the exact state, there may or may not be an open KV txn
// associated with the SQL txn; if there is, that's one of the more important
// things that the txnState manages.
type txnState2 struct {
	// sts contains the fields that control state transitions. See
	// stateMachineState.
	sts stateMachineState

	// When inside a txn, this specifies the type of txn.
	txnType txnType
	// When inside a txn, autoRetryCounter keeps track of the number of the
	// current attempt. This field is set to 0 whenever a transaction ends, and is
	// incremented by 1 whenever a rewind happens.
	autoRetryCounter int

	// Mutable fields accessed from goroutines not synchronized by this txn's session,
	// such as when a SHOW SESSIONS statement is executed on another session.
	// Note that reads of mu.txn from the session's main goroutine
	// do not require acquiring a read lock - since only that
	// goroutine will ever write to mu.txn.
	mu struct {
		syncutil.RWMutex

		txn *client.Txn
	}

	// Ctx is the context for everything running in this SQL txn.
	// This is only set while the state is != NoTxn.
	Ctx context.Context

	// sp is the span corresponding to the SQL txn. These are often root spans, as
	// SQL txns are frequently the level at which we do tracing.
	sp opentracing.Span

	// cancel is the cancellation function for the above context. Called upon
	// COMMIT/ROLLBACK of the transaction to release resources associated with the
	// context. nil when no txn is in progress.
	cancel context.CancelFunc

	// The timestamp to report for current_timestamp(), now() etc.
	// This must be constant for the lifetime of a SQL transaction.
	sqlTimestamp time.Time

	// The transaction's isolation level.
	isolation enginepb.IsolationType

	// The transaction's priority.
	priority roachpb.UserPriority

	// mon tracks txn-bound objects like the running state of
	// planNode in the midst of performing a computation.
	mon *mon.BytesMonitor

	// The schema change closures to run when this txn is done.
	schemaChangers schemaChangerCollection

	// spanListener, if set, is used to deliver events about txn spans.
	spanListener txnSpanListener
}

// txnSpanListener is an interface implemented by the session tracing mechanism
// for listening to transaction events. The event handlers get access to the
// span that is created/finished for a new/finished txn.
type txnSpanListener interface {
	onNewTxn(opentracing.Span)
	onTxnFinish(opentracing.Span) error
}

// eventType is the type of an inputEvent.
type eventType int

//go:generate stringer -type=eventType
const (
	unknownEvent eventType = iota

	// noTopLevelTransition means that the state stays as it is. The top level
	// state stays unchanged; other fields of txnState may still be changed in
	// response to this even.
	noTopLevelTransition

	// txnStarting means that a transaction has just been started.
	txnStarting

	// txnFinished means that a transaction has just finished.
	txnFinished

	// txnRestarted means that a transaction has just been restarted.
	txnRestarted

	// retriableErr means that a retriableErr has occurred. In this case, the KV
	// txn is expected to have been prepared for a restart (epoch incremented or
	// such) by someone; the SQL session state machine does not deal with that.
	retriableErr

	// nonRetriableErr means that a non-retriable error has occurred.
	nonRetriableErr
)

var _ = unknownEvent

// inputEvent represents an event that the txnState state machine needs to react
// to, through its performStateTransition method.
type inputEvent struct {
	typ eventType

	errDetails errFields

	// For events txnStarting/txnRestarted.
	txnStartingDetails txnStartingFields

	noTopLevelTransitionDetails noTopLevelTransitionFields
}

// errFields contains inputEvent fields specific to retriableErr/nonRetriableErr
// events.
type errFields struct {
	// err is the query execution error that's causing this transition.
	err error

	// stmt is the statement that encountered the error. Only used for its tag
	// (because errors encountered by COMMITs need special treatment).
	//
	// TODO(andrei): Figure out something more restricted to use as the tag, and
	// do the same for the ResultWriter interface.
	stmt tree.Statement
}

// txnStartingFields contains inputEvent fields specific to
// txnStarting/txnRestarted events.
type txnStartingFields struct {
	// txnType indicates the type of transaction that is starting.
	txnType txnType
	// firstStmtPos indicates the position of the first statement "inside" the
	// transaction that's just starting. That is, the first statement that has to
	// be executed again in case of an automatic retry. For an implicitTxn, this
	// is the position of the respective statement. For transactions started by a
	// BEGIN or by a ROLLBACK TO SAVEPOINT statement, this is the position just
	// after that statement.
	firstStmtPos cursorPosition
}

// noTopLevelTransitionFields contains inputEvent fields specific to
// noTopLevelTransition events.
type noTopLevelTransitionFields struct {
	// retryIntent, if != retryIntentUnknown, informs the state machine that the
	// client has opted into the client-directed retries mechanism.  Setting this
	// is only valid when the txn is in the Open state. A value of
	// retryIntentNotSet is never valid here - once a retry intent is set, in
	// cannot be taken back - the client cannot opt out of the client-directed
	// retries protocol after opting in.
	retryIntent retryIntentEnum

	// flushResults, if set, means that the results (up to and including the
	// result for the statement that generated this transition) can be delivered
	// to the client. This field is ignored unless the current state is Open. In
	// any other state, results are flushed automatically.
	// This is used after statements such as BEGIN or SET TRANSACTION in order to
	// signal that, even if their results are delivered to the client, the Session
	// can still auto-retry the transaction. When seeing this set, the transaction
	// start point is advanced (thus, the statement and all preceding ones are not
	// going to be executed again in case of a transaction retry).
	flushResults bool
}

func (tran inputEvent) String() string {
	return fmt.Sprintf("tran: %s", tran.typ)
}

// See noTopLevelTransitionFields.retryIntent.
type retryIntentEnum int

const (
	retryIntentUnknown retryIntentEnum = iota
	retryIntentSet
	retryIntentNotSet
)

// flushResultOpt is the argument passed to makeNoTopLevelTransitionEvent().
// See inputEvent.noTopLevelTransitionFields.flushResults.
type flushResultOpt int

const (
	// dontFlushResult means that no special flushing instructions are to be given
	// to the module delivering results to the client.
	dontFlushResult flushResultOpt = iota
	// flushSpecialTxnPrefix means that all the results produced so far can be
	// delivered to the client; we're never going to need to rewind past the
	// current point. This is set, for example, after a BEGIN statement - the
	// BEGIN itself is never executed twice, so its results can be flushed asap.
	flushSpecialTxnPrefix
)

// WIP(andrei)
var _ = dontFlushResult
var _ = flushSpecialTxnPrefix

func makeNoTopLevelTransitionEvent(retryIntent retryIntentEnum, flush flushResultOpt) inputEvent {
	return inputEvent{
		typ: noTopLevelTransition,
		noTopLevelTransitionDetails: noTopLevelTransitionFields{
			retryIntent:  retryIntent,
			flushResults: flush == flushSpecialTxnPrefix,
		},
	}
}

func makeBeginTxnEvent(typ txnType, firstStmtPos cursorPosition) inputEvent {
	return inputEvent{
		typ: txnStarting,
		txnStartingDetails: txnStartingFields{
			txnType:      typ,
			firstStmtPos: firstStmtPos,
		},
	}
}

func makeTxnFinishedEvent() inputEvent {
	return inputEvent{typ: txnFinished}
}

func makeTxnRestartEvent(firstStmtPos cursorPosition) inputEvent {
	return inputEvent{
		typ: txnRestarted,
		txnStartingDetails: txnStartingFields{
			// Only explicit txn can be restarted.
			txnType:      explicitTxn,
			firstStmtPos: firstStmtPos,
		},
	}
}

func makeErrEvent(err error, curTxn *client.Txn, stmt tree.Statement) inputEvent {
	retErr, retriable := err.(*roachpb.HandledRetryableTxnError)
	if retriable && curTxn.IsRetryableErrMeantForTxn(*retErr) {
		return inputEvent{
			typ: retriableErr,
			errDetails: errFields{
				err:  err,
				stmt: stmt,
			},
		}
	}
	return inputEvent{
		typ: nonRetriableErr,
		errDetails: errFields{
			err:  err,
			stmt: stmt,
		},
	}
}

// SetTopLevelState updates the transaction state field.
func (ts *txnState2) SetTopLevelState(ctx context.Context, val TxnStateEnum2) {
	if val == StateNoTxn {
		ts.assertReadyToMoveToNoTxn(ctx)
	}
	ts.sts.topLevelState = val
}

func (ts *txnState2) assertReadyToMoveToNoTxn(ctx context.Context) {
	if ts.mu.txn != nil {
		log.Fatalf(ctx,
			"attempting to move SQL state to NoTxn but KV txn still present. "+
				"Current state: %s. txn: %+v",
			ts.sts, ts.mu.txn)
	}
	if ts.Ctx != nil {
		log.Fatalf(ctx, "Moving to StateNoTxn but ts.Ctx has not been reset. "+
			"Current state: %s. Has finishSQLTxn() been called?", ts.sts)
	}
}

// resetForNewSQLTxn (re)initializes the txnState for a new transaction.
// It creates a new client.Txn and initializes it using the session defaults.
//
// ctx: The context in which the new transaction is started. ts.Ctx will be set
//   to a child context and should be used for everything that happens within
//   this SQL transaction.
// txnType: The type of the starting txn.
// sqlTimestamp: The timestamp to report for current_timestamp(), now() etc.
// isolation: The transaction's isolation level.
// priority: The transaction's priority.
// firstStmtPos: The position of the first statement in the transaction.
// 	 In case we are to perform an automatic retry, the statement indicated here
//   will be the first one to be executed again. If the transaction is starting
//   because of a BEGIN, firstStmtPos has to indicate the position of the _next_
//   statement (i.e. the BEGIN shouldn't be re-executed on automatic retry).
// rew: The rewindInterface. Used to set the txn's start position.
// tranCtx: A bag of extra execution context.
func (ts *txnState2) resetForNewSQLTxn(
	ctx context.Context,
	txnType txnType,
	sqlTimestamp time.Time,
	isolation enginepb.IsolationType,
	priority roachpb.UserPriority,
	firstStmtPos cursorPosition,
	rew rewindInterface,
	tranCtx transitionCtx,
) {
	// Reset state vars to defaults.
	ts.sqlTimestamp = sqlTimestamp
	ts.txnType = txnType

	// Create a context for this transaction. It will include a root span that
	// will contain everything executed as part of the upcoming SQL txn, including
	// (automatic or user-directed) retries. The span is closed by finishSQLTxn().
	// TODO(andrei): figure out how to close these spans on server shutdown? Ties
	// into a larger discussion about how to drain SQL and rollback open txns.
	var sp opentracing.Span
	var opName string
	if txnType == implicitTxn {
		opName = sqlImplicitTxnName
	} else {
		opName = sqlTxnName
	}

	// Create a span for the new txn. The span is always Recordable to support the
	// use of session tracing, which may start recording on it.
	if parentSp := opentracing.SpanFromContext(ctx); parentSp != nil {
		// Create a child span for this SQL txn.
		sp = parentSp.Tracer().StartSpan(
			opName, opentracing.ChildOf(parentSp.Context()), tracing.Recordable)
	} else {
		// Create a root span for this SQL txn.
		sp = tranCtx.tracer.StartSpan(opName, tracing.Recordable)
	}

	// Put the new span in the context.
	ctx = opentracing.ContextWithSpan(ctx, sp)

	if !tracing.IsRecordable(sp) {
		log.Fatalf(ctx, "non-recordable transaction span of type: %T", sp)
	}

	ts.sp = sp
	ts.Ctx, ts.cancel = contextutil.WithCancel(ctx)
	if ts.spanListener != nil {
		ts.spanListener.onNewTxn(ts.sp)
	}

	rew.setTxnStartPos(ts.Ctx, firstStmtPos)

	ts.mon.Start(ts.Ctx, tranCtx.connMon, mon.BoundAccount{} /* reserved */)

	ts.mu.Lock()
	ts.mu.txn = client.NewTxn(tranCtx.db, tranCtx.nodeID)
	ts.mu.txn.SetDebugName(opName)
	ts.mu.Unlock()

	if err := ts.setIsolationLevel(isolation); err != nil {
		panic(err)
	}
	if err := ts.setPriority(priority); err != nil {
		panic(err)
	}

	// Discard the old schemaChangers, if any.
	ts.schemaChangers = schemaChangerCollection{}
}

type resetKVTxnOpt bool

const (
	resetKVTxn        resetKVTxnOpt = false
	kvTxnAlreadyReset resetKVTxnOpt = true
)

// finishSQLTxn finalizes a transaction's results and closes the root span for
// the current SQL txn. This needs to be called before resetForNewSQLTxn() is
// called for starting another SQL txn.
// The caller may or may not have already cleared ts.mu.txn. If it has,
// kvTxnAlreadyReset has to be passed in; otherwise, the KV txn is expected to
// be finalized.
//
// ctx is the ConnExecutor's context. This will be used once ts.Ctx is
// finalized.
func (ts *txnState2) finishSQLTxn(connCtx context.Context, resetKV resetKVTxnOpt) {
	ts.mon.Stop(ts.Ctx)
	if ts.cancel != nil {
		ts.cancel()
		ts.cancel = nil
	}
	if ts.sp == nil {
		panic("No span in context? Was resetForNewSQLTxn() called previously?")
	}

	if (resetKV == resetKVTxn) && !ts.mu.txn.IsFinalized() {
		panic(fmt.Sprintf(
			"attempting to finishSQLTxn(), but KV txn is not finalized: %+v", ts.mu.txn))
	}

	ts.sp.Finish()
	sp := ts.sp
	ts.sp = nil
	ts.Ctx = nil
	if resetKV == resetKVTxn {
		ts.mu.txn = nil
	}
	if ts.spanListener != nil {
		if err := ts.spanListener.onTxnFinish(sp); err != nil {
			log.Errorf(connCtx, "error finishing trace: %s", err)
		}
	}
}

func (ts *txnState2) setIsolationLevel(isolation enginepb.IsolationType) error {
	if err := ts.mu.txn.SetIsolation(isolation); err != nil {
		return err
	}
	ts.isolation = isolation
	return nil
}

func (ts *txnState2) setPriority(userPriority roachpb.UserPriority) error {
	if err := ts.mu.txn.SetUserPriority(userPriority); err != nil {
		return err
	}
	ts.priority = userPriority
	return nil
}

// advanceCode is part of advanceInfo; it instructs the module managing the
// statements buffer on what action to take.
type advanceCode int

//go:generate stringer -type=advanceCode
const (
	advanceUnknown advanceCode = iota
	// stayInPlace means that the cursor should remain where it is. The same
	// statement will be executed next.
	stayInPlace
	// advanceOne means that the cursor should be advanced by one position. This
	// is the code commonly used after a successful statement execution.
	advanceOne
	// skipQueryStr means that the cursor should skip over any remaining
	// statements that are part of the current query string and be positioned on
	// the first statement in the next query string.
	skipQueryStr
	// rewind means that the cursor should be moved back to the position indicated
	// by rewCap.
	rewind
)

// WIP(andrei)
var _ = advanceUnknown

type advanceInfo struct {
	code advanceCode

	// flush, if set, means we'll flush all the accumulated results and advance
	// the "start of the transaction".
	flush bool

	// Fields for the rewind code:

	// rewCap is the capability to rewind to the beginning of the transaction.
	// rewCap.rewindAndUnlock() needs to be called to perform the promised rewind.
	//
	// This field should not be set directly; buildRewindInstructions() should be
	// used.
	rewCap rewindCapability
}

// flushOpt is the argument passed to buildAdvanceInfo indicating flushing
// instructions.
type flushOpt int

const (
	// flushIfNotOpen means that the results should be flushed if we're not in the
	// Open state. This is the only flushing option valid when the state is not
	// Open - the results are always flushed in other states.
	flushIfNotOpen flushOpt = iota
	// forceFlush means that the results should be flushed even if we are in the
	// Open state.
	forceFlush
)

// buildAdvanceInfo creates an advanceInfo with a given code.
//
// Args:
// curState: The connection's state when the cursor advancement is to be
// 	 performed.
// code: The advancing instructions.
// flush: Options for flushing. Must be flushIfNotOpen if curState != Open. If
//   we're in any state but open, the flushing is implied.
func buildAdvanceInfo(curState TxnStateEnum2, code advanceCode, flush flushOpt) advanceInfo {
	if flush == forceFlush && curState != StateOpen {
		panic(fmt.Sprintf("can't specify forceFlush in state: %s != Open", curState))
	}
	// We'll flush if we're in any state but Open (since that means that we don't
	// ever need to rewind past this point) or if we've been specifically
	// instructed to flush (in which case the caller took particular
	// responsibility to not ever rewind past this point).
	flushVal := (curState != StateOpen) || (flush == forceFlush)
	return advanceInfo{
		code:  code,
		flush: flushVal,
	}
}

// buildRewindInstructions creates an advanceInfo with code rewind.
// This can only be used with code == rewind and curState == Open.
func buildRewindInstructions(curState TxnStateEnum2, rewCap rewindCapability) advanceInfo {
	if curState != StateOpen {
		panic(fmt.Sprintf("can't rewind from state %s != Open", curState))
	}
	return advanceInfo{
		code:   rewind,
		flush:  false,
		rewCap: rewCap,
	}
}

// rewindInterface represents the subset of ConnExecutor that
// performStateTransition() needs. It contains methods related to rewinding.
type rewindInterface interface {
	// getRewindTxnCapability checks whether rewinding to the position previously
	// set through setTxnStartPos() is possible and, if it is, returns a
	// rewindCapability bound to that position. The returned bool is true if the
	// rewind is possible. If it is, client communication is blocked until the
	// rewindCapability is exercised.
	getRewindTxnCapability() (rewindCapability, bool)

	// setTxnStartPos resets the position to which a future rewind operation will
	// refer.
	setTxnStartPos(context.Context, cursorPosition)
}

// invalidTransitionError is returned for invalid state transitions.
type invalidTransitionError struct {
	fromState stateMachineState
	msg       string
	ev        inputEvent
}

var _ error = &invalidTransitionError{}

func newInvalidTransitionError(fromState stateMachineState, ev inputEvent) error {
	return &invalidTransitionError{
		fromState: fromState,
		ev:        ev,
	}
}

func newInvalidTransitionErrorWithMsg(
	fromState stateMachineState, ev inputEvent, msg string,
) error {
	e := newInvalidTransitionError(fromState, ev)
	e.(*invalidTransitionError).msg = msg
	return e
}

func (e *invalidTransitionError) Error() string {
	if e.msg != "" {
		return fmt.Sprintf("invalid transition from %s of type %s: %s",
			e.fromState, e.ev.typ, e.msg)
	}
	return fmt.Sprintf("invalid transition from %s of type %s",
		e.fromState, e.ev.typ)
}

type transitionCtx struct {
	db     *client.DB
	nodeID roachpb.NodeID
	clock  *hlc.Clock
	// connMon is the ConnExecutor's monitor. New transactions will create a child
	// monitor tracking txn-scoped objects.
	connMon *mon.BytesMonitor
	// The Tracer used to create root spans for new txns if the parent ctx doesn't
	// have a span.
	tracer opentracing.Tracer

	DefaultIsolationLevel enginepb.IsolationType
}

// Here's a list of all the supported transitions. Note that this only talks
// about transitions in the stateMachineState; it doesn't talk about about the
// side effects to the other members of txnState or to the resulting
// advanceInfo. For those, see the implementation in txnState.performStateTransition().
//
// Transitions:
//
// From (top level state, retryIntent)  -> To (tls, retryIntent) | eventType && (condition)
// ========================================================================================
// (s, x)        -> (s, x)               | noTopLevelTransition && !retryIntentSet
// (Open, false) -> (Open, true)         | noTopLevelTransition && retryIntentSet
// (NoTxn, *)    -> (Open, false)        | txnStarting
// (Open, *)     -> (NoTxn, _)           | txnFinishing
// (Open, x)     -> (Aborted, x)         | nonRetriableErr && !isCommit(ev.errDetails.stmt)
// (Open, *)     -> (NoTxn, _)           | nonRetriableErr && isCommit(ev.errDetails.stmt)
// (Open, x)     -> (Open, x)            | retriableErr && canAutoRetry
// (Open, false) -> (Aborted, false)     | retriableErr && !canAutoRetry
// (Open, true)  -> (RestartWait, true)  | retriableErr && !canAutoRetry
// (Open, true)  -> (NoTxn, _)           | retriableErr && !canAutoRetry && isCommit(ev.errDetails.stmt)
// (Aborted, *)  -> (NoTxn, _) 			     | txnFinished
// (Aborted, x)  -> (Open, x)            | txnStarting   (happens on ROLLBACK TO SAVEPOINT)
// (Aborted, x)  -> (Aborted, x)         | nonRetriableErr
// (RestartWait, *) -> (NoTxn, _)        | txnFinished
// (RestartWait, true) -> (Open, true)   | txnRestarted
// (RestartWait, true) -> (Aborted, true)| nonRetriableErr
//
// Legend:
// * - any value
// _ - undefined value
// x - any value when present on the left-hand side, the value from the lhs when
//     present on the rhs.

// performStateTransition deals with maintaining the transaction state after a
// statement has been executed. It performs the state transition indicated by
// ev and returns instructions describing which statement should be executed
// next.
//
// This method is the only one that manipulates ts.sts.
//
// ctx: The ConnExecutor's context. Note that while we are inside a txn, ts.Ctx
// 	 should generally be passed to operations.
// ev: The inputEvent causing the transition.
// rew: The rewindInterface to be used for registering transaction starts and
//   checking whether we can rewind to the beginning of a txn.
// curPos: The position of the statement that triggered this transition. It
//   might have to be remembered for future rewinds of the statement buffer and
//   results.
// tranCtx: A bag of extra execution context.
func (ts *txnState2) performStateTransition(
	ctx context.Context,
	ev inputEvent,
	rew rewindInterface,
	curPos cursorPosition,
	tranCtx transitionCtx,
) (retVal advanceInfo, retErr error) {

	defer func() {
		if ts.sts.topLevelState != StateOpen {
			ts.autoRetryCounter = 0
		} else if retVal.code == rewind {
			ts.autoRetryCounter++
		}
	}()

	if ev.typ == noTopLevelTransition {
		if ev.noTopLevelTransitionDetails.retryIntent != retryIntentUnknown {
			if ts.sts.topLevelState != StateOpen {
				return advanceInfo{}, newInvalidTransitionErrorWithMsg(
					ts.sts, ev, "cannot set retryIntent while not in the Open state")
			}
			if ev.noTopLevelTransitionDetails.retryIntent != retryIntentSet {
				return advanceInfo{}, newInvalidTransitionErrorWithMsg(
					ts.sts, ev, fmt.Sprintf("unexpected retryIntent: %t", ts.sts.retryIntent))
			}
			ts.sts.retryIntent = true
		}

		flush := flushIfNotOpen
		if ts.sts.topLevelState == StateOpen && ev.noTopLevelTransitionDetails.flushResults {
			flush = forceFlush
		}
		return buildAdvanceInfo(ts.sts.topLevelState, advanceOne, flush), nil
	}

	switch ts.sts.topLevelState {
	case StateNoTxn:
		switch ev.typ {
		case txnStarting:
			// BEGIN, or before any statement running as an implicit txn.
			ts.resetForNewSQLTxn(
				ctx,
				ev.txnStartingDetails.txnType,
				tranCtx.clock.PhysicalTime(), /* sqlTimestamp */
				tranCtx.DefaultIsolationLevel,
				roachpb.NormalUserPriority,
				ev.txnStartingDetails.firstStmtPos,
				rew,
				tranCtx,
			)
			ts.SetTopLevelState(ts.Ctx, StateOpen)
			// A new transaction always starts without the retry intent.
			ts.sts.retryIntent = false
			// When starting a transaction from the NoTxn state, we don't advance
			// the cursor - we want the same statement to be executed again. Note
			// that this is true for both implicit and explicit transactions. See
			// execStmtInNoTxn() for details.
			//
			// Also note that we flush the results here.
			// ev.txnStartingDetails.firstStmtPos has been set such that we never
			// rewind below this point.
			return buildAdvanceInfo(ts.sts.topLevelState, stayInPlace, forceFlush), nil
		default:
			return advanceInfo{}, newInvalidTransitionError(ts.sts, ev)
		}

	case StateOpen:
		switch ev.typ {
		case txnFinished:
			// COMMIT/ROLLBACK
			ts.finishSQLTxn(ctx, resetKVTxn)
			ts.SetTopLevelState(ctx, StateNoTxn)
			return buildAdvanceInfo(ts.sts.topLevelState, advanceOne, flushIfNotOpen), nil
		case retriableErr:
			// We got a retriable error. Depending on the situation, we may, in the
			// order of preference:
			// a) automatically retry the transaction, or
			// b) move to RestartWait, expecting the client to perform the
			// client-directed retry protocol, or
			// c) move to Aborted.

			rewCap, canAutoRetry := rew.getRewindTxnCapability()
			if canAutoRetry {
				// We leave the transaction in Open. In particular, we don't move to
				// RestartWait, as there'd be nothing to move us back from RestartWait
				// to Open.
				// Note: Preparing the KV txn for restart has happened below our
				// control: it's done by the TxnCoordSender.

				// The caller will need to call rewCap.rewindAndUnlock().
				return buildRewindInstructions(ts.sts.topLevelState, rewCap), nil
			}
			// If the client is implementing the client-directed retries protocol, we
			// move to RestartWait. Unless the error was encountered by a COMMIT; in
			// which case the client did not obey the protocol correctly and we can't
			// move to RestartWait.
			if ts.sts.retryIntent && !isCommit(ev.errDetails.stmt) {
				ts.SetTopLevelState(ts.Ctx, StateRestartWait)
				return buildAdvanceInfo(ts.sts.topLevelState, skipQueryStr, flushIfNotOpen), nil
			}
			// If we can't auto-retry and also the client didn't declare interest in
			// the client-directed retry protocol, we handle the retriable error just
			// like every other error.
			fallthrough
		case nonRetriableErr:
			ts.mu.txn.CleanupOnError(ctx, ev.errDetails.err)
			// We move to either Aborted (in which case the client will have to send a
			// ROLLBACK), or to NoTxn, depending on the case.
			// To respect Postgres behavior, COMMIT doesn't leave the transaction in
			// Aborted.
			moveToAborted := (ts.txnType == explicitTxn) && !isCommit(ev.errDetails.stmt)
			if moveToAborted {
				ts.mu.txn = nil
				ts.SetTopLevelState(ts.Ctx, StateAborted)
			} else {
				ts.finishSQLTxn(ctx, resetKVTxn)
				ts.SetTopLevelState(ts.Ctx, StateNoTxn)
			}
			return buildAdvanceInfo(ts.sts.topLevelState, skipQueryStr, flushIfNotOpen), nil
		default:
			return advanceInfo{}, newInvalidTransitionError(ts.sts, ev)
		}

	case StateAborted:
		switch ev.typ {
		case txnFinished:
			// ROLLBACK
			ts.finishSQLTxn(ctx, kvTxnAlreadyReset)
			ts.SetTopLevelState(ctx, StateNoTxn)
			return buildAdvanceInfo(ts.sts.topLevelState, advanceOne, flushIfNotOpen), nil
		case txnStarting:
			// ROLLBACK TO SAVEPOINT
			if ev.txnStartingDetails.txnType == implicitTxn {
				return advanceInfo{}, newInvalidTransitionErrorWithMsg(
					ts.sts, ev,
					"Trying to start an implicit txn from the Aborted state. How can that be?")
			}

			ts.finishSQLTxn(ctx, kvTxnAlreadyReset)
			ts.resetForNewSQLTxn(
				ctx,
				ev.txnStartingDetails.txnType,
				ts.sqlTimestamp, ts.isolation, ts.priority,
				ev.txnStartingDetails.firstStmtPos,
				rew,
				tranCtx,
			)
			// N.B. ts.sts.retryIntent stays unchanged.
			ts.SetTopLevelState(ts.Ctx, StateOpen)

			return buildAdvanceInfo(ts.sts.topLevelState, advanceOne, forceFlush), nil
		case nonRetriableErr:
			// Any other statement.
			return buildAdvanceInfo(ts.sts.topLevelState, skipQueryStr, flushIfNotOpen), nil
		default:
			return advanceInfo{}, newInvalidTransitionError(ts.sts, ev)
		}

	case StateRestartWait:
		switch ev.typ {
		case txnFinished:
			// ROLLBACK (and also COMMIT which acts like ROLLBACK)
			if err := ts.mu.txn.Rollback(ts.Ctx); err != nil {
				log.Warningf(ts.Ctx, "txn rollback failed: %s", err)
			}
			ts.finishSQLTxn(ts.Ctx, resetKVTxn)
			ts.SetTopLevelState(ctx, StateNoTxn)
			return buildAdvanceInfo(ts.sts.topLevelState, advanceOne, flushIfNotOpen), nil
		case txnRestarted:
			// ROLLBACK TO SAVEPOINT
			// NOTE: The KV txn has been prepared for a restart before the state moved
			// to RestartWait, as per the contract of ev.errFields.retriableErr.
			// There's nothing for us to do here about the KV txn.
			ts.SetTopLevelState(ts.Ctx, StateOpen)
			// Move the transaction start position forward. Note that we're not
			// rewinding the statements here; the transaction is starting again at the
			// current position. If a retriable error is encountered from now on,
			// we'll rewind to this point not to the original BEGIN.
			rew.setTxnStartPos(ctx, ev.txnStartingDetails.firstStmtPos)
			return buildAdvanceInfo(ts.sts.topLevelState, advanceOne, forceFlush), nil
		case nonRetriableErr:
			// Any statement but the ones handled above.
			ts.mu.txn.CleanupOnError(ctx, ev.errDetails.err)
			ts.mu.txn = nil
			ts.SetTopLevelState(ts.Ctx, StateAborted)
			return buildAdvanceInfo(ts.sts.topLevelState, skipQueryStr, flushIfNotOpen), nil
		default:
			return advanceInfo{}, newInvalidTransitionError(ts.sts, ev)
		}
	}
	panic("not reached")
}

// isCommit returns true if stmt is a "COMMIT" statement.
func isCommit(stmt tree.Statement) bool {
	_, ok := stmt.(*tree.CommitTransaction)
	return ok
}
