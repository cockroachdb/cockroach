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
// encapsulates a connection's state and concetrates all the state mutations
// into one method - txnState.performStateTransition().
//
//txnState represents the middle layer of the ConnExecutor: it takes state
// transition instructions from the bottom execution layer and it returns
// instructions to the top layer about what statement to execute next.
//
// The "state" that the txnState manages includes:
// - the high-level transaction state (i.e. NoTxn, Open, Aborted, RetryWait,
//   etc.)
// - the KV-level client.Txn
// - the tracing spans related to transactions

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

// txnState contains state associated with an ongoing SQL txn; it is associated
// with and stored inside a ConnExecutor.
//
// The ConnExecutor does not mutate the txnState directly; instead, the
// ConnExecutor describes the desired transitions by creating transition{}s in
// response to statement execution and then mutations are done through
// performStateTransition().
//
// Depending on the exact state, there may or may not be an open KV txn
// associated with the SQL txn; if there is, that's one of the more important
// things that the txnState manages.
type txnState2 struct {
	// state represents the transactional state that the connection is currently
	// in.
	state TxnStateEnum2

	// When inside a txn, this specifies the type of txn.
	txnType txnType

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

	// cancel is the cancelation function for the above context. Called upon
	// COMMIT/ROLLBACK of the transaction to release resources associated with the
	// context. nil when no txn is in progress.
	cancel context.CancelFunc

	// If set, the user declared the intention to retry the txn in case of retriable
	// errors. The txn will enter a RestartWait state in case of such errors.
	retryIntent bool

	// The timestamp to report for current_timestamp(), now() etc.
	// This must be constant for the lifetime of a SQL transaction.
	sqlTimestamp time.Time

	// The transaction's isolation level.
	isolation enginepb.IsolationType

	// The transaction's priority.
	priority roachpb.UserPriority

	// mon tracks txn-bound objects like the running state of
	// planNode in the midst of performing a computation.
	mon mon.BytesMonitor

	// The schema change closures to run when this txn is done.
	schemaChangers schemaChangerCollection

	// spanListener, is set, is used to deliver events about txn spans.
	spanListener txnSpanListener
}

// txnSpanListener is an interface implemented by the session tracing mechanism
// for listening to transaction events. The event handlers get access to the
// span that is created/finished for a new/finished txn.
type txnSpanListener interface {
	onNewTxn(opentracing.Span)
	onTxnFinish(opentracing.Span) error
}

// transitionType is the type of a state transition.
type transitionType int

//go:generate stringer -type=transitionType
const (
	unknownTransition transitionType = iota

	// noTransition means that the state stays as it is. Note that this only
	// refers to txnState.state staying unchanged; other fields of txnState may
	// still be changed by a noTransition transition.
	noTransition

	// txnStarting means that a transaction has just been started.
	txnStarting

	// txnFinished means that a transaction has just finished.
	txnFinished

	// txnRestarted means that a transaction has just been restarted.
	txnRestarted

	// retriableErr means that a retriableErr has occurred. In this case, the kv
	// txn is expected to have been prepared for a restart (epoch incremented or
	// such) by someone. The SQL session state machine will not do that.
	retriableErr

	// nonRetriableErr means that a non-retriable error has occurred.
	nonRetriableErr
)

type transition struct {
	typ transitionType

	//
	// Fields only for retriableErr/nonRetriableErr
	//

	// err is the query execution error that's causing this transition.
	err error

	// stmt is the statement that encountered the error. Only used for its tag
	// (because errors encountered by COMMITs need special treatment).
	//
	// TODO(andrei): Figure out something more restricted to use as the tag, and
	// do the same for the ResultWriter interface.
	stmt tree.Statement

	//
	// Fields only for txnStarting
	//

	// txnType indicates the type of transaction that is starting.
	txnType txnType
	// firstStmtPos indicates the position of the first statement "inside" the
	// transaction that's just starting. That is, the first statement that has to
	// be executed again in case of an automatic retry. For an implicitTxn, this
	// is the position of the respective statement. For transactions started by a
	// BEGIN or by a ROLLBACK TO SAVEPOINT statement, this is the position just
	// after that statement.
	firstStmtPos cursorPosition

	//
	// Field only for noTransition
	//

	// retryIntent, if != retryIntentUnknown, informs the state machine that the
	// client has opted into the client-directed retries mechanism.
	// Setting this is only valid when the txn is in the Open state. A value of
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

type retryIntentEnum int

const (
	retryIntentUnknown retryIntentEnum = iota
	retryIntentSet
	retryIntentNotSet
)

// See transition.flushResults.
type flushResultOpt int

const (
	dontFlushResult flushResultOpt = iota
	flushSpecialTxnPrefix
)

func makeNoopTransition(flush flushResultOpt) transition {
	return transition{
		typ:          noTransition,
		flushResults: flush == flushSpecialTxnPrefix,
	}
}

func makeBeginTxnTransition(typ txnType, firstStmtPos cursorPosition) transition {
	return transition{
		typ:          txnStarting,
		txnType:      typ,
		firstStmtPos: firstStmtPos,
	}
}

func makeTxnFinishedTransition() transition {
	return transition{typ: txnFinished}
}

func makeErrTransition(err error, curTxn *client.Txn, stmt tree.Statement) transition {
	retErr, retriable := err.(*roachpb.HandledRetryableTxnError)
	if retriable && curTxn.IsRetryableErrMeantForTxn(*retErr) {
		return transition{
			typ:  retriableErr,
			err:  err,
			stmt: stmt,
		}
	}
	return transition{
		typ:  nonRetriableErr,
		err:  err,
		stmt: stmt,
	}
}

// SetState updates the transaction state field.
// When transitioning to NoTxn, ts.Ctx is cleared.
func (ts *txnState2) SetState(ctx context.Context, val TxnStateEnum2) {
	if val == StateNoTxn {
		ts.assertReadyToMoveToNoTxn(ctx)
	}
	ts.state = val
}

func (ts *txnState2) assertReadyToMoveToNoTxn(ctx context.Context) {
	if ts.mu.txn != nil {
		log.Fatalf(ctx,
			"attempting to move SQL state to NoTxn but KV txn still present. "+
				"Current state: %s. txn: %+v",
			ts.state, ts.mu.txn)
	}
	if ts.Ctx != nil {
		log.Fatal(ctx, "Moving to StateNoTxn but ts.Ctx has not been reset. "+
			"Current state: %s. Has finishSQLTxn been called?", ts.state)
	}
}

// resetForNewSQLTxn (re)initializes the txnState for a new transaction.
// It creates a new client.Txn and initializes it using the session defaults.
// txnState.State will be set to Open.
//
// ctx: The context in which the new transaction is started. ts.Ctx will be set
//   to a child context and should be used for everything that happens within
//   this SQL transaction.
// txnType: The type of the starting txn.
// retryIntent: Indicates whether or not the client wants to participate in the
//   client-directed retry protocol (i.e. if "SAVEPOINT cockroach_restart" has
//   been used); if it has, then we may transition to the RestartWait state
//   later.
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
	retryIntent retryIntentEnum,
	sqlTimestamp time.Time,
	isolation enginepb.IsolationType,
	priority roachpb.UserPriority,
	firstStmtPos cursorPosition,
	rew rewindInterface,
	tranCtx transitionCtx,
) {
	if ts.state != StateNoTxn {
		log.Fatalf(ctx, "Attempting to start a txn from state %s.", ts.state)
	}

	ts.retryIntent = retryIntent == retryIntentSet
	// Reset state vars to defaults.
	ts.sqlTimestamp = sqlTimestamp
	ts.txnType = txnType

	// Create a context for this transaction. It will include a
	// root span that will contain everything executed as part of the
	// upcoming SQL txn, including (automatic or user-directed) retries.
	// The span is closed by finishSQLTxn().
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

	ts.SetState(ts.Ctx, StateOpen)
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

// finishSQLTxn finalizes a transaction's results and closes the root span for
// the current SQL txn. This needs to be called before resetForNewSQLTxn() is
// called for starting another SQL txn.
// When this returns, the connection's state is set to NoTxn.
//
// ctx is the ConnExecutor's context. This will be used once ts.Ctx is
// finalized.
func (ts *txnState2) finishSQLTxn(connCtx context.Context) {
	ts.mon.Stop(ts.Ctx)
	if ts.cancel != nil {
		ts.cancel()
		ts.cancel = nil
	}
	if ts.sp == nil {
		panic("No span in context? Was resetForNewSQLTxn() called previously?")
	}

	ts.sp.Finish()
	sp := ts.sp
	ts.sp = nil
	ts.Ctx = nil
	if ts.spanListener != nil {
		if err := ts.spanListener.onTxnFinish(sp); err != nil {
			log.Errorf(connCtx, "error finishing trace: %s", err)
		}
	}
	ts.SetState(connCtx, StateNoTxn)
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

type advanceCode int

//go:generate stringer -type=advanceCode
const (
	advanceUnknown advanceCode = iota
	stayInPlace
	advanceOne
	skipQueryStr
	rewind
)

type advanceInfo struct {
	code advanceCode

	// flush, if set, means we'll flush all the accumulated results and advance
	// the "start of the transaction".
	flush bool

	// Fields for the rewind code:

	// rewCap is the capability to rewind to the beginning of the transaction.
	// rewCap.rewindAndUnlock() needs to be called to perform the promised rewind.
	//
	// This field should not be set directly; buildAdvanceInfoWithRewCap() should
	// be used.
	rewCap rewindCapability
}

type flushOpt int

const (
	flushUnknown flushOpt = iota
	forceFlush
)

// buildAdvanceInfo creates an advanceInfo with a given code.
//
// Args:
// curState: The connection's state when the cursor advancement is to be
// 	performed.
// code: The advancing instructions.
// flush: Options for flushing. Must be flushUnknown if curState != Open. If
//  we're in any state but open, the flushing is implied.
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

// buildAdvanceInfoWithRewCap creates an advanceInfo with a rewindCapability.
// This can only be used with code == rewind and curState == Open.
func buildAdvanceInfoWithRewCap(
	curState TxnStateEnum2, code advanceCode, rewCap rewindCapability,
) advanceInfo {
	if code != rewind {
		panic("can't use a rewindCapability when not rewinding")
	}
	if curState != StateOpen {
		panic(fmt.Sprintf("can't rewind from state %s != Open", curState))
	}
	return advanceInfo{
		code:   code,
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
	fromState TxnStateEnum2
	msg       string
	tran      transition
}

var _ error = &invalidTransitionError{}

func newInvalidTransitionError(fromState TxnStateEnum2, tran transition) error {
	return &invalidTransitionError{
		fromState: fromState,
		tran:      tran,
	}
}

func newInvalidTransitionErrorWithMsg(fromState TxnStateEnum2, tran transition, msg string) error {
	e := newInvalidTransitionError(fromState, tran)
	e.(*invalidTransitionError).msg = msg
	return e
}

func (e *invalidTransitionError) Error() string {
	if e.msg != "" {
		return fmt.Sprintf("invalid transition from %s of type %s: %s",
			e.fromState, e.tran.typ, e.msg)
	}
	return fmt.Sprintf("invalid transition from %s of type %s",
		e.fromState, e.tran.typ)
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

// performStateTransition deals with maintaining the transaction state after a
// statement has been executed. It performs the state transition indicated by
// tran. It returns instructions describing which statement should be executed
// next.
//
// ctx: The ConnExecutor's context. Note that while we are inside a txn, ts.Ctx
// 	 should generally be passed to operations.
// tran: The transition to perform.
// rew: The rewindInterface to be used for registering transaction starts and
//   checking whether we can rewind to the beginning of a txn.
// curPos: The position of the statement that triggered this transition. It
//   might have to be remembered for future rewinds of the statement buffer and
//   results.
// tranCtx: A bag of extra execution context.
func (ts *txnState2) performStateTransition(
	ctx context.Context,
	tran transition,
	rew rewindInterface,
	curPos cursorPosition,
	tranCtx transitionCtx,
) (advanceInfo, error) {

	if tran.typ == noTransition {
		if tran.retryIntent != retryIntentUnknown {
			if ts.state != StateOpen {
				return advanceInfo{}, newInvalidTransitionErrorWithMsg(ts.state, tran,
					"cannot set retryIntent while not in the Open state")
			}
			ts.retryIntent = true
		}

		flush := flushUnknown
		if ts.state == StateOpen && tran.flushResults {
			flush = forceFlush
		}
		return buildAdvanceInfo(ts.state, advanceOne, flush), nil
	}

	switch ts.state {
	case StateNoTxn:
		switch tran.typ {
		case txnStarting:
			// BEGIN, or before any statement running as an implicit txn.
			ts.resetForNewSQLTxn(
				ctx,
				tran.txnType,
				// A new transaction always starts without the retry intent.
				retryIntentNotSet,
				tranCtx.clock.PhysicalTime(), /* sqlTimestamp */
				tranCtx.DefaultIsolationLevel,
				roachpb.NormalUserPriority,
				tran.firstStmtPos,
				rew,
				tranCtx,
			)
			// When starting a transaction from the NoTxn state, we don't advance
			// the cursor - we want the same statement to be executed again. Note
			// that this is true for both implicit and explicit transactions. See
			// execStmtInNoTxn() for details.
			//
			// Also note that we flush the results here. tran.firstStmtPos has been
			// set such that we never rewind below this point.
			return buildAdvanceInfo(ts.state, stayInPlace, forceFlush), nil
		default:
			return advanceInfo{}, newInvalidTransitionError(ts.state, tran)
		}

	case StateOpen:
		switch tran.typ {
		case txnFinished:
			// COMMIT/ROLLBACK
			ts.finishSQLTxn(ctx)
			return buildAdvanceInfo(ts.state, advanceOne, flushUnknown), nil
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
				return buildAdvanceInfoWithRewCap(ts.state, rewind, rewCap), nil
			}
			// If the client is implementing the client-directed retries protocol, we
			// move to RestartWait. Unless the error was encountered by a COMMIT; in
			// which case the client did not obey the protocol correctly and we can't
			// move to RestartWait.
			if ts.retryIntent && !isCommit(tran.stmt) {
				ts.SetState(ts.Ctx, StateRestartWait)
				return buildAdvanceInfo(ts.state, skipQueryStr, flushUnknown), nil
			}
			// If we can't auto-retry and also the client didn't declare interest in
			// the client-directed retry protocol, we handle the retriable error just
			// like every other error.
			fallthrough
		case nonRetriableErr:
			ts.mu.txn.CleanupOnError(ctx, tran.err)
			ts.mu.txn = nil
			if ts.txnType == explicitTxn {
				if !isCommit(tran.stmt) {
					ts.SetState(ts.Ctx, StateAborted)
				} else {
					// To respect Postgres behavior, COMMIT doesn't leave the transaction
					// in Aborted.
					ts.SetState(ts.Ctx, StateNoTxn)
				}
			} else {
				if ts.txnType != implicitTxn {
					log.Fatalf(ctx, "unexpected transactionType: %d", ts.txnType)
				}
				ts.SetState(ts.Ctx, StateNoTxn)
			}
			return buildAdvanceInfo(ts.state, skipQueryStr, flushUnknown), nil
		default:
			return advanceInfo{}, newInvalidTransitionError(ts.state, tran)
		}

	case StateAborted:
		switch tran.typ {
		case txnFinished:
			// ROLLBACK
			ts.finishSQLTxn(ctx)
			return buildAdvanceInfo(ts.state, advanceOne, flushUnknown), nil
		case txnStarting:
			// ROLLBACK TO SAVEPOINT
			if tran.txnType == implicitTxn {
				return advanceInfo{}, newInvalidTransitionErrorWithMsg(
					ts.state, tran,
					"Trying to start an implicit txn from the Aborted state. How can that be?")
			}

			ts.finishSQLTxn(ctx)
			// We'll leave the current retryIntent in place.
			var retIntentEnum retryIntentEnum
			if ts.retryIntent {
				retIntentEnum = retryIntentSet
			} else {
				retIntentEnum = retryIntentNotSet
			}
			ts.resetForNewSQLTxn(
				ctx,
				tran.txnType,
				retIntentEnum,
				ts.sqlTimestamp, ts.isolation, ts.priority,
				tran.firstStmtPos,
				rew,
				tranCtx,
			)
			return buildAdvanceInfo(ts.state, advanceOne, flushUnknown), nil
		case nonRetriableErr:
			// Any other statement.
			return buildAdvanceInfo(ts.state, skipQueryStr, flushUnknown), nil
		default:
			return advanceInfo{}, newInvalidTransitionError(ts.state, tran)
		}

	case StateRestartWait:
		switch tran.typ {
		case txnFinished:
			// ROLLBACK (and also COMMIT which acts like ROLLBACK)
			ts.finishSQLTxn(ts.Ctx)
			return buildAdvanceInfo(ts.state, advanceOne, flushUnknown), nil
		case txnRestarted:
			// ROLLBACK TO SAVEPOINT
			ts.SetState(ts.Ctx, StateOpen)
			// Move the transaction start position forward. Note that we're not
			// rewinding the statements here; the transaction is starting again at the
			// current position. If a retriable error is encountered from now on,
			// we'll rewind to this point not to the original BEGIN.
			rew.setTxnStartPos(ctx, tran.firstStmtPos)
			return buildAdvanceInfo(ts.state, advanceOne, flushUnknown), nil
		case nonRetriableErr:
			// Any statement but the ones handled above.
			ts.mu.txn.CleanupOnError(ctx, tran.err)
			ts.mu.txn = nil
			ts.SetState(ts.Ctx, StateAborted)
			return buildAdvanceInfo(ts.state, skipQueryStr, flushUnknown), nil
		default:
			return advanceInfo{}, newInvalidTransitionError(ts.state, tran)
		}
	}
	panic("not reached")
}

// isCommit returns true if stmt is a "COMMIT" statement.
func isCommit(stmt tree.Statement) bool {
	_, ok := stmt.(*tree.CommitTransaction)
	return ok
}
