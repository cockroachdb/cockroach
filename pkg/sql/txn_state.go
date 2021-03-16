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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// txnState contains state associated with an ongoing SQL txn; it constitutes
// the ExtendedState of a connExecutor's state machine (defined in conn_fsm.go).
// It contains fields that are mutated as side-effects of state transitions;
// notably the KV client.Txn.  All mutations to txnState are performed through
// calling fsm.Machine.Apply(event); see conn_fsm.go for the definition of the
// state machine.
type txnState struct {
	// Mutable fields accessed from goroutines not synchronized by this txn's
	// session, such as when a SHOW SESSIONS statement is executed on another
	// session.
	//
	// Note that reads of mu.txn from the session's main goroutine do not require
	// acquiring a read lock - since only that goroutine will ever write to
	// mu.txn. Writes to mu.txn do require a write lock to guarantee safety with
	// reads by other goroutines.
	mu struct {
		syncutil.RWMutex

		txn *kv.Txn

		// txnStart records the time that txn started.
		txnStart time.Time

		// stmtCount keeps track of the number of statements that the transaction
		// has executed.
		stmtCount int
	}

	// connCtx is the connection's context. This is the parent of Ctx.
	connCtx context.Context

	// Ctx is the context for everything running in this SQL txn.
	// This is only set while the session's state is not stateNoTxn.
	//
	// It also embeds the tracing span associated with the SQL txn. These are
	// often root spans, as SQL txns are frequently the level at which we do
	// tracing. This context is hijacked when session tracing is enabled.
	Ctx context.Context

	// recordingThreshold, is not zero, indicates that sp is recording and that
	// the recording should be dumped to the log if execution of the transaction
	// took more than this.
	recordingThreshold time.Duration
	recordingStart     time.Time

	// cancel is Ctx's cancellation function. Called upon COMMIT/ROLLBACK of the
	// transaction to release resources associated with the context. nil when no
	// txn is in progress.
	cancel context.CancelFunc

	// The timestamp to report for current_timestamp(), now() etc.
	// This must be constant for the lifetime of a SQL transaction.
	sqlTimestamp time.Time

	// The transaction's priority.
	priority roachpb.UserPriority

	// The transaction's read only state.
	readOnly bool

	// Set to true when the current transaction is using a historical timestamp
	// through the use of AS OF SYSTEM TIME.
	isHistorical bool

	// mon tracks txn-bound objects like the running state of
	// planNode in the midst of performing a computation.
	mon *mon.BytesMonitor

	// adv is overwritten after every transition. It represents instructions for
	// for moving the cursor over the stream of input statements to the next
	// statement to be executed.
	// Do not use directly; set through setAdvanceInfo() and read through
	// consumeAdvanceInfo().
	adv advanceInfo

	// txnAbortCount is incremented whenever the state transitions to
	// stateAborted.
	txnAbortCount *metric.Counter

	// testingForceRealTracingSpans is a test-only knob that forces the use of
	// real (i.e. not no-op) tracing spans for every statement.
	testingForceRealTracingSpans bool
}

// txnType represents the type of a SQL transaction.
type txnType int

//go:generate stringer -type=txnType
const (
	// implicitTxn means that the txn was created for a (single) SQL statement
	// executed outside of a transaction.
	implicitTxn txnType = iota
	// explicitTxn means that the txn was explicitly started with a BEGIN
	// statement.
	explicitTxn
)

// resetForNewSQLTxn (re)initializes the txnState for a new transaction.
// It creates a new client.Txn and initializes it using the session defaults.
//
// connCtx: The context in which the new transaction is started (usually a
// 	 connection's context). ts.Ctx will be set to a child context and should be
// 	 used for everything that happens within this SQL transaction.
// txnType: The type of the starting txn.
// sqlTimestamp: The timestamp to report for current_timestamp(), now() etc.
// historicalTimestamp: If non-nil indicates that the transaction is historical
//   and should be fixed to this timestamp.
// priority: The transaction's priority. Pass roachpb.UnspecifiedUserPriority if the txn arg is
//   not nil.
// readOnly: The read-only character of the new txn.
// txn: If not nil, this txn will be used instead of creating a new txn. If so,
//   all the other arguments need to correspond to the attributes of this txn
//   (unless otherwise specified).
// tranCtx: A bag of extra execution context.
func (ts *txnState) resetForNewSQLTxn(
	connCtx context.Context,
	txnType txnType,
	sqlTimestamp time.Time,
	historicalTimestamp *hlc.Timestamp,
	priority roachpb.UserPriority,
	readOnly tree.ReadWriteMode,
	txn *kv.Txn,
	tranCtx transitionCtx,
) {
	// Reset state vars to defaults.
	ts.sqlTimestamp = sqlTimestamp
	ts.isHistorical = false

	// Create a context for this transaction. It will include a root span that
	// will contain everything executed as part of the upcoming SQL txn, including
	// (automatic or user-directed) retries. The span is closed by finishSQLTxn().
	opName := sqlTxnName
	alreadyRecording := tranCtx.sessionTracing.Enabled()

	var txnCtx context.Context
	var sp *tracing.Span
	if alreadyRecording || ts.testingForceRealTracingSpans {
		// WithForceRealSpan is used to support the use of session tracing,
		// which will start recording on this span.
		txnCtx, sp = createRootOrChildSpan(connCtx, opName, tranCtx.tracer, tracing.WithForceRealSpan())
	} else {
		txnCtx, sp = createRootOrChildSpan(connCtx, opName, tranCtx.tracer)
	}
	if txnType == implicitTxn {
		sp.SetTag("implicit", "true")
	}

	duration := traceTxnThreshold.Get(&tranCtx.settings.SV)
	if !alreadyRecording && (duration > 0) {
		sp.SetVerbose(true)
		ts.recordingThreshold = duration
		ts.recordingStart = timeutil.Now()
	}

	ts.Ctx, ts.cancel = contextutil.WithCancel(txnCtx)
	ts.mon.Start(ts.Ctx, tranCtx.connMon, mon.BoundAccount{} /* reserved */)
	ts.mu.Lock()
	ts.mu.stmtCount = 0
	if txn == nil {
		ts.mu.txn = kv.NewTxnWithSteppingEnabled(ts.Ctx, tranCtx.db, tranCtx.nodeIDOrZero)
		ts.mu.txn.SetDebugName(opName)
		if err := ts.setPriorityLocked(priority); err != nil {
			panic(err)
		}
	} else {
		if priority != roachpb.UnspecifiedUserPriority {
			panic(errors.AssertionFailedf("unexpected priority when using an existing txn: %s", priority))
		}
		ts.mu.txn = txn
	}
	ts.mu.txnStart = timeutil.Now()
	ts.mu.Unlock()
	if historicalTimestamp != nil {
		ts.setHistoricalTimestamp(ts.Ctx, *historicalTimestamp)
	}
	if err := ts.setReadOnlyMode(readOnly); err != nil {
		panic(err)
	}
}

// finishSQLTxn finalizes a transaction's results and closes the root span for
// the current SQL txn. This needs to be called before resetForNewSQLTxn() is
// called for starting another SQL txn.
func (ts *txnState) finishSQLTxn() {
	ts.mon.Stop(ts.Ctx)
	if ts.cancel != nil {
		ts.cancel()
		ts.cancel = nil
	}
	sp := tracing.SpanFromContext(ts.Ctx)
	if sp == nil {
		panic(errors.AssertionFailedf("No span in context? Was resetForNewSQLTxn() called previously?"))
	}

	if ts.recordingThreshold > 0 {
		logTraceAboveThreshold(ts.Ctx, sp.GetRecording(), "SQL txn", ts.recordingThreshold, timeutil.Since(ts.recordingStart))
	}

	sp.Finish()
	ts.Ctx = nil
	ts.mu.Lock()
	ts.mu.txn = nil
	ts.mu.txnStart = time.Time{}
	ts.mu.Unlock()
	ts.recordingThreshold = 0
}

// finishExternalTxn is a stripped-down version of finishSQLTxn used by
// connExecutors that run within a higher-level transaction (through the
// InternalExecutor). These guys don't want to mess with the transaction per-se,
// but still want to clean up other stuff.
func (ts *txnState) finishExternalTxn() {
	if ts.Ctx == nil {
		ts.mon.Stop(ts.connCtx)
	} else {
		ts.mon.Stop(ts.Ctx)
	}
	if ts.cancel != nil {
		ts.cancel()
		ts.cancel = nil
	}

	if ts.Ctx != nil {
		if sp := tracing.SpanFromContext(ts.Ctx); sp != nil {
			sp.Finish()
		}
	}
	ts.Ctx = nil
	ts.mu.Lock()
	ts.mu.txn = nil
	ts.mu.Unlock()
}

func (ts *txnState) setHistoricalTimestamp(ctx context.Context, historicalTimestamp hlc.Timestamp) {
	ts.mu.Lock()
	ts.mu.txn.SetFixedTimestamp(ctx, historicalTimestamp)
	ts.mu.Unlock()
	ts.isHistorical = true
}

// getReadTimestamp returns the transaction's current read timestamp.
func (ts *txnState) getReadTimestamp() hlc.Timestamp {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.mu.txn.ReadTimestamp()
}

func (ts *txnState) setPriority(userPriority roachpb.UserPriority) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.setPriorityLocked(userPriority)
}

func (ts *txnState) setPriorityLocked(userPriority roachpb.UserPriority) error {
	if err := ts.mu.txn.SetUserPriority(userPriority); err != nil {
		return err
	}
	ts.priority = userPriority
	return nil
}

func (ts *txnState) setReadOnlyMode(mode tree.ReadWriteMode) error {
	switch mode {
	case tree.UnspecifiedReadWriteMode:
		return nil
	case tree.ReadOnly:
		ts.readOnly = true
	case tree.ReadWrite:
		if ts.isHistorical {
			return tree.ErrAsOfSpecifiedWithReadWrite
		}
		ts.readOnly = false
	default:
		return errors.AssertionFailedf("unknown read mode: %s", errors.Safe(mode))
	}
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
	// skipBatch means that the cursor should skip over any remaining commands
	// that are part of the current batch and be positioned on the first
	// comamnd in the next batch.
	skipBatch

	// rewind means that the cursor should be moved back to the position indicated
	// by rewCap.
	rewind
)

// txnEvent is part of advanceInfo, informing the connExecutor about some
// transaction events. It is used by the connExecutor to clear state associated
// with a SQL transaction (other than the state encapsulated in TxnState; e.g.
// schema changes and portals).
//
//go:generate stringer -type=txnEvent
type txnEvent int

const (
	noEvent txnEvent = iota

	// txnStart means that the statement that just ran started a new transaction.
	// Note that when a transaction is restarted, txnStart event is not emitted.
	txnStart
	// txnCommit means that the transaction has committed (successfully). This
	// doesn't mean that the SQL txn is necessarily "finished" - this event can be
	// generated by a RELEASE statement and the connection is still waiting for a
	// COMMIT.
	// This event is produced both when entering the CommitWait state and also
	// when leaving it.
	txnCommit
	// txnRollback means that the SQL transaction has been rolled back (completely
	// rolled back, not to a savepoint). It is generated when an implicit
	// transaction fails and when an explicit transaction runs a ROLLBACK.
	txnRollback
	// txnRestart means that the transaction is restarting. The iteration of the
	// txn just finished will not commit. It is generated when we're about to
	// auto-retry a txn and after a rollback to a savepoint placed at the start of
	// the transaction. This allows such savepoints to reset more state than other
	// savepoints.
	txnRestart
)

// advanceInfo represents instructions for the connExecutor about what statement
// to execute next (how to move its cursor over the input statements) and how
// to handle the results produced so far - can they be delivered to the client
// ASAP or not. advanceInfo is the "output" of performing a state transition.
type advanceInfo struct {
	code advanceCode

	// txnEvent is filled in when the transaction commits, aborts or starts
	// waiting for a retry.
	txnEvent txnEvent

	// Fields for the rewind code:

	// rewCap is the capability to rewind to the beginning of the transaction.
	// rewCap.rewindAndUnlock() needs to be called to perform the promised rewind.
	//
	// This field should not be set directly; buildRewindInstructions() should be
	// used.
	rewCap rewindCapability
}

// transitionCtx is a bag of fields needed by some state machine events.
type transitionCtx struct {
	db           *kv.DB
	nodeIDOrZero roachpb.NodeID // zero on SQL tenant servers, see #48008
	clock        *hlc.Clock
	// connMon is the connExecutor's monitor. New transactions will create a child
	// monitor tracking txn-scoped objects.
	connMon *mon.BytesMonitor
	// The Tracer used to create root spans for new txns if the parent ctx doesn't
	// have a span.
	tracer *tracing.Tracer
	// sessionTracing provides access to the session's tracing interface. The
	// state machine needs to see if session tracing is enabled.
	sessionTracing   *SessionTracing
	settings         *cluster.Settings
	execTestingKnobs ExecutorTestingKnobs
}

var noRewind = rewindCapability{}

// setAdvanceInfo sets the adv field. This has to be called as part of any state
// transition. The connExecutor is supposed to inspect adv after any transition
// and act on it.
func (ts *txnState) setAdvanceInfo(code advanceCode, rewCap rewindCapability, ev txnEvent) {
	if ts.adv.code != advanceUnknown {
		panic("previous advanceInfo has not been consume()d")
	}
	if code != rewind && rewCap != noRewind {
		panic("if rewCap is specified, code needs to be rewind")
	}
	ts.adv = advanceInfo{
		code:     code,
		rewCap:   rewCap,
		txnEvent: ev,
	}
}

// consumerAdvanceInfo returns the advanceInfo set by the last transition and
// resets the state so that another transition can overwrite it.
func (ts *txnState) consumeAdvanceInfo() advanceInfo {
	adv := ts.adv
	ts.adv = advanceInfo{}
	return adv
}
