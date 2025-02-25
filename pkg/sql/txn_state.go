// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"go.opentelemetry.io/otel/attribute"
)

// txnState contains state associated with an ongoing SQL txn; it constitutes
// the ExtendedState of a connExecutor's state machine (defined in conn_fsm.go).
// It contains fields that are mutated as side-effects of state transitions;
// notably the kv.Txn.
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
		txnStart crtime.Mono

		// The transaction's priority.
		priority roachpb.UserPriority

		// The transaction's isolation level.
		isolationLevel isolation.Level

		// stmtCount keeps track of the number of statements that the transaction
		// has executed.
		stmtCount int

		// autoRetryReason records the error causing an auto-retryable error event
		// if the current transaction is being automatically retried. This is used
		// in statement traces to give more information in statement diagnostic
		// bundles, and also is surfaced in the DB Console.
		autoRetryReason error

		// autoRetryCounter keeps track of the number of automatic retries that have
		// occurred. It includes per-statement retries performed under READ
		// COMMITTED as well as transaction retries for serialization failures under
		// REPEATABLE READ and SERIALIZABLE. It's 0 whenever the transaction state
		// is not stateOpen.
		autoRetryCounter int32

		hasSavepoints bool
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

	// txnCancelFn is a function that can be used to cancel the current
	// txn context.
	txnCancelFn context.CancelFunc

	// recordingThreshold, is not zero, indicates that sp is recording and that
	// the recording should be dumped to the log if execution of the transaction
	// took more than this.
	recordingThreshold time.Duration
	recordingStart     time.Time

	// The timestamp to report for current_timestamp(), now() etc.
	// This must be constant for the lifetime of a SQL transaction.
	sqlTimestamp time.Time

	// The transaction's read only state.
	readOnly atomic.Bool

	// Set to true when the current transaction is using a historical timestamp
	// through the use of AS OF SYSTEM TIME.
	isHistorical atomic.Bool

	// injectedTxnRetryCounter keeps track of how many errors have been
	// injected in this transaction with the inject_retry_errors_enabled
	// flag.
	injectedTxnRetryCounter int

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
	// upgradedExplicitTxn means that the txn started as implicit, but a BEGIN
	// in the middle of it caused it to become explicit.
	upgradedExplicitTxn
)

// resetForNewSQLTxn (re)initializes the txnState for a new transaction.
// It creates a new kv.Txn and initializes it using the session defaults
// and returns the ID of the new transaction.
//
// connCtx: The context in which the new transaction is started (usually a
// connection's context). ts.Ctx will be set to a child context and should be
// used for everything that happens within this SQL transaction.
//
// txnType: The type of the starting txn.
//
// sqlTimestamp: The timestamp to report for current_timestamp(), now() etc.
//
// historicalTimestamp: If non-nil indicates that the transaction is historical
// and should be fixed to this timestamp.
//
// priority: The transaction's priority. Pass roachpb.UnspecifiedUserPriority if the txn arg is
// not nil.
//
// readOnly: The read-only character of the new txn.
//
// txn: If not nil, this txn will be used instead of creating a new txn. If so,
// all the other arguments need to correspond to the attributes of this txn
// (unless otherwise specified).
//
// tranCtx: A bag of extra execution context.
//
// qualityOfService: If txn is nil, the QoSLevel/WorkPriority to assign the new
// transaction for use in admission queues.
func (ts *txnState) resetForNewSQLTxn(
	connCtx context.Context,
	txnType txnType,
	sqlTimestamp time.Time,
	historicalTimestamp *hlc.Timestamp,
	priority roachpb.UserPriority,
	readOnly tree.ReadWriteMode,
	txn *kv.Txn,
	tranCtx transitionCtx,
	qualityOfService sessiondatapb.QoSLevel,
	isoLevel isolation.Level,
	omitInRangefeeds bool,
	bufferedWritesEnabled bool,
) (txnID uuid.UUID) {
	// Reset state vars to defaults.
	ts.sqlTimestamp = sqlTimestamp
	ts.isHistorical.Swap(false)
	ts.injectedTxnRetryCounter = 0

	// Create a context for this transaction. It will include a root span that
	// will contain everything executed as part of the upcoming SQL txn, including
	// (automatic or user-directed) retries. The span is closed by finishSQLTxn().
	opName := sqlTxnName
	alreadyRecording := tranCtx.sessionTracing.Enabled()
	ctx, cancelFn := context.WithCancel(connCtx)
	var sp *tracing.Span
	duration := traceTxnThreshold.Get(&tranCtx.settings.SV)
	if alreadyRecording || duration > 0 {
		ts.Ctx, sp = tracing.EnsureChildSpan(ctx, tranCtx.tracer, opName,
			tracing.WithRecording(tracingpb.RecordingVerbose))
	} else if ts.testingForceRealTracingSpans {
		ts.Ctx, sp = tracing.EnsureChildSpan(ctx, tranCtx.tracer, opName, tracing.WithForceRealSpan())
	} else {
		ts.Ctx, sp = tracing.EnsureChildSpan(ctx, tranCtx.tracer, opName)
	}
	ts.txnCancelFn = cancelFn
	if txnType == implicitTxn {
		sp.SetTag("implicit", attribute.StringValue("true"))
	}

	if !alreadyRecording && (duration > 0) {
		ts.recordingThreshold = duration
		ts.recordingStart = timeutil.Now()
	}

	ts.mon.StartNoReserved(ts.Ctx, tranCtx.connMon)
	txnID = func() (txnID uuid.UUID) {
		ts.mu.Lock()
		defer ts.mu.Unlock()

		ts.mu.stmtCount = 0
		if txn == nil {
			ts.mu.txn = kv.NewTxnWithSteppingEnabled(ts.Ctx, tranCtx.db, tranCtx.nodeIDOrZero, qualityOfService)
			ts.mu.txn.SetDebugName(opName)
			if omitInRangefeeds {
				ts.mu.txn.SetOmitInRangefeeds()
			}
			if err := ts.setPriorityLocked(priority); err != nil {
				panic(err)
			}
			if err := ts.setIsolationLevelLocked(isoLevel); err != nil {
				panic(err)
			}
			if bufferedWritesEnabled {
				ts.mu.txn.SetBufferedWritesEnabled(true /* enabled */)
			}
		} else {
			if priority != roachpb.UnspecifiedUserPriority {
				panic(errors.AssertionFailedf("unexpected priority when using an existing txn: %s", priority))
			}
			ts.mu.txn = txn
		}

		txnID = ts.mu.txn.ID()
		sp.SetTag("txn", attribute.StringValue(txnID.String()))
		ts.mu.txnStart = crtime.NowMono()
		ts.mu.autoRetryCounter = 0
		ts.mu.autoRetryReason = nil
		return txnID
	}()
	if historicalTimestamp != nil {
		if err := ts.setHistoricalTimestamp(ts.Ctx, *historicalTimestamp); err != nil {
			panic(err)
		}
	}
	if err := ts.setReadOnlyMode(readOnly); err != nil {
		panic(err)
	}

	return txnID
}

// finishSQLTxn finalizes a transaction's results and closes the root span for
// the current SQL txn. This needs to be called before resetForNewSQLTxn() is
// called for starting another SQL txn. The ID of the finalized transaction is
// returned.
func (ts *txnState) finishSQLTxn() (txnID uuid.UUID, commitTimestamp hlc.Timestamp) {
	ts.mon.Stop(ts.Ctx)
	sp := tracing.SpanFromContext(ts.Ctx)

	if ts.recordingThreshold > 0 {
		if elapsed := timeutil.Since(ts.recordingStart); elapsed >= ts.recordingThreshold {
			logTraceAboveThreshold(ts.Ctx,
				sp.GetRecording(sp.RecordingType()), /* recording */
				"SQL txn",                           /* opName */
				redact.Sprint(redact.Safe(txnID)),   /* detail */
				ts.recordingThreshold,               /* threshold */
				elapsed,                             /* elapsed */
			)
		}
	}

	sp.Finish()
	if ts.txnCancelFn != nil {
		ts.txnCancelFn()
	}
	ts.Ctx = nil
	ts.recordingThreshold = 0
	return func() (txnID uuid.UUID, timestamp hlc.Timestamp) {
		ts.mu.Lock()
		defer ts.mu.Unlock()
		txnID = ts.mu.txn.ID()
		if ts.mu.txn.IsCommitted() {
			var err error
			timestamp, err = ts.mu.txn.CommitTimestamp()
			if err != nil {
				panic(errors.Wrapf(err, "failed to get commit timestamp of committed txn"))
			}
		}
		ts.mu.txn = nil
		ts.mu.txnStart = 0
		return txnID, timestamp
	}()
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

	if ts.Ctx != nil {
		if sp := tracing.SpanFromContext(ts.Ctx); sp != nil {
			sp.Finish()
		}
	}
	if ts.txnCancelFn != nil {
		ts.txnCancelFn()
	}
	ts.Ctx = nil
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.mu.txn = nil
}

func (ts *txnState) setHistoricalTimestamp(
	ctx context.Context, historicalTimestamp hlc.Timestamp,
) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if err := ts.mu.txn.SetFixedTimestamp(ctx, historicalTimestamp); err != nil {
		return err
	}
	ts.sqlTimestamp = historicalTimestamp.GoTime()
	ts.isHistorical.Swap(true)
	return nil
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
	ts.mu.priority = userPriority
	return nil
}

func (ts *txnState) setIsolationLevel(level isolation.Level) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.setIsolationLevelLocked(level)
}

func (ts *txnState) setIsolationLevelLocked(level isolation.Level) error {
	if err := ts.mu.txn.SetIsoLevel(level); err != nil {
		return err
	}
	ts.mu.isolationLevel = level
	return nil
}

func (ts *txnState) setReadOnlyMode(mode tree.ReadWriteMode) error {
	switch mode {
	case tree.UnspecifiedReadWriteMode:
		return nil
	case tree.ReadOnly:
		ts.readOnly.Swap(true)
	case tree.ReadWrite:
		if ts.isHistorical.Load() {
			return tree.ErrAsOfSpecifiedWithReadWrite
		}
		ts.readOnly.Swap(false)
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
// transaction events.
type txnEvent struct {
	// eventType is used by the connExecutor to clear state associated
	// with a SQL transaction (other than the state encapsulated in TxnState; e.g.
	// schema changes and portals).
	eventType txnEventType

	// txnID is filled when a transaction starts, commits or aborts.
	// When a transaction starts, txnID is set to the ID of the transaction that
	// was created.
	// When a transaction commits or aborts, txnID is set to the ID of the
	// transaction that just finished execution.
	txnID uuid.UUID

	// commitTimestamp is populated with the timestamp of the recently finished
	// transaction corresponding to txnID. It will only be populated if that
	// transaction committed.
	commitTimestamp hlc.Timestamp
}

//go:generate stringer -type=txnEventType
type txnEventType int

const (
	noEvent txnEventType = iota

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
	// txnPrepare means that the SQL transaction has been prepared and is now
	// being dissociated from the session. It is generated when an explicit
	// transaction runs a PREPARE TRANSACTION statement.
	txnPrepare
	// txnRestart means that the transaction is restarting. The iteration of the
	// txn just finished will not commit. It is generated when we're about to
	// auto-retry a txn and after a rollback to a savepoint placed at the start of
	// the transaction. This allows such savepoints to reset more state than other
	// savepoints.
	txnRestart
	// txnUpgradeToExplicit means that the current implicit transaction was
	// upgraded to an explicit one. This happens when BEGIN is executed during the
	// extended protocol or as part of a batch of statements. It's used to
	// indicate that the transaction rewind position should be updated.
	txnUpgradeToExplicit
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

// checkReadsAndWrites returns an error if the transaction has performed reads
// or writes.
func (ts *txnState) checkReadsAndWrites() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.mu.txn.Sender().HasPerformedReads() {
		return pgerror.Newf(
			pgcode.InvalidTransactionState,
			"cannot set fixed timestamp, txn %s already performed reads",
			ts.mu.txn)
	}

	if ts.mu.txn.Sender().HasPerformedWrites() {
		return pgerror.Newf(
			pgcode.InvalidTransactionState,
			"cannot set fixed timestamp, txn %s already performed writes",
			ts.mu.txn)
	}
	return nil
}
