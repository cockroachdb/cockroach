// Copyright 2015 The Cockroach Authors.
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
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// traceTxnThreshold can be used to log SQL transactions that take
// longer than duration to complete. For example, traceTxnThreshold=1s
// will log the trace for any transaction that takes 1s or longer. To
// log traces for all transactions use traceTxnThreshold=1ns. Note
// that any positive duration will enable tracing and will slow down
// all execution because traces are gathered for all transactions even
// if they are not output.
var traceTxnThreshold = settings.RegisterDurationSetting(
	"sql.trace.txn.enable_threshold",
	"duration beyond which all transactions are traced (set to 0 to disable)", 0)

// traceSessionEventLogEnabled can be used to enable the event log
// that is normally kept for every SQL connection. The event log has a
// non-trivial performance impact and also reveals SQL statements
// which may be a privacy concern.
var traceSessionEventLogEnabled = settings.RegisterBoolSetting(
	"sql.trace.session_eventlog.enabled",
	"set to true to enable session tracing", false)

// debugTrace7881Enabled causes all SQL transactions to be traced, in the hope
// that we'll catch #7881 and dump the current trace for debugging.
var debugTrace7881Enabled = envutil.EnvOrDefaultBool("COCKROACH_TRACE_7881", false)

// logStatementsExecuteEnabled causes the Executor to log executed
// statements and, if any, resulting errors.
var logStatementsExecuteEnabled = settings.RegisterBoolSetting(
	"sql.trace.log_statement_execute",
	"set to true to enable logging of executed statements", false)

// span baggage key used for marking a span
const keyFor7881Sample = "found#7881"

// DistSQLExecMode controls if and when the Executor uses DistSQL.
type DistSQLExecMode int64

const (
	// DistSQLOff means that we never use distSQL.
	DistSQLOff DistSQLExecMode = iota
	// DistSQLAuto means that we automatically decide on a case-by-case basis if
	// we use distSQL.
	DistSQLAuto
	// DistSQLOn means that we use distSQL for queries that are supported.
	DistSQLOn
	// DistSQLAlways means that we only use distSQL; unsupported queries fail.
	DistSQLAlways
)

func (m DistSQLExecMode) String() string {
	switch m {
	case DistSQLOff:
		return "off"
	case DistSQLAuto:
		return "auto"
	case DistSQLOn:
		return "on"
	case DistSQLAlways:
		return "always"
	default:
		return fmt.Sprintf("invalid (%d)", m)
	}
}

// DistSQLExecModeFromInt converts an int64 into a DistSQLExecMode
func DistSQLExecModeFromInt(val int64) DistSQLExecMode {
	return DistSQLExecMode(val)
}

// DistSQLExecModeFromString converts a string into a DistSQLExecMode
func DistSQLExecModeFromString(val string) DistSQLExecMode {
	switch strings.ToUpper(val) {
	case "OFF":
		return DistSQLOff
	case "AUTO":
		return DistSQLAuto
	case "ON":
		return DistSQLOn
	case "ALWAYS":
		return DistSQLAlways
	default:
		panic(fmt.Sprintf("unknown DistSQL mode %s", val))
	}
}

// DistSQLClusterExecMode controls the cluster default for when DistSQL is used.
var DistSQLClusterExecMode = settings.RegisterEnumSetting(
	"sql.defaults.distsql",
	"Default distributed SQL execution mode",
	"Auto",
	map[int64]string{
		int64(DistSQLOff):  "Off",
		int64(DistSQLAuto): "Auto",
		int64(DistSQLOn):   "On",
	},
)

// Session contains the state of a SQL client connection.
// Create instances using NewSession().
type Session struct {
	//
	// Session parameters, user-configurable.
	//

	// ApplicationName is the name of the application running the
	// current session. This can be used for logging and per-application
	// statistics. Change via resetApplicationName().
	ApplicationName string
	// Database indicates the "current" database for the purpose of
	// resolving names. See searchAndQualifyDatabase() for details.
	Database string
	// DefaultIsolationLevel indicates the default isolation level of
	// newly created transactions.
	DefaultIsolationLevel enginepb.IsolationType
	// DistSQLMode indicates whether to run queries using the distributed
	// execution engine.
	DistSQLMode DistSQLExecMode
	// Location indicates the current time zone.
	Location *time.Location
	// SearchPath is a list of databases that will be searched for a table name
	// before the database. Currently, this is used only for SELECTs.
	// Names in the search path must have been normalized already.
	SearchPath parser.SearchPath
	// User is the name of the user logged into the session.
	User string

	// defaults is used to restore default configuration values into
	// SET ... TO DEFAULT statements.
	defaults sessionDefaults

	//
	// State structures for the logical SQL session.
	//

	// TxnState carries information about the open transaction (if any),
	// including the retry status and the KV client Txn object.
	TxnState txnState
	// PreparedStatements and PreparedPortals store the statements/portals
	// that have been prepared via pgwire.
	PreparedStatements PreparedStatements
	PreparedPortals    PreparedPortals
	// virtualSchemas aliases Executor.virtualSchemas.
	// It is duplicated in Session to provide easier access to
	// the various methods that need this reference.
	// TODO(knz): place this in an executionContext parameter-passing
	// structure.
	virtualSchemas virtualSchemaHolder

	// planner is the "default planner" on a session, to save planner allocations
	// during serial execution. Since planners are not threadsafe, this is only
	// safe to use when a statement is not being parallelized. It must be reset
	// before using.
	planner planner

	//
	// Run-time state.
	//

	// execCfg is the configuration of the Executor that is executing this
	// session.
	execCfg *ExecutorConfig
	// distSQLPlanner is in charge of distSQL physical planning and running
	// logic.
	distSQLPlanner *distSQLPlanner
	// context is the Session's base context, to be used for all
	// SQL-related logging. See Ctx().
	context context.Context
	// eventLog for SQL statements and results.
	eventLog trace.EventLog
	// cancel is a method to call when the session terminates, to
	// release resources associated with the context above.
	// TODO(andrei): We need to either get rid of this cancel field, or
	// it needs to move to the TxnState and become a per-txn
	// cancel. Right now, we're cancelling all the txns that have ever
	// run on this session when the session is closed, as opposed to
	// cancelling the individual transactions as soon as they
	// COMMIT/ROLLBACK.
	cancel context.CancelFunc
	// parallelizeQueue is a queue managing all parallelized SQL statements
	// running in this session.
	parallelizeQueue ParallelizeQueue
	// mon tracks memory usage for SQL activity within this session. It
	// is not directly used, but rather indirectly used via sessionMon
	// and TxnState.mon. sessionMon tracks session-bound objects like prepared
	// statements and result sets.
	//
	// The reason why TxnState.mon and mon are split is to enable
	// separate reporting of statistics per transaction and per
	// session. This is because the "interesting" behavior w.r.t memory
	// is typically caused by transactions, not sessions. The reason why
	// sessionMon and mon are split is to enable separate reporting of
	// statistics for result sets (which escape transactions).
	mon        mon.MemoryMonitor
	sessionMon mon.MemoryMonitor

	Tracing SessionTracing

	leases LeaseCollection

	// If set, contains the in progress COPY FROM columns.
	copyFrom *copyNode

	//
	// Testing state.
	//

	// If set, called after the Session is done executing the current SQL statement.
	// It can be used to verify assumptions about how metadata will be asynchronously
	// updated. Note that this can overwrite a previous callback that was waiting to be
	// verified, which is not ideal.
	testingVerifyMetadataFn func(config.SystemConfig) error
	verifyFnCheckedOnce     bool

	//
	// Per-session statistics.
	//

	// memMetrics track memory usage by SQL execution.
	memMetrics *MemoryMetrics
	// sqlStats tracks per-application statistics for all
	// applications on each node.
	sqlStats *sqlStats
	// appStats track per-application SQL usage statistics.
	appStats *appStats
	// phaseTimes tracks session-level phase times. It is copied-by-value
	// to each planner in session.newPlanner.
	phaseTimes phaseTimes

	// noCopy is placed here to guarantee that Session objects are not
	// copied.
	noCopy util.NoCopy
}

// sessionDefaults mirrors fields in Session, for restoring default
// configuration values in SET ... TO DEFAULT statements.
type sessionDefaults struct {
	applicationName string
	database        string
}

// SessionArgs contains arguments for creating a new Session with NewSession().
type SessionArgs struct {
	Database        string
	User            string
	ApplicationName string
}

// NewSession creates and initializes a new Session object.
// remote can be nil.
func NewSession(
	ctx context.Context, args SessionArgs, e *Executor, remote net.Addr, memMetrics *MemoryMetrics,
) *Session {
	ctx = e.AnnotateCtx(ctx)
	distSQLMode := DistSQLExecModeFromInt(DistSQLClusterExecMode.Get())
	if e.cfg.TestingKnobs.OverrideDistSQLMode != nil {
		distSQLMode = DistSQLExecModeFromInt(e.cfg.TestingKnobs.OverrideDistSQLMode.Get())
	}
	s := &Session{
		Database:         args.Database,
		DistSQLMode:      distSQLMode,
		SearchPath:       sqlbase.DefaultSearchPath,
		Location:         time.UTC,
		User:             args.User,
		virtualSchemas:   e.virtualSchemas,
		execCfg:          &e.cfg,
		distSQLPlanner:   e.distSQLPlanner,
		parallelizeQueue: MakeParallelizeQueue(NewSpanBasedDependencyAnalyzer()),
		memMetrics:       memMetrics,
		sqlStats:         &e.sqlStats,
		defaults: sessionDefaults{
			applicationName: args.ApplicationName,
			database:        args.Database,
		},
		leases: LeaseCollection{
			leaseMgr:      e.cfg.LeaseManager,
			databaseCache: e.getDatabaseCache(),
		},
	}
	s.phaseTimes[sessionInit] = timeutil.Now()
	s.resetApplicationName(args.ApplicationName)
	s.PreparedStatements = makePreparedStatements(s)
	s.PreparedPortals = makePreparedPortals(s)
	s.Tracing.session = s

	if traceSessionEventLogEnabled.Get() {
		remoteStr := "<admin>"
		if remote != nil {
			remoteStr = remote.String()
		}
		s.eventLog = trace.NewEventLog(fmt.Sprintf("sql [%s]", args.User), remoteStr)
	}
	s.context, s.cancel = context.WithCancel(ctx)

	return s
}

// Finish releases resources held by the Session. It is called by the Session's
// main goroutine, so no synchronous queries will be in-flight during the
// method's execution. However, it could be called when asynchronous queries are
// operating in the background in the case of parallelized statements, which
// is why we make sure to drain background statements.
func (s *Session) Finish(e *Executor) {
	log.VEvent(s.context, 2, "finishing session")

	if s.mon == (mon.MemoryMonitor{}) {
		// This check won't catch the cases where Finish is never called, but it's
		// proven to be easier to remember to call Finish than it is to call
		// StartMonitor.
		panic("session.Finish: session monitors were never initialized. Missing call " +
			"to session.StartMonitor?")
	}

	// Make sure that no statements remain in the ParallelizeQueue. If no statements
	// are in the queue, this will be a no-op. If there are statements in the
	// queue, they would have eventually drained on their own, but if we don't
	// wait here, we risk alarming the MemoryMonitor. We ignore the error because
	// it will only ever be non-nil if there are statements in the queue, meaning
	// that the Session was abandoned in the middle of a transaction, in which
	// case the error doesn't matter.
	//
	// TODO(nvanbenschoten): Once we have better support for cancelling ongoing
	// statement execution by the infrastructure added to support CancelRequest,
	// we should try to actively drain this queue instead of passively waiting
	// for it to drain.
	_ = s.parallelizeQueue.Wait()

	// If we're inside a txn, roll it back.
	if s.TxnState.State.kvTxnIsOpen() {
		s.TxnState.updateStateAndCleanupOnErr(
			errors.Errorf("session closing"), e)
	}
	if s.TxnState.State != NoTxn {
		s.TxnState.finishSQLTxn(s.context, s)
	}

	// Cleanup leases. We might have unreleased leases if we're finishing the
	// session abruptly in the middle of a transaction, or, until #7648 is
	// addressed, there might be leases accumulated by preparing statements.
	s.leases.releaseLeases(s.context)

	s.ClearStatementsAndPortals(s.context)
	s.sessionMon.Stop(s.context)
	s.mon.Stop(s.context)

	if s.eventLog != nil {
		s.eventLog.Finish()
		s.eventLog = nil
	}

	if s.Tracing.tracing {
		if err := s.Tracing.StopTracing(); err != nil {
			log.Infof(s.context, "error stopping tracing: %s", err)
		}
	}

	// This will stop the heartbeating of the of the txn record.
	// TODO(andrei): This shouldn't have any effect, since, if there was a
	// transaction, we just explicitly rolled it back above, so the heartbeat loop
	// in the TxnCoordSender should not be waiting on this channel any more.
	// Consider getting rid of this cancel field all-together.
	s.cancel()
}

// Ctx returns the current context for the session: if there is an active SQL
// transaction it returns the transaction context, otherwise it returns the
// session context.
// Note that in some cases we may want the session context even if there is an
// active transaction (an example is when we want to log an event to the session
// event log); in that case s.context should be used directly.
func (s *Session) Ctx() context.Context {
	if s.TxnState.State != NoTxn {
		return s.TxnState.Ctx
	}
	return s.context
}

// hijackCtx changes the current transaction's context to the provided one and
// returns a cleanup function to be used to restore the original context when
// the hijack is no longer needed.
// TODO(andrei): delete this when EXPLAIN(TRACE) goes away
func (s *Session) hijackCtx(ctx context.Context) func() {
	if s.TxnState.State != Open {
		// This hijacking is dubious to begin with. Let's at least assert it's being
		// done when the TxnState is in an expected state. In particular, if the
		// state would be NoTxn, then we'd need to hijack session.Ctx instead of the
		// txnState's context.
		log.Fatalf(ctx, "can only hijack while a SQL txn is Open. txnState: %+v",
			&s.TxnState)
	}
	return s.TxnState.hijackCtx(ctx)
}

func (s *Session) resetPlanner(p *planner, e *Executor, txn *client.Txn) {
	p.session = s
	// phaseTimes is an array, not a slice, so this performs a copy-by-value.
	p.phaseTimes = s.phaseTimes

	p.semaCtx = parser.MakeSemaContext(s.User == security.RootUser)
	p.semaCtx.Location = &s.Location
	p.semaCtx.SearchPath = s.SearchPath

	p.evalCtx = s.evalCtx()
	p.evalCtx.Planner = p
	if e != nil {
		p.evalCtx.NodeID = e.cfg.NodeID.Get()
		p.evalCtx.ReCache = e.reCache
	}

	p.setTxn(txn)
}

// FinishPlan releases the resources that were consumed by the currently active
// default planner. It does not check to see whether any other resources are
// still pointing to the planner, so it should only be called when a connection
// is entirely finished executing a statement.
func (s *Session) FinishPlan() {
	s.planner = emptyPlanner
}

// newPlanner creates a planner inside the scope of the given Session. The
// statement executed by the planner will be executed in txn. The planner
// should only be used to execute one statement.
func (s *Session) newPlanner(e *Executor, txn *client.Txn) *planner {
	p := &planner{}
	s.resetPlanner(p, e, txn)
	return p
}

// evalCtx creates a parser.EvalContext from the Session's current configuration.
func (s *Session) evalCtx() parser.EvalContext {
	return parser.EvalContext{
		Location:   &s.Location,
		Database:   s.Database,
		SearchPath: s.SearchPath,
		Ctx:        s.Ctx,
		Mon:        &s.TxnState.mon,
	}
}

// resetForBatch prepares the Session for executing a new batch of statements.
func (s *Session) resetForBatch(e *Executor) {
	// Update the database cache to a more recent copy, so that we can use tables
	// that we created in previous batches of the same transaction.
	s.leases.databaseCache = e.getDatabaseCache()
	s.TxnState.schemaChangers.curGroupNum++
}

// releaseLeases releases all leases currently held by the Session.
func (lc *LeaseCollection) releaseLeases(ctx context.Context) {
	if lc.leases != nil {
		if log.V(2) {
			log.VEventf(ctx, 2, "releasing %d leases", len(lc.leases))
		}
		for _, lease := range lc.leases {
			if err := lc.leaseMgr.Release(lease); err != nil {
				log.Warning(ctx, err)
			}
		}
		lc.leases = nil
	}
}

// setTestingVerifyMetadata sets a callback to be called after the Session
// is done executing the current SQL statement. It can be used to verify
// assumptions about how metadata will be asynchronously updated.
// Note that this can overwrite a previous callback that was waiting to be
// verified, which is not ideal.
func (s *Session) setTestingVerifyMetadata(fn func(config.SystemConfig) error) {
	s.testingVerifyMetadataFn = fn
	s.verifyFnCheckedOnce = false
}

// TxnStateEnum represents the state of a SQL txn.
type TxnStateEnum int

//go:generate stringer -type=TxnStateEnum
const (
	// No txn is in scope. Either there never was one, or it got committed/rolled
	// back. Note that this state will not be experienced outside of the Session
	// and Executor (i.e. it will not be observed by a running query) because the
	// Executor opens implicit transactions before executing non-transactional
	// queries.
	NoTxn TxnStateEnum = iota
	// A txn is in scope.
	Open
	// The txn has encountered a (non-retriable) error.
	// Statements will be rejected until a COMMIT/ROLLBACK is seen.
	Aborted
	// The txn has encountered a retriable error.
	// Statements will be rejected until a RESTART_TRANSACTION is seen.
	RestartWait
	// The KV txn has been committed successfully through a RELEASE.
	// Statements are rejected until a COMMIT is seen.
	CommitWait
)

// Some states mean that a client.Txn is open, others don't.
func (s TxnStateEnum) kvTxnIsOpen() bool {
	return s == Open || s == RestartWait
}

// txnState contains state associated with an ongoing SQL txn.
// There may or may not be an open KV txn associated with the SQL txn.
// For interactive transactions (open across batches of SQL commands sent by a
// user), txnState is intended to be stored as part of a user Session.
type txnState struct {
	txn   *client.Txn
	State TxnStateEnum

	// Ctx is the context for everything running in this SQL txn.
	Ctx context.Context

	// implicitTxn if set if the transaction was automatically created for a
	// single statement.
	implicitTxn bool

	// If set, the user declared the intention to retry the txn in case of retriable
	// errors. The txn will enter a RestartWait state in case of such errors.
	retryIntent bool

	// The transaction will be retried in case of retriable error. The retry will be
	// automatic (done by Txn.Exec()). This field behaves the same as retryIntent,
	// except it's reset in between client round trips.
	autoRetry bool

	// A COMMIT statement has been processed. Useful for allowing the txn to
	// survive retriable errors if it will be auto-retried (BEGIN; ... COMMIT; in
	// the same batch), but not if the error needs to be reported to the user.
	commitSeen bool

	// The schema change closures to run when this txn is done.
	schemaChangers schemaChangerCollection

	sp opentracing.Span

	// The timestamp to report for current_timestamp(), now() etc.
	// This must be constant for the lifetime of a SQL transaction.
	sqlTimestamp time.Time

	// mon tracks txn-bound objects like the running state of
	// planNode in the midst of performing a computation. We
	// host this here instead of TxnState because TxnState is
	// fully reset upon each call to resetForNewSQLTxn().
	mon mon.MemoryMonitor
}

// resetForNewSQLTxn (re)initializes the txnState for a new transaction.
// It creates a new client.Txn and initializes it using the session defaults.
// txnState.State will be set to Open.
func (ts *txnState) resetForNewSQLTxn(e *Executor, s *Session, implicitTxn bool) {
	if ts.sp != nil {
		panic(fmt.Sprintf("txnState.reset() called on ts with active span. How come "+
			"finishSQLTxn() wasn't called previously? ts: %+v", ts))
	}

	// Reset state vars to defaults.
	ts.retryIntent = false
	ts.autoRetry = false
	ts.commitSeen = false

	// Create a context for this transaction. It will include a
	// root span that will contain everything executed as part of the
	// upcoming SQL txn, including (automatic or user-directed) retries.
	// The span is closed by finishSQLTxn().
	// TODO(andrei): figure out how to close these spans on server shutdown? Ties
	// into a larger discussion about how to drain SQL and rollback open txns.
	ctx := s.context
	tracer := e.cfg.AmbientCtx.Tracer
	var sp opentracing.Span
	var opName string
	if implicitTxn {
		opName = sqlImplicitTxnName
	} else {
		opName = sqlTxnName
	}

	if parentSp := opentracing.SpanFromContext(ctx); parentSp != nil {
		// Create a child span for this SQL txn.
		sp = parentSp.Tracer().StartSpan(
			opName, opentracing.ChildOf(parentSp.Context()), tracing.Recordable)
	} else {
		// Create a root span for this SQL txn.
		sp = tracer.StartSpan(opName, tracing.Recordable)
	}

	// Start recording for the traceTxnThreshold and debugTrace7881Enabled
	// cases.
	//
	// TODO(andrei): we now only do this when !s.Tracing.TracingActive() because
	// when session tracing is active, that's going to do its own StartRecording()
	// and the two calls trample each other. We should figure out how to get
	// traceTxnThreshold and debugTrace7881Enabled to integrate more nicely with
	// session tracing.
	if !s.Tracing.TracingActive() && traceTxnThreshold.Get() > 0 && debugTrace7881Enabled {
		tracing.StartRecording(sp, tracing.SingleNodeRecording)
	}

	// Put the new span in the context.
	ctx = opentracing.ContextWithSpan(ctx, sp)

	if !tracing.IsRecordable(sp) {
		log.Fatalf(ctx, "non-recordable transaction span of type: %T", sp)
	}

	ts.sp = sp
	ts.Ctx = ctx
	ts.State = Open
	s.Tracing.onNewSQLTxn(ts.sp)

	ts.mon.Start(ctx, &s.mon, mon.BoundAccount{})

	ts.txn = client.NewTxn(e.cfg.DB)
	if err := ts.txn.SetIsolation(s.DefaultIsolationLevel); err != nil {
		panic(err)
	}

	// Discard the old schemaChangers, if any.
	ts.schemaChangers = schemaChangerCollection{}
}

// willBeRetried returns true if the SQL transaction is going to be retried
// because of err.
func (ts *txnState) willBeRetried() bool {
	return ts.autoRetry || ts.retryIntent
}

// resetStateAndTxn moves the txnState into a specified state, as a result of
// the client.Txn being done.
func (ts *txnState) resetStateAndTxn(state TxnStateEnum) {
	if state != NoTxn && state != Aborted && state != CommitWait {
		panic(fmt.Sprintf("resetStateAndTxn called with unsupported state: %v", state))
	}
	if ts.txn != nil && !ts.txn.IsFinalized() {
		panic(fmt.Sprintf(
			"attempting to move SQL txn to state %v inconsistent with KV txn state: %s "+
				"(finalized: false)", state, ts.txn.Proto().Status))
	}
	ts.State = state
	ts.txn = nil
}

// finishSQLTxn closes the root span for the current SQL txn.
// This needs to be called before resetForNewSQLTransaction() is called for
// starting another SQL txn.
// The session context is just used for logging the SQL trace.
func (ts *txnState) finishSQLTxn(sessionCtx context.Context, s *Session) {
	ts.mon.Stop(ts.Ctx)
	if ts.sp == nil {
		panic("No span in context? Was resetForNewSQLTxn() called previously?")
	}
	sampledFor7881 := (ts.sp.BaggageItem(keyFor7881Sample) != "")
	ts.sp.Finish()
	if err := s.Tracing.onFinishSQLTxn(ts.sp); err != nil {
		log.Errorf(sessionCtx, "error finishing trace: %s", err)
	}
	if r := tracing.GetRecording(ts.sp); r != nil {
		durThreshold := traceTxnThreshold.Get()
		if sampledFor7881 || (durThreshold > 0 && timeutil.Since(ts.sqlTimestamp) >= durThreshold) {
			dump := tracing.FormatRawSpans(r)
			if len(dump) > 0 {
				log.Infof(sessionCtx, "SQL trace:\n%s", dump)
			}
		}
	}
	ts.sp = nil
}

// updateStateAndCleanupOnErr updates txnState based on the type of error that we
// received. If it's a retriable error and we're going to retry the txn,
// then the state moves to RestartWait. Otherwise, the state moves to Aborted
// and the KV txn is cleaned up.
func (ts *txnState) updateStateAndCleanupOnErr(err error, e *Executor) {
	if err == nil {
		panic("updateStateAndCleanupOnErr called with no error")
	}
	if retErr, ok := err.(*roachpb.HandledRetryableTxnError); !ok ||
		!ts.willBeRetried() ||
		!ts.txn.IsRetryableErrMeantForTxn(*retErr) {

		// We can't or don't want to retry this txn, so the txn is over.
		e.TxnAbortCount.Inc(1)
		// This call rolls back a PENDING transaction and cleans up all its
		// intents.
		ts.txn.CleanupOnError(ts.Ctx, err)
		ts.resetStateAndTxn(Aborted)
	} else {
		// If we got a retriable error, move the SQL txn to the RestartWait state.
		// Note that TransactionAborted is also a retriable error, handled here;
		// in this case cleanup for the txn has been done for us under the hood.
		ts.State = RestartWait
	}
}

// hijackCtx changes the transaction's context to the provided one and returns a
// cleanup function to be used to restore the original context when the hijack
// is no longer needed.
// TODO(andrei): delete this when EXPLAIN(TRACE) goes away.
func (ts *txnState) hijackCtx(ctx context.Context) func() {
	origCtx := ts.Ctx
	ts.Ctx = ctx
	return func() {
		ts.Ctx = origCtx
	}
}

type schemaChangerCollection struct {
	// A schemaChangerCollection accumulates schemaChangers from potentially
	// multiple user requests, part of the same SQL transaction. We need to
	// remember what group and index within the group each schemaChanger came
	// from, so we can map failures back to the statement that produced them.
	curGroupNum int

	// The index of the current statement, relative to its group. For statements
	// that have been received from the client in the same batch, the
	// group consists of all statements in the same transaction.
	curStatementIdx int
	// schema change callbacks together with the index of the statement
	// that enqueued it (within its group of statements).
	schemaChangers []struct {
		epoch int
		idx   int
		sc    SchemaChanger
	}
}

func (scc *schemaChangerCollection) queueSchemaChanger(schemaChanger SchemaChanger) {
	scc.schemaChangers = append(
		scc.schemaChangers,
		struct {
			epoch int
			idx   int
			sc    SchemaChanger
		}{scc.curGroupNum, scc.curStatementIdx, schemaChanger})
}

// execSchemaChanges releases schema leases and runs the queued
// schema changers. This needs to be run after the transaction
// scheduling the schema change has finished.
//
// The list of closures is cleared after (attempting) execution.
//
// Args:
//  results: The results from all statements in the group that scheduled the
//    schema changes we're about to execute. Results corresponding to the
//    schema change statements will be changed in case an error occurs.
func (scc *schemaChangerCollection) execSchemaChanges(
	ctx context.Context, e *Executor, session *Session, results ResultList,
) {
	// Release the leases once a transaction is complete.
	session.leases.releaseLeases(ctx)
	if e.cfg.SchemaChangerTestingKnobs.SyncFilter != nil {
		e.cfg.SchemaChangerTestingKnobs.SyncFilter(TestingSchemaChangerCollection{scc})
	}
	// Execute any schema changes that were scheduled, in the order of the
	// statements that scheduled them.
	for _, scEntry := range scc.schemaChangers {
		sc := &scEntry.sc
		sc.db = *e.cfg.DB
		sc.testingKnobs = e.cfg.SchemaChangerTestingKnobs
		sc.distSQLPlanner = e.distSQLPlanner
		for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
			evalCtx := createSchemaChangeEvalCtx(e.cfg.Clock.Now())
			if err := sc.exec(ctx, evalCtx); err != nil {
				if shouldLogSchemaChangeError(err) {
					log.Warningf(ctx, "Error executing schema change: %s", err)
				}
				if err == sqlbase.ErrDescriptorNotFound {
				} else if sqlbase.IsPermanentSchemaChangeError(err) {
					// All constraint violations can be reported; we report it as the result
					// corresponding to the statement that enqueued this changer.
					// There's some sketchiness here: we assume there's a single result
					// per statement and we clobber the result/error of the corresponding
					// statement.
					// There's also another subtlety: we can only report results for
					// statements in the current batch; we can't modify the results of older
					// statements.
					if scEntry.epoch == scc.curGroupNum {
						results[scEntry.idx] = Result{Err: err}
					}
				} else {
					// retryable error.
					continue
				}
			}
			break
		}
	}
	scc.schemaChangers = scc.schemaChangers[:0]
}

// SessionTracing holds the state used by SET TRACE {ON,OFF,LOCAL} statements in
// the context of one SQL session.
// It holds the current trace being collected (or the last trace collected, if
// tracing is not currently ongoing).
//
// SessionTracing and its interactions with the Session are thread-safe; tracing
// can be turned on at any time.
type SessionTracing struct {
	session *Session
	// tracing is set at times when "session tracing" is active - i.e. when
	// transactions are being recorded.
	tracing bool
	// If tracing==true, recordingType indicates the type of type of the current
	// recording.
	recordingType tracing.RecordingType

	// txnRecording accumulates the recorded spans. Each []RawSpan represents the
	// trace of a SQL transaction. The first element corresponds to the
	// partially-recorded transaction in which SET TRACE ON was run (or its
	// implicit txn, if there wasn't a SQL transaction already running). The last
	// one will contain the partial-recording of the transaction in which SET
	// TRACE OFF has been run.
	txnRecordings [][]basictracer.RawSpan
}

// StartTracing starts "session tracing". After calling this, all SQL
// transactions running on this session will be traced. The current transaction,
// if any, will also be traced (except that children spans of the current txn
// span that have already been created will not be traced).
//
// StopTracing() needs to be called to finish this trace.
func (st *SessionTracing) StartTracing(recType tracing.RecordingType) error {
	if st.tracing {
		return errors.Errorf("already tracing")
	}
	sp := opentracing.SpanFromContext(st.session.Ctx())
	if sp == nil {
		return errors.Errorf("no span for SessionTracing")
	}

	// Reset the previous recording, if any.
	st.txnRecordings = nil

	tracing.StartRecording(sp, recType)
	st.tracing = true
	st.recordingType = recType
	return nil
}

// StopTracing stops the trace that was started with StartTracing().
//
// An error is returned if tracing was not active.
func (st *SessionTracing) StopTracing() error {
	if !st.tracing {
		return errors.Errorf("not tracing")
	}
	st.tracing = false
	// Stop recording the current transaction.
	sp := opentracing.SpanFromContext(st.session.Ctx())
	if sp == nil {
		return errors.Errorf("no span for SessionTracing")
	}
	spans := tracing.GetRecording(sp)
	tracing.StopRecording(sp)
	if spans == nil {
		return errors.Errorf("nil recording")
	}
	// Append the partially-recorded current transaction to the list of
	// transactions.
	st.txnRecordings = append(st.txnRecordings, spans)
	return nil
}

// onFinishSQLTxn is called when a SQL transaction is about to be finished (i.e.
// just before the span corresponding to the txn is Finish()ed). It saves that
// span's recording in the SessionTracing.
//
// sp is the transaction's span.
func (st *SessionTracing) onFinishSQLTxn(sp opentracing.Span) error {
	if !st.TracingActive() {
		return nil
	}

	if sp == nil {
		return errors.Errorf("no span for SessionTracing")
	}
	spans := tracing.GetRecording(sp)
	if spans == nil {
		return errors.Errorf("nil recording")
	}
	st.txnRecordings = append(st.txnRecordings, spans)
	// tracing.StopRecording() is not necessary. The span is about to be closed
	// anyway.
	return nil
}

// onNewSQLTxn is called when a new SQL txn is started (i.e. soon after the span
// corresponding to that transaction has been created). It starts recording on
// that new span. The recording will be retrieved when the transaction finishes
// (in onFinishSQLTxn).
//
// sp is the span corresponding to the new SQL transaction.
func (st *SessionTracing) onNewSQLTxn(sp opentracing.Span) {
	if !st.TracingActive() {
		return
	}
	if sp == nil {
		panic("no span for SessionTracing")
	}
	tracing.StartRecording(sp, st.recordingType)
}

// TracingActive checks whether session tracing is currently enabled.
func (st *SessionTracing) TracingActive() bool {
	return st.tracing
}

// GenerateSessionTraceVTable generates the rows of said table by using the log
// messages from the session's trace (i.e. the ongoing trace, if any, or the
// last one recorded). Note that, if there's an ongoing trace, the current
// transaction is not part of it yet.
func (st *SessionTracing) GenerateSessionTraceVTable() [][6]parser.Datum {
	var res [][6]parser.Datum

	spanIdx := 0
	// Iterate through the recording for each transaction.
	var allLogs []logRecordRow
	for _, spans := range st.txnRecordings {
		logs := make([]logRecordRow, 0)
		// Iterate through the spans grab and sort all the log messages. We rely on
		// transactions not overlapping in time, as we don't sort messages in
		// between transactions.
		for i := range spans {
			spanWithIndex := spanWithIndex{
				RawSpan:  &spans[i],
				index:    spanIdx,
				topLevel: i == 0,
			}
			spanIdx++
			for j, entry := range spans[i].Logs {
				lrr := logRecordRow{
					LogRecord: entry,
					span:      spanWithIndex,
					index:     j,
				}
				logs = append(logs, lrr)
			}
		}
		sort.Sort(byTimestamp(logs))
		allLogs = append(allLogs, logs...)
	}

	for _, lrr := range allLogs {
		// TODO(andrei): should we add a dummy log message at the beginning (and
		// also end?) of each span, to a) show empty spans and b) show the span's
		// start timestamp?

		// Extract the message of the event, which is either in an "event" or
		// "error" field.
		var msg string
		for _, f := range lrr.Fields {
			key := f.Key()
			if key == "event" {
				msg = fmt.Sprint(f.Value())
				break
			}
			if key == "error" {
				msg = fmt.Sprint("error:", f.Value())
				break
			}
		}
		// The "operation" column is only set for the first row in span.
		var operation parser.Datum
		if lrr.index == 0 {
			operation = parser.NewDString(lrr.span.Operation)
		} else {
			operation = parser.DNull
		}
		row := [6]parser.Datum{
			parser.MakeDBool(
				parser.DBool(lrr.span.topLevel && lrr.index == 0)), // top_level
			parser.NewDInt(parser.DInt(lrr.span.index)),           // span_idx
			parser.NewDInt(parser.DInt(lrr.index)),                // message_idx
			parser.MakeDTimestamp(lrr.Timestamp, time.Nanosecond), // timestamp
			operation,              // operation
			parser.NewDString(msg), // log
		}
		res = append(res, row)
	}
	return res
}

type logRecordRow struct {
	opentracing.LogRecord
	span  spanWithIndex
	index int
}

type spanWithIndex struct {
	*basictracer.RawSpan
	index    int
	topLevel bool
}

type byTimestamp []logRecordRow

func (a byTimestamp) Less(i, j int) bool {
	return a[i].Timestamp.Before(a[j].Timestamp)
}

func (a byTimestamp) Len() int {
	return len(a)
}

func (a byTimestamp) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
