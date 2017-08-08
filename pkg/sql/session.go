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

package sql

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"
	"unicode/utf8"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// debugTrace7881Enabled causes all SQL transactions to be traced, in the hope
// that we'll catch #7881 and dump the current trace for debugging.
var debugTrace7881Enabled = envutil.EnvOrDefaultBool("COCKROACH_TRACE_7881", false)

// span baggage key used for marking a span
const keyFor7881Sample = "found#7881"

// queryPhase represents a phase during a query's execution.
type queryPhase int

const (
	// The phase before start of execution (includes parsing, building a plan).
	preparing queryPhase = 0

	// Execution phase.
	executing = 1
)

// queryMeta stores metadata about a query. Stored as reference in
// session.mu.ActiveQueries and executor.cfg.queryRegistry.
type queryMeta struct {
	// The timestamp when this query began execution.
	start time.Time

	// AST of the SQL statement - converted to query string only when necessary.
	stmt parser.Statement

	// States whether this query is distributed. Note that all queries,
	// including those that are distributed, have this field set to false until
	// start of execution; only at that point can we can actually determine whether
	// this query will be distributed. Use the phase variable below
	// to determine whether this query has entered execution yet.
	isDistributed bool

	// Current phase of execution of query.
	phase queryPhase

	// Context associated with this query's transaction.
	ctx context.Context

	// Cancellation function for the context associated with this query's transaction.
	// Set to session.txnState.cancel in executor.
	ctxCancel context.CancelFunc

	// Flag that denotes if this query has been cancelled yet. Set and checked
	// using sync.atomic.{Load,Store}Int32.
	isCancelled int32

	// Reference to the Session that contains this query.
	session *Session
}

// cancel cancels the query associated with this queryMeta, by atomically
// setting the cancelled flag and closing the associated txn context.
func (q *queryMeta) cancel() {
	q.ctxCancel()

	atomic.StoreInt32(&q.isCancelled, 1)
}

// isCancelled atomically checks the cancellation flag, as well as the txn
// context (if checkTxn = true), for whether the query has been cancelled.
// Checking the context is a slower operation but must still be done occasionally.
// A CANCEL QUERY directed at this query will set the flag as well
// as close the context, but if the CANCEL is directed at another query within
// the same transaction (which could be running in parallel), then the shared
// transaction context will be closed but isCancelled won't be set to true.
func (q *queryMeta) isQueryCancelled(checkTxn bool) bool {
	if atomic.LoadInt32(&q.isCancelled) == 1 {
		return true
	}

	if !checkTxn {
		return false
	}

	select {
	case <-q.ctx.Done():
		return true
	default:
		return false
	}
}

// Session contains the state of a SQL client connection.
// Create instances using NewSession().
type Session struct {
	//
	// Session parameters, user-configurable.
	//

	// Database indicates the "current" database for the purpose of
	// resolving names. See searchAndQualifyDatabase() for details.
	Database string
	// DefaultIsolationLevel indicates the default isolation level of
	// newly created transactions.
	DefaultIsolationLevel enginepb.IsolationType
	// DistSQLMode indicates whether to run queries using the distributed
	// execution engine.
	DistSQLMode cluster.DistSQLExecMode
	// Location indicates the current time zone.
	Location *time.Location
	// SearchPath is a list of databases that will be searched for a table name
	// before the database. Currently, this is used only for SELECTs.
	// Names in the search path must have been normalized already.
	SearchPath parser.SearchPath
	// User is the name of the user logged into the session.
	User string

	//
	// Session parameters, non-user-configurable.
	//

	// defaults is used to restore default configuration values into
	// SET ... TO DEFAULT statements.
	defaults sessionDefaults

	// ClientAddr is the client's IP address and port.
	ClientAddr string

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
	mon        mon.BytesMonitor
	sessionMon mon.BytesMonitor
	// emergencyShutdown is set to true by EmergencyClose() to
	// indicate to Finish() that the session is already closed.
	emergencyShutdown bool

	Tracing SessionTracing

	tables TableCollection

	// If set, contains the in progress COPY FROM columns.
	copyFrom *copyNode

	// mu contains of all elements of the struct that can be changed
	// after initialization, and may be accessed from another thread.
	mu struct {
		syncutil.RWMutex

		//
		// Session parameters, user-configurable.
		//

		// ApplicationName is the name of the application running the
		// current session. This can be used for logging and per-application
		// statistics. Change via resetApplicationName().
		ApplicationName string

		//
		// State structures for the logical SQL session.
		//

		// ActiveQueries contains all queries in flight.
		ActiveQueries map[uint128.Uint128]*queryMeta
	}

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

// SessionRegistry stores a set of all sessions on this node.
// Use register() and deregister() to modify this registry.
type SessionRegistry struct {
	syncutil.Mutex
	store map[*Session]struct{}
}

// MakeSessionRegistry creates a new SessionRegistry with an empty set
// of sessions.
func MakeSessionRegistry() *SessionRegistry {
	return &SessionRegistry{store: make(map[*Session]struct{})}
}

func (r *SessionRegistry) register(s *Session) {
	r.Lock()
	r.store[s] = struct{}{}
	r.Unlock()
}

func (r *SessionRegistry) deregister(s *Session) {
	r.Lock()
	delete(r.store, s)
	r.Unlock()
}

// SerializeAll returns a slice of all sessions in the registry, converted to serverpb.Sessions.
func (r *SessionRegistry) SerializeAll() []serverpb.Session {
	r.Lock()
	defer r.Unlock()

	response := make([]serverpb.Session, 0, len(r.store))

	for s := range r.store {
		response = append(response, s.serialize())
	}

	return response
}

// NewSession creates and initializes a new Session object.
// remote can be nil.
func NewSession(
	ctx context.Context, args SessionArgs, e *Executor, remote net.Addr, memMetrics *MemoryMetrics,
) *Session {
	ctx = e.AnnotateCtx(ctx)
	distSQLMode := cluster.DistSQLExecMode(e.cfg.Settings.DistSQLClusterExecMode.Get())

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
		tables: TableCollection{
			leaseMgr:      e.cfg.LeaseManager,
			databaseCache: e.getDatabaseCache(),
		},
	}
	s.phaseTimes[sessionInit] = timeutil.Now()
	s.resetApplicationName(args.ApplicationName)
	s.PreparedStatements = makePreparedStatements(s)
	s.PreparedPortals = makePreparedPortals(s)
	s.Tracing.session = s
	s.mu.ActiveQueries = make(map[uint128.Uint128]*queryMeta)

	remoteStr := "<admin>"
	if remote != nil {
		remoteStr = remote.String()
	}
	s.ClientAddr = remoteStr

	if e.cfg.Settings.TraceSessionEventLogEnabled.Get() {
		s.eventLog = trace.NewEventLog(fmt.Sprintf("sql [%s]", args.User), remoteStr)
	}
	s.context, s.cancel = context.WithCancel(ctx)

	e.cfg.SessionRegistry.register(s)

	return s
}

// Finish releases resources held by the Session. It is called by the Session's
// main goroutine, so no synchronous queries will be in-flight during the
// method's execution. However, it could be called when asynchronous queries are
// operating in the background in the case of parallelized statements, which
// is why we make sure to drain background statements.
func (s *Session) Finish(e *Executor) {
	log.VEvent(s.context, 2, "finishing session")

	if s.emergencyShutdown {
		// closed by EmergencyClose() already.
		return
	}

	if s.mon == (mon.BytesMonitor{}) {
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
	if s.TxnState.State().kvTxnIsOpen() {
		s.TxnState.updateStateAndCleanupOnErr(
			errors.Errorf("session closing"), e)
	}
	if s.TxnState.State() != NoTxn {
		s.TxnState.finishSQLTxn(s)
	}

	// We might have unreleased tables if we're finishing the
	// session abruptly in the middle of a transaction, or, until #7648 is
	// addressed, there might be leases accumulated by preparing statements.
	s.tables.releaseTables(s.context)

	s.ClearStatementsAndPortals(s.context)
	s.sessionMon.Stop(s.context)
	s.mon.Stop(s.context)

	if s.eventLog != nil {
		s.eventLog.Finish()
		s.eventLog = nil
	}

	if s.Tracing.Enabled() {
		if err := s.Tracing.StopTracing(); err != nil {
			log.Infof(s.context, "error stopping tracing: %s", err)
		}
	}
	// Clear this session from the sessions registry.
	e.cfg.SessionRegistry.deregister(s)

	// This will stop the heartbeating of the of the txn record.
	// TODO(andrei): This shouldn't have any effect, since, if there was a
	// transaction, we just explicitly rolled it back above, so the heartbeat loop
	// in the TxnCoordSender should not be waiting on this channel any more.
	// Consider getting rid of this cancel field all-together.
	s.cancel()
}

// EmergencyClose is a simplified replacement for Finish() which is
// less picky about the current state of the Session. In particular
// this can be used to tidy up after a session even in the middle of a
// transaction, where there may still be memory activity registered to
// a monitor and not cleanly released.
func (s *Session) EmergencyClose() {
	// Ensure that all in-flight statements are done, so that monitor
	// traffic is stopped.
	_ = s.parallelizeQueue.Wait()

	// Release the leases - to ensure other sessions don't get stuck.
	s.tables.releaseTables(s.context)

	// The KV txn may be unusable - just leave it dead. Simply
	// shut down its memory monitor.
	s.TxnState.mon.EmergencyStop(s.context)
	// Shut the remaining monitors down.
	s.sessionMon.EmergencyStop(s.context)
	s.mon.EmergencyStop(s.context)

	// Finalize the event log.
	if s.eventLog != nil {
		s.eventLog.Finish()
		s.eventLog = nil
	}

	// Stop the heartbeating.
	s.cancel()

	// Mark the session as already closed, so that Finish() doesn't get confused.
	s.emergencyShutdown = true
}

// Ctx returns the current context for the session: if there is an active SQL
// transaction it returns the transaction context, otherwise it returns the
// session context.
// Note that in some cases we may want the session context even if there is an
// active transaction (an example is when we want to log an event to the session
// event log); in that case s.context should be used directly.
func (s *Session) Ctx() context.Context {
	if s.TxnState.State() != NoTxn {
		return s.TxnState.Ctx
	}
	return s.context
}

func (s *Session) resetPlanner(p *planner, e *Executor, txn *client.Txn) {
	p.session = s
	// phaseTimes is an array, not a slice, so this performs a copy-by-value.
	p.phaseTimes = s.phaseTimes
	p.stmt = nil
	p.cancelChecker = &nullCancelChecker{}

	p.semaCtx = parser.MakeSemaContext(s.User == security.RootUser)
	p.semaCtx.Location = &s.Location
	p.semaCtx.SearchPath = s.SearchPath

	p.evalCtx = s.evalCtx()
	p.evalCtx.Planner = p
	if e != nil {
		p.evalCtx.ClusterID = e.cfg.ClusterID()
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
	s.tables.databaseCache = e.getDatabaseCache()
	s.TxnState.schemaChangers.curGroupNum++
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

// addActiveQuery adds a running query to the session's internal store of active
// queries. Called from executor's execStmt and execStmtInParallel.
func (s *Session) addActiveQuery(queryID uint128.Uint128, queryMeta *queryMeta) {
	s.mu.Lock()
	s.mu.ActiveQueries[queryID] = queryMeta
	queryMeta.session = s
	s.mu.Unlock()
}

// removeActiveQuery removes a query from a session's internal store of active
// queries. Called when a query finishes execution.
func (s *Session) removeActiveQuery(queryID uint128.Uint128) {
	s.mu.Lock()
	delete(s.mu.ActiveQueries, queryID)
	s.mu.Unlock()
}

// setQueryExecutionMode is called upon start of execution of a query, and sets
// the query's metadata to indicate whether it's distributed or not.
func (s *Session) setQueryExecutionMode(queryID uint128.Uint128, isDistributed bool) {
	s.mu.Lock()
	queryMeta := s.mu.ActiveQueries[queryID]
	queryMeta.phase = executing
	queryMeta.isDistributed = isDistributed
	s.mu.Unlock()
}

// MaxSQLBytes is the maximum length in bytes of SQL statements serialized
// into a serverpb.Session. Exported for testing.
const MaxSQLBytes = 1000

// serialize serializes a Session into a serverpb.Session
// that can be served over RPC.
func (s *Session) serialize() serverpb.Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.TxnState.mu.RLock()
	defer s.TxnState.mu.RUnlock()

	var kvTxnID *uuid.UUID
	txn := s.TxnState.mu.txn
	if txn != nil {
		kvTxnID = txn.ID()
	}

	activeQueries := make([]serverpb.ActiveQuery, 0, len(s.mu.ActiveQueries))

	for id, query := range s.mu.ActiveQueries {
		sql := query.stmt.String()
		if len(sql) > MaxSQLBytes {
			sql = sql[:MaxSQLBytes-utf8.RuneLen('…')]
			// Ensure the resulting string is valid utf8.
			for {
				if r, _ := utf8.DecodeLastRuneInString(sql); r != utf8.RuneError {
					break
				}
				sql = sql[:len(sql)-1]
			}
			sql += "…"
		}
		activeQueries = append(activeQueries, serverpb.ActiveQuery{
			ID:            id.String(),
			Start:         query.start.UTC(),
			Sql:           sql,
			IsDistributed: query.isDistributed,
			Phase:         (serverpb.ActiveQuery_Phase)(query.phase),
		})
	}

	return serverpb.Session{
		Username:        s.User,
		ClientAddress:   s.ClientAddr,
		ApplicationName: s.mu.ApplicationName,
		Start:           s.phaseTimes[sessionInit].UTC(),
		ActiveQueries:   activeQueries,
		KvTxnID:         kvTxnID,
	}
}

// TxnStateEnum represents the state of a SQL txn.
type TxnStateEnum int64

//go:generate stringer -type=TxnStateEnum
const (
	// No txn is in scope. Either there never was one, or it got committed/rolled
	// back. Note that this state will not be experienced outside of the Session
	// and Executor (i.e. it will not be observed by a running query) because the
	// Executor opens implicit transactions before executing non-transactional
	// queries.
	NoTxn TxnStateEnum = iota

	// A txn is in scope, and we're currently executing statements from the first
	// batch of statements using the transaction. If BEGIN was the last (or the
	// only) statement in a batch, that batch doesn't count (the next batch will
	// be considered the first one). The first batch of statements can be
	// automatically retried in case of retryable errors since there's been no
	// client logic relying on reads performed in the transaction.
	//
	// A BEGIN statement makes the transaction enter this state (from a previous
	// NoTxn state). The end of the first batch of statements, if executed
	// successfully, will move the state to Open.
	//
	// TODO(andrei): It'd be cool if exiting this state would be based not on
	// batches sent by the client, but results being sent by the server to the
	// client (i.e. the client can send 100 batches but, if we haven't sent it any
	// results yet, we know that we can still retry them all).
	FirstBatch

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
	return s == Open || s == FirstBatch || s == RestartWait
}

// txnState contains state associated with an ongoing SQL txn.
// There may or may not be an open KV txn associated with the SQL txn.
// For interactive transactions (open across batches of SQL commands sent by a
// user), txnState is intended to be stored as part of a user Session.
type txnState struct {
	// state is read and written to atomically because it can be updated
	// concurrently with the execution of statements in the parallelizeQueue.
	// Access with State() / SetState().
	//
	// NOTE: Only state updates that are inconsequential to statement execution
	// are allowed concurrently with the execution of the parallizeQueue (e.g.
	// Open->FirstBatch).
	state TxnStateEnum

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
	Ctx context.Context

	// cancel is the cancellation function for the above context. Called upon
	// COMMIT/ROLLBACK of the transaction to release resources associated with
	// the context. nil when no txn is in progress.
	cancel context.CancelFunc

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

	// The transaction's isolation level.
	isolation enginepb.IsolationType

	// The transaction's priority.
	priority roachpb.UserPriority

	// mon tracks txn-bound objects like the running state of
	// planNode in the midst of performing a computation. We
	// host this here instead of TxnState because TxnState is
	// fully reset upon each call to resetForNewSQLTxn().
	mon mon.BytesMonitor
}

// State returns the current state of the session.
func (ts *txnState) State() TxnStateEnum {
	return TxnStateEnum(atomic.LoadInt64((*int64)(&ts.state)))
}

// SetState updates the state of the session.
func (ts *txnState) SetState(val TxnStateEnum) {
	atomic.StoreInt64((*int64)(&ts.state), int64(val))
}

// TxnIsOpen returns true if we are presently inside a SQL txn, and the txn is
// not in an error state.
func (ts *txnState) TxnIsOpen() bool {
	return ts.State() == Open || ts.State() == FirstBatch
}

// resetForNewSQLTxn (re)initializes the txnState for a new transaction.
// It creates a new client.Txn and initializes it using the session defaults.
// txnState.State will be set to Open.
//
// implicitTxn is set if the txn corresponds to an implicit SQL txn and controls
// the debug name of the txn.
// retryIntent is set if the client is prepared to handle the RestartWait and
// controls state transitions in case of error.
// sqlTimestamp is the timestamp to report for current_timestamp(), now() etc.
// isolation is the transaction's isolation level.
// priority is the transaction's priority.
func (ts *txnState) resetForNewSQLTxn(
	e *Executor,
	s *Session,
	implicitTxn bool,
	retryIntent bool,
	sqlTimestamp time.Time,
	isolation enginepb.IsolationType,
	priority roachpb.UserPriority,
) {
	if ts.sp != nil {
		panic(fmt.Sprintf("txnState.reset() called on ts with active span. How come "+
			"finishSQLTxn() wasn't called previously? ts: %+v", ts))
	}

	ts.retryIntent = retryIntent
	// Reset state vars to defaults.
	ts.autoRetry = true
	ts.commitSeen = false
	ts.sqlTimestamp = sqlTimestamp

	ts.implicitTxn = implicitTxn

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
	if !s.Tracing.Enabled() && (s.execCfg.Settings.TraceTxnThreshold.Get() > 0 || debugTrace7881Enabled) {
		mode := tracing.SingleNodeRecording
		if s.execCfg.Settings.TraceTxnThreshold.Get() > 0 {
			mode = tracing.SnowballRecording
		}
		tracing.StartRecording(sp, mode)
	}

	// Put the new span in the context.
	ctx = opentracing.ContextWithSpan(ctx, sp)

	if !tracing.IsRecordable(sp) {
		log.Fatalf(ctx, "non-recordable transaction span of type: %T", sp)
	}

	ts.sp = sp
	ts.Ctx, ts.cancel = context.WithCancel(ctx)
	ts.SetState(FirstBatch)
	s.Tracing.onNewSQLTxn(ts.sp)

	ts.mon.Start(ctx, &s.mon, mon.BoundAccount{})

	ts.mu.Lock()
	ts.mu.txn = client.NewTxn(e.cfg.DB)
	ts.mu.Unlock()
	if ts.implicitTxn {
		ts.mu.txn.SetDebugName(sqlImplicitTxnName)
	} else {
		ts.mu.txn.SetDebugName(sqlTxnName)
	}
	if err := ts.setIsolationLevel(isolation); err != nil {
		panic(err)
	}
	if err := ts.setPriority(priority); err != nil {
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
	if ts.mu.txn != nil && !ts.mu.txn.IsFinalized() {
		panic(fmt.Sprintf(
			"attempting to move SQL txn to state %v inconsistent with KV txn state: %s "+
				"(finalized: false)", state, ts.mu.txn.Proto().Status))
	}
	ts.SetState(state)
	ts.mu.Lock()
	ts.mu.txn = nil
	ts.mu.Unlock()
}

// finishSQLTxn closes the root span for the current SQL txn.  This needs to be
// called before resetForNewSQLTxn() is called for starting another SQL txn.
func (ts *txnState) finishSQLTxn(s *Session) {
	ts.mon.Stop(ts.Ctx)
	if ts.cancel != nil {
		ts.cancel()
		ts.cancel = nil
	}
	if ts.sp == nil {
		panic("No span in context? Was resetForNewSQLTxn() called previously?")
	}
	sampledFor7881 := (ts.sp.BaggageItem(keyFor7881Sample) != "")
	ts.sp.Finish()
	if err := s.Tracing.onFinishSQLTxn(ts.sp); err != nil {
		log.Errorf(s.context, "error finishing trace: %s", err)
	}
	// TODO(andrei): we should find a cheap way to get a trace's duration without
	// calling the expensive GetRecording().
	durThreshold := s.execCfg.Settings.TraceTxnThreshold.Get()
	if sampledFor7881 || durThreshold > 0 {
		if r := tracing.GetRecording(ts.sp); r != nil {
			if sampledFor7881 || (durThreshold > 0 && timeutil.Since(ts.sqlTimestamp) >= durThreshold) {
				dump := tracing.FormatRecordedSpans(r)
				if len(dump) > 0 {
					log.Infof(s.context, "SQL trace:\n%s", dump)
				}
			}
		} else {
			log.Warning(s.context, "Missing trace when sampled was enabled. "+
				"Was sql.trace.txn.enable_threshold just set recently?")
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
		!ts.mu.txn.IsRetryableErrMeantForTxn(*retErr) {

		// We can't or don't want to retry this txn, so the txn is over.
		e.TxnAbortCount.Inc(1)
		// This call rolls back a PENDING transaction and cleans up all its
		// intents.
		ts.mu.txn.CleanupOnError(ts.Ctx, err)
		ts.resetStateAndTxn(Aborted)
	} else {
		// If we got a retriable error, move the SQL txn to the RestartWait state.
		// Note that TransactionAborted is also a retriable error, handled here;
		// in this case cleanup for the txn has been done for us under the hood.
		ts.SetState(RestartWait)
	}
}

func (ts *txnState) setIsolationLevel(isolation enginepb.IsolationType) error {
	if err := ts.mu.txn.SetIsolation(isolation); err != nil {
		return err
	}
	ts.isolation = isolation
	return nil
}

func (ts *txnState) setPriority(userPriority roachpb.UserPriority) error {
	if err := ts.mu.txn.SetUserPriority(userPriority); err != nil {
		return err
	}
	ts.priority = userPriority
	return nil
}

// isSerializableRestart returns true if the KV transaction is serializable and
// its timestamp has been pushed. Used to detect whether the SQL txn will be
// allowed to commit.
//
// Note that this method allows for false negatives: sometimes the client only
// figures out that it's been pushed when it sends an EndTransaction - i.e. it's
// possible for the txn to have been pushed asynchoronously by some other
// operation (usually, but not exclusively, by a high-priority txn with
// conflicting writes).
func (ts *txnState) isSerializableRestart() bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	txn := ts.mu.txn
	if txn == nil {
		return false
	}
	return txn.IsSerializableRestart()
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
	session.tables.releaseTables(ctx)
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
			if err := sc.exec(ctx, true /* inSession */, evalCtx); err != nil {
				if shouldLogSchemaChangeError(err) {
					log.Warningf(ctx, "error executing schema change: %s", err)
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

// maybeRecover catches SQL panics and does some log reporting before
// propagating the panic further.
// TODO(knz): this is where we can place code to recover from
// recoverable panics.
func (s *Session) maybeRecover(action, stmt string) {
	if r := recover(); r != nil {
		err := errors.Errorf("panic while %s %q: %s", action, stmt, r)

		// A short warning header guaranteed to go to stderr.
		log.Shout(s.Ctx(), log.Severity_ERROR,
			"a SQL panic has occurred!")
		// TODO(knz): log panic details to logs once panics
		// are not propagated to the top-level and printed out by the Go runtime.

		// Close the session with force shutdown of the monitors. This is
		// guaranteed to succeed, or fail with a panic which we can't
		// recover from: if there's a panic, that means the lease /
		// tracing / kv subsystem is broken and we can't resume from that.
		s.EmergencyClose()

		// Propagate the panic further.
		panic(err)
	}
}

// SessionTracing holds the state used by SET TRACING {ON,OFF,LOCAL} statements in
// the context of one SQL session.
// It holds the current trace being collected (or the last trace collected, if
// tracing is not currently ongoing).
//
// SessionTracing and its interactions with the Session are thread-safe; tracing
// can be turned on at any time.
type SessionTracing struct {
	session *Session
	// enabled is set at times when "session enabled" is active - i.e. when
	// transactions are being recorded.
	enabled bool

	// kvTracingEnabled is set at times when KV tracing is active. When
	// KV tracning is enabled, the SQL/KV interface logs individual K/V
	// operators to the current context.
	kvTracingEnabled bool

	// If tracing==true, recordingType indicates the type of the current
	// recording.
	recordingType tracing.RecordingType

	// txnRecording accumulates the recorded spans. Each []RawSpan represents the
	// trace of a SQL transaction. The first element corresponds to the
	// partially-recorded transaction in which SET TRACING ON was run (or its
	// implicit txn, if there wasn't a SQL transaction already running). The last
	// one will contain the partial-recording of the transaction in which SET
	// TRACE OFF has been run.
	txnRecordings [][]tracing.RecordedSpan
}

// StartTracing starts "session tracing". After calling this, all SQL
// transactions running on this session will be traced. The current transaction,
// if any, will also be traced (except that children spans of the current txn
// span that have already been created will not be traced).
//
// StopTracing() needs to be called to finish this trace.
func (st *SessionTracing) StartTracing(recType tracing.RecordingType, traceKV bool) error {
	if st.enabled {
		return errors.Errorf("already tracing")
	}
	sp := opentracing.SpanFromContext(st.session.Ctx())
	if sp == nil {
		return errors.Errorf("no span for SessionTracing")
	}

	// Reset the previous recording, if any.
	st.txnRecordings = nil

	tracing.StartRecording(sp, recType)
	st.enabled = true
	st.kvTracingEnabled = traceKV
	st.recordingType = recType
	return nil
}

// StopTracing stops the trace that was started with StartTracing().
//
// An error is returned if tracing was not active.
func (st *SessionTracing) StopTracing() error {
	if !st.enabled {
		return errors.Errorf("not tracing")
	}
	st.enabled = false
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
	if !st.Enabled() {
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
	if !st.Enabled() {
		return
	}
	if sp == nil {
		panic("no span for SessionTracing")
	}
	tracing.StartRecording(sp, st.recordingType)
}

// RecordingType returns which type of tracing is currently being done.
func (st *SessionTracing) RecordingType() tracing.RecordingType {
	return st.recordingType
}

// KVTracingEnabled checks whether KV tracing is currently enabled.
func (st *SessionTracing) KVTracingEnabled() bool {
	return st.kvTracingEnabled
}

// Enabled checks whether session tracing is currently enabled.
func (st *SessionTracing) Enabled() bool {
	return st.enabled
}

// extractMsgFromRecord extracts the message of the event, which is either in an
// "event" or "error" field.
func extractMsgFromRecord(rec tracing.RecordedSpan_LogRecord) string {
	for _, f := range rec.Fields {
		key := f.Key
		if key == "event" {
			return f.Value
		}
		if key == "error" {
			return fmt.Sprint("error:", f.Value)
		}
	}
	return "<event missing in trace message>"
}

type traceRow [7]parser.Datum

// generateSessionTraceVTable generates the rows of said table by using the log
// messages from the session's trace (i.e. the ongoing trace, if any, or the
// last one recorded). Note that, if there's an ongoing trace, the current
// transaction is not part of it yet.
//
// All the log messages from the current recording are returned, in
// the order in which they should be presented in the crdb_internal.session_info
// virtual table. Messages from child spans are inserted as a block in between
// messages from the parent span. Messages from sibling spans are not
// interleaved.
//
// Here's a drawing showing the order in which messages from different spans
// will be interleaved. Each box is a span; inner-boxes are child spans. The
// numbers indicate the order in which the log messages will appear in the
// virtual table.
//
// +-----------------------+
// |           1           |
// | +-------------------+ |
// | |         2         | |
// | |  +----+           | |
// | |  |    | +----+    | |
// | |  | 3  | | 4  |    | |
// | |  |    | |    |  5 | |
// | |  |    | |    | ++ | |
// | |  |    | |    |    | |
// | |  +----+ |    |    | |
// | |         +----+    | |
// | |                   | |
// | |          6        | |
// | +-------------------+ |
// |            7          |
// +-----------------------+
func (st *SessionTracing) generateSessionTraceVTable() ([]traceRow, error) {
	// Get all the log messages, in the right order.
	var allLogs []logRecordRow
	for txnIdx, spans := range st.txnRecordings {
		seenSpans := make(map[uint64]struct{})

		// The spans are recorded in the order in which they are started, so the
		// first one will be a txn's root span.
		// In the loop below, we expect that, once we call getMessagesForSubtrace on
		// the first span in the transaction, all spans will be recursively marked
		// as seen. However, if that doesn't happen (e.g. we're missing a parent
		// span for some reason), the loop will handle the situation.
		for spanIdx, span := range spans {
			if _, ok := seenSpans[span.SpanID]; ok {
				continue
			}
			spanWithIndex := spanWithIndex{
				RecordedSpan: &spans[spanIdx],
				index:        spanIdx,
				txnIdx:       txnIdx,
			}
			msgs, err := getMessagesForSubtrace(spanWithIndex, spans, seenSpans)
			if err != nil {
				return nil, err
			}
			allLogs = append(allLogs, msgs...)
		}
	}

	// Transform the log messages into table rows.
	var res []traceRow
	for _, lrr := range allLogs {
		// The "operation" column is only set for the first row in span.
		var operation parser.Datum
		if lrr.index == 0 {
			operation = parser.NewDString(lrr.span.Operation)
		} else {
			operation = parser.DNull
		}
		var dur parser.Datum
		if lrr.index == 0 && lrr.span.Duration != 0 {
			dur = &parser.DInterval{
				Duration: duration.Duration{
					Nanos: lrr.span.Duration.Nanoseconds(),
				},
			}
		} else {
			// Span was not finished.
			dur = parser.DNull
		}

		row := traceRow{
			parser.NewDInt(parser.DInt(lrr.span.txnIdx)),            // txn_idx
			parser.NewDInt(parser.DInt(lrr.span.index)),             // span_idx
			parser.NewDInt(parser.DInt(lrr.index)),                  // message_idx
			parser.MakeDTimestampTZ(lrr.timestamp, time.Nanosecond), // timestamp
			dur,                        // duration
			operation,                  // operation
			parser.NewDString(lrr.msg), // message
		}
		res = append(res, row)
	}
	return res, nil
}

// getOrderedChildSpans returns all the spans in allSpans that are children of
// spanID. It assumes the input is ordered by start time, in which case the
// output is also ordered.
func getOrderedChildSpans(
	spanID uint64, txnIdx int, allSpans []tracing.RecordedSpan,
) []spanWithIndex {
	children := make([]spanWithIndex, 0)
	for i := range allSpans {
		if allSpans[i].ParentSpanID == spanID {
			children = append(
				children,
				spanWithIndex{
					RecordedSpan: &allSpans[i],
					index:        i,
					txnIdx:       txnIdx,
				})
		}
	}
	return children
}

// getMessagesForSubtrace takes a span and interleaves its log messages with
// those from its children (recursively). The order is the one defined in the
// comment on SessionTracing.GenerateSessionTraceVTable.
//
// seenSpans is modified to record all the spans that are part of the subtrace
// rooted at span.
func getMessagesForSubtrace(
	span spanWithIndex, allSpans []tracing.RecordedSpan, seenSpans map[uint64]struct{},
) ([]logRecordRow, error) {
	if _, ok := seenSpans[span.SpanID]; ok {
		return nil, errors.Errorf("duplicate span %d", span.SpanID)
	}
	var allLogs []logRecordRow
	const spanStartMsgTemplate = "=== SPAN START: %s ==="

	// Add a dummy log message marking the beginning of the span, to indicate
	// the start time and duration of span.
	allLogs = append(allLogs,
		logRecordRow{
			timestamp: span.StartTime,
			msg:       fmt.Sprintf(spanStartMsgTemplate, span.Operation),
			span:      span,
			index:     0,
		})

	seenSpans[span.SpanID] = struct{}{}
	childSpans := getOrderedChildSpans(span.SpanID, span.txnIdx, allSpans)
	var i, j int
	// Sentinel value - year 6000.
	maxTime := time.Date(6000, 0, 0, 0, 0, 0, 0, time.UTC)
	// Merge the logs with the child spans.
	for i < len(span.Logs) || j < len(childSpans) {
		logTime := maxTime
		childTime := maxTime
		if i < len(span.Logs) {
			logTime = span.Logs[i].Time
		}
		if j < len(childSpans) {
			childTime = childSpans[j].StartTime
		}

		if logTime.Before(childTime) {
			allLogs = append(allLogs,
				logRecordRow{
					timestamp: logTime,
					msg:       extractMsgFromRecord(span.Logs[i]),
					span:      span,
					// Add 1 to the index to account for the first dummy message in a span.
					index: i + 1,
				})
			i++
		} else {
			// Recursively append messages from the trace rooted at the child.
			childMsgs, err := getMessagesForSubtrace(childSpans[j], allSpans, seenSpans)
			if err != nil {
				return nil, err
			}
			allLogs = append(allLogs, childMsgs...)
			j++
		}
	}
	return allLogs, nil
}

// logRecordRow is used to temporarily hold on to log messages and their
// metadata while flattening a trace.
type logRecordRow struct {
	timestamp time.Time
	msg       string
	span      spanWithIndex
	// index of the log message within its span.
	index int
}

type spanWithIndex struct {
	*tracing.RecordedSpan
	index int
	// txnIdx is the 0-based index of the transaction in which this span was
	// recorded.
	txnIdx int
}
