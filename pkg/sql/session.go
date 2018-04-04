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
	"context"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// debugTrace7881Enabled causes all SQL transactions to be traced, in the hope
// that we'll catch #7881 and dump the current trace for debugging.
var debugTrace7881Enabled = envutil.EnvOrDefaultBool("COCKROACH_TRACE_7881", false)

// span baggage key used for marking a span
const keyFor7881Sample = "found#7881"

// traceTxnThreshold can be used to log SQL transactions that take
// longer than duration to complete. For example, traceTxnThreshold=1s
// will log the trace for any transaction that takes 1s or longer. To
// log traces for all transactions use traceTxnThreshold=1ns. Note
// that any positive duration will enable tracing and will slow down
// all execution because traces are gathered for all transactions even
// if they are not output.
var traceTxnThreshold = settings.RegisterDurationSetting(
	"sql.trace.txn.enable_threshold",
	"duration beyond which all transactions are traced (set to 0 to disable)", 0,
)

// traceSessionEventLogEnabled can be used to enable the event log
// that is normally kept for every SQL connection. The event log has a
// non-trivial performance impact and also reveals SQL statements
// which may be a privacy concern.
var traceSessionEventLogEnabled = settings.RegisterBoolSetting(
	"sql.trace.session_eventlog.enabled",
	"set to true to enable session tracing", false,
)

// DistSQLClusterExecMode controls the cluster default for when DistSQL is used.
var DistSQLClusterExecMode = settings.RegisterEnumSetting(
	"sql.defaults.distsql",
	"Default distributed SQL execution mode",
	"Auto",
	map[int64]string{
		int64(sessiondata.DistSQLOff):  "Off",
		int64(sessiondata.DistSQLAuto): "Auto",
		int64(sessiondata.DistSQLOn):   "On",
	},
)

// queryPhase represents a phase during a query's execution.
type queryPhase int

const (
	// The phase before start of execution (includes parsing, building a plan).
	preparing queryPhase = 0

	// Execution phase.
	executing queryPhase = 1
)

// queryMeta stores metadata about a query. Stored as reference in
// session.mu.ActiveQueries.
type queryMeta struct {
	// The timestamp when this query began execution.
	start time.Time

	// AST of the SQL statement - converted to query string only when necessary.
	stmt tree.Statement

	// States whether this query is distributed. Note that all queries,
	// including those that are distributed, have this field set to false until
	// start of execution; only at that point can we can actually determine whether
	// this query will be distributed. Use the phase variable below
	// to determine whether this query has entered execution yet.
	isDistributed bool

	// Current phase of execution of query.
	phase queryPhase

	// Cancellation function for the context associated with this query's transaction.
	ctxCancel context.CancelFunc

	// If set, this query will not be reported as part of SHOW QUERIES. This is
	// set based on the statement implementing tree.HiddenFromShowQueries.
	hidden bool
}

// cancel cancels the query associated with this queryMeta, by closing the associated
// txn context.
func (q *queryMeta) cancel() {
	q.ctxCancel()
}

// Session contains the state of a SQL client connection.
// Create instances using NewSession().
type Session struct {
	// data contains user-configurable session-scoped variables. This is the
	// authoritative copy of these variables; a planner's evalCtx gets a copy.
	data        sessiondata.SessionData
	dataMutator sessionDataMutator

	//
	// Session parameters, non-user-configurable.
	//

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
	// conn is the pgwire connection driving this Session. Used for the Copy-in
	// subprotocol.
	conn pgwirebase.Conn
	// distSQLPlanner is in charge of distSQL physical planning and running
	// logic.
	distSQLPlanner *DistSQLPlanner
	// context is the Session's base context, to be used for all
	// SQL-related logging. See Ctx().
	context context.Context
	// eventLog for SQL statements and results.
	eventLog trace.EventLog
	// cancel is a method to call when the session terminates, to
	// release resources associated with the context above.
	// TODO(andrei): We need to either get rid of this cancel field, or
	// it needs to move to the TxnState and become a per-txn
	// cancel. Right now, we're canceling all the txns that have ever
	// run on this session when the session is closed, as opposed to
	// canceling the individual transactions as soon as they
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

	// ResultsWriter is where query results are written to. It's set to a
	// pgwire.v3conn for sessions opened for SQL client connections and a
	// bufferedResultWriter for internal uses.
	ResultsWriter ResultsWriter

	tables TableCollection

	// ActiveSyncQueries contains query IDs of all synchronous (i.e. non-parallel)
	// queries in flight. All ActiveSyncQueries must also be in mu.ActiveQueries.
	ActiveSyncQueries []ClusterWideID

	// mu contains of all elements of the struct that can be changed
	// after initialization, and may be accessed from another thread.
	mu struct {
		syncutil.RWMutex

		//
		// Session parameters, user-configurable.
		//

		//
		// State structures for the logical SQL session.
		//

		// ActiveQueries contains all queries in flight.
		ActiveQueries map[ClusterWideID]*queryMeta

		// LastActiveQuery contains a reference to the AST of the last
		// query that ran on this session.
		LastActiveQuery tree.Statement
	}

	//
	// Per-session statistics.
	//

	// memMetrics track memory usage by SQL execution.
	memMetrics *MemoryMetrics
	// sqlStats tracks per-application statistics for all
	// applications on each node.
	sqlStats *sqlStats
	// appStats track per-application SQL usage statistics. This is a pointer into
	// sqlStats set as the session's current app.
	appStats *appStats
	// phaseTimes tracks session-level phase times. It is copied-by-value
	// to each planner in session.newPlanner.
	phaseTimes phaseTimes

	// noCopy is placed here to guarantee that Session objects are not
	// copied.
	noCopy util.NoCopy

	sessionID ClusterWideID
}

// sessionDefaults mirrors fields in Session, for restoring default
// configuration values in SET ... TO DEFAULT (or RESET ...) statements.
type sessionDefaults struct {
	applicationName string
	database        string
}

// SessionArgs contains arguments for creating a new Session with NewSession().
type SessionArgs struct {
	Database        string
	User            string
	ApplicationName string
	RemoteAddr      net.Addr
}

// SessionRegistry stores a set of all sessions on this node.
// Use register() and deregister() to modify this registry.
type SessionRegistry struct {
	syncutil.Mutex
	store map[ClusterWideID]registrySession
}

// MakeSessionRegistry creates a new SessionRegistry with an empty set
// of sessions.
func MakeSessionRegistry() *SessionRegistry {
	return &SessionRegistry{store: make(map[ClusterWideID]registrySession)}
}

func (r *SessionRegistry) register(id ClusterWideID, s registrySession) {
	r.Lock()
	r.store[id] = s
	r.Unlock()
}

func (r *SessionRegistry) deregister(id ClusterWideID) {
	r.Lock()
	delete(r.store, id)
	r.Unlock()
}

type registrySession interface {
	user() string
	cancelQuery(queryID ClusterWideID) bool
	cancelSession()
	// serialize serializes a Session into a serverpb.Session
	// that can be served over RPC.
	serialize() serverpb.Session
}

// CancelQuery looks up the associated query in the session registry and cancels it.
func (r *SessionRegistry) CancelQuery(queryIDStr string, username string) (bool, error) {
	queryID, err := StringToClusterWideID(queryIDStr)
	if err != nil {
		return false, fmt.Errorf("query ID %s malformed: %s", queryID, err)
	}

	r.Lock()
	defer r.Unlock()

	for _, session := range r.store {
		if !(username == security.RootUser || username == session.user()) {
			// Skip this session.
			continue
		}

		if session.cancelQuery(queryID) {
			return true, nil
		}
	}

	return false, fmt.Errorf("query ID %s not found", queryID)
}

// CancelSession looks up the specified session in the session registry and cancels it.
func (r *SessionRegistry) CancelSession(sessionIDBytes []byte, username string) (bool, error) {
	sessionID := BytesToClusterWideID(sessionIDBytes)

	r.Lock()
	defer r.Unlock()

	for id, session := range r.store {
		if !(username == security.RootUser || username == session.user()) {
			// Skip this session.
			continue
		}

		if id == sessionID {
			session.cancelSession()
			return true, nil
		}
	}

	return false, fmt.Errorf("session ID %s not found", sessionID)
}

// SerializeAll returns a slice of all sessions in the registry, converted to serverpb.Sessions.
func (r *SessionRegistry) SerializeAll() []serverpb.Session {
	r.Lock()
	defer r.Unlock()

	response := make([]serverpb.Session, 0, len(r.store))

	for _, s := range r.store {
		response = append(response, s.serialize())
	}

	return response
}

// NewSession creates and initializes a new Session object.
// remote can be nil.
//
// Args:
// conn: The pgwire connection driving this Session. Used for the Copy-in
//   subprotocol. If that's not going to be used on the Session, it can be nil.
func NewSession(
	ctx context.Context,
	args SessionArgs,
	e *Executor,
	memMetrics *MemoryMetrics,
	conn pgwirebase.Conn,
) *Session {
	ctx = e.AnnotateCtx(ctx)
	distSQLMode := sessiondata.DistSQLExecMode(DistSQLClusterExecMode.Get(&e.cfg.Settings.SV))

	s := &Session{
		data: sessiondata.SessionData{
			Database:      args.Database,
			DistSQLMode:   distSQLMode,
			SearchPath:    sqlbase.DefaultSearchPath,
			Location:      time.UTC,
			User:          args.User,
			SequenceState: sessiondata.NewSequenceState(),
		},
		execCfg:          &e.cfg,
		distSQLPlanner:   e.distSQLPlanner,
		parallelizeQueue: MakeParallelizeQueue(NewSpanBasedDependencyAnalyzer()),
		memMetrics:       memMetrics,
		sqlStats:         &e.sqlStats,
		tables: TableCollection{
			leaseMgr:          e.cfg.LeaseManager,
			databaseCache:     e.dbCache.getDatabaseCache(),
			dbCacheSubscriber: e.dbCache,
		},
		conn:    conn,
		planner: planner{execCfg: &e.cfg},
	}
	s.dataMutator = sessionDataMutator{
		data: &s.data,
		defaults: sessionDefaults{
			applicationName: args.ApplicationName,
			database:        args.Database,
		},
		settings:       e.cfg.Settings,
		curTxnReadOnly: &s.TxnState.readOnly,
		applicationNameChanged: func(newName string) {
			if s.sqlStats != nil {
				s.appStats = s.sqlStats.getStatsForApplication(newName)
			}
		},
	}
	s.phaseTimes[sessionInit] = timeutil.Now()
	s.dataMutator.SetApplicationName(args.ApplicationName)
	s.PreparedStatements = makePreparedStatements(s)
	s.PreparedPortals = makePreparedPortals(s)
	s.mu.ActiveQueries = make(map[ClusterWideID]*queryMeta)
	s.ActiveSyncQueries = make([]ClusterWideID, 0)

	remoteStr := "<admin>"
	if args.RemoteAddr != nil {
		remoteStr = args.RemoteAddr.String()
	}
	s.ClientAddr = remoteStr

	if traceSessionEventLogEnabled.Get(&e.cfg.Settings.SV) {
		s.eventLog = trace.NewEventLog(fmt.Sprintf("sql [%s]", args.User), remoteStr)
	}
	s.context, s.cancel = contextutil.WithCancel(ctx)

	s.sessionID = e.generateID()
	e.cfg.SessionRegistry.register(s.sessionID, s)

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
	// TODO(nvanbenschoten): Once we have better support for canceling ongoing
	// statement execution by the infrastructure added to support CancelRequest,
	// we should try to actively drain this queue instead of passively waiting
	// for it to drain. (andrei, 2017/09) - We now have support for statement
	// cancellation. Now what?
	_ = s.synchronizeParallelStmts(s.context)

	// If we're inside a txn, roll it back.
	if s.TxnState.State().kvTxnIsOpen() {
		_ = s.TxnState.updateStateAndCleanupOnErr(fmt.Errorf("session closing"), e)
	}
	if s.TxnState.State() != NoTxn {
		s.TxnState.finishSQLTxn(s)
	}

	// We might have unreleased tables if we're finishing the
	// session abruptly in the middle of a transaction, or, until #7648 is
	// addressed, there might be leases accumulated by preparing statements.
	if err := s.tables.releaseTables(s.context, dontBlockForDBCacheUpdate); err != nil {
		log.Warningf(s.context, "error releasing tables: %s", err)
	}

	s.ClearStatementsAndPortals(s.context)
	s.sessionMon.Stop(s.context)
	s.mon.Stop(s.context)

	if s.eventLog != nil {
		s.eventLog.Finish()
		s.eventLog = nil
	}

	if s.dataMutator.sessionTracing.Enabled() {
		if err := s.dataMutator.StopSessionTracing(); err != nil {
			log.Infof(s.context, "error stopping tracing: %s", err)
		}
	}
	// Clear this session from the sessions registry.
	e.cfg.SessionRegistry.deregister(s.sessionID)

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
	_ = s.synchronizeParallelStmts(s.context)

	// Release the leases - to ensure other sessions don't get stuck.
	if err := s.tables.releaseTables(s.context, dontBlockForDBCacheUpdate); err != nil {
		log.Warningf(s.context, "error releasing tables: %s", err)
	}

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

func (s *Session) resetPlanner(
	p *planner,
	txn *client.Txn,
	txnTimestamp time.Time,
	stmtTimestamp time.Time,
	reCache *tree.RegexpCache,
	statsCollector sqlStatsCollector,
) {
	p.statsCollector = statsCollector
	p.txn = txn
	p.stmt = nil
	p.cancelChecker = sqlbase.NewCancelChecker(s.Ctx())

	p.semaCtx = tree.MakeSemaContext(s.data.User == security.RootUser)
	p.semaCtx.Location = &s.data.Location
	p.semaCtx.SearchPath = s.data.SearchPath

	p.extendedEvalCtx = s.extendedEvalCtx(txn, txnTimestamp, stmtTimestamp)
	p.extendedEvalCtx.Planner = p
	p.extendedEvalCtx.Sequence = p
	p.extendedEvalCtx.ClusterID = s.execCfg.ClusterID()
	p.extendedEvalCtx.NodeID = s.execCfg.NodeID.Get()
	p.extendedEvalCtx.ReCache = reCache

	p.sessionDataMutator = &s.dataMutator
	p.preparedStatements = &s.PreparedStatements
	p.autoCommit = false
}

// FinishPlan releases the resources that were consumed by the currently active
// default planner. It does not check to see whether any other resources are
// still pointing to the planner, so it should only be called when a connection
// is entirely finished executing a statement and all results have been sent.
func (s *Session) FinishPlan() {
	if len(s.ActiveSyncQueries) > 0 {
		s.mu.Lock()
		// Store the last sync query as the last active query.
		lastQueryID := s.ActiveSyncQueries[len(s.ActiveSyncQueries)-1]
		s.mu.LastActiveQuery = s.mu.ActiveQueries[lastQueryID].stmt
		// All results have been sent to the client; so deregister all synchronous
		// active queries from this session. Cannot deregister asynchronous ones
		// because those might still be executing in the parallelizeQueue.
		for _, queryID := range s.ActiveSyncQueries {
			delete(s.mu.ActiveQueries, queryID)
		}
		s.mu.Unlock()
		s.ActiveSyncQueries = make([]ClusterWideID, 0)
	}

	s.planner = planner{execCfg: s.execCfg}
}

// newPlanner creates a planner inside the scope of the given Session. The
// statement executed by the planner will be executed in txn. The planner
// should only be used to execute one statement. If txn is nil, none of the
// timestamp fields of the eval ctx will be set (this is in addition to the
// various other fields that aren't set in either case). But presumably if that
// is the case, the caller already doesn't care about SQL semantics too much.
func (s *Session) newPlanner(
	txn *client.Txn,
	txnTimestamp time.Time,
	stmtTimestamp time.Time,
	reCache *tree.RegexpCache,
	statsCollector sqlStatsCollector,
) *planner {
	p := &planner{execCfg: s.execCfg}
	s.resetPlanner(p, txn, txnTimestamp, stmtTimestamp, reCache, statsCollector)
	return p
}

// extendedEvalCtx creates an evaluation context from the Session's current
// configuration.
func (s *Session) extendedEvalCtx(
	txn *client.Txn, txnTimestamp time.Time, stmtTimestamp time.Time,
) extendedEvalContext {
	var evalContextTestingKnobs tree.EvalContextTestingKnobs
	var st *cluster.Settings
	var statusServer serverpb.StatusServer
	if s.execCfg != nil {
		evalContextTestingKnobs = s.execCfg.EvalContextTestingKnobs
		// TODO(tschottdorf): it looks like this should always be provided.
		// Perhaps `*Settings` should live somewhere else.
		st = s.execCfg.Settings
		statusServer = s.execCfg.StatusServer
	}

	scInterface := newSchemaInterface(&s.tables, s.execCfg.VirtualSchemas)

	return extendedEvalContext{
		EvalContext: tree.EvalContext{
			Txn:             txn,
			SessionData:     &s.data,
			ApplicationName: s.dataMutator.ApplicationName(),
			TxnState:        getTransactionState(&s.TxnState),
			TxnReadOnly:     s.TxnState.readOnly,
			TxnImplicit:     s.TxnState.implicitTxn,
			Settings:        st,
			CtxProvider:     s,
			Mon:             &s.TxnState.mon,
			TestingKnobs:    evalContextTestingKnobs,
			StmtTimestamp:   stmtTimestamp,
			TxnTimestamp:    txnTimestamp,
		},
		SessionMutator:  &s.dataMutator,
		VirtualSchemas:  s.execCfg.VirtualSchemas,
		Tracing:         &s.dataMutator.sessionTracing,
		StatusServer:    statusServer,
		MemMetrics:      s.memMetrics,
		Tables:          &s.tables,
		ExecCfg:         s.execCfg,
		DistSQLPlanner:  s.distSQLPlanner,
		TxnModesSetter:  &s.TxnState,
		SchemaChangers:  &s.TxnState.schemaChangers,
		schemaAccessors: scInterface,
	}
}

func newSchemaInterface(tables *TableCollection, vt VirtualTabler) *schemaInterface {
	sc := &schemaInterface{
		physical: &CachedPhysicalAccessor{
			SchemaAccessor: UncachedPhysicalAccessor{},
			tc:             tables,
		},
	}
	sc.logical = &LogicalSchemaAccessor{
		SchemaAccessor: sc.physical,
		vt:             vt,
	}
	return sc
}

// resetForBatch prepares the Session for executing a new batch of statements.
func (s *Session) resetForBatch(e *Executor) {
	// Update the database cache to a more recent copy, so that we can use tables
	// that we created in previous batches of the same transaction.
	s.tables.databaseCache = e.dbCache.getDatabaseCache()
}

// addActiveQuery adds a running query to the session's internal store of active
// queries, as well as to the executor's query registry. Called from executor
// before start of execution.
func (s *Session) addActiveQuery(queryID ClusterWideID, queryMeta *queryMeta) {
	s.mu.Lock()
	s.mu.ActiveQueries[queryID] = queryMeta
	s.mu.Unlock()
	// addActiveQuery is called from the main goroutine of the session;
	// and at this stage, this query is a synchronous query for our purposes.
	// setQueryExecutionMode will remove this element if this query enters the
	// parallelizeQueue.
	s.ActiveSyncQueries = append(s.ActiveSyncQueries, queryID)
}

// removeActiveQuery removes a query from a session's internal store of active
// queries, as well as from the executor's query registry.
// Called when a query finishes execution.
func (s *Session) removeActiveQuery(queryID ClusterWideID) {
	s.mu.Lock()
	queryMeta, ok := s.mu.ActiveQueries[queryID]
	if ok {
		delete(s.mu.ActiveQueries, queryID)
		s.mu.LastActiveQuery = queryMeta.stmt
	}
	s.mu.Unlock()
}

// setQueryExecutionMode is called upon start of execution of a query, and sets
// the query's metadata to indicate whether it's distributed or not.
func (s *Session) setQueryExecutionMode(
	queryID ClusterWideID, isDistributed bool, isParallel bool,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queryMeta, ok := s.mu.ActiveQueries[queryID]
	if !ok {
		// Could be a statement that implements HiddenFromShowQueries.
		// These statements have a query ID but do not have an entry
		// in session.mu.ActiveQueries.
		return
	}
	queryMeta.phase = executing
	queryMeta.isDistributed = isDistributed

	if isParallel {
		// We default to putting queries in ActiveSyncQueries. Since
		// this query is not synchronous anymore, remove it from
		// ActiveSyncQueries. We expect the last element in
		// ActiveSyncQueries to be this query; because all execution
		// up to this call of setQueryExecutionMode is synchronous.
		lenSyncQueries := len(s.ActiveSyncQueries)
		s.ActiveSyncQueries = s.ActiveSyncQueries[:lenSyncQueries-1]
	}
}

// synchronizeParallelStmts waits for all statements in the parallelizeQueue to
// finish. If errors are seen in the parallel batch, we attempt to turn these
// errors into a single error we can send to the client. We do this by prioritizing
// non-retryable errors over retryable errors.
func (s *Session) synchronizeParallelStmts(ctx context.Context) error {
	if errs := s.parallelizeQueue.Wait(); len(errs) > 0 {
		s.TxnState.mu.Lock()
		defer s.TxnState.mu.Unlock()

		// Sort the errors according to their importance.
		curTxn := s.TxnState.mu.txn.Proto()
		sort.Slice(errs, func(i, j int) bool {
			errPriority := func(err error) int {
				switch t := err.(type) {
				case *roachpb.HandledRetryableTxnError:
					errTxn := t.Transaction
					if errTxn.ID == curTxn.ID && errTxn.Epoch == curTxn.Epoch {
						// A retryable error for the current transaction
						// incarnation is given the highest priority.
						return 1
					}
					return 2
				case *roachpb.TxnPrevAttemptError:
					// Symptom of concurrent retry.
					return 3
				default:
					// Any other error. We sort these behind retryable errors
					// and errors we know to be their symptoms because it is
					// impossible to conclusively determine in all cases whether
					// one of these errors is a symptom of a concurrent retry or
					// not. If the error is a symptom then we want to ignore it.
					// If it is not, we expect to see the same error during a
					// transaction retry.
					return 4
				}
			}
			return errPriority(errs[i]) < errPriority(errs[j])
		})

		// Return the "best" error.
		bestErr := errs[0]
		switch bestErr.(type) {
		case *roachpb.HandledRetryableTxnError:
			// If any of the errors are retryable, we need to bump the transaction
			// epoch to invalidate any writes performed by any workers after the
			// retry updated the txn's proto but before we synchronized (some of
			// these writes might have been performed at the wrong epoch). Note
			// that we don't need to lock the client.Txn because we're synchronized.
			// See #17197.
			s.TxnState.mu.txn.Proto().BumpEpoch()
		case *roachpb.TxnPrevAttemptError:
			log.Fatalf(ctx, "found symptoms of a concurrent retry, but did "+
				"not find the final retry error: %v", errs)
		}
		return bestErr
	}
	return nil
}

// MaxSQLBytes is the maximum length in bytes of SQL statements serialized
// into a serverpb.Session. Exported for testing.
const MaxSQLBytes = 1000

// serialize is part of the registrySession interface.
func (s *Session) serialize() serverpb.Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.TxnState.mu.RLock()
	defer s.TxnState.mu.RUnlock()

	var kvTxnID *uuid.UUID
	txn := s.TxnState.mu.txn
	if txn != nil {
		id := txn.ID()
		kvTxnID = &id
	}

	activeQueries := make([]serverpb.ActiveQuery, 0, len(s.mu.ActiveQueries))
	truncateSQL := func(sql string) string {
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
		return sql
	}

	for id, query := range s.mu.ActiveQueries {
		sql := truncateSQL(query.stmt.String())
		activeQueries = append(activeQueries, serverpb.ActiveQuery{
			ID:            id.String(),
			Start:         query.start.UTC(),
			Sql:           sql,
			IsDistributed: query.isDistributed,
			Phase:         (serverpb.ActiveQuery_Phase)(query.phase),
		})
	}
	lastActiveQuery := ""
	if s.mu.LastActiveQuery != nil {
		lastActiveQuery = truncateSQL(s.mu.LastActiveQuery.String())
	}

	return serverpb.Session{
		Username:        s.data.User,
		ClientAddress:   s.ClientAddr,
		ApplicationName: s.data.ApplicationName(),
		Start:           s.phaseTimes[sessionInit].UTC(),
		ActiveQueries:   activeQueries,
		KvTxnID:         kvTxnID,
		LastActiveQuery: lastActiveQuery,
		ID:              s.sessionID.GetBytes(),
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

	// Like Open, a txn is in scope. The difference is that, while in the
	// AutoRetry state, a retriable error will be handled by an automatic
	// transaction retry, whereas we can't do that in Open. There's a caveat -
	// even if we're in AutoRetry, we can't do automatic retries if any
	// results for statements in the current transaction have already been
	// delivered to the client.
	// In principle, we can do automatic retries for the first batch of statements
	// in a transaction. There is an extension to the rule, though: for
	// example, is we get a batch with "BEGIN; SET TRANSACTION ISOLATION LEVEL
	// foo; SAVEPOINT cockroach_restart;" followed by a 2nd batch, we can
	// automatically retry the 2nd batch even though the statements in the first
	// batch will not be executed again and their results have already been sent
	// to the clients. We can do this because some statements are special in that
	// their execution always generates exactly the same results to the consumer
	// (i.e. the SQL client).
	//
	// TODO(andrei): This state shouldn't exist; the decision about whether we can
	// retry automatically or not should be entirely dynamic, based on which
	// results we've delivered to the client already. It should have nothing to do
	// with the client's batching of statements. For example, the client can send
	// 100 batches but, if we haven't sent it any results yet, we should still be
	// able to retry them all). Currently the splitting into batches is relevant
	// because we don't keep track of statements from previous batches, so we
	// would not be capable of retrying them even if we knew that no results have
	// been delivered.
	AutoRetry

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
	return s == Open || s == AutoRetry || s == RestartWait
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
	// Open->AutoRetry).
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

	// If we're in a SQL txn, txnResults is the ResultsGroup that statements in
	// this transaction should write results to.
	txnResults ResultsGroup

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

	// The transaction's read only state.
	readOnly bool

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
	return ts.State() == Open || ts.State() == AutoRetry
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
	readOnly bool,
) {
	if ts.sp != nil || ts.txnResults != nil {
		log.Fatalf(s.Ctx(),
			"txnState.reset() called on ts with active span or active txnResults. "+
				"How come finishSQLTxn() wasn't called previously? ts: %+v", ts)
	}

	ts.retryIntent = retryIntent
	// Reset state vars to defaults.
	ts.commitSeen = false
	ts.sqlTimestamp = sqlTimestamp
	ts.implicitTxn = implicitTxn
	ts.txnResults = s.ResultsWriter.NewResultsGroup()

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
	st := s.execCfg.Settings
	if !s.dataMutator.sessionTracing.Enabled() && (traceTxnThreshold.Get(&st.SV) > 0 || debugTrace7881Enabled) {
		mode := tracing.SingleNodeRecording
		if traceTxnThreshold.Get(&st.SV) > 0 {
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
	ts.Ctx, ts.cancel = contextutil.WithCancel(ctx)
	ts.SetState(AutoRetry)

	ts.mon.Start(ctx, &s.mon, mon.BoundAccount{})

	ts.mu.Lock()
	ts.mu.txn = client.NewTxn(e.cfg.DB, e.cfg.NodeID.Get(), client.RootTxn)
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
	ts.setReadOnly(readOnly)

	// Discard the old schemaChangers, if any.
	ts.schemaChangers = schemaChangerCollection{}
}

// willBeRetried returns true if the SQL transaction is going to be retried
// because of err.
func (ts *txnState) willBeRetried() bool {
	return ts.State() == AutoRetry || ts.retryIntent
}

// resetStateAndTxn moves the txnState into a specified state, as a result of
// the client.Txn being done.
func (ts *txnState) resetStateAndTxn(state TxnStateEnum) {
	if state != NoTxn && state != Aborted && state != CommitWait {
		panic(fmt.Sprintf("resetStateAndTxn called with unsupported state: %v", state))
	}
	if ts.mu.txn != nil && !ts.mu.txn.IsFinalized() {
		panic(fmt.Sprintf(
			"attempting to move SQL txn to state %s inconsistent with KV txn state: %s "+
				"(finalized: false)", state, ts.mu.txn.Proto().Status))
	}
	ts.SetState(state)
	ts.mu.Lock()
	ts.mu.txn = nil
	ts.mu.Unlock()
}

// finishSQLTxn finalizes a transaction's results and closes the root span for
// the current SQL txn. This needs to be called before resetForNewSQLTxn() is
// called for starting another SQL txn.
func (ts *txnState) finishSQLTxn(s *Session) {
	ts.mon.Stop(ts.Ctx)
	if ts.cancel != nil {
		ts.cancel()
		ts.cancel = nil
	}
	if ts.sp == nil {
		panic("No span in context? Was resetForNewSQLTxn() called previously?")
	}

	// Finalize the transaction's results.
	ts.txnResults.Close()
	ts.txnResults = nil

	sampledFor7881 := (ts.sp.BaggageItem(keyFor7881Sample) != "")
	ts.sp.Finish()
	// TODO(andrei): we should find a cheap way to get a trace's duration without
	// calling the expensive GetRecording().
	durThreshold := traceTxnThreshold.Get(&s.execCfg.Settings.SV)
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
// received. If it's a retriable error and it looks like we're going to retry
// the txn (we're either in the AutoRetry state, meaning that we can do
// auto-retries, or the client is doing client-directed retries), then the state
// moves to RestartWait. Otherwise, the state moves to Aborted and the KV txn is
// cleaned up.
// Note that even if we move to RestartWait here, this doesn't automatically
// mean that we're going to auto-retry. It might be the case, for example, that
// we've already streamed results to the client and so we can't auto-retry for
// that reason. It is the responsibility of higher layers to catch this and
// terminate the transaction, if appropriate.
//
// This method always returns an error. Usually it's the input err, except that
// a retriable error meant for another txn is replaced with a non-retriable
// error because higher layers are not supposed to consider it retriable.
func (ts *txnState) updateStateAndCleanupOnErr(err error, e *Executor) error {
	if err == nil {
		panic("updateStateAndCleanupOnErr called with no error")
	}
	if ts.mu.txn == nil {
		panic(fmt.Sprintf(
			"updateStateAndCleanupOnErr called in state with no KV txn. State: %s",
			ts.State()))
	}
	if retErr, ok := err.(*roachpb.HandledRetryableTxnError); !ok ||
		!ts.willBeRetried() ||
		!ts.mu.txn.IsRetryableErrMeantForTxn(*retErr) ||
		// If we ran a COMMIT, then we can only do auto-retries, not client-directed
		// retries.
		(ts.commitSeen && ts.State() != AutoRetry) {

		// We can't or don't want to retry this txn, so the txn is over.
		e.TxnAbortCount.Inc(1)

		// If we got a retriable error but it was meant for another txn, we'll
		// return a non-retriable error instead further down. We need to identify
		// this here, before the call to ts.resetStateAndTxn().
		var retriableErrForAnotherTxn bool
		txnID := ts.mu.txn.Proto().ID
		if ok && !ts.mu.txn.IsRetryableErrMeantForTxn(*retErr) {
			retriableErrForAnotherTxn = true
		}

		// This call rolls back a PENDING transaction and cleans up all its
		// intents.
		ts.mu.txn.CleanupOnError(ts.Ctx, err)
		ts.resetStateAndTxn(Aborted)

		if retriableErrForAnotherTxn {
			return errors.Wrapf(
				retErr,
				"retryable error from another txn. Current txn ID: %v", txnID)
		}
	} else {
		// If we got a retriable error, move the SQL txn to the RestartWait state.
		// Note that TransactionAborted is also a retriable error, handled here;
		// in this case cleanup for the txn has been done for us under the hood.
		ts.SetState(RestartWait)
	}
	return err
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

func (ts *txnState) setReadOnly(readOnly bool) {
	ts.readOnly = readOnly
}

func (ts *txnState) setTransactionModes(modes tree.TransactionModes) error {
	if err := ts.setSQLIsolationLevel(modes.Isolation); err != nil {
		return err
	}
	if err := ts.setUserPriority(modes.UserPriority); err != nil {
		return err
	}
	return ts.setReadWriteMode(modes.ReadWriteMode)
}

func (ts *txnState) setSQLIsolationLevel(level tree.IsolationLevel) error {
	var iso enginepb.IsolationType
	switch level {
	case tree.UnspecifiedIsolation:
		return nil
	case tree.SnapshotIsolation:
		iso = enginepb.SNAPSHOT
	case tree.SerializableIsolation:
		iso = enginepb.SERIALIZABLE
	default:
		return errors.Errorf("unknown isolation level: %s", level)
	}

	return ts.setIsolationLevel(iso)
}

func (ts *txnState) setUserPriority(userPriority tree.UserPriority) error {
	var up roachpb.UserPriority
	switch userPriority {
	case tree.UnspecifiedUserPriority:
		return nil
	case tree.Low:
		up = roachpb.MinUserPriority
	case tree.Normal:
		up = roachpb.NormalUserPriority
	case tree.High:
		up = roachpb.MaxUserPriority
	default:
		return errors.Errorf("unknown user priority: %s", userPriority)
	}
	return ts.setPriority(up)
}

func (ts *txnState) setReadWriteMode(readWriteMode tree.ReadWriteMode) error {
	switch readWriteMode {
	case tree.UnspecifiedReadWriteMode:
		return nil
	case tree.ReadOnly:
		ts.setReadOnly(true)
	case tree.ReadWrite:
		ts.setReadOnly(false)
	default:
		return errors.Errorf("unknown read mode: %s", readWriteMode)
	}
	return nil
}

type schemaChangerCollection struct {
	schemaChangers []SchemaChanger
}

func (scc *schemaChangerCollection) queueSchemaChanger(schemaChanger SchemaChanger) {
	scc.schemaChangers = append(scc.schemaChangers, schemaChanger)
}

func (scc *schemaChangerCollection) reset() {
	scc.schemaChangers = nil
}

// execSchemaChanges releases schema leases and runs the queued
// schema changers. This needs to be run after the transaction
// scheduling the schema change has finished.
//
// The list of closures is cleared after (attempting) execution.
func (scc *schemaChangerCollection) execSchemaChanges(
	ctx context.Context, cfg *ExecutorConfig,
) error {
	if cfg.SchemaChangerTestingKnobs.SyncFilter != nil {
		cfg.SchemaChangerTestingKnobs.SyncFilter(TestingSchemaChangerCollection{scc})
	}
	// Execute any schema changes that were scheduled, in the order of the
	// statements that scheduled them.
	var firstError error
	for _, sc := range scc.schemaChangers {
		sc.db = cfg.DB
		sc.testingKnobs = cfg.SchemaChangerTestingKnobs
		sc.distSQLPlanner = cfg.DistSQLPlanner
		for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
			evalCtx := createSchemaChangeEvalCtx(cfg.Clock.Now())
			if err := sc.exec(ctx, true /* inSession */, &evalCtx); err != nil {
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
					if firstError == nil {
						firstError = err
					}
				} else {
					// retryable error.
					continue
				}
			}
			break
		}
	}
	scc.schemaChangers = nil
	return firstError
}

const panicLogOutputCutoffChars = 500

func anonymizeStmtAndConstants(stmt tree.Statement) string {
	return tree.AsStringWithFlags(stmt, tree.FmtAnonymize|tree.FmtHideConstants)
}

// AnonymizeStatementsForReporting transforms an action, SQL statements, and a value
// (usually a recovered panic) into an error that will be useful when passed to
// our error reporting as it exposes a scrubbed version of the statements.
func AnonymizeStatementsForReporting(action, sqlStmts string, r interface{}) error {
	var anonymized []string
	{
		stmts, err := parser.Parse(sqlStmts)
		if err == nil {
			for _, stmt := range stmts {
				anonymized = append(anonymized, anonymizeStmtAndConstants(stmt))
			}
		}
	}
	anonStmtsStr := strings.Join(anonymized, "; ")
	if len(anonStmtsStr) > panicLogOutputCutoffChars {
		anonStmtsStr = anonStmtsStr[:panicLogOutputCutoffChars] + " [...]"
	}

	return log.Safe(
		fmt.Sprintf("panic while %s %d statements: %s", action, len(anonymized), anonStmtsStr),
	).WithCause(r)
}

// maybeRecover catches SQL panics and does some log reporting before
// propagating the panic further.
//
// TODO(knz): this is where we can place code to recover from
// recoverable panics.
func (s *Session) maybeRecover(action, stmts string) {
	if r := recover(); r != nil {
		// A warning header guaranteed to go to stderr. This is unanonymized.
		cutStmts := stmts
		if len(cutStmts) > panicLogOutputCutoffChars {
			cutStmts = stmts[:panicLogOutputCutoffChars] + " [...]"
		}

		log.Shout(s.Ctx(), log.Severity_ERROR,
			fmt.Sprintf("a SQL panic has occurred while %s %q: %s",
				action, cutStmts, r))
		// TODO(knz): log panic details to logs once panics
		// are not propagated to the top-level and printed out by the Go runtime.

		// Close the session with force shutdown of the monitors. This is
		// guaranteed to succeed, or fail with a panic which we can't
		// recover from: if there's a panic, that means the lease /
		// tracing / kv subsystem is broken and we can't resume from that.
		s.EmergencyClose()

		safeErr := AnonymizeStatementsForReporting(action, stmts, r)

		// Propagate the (sanitized) panic further.
		panic(safeErr)
	}
}

// cancelQuery is part of the registrySession interface.
func (s *Session) cancelQuery(queryID ClusterWideID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if queryMeta, exists := s.mu.ActiveQueries[queryID]; exists {
		queryMeta.cancel()
		return true
	}
	return false
}

// cancelSession is part of the registrySession interface.
func (s *Session) cancelSession() {
	s.cancel()
}

// user is part of the registrySession interface.
func (s *Session) user() string {
	return s.data.User
}

// SessionTracing holds the state used by SET TRACING {ON,OFF,LOCAL} statements in
// the context of one SQL session.
// It holds the current trace being collected (or the last trace collected, if
// tracing is not currently ongoing).
//
// SessionTracing and its interactions with the connExecutor are thread-safe;
// tracing can be turned on at any time.
type SessionTracing struct {
	// enabled is set at times when "session enabled" is active - i.e. when
	// transactions are being recorded.
	enabled bool

	// kvTracingEnabled is set at times when KV tracing is active. When
	// KV tracning is enabled, the SQL/KV interface logs individual K/V
	// operators to the current context.
	kvTracingEnabled bool

	// If recording==true, recordingType indicates the type of the current
	// recording.
	recordingType tracing.RecordingType

	// ex is the connExecutor to which this SessionTracing is tied.
	ex *connExecutor

	// firstTxnSpan is the span of the first txn that was active when session
	// tracing was enabled.
	firstTxnSpan opentracing.Span

	// connSpan is the connection's span. This is recording.
	connSpan opentracing.Span

	// lastRecording will collect the recording when stopping tracing.
	lastRecording []traceRow
}

// getRecording returns the session trace. If we're not currently tracing, this
// will be the last recorded trace. If we are currently tracing, we'll return
// whatever was recorded so far.
func (st *SessionTracing) getRecording() ([]traceRow, error) {
	if !st.enabled {
		return st.lastRecording, nil
	}

	var spans []tracing.RecordedSpan
	if st.firstTxnSpan != nil {
		spans = append(spans, tracing.GetRecording(st.firstTxnSpan)...)
	}
	spans = append(spans, tracing.GetRecording(st.connSpan)...)

	return generateSessionTraceVTable(spans)
}

// StartTracing starts "session tracing". From this moment on, everything
// happening on both the connection's context and the current txn's context (if
// any) will be traced.
// StopTracing() needs to be called to finish this trace.
//
// There's two contexts on which we must record:
// 1) If we're inside a txn, we start recording on the txn's span. We assume
// that the txn's ctx has a recordable span on it.
// 2) Regardless of whether we're in a txn or not, we need to record the
// connection's context. This context generally does not have a span, so we
// "hijack" it with one that does. Whatever happens on that context, plus
// whatever happens in future derived txn contexts, will be recorded.
//
// Args:
// kvTracingEnabled: If set, the traces will also include "KV trace" messages -
//   verbose messages around the interaction of SQL with KV. Some of the messages
//   are per-row.
func (st *SessionTracing) StartTracing(recType tracing.RecordingType, kvTracingEnabled bool) error {
	if st.enabled {
		return errors.Errorf("already tracing")
	}

	// If we're inside a transaction, start recording on the txn span.
	if _, ok := st.ex.machine.CurState().(stateNoTxn); !ok {
		sp := opentracing.SpanFromContext(st.ex.state.Ctx)
		if sp == nil {
			return errors.Errorf("no txn span for SessionTracing")
		}
		tracing.StartRecording(sp, recType)
		st.firstTxnSpan = sp
	}

	st.enabled = true
	st.kvTracingEnabled = kvTracingEnabled
	st.recordingType = recType

	// Now hijack the conn's ctx with one that has a recording span.

	opName := "session recording"
	var sp opentracing.Span
	if parentSp := opentracing.SpanFromContext(st.ex.ctxHolder.connCtx); parentSp != nil {
		// Create a child span while recording.
		sp = parentSp.Tracer().StartSpan(
			opName, opentracing.ChildOf(parentSp.Context()), tracing.Recordable)
	} else {
		// Create a root span while recording.
		sp = st.ex.server.cfg.AmbientCtx.Tracer.StartSpan(opName, tracing.Recordable)
	}
	tracing.StartRecording(sp, recType)
	st.connSpan = sp

	// Hijack the connections context.
	newConnCtx := opentracing.ContextWithSpan(st.ex.ctxHolder.connCtx, sp)
	st.ex.ctxHolder.hijack(newConnCtx)

	return nil
}

// StopTracing stops the trace that was started with StartTracing().
// An error is returned if tracing was not active.
func (st *SessionTracing) StopTracing() error {
	if !st.enabled {
		return errors.Errorf("not tracing")
	}
	st.enabled = false

	var spans []tracing.RecordedSpan

	if st.firstTxnSpan != nil {
		spans = append(spans, tracing.GetRecording(st.firstTxnSpan)...)
		tracing.StopRecording(st.firstTxnSpan)
	}
	st.connSpan.Finish()
	spans = append(spans, tracing.GetRecording(st.connSpan)...)
	// NOTE: We're stopping recording on the connection's ctx only; the stopping
	// is not inherited by children. If we are inside of a txn, that span will
	// continue recording, even though nobody will collect its recording again.
	tracing.StopRecording(st.connSpan)
	st.ex.ctxHolder.unhijack()

	var err error
	st.lastRecording, err = generateSessionTraceVTable(spans)
	return err
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

// traceRow is the type of a single row in the session_trace vtable.
// The columns are as follows:
// - span_idx
// - message_idx
// - timestamp
// - duration
// - operation
// - location
// - tag
// - message
type traceRow [8]tree.Datum

// A regular expression to split log messages.
// It has three parts:
// - the (optional) code location, with at least one forward slash and a period
//   in the file name:
//   ((?:[^][ :]+/[^][ :]+\.[^][ :]+:[0-9]+)?)
// - the (optional) tag: ((?:\[(?:[^][]|\[[^]]*\])*\])?)
// - the message itself: the rest.
var logMessageRE = regexp.MustCompile(
	`(?s:^((?:[^][ :]+/[^][ :]+\.[^][ :]+:[0-9]+)?) *((?:\[(?:[^][]|\[[^]]*\])*\])?) *(.*))`)

// generateSessionTraceVTable generates the rows of said table by using the log
// messages from the session's trace (i.e. the ongoing trace, if any, or the
// last one recorded).
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
//
// Note that what's described above is not the order in which SHOW TRACE FOR ...
// displays the information.
func generateSessionTraceVTable(spans []tracing.RecordedSpan) ([]traceRow, error) {
	// Get all the log messages, in the right order.
	var allLogs []logRecordRow

	// NOTE: The spans are recorded in the order in which they are started.
	seenSpans := make(map[uint64]struct{})
	for spanIdx, span := range spans {
		if _, ok := seenSpans[span.SpanID]; ok {
			continue
		}
		spanWithIndex := spanWithIndex{
			RecordedSpan: &spans[spanIdx],
			index:        spanIdx,
		}
		msgs, err := getMessagesForSubtrace(spanWithIndex, spans, seenSpans)
		if err != nil {
			return nil, err
		}
		allLogs = append(allLogs, msgs...)
	}

	// Transform the log messages into table rows.
	var res []traceRow
	for _, lrr := range allLogs {
		// The "operation" column is only set for the first row in span.
		var operation tree.Datum
		if lrr.index == 0 {
			operation = tree.NewDString(lrr.span.Operation)
		} else {
			operation = tree.DNull
		}
		var dur tree.Datum
		if lrr.index == 0 && lrr.span.Duration != 0 {
			dur = &tree.DInterval{
				Duration: duration.Duration{
					Nanos: lrr.span.Duration.Nanoseconds(),
				},
			}
		} else {
			// Span was not finished.
			dur = tree.DNull
		}

		// Split the message into component parts.
		//
		// The result of FindStringSubmatchIndex is a 1D array of pairs
		// [start, end) of positions in the input string.  The first pair
		// identifies the entire match; the 2nd pair corresponds to the
		// 1st parenthetized expression in the regexp, and so on.
		loc := logMessageRE.FindStringSubmatchIndex(lrr.msg)
		if loc == nil {
			return nil, fmt.Errorf("unable to split trace message: %q", lrr.msg)
		}

		row := traceRow{
			tree.NewDInt(tree.DInt(lrr.span.index)),               // span_idx
			tree.NewDInt(tree.DInt(lrr.index)),                    // message_idx
			tree.MakeDTimestampTZ(lrr.timestamp, time.Nanosecond), // timestamp
			dur,       // duration
			operation, // operation
			tree.NewDString(lrr.msg[loc[2]:loc[3]]), // location
			tree.NewDString(lrr.msg[loc[4]:loc[5]]), // tag
			tree.NewDString(lrr.msg[loc[6]:loc[7]]), // message
		}
		res = append(res, row)
	}
	return res, nil
}

// getOrderedChildSpans returns all the spans in allSpans that are children of
// spanID. It assumes the input is ordered by start time, in which case the
// output is also ordered.
func getOrderedChildSpans(spanID uint64, allSpans []tracing.RecordedSpan) []spanWithIndex {
	children := make([]spanWithIndex, 0)
	for i := range allSpans {
		if allSpans[i].ParentSpanID == spanID {
			children = append(
				children,
				spanWithIndex{
					RecordedSpan: &allSpans[i],
					index:        i,
				})
		}
	}
	return children
}

// getMessagesForSubtrace takes a span and interleaves its log messages with
// those from its children (recursively). The order is the one defined in the
// comment on generateSessionTraceVTable().
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
	childSpans := getOrderedChildSpans(span.SpanID, allSpans)
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
}

// sessionDataMutator is the interface used by sessionVars to change the session
// state. It mostly mutates the Session's SessionData, but not exclusively (e.g.
// see curTxnReadOnly).
type sessionDataMutator struct {
	data     *sessiondata.SessionData
	defaults sessionDefaults
	settings *cluster.Settings
	// curTxnReadOnly is a value to be mutated through SET transaction_read_only = ...
	curTxnReadOnly *bool
	sessionTracing SessionTracing
	// applicationNamedChanged, if set, is called when the "application name"
	// variable is updated.
	applicationNameChanged func(newName string)
}

// SetApplicationName sets the application name.
func (m *sessionDataMutator) SetApplicationName(appName string) {
	m.data.SetApplicationName(appName)
	if m.applicationNameChanged != nil {
		m.applicationNameChanged(appName)
	}
}

// ApplicationName returns the session's "application_name" variable. This is
// not a setter method, but the method is here nevertheless because
// ApplicationName is not part of SessionData because accessing it needs
// locking.
func (m *sessionDataMutator) ApplicationName() string {
	return m.data.ApplicationName()
}

func (m *sessionDataMutator) SetDatabase(dbName string) {
	m.data.Database = dbName
}

func (m *sessionDataMutator) SetDefaultIsolationLevel(iso enginepb.IsolationType) {
	m.data.DefaultIsolationLevel = iso
}

func (m *sessionDataMutator) SetDefaultReadOnly(val bool) {
	m.data.DefaultReadOnly = val
}

func (m *sessionDataMutator) SetDistSQLMode(val sessiondata.DistSQLExecMode) {
	m.data.DistSQLMode = val
}

func (m *sessionDataMutator) SetLookupJoinEnabled(val bool) {
	m.data.LookupJoinEnabled = val
}

func (m *sessionDataMutator) SetOptimizerMode(val sessiondata.OptimizerMode) {
	m.data.OptimizerMode = val
}

func (m *sessionDataMutator) SetSafeUpdates(val bool) {
	m.data.SafeUpdates = val
}

func (m *sessionDataMutator) SetSearchPath(val sessiondata.SearchPath) {
	m.data.SearchPath = val
}

func (m *sessionDataMutator) SetLocation(loc *time.Location) {
	m.data.Location = loc
}

func (m *sessionDataMutator) SetReadOnly(val bool) {
	*m.curTxnReadOnly = val
}

func (m *sessionDataMutator) SetStmtTimeout(timeout time.Duration) {
	m.data.StmtTimeout = timeout
}

func (m *sessionDataMutator) StopSessionTracing() error {
	return m.sessionTracing.StopTracing()
}

func (m *sessionDataMutator) StartSessionTracing(
	recType tracing.RecordingType, kvTracingEnabled bool,
) error {
	return m.sessionTracing.StartTracing(recType, kvTracingEnabled)
}

// RecordLatestSequenceValue records that value to which the session incremented
// a sequence.
func (m *sessionDataMutator) RecordLatestSequenceVal(seqID uint32, val int64) {
	m.data.SequenceState.RecordValue(seqID, val)
}

// Location exports the location session variable.
func (s *Session) Location() *time.Location {
	return s.data.Location
}

// statsCollector returns an sqlStatsCollector that will record stats in the
// session's stats containers.
func (s *Session) statsCollector() sqlStatsCollector {
	return newSQLStatsCollectorImpl(s.sqlStats, s.appStats, s.phaseTimes)
}

type sqlStatsCollectorImpl struct {
	// sqlStats tracks per-application statistics for all
	// applications on each node.
	sqlStats *sqlStats
	// appStats track per-application SQL usage statistics. This is a pointer into
	// sqlStats set as the session's current app.
	appStats *appStats
	// phaseTimes tracks session-level phase times. It is copied-by-value
	// to each planner in session.newPlanner.
	phaseTimes phaseTimes
}

// sqlStatsCollectorImpl implements the sqlStatsCollector interface.
var _ sqlStatsCollector = &sqlStatsCollectorImpl{}

// newSQLStatsCollectorImpl creates an instance of sqlStatsCollectorImpl.
//
// note that phaseTimes is an array, not a slice, so this performs a copy-by-value.
func newSQLStatsCollectorImpl(
	sqlStats *sqlStats, appStats *appStats, phaseTimes phaseTimes,
) *sqlStatsCollectorImpl {
	return &sqlStatsCollectorImpl{
		sqlStats:   sqlStats,
		appStats:   appStats,
		phaseTimes: phaseTimes,
	}
}

// PhaseTimes is part of the sqlStatsCollector interface.
func (s *sqlStatsCollectorImpl) PhaseTimes() *phaseTimes {
	return &s.phaseTimes
}

// RecordStatement is part of the sqlStatsCollector interface.
func (s *sqlStatsCollectorImpl) RecordStatement(
	stmt Statement,
	distSQLUsed bool,
	automaticRetryCount int,
	numRows int,
	err error,
	parseLat, planLat, runLat, svcLat, ovhLat float64,
) {
	s.appStats.recordStatement(
		stmt, distSQLUsed, automaticRetryCount, numRows, err,
		parseLat, planLat, runLat, svcLat, ovhLat)
}

// SQLStats is part of the sqlStatsCollector interface.
func (s *sqlStatsCollectorImpl) SQLStats() *sqlStats {
	return s.sqlStats
}
