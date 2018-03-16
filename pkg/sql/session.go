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
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

	//
	// Run-time state.
	//

	// execCfg is the configuration of the Executor that is executing this
	// session.
	execCfg *ExecutorConfig
	// distSQLPlanner is in charge of distSQL physical planning and running
	// logic.
	distSQLPlanner *DistSQLPlanner
	// context is the Session's base context, to be used for all
	// SQL-related logging. See Ctx().
	context context.Context
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

	tables TableCollection

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
		ActiveQueries map[uint128.Uint128]*queryMeta

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
}

// sessionDefaults mirrors fields in Session, for restoring default
// configuration values in SET ... TO DEFAULT (or RESET ...) statements.
type sessionDefaults struct {
	applicationName string
	database        string
}

// SessionArgs contains arguments for serving a client connection.
type SessionArgs struct {
	Database        string
	User            string
	ApplicationName string
	// RemoteAddr is the client's address. This is nil iff this is an internal
	// client.
	RemoteAddr net.Addr
}

// SessionRegistry stores a set of all sessions on this node.
// Use register() and deregister() to modify this registry.
type SessionRegistry struct {
	syncutil.Mutex
	store map[registrySession]struct{}
}

// MakeSessionRegistry creates a new SessionRegistry with an empty set
// of sessions.
func MakeSessionRegistry() *SessionRegistry {
	return &SessionRegistry{store: make(map[registrySession]struct{})}
}

func (r *SessionRegistry) register(s registrySession) {
	r.Lock()
	r.store[s] = struct{}{}
	r.Unlock()
}

func (r *SessionRegistry) deregister(s registrySession) {
	r.Lock()
	delete(r.store, s)
	r.Unlock()
}

type registrySession interface {
	user() string
	cancelQuery(queryID uint128.Uint128) bool
	// serialize serializes a Session into a serverpb.Session
	// that can be served over RPC.
	serialize() serverpb.Session
}

// CancelQuery looks up the associated query in the session registry and cancels it.
func (r *SessionRegistry) CancelQuery(queryIDStr string, username string) (bool, error) {
	queryID, err := uint128.FromString(queryIDStr)
	if err != nil {
		return false, fmt.Errorf("query ID %s malformed: %s", queryID, err)
	}

	r.Lock()
	defer r.Unlock()

	for session := range r.store {
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

	// Ctx is the context for everything running in this SQL txn.
	Ctx context.Context

	// implicitTxn if set if the transaction was automatically created for a
	// single statement.
	implicitTxn bool

	// The schema change closures to run when this txn is done.
	schemaChangers schemaChangerCollection

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
	if cfg.SchemaChangerTestingKnobs.SyncFilter != nil && (len(scc.schemaChangers) > 0) {
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

// cancelQuery is part of the registrySession interface.
func (s *Session) cancelQuery(queryID uint128.Uint128) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if queryMeta, exists := s.mu.ActiveQueries[queryID]; exists {
		queryMeta.cancel()
		return true
	}
	return false
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
