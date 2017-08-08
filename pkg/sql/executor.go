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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errStaleMetadata = errors.New("metadata is still stale")
var errTransactionInProgress = errors.New("there is already a transaction in progress")

func errWrongNumberOfPreparedStatements(n int) error {
	return pgerror.NewErrorf(pgerror.CodeInvalidPreparedStatementDefinitionError,
		"prepared statement had %d statements, expected 1", n)
}

const sqlTxnName string = "sql txn"
const sqlImplicitTxnName string = "sql txn implicit"
const metricsSampleInterval = 10 * time.Second

// Fully-qualified names for metrics.
var (
	MetaTxnBegin = metric.Metadata{
		Name: "sql.txn.begin.count",
		Help: "Number of SQL transaction BEGIN statements"}
	MetaTxnCommit = metric.Metadata{
		Name: "sql.txn.commit.count",
		Help: "Number of SQL transaction COMMIT statements"}
	MetaTxnAbort = metric.Metadata{
		Name: "sql.txn.abort.count",
		Help: "Number of SQL transaction ABORT statements"}
	MetaTxnRollback = metric.Metadata{
		Name: "sql.txn.rollback.count",
		Help: "Number of SQL transaction ROLLBACK statements"}
	MetaSelect = metric.Metadata{
		Name: "sql.select.count",
		Help: "Number of SQL SELECT statements"}
	MetaSQLExecLatency = metric.Metadata{
		Name: "sql.exec.latency",
		Help: "Latency of SQL statement execution"}
	MetaSQLServiceLatency = metric.Metadata{
		Name: "sql.service.latency",
		Help: "Latency of SQL request execution"}
	MetaDistSQLSelect = metric.Metadata{
		Name: "sql.distsql.select.count",
		Help: "Number of dist-SQL SELECT statements"}
	MetaDistSQLExecLatency = metric.Metadata{
		Name: "sql.distsql.exec.latency",
		Help: "Latency of dist-SQL statement execution"}
	MetaDistSQLServiceLatency = metric.Metadata{
		Name: "sql.distsql.service.latency",
		Help: "Latency of dist-SQL request execution"}
	MetaUpdate = metric.Metadata{
		Name: "sql.update.count",
		Help: "Number of SQL UPDATE statements"}
	MetaInsert = metric.Metadata{
		Name: "sql.insert.count",
		Help: "Number of SQL INSERT statements"}
	MetaDelete = metric.Metadata{
		Name: "sql.delete.count",
		Help: "Number of SQL DELETE statements"}
	MetaDdl = metric.Metadata{
		Name: "sql.ddl.count",
		Help: "Number of SQL DDL statements"}
	MetaMisc = metric.Metadata{
		Name: "sql.misc.count",
		Help: "Number of other SQL statements"}
	MetaQuery = metric.Metadata{
		Name: "sql.query.count",
		Help: "Number of SQL queries"}
)

type traceResult struct {
	tag   string
	count int
}

func (r *traceResult) String() string {
	if r.count < 0 {
		return r.tag
	}
	return fmt.Sprintf("%s (%d results)", r.tag, r.count)
}

// ResultList represents a list of results for a list of SQL statements.
// There is one result object per SQL statement in the request.
type ResultList []Result

// StatementResults represents a list of results from running a batch of
// SQL statements, plus some meta info about the batch.
type StatementResults struct {
	ResultList
	// Indicates that after parsing, the request contained 0 non-empty statements.
	Empty bool
}

// Close ensures that the resources claimed by the results are released.
func (s *StatementResults) Close(ctx context.Context) {
	s.ResultList.Close(ctx)
}

// Close ensures that the resources claimed by the results are released.
func (rl ResultList) Close(ctx context.Context) {
	for _, r := range rl {
		r.Close(ctx)
	}
}

// Result corresponds to the execution of a single SQL statement.
type Result struct {
	Err error
	// The type of statement that the result is for.
	Type parser.StatementType
	// The tag of the statement that the result is for.
	PGTag string
	// RowsAffected will be populated if the statement type is "RowsAffected".
	RowsAffected int
	// Columns will be populated if the statement type is "Rows". It will contain
	// the names and types of the columns returned in the result set in the order
	// specified in the SQL statement. The number of columns will equal the number
	// of values in each Row.
	Columns sqlbase.ResultColumns
	// Rows will be populated if the statement type is "Rows". It will contain
	// the result set of the result.
	// TODO(nvanbenschoten): Can this be streamed from the planNode?
	Rows *sqlbase.RowContainer
}

// Close ensures that the resources claimed by the result are released.
func (r *Result) Close(ctx context.Context) {
	// The Rows pointer may be nil if the statement returned no rows or
	// if an error occurred.
	if r.Rows != nil {
		r.Rows.Close(ctx)
	}
}

// An Executor executes SQL statements.
// Executor is thread-safe.
type Executor struct {
	cfg            ExecutorConfig
	stopper        *stop.Stopper
	reCache        *parser.RegexpCache
	virtualSchemas virtualSchemaHolder

	// Transient stats.
	SelectCount *metric.Counter
	// The subset of SELECTs that are processed through DistSQL.
	DistSQLSelectCount    *metric.Counter
	DistSQLExecLatency    *metric.Histogram
	SQLExecLatency        *metric.Histogram
	DistSQLServiceLatency *metric.Histogram
	SQLServiceLatency     *metric.Histogram
	TxnBeginCount         *metric.Counter

	// txnCommitCount counts the number of times a COMMIT was attempted.
	TxnCommitCount *metric.Counter

	TxnAbortCount    *metric.Counter
	TxnRollbackCount *metric.Counter
	UpdateCount      *metric.Counter
	InsertCount      *metric.Counter
	DeleteCount      *metric.Counter
	DdlCount         *metric.Counter
	MiscCount        *metric.Counter
	QueryCount       *metric.Counter

	// System Config and mutex.
	systemConfig config.SystemConfig
	// databaseCache is updated with systemConfigMu held, but read atomically in
	// order to avoid recursive locking. See WaitForGossipUpdate.
	databaseCache    atomic.Value
	systemConfigMu   syncutil.Mutex
	systemConfigCond *sync.Cond

	distSQLPlanner *distSQLPlanner

	// Application-level SQL statistics
	sqlStats sqlStats

	// Attempts to use unimplemented features.
	unimplementedErrors struct {
		syncutil.Mutex
		counts map[string]int64
	}
}

// NodeInfo contains metadata about the executing node and cluster.
type NodeInfo struct {
	ClusterID    func() uuid.UUID
	NodeID       *base.NodeIDContainer
	Organization *settings.StringSetting
}

// An ExecutorConfig encompasses the auxiliary objects and configuration
// required to create an executor.
// All fields holding a pointer or an interface are required to create
// a Executor; the rest will have sane defaults set if omitted.
type ExecutorConfig struct {
	Settings *cluster.Settings
	NodeInfo
	AmbientCtx      log.AmbientContext
	DB              *client.DB
	Gossip          *gossip.Gossip
	DistSender      *kv.DistSender
	RPCContext      *rpc.Context
	LeaseManager    *LeaseManager
	Clock           *hlc.Clock
	DistSQLSrv      *distsqlrun.ServerImpl
	StatusServer    serverpb.StatusServer
	SessionRegistry *SessionRegistry
	QueryRegistry   *QueryRegistry
	JobRegistry     *jobs.Registry

	TestingKnobs              *ExecutorTestingKnobs
	SchemaChangerTestingKnobs *SchemaChangerTestingKnobs
	// HistogramWindowInterval is (server.Context).HistogramWindowInterval.
	HistogramWindowInterval time.Duration

	// Caches updated by DistSQL.
	RangeDescriptorCache *kv.RangeDescriptorCache
	LeaseHolderCache     *kv.LeaseHolderCache
}

var _ base.ModuleTestingKnobs = &ExecutorTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ExecutorTestingKnobs) ModuleTestingKnobs() {}

// StatementFilter is the type of callback that
// ExecutorTestingKnobs.StatementFilter takes.
type StatementFilter func(context.Context, string, *Result)

// ExecutorTestingKnobs is part of the context used to control parts of the
// system during testing.
type ExecutorTestingKnobs struct {
	// WaitForGossipUpdate causes metadata-mutating operations to wait
	// for the new metadata to back-propagate through gossip.
	WaitForGossipUpdate bool

	// CheckStmtStringChange causes Executor.execStmtsInCurrentTxn to verify
	// that executed statements are not modified during execution.
	CheckStmtStringChange bool

	// StatementFilter can be used to trap execution of SQL statements and
	// optionally change their results. The filter function is invoked after each
	// statement has been executed.
	StatementFilter StatementFilter

	// BeforePrepare is called by the Executor before preparing any statement. It
	// gives access to the planner that will be used to do the prepare. If any of
	// the return values are not nil, the values are used as the prepare results
	// and normal preparation is short-circuited.
	BeforePrepare func(ctx context.Context, stmt string, planner *planner) (*PreparedStatement, error)

	// BeforeExecute is called by the Executor before plan execution. It is useful
	// for synchronizing statement execution, such as with parallel statemets.
	BeforeExecute func(ctx context.Context, stmt string, isParallel bool)

	// AfterExecute is like StatementFilter, but it runs in the same goroutine of the
	// statement.
	AfterExecute func(ctx context.Context, stmt string, res *Result, err error)

	// DisableAutoCommit, if set, disables the auto-commit functionality of some
	// SQL statements. That functionality allows some statements to commit
	// directly when they're executed in an implicit SQL txn, without waiting for
	// the Executor to commit the implicit txn.
	// This has to be set in tests that need to abort such statements using a
	// StatementFilter; otherwise, the statement commits immediately after
	// execution so there'll be nothing left to abort by the time the filter runs.
	DisableAutoCommit bool

	// DistSQLPlannerKnobs are testing knobs for distSQLPlanner.
	DistSQLPlannerKnobs DistSQLPlannerTestingKnobs
}

// DistSQLPlannerTestingKnobs is used to control internals of the distSQLPlanner
// for testing purposes.
type DistSQLPlannerTestingKnobs struct {
	// If OverrideSQLHealthCheck is set, we use this callback to get the health of
	// a node.
	OverrideHealthCheck func(node roachpb.NodeID, addrString string) error
}

// QueryRegistry holds a map of all queries executing with this node as its gateway.
type QueryRegistry struct {
	// TODO(peter): Investigate if it is worthwhile to shard based on the query
	// ID in order to reduce contention on this lock.
	syncutil.Mutex
	store map[uint128.Uint128]*queryMeta
}

// MakeQueryRegistry instantiates a new, empty query registry.
func MakeQueryRegistry() *QueryRegistry {
	return &QueryRegistry{store: make(map[uint128.Uint128]*queryMeta)}
}

func (r *QueryRegistry) register(queryID uint128.Uint128, query *queryMeta) {
	r.Lock()
	r.store[queryID] = query
	r.Unlock()
}

func (r *QueryRegistry) deregister(queryID uint128.Uint128) {
	r.Lock()
	delete(r.store, queryID)
	r.Unlock()
}

// Cancel looks up the associated query in the registry and cancels it (if it is cancellable).
func (r *QueryRegistry) Cancel(queryIDStr string, username string) (bool, error) {
	queryID, err := uint128.FromString(queryIDStr)
	if err != nil {
		return false, fmt.Errorf("query ID %s malformed: %s", queryID, err)
	}

	r.Lock()
	defer r.Unlock()

	if queryMeta, exists := r.store[queryID]; exists {
		if !(username == security.RootUser || username == queryMeta.session.User) {
			// This user does not have cancel privileges over this query.
			return false, fmt.Errorf("query ID %s not found", queryID)
		}

		queryMeta.cancel()

		return true, nil
	}

	return false, fmt.Errorf("query ID %s not found", queryID)
}

// NewExecutor creates an Executor and registers a callback on the
// system config.
func NewExecutor(cfg ExecutorConfig, stopper *stop.Stopper) *Executor {
	return &Executor{
		cfg:     cfg,
		stopper: stopper,
		reCache: parser.NewRegexpCache(512),

		TxnBeginCount:      metric.NewCounter(MetaTxnBegin),
		TxnCommitCount:     metric.NewCounter(MetaTxnCommit),
		TxnAbortCount:      metric.NewCounter(MetaTxnAbort),
		TxnRollbackCount:   metric.NewCounter(MetaTxnRollback),
		SelectCount:        metric.NewCounter(MetaSelect),
		DistSQLSelectCount: metric.NewCounter(MetaDistSQLSelect),
		// TODO(mrtracy): See HistogramWindowInterval in server/config.go for the 6x factor.
		DistSQLExecLatency: metric.NewLatency(MetaDistSQLExecLatency,
			6*metricsSampleInterval),
		SQLExecLatency: metric.NewLatency(MetaSQLExecLatency,
			6*metricsSampleInterval),
		DistSQLServiceLatency: metric.NewLatency(MetaDistSQLServiceLatency,
			6*metricsSampleInterval),
		SQLServiceLatency: metric.NewLatency(MetaSQLServiceLatency,
			6*metricsSampleInterval),
		UpdateCount: metric.NewCounter(MetaUpdate),
		InsertCount: metric.NewCounter(MetaInsert),
		DeleteCount: metric.NewCounter(MetaDelete),
		DdlCount:    metric.NewCounter(MetaDdl),
		MiscCount:   metric.NewCounter(MetaMisc),
		QueryCount:  metric.NewCounter(MetaQuery),
		sqlStats:    sqlStats{st: cfg.Settings, apps: make(map[string]*appStats)},
	}
}

// Start starts workers for the executor and initializes the distSQLPlanner.
func (e *Executor) Start(
	ctx context.Context, startupMemMetrics *MemoryMetrics, nodeDesc roachpb.NodeDescriptor,
) {
	ctx = e.AnnotateCtx(ctx)
	log.Infof(ctx, "creating distSQLPlanner with address %s", nodeDesc.Address)
	e.distSQLPlanner = newDistSQLPlanner(
		e.cfg.Settings,
		nodeDesc,
		e.cfg.RPCContext,
		e.cfg.DistSQLSrv,
		e.cfg.DistSender,
		e.cfg.Gossip,
		e.stopper,
		e.cfg.TestingKnobs.DistSQLPlannerKnobs,
	)

	e.databaseCache.Store(newDatabaseCache(e.systemConfig))
	e.systemConfigCond = sync.NewCond(&e.systemConfigMu)

	gossipUpdateC := e.cfg.Gossip.RegisterSystemConfigChannel()
	e.stopper.RunWorker(ctx, func(ctx context.Context) {
		for {
			select {
			case <-gossipUpdateC:
				sysCfg, _ := e.cfg.Gossip.GetSystemConfig()
				e.updateSystemConfig(sysCfg)
			case <-e.stopper.ShouldStop():
				return
			}
		}
	})

	ctx = log.WithLogTag(ctx, "startup", nil)
	startupSession := NewSession(ctx, SessionArgs{}, e, nil, startupMemMetrics)
	startupSession.StartUnlimitedMonitor()
	if err := e.virtualSchemas.init(ctx, startupSession.newPlanner(e, nil)); err != nil {
		log.Fatal(ctx, err)
	}
	startupSession.Finish(e)
}

// GetVirtualTabler retrieves the VirtualTabler reference for this executor.
func (e *Executor) GetVirtualTabler() VirtualTabler {
	return &e.virtualSchemas
}

// SetDistSQLSpanResolver changes the SpanResolver used for DistSQL. It is the
// caller's responsibility to make sure no queries are being run with DistSQL at
// the same time.
func (e *Executor) SetDistSQLSpanResolver(spanResolver distsqlplan.SpanResolver) {
	e.distSQLPlanner.setSpanResolver(spanResolver)
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (e *Executor) AnnotateCtx(ctx context.Context) context.Context {
	return e.cfg.AmbientCtx.AnnotateCtx(ctx)
}

// updateSystemConfig is called whenever the system config gossip entry is updated.
func (e *Executor) updateSystemConfig(cfg config.SystemConfig) {
	e.systemConfigMu.Lock()
	defer e.systemConfigMu.Unlock()
	e.systemConfig = cfg
	// The database cache gets reset whenever the system config changes.
	e.databaseCache.Store(newDatabaseCache(cfg))
	e.systemConfigCond.Broadcast()
}

// getDatabaseCache returns a database cache with a copy of the latest
// system config.
func (e *Executor) getDatabaseCache() *databaseCache {
	if v := e.databaseCache.Load(); v != nil {
		return v.(*databaseCache)
	}
	return nil
}

// Prepare returns the result types of the given statement. pinfo may
// contain partial type information for placeholders. Prepare will
// populate the missing types. The PreparedStatement is returned (or
// nil if there are no results).
func (e *Executor) Prepare(
	stmt Statement, stmtStr string, session *Session, pinfo parser.PlaceholderTypes,
) (res *PreparedStatement, err error) {
	session.resetForBatch(e)
	sessionEventf(session, "preparing: %s", stmtStr)

	defer session.maybeRecover("preparing", stmtStr)

	prepared := &PreparedStatement{
		SQLTypes:    pinfo,
		portalNames: make(map[string]struct{}),
	}

	// We need a memory account available in order to prepare a statement, since we
	// might need to allocate memory for constant-folded values in the process of
	// planning it.
	prepared.constantAcc = session.mon.MakeBoundAccount()

	if stmt.AST == nil {
		return prepared, nil
	}

	prepared.Statement = stmt.AST
	if err := pinfo.ProcessPlaceholderAnnotations(stmt.AST); err != nil {
		return nil, err
	}
	protoTS, err := isAsOf(session, stmt.AST, e.cfg.Clock.Now())
	if err != nil {
		return nil, err
	}

	// Prepare needs a transaction because it needs to retrieve db/table
	// descriptors for type checking.
	txn := session.TxnState.mu.txn

	if txn == nil {
		// The new txn need not be the same transaction used by statements following
		// this prepare statement because it can only be used by prepare() to get a
		// table lease that is eventually added to the session.
		//
		// TODO(vivek): perhaps we should be more consistent and update
		// session.TxnState.mu.txn, but more thought needs to be put into whether that
		// is really needed.
		txn = client.NewTxn(e.cfg.DB)
		if err := txn.SetIsolation(session.DefaultIsolationLevel); err != nil {
			panic(fmt.Errorf("cannot set up txn for prepare %q: %v", stmtStr, err))
		}
		txn.Proto().OrigTimestamp = e.cfg.Clock.Now()
	}

	planner := session.newPlanner(e, txn)
	planner.semaCtx.Placeholders.SetTypes(pinfo)
	planner.evalCtx.PrepareOnly = true
	planner.evalCtx.ActiveMemAcc = &prepared.constantAcc

	if protoTS != nil {
		planner.avoidCachedDescriptors = true
		txn.SetFixedTimestamp(*protoTS)
	}

	if filter := e.cfg.TestingKnobs.BeforePrepare; filter != nil {
		res, err := filter(session.Ctx(), stmtStr, planner)
		if res != nil || err != nil {
			return res, err
		}
	}

	plan, err := planner.prepare(session.Ctx(), stmt.AST)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return prepared, nil
	}
	defer plan.Close(session.Ctx())
	prepared.Columns = planColumns(plan)
	for _, c := range prepared.Columns {
		if err := checkResultType(c.Typ); err != nil {
			return nil, err
		}
	}
	return prepared, nil
}

// ExecuteStatements executes the given statement(s) and returns a response.
func (e *Executor) ExecuteStatements(
	session *Session, stmts string, pinfo *parser.PlaceholderInfo,
) StatementResults {
	session.resetForBatch(e)
	session.phaseTimes[sessionStartBatch] = timeutil.Now()

	defer session.maybeRecover("executing", stmts)

	// If the Executor wants config updates to be blocked, then block them so
	// that session.testingVerifyMetadataFn can later be run on a known version
	// of the system config. The point is to lock the system config so that no
	// gossip updates sneak in under us. We're then able to assert that the
	// verify callback only succeeds after a gossip update.
	//
	// This lock does not change semantics. Even outside of tests, the Executor
	// uses static systemConfig for a user request, so locking the Executor's
	// systemConfig cannot change the semantics of the SQL operation being
	// performed under lock.
	//
	// NB: The locking here implies that ExecuteStatements cannot be
	// called recursively. So don't do that and don't try to adjust this locking
	// to allow this method to be called recursively (sync.{Mutex,RWMutex} do not
	// allow that).
	if e.cfg.TestingKnobs.WaitForGossipUpdate {
		e.systemConfigCond.L.Lock()
		defer e.systemConfigCond.L.Unlock()
	}

	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	return e.execRequest(session, stmts, pinfo, copyMsgNone)
}

// ExecutePreparedStatement executes the given statement and returns a response.
func (e *Executor) ExecutePreparedStatement(
	session *Session, stmt *PreparedStatement, pinfo *parser.PlaceholderInfo,
) (StatementResults, error) {
	defer session.maybeRecover("executing", stmt.Str)

	// Block system config updates. For more details, see the comment in
	// ExecuteStatements.
	if e.cfg.TestingKnobs.WaitForGossipUpdate {
		e.systemConfigCond.L.Lock()
		defer e.systemConfigCond.L.Unlock()
	}

	{
		// No parsing is taking place, but we need to set the parsing phase time
		// because the service latency is measured from
		// phaseTimes[sessionStartParse].
		now := timeutil.Now()
		session.phaseTimes[sessionStartParse] = now
		session.phaseTimes[sessionEndParse] = now
	}

	return e.execPrepared(session, stmt, pinfo)
}

// execPrepared executes a prepared statement. It returns an error if there
// is more than 1 result or the returned types differ from the prepared
// return types.
func (e *Executor) execPrepared(
	session *Session, stmt *PreparedStatement, pinfo *parser.PlaceholderInfo,
) (StatementResults, error) {
	if log.V(2) || e.cfg.Settings.LogStatementsExecuteEnabled.Get() {
		log.Infof(session.Ctx(), "execPrepared: %s", stmt.Str)
	}

	var stmts StatementList
	if stmt.Statement != nil {
		stmts = StatementList{{
			AST:           stmt.Statement,
			ExpectedTypes: stmt.Columns,
		}}
	}
	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	results := e.execParsed(session, stmts, pinfo, copyMsgNone)
	// This shouldn't ever happen since prepared statements are limited during
	// creation to one statement, but just in case it does, throw an error.
	if len(results.ResultList) > 1 {
		results.Close(session.Ctx())
		return StatementResults{}, pgerror.NewError(pgerror.CodeInternalError, "unexpected number of results")
	}
	return results, nil
}

// CopyData adds data to the COPY buffer and executes if there are enough rows.
func (e *Executor) CopyData(session *Session, data string) StatementResults {
	return e.execRequest(session, data, nil, copyMsgData)
}

// CopyDone executes the buffered COPY data.
func (e *Executor) CopyDone(session *Session) StatementResults {
	return e.execRequest(session, "", nil, copyMsgDone)
}

// CopyEnd ends the COPY mode. Any buffered data is discarded.
func (session *Session) CopyEnd(ctx context.Context) {
	session.copyFrom.Close(ctx)
	session.copyFrom = nil
}

// execRequest executes the request in the provided Session.
// It parses the sql into statements, iterates through the statements, creates
// KV transactions and automatically retries them when possible, and executes
// the (synchronous attempt of) schema changes.
// It will accumulate a result in Response for each statement.
// It will resume a SQL transaction, if one was previously open for this client.
//
// execRequest handles the mismatch between the SQL interface that the Executor
// provides, based on statements being streamed from the client in the context
// of a session, and the KV client.Txn interface, based on (possibly-retriable)
// callbacks passed to be executed in the context of a transaction. Actual
// execution of statements in the context of a KV txn is delegated to
// runTxnAttempt().
func (e *Executor) execRequest(
	session *Session, sql string, pinfo *parser.PlaceholderInfo, copymsg copyMsg,
) StatementResults {
	var stmts StatementList
	var err error
	txnState := &session.TxnState

	if log.V(2) || e.cfg.Settings.LogStatementsExecuteEnabled.Get() {
		log.Infof(session.Ctx(), "execRequest: %s", sql)
	}

	session.phaseTimes[sessionStartParse] = timeutil.Now()
	if session.copyFrom != nil {
		stmts, err = session.ProcessCopyData(session.Ctx(), sql, copymsg)
	} else if copymsg != copyMsgNone {
		err = fmt.Errorf("unexpected copy command")
	} else {
		var sl parser.StatementList
		sl, err = parser.Parse(sql)
		stmts = NewStatementList(sl)
	}
	session.phaseTimes[sessionEndParse] = timeutil.Now()

	if err != nil {
		if pgErr, ok := pgerror.GetPGCause(err); ok {
			if pgErr.Code == pgerror.CodeFeatureNotSupportedError {
				e.recordUnimplementedFeature(pgErr.InternalCommand)
			}
		}
		if log.V(2) || e.cfg.Settings.LogStatementsExecuteEnabled.Get() {
			log.Infof(session.Ctx(), "execRequest: error: %v", err)
		}
		// A parse error occurred: we can't determine if there were multiple
		// statements or only one, so just pretend there was one.
		if txnState.mu.txn != nil {
			// Rollback the txn.
			txnState.updateStateAndCleanupOnErr(err, e)
		}
		var res StatementResults
		res.ResultList = append(res.ResultList, Result{Err: err})
		return res
	}
	return e.execParsed(session, stmts, pinfo, copymsg)
}

func (e *Executor) execParsed(
	session *Session, stmts StatementList, pinfo *parser.PlaceholderInfo, copymsg copyMsg,
) StatementResults {
	var res StatementResults
	var avoidCachedDescriptors bool
	txnState := &session.TxnState

	if len(stmts) == 0 {
		res.Empty = true
		return res
	}

	for len(stmts) > 0 {
		// Each iteration consumes a transaction's worth of statements. Any error
		// that is encountered resets stmts.

		inTxn := txnState.State() != NoTxn
		execOpt := client.TxnExecOptions{
			AssignTimestampImmediately: true,
		}
		// Figure out the statements out of which we're going to try to consume
		// this iteration. If we need to create an implicit txn, only one statement
		// can be consumed.
		stmtsToExec := stmts
		// If protoTS is set, the transaction proto sets its Orig and Max timestamps
		// to it each retry.
		var protoTS *hlc.Timestamp
		// We can AutoRetry the next batch of statements if we're in a clean state
		// (i.e. the next statements we're going to see are the first statements in
		// a transaction).
		if !inTxn {
			// Detect implicit transactions - they need to be autocommitted.
			if _, isBegin := stmts[0].AST.(*parser.BeginTransaction); !isBegin {
				execOpt.AutoCommit = true
				stmtsToExec = stmtsToExec[:1]
				// Check for AS OF SYSTEM TIME. If it is present but not detected here,
				// it will raise an error later on.
				var err error
				protoTS, err = isAsOf(session, stmtsToExec[0].AST, e.cfg.Clock.Now())
				if err != nil {
					res.ResultList = append(res.ResultList, Result{Err: err})
					return res
				}
				if protoTS != nil {
					// When running AS OF SYSTEM TIME queries, we want to use the
					// table descriptors from the specified time, and never lease
					// anything. To do this, we pass down the avoidCachedDescriptors
					// flag and set the transaction's timestamp to the specified time.
					avoidCachedDescriptors = true
				}
			}
			txnState.resetForNewSQLTxn(
				e, session,
				execOpt.AutoCommit, /* implicitTxn */
				false,              /* retryIntent */
				e.cfg.Clock.PhysicalTime(), /* sqlTimestamp */
				session.DefaultIsolationLevel,
				roachpb.NormalUserPriority,
			)
		} else {
			// If we are in a txn, the first batch get auto-retried.
			txnState.autoRetry = txnState.State() == FirstBatch
		}
		execOpt.AutoRetry = txnState.autoRetry
		if txnState.State() == NoTxn {
			panic("we failed to initialize a txn")
		}
		// Now actually run some statements.
		var remainingStmts StatementList
		var results []Result
		origState := txnState.State()

		// Track if we are retrying this query, so that we do not double count.
		automaticRetryCount := 0
		txnClosure := func(ctx context.Context, txn *client.Txn, opt *client.TxnExecOptions) error {
			defer func() { automaticRetryCount++ }()
			if txnState.TxnIsOpen() && txnState.mu.txn != txn {
				panic(fmt.Sprintf("closure wasn't called in the txn we set up for it."+
					"\ntxnState.mu.txn:%+v\ntxn:%+v\ntxnState:%+v", txnState.mu.txn, txn, txnState))
			}
			txnState.mu.Lock()
			txnState.mu.txn = txn
			txnState.mu.Unlock()

			if protoTS != nil {
				txnState.mu.txn.SetFixedTimestamp(*protoTS)
			}

			var err error
			if results != nil {
				// Some results were produced by a previous attempt. Discard them.
				ResultList(results).Close(ctx)
			}

			results, remainingStmts, err = runTxnAttempt(
				e, session, stmtsToExec, pinfo, origState, opt,
				!inTxn /* txnPrefix */, avoidCachedDescriptors, automaticRetryCount)

			// TODO(andrei): Until #7881 fixed.
			if err == nil && txnState.State() == Aborted {
				doWarn := true
				if len(stmtsToExec) > 0 {
					if _, ok := stmtsToExec[0].AST.(*parser.ShowTransactionStatus); ok {
						doWarn = false
					}
				}
				if doWarn {
					log.Errorf(ctx,
						"7881: txnState is Aborted without an error propagating. stmtsToExec: %s, "+
							"results: %+v, remainingStmts: %s, txnState: %+v", stmtsToExec, results,
						remainingStmts, txnState)
				}
			}

			return err
		}
		// This is where the magic happens - we ask db to run a KV txn and possibly retry it.
		txn := txnState.mu.txn // this might be nil if the txn was already aborted.
		err := txn.Exec(session.Ctx(), execOpt, txnClosure)
		if err != nil && len(results) > 0 {
			// Set or override the error in the last result, if any.
			// The error might have come from auto-commit, in which case it wasn't
			// captured in a result. Or, we might have had a RetryableTxnError that
			// got converted to a non-retryable error when the txn closure was done.
			lastRes := &results[len(results)-1]
			lastRes.Err = convertToErrWithPGCode(err)
		}

		if err != nil && (log.V(2) || e.cfg.Settings.LogStatementsExecuteEnabled.Get()) {
			log.Infof(session.Ctx(), "execRequest: error: %v", err)
		}

		// Update the Err field of the last result if the error was coming from
		// auto commit. The error was generated outside of the txn closure, so it was not
		// set in any result.
		if err != nil {
			if aErr, ok := err.(*client.AutoCommitError); ok {
				// TODO(andrei): Until #7881 fixed.
				{
					if txnState.mu.txn != nil {
						log.Eventf(session.Ctx(), "executor got AutoCommitError: %s\n"+
							"txn: %+v\nexecOpt.AutoRetry %t, execOpt.AutoCommit:%t, stmts %+v, remaining %+v",
							aErr, txnState.mu.txn.Proto(), execOpt.AutoRetry, execOpt.AutoCommit, stmts,
							remainingStmts)
					} else {
						log.Errorf(session.Ctx(), "7881: AutoCommitError on nil txn: %s, "+
							"txnState %+v, execOpt %+v, stmts %+v, remaining %+v, txn captured before executing batch: %+v",
							aErr, txnState, execOpt, stmts, remainingStmts, txn)
						txnState.sp.SetBaggageItem(keyFor7881Sample, "sample me please")
					}
				}
				e.TxnAbortCount.Inc(1)
				// TODO(andrei): Once 7881 is fixed, this should be
				// txnState.mu.txn.CleanupOnError().
				txn.CleanupOnError(session.Ctx(), err)
			}
		}

		// Sanity check about not leaving KV txns open on errors.
		if err != nil && txnState.mu.txn != nil && !txnState.mu.txn.IsFinalized() {
			if _, retryable := err.(*roachpb.HandledRetryableTxnError); !retryable {
				log.Fatalf(session.Ctx(), "got a non-retryable error but the KV "+
					"transaction is not finalized. TxnState: %s, err: %s\n"+
					"err:%+v\n\ntxn: %s", txnState.State(), err, err, txnState.mu.txn.Proto())
			}
		}

		res.ResultList = append(res.ResultList, results...)
		// Now make sense of the state we got into and update txnState.
		if (txnState.State() == RestartWait || txnState.State() == Aborted) &&
			txnState.commitSeen {
			// A COMMIT got an error (retryable or not). Too bad, this txn is toast.
			// After we return a result for COMMIT (with the COMMIT pgwire tag), the
			// user can't send any more commands.
			e.TxnAbortCount.Inc(1)
			txn.CleanupOnError(session.Ctx(), err)
			txnState.resetStateAndTxn(NoTxn)
		}

		if execOpt.AutoCommit {
			// If execOpt.AutoCommit was set, then the txn no longer exists at this point.
			txnState.resetStateAndTxn(NoTxn)
		}

		// If we're no longer in a transaction, finish the trace.
		if txnState.State() == NoTxn {
			txnState.finishSQLTxn(session)
		}

		// Verify that the metadata callback fails, if one was set. This is
		// the precondition for validating that we need a gossip update for
		// the callback to eventually succeed. Note that we are careful to
		// check this just once per metadata callback (setting the callback
		// clears session.verifyFnCheckedOnce).
		if e.cfg.TestingKnobs.WaitForGossipUpdate {
			// Turn off test verification of metadata changes made by the
			// transaction if an error is seen during a transaction.
			if err != nil {
				session.testingVerifyMetadataFn = nil
			}
			if fn := session.testingVerifyMetadataFn; fn != nil && !session.verifyFnCheckedOnce {
				if fn(e.systemConfig) == nil {
					panic(fmt.Sprintf(
						"expected %q (or the statements before them) to require a "+
							"gossip update, but they did not", stmts))
				}
				session.verifyFnCheckedOnce = true
			}
		}

		// If the txn is not in an "open" state any more, exec the schema changes.
		// They'll short-circuit themselves if the mutation that queued them has
		// been rolled back from the table descriptor.
		if !txnState.TxnIsOpen() {
			// Verify that metadata callback eventually succeeds, if one was
			// set.
			if e.cfg.TestingKnobs.WaitForGossipUpdate {
				if fn := session.testingVerifyMetadataFn; fn != nil {
					if !session.verifyFnCheckedOnce {
						panic("initial state of the condition to verify was not checked")
					}

					for fn(e.systemConfig) != nil {
						e.systemConfigCond.Wait()
					}

					session.testingVerifyMetadataFn = nil
				}
			}

			// Exec the schema changers (if the txn rolled back, the schema changers
			// will short-circuit because the corresponding descriptor mutation is not
			// found).
			session.tables.releaseTables(session.Ctx())
			txnState.schemaChangers.execSchemaChanges(session.Ctx(), e, session, res.ResultList)
		}

		// Figure out what statements to run on the next iteration.
		if err != nil {
			// Don't execute anything further.
			stmts = nil
		} else if execOpt.AutoCommit {
			stmts = stmts[1:]
		} else {
			stmts = remainingStmts
		}
	}

	return res
}

// If the plan has a fast path we attempt to query that,
// otherwise we fall back to counting via plan.Next().
func countRowsAffected(runParams runParams, p planNode) (int, error) {
	if a, ok := p.(planNodeFastPath); ok {
		if count, res := a.FastPathResults(); res {
			return count, nil
		}
	}
	count := 0
	next, err := p.Next(runParams)
	for ; next; next, err = p.Next(runParams) {
		count++
	}
	return count, err
}

// runTxnAttempt is used in the closure we pass to txn.Exec(). It
// will be called possibly multiple times (if opt.AutoRetry is set).
//
// txnPrefix: set if the statements represent the first batch of statements in a
// 	txn. Used to trap nested BEGINs.
func runTxnAttempt(
	e *Executor,
	session *Session,
	stmts StatementList,
	pinfo *parser.PlaceholderInfo,
	origState TxnStateEnum,
	opt *client.TxnExecOptions,
	txnPrefix bool,
	avoidCachedDescriptors bool,
	automaticRetryCount int,
) ([]Result, StatementList, error) {

	// Ignore the state that might have been set by a previous try of this
	// closure. By putting these modifications to txnState behind the
	// automaticRetryCount condition, we guarantee that no asynchronous
	// statements are still executing and reading from the state. This
	// means that no synchronization is necessary to prevent data races.
	if automaticRetryCount > 0 {
		session.TxnState.SetState(origState)
		session.TxnState.commitSeen = false
	}

	results, remainingStmts, err := e.execStmtsInCurrentTxn(
		session, stmts, pinfo,
		txnPrefix, avoidCachedDescriptors, automaticRetryCount)

	if opt.AutoCommit && len(remainingStmts) > 0 {
		panic("implicit txn failed to execute all stmts")
	}
	return results, remainingStmts, err
}

// execStmtsInCurrentTxn consumes a prefix of stmts, namely the
// statements belonging to a single SQL transaction. It executes in
// the session's current transaction, which is assumed to exist.
//
// COMMIT/ROLLBACK statements can end the current transaction. If that happens,
// this method returns, and the remaining statements are returned.
//
// If an error occurs while executing a statement, the SQL txn will be
// considered aborted and subsequent statements will be discarded (they will
// not be executed, they will not be returned for future execution, they will
// not generate results). Note that this also includes COMMIT/ROLLBACK
// statements. Further note that errTransactionAborted is no exception -
// encountering it will discard subsequent statements. This means that, to
// recover from an aborted txn, a COMMIT/ROLLBACK statement needs to be the
// first one in stmts.
//
// Args:
// session: the session to execute the statement in.
// stmts: the semicolon-separated list of statements to execute.
// pinfo: the placeholders to use in the statements.
// txnPrefix: set if the statements represent the first batch of statements in a
// 	txn. Used to trap nested BEGINs.
// avoidCachedDescriptors: set if the statement execution should avoid
//  using cached descriptors.
// automaticRetryCount: increases with each retry; 0 for the first attempt.
//
// Returns:
//  - the list of results (one per executed statement).
//  - the statements that haven't been executed because the transaction has
//    been committed or rolled back. In returning an error, this will be nil.
//  - the error encountered while executing statements, if any. If an error
//    occurred, it corresponds to the last result returned. Subsequent statements
//    have not been executed. Note that usually the error is not reflected in
//    this last result; the caller is responsible copying it into the result
//    after converting it adequately.
func (e *Executor) execStmtsInCurrentTxn(
	session *Session,
	stmts StatementList,
	pinfo *parser.PlaceholderInfo,
	txnPrefix bool,
	avoidCachedDescriptors bool,
	automaticRetryCount int,
) ([]Result, StatementList, error) {
	var results []Result

	txnState := &session.TxnState
	if txnState.State() == NoTxn {
		panic("execStmtsInCurrentTransaction called outside of a txn")
	}

	for i, stmt := range stmts {
		if log.V(2) || e.cfg.Settings.LogStatementsExecuteEnabled.Get() ||
			log.HasSpanOrEvent(session.Ctx()) {
			log.VEventf(session.Ctx(), 2, "executing %d/%d: %s", i+1, len(stmts), stmt)
		}

		txnState.schemaChangers.curStatementIdx = i

		queryID := e.generateQueryID()

		queryMeta := &queryMeta{
			start: session.phaseTimes[sessionEndParse],
			stmt:  stmt.AST,
			phase: preparing,
		}

		stmt.queryID = queryID
		stmt.queryMeta = queryMeta

		// Cancelling a query cancels its transaction's context. Copy reference to
		// txn context and its cancellation function here.
		//
		// TODO(itsbilal): Ideally we'd like to fork off a context for each individual
		// statement. But the heartbeat loop in TxnCoordSender currently assumes that the context of the
		// first operation in a txn batch lasts at least as long as the transaction itself. Once that
		// sender is able to distinguish between statement and transaction contexts, queryMeta could
		// move to per-statement contexts.
		queryMeta.ctx = txnState.Ctx
		queryMeta.ctxCancel = txnState.cancel

		session.addActiveQuery(queryID, queryMeta)
		defer session.removeActiveQuery(queryID)
		e.cfg.QueryRegistry.register(queryID, queryMeta)
		defer e.cfg.QueryRegistry.deregister(queryID)

		var stmtStrBefore string
		// TODO(nvanbenschoten): Constant literals can change their representation (1.0000 -> 1) when type checking,
		// so we need to reconsider how this works.
		if (e.cfg.TestingKnobs.CheckStmtStringChange && false) ||
			(e.cfg.TestingKnobs.StatementFilter != nil) {
			// We do "statement string change" if a StatementFilter is installed,
			// because the StatementFilter relies on the textual representation of
			// statements to not change from what the client sends.
			stmtStrBefore = stmt.String()
		}

		var res Result
		var err error
		// Run SHOW TRANSACTION STATUS in a separate code path so it is
		// always guaranteed to execute regardless of the current transaction state.
		if _, ok := stmt.AST.(*parser.ShowTransactionStatus); ok {
			res, err = runShowTransactionState(session)
		} else {
			switch txnState.State() {
			case Open, FirstBatch:
				res, err = e.execStmtInOpenTxn(
					session, stmt, pinfo, txnPrefix && (i == 0), /* firstInTxn */
					avoidCachedDescriptors, automaticRetryCount)
			case Aborted, RestartWait:
				res, err = e.execStmtInAbortedTxn(session, stmt)
			case CommitWait:
				res, err = e.execStmtInCommitWaitTxn(session, stmt)
			default:
				panic(fmt.Sprintf("unexpected txn state: %s", txnState.State()))
			}
			if (e.cfg.TestingKnobs.CheckStmtStringChange && false) ||
				(e.cfg.TestingKnobs.StatementFilter != nil) {
				if after := stmt.String(); after != stmtStrBefore {
					panic(fmt.Sprintf("statement changed after exec; before:\n    %s\nafter:\n    %s",
						stmtStrBefore, after))
				}
			}
		}
		if filter := e.cfg.TestingKnobs.StatementFilter; filter != nil {
			filter(session.Ctx(), stmt.String(), &res)
		}
		results = append(results, res)
		if err != nil {
			// After an error happened, skip executing all the remaining statements
			// in this batch.  This is Postgres behavior, and it makes sense as the
			// protocol doesn't let you return results after an error.
			return results, nil, err
		}
		if txnState.State() == NoTxn {
			// If the transaction is done, return the remaining statements to
			// be executed as a different group.
			return results, stmts[i+1:], nil
		}
	}
	// If we got here, we've managed to consume all statements and we're still in a txn.
	return results, nil, nil
}

// getTransactionState retrieves a text representation of the given state.
func getTransactionState(txnState *txnState) string {
	state := txnState.State()
	if txnState.implicitTxn {
		state = NoTxn
	}
	// For the purposes of representing the states to client, make the FirstBatch
	// state look like Open.
	if state == FirstBatch {
		state = Open
	}
	return state.String()
}

// runShowTransactionState returns the state of current transaction.
func runShowTransactionState(session *Session) (Result, error) {
	var result Result
	result.PGTag = (*parser.Show)(nil).StatementTag()
	result.Type = (*parser.Show)(nil).StatementType()
	result.Columns = sqlbase.ResultColumns{{Name: "TRANSACTION STATUS", Typ: parser.TypeString}}
	result.Rows = sqlbase.NewRowContainer(
		session.makeBoundAccount(), sqlbase.ColTypeInfoFromResCols(result.Columns), 0,
	)
	state := getTransactionState(&session.TxnState)
	if _, err := result.Rows.AddRow(
		session.Ctx(), parser.Datums{parser.NewDString(state)},
	); err != nil {
		result.Rows.Close(session.Ctx())
		return result, err
	}
	return result, nil
}

// execStmtInAbortedTxn executes a statement in a txn that's in state
// Aborted or RestartWait. All statements cause errors except:
// - COMMIT / ROLLBACK: aborts the current transaction.
// - ROLLBACK TO SAVEPOINT / SAVEPOINT: reopens the current transaction,
//   allowing it to be retried.
func (e *Executor) execStmtInAbortedTxn(session *Session, stmt Statement) (Result, error) {
	txnState := &session.TxnState
	if txnState.State() != Aborted && txnState.State() != RestartWait {
		panic("execStmtInAbortedTxn called outside of an aborted txn")
	}
	// TODO(andrei/cuongdo): Figure out what statements to count here.
	switch s := stmt.AST.(type) {
	case *parser.CommitTransaction, *parser.RollbackTransaction:
		if txnState.State() == RestartWait {
			return rollbackSQLTransaction(txnState), nil
		}
		// Reset the state to allow new transactions to start.
		// The KV txn has already been rolled back when we entered the Aborted state.
		// Note: postgres replies to COMMIT of failed txn with "ROLLBACK" too.
		result := Result{PGTag: (*parser.RollbackTransaction)(nil).StatementTag()}
		txnState.resetStateAndTxn(NoTxn)
		return result, nil
	case *parser.RollbackToSavepoint, *parser.Savepoint:
		// We accept both the "ROLLBACK TO SAVEPOINT cockroach_restart" and the
		// "SAVEPOINT cockroach_restart" commands to indicate client intent to
		// retry a transaction in a RestartWait state.
		var spName string
		switch n := s.(type) {
		case *parser.RollbackToSavepoint:
			spName = n.Savepoint
		case *parser.Savepoint:
			spName = n.Name
		default:
			panic("unreachable")
		}
		if err := parser.ValidateRestartCheckpoint(spName); err != nil {
			if txnState.State() == RestartWait {
				txnState.updateStateAndCleanupOnErr(err, e)
			}
			return Result{}, err
		}
		if !txnState.retryIntent {
			err := fmt.Errorf("SAVEPOINT %s has not been used", parser.RestartSavepointName)
			if txnState.State() == RestartWait {
				txnState.updateStateAndCleanupOnErr(err, e)
			}
			return Result{}, err
		}

		if txnState.State() == RestartWait {
			// Reset the state to FirstBatch. We're in an "open" txn again.
			txnState.SetState(FirstBatch)
		} else {
			// We accept ROLLBACK TO SAVEPOINT even after non-retryable errors to make
			// it easy for client libraries that want to indiscriminately issue
			// ROLLBACK TO SAVEPOINT after every error and possibly follow it with a
			// ROLLBACK and also because we accept ROLLBACK TO SAVEPOINT in the Open
			// state, so this is consistent.
			// The old txn has already been rolled back; we start a new txn with the
			// same sql timestamp and isolation as the current one.
			curTs, curIso, curPri := txnState.sqlTimestamp, txnState.isolation, txnState.priority
			txnState.finishSQLTxn(session)
			txnState.resetForNewSQLTxn(
				e, session,
				false /* implicitTxn */, true, /* retryIntent */
				curTs /* sqlTimestamp */, curIso /* isolation */, curPri /* priority */)
		}
		// TODO(andrei/cdo): add a counter for user-directed retries.
		return Result{}, nil
	default:
		if txnState.State() == RestartWait {
			err := sqlbase.NewTransactionAbortedError(
				"Expected \"ROLLBACK TO SAVEPOINT COCKROACH_RESTART\"" /* customMsg */)
			// If we were waiting for a restart, but the client failed to perform it,
			// we'll cleanup the txn. The client is not respecting the protocol, so
			// there seems to be little point in staying in RestartWait (plus,
			// higher-level code asserts that we're only in RestartWait when returning
			// retryable errors to the client).
			txnState.updateStateAndCleanupOnErr(err, e)
			return Result{}, err
		}
		return Result{}, sqlbase.NewTransactionAbortedError("" /* customMsg */)
	}
}

// execStmtInCommitWaitTxn executes a statement in a txn that's in state
// CommitWait.
// Everything but COMMIT/ROLLBACK causes errors. ROLLBACK is treated like COMMIT.
func (e *Executor) execStmtInCommitWaitTxn(session *Session, stmt Statement) (Result, error) {
	txnState := &session.TxnState
	if txnState.State() != CommitWait {
		panic("execStmtInCommitWaitTxn called outside of an aborted txn")
	}
	e.updateStmtCounts(stmt)
	switch stmt.AST.(type) {
	case *parser.CommitTransaction, *parser.RollbackTransaction:
		// Reset the state to allow new transactions to start.
		result := Result{PGTag: (*parser.CommitTransaction)(nil).StatementTag()}
		txnState.resetStateAndTxn(NoTxn)
		return result, nil
	default:
		err := sqlbase.NewTransactionCommittedError()
		return Result{}, err
	}
}

// sessionEventf logs an event to the current transaction context and to the
// session event log.
func sessionEventf(session *Session, format string, args ...interface{}) {
	if session.eventLog == nil {
		log.VEventf(session.context, 2, format, args...)
	} else {
		str := fmt.Sprintf(format, args...)
		log.VEvent(session.context, 2, str)
		session.eventLog.Printf("%s", str)
	}
}

// execStmtInOpenTxn executes one statement in the context
// of the session's current transaction (which is assumed to exist).
// It handles statements that affect the transaction state (BEGIN, COMMIT)
// and delegates everything else to `execStmt`.
// It binds placeholders.
//
// The current transaction might be committed/rolled back when this returns.
// It might also transition to the aborted or RestartWait state, and it
// might transition from FirstBatch to Open.
//
// Args:
// session: the session to execute the statement in.
// stmt: the statement to execute.
// pinfo: the placeholders to use in the statement.
// firstInTxn: set for the first statement in a transaction. Used
//  so that nested BEGIN statements are caught.
// avoidCachedDescriptors: set if the statement execution should avoid
//  using cached descriptors.
// automaticRetryCount: increases with each retry; 0 for the first attempt.
//
// Returns:
// - a Result
// - an error, if any. In case of error, the result returned also reflects this error.
func (e *Executor) execStmtInOpenTxn(
	session *Session,
	stmt Statement,
	pinfo *parser.PlaceholderInfo,
	firstInTxn bool,
	avoidCachedDescriptors bool,
	automaticRetryCount int,
) (_ Result, err error) {
	txnState := &session.TxnState
	if !txnState.TxnIsOpen() {
		panic("execStmtInOpenTxn called outside of an open txn")
	}

	sessionEventf(session, "%s", stmt)

	// Do not double count automatically retried transactions.
	if automaticRetryCount == 0 {
		e.updateStmtCounts(stmt)
	}

	// After the statement is executed, we might have to do state transitions.
	defer func() {
		if err != nil {
			if !txnState.TxnIsOpen() {
				panic(fmt.Sprintf("unexpected txnState when cleaning up: %v", txnState.State()))
			}
			txnState.updateStateAndCleanupOnErr(err, e)

			if firstInTxn && isBegin(stmt) {
				// A failed BEGIN statement that was starting a txn doesn't leave the
				// txn in the Aborted state; we transition back to NoTxn.
				txnState.resetStateAndTxn(NoTxn)
			}
		} else if txnState.autoRetry &&
			session.parallelizeQueue.Empty() &&
			txnState.isSerializableRestart() {
			// If we can still automatically retry the txn, and we detect that the
			// transaction won't be allowed to commit because its timestamp has been
			// pushed, we go ahead and retry now - if we'd execute further statements,
			// we probably wouldn't be allowed to retry automatically any more.
			// If there are any statements currently being executed (through the
			// parallelize queue), then we don't do this push detection. That's
			// because it's currently not kosher to send request after the proto has
			// been restarted (see #17197).
			// TODO(andrei): Remove the parallelizeQueue restriction after #17197 is
			// fixed.
			txnState.mu.txn.Proto().Restart(
				0 /* userPriority */, 0 /* upgradePriority */, e.cfg.Clock.Now())
			// Force an auto-retry by returning a retryable error to the higher
			// levels.
			err = roachpb.NewHandledRetryableTxnError(
				"serializable transaction timestamp pushed (detected by sql Executor)",
				txnState.mu.txn.ID(),
				// No updated transaction required; we've already manually updated our
				// client.Txn.
				roachpb.Transaction{},
			)
		} else if txnState.State() == FirstBatch &&
			!canStayInFirstBatchState(stmt) {
			// Transition from FirstBatch to Open except in the case of special
			// statements that don't return results to the client. This transition
			// does not affect the current batch - future statements in it will still
			// be retried automatically.
			txnState.SetState(Open)
		}
	}()

	// Check if the statement is parallelized or is independent from parallel
	// execution. If neither of these cases are true, we need to synchronize
	// parallel execution by letting it drain before we can begin executing ourselves.
	parallelize := IsStmtParallelized(stmt)
	_, independentFromParallelStmts := stmt.AST.(parser.IndependentFromParallelizedPriors)
	if !(parallelize || independentFromParallelStmts) {
		if err := session.parallelizeQueue.Wait(); err != nil {
			return Result{}, err
		}
	}

	if txnState.implicitTxn && !stmtAllowedInImplicitTxn(stmt) {
		return Result{}, errNoTransactionInProgress
	}

	switch s := stmt.AST.(type) {
	case *parser.BeginTransaction:
		if !firstInTxn {
			return Result{}, errTransactionInProgress
		}

	case *parser.CommitTransaction:
		// CommitTransaction is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		return commitSQLTransaction(txnState, commit)

	case *parser.ReleaseSavepoint:
		if err := parser.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return Result{}, err
		}
		// ReleaseSavepoint is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		return commitSQLTransaction(txnState, release)

	case *parser.RollbackTransaction:
		// Turn off test verification of metadata changes made by the
		// transaction.
		session.testingVerifyMetadataFn = nil
		// RollbackTransaction is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		// Notice that we don't return any errors on rollback.
		return rollbackSQLTransaction(txnState), nil

	case *parser.Savepoint:
		if err := parser.ValidateRestartCheckpoint(s.Name); err != nil {
			return Result{}, err
		}
		// We want to disallow SAVEPOINTs to be issued after a transaction has
		// started running. The client txn's statement count indicates how many
		// statements have been executed as part of this transaction.
		if txnState.mu.txn.CommandCount() > 0 {
			return Result{}, errors.Errorf("SAVEPOINT %s needs to be the first statement in a "+
				"transaction", parser.RestartSavepointName)
		}
		// Note that Savepoint doesn't have a corresponding plan node.
		// This here is all the execution there is.
		txnState.retryIntent = true
		return Result{}, nil

	case *parser.RollbackToSavepoint:
		if err := parser.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return Result{}, err
		}
		if !txnState.retryIntent {
			err := fmt.Errorf("SAVEPOINT %s has not been used", parser.RestartSavepointName)
			return Result{}, err
		}
		// If commands have already been sent through the transaction,
		// restart the client txn's proto to increment the epoch. The SQL
		// txn's state is already set to OPEN.
		if txnState.mu.txn.CommandCount() > 0 {
			// TODO(andrei): Should the timestamp below be e.cfg.Clock.Now(), so that
			// the transaction gets a new timestamp?
			txnState.mu.txn.Proto().Restart(
				0 /* userPriority */, 0 /* upgradePriority */, hlc.Timestamp{})
		}
		return Result{}, nil

	case *parser.Prepare:
		// This must be handled here instead of the common path below
		// beause we need to use the Executor reference.
		return Result{}, e.PrepareStmt(session, s)

	case *parser.Execute:
		// Substitute the placeholder information and actual statement with that of
		// the saved prepared statement and pass control back to the ordinary
		// execute path.
		ps, newPInfo, err := getPreparedStatementForExecute(session, s)
		if err != nil {
			return Result{}, err
		}
		pinfo = newPInfo
		stmt.AST = ps.Statement
		stmt.ExpectedTypes = ps.Columns
	}

	var p *planner
	runInParallel := parallelize && !txnState.implicitTxn
	if runInParallel {
		// Create a new planner from the Session to execute the statement, since
		// we're executing in parallel.
		p = session.newPlanner(e, txnState.mu.txn)
	} else {
		// We're not executing in parallel. We can use the cached planner on the
		// session.
		p = &session.planner
		session.resetPlanner(p, e, txnState.mu.txn)
	}
	p.evalCtx.SetTxnTimestamp(txnState.sqlTimestamp)
	p.evalCtx.SetStmtTimestamp(e.cfg.Clock.PhysicalTime())
	p.semaCtx.Placeholders.Assign(pinfo)
	p.avoidCachedDescriptors = avoidCachedDescriptors
	p.phaseTimes[plannerStartExecStmt] = timeutil.Now()
	p.stmt = &stmt
	p.cancelChecker = makeCancelChecker(p.stmt)

	// constantMemAcc accounts for all constant folded values that are computed
	// prior to any rows being computed.
	constantMemAcc := p.evalCtx.Mon.MakeBoundAccount()
	p.evalCtx.ActiveMemAcc = &constantMemAcc
	defer constantMemAcc.Close(session.Ctx())

	var result Result
	if runInParallel {
		// Only run statements asynchronously through the parallelize queue if the
		// statements are parallelized and we're in a transaction. Parallelized
		// statements outside of a transaction are run synchronously with mocked
		// results, which has the same effect as running asynchronously but
		// immediately blocking.
		result, err = e.execStmtInParallel(stmt, p)
	} else {
		p.autoCommit = txnState.implicitTxn && !e.cfg.TestingKnobs.DisableAutoCommit
		result, err = e.execStmt(stmt, p,
			automaticRetryCount, parallelize /* mockResults */)
		// Zeroing the cached planner allows the GC to clean up any memory hanging
		// off the planner, which we're finished using at this point.
	}

	if err != nil {
		if independentFromParallelStmts {
			// If the statement run was independent from parallelized execution, it
			// might have been run concurrently with parallelized statements. Make
			// sure all complete before returning the error.
			_ = session.parallelizeQueue.Wait()
		}

		sessionEventf(session, "ERROR: %v", err)
		return Result{}, err
	}

	tResult := &traceResult{tag: result.PGTag, count: -1}
	switch result.Type {
	case parser.RowsAffected:
		tResult.count = result.RowsAffected
	case parser.Rows:
		tResult.count = result.Rows.Len()
	}
	sessionEventf(session, "%s done", tResult)
	return result, nil
}

// stmtAllowedInImplicitTxn returns whether the statement is allowed in an
// implicit transaction or not.
func stmtAllowedInImplicitTxn(stmt Statement) bool {
	switch stmt.AST.(type) {
	case *parser.CommitTransaction:
	case *parser.ReleaseSavepoint:
	case *parser.RollbackTransaction:
	case *parser.SetTransaction:
	case *parser.Savepoint:
	default:
		return true
	}
	return false
}

// rollbackSQLTransaction rolls back a transaction. All errors are swallowed.
func rollbackSQLTransaction(txnState *txnState) Result {
	if !txnState.TxnIsOpen() && txnState.State() != RestartWait {
		panic(fmt.Sprintf("rollbackSQLTransaction called on txn in wrong state: %s (txn: %s)",
			txnState.State(), txnState.mu.txn.Proto()))
	}
	err := txnState.mu.txn.Rollback(txnState.Ctx)
	result := Result{PGTag: (*parser.RollbackTransaction)(nil).StatementTag()}
	if err != nil {
		log.Warningf(txnState.Ctx, "txn rollback failed. The error was swallowed: %s", err)
		result.Err = err
	}
	// We're done with this txn.
	txnState.resetStateAndTxn(NoTxn)
	return result
}

type commitType int

const (
	commit commitType = iota
	release
)

// commitSqlTransaction commits a transaction.
func commitSQLTransaction(txnState *txnState, commitType commitType) (Result, error) {
	if !txnState.TxnIsOpen() {
		panic(fmt.Sprintf("commitSqlTransaction called on non-open txn: %+v", txnState.mu.txn))
	}
	if commitType == commit {
		txnState.commitSeen = true
	}
	if err := txnState.mu.txn.Commit(txnState.Ctx); err != nil {
		// Errors on COMMIT need special handling: if the errors is not handled by
		// auto-retry, COMMIT needs to finalize the transaction (it can't leave it
		// in Aborted or RestartWait). Higher layers will handle this with the help
		// of `txnState.commitSeen`, set above.
		return Result{}, err
	}

	switch commitType {
	case release:
		// We'll now be waiting for a COMMIT.
		txnState.resetStateAndTxn(CommitWait)
	case commit:
		// We're done with this txn.
		txnState.resetStateAndTxn(NoTxn)
	}
	return Result{PGTag: (*parser.CommitTransaction)(nil).StatementTag()}, nil
}

// exectDistSQL converts a classic plan to a distributed SQL physical plan and
// runs it.
func (e *Executor) execDistSQL(planner *planner, tree planNode, result *Result) error {
	// Note: if we just want the row count, result.Rows is nil here.
	ctx := planner.session.Ctx()
	recv, err := makeDistSQLReceiver(
		ctx, result.Rows,
		e.cfg.RangeDescriptorCache, e.cfg.LeaseHolderCache,
		planner.txn,
		func(ts hlc.Timestamp) {
			_ = e.cfg.Clock.Update(ts)
		},
	)
	if err != nil {
		return err
	}
	err = e.distSQLPlanner.PlanAndRun(ctx, planner.txn, tree, &recv, planner.evalCtx)
	if err != nil {
		return err
	}
	if recv.err != nil {
		return recv.err
	}
	if result.Type == parser.RowsAffected {
		result.RowsAffected = int(recv.numRows)
	}
	return nil
}

// execClassic runs a plan using the classic (non-distributed) SQL
// implementation.
func (e *Executor) execClassic(planner *planner, plan planNode, result *Result) error {
	ctx := planner.session.Ctx()
	rowAcc := planner.evalCtx.Mon.MakeBoundAccount()
	planner.evalCtx.ActiveMemAcc = &rowAcc
	// We enclose this in a func because in the parser.Rows case we swap out the
	// account, so we want to ensure that we close the currently active account at
	// the conclusion of this function.
	defer func() {
		planner.evalCtx.ActiveMemAcc.Close(ctx)
	}()

	if err := planner.startPlan(ctx, plan); err != nil {
		return err
	}

	params := runParams{
		ctx: ctx,
		p:   planner,
	}

	switch result.Type {
	case parser.RowsAffected:
		count, err := countRowsAffected(params, plan)
		if err != nil {
			return err
		}
		result.RowsAffected += count

	case parser.Rows:
		next, err := plan.Next(params)
		for ; next; next, err = plan.Next(params) {
			planner.evalCtx.ActiveMemAcc.Close(ctx)
			rowAcc = planner.evalCtx.Mon.MakeBoundAccount()
			planner.evalCtx.ActiveMemAcc = &rowAcc

			// The plan.Values Datums needs to be copied on each iteration.
			values := plan.Values()

			for _, val := range values {
				if err := checkResultType(val.ResolvedType()); err != nil {
					return err
				}
			}
			if _, err := result.Rows.AddRow(ctx, values); err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
	case parser.DDL:
		if n, ok := plan.(*createTableNode); ok && n.n.As() {
			result.RowsAffected += n.count
		}
	}
	return nil
}

// shouldUseDistSQL determines whether we should use DistSQL for a plan, based
// on the session settings.
func (e *Executor) shouldUseDistSQL(planner *planner, plan planNode) (bool, error) {
	distSQLMode := planner.session.DistSQLMode
	if distSQLMode == cluster.DistSQLOff {
		return false, nil
	}
	// Don't try to run empty nodes (e.g. SET commands) with distSQL.
	if _, ok := plan.(*emptyNode); ok {
		return false, nil
	}

	var err error
	var distribute bool

	// Temporary workaround for #13376: if the transaction wrote something,
	// we can't allow it to do DistSQL reads any more because we can't guarantee
	// that the reads don't happen after the gateway's TxnCoordSender has
	// abandoned the transaction (and so the reads could miss to see their own
	// writes). We detect this by checking if the transaction's "anchor" key is
	// set.
	if planner.txn.AnchorKey() != nil {
		err = errors.New("writing txn")
	} else {
		// Trigger limit propagation.
		setUnlimited(plan)
		distribute, err = e.distSQLPlanner.CheckSupport(plan)
	}

	if err != nil {
		// If the distSQLMode is ALWAYS, any unsupported statement is an error.
		if distSQLMode == cluster.DistSQLAlways {
			return false, err
		}
		// Don't use distSQL for this request.
		log.VEventf(planner.session.Ctx(), 1, "query not supported for distSQL: %s", err)
		return false, nil
	}

	if distSQLMode == cluster.DistSQLAuto && !distribute {
		log.VEventf(planner.session.Ctx(), 1, "not distributing query")
		return false, nil
	}

	// In ON or ALWAYS mode, all supported queries are distributed.
	return true, nil
}

// makeRes creates an empty result set for the given statement and plan.
func makeRes(stmt Statement, planner *planner, plan planNode) (Result, error) {
	result := Result{
		PGTag: stmt.AST.StatementTag(),
		Type:  stmt.AST.StatementType(),
	}
	if result.Type == parser.Rows {
		result.Columns = planColumns(plan)
		for _, c := range result.Columns {
			if err := checkResultType(c.Typ); err != nil {
				return Result{}, err
			}
		}
		result.Rows = sqlbase.NewRowContainer(
			planner.session.makeBoundAccount(), sqlbase.ColTypeInfoFromResCols(result.Columns), 0,
		)
	}
	return result, nil
}

// execStmt executes the statement synchronously and returns the statement's result.
// If mockResults is set, these results will be replaced by the "zero value" of the
// statement's result type, identical to the mock results returned by execStmtInParallel.
func (e *Executor) execStmt(
	stmt Statement, planner *planner, automaticRetryCount int, mockResults bool,
) (Result, error) {
	session := planner.session
	ctx := session.Ctx()

	planner.phaseTimes[plannerStartLogicalPlan] = timeutil.Now()
	plan, err := planner.makePlan(ctx, stmt)
	planner.phaseTimes[plannerEndLogicalPlan] = timeutil.Now()
	if err != nil {
		return Result{}, err
	}

	defer plan.Close(ctx)

	result, err := makeRes(stmt, planner, plan)
	if err != nil {
		return Result{}, err
	}

	useDistSQL, err := e.shouldUseDistSQL(planner, plan)
	if err != nil {
		result.Close(ctx)
		return Result{}, err
	}

	if e.cfg.TestingKnobs.BeforeExecute != nil {
		e.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), false /* isParallel */)
	}

	planner.phaseTimes[plannerStartExecStmt] = timeutil.Now()
	session.setQueryExecutionMode(stmt.queryID, useDistSQL)
	if useDistSQL {
		err = e.execDistSQL(planner, plan, &result)
	} else {
		err = e.execClassic(planner, plan, &result)
	}
	planner.phaseTimes[plannerEndExecStmt] = timeutil.Now()
	e.recordStatementSummary(
		planner, stmt, useDistSQL, automaticRetryCount, result, err,
	)
	if e.cfg.TestingKnobs.AfterExecute != nil {
		e.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), &result, err)
	}
	if err != nil {
		result.Close(ctx)
		return Result{}, err
	}

	if mockResults {
		result.Close(ctx)
		return makeRes(stmt, planner, plan)
	}
	return result, nil
}

// execStmtInParallel executes the statement asynchronously and returns mocked out
// results. These mocked out results will be the "zero value" of the statement's
// result type:
// - parser.Rows -> an empty set of rows
// - parser.RowsAffected -> zero rows affected
//
// TODO(nvanbenschoten): We do not currently support parallelizing distributed SQL
// queries, so this method can only be used with classical SQL.
func (e *Executor) execStmtInParallel(stmt Statement, planner *planner) (Result, error) {
	session := planner.session
	ctx := session.Ctx()

	plan, err := planner.makePlan(ctx, stmt)
	if err != nil {
		return Result{}, err
	}

	mockResult, err := makeRes(stmt, planner, plan)
	if err != nil {
		return Result{}, err
	}

	session.parallelizeQueue.Add(ctx, plan, func(plan planNode) error {
		result, err := makeRes(stmt, planner, plan)
		if err != nil {
			return err
		}
		defer result.Close(ctx)

		if e.cfg.TestingKnobs.BeforeExecute != nil {
			e.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), true /* isParallel */)
		}

		planner.phaseTimes[plannerStartExecStmt] = timeutil.Now()
		err = e.execClassic(planner, plan, &result)
		planner.phaseTimes[plannerEndExecStmt] = timeutil.Now()
		e.recordStatementSummary(planner, stmt, false, 0, result, err)
		if e.cfg.TestingKnobs.AfterExecute != nil {
			e.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), &result, err)
		}
		return err
	})
	return mockResult, nil
}

// updateStmtCounts updates metrics for the number of times the different types of SQL
// statements have been received by this node.
func (e *Executor) updateStmtCounts(stmt Statement) {
	e.QueryCount.Inc(1)
	switch stmt.AST.(type) {
	case *parser.BeginTransaction:
		e.TxnBeginCount.Inc(1)
	case *parser.Select:
		e.SelectCount.Inc(1)
	case *parser.Update:
		e.UpdateCount.Inc(1)
	case *parser.Insert:
		e.InsertCount.Inc(1)
	case *parser.Delete:
		e.DeleteCount.Inc(1)
	case *parser.CommitTransaction:
		e.TxnCommitCount.Inc(1)
	case *parser.RollbackTransaction:
		e.TxnRollbackCount.Inc(1)
	default:
		if stmt.AST.StatementType() == parser.DDL {
			e.DdlCount.Inc(1)
		} else {
			e.MiscCount.Inc(1)
		}
	}
}

// generateQueryID generates a unique ID for a query based on the node's
// ID and its current HLC timestamp.
func (e *Executor) generateQueryID() uint128.Uint128 {
	timestamp := e.cfg.Clock.Now()

	loInt := (uint64)(e.cfg.NodeID.Get())
	loInt = loInt | ((uint64)(timestamp.Logical) << 32)

	return uint128.FromInts((uint64)(timestamp.WallTime), loInt)
}

// golangFillQueryArguments populates the placeholder map with
// types and values from an array of Go values.
// TODO: This does not support arguments of the SQL 'Date' type, as there is not
// an equivalent type in Go's standard library. It's not currently needed by any
// of our internal tables.
func golangFillQueryArguments(pinfo *parser.PlaceholderInfo, args []interface{}) {
	pinfo.Clear()

	for i, arg := range args {
		k := strconv.Itoa(i + 1)
		if arg == nil {
			pinfo.SetValue(k, parser.DNull)
			continue
		}

		// A type switch to handle a few explicit types with special semantics:
		// - Datums are passed along as is.
		// - Time datatypes get special representation in the database.
		var d parser.Datum
		switch t := arg.(type) {
		case parser.Datum:
			d = t
		case time.Time:
			d = parser.MakeDTimestamp(t, time.Microsecond)
		case time.Duration:
			d = &parser.DInterval{Duration: duration.Duration{Nanos: t.Nanoseconds()}}
		case *apd.Decimal:
			dd := &parser.DDecimal{}
			dd.Set(t)
			d = dd
		}
		if d == nil {
			// Handle all types which have an underlying type that can be stored in the
			// database.
			// Note: if this reflection becomes a performance concern in the future,
			// commonly used types could be added explicitly into the type switch above
			// for a performance gain.
			val := reflect.ValueOf(arg)
			switch val.Kind() {
			case reflect.Bool:
				d = parser.MakeDBool(parser.DBool(val.Bool()))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				d = parser.NewDInt(parser.DInt(val.Int()))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				d = parser.NewDInt(parser.DInt(val.Uint()))
			case reflect.Float32, reflect.Float64:
				d = parser.NewDFloat(parser.DFloat(val.Float()))
			case reflect.String:
				d = parser.NewDString(val.String())
			case reflect.Slice:
				// Handle byte slices.
				if val.Type().Elem().Kind() == reflect.Uint8 {
					d = parser.NewDBytes(parser.DBytes(val.Bytes()))
				}
			}
			if d == nil {
				panic(fmt.Sprintf("unexpected type %T", arg))
			}
		}
		pinfo.SetValue(k, d)
	}
}

func checkResultType(typ parser.Type) error {
	// Compare all types that can rely on == equality.
	switch parser.UnwrapType(typ) {
	case parser.TypeNull:
	case parser.TypeBool:
	case parser.TypeInt:
	case parser.TypeFloat:
	case parser.TypeDecimal:
	case parser.TypeBytes:
	case parser.TypeString:
	case parser.TypeDate:
	case parser.TypeTimestamp:
	case parser.TypeTimestampTZ:
	case parser.TypeInterval:
	case parser.TypeUUID:
	case parser.TypeNameArray:
	case parser.TypeOid:
	case parser.TypeRegClass:
	case parser.TypeRegNamespace:
	case parser.TypeRegProc:
	case parser.TypeRegProcedure:
	case parser.TypeRegType:
	default:
		// Compare all types that cannot rely on == equality.
		istype := typ.FamilyEqual
		switch {
		case istype(parser.TypeArray):
			if istype(parser.UnwrapType(typ).(parser.TArray).Typ) {
				return pgerror.Unimplemented("nested arrays", "arrays cannot have arrays as element type")
			}
		case istype(parser.TypeCollatedString):
		case istype(parser.TypeTuple):
		case istype(parser.TypePlaceholder):
			return errors.Errorf("could not determine data type of %s", typ)
		default:
			return errors.Errorf("unsupported result type: %s", typ)
		}
	}
	return nil
}

// EvalAsOfTimestamp evaluates and returns the timestamp from an AS OF SYSTEM
// TIME clause.
func EvalAsOfTimestamp(
	evalCtx *parser.EvalContext, asOf parser.AsOfClause, max hlc.Timestamp,
) (hlc.Timestamp, error) {
	var ts hlc.Timestamp
	te, err := asOf.Expr.TypeCheck(nil, parser.TypeString)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	d, err := te.Eval(evalCtx)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	switch d := d.(type) {
	case *parser.DString:
		s := string(*d)
		// Allow nanosecond precision because the timestamp is only used by the
		// system and won't be returned to the user over pgwire.
		if dt, err := parser.ParseDTimestamp(s, time.Nanosecond); err == nil {
			ts.WallTime = dt.Time.UnixNano()
			break
		}
		// Attempt to parse as a decimal.
		if dec, _, err := apd.NewFromString(s); err == nil {
			if ts, err = decimalToHLC(dec); err == nil {
				break
			}
		}
		return hlc.Timestamp{}, fmt.Errorf("could not parse '%s' as timestamp", s)
	case *parser.DInt:
		ts.WallTime = int64(*d)
	case *parser.DDecimal:
		if ts, err = decimalToHLC(&d.Decimal); err != nil {
			return hlc.Timestamp{}, err
		}
	default:
		return hlc.Timestamp{}, fmt.Errorf("unexpected AS OF SYSTEM TIME argument: %s (%T)", d.ResolvedType(), d)
	}
	if max.Less(ts) {
		return hlc.Timestamp{}, fmt.Errorf("cannot specify timestamp in the future")
	}
	return ts, nil
}

func decimalToHLC(d *apd.Decimal) (hlc.Timestamp, error) {
	// Format the decimal into a string and split on `.` to extract the nanosecond
	// walltime and logical tick parts.
	// TODO(mjibson): use d.Modf() instead of converting to a string.
	s := d.Text('f')
	parts := strings.SplitN(s, ".", 2)
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return hlc.Timestamp{}, errors.Wrap(err, "parse AS OF SYSTEM TIME argument")
	}
	var logical int64
	if len(parts) > 1 {
		// logicalLength is the number of decimal digits expected in the
		// logical part to the right of the decimal. See the implementation of
		// cluster_logical_timestamp().
		const logicalLength = 10
		p := parts[1]
		if lp := len(p); lp > logicalLength {
			return hlc.Timestamp{}, errors.Errorf("bad AS OF SYSTEM TIME argument: logical part has too many digits")
		} else if lp < logicalLength {
			p += strings.Repeat("0", logicalLength-lp)
		}
		logical, err = strconv.ParseInt(p, 10, 32)
		if err != nil {
			return hlc.Timestamp{}, errors.Wrap(err, "parse AS OF SYSTEM TIME argument")
		}
	}
	return hlc.Timestamp{
		WallTime: nanos,
		Logical:  int32(logical),
	}, nil
}

// isAsOf analyzes a select statement to bypass the logic in newPlan(),
// since that requires the transaction to be started already. If the returned
// timestamp is not nil, it is the timestamp to which a transaction should
// be set.
//
// max is a lower bound on what the transaction's timestamp will be. Used to
// check that the user didn't specify a timestamp in the future.
func isAsOf(session *Session, stmt parser.Statement, max hlc.Timestamp) (*hlc.Timestamp, error) {
	s, ok := stmt.(*parser.Select)
	if !ok {
		return nil, nil
	}
	sc, ok := s.Select.(*parser.SelectClause)
	if !ok {
		return nil, nil
	}
	if sc.From == nil || sc.From.AsOf.Expr == nil {
		return nil, nil
	}

	evalCtx := session.evalCtx()
	ts, err := EvalAsOfTimestamp(&evalCtx, sc.From.AsOf, max)
	return &ts, err
}

// isSavepoint returns true if stmt is a SAVEPOINT statement.
func isSavepoint(stmt Statement) bool {
	_, isSavepoint := stmt.AST.(*parser.Savepoint)
	return isSavepoint
}

// isBegin returns true if stmt is a BEGIN statement.
func isBegin(stmt Statement) bool {
	_, isBegin := stmt.AST.(*parser.BeginTransaction)
	return isBegin
}

// isSetTransaction returns true if stmt is a "SET TRANSACTION ..." statement.
func isSetTransaction(stmt Statement) bool {
	_, isSet := stmt.AST.(*parser.SetTransaction)
	return isSet
}

// isRollbackToSavepoint returns true if stmt is a "ROLLBACK TO SAVEPOINT"
// statement.
func isRollbackToSavepoint(stmt Statement) bool {
	_, isSet := stmt.AST.(*parser.RollbackToSavepoint)
	return isSet
}

// canStayInFirstBatchState returns true if the statement can leave the
// transaction in the FirstBatch state (as opposed to transitioning it to Open).
//
// Only statements that affect the transaction state in such a way such that a
// restart would not destroy the state can currently be added here; we don't
// replay past batches in case of a restart. So, for example, RETURNING NOTHING
// statements can't be put here without adding some replay capability.
// TODO(andrei): support RETURNING NOTHING statements.
func canStayInFirstBatchState(stmt Statement) bool {
	return isBegin(stmt) ||
		isSavepoint(stmt) ||
		isSetTransaction(stmt) ||
		// ROLLBACK TO SAVEPOINT does its own state transitions; if it leaves the
		// transaction in the FirstBatch state, don't mess with it.
		isRollbackToSavepoint(stmt)
}

// convertToErrWithPGCode recognizes errs that should have SQL error codes to be
// reported to the client and converts err to them. If this doesn't apply, err
// is returned.
// Note that this returns a new error, and details from the original error are
// not preserved in any way (except possibly the message).
//
// TODO(andrei): sqlbase.ConvertBatchError() seems to serve similar purposes, but
// it's called from more specialized contexts. Consider unifying the two.
func convertToErrWithPGCode(err error) error {
	if err == nil {
		return nil
	}
	switch tErr := err.(type) {
	case *roachpb.HandledRetryableTxnError:
		return sqlbase.NewRetryError(err)
	case *roachpb.AmbiguousResultError:
		// TODO(andrei): Once DistSQL starts executing writes, we'll need a
		// different mechanism to marshal AmbiguousResultErrors from the executing
		// nodes.
		return sqlbase.NewStatementCompletionUnknownError(tErr)
	default:
		return err
	}
}
