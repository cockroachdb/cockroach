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
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// ClusterOrganization is the organization name.
var ClusterOrganization = settings.RegisterStringSetting(
	"cluster.organization",
	"organization name",
	"",
)

// ClusterSecret is a cluster specific secret. This setting is hidden.
var ClusterSecret = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		"cluster.secret",
		"cluster specific secret",
		"",
	)
	s.Hide()
	return s
}()

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errTransactionInProgress = errors.New("there is already a transaction in progress")

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
		Help: "Latency in nanoseconds of SQL statement execution"}
	MetaSQLServiceLatency = metric.Metadata{
		Name: "sql.service.latency",
		Help: "Latency in nanoseconds of SQL request execution"}
	MetaDistSQLSelect = metric.Metadata{
		Name: "sql.distsql.select.count",
		Help: "Number of DistSQL SELECT statements"}
	MetaDistSQLExecLatency = metric.Metadata{
		Name: "sql.distsql.exec.latency",
		Help: "Latency in nanoseconds of DistSQL statement execution"}
	MetaDistSQLServiceLatency = metric.Metadata{
		Name: "sql.distsql.service.latency",
		Help: "Latency in nanoseconds of DistSQL request execution"}
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
	Type tree.StatementType
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
	cfg     ExecutorConfig
	stopper *stop.Stopper
	reCache *tree.RegexpCache

	// Transient stats.
	SelectCount   *metric.Counter
	TxnBeginCount *metric.Counter
	EngineMetrics EngineMetrics

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

	dbCache *databaseCacheHolder

	distSQLPlanner *DistSQLPlanner

	// Application-level SQL statistics
	sqlStats sqlStats

	// Attempts to use unimplemented features.
	errorCounts struct {
		syncutil.Mutex
		unimplemented map[string]int64
		codes         map[string]int64
	}
}

// NodeInfo contains metadata about the executing node and cluster.
type NodeInfo struct {
	ClusterID func() uuid.UUID
	NodeID    *base.NodeIDContainer
	AdminURL  func() *url.URL
	PGURL     func(*url.Userinfo) (*url.URL, error)
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
	JobRegistry     *jobs.Registry
	VirtualSchemas  *VirtualSchemaHolder
	DistSQLPlanner  *DistSQLPlanner
	TableStatsCache *stats.TableStatisticsCache
	ExecLogger      *log.SecondaryLogger
	AuditLogger     *log.SecondaryLogger

	TestingKnobs              *ExecutorTestingKnobs
	SchemaChangerTestingKnobs *SchemaChangerTestingKnobs
	EvalContextTestingKnobs   tree.EvalContextTestingKnobs
	// HistogramWindowInterval is (server.Config).HistogramWindowInterval.
	HistogramWindowInterval time.Duration

	// Caches updated by DistSQL.
	RangeDescriptorCache *kv.RangeDescriptorCache
	LeaseHolderCache     *kv.LeaseHolderCache

	// ConnResultsBufferBytes is the size of the buffer in which each connection
	// accumulates results set. Results are flushed to the network when this
	// buffer overflows.
	ConnResultsBufferBytes int
}

// Organization returns the value of cluster.organization.
func (ec *ExecutorConfig) Organization() string {
	return ClusterOrganization.Get(&ec.Settings.SV)
}

var _ base.ModuleTestingKnobs = &ExecutorTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ExecutorTestingKnobs) ModuleTestingKnobs() {}

// StatementFilter is the type of callback that
// ExecutorTestingKnobs.StatementFilter takes.
type StatementFilter func(context.Context, string, error)

// ExecutorTestingKnobs is part of the context used to control parts of the
// system during testing.
type ExecutorTestingKnobs struct {
	// CheckStmtStringChange causes Executor.execStmtGroup to verify that executed
	// statements are not modified during execution.
	CheckStmtStringChange bool

	// StatementFilter can be used to trap execution of SQL statements and
	// optionally change their results. The filter function is invoked after each
	// statement has been executed.
	StatementFilter StatementFilter

	// BeforeExecute is called by the Executor before plan execution. It is useful
	// for synchronizing statement execution, such as with parallel statemets.
	BeforeExecute func(ctx context.Context, stmt string, isParallel bool)

	// AfterExecute is like StatementFilter, but it runs in the same goroutine of the
	// statement.
	AfterExecute func(ctx context.Context, stmt string, err error)

	// DisableAutoCommit, if set, disables the auto-commit functionality of some
	// SQL statements. That functionality allows some statements to commit
	// directly when they're executed in an implicit SQL txn, without waiting for
	// the Executor to commit the implicit txn.
	// This has to be set in tests that need to abort such statements using a
	// StatementFilter; otherwise, the statement commits immediately after
	// execution so there'll be nothing left to abort by the time the filter runs.
	DisableAutoCommit bool

	// DistSQLPlannerKnobs are testing knobs for DistSQLPlanner.
	DistSQLPlannerKnobs DistSQLPlannerTestingKnobs

	// BeforeAutoCommit is called when the Executor is about to commit the KV
	// transaction after running a statement in an implicit transaction, allowing
	// tests to inject errors into that commit.
	// If an error is returned, that error will be considered the result of
	// txn.Commit(), and the txn.Commit() call will not actually be
	// made. If no error is returned, txn.Commit() is called normally.
	//
	// Note that this is not called if the SQL statement representing the implicit
	// transaction has committed the KV txn itself (e.g. if it used the 1-PC
	// optimization). This is only called when the Executor is the one doing the
	// committing.
	BeforeAutoCommit func(ctx context.Context, stmt string) error
}

// DistSQLPlannerTestingKnobs is used to control internals of the DistSQLPlanner
// for testing purposes.
type DistSQLPlannerTestingKnobs struct {
	// If OverrideSQLHealthCheck is set, we use this callback to get the health of
	// a node.
	OverrideHealthCheck func(node roachpb.NodeID, addrString string) error
}

// NewExecutor creates an Executor and registers a callback on the
// system config.
func NewExecutor(cfg ExecutorConfig, stopper *stop.Stopper) *Executor {
	return &Executor{
		cfg:     cfg,
		stopper: stopper,
		reCache: tree.NewRegexpCache(512),

		TxnBeginCount:    metric.NewCounter(MetaTxnBegin),
		TxnCommitCount:   metric.NewCounter(MetaTxnCommit),
		TxnAbortCount:    metric.NewCounter(MetaTxnAbort),
		TxnRollbackCount: metric.NewCounter(MetaTxnRollback),
		SelectCount:      metric.NewCounter(MetaSelect),
		EngineMetrics: EngineMetrics{
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
		},
		UpdateCount: metric.NewCounter(MetaUpdate),
		InsertCount: metric.NewCounter(MetaInsert),
		DeleteCount: metric.NewCounter(MetaDelete),
		DdlCount:    metric.NewCounter(MetaDdl),
		MiscCount:   metric.NewCounter(MetaMisc),
		QueryCount:  metric.NewCounter(MetaQuery),
		sqlStats:    sqlStats{st: cfg.Settings, apps: make(map[string]*appStats)},
		// The cache will be updated on Start() through gossip.
		dbCache: newDatabaseCacheHolder(newDatabaseCache(config.SystemConfig{})),
	}
}

// Start starts workers for the executor.
func (e *Executor) Start(ctx context.Context, dsp *DistSQLPlanner) {
	ctx = e.AnnotateCtx(ctx)
	e.distSQLPlanner = dsp

	gossipUpdateC := e.cfg.Gossip.RegisterSystemConfigChannel()
	e.stopper.RunWorker(ctx, func(ctx context.Context) {
		for {
			select {
			case <-gossipUpdateC:
				sysCfg, _ := e.cfg.Gossip.GetSystemConfig()
				e.dbCache.updateSystemConfig(sysCfg)
			case <-e.stopper.ShouldStop():
				return
			}
		}
	})
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

// databaseCacheHolder is a thread-safe container for a *databaseCache.
// It also allows clients to block until the cache is updated to a desired
// state.
//
// NOTE(andrei): The way in which we handle the database cache is funky: there's
// this top-level holder, which gets updated on gossip updates. Then, each
// session gets its *databaseCache, which is updated from the holder after every
// transaction - the SystemConfig is updated and the lazily computer map of db
// names to ids is wiped. So many session are sharing and contending on a
// mutable cache, but nobody's sharing this holder. We should make up our mind
// about whether we like the sharing or not and, if we do, share the holder too.
// Also, we could use the SystemConfigDeltaFilter to limit the updates to
// databases that chaged.
type databaseCacheHolder struct {
	mu struct {
		syncutil.Mutex
		c  *databaseCache
		cv *sync.Cond
	}
}

func newDatabaseCacheHolder(c *databaseCache) *databaseCacheHolder {
	dc := &databaseCacheHolder{}
	dc.mu.c = c
	dc.mu.cv = sync.NewCond(&dc.mu.Mutex)
	return dc
}

func (dc *databaseCacheHolder) getDatabaseCache() *databaseCache {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.mu.c
}

// waitForCacheState implements the dbCacheSubscriber interface.
func (dc *databaseCacheHolder) waitForCacheState(cond func(*databaseCache) (bool, error)) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for {
		done, err := cond(dc.mu.c)
		if err != nil {
			return err
		}
		if done {
			break
		}
		dc.mu.cv.Wait()
	}
	return nil
}

// databaseCacheHolder implements the dbCacheSubscriber interface.
var _ dbCacheSubscriber = &databaseCacheHolder{}

// updateSystemConfig is called whenever a new system config gossip entry is
// received.
func (dc *databaseCacheHolder) updateSystemConfig(cfg config.SystemConfig) {
	dc.mu.Lock()
	dc.mu.c = newDatabaseCache(cfg)
	dc.mu.cv.Broadcast()
	dc.mu.Unlock()
}

// Prepare returns the result types of the given statement. placeholderHints may
// contain partial type information for placeholders. Prepare will populate the
// missing types. The PreparedStatement is returned (or nil if there are no
// results).
func (e *Executor) Prepare(
	stmt Statement, stmtStr string, session *Session, placeholderHints tree.PlaceholderTypes,
) (res *PreparedStatement, err error) {
	session.resetForBatch(e)
	sessionEventf(session, "preparing: %s", stmtStr)

	defer session.maybeRecover("preparing", stmtStr)

	prepared := &PreparedStatement{
		TypeHints:   placeholderHints,
		portalNames: make(map[string]struct{}),
		memAcc:      session.sessionMon.MakeBoundAccount(),
	}

	if stmt.AST == nil {
		return prepared, nil
	}

	prepared.Statement = stmt.AST
	prepared.AnonymizedStr = anonymizeStmt(stmt)

	if err := placeholderHints.ProcessPlaceholderAnnotations(stmt.AST); err != nil {
		return nil, err
	}
	evalCtx := session.extendedEvalCtx(
		session.TxnState.mu.txn,
		e.cfg.Clock.PhysicalTime(),
		e.cfg.Clock.PhysicalTime()).EvalContext
	protoTS, err := isAsOf(stmt.AST, &evalCtx, e.cfg.Clock.Now())
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
		txn = client.NewTxn(e.cfg.DB, e.cfg.NodeID.Get(), client.RootTxn)
		if err := txn.SetIsolation(session.data.DefaultIsolationLevel); err != nil {
			panic(fmt.Errorf("cannot set up txn for prepare %q: %v", stmtStr, err))
		}
		txn.Proto().OrigTimestamp = e.cfg.Clock.Now()
	}

	planner := session.newPlanner(
		txn,
		session.TxnState.sqlTimestamp,
		e.cfg.Clock.PhysicalTime(), /* stmtTimestamp */
		e.reCache,
		session.statsCollector(),
	)
	planner.semaCtx.Placeholders.SetTypeHints(placeholderHints)
	planner.extendedEvalCtx.PrepareOnly = true
	planner.extendedEvalCtx.ActiveMemAcc = &prepared.memAcc

	ctx := session.Ctx()
	if protoTS != nil {
		planner.asOfSystemTime = true
		// We can't use cached descriptors anywhere in this query, because
		// we want the descriptors at the timestamp given, not the latest
		// known to the cache.
		planner.avoidCachedDescriptors = true
		txn.SetFixedTimestamp(ctx, *protoTS)
	}

	startTime := timeutil.Now()
	if err := planner.prepare(ctx, stmt.AST); err != nil {
		planner.maybeLogStatementInternal(ctx, "prepare", 0, err, startTime)
		return nil, err
	}
	planner.maybeLogStatementInternal(ctx, "prepare", 0, nil, startTime)

	if planner.curPlan.plan == nil {
		// The statement cannot be prepared. Nothing to do.
		return prepared, nil
	}
	defer func() {
		planner.curPlan.close(ctx)
		// NB: if we start caching the plan, we'll want to keep around the memory
		// account used for the plan, rather than clearing it.
		prepared.memAcc.Clear(ctx)
	}()
	prepared.Columns = planner.curPlan.columns()
	for _, c := range prepared.Columns {
		if err := checkResultType(c.Typ); err != nil {
			return nil, err
		}
	}
	prepared.Types = planner.semaCtx.Placeholders.Types
	return prepared, nil
}

// ExecuteStatementsBuffered executes the given statement(s), buffering them
// entirely in memory prior to returning a response. If there is an error then
// we return an empty StatementResults and the error.
//
// Note that we will only receive an error even if we run a successful statement
// followed by a statement which has an error then the caller will only receive
// the error, however the first statement will have been executed.
//
// If no error is returned, the caller has to call Close() on the returned
// StatementResults.
func (e *Executor) ExecuteStatementsBuffered(
	session *Session, stmts string, pinfo *tree.PlaceholderInfo, expectedNumResults int,
) (StatementResults, error) {
	b := newBufferedWriter(session.makeBoundAccount())
	session.ResultsWriter = b
	err := e.ExecuteStatements(session, stmts, pinfo)
	res := b.results()
	if err != nil {
		res.Close(session.Ctx())
		return StatementResults{}, err
	}
	for _, result := range res.ResultList {
		if result.Err != nil {
			res.Close(session.Ctx())
			return StatementResults{}, errors.Errorf("%s", result.Err)
		}
	}
	// This needs to be the last error check since in the case of an error during
	// execution this would swallow the true error.
	if a, e := len(res.ResultList), expectedNumResults; a != e {
		res.Close(session.Ctx())
		return StatementResults{}, errors.Errorf("number of results %d != expected %d", a, e)
	}
	return res, nil
}

// ExecuteStatements executes the given statement(s).
func (e *Executor) ExecuteStatements(
	session *Session, stmts string, pinfo *tree.PlaceholderInfo,
) error {
	session.resetForBatch(e)

	defer session.maybeRecover("executing", stmts)

	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	return e.execRequest(session, stmts, pinfo)
}

// ExecutePreparedStatement executes the given statement and returns a response.
func (e *Executor) ExecutePreparedStatement(
	session *Session, stmt *PreparedStatement, pinfo *tree.PlaceholderInfo,
) error {
	defer session.maybeRecover("executing", stmt.Str)

	{
		// No parsing is taking place, but we need to set the parsing phase time
		// because the service latency is measured from
		// phaseTimes[sessionStartParse].
		now := timeutil.Now()
		session.phaseTimes[sessionQueryReceived] = now
		session.phaseTimes[sessionStartParse] = now
		session.phaseTimes[sessionEndParse] = now
	}

	return e.execPrepared(session, stmt, pinfo)
}

// execPrepared executes a prepared statement. It returns an error if there
// is more than 1 result or the returned types differ from the prepared
// return types.
func (e *Executor) execPrepared(
	session *Session, stmt *PreparedStatement, pinfo *tree.PlaceholderInfo,
) error {
	var stmts StatementList
	if stmt.Statement != nil {
		stmts = StatementList{{
			AST:           stmt.Statement,
			ExpectedTypes: stmt.Columns,
			AnonymizedStr: stmt.AnonymizedStr,
		}}
	}
	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	return e.execParsed(session, stmts, pinfo)
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
func (e *Executor) execRequest(session *Session, sql string, pinfo *tree.PlaceholderInfo) error {
	txnState := &session.TxnState

	now := timeutil.Now()
	session.phaseTimes[sessionQueryReceived] = now
	session.phaseTimes[sessionStartParse] = now
	sl, err := parser.Parse(sql)
	stmts := NewStatementList(sl)
	session.phaseTimes[sessionEndParse] = timeutil.Now()

	if err != nil {
		// A parse error occurred: we can't determine if there were multiple
		// statements or only one, so just pretend there was one.
		if txnState.mu.txn != nil {
			// Rollback the txn.
			err = txnState.updateStateAndCleanupOnErr(err, e)
		}
		return err
	}
	return e.execParsed(session, stmts, pinfo)
}

// RecordError is called by the common error handling routine in pgwire. Since
// all pgwire errors are returned via a single helper, this is easier than
// trying to log errors in the executor itself before they're returned.
func (e *Executor) RecordError(err error) {
	if err == nil {
		return
	}
	e.errorCounts.Lock()

	if e.errorCounts.codes == nil {
		e.errorCounts.codes = make(map[string]int64)
	}
	if pgErr, ok := pgerror.GetPGCause(err); ok {
		e.errorCounts.codes[pgErr.Code]++

		if pgErr.Code == pgerror.CodeFeatureNotSupportedError {
			if feature := pgErr.InternalCommand; feature != "" {
				if e.errorCounts.unimplemented == nil {
					e.errorCounts.unimplemented = make(map[string]int64)
				}
				e.errorCounts.unimplemented[feature]++
			}
		}
	} else {
		typ := util.ErrorSource(err)
		if typ == "" {
			typ = "unknown"
		}
		e.errorCounts.codes[typ]++
	}
	e.errorCounts.Unlock()
}

// execParsed executes a batch of statements received as a unit from the client
// and returns query execution errors and communication errors.
func (e *Executor) execParsed(
	session *Session, stmts StatementList, pinfo *tree.PlaceholderInfo,
) error {
	var avoidCachedDescriptors bool
	var asOfSystemTime bool
	txnState := &session.TxnState
	resultWriter := session.ResultsWriter

	if len(stmts) == 0 {
		resultWriter.SetEmptyQuery()
		return nil
	}

	for len(stmts) > 0 {
		// Each iteration consumes a transaction's worth of statements. Any error
		// that is encountered resets stmts.

		inTxn := txnState.State() != NoTxn
		// Figure out the statements out of which we're going to try to consume
		// this iteration. If we need to create an implicit txn, only one statement
		// can be consumed.
		stmtsToExec := stmts
		// If protoTS is set, the transaction proto sets its Orig and Max timestamps
		// to it each retry.
		var protoTS *hlc.Timestamp

		// autoCommit will be set if we're now starting an implicit transaction and
		// thus should automatically commit after we've run the statement.
		autoCommit := false

		// If we're not in a transaction, then the next statement is part of a new
		// transaction (implicit txn or explicit txn). We do the corresponding state
		// reset.
		if !inTxn {
			// Detect implicit transactions - they need to be autocommitted.
			if _, isBegin := stmts[0].AST.(*tree.BeginTransaction); !isBegin {
				autoCommit = true
				stmtsToExec = stmtsToExec[:1]
				// Check for AS OF SYSTEM TIME. If it is present but not detected here,
				// it will raise an error later on.
				var err error
				evalCtx := session.extendedEvalCtx(
					session.TxnState.mu.txn,
					e.cfg.Clock.PhysicalTime(),
					e.cfg.Clock.PhysicalTime()).EvalContext
				protoTS, err = isAsOf(
					stmtsToExec[0].AST, &evalCtx, e.cfg.Clock.Now(),
				)
				if err != nil {
					return err
				}
				if protoTS != nil {
					asOfSystemTime = true
					// When running AS OF SYSTEM TIME queries, we want to use the
					// table descriptors from the specified time, and never lease
					// anything. To do this, we pass down the avoidCachedDescriptors
					// flag and set the transaction's timestamp to the specified time.
					avoidCachedDescriptors = true
				}
			}
			txnState.resetForNewSQLTxn(
				e, session,
				autoCommit, /* implicitTxn */
				false,      /* retryIntent */
				e.cfg.Clock.PhysicalTime(), /* sqlTimestamp */
				session.data.DefaultIsolationLevel,
				roachpb.NormalUserPriority,
				session.data.DefaultReadOnly,
			)
		}

		if txnState.State() == NoTxn {
			panic("we failed to initialize a txn")
		}

		var err error
		var remainingStmts StatementList
		var transitionToOpen bool
		remainingStmts, transitionToOpen, err = runWithAutoRetry(
			e, session, stmtsToExec, !inTxn /* txnPrefix */, autoCommit,
			protoTS, pinfo, asOfSystemTime, avoidCachedDescriptors,
		)
		if autoCommit && txnState.State() != NoTxn {
			log.Fatalf(session.Ctx(), "after an implicit txn, state should always be NoTxn, but found: %s",
				txnState.State())
		}

		// If we've been told that we should move to Open, do it now.
		if err == nil && (txnState.State() == AutoRetry) && transitionToOpen {
			txnState.SetState(Open)
		}

		if err != nil && (log.V(2) || logStatementsExecuteEnabled.Get(&e.cfg.Settings.SV)) {
			log.Infof(session.Ctx(), "execParsed: error: %v. state: %s", err, txnState.State())
		}

		// Sanity check about not leaving KV txns open on errors (other than
		// retriable errors).
		if err != nil && txnState.mu.txn != nil && !txnState.mu.txn.IsFinalized() {
			if _, retryable := err.(*roachpb.HandledRetryableTxnError); !retryable {
				log.Fatalf(session.Ctx(), "got a non-retryable error but the KV "+
					"transaction is not finalized. TxnState: %s, err: %s\n"+
					"err:%+v\n\ntxn: %s", txnState.State(), err, err, txnState.mu.txn.Proto())
			}
		}

		if txnState.commitSeen && txnState.State() == Aborted {
			// A COMMIT got an error (retryable or not); we'll move to state NoTxn.
			// After we return a result for COMMIT (with the COMMIT pgwire tag), the
			// user can't send any more commands.
			txnState.resetStateAndTxn(NoTxn)
		}

		// If we're no longer in a transaction, close the transaction-scoped
		// resources.
		if txnState.State() == NoTxn {
			txnState.finishSQLTxn(session)
		}

		// If we're no longer in a transaction, exec the schema changes.
		// They'll short-circuit themselves if the mutation that queued
		// them has been rolled back from the table descriptor. (It's
		// important that this condition match the one used to call
		// finishSQLTxn above:
		if txnState.State() == NoTxn {
			blockOpt := blockForDBCacheUpdate
			if err != nil {
				blockOpt = dontBlockForDBCacheUpdate
			}
			// Release any leases the transaction(s) may have used.
			if err := session.tables.releaseTables(session.Ctx(), blockOpt); err != nil {
				return err
			}

			// Exec the schema changers (if the txn rolled back, the schema changers
			// will short-circuit because the corresponding descriptor mutation is not
			// found).
			if err := txnState.schemaChangers.execSchemaChanges(session.Ctx(), &e.cfg); err != nil {
				return err
			}
		}

		// Figure out what statements to run on the next iteration.
		if err != nil {
			return convertToErrWithPGCode(err)
		} else if autoCommit {
			stmts = stmts[1:]
		} else {
			stmts = remainingStmts
		}
	}

	return nil
}

// runWithAutoRetry runs a prefix of stmtsToExec corresponding to the current
// transaction. It deals with auto-retries: when possible, the statements are
// retried in case of retriable errors. It also deals with "autoCommit" - if the
// current transaction only consists of a single statement (i.e. it's an
// "implicit txn"), then this function deal with committing the transaction and
// possibly retrying it if the commit gets a retriable error.
//
// Args:
// stmtsToExec: A prefix of these will be executed. The remaining ones will be
//   returned as remainingStmts.
// txnPrefix: Set if stmtsToExec corresponds to the start of the current
//   transaction. Used to trap nested BEGINs.
// autoCommit: If set, the transaction will be committed after running the
//   statement. If set, stmtsToExec can only contain a single statement.
//   If set, the transaction state will always be NoTxn when this function
//   returns, regardless of errors.
//   Errors encountered when committing are reported to the caller and are
//   indistinguishable from errors encountered while running the query.
// protoTS: If not nil, the transaction proto sets its Orig and Max timestamps
//   to it each retry.
//
// Returns:
// remainingStmts: all the statements that were not executed.
// transitionToOpen: specifies if the caller should move from state AutoRetry to
//   state Open. This will be false if the state is not AutoRetry when this
//   returns.
// err: An error that occurred while executing the queries.
func runWithAutoRetry(
	e *Executor,
	session *Session,
	stmtsToExec StatementList,
	txnPrefix bool,
	autoCommit bool,
	protoTS *hlc.Timestamp,
	pinfo *tree.PlaceholderInfo,
	asOfSystemTime bool,
	avoidCachedDescriptors bool,
) (remainingStmts StatementList, transitionToOpen bool, _ error) {

	if autoCommit && !txnPrefix {
		log.Fatal(session.Ctx(), "autoCommit implies txnPrefix. "+
			"How could the transaction have been started before an implicit txn?")
	}
	if autoCommit && len(stmtsToExec) != 1 {
		log.Fatal(session.Ctx(), "Only one statement should be executed when "+
			"autoCommit is set. stmtsToExec: %s", stmtsToExec)
	}

	txnState := &session.TxnState
	origState := txnState.State()

	// Whether or not we can do auto-retries depends on the state before we
	// execute statements in the batch.
	//
	// TODO(andrei): It's unfortunate that we're keeping track of what the state
	// was before running the statements. It'd be great if the state in which we
	// find ourselves after running the statements told us if we can auto-retry.
	// A way to do that is to introduce another state called AutoRestartWait,
	// similar to RestartWait. We'd enter this state whenever we're in state
	// AutoRetry and we get a retriable error.
	txnCanBeAutoRetried := txnState.State() == AutoRetry
	if autoCommit && !txnCanBeAutoRetried {
		log.Fatalf(session.Ctx(), "autoCommit is only supported in the AutoRetry state. "+
			"Current state: %s", txnState.State())
	}

	// Track if we are retrying this query, so that we do not double count.
	automaticRetryCount := 0

	var err error
	for {
		if protoTS != nil {
			txnState.mu.txn.SetFixedTimestamp(session.Ctx(), *protoTS)
		}
		// Some results may have been produced by a previous attempt.
		txnState.txnResults.Reset(session.Ctx())

		// Run some statements.
		remainingStmts, transitionToOpen, err = runTxnAttempt(
			e, session, stmtsToExec, pinfo, origState, txnPrefix, autoCommit,
			asOfSystemTime, avoidCachedDescriptors, automaticRetryCount, txnState.txnResults)

		// Sanity checks.
		if err != nil && txnState.TxnIsOpen() {
			log.Fatalf(session.Ctx(), "statement(s) generated error but state is %s. err: %s",
				txnState.State(), err)
		} else {
			state := txnState.State()
			if err == nil && (state == Aborted || state == RestartWait) {
				crash := true
				// Observer statements are always allowed regardless of
				// the transaction state.
				if len(stmtsToExec) > 0 {
					if _, ok := stmtsToExec[0].AST.(tree.ObserverStatement); ok {
						crash = false
					}
				}
				if crash {
					log.Fatalf(session.Ctx(), "no error, but we're in error state: %s", state)
				}
			}
		}

		if autoCommit {
			// After autoCommit, unless we're in RestartWait, we leave the transaction
			// in NoTxn, regardless of whether we executed the query successfully or
			// we encountered an error.
			if txnState.State() != RestartWait {
				txnState.resetStateAndTxn(NoTxn)
			}
		}

		if _, ok := err.(*roachpb.UnhandledRetryableError); ok {
			// We sent transactional requests, so the TxnCoordSender was supposed to
			// turn retryable errors into HandledRetryableTxnError.
			log.Fatalf(session.Ctx(), "unexpected UnhandledRetryableError at the Executor level: %s", err)
		}

		resultsSentToClient := txnState.txnResults.ResultsSentToClient()
		shouldAutoRetry := txnState.State() == RestartWait && txnCanBeAutoRetried
		if shouldAutoRetry && resultsSentToClient {
			shouldAutoRetry = false
			// We otherwise can and should auto-retry, but, alas, we've already sent
			// some results to the client, so we can no longer auto-retry. We only
			// stay in RestartWait if the client is doing client-directed retries.
			// Otherwise, we move to Aborted.
			if !txnState.retryIntent {
				e.TxnAbortCount.Inc(1)
				txnState.mu.txn.CleanupOnError(session.Ctx(), err)
				if autoCommit {
					// autoCommit always leaves the transaction in NoTxn.
					txnState.resetStateAndTxn(NoTxn)
				} else {
					txnState.resetStateAndTxn(Aborted)
				}
			}
		}

		if !shouldAutoRetry {
			break
		}
		txnState.mu.txn.PrepareForRetry(session.Ctx(), err)
		automaticRetryCount++
	}
	return remainingStmts, transitionToOpen, err
}

// runTxnAttempt takes in a batch of statements and executes a prefix of them
// corresponding to one transaction. It is called multiple times with the same
// set of statements when performing auto-retries.
//
// More technically, it run statements one by one until either:
// - a statement returns an error
// - the end of the the batch is reached
// - a statement transition the state to a non-"open" one, meaning that we're
// not in a transaction any more (or we are in a transaction but in an error
// state, although this also triggers the error condition above).
//
// Args:
// txnPrefix: set if the start of the batch corresponds to the start of the
//   current transaction. Used to trap nested BEGINs.
// autoCommit: If set, the transaction will be committed after running the
//   statement.
// txnResults: used to push query results.
//
// It returns:
// remainingStmts: all the statements that were not executed.
// transitionToOpen: specifies if the caller should move from state AutoRetry to
//   state Open. This will be false if the state is not AutoRetry when this
//   returns.
func runTxnAttempt(
	e *Executor,
	session *Session,
	stmts StatementList,
	pinfo *tree.PlaceholderInfo,
	origState TxnStateEnum,
	txnPrefix bool,
	autoCommit bool,
	asOfSystemTime bool,
	avoidCachedDescriptors bool,
	automaticRetryCount int,
	txnResults ResultsGroup,
) (remainingStmts StatementList, transitionToOpen bool, _ error) {

	txnState := &session.TxnState

	// Ignore the state that might have been set by a previous try of this
	// closure. By putting these modifications to txnState behind the
	// automaticRetryCount condition, we guarantee that no asynchronous
	// statements are still executing and reading from the state. This
	// means that no synchronization is necessary to prevent data races.
	if automaticRetryCount > 0 {
		txnState.SetState(origState)
		txnState.commitSeen = false
	}

	// Keep track of whether we've seen any statement that will force us to
	// transition from AutoRetry to Open at the end of the batch (assuming we are
	// currently in AutoRetry and we get to the end of the batch, both of which
	// may not hold).
	encounteredNonAutoRetryStmt := false
	var i int
	var stmt Statement
	for i, stmt = range stmts {
		encounteredNonAutoRetryStmt = encounteredNonAutoRetryStmt || !canStayInAutoRetryState(stmt)

		if log.V(2) || logStatementsExecuteEnabled.Get(&e.cfg.Settings.SV) ||
			log.HasSpanOrEvent(session.Ctx()) {
			log.VEventf(session.Ctx(), 2, "executing %d/%d: %s", i+1, len(stmts), stmt)
		}

		// Create a StatementResult to receive the results of the statement that
		// we're about to run. e.execStmt() will take ownership and close the
		// result.
		stmtResult := txnResults.NewStatementResult()
		if err := e.execSingleStatement(
			session, stmt, pinfo,
			txnPrefix && i == 0, /* firstInTxn */
			asOfSystemTime, avoidCachedDescriptors, automaticRetryCount, stmtResult,
		); err != nil {
			return nil, false, err
		}

		// If the transaction is done, stop executing more statements.
		if !txnState.TxnIsOpen() {
			break
		}
	}

	// Check if we need to auto-commit. If so, we end the transaction now; the
	// transaction was only supposed to exist for the statement that we just
	// ran. This needs to happen before the txnResults Flush below.
	if autoCommit {
		txn := txnState.mu.txn
		if txn == nil {
			log.Fatalf(session.Ctx(), "implicit txn returned with no error and yet "+
				"the kv txn is gone. No state transition should have done that. State: %s",
				txnState.State())
		}

		// We were told to autoCommit. The KV txn might already be committed
		// (planNodes are free to do that when running an implicit transaction,
		// and some try to do it to take advantage of 1-PC txns). If it is, then
		// there's nothing to do. If it isn't, then we commit it here.
		//
		// NOTE(andrei): It bothers me some that we're peeking at txn to figure
		// out whether we committed or not, where SQL could already know that -
		// individual statements could report this back.
		if txn.IsAborted() {
			log.Fatalf(session.Ctx(), "#7881: the statement we just ran didn't generate an error "+
				"but the txn proto is aborted. This should never happen. txn: %+v",
				txn)
		}

		if !txn.IsCommitted() {
			if filter := e.cfg.TestingKnobs.BeforeAutoCommit; filter != nil {
				if err := filter(session.Ctx(), stmts[0].String()); err != nil {
					err = txnState.updateStateAndCleanupOnErr(err, e)
					return nil, false, err
				}
			}
			err := txn.Commit(session.Ctx())
			log.Eventf(session.Ctx(), "AutoCommit. err: %v\ntxn: %+v", err, txn.Proto())
			if err != nil {
				err = txnState.updateStateAndCleanupOnErr(err, e)
				return nil, false, err
			}
		}
	}

	// Flush the results accumulated in AutoRetry. We want possible future
	// Reset()s to not discard them since we're not going to retry the
	// statements. We also want future ResultsSentToClient() calls to return
	// false.
	if err := txnState.txnResults.Flush(session.Ctx()); err != nil {
		// If there's a KV txn open, we have to roll it back.
		// If there isn't, there's nothing to do. The state of the session doesn't
		// matter any more after a communication error.
		if txnState.state.kvTxnIsOpen() {
			err = txnState.updateStateAndCleanupOnErr(err, e)
		}
		return nil, false, err
	}

	// If we're still in AutoRetry but we've seen statements that would need to be
	// retried if we'd later want to retry the transaction, we transition to Open.
	// Automatic retries are not possible beyond this point.
	transitionToOpen = txnState.State() == AutoRetry && encounteredNonAutoRetryStmt

	// Return the statements that weren't executed in the current txn, so they can
	// be executed in the context of other transactions. The set can be empty if
	// we executed everything that was passed in.
	return stmts[i+1:], transitionToOpen, nil
}

// execSingleStatement executes one statement by dispatching according to the
// current state.
//
// If an error occurs while executing the statement, the SQL txn will be
// considered aborted and subsequent statements will be discarded (they will not
// be executed, they will not be returned for future execution, they will not
// generate results). Note that this also includes COMMIT/ROLLBACK statements.
// Further note that errTransactionAborted is no exception - encountering it
// will discard subsequent statements. This means that, to recover from an
// aborted txn, a COMMIT/ROLLBACK statement needs to be the first one in stmts.
//
// Args:
// session:    The session to execute the statement in.
// stmt: 		   The statement to execute.
// pinfo:      The placeholders to use in the statements.
// firstInTxn: Set if the statements represents the first statement in a txn.
//   Used to trap nested BEGINs.
// asOfSystemTime: Set if the statement is using AS OF SYSTEM TIME.
// avoidCachedDescriptors: Set if the statement execution should avoid
//   using cached descriptors.
// automaticRetryCount: Increases with each retry; 0 for the first attempt.
// res:                 Used to pass query results to the caller.
//
// Returns the error encountered while executing the query, if any.
func (e *Executor) execSingleStatement(
	session *Session,
	stmt Statement,
	pinfo *tree.PlaceholderInfo,
	firstInTxn bool,
	asOfSystemTime bool,
	avoidCachedDescriptors bool,
	automaticRetryCount int,
	res StatementResult,
) error {
	txnState := &session.TxnState
	if txnState.State() == NoTxn {
		panic("execStmt called outside of a txn")
	}

	queryID := e.generateID()

	queryMeta := &queryMeta{
		start: session.phaseTimes[sessionEndParse],
		stmt:  stmt.AST,
		phase: preparing,
	}

	stmt.queryID = queryID

	// Canceling a query cancels its transaction's context so we take a reference to
	// the cancellation function here.
	queryMeta.ctxCancel = txnState.cancel

	// Ignore statements that spawn jobs from SHOW QUERIES and from being cancellable
	// using CANCEL QUERY. Jobs have their own run control statements (CANCEL JOB,
	// PAUSE JOB, etc). We implement this ignore by not registering queryMeta in
	// session.mu.ActiveQueries.
	if _, ok := stmt.AST.(tree.HiddenFromShowQueries); !ok {
		// For parallel/async queries, we deregister queryMeta from these registries
		// after execution finishes in the parallelizeQueue. For all other
		// (synchronous) queries, we deregister these in session.FinishPlan when
		// all results have been sent. We cannot deregister asynchronous queries in
		// session.FinishPlan because they may still be executing at that instant.
		session.addActiveQuery(queryID, queryMeta)
	}

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

	var err error
	// Run observer statements in a separate code path so they are
	// always guaranteed to execute regardless of the current transaction state.
	if _, ok := stmt.AST.(tree.ObserverStatement); ok {
		err = runObserverStatement(session, res, stmt)
	} else {
		switch txnState.State() {
		case Open, AutoRetry:
			err = e.execStmtInOpenTxn(
				session, stmt, pinfo, firstInTxn,
				asOfSystemTime, avoidCachedDescriptors, automaticRetryCount, res)
		case Aborted, RestartWait:
			err = e.execStmtInAbortedTxn(session, stmt, res)
		case CommitWait:
			err = e.execStmtInCommitWaitTxn(session, stmt, res)
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
		filter(session.Ctx(), stmt.String(), err)
	}
	if err != nil {
		// If error contains a context cancellation error, wrap it with a
		// user-friendly query execution canceled one.
		if strings.Contains(err.Error(), "context canceled") {
			err = errors.Wrapf(err, "query execution canceled")
		}
		// After an error happened, skip executing all the remaining statements
		// in this batch.  This is Postgres behavior, and it makes sense as the
		// protocol doesn't let you return results after an error.
		return err
	}
	return nil
}

// getTransactionState retrieves a text representation of the given state.
func getTransactionState(txnState *txnState) string {
	state := txnState.State()
	// If the statement reading the state is in an implicit transaction, then we
	// want to tell NoTxn to the client.
	// If implicitTxn is set, the state is supposed to be AutoRetry. However, we
	// test the state too, just in case we have a state machine bug, in which case
	// we don't want this code to hide a bug.
	if txnState.implicitTxn && state == AutoRetry {
		state = NoTxn
	}
	// For the purposes of representing the states to client, make the AutoRetry
	// state look like Open.
	if state == AutoRetry {
		state = Open
	}
	return state.String()
}

// runObserverStatement executes the given observer statement.
func runObserverStatement(session *Session, res StatementResult, stmt Statement) error {
	res.BeginResult(stmt.AST)

	switch sqlStmt := stmt.AST.(type) {
	case *tree.ShowTransactionStatus:
		res.SetColumns(sqlbase.ResultColumns{{Name: "TRANSACTION STATUS", Typ: types.String}})
		state := getTransactionState(&session.TxnState)
		if err := res.AddRow(session.Ctx(), tree.Datums{tree.NewDString(state)}); err != nil {
			return err
		}

	case *tree.ShowSyntax:
		res.SetColumns(sqlbase.ResultColumns{
			{Name: "field", Typ: types.String},
			{Name: "message", Typ: types.String},
		})
		if err := runShowSyntax(session.Ctx(), sqlStmt.Statement,
			func(ctx context.Context, field, msg string) error {
				return res.AddRow(ctx, tree.Datums{tree.NewDString(field), tree.NewDString(msg)})
			},
			nil, /* reportErr */
		); err != nil {
			return err
		}

	default:
		return pgerror.NewErrorf(pgerror.CodeInternalError,
			"programming error: unrecognized observer statement type %T", stmt.AST)
	}

	return res.CloseResult()
}

// execStmtInAbortedTxn executes a statement in a txn that's in state
// Aborted or RestartWait. All statements cause errors except:
// - COMMIT / ROLLBACK: aborts the current transaction.
// - ROLLBACK TO SAVEPOINT / SAVEPOINT: reopens the current transaction,
//   allowing it to be retried.
func (e *Executor) execStmtInAbortedTxn(
	session *Session, stmt Statement, res StatementResult,
) error {
	txnState := &session.TxnState
	if txnState.State() != Aborted && txnState.State() != RestartWait {
		panic("execStmtInAbortedTxn called outside of an aborted txn")
	}
	// TODO(andrei/cuongdo): Figure out what statements to count here.
	switch s := stmt.AST.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		if txnState.State() == RestartWait {
			transition := rollbackSQLTransaction(txnState, res)
			if transition.transitionDependentOnErrType {
				log.Fatal(session.Ctx(), "transitionDependentOnErrType should not be "+
					"returned by rollbackSQLTransaction")
			}
			txnState.resetStateAndTxn(transition.targetState)
			return transition.err
		}
		// Reset the state to allow new transactions to start.
		// The KV txn has already been rolled back when we entered the Aborted state.
		// Note: postgres replies to COMMIT of failed txn with "ROLLBACK" too.
		txnState.resetStateAndTxn(NoTxn)
		res.BeginResult((*tree.RollbackTransaction)(nil))
		return res.CloseResult()
	case *tree.RollbackToSavepoint, *tree.Savepoint:
		// We accept both the "ROLLBACK TO SAVEPOINT cockroach_restart" and the
		// "SAVEPOINT cockroach_restart" commands to indicate client intent to
		// retry a transaction in a RestartWait state.
		var spName string
		switch n := s.(type) {
		case *tree.RollbackToSavepoint:
			spName = n.Savepoint
		case *tree.Savepoint:
			spName = n.Name
		default:
			panic("unreachable")
		}
		if err := tree.ValidateRestartCheckpoint(spName); err != nil {
			if txnState.State() == RestartWait {
				err = txnState.updateStateAndCleanupOnErr(err, e)
			}
			return err
		}
		if !txnState.retryIntent {
			err := fmt.Errorf("SAVEPOINT %s has not been used", tree.RestartSavepointName)
			if txnState.State() == RestartWait {
				err = txnState.updateStateAndCleanupOnErr(err, e)
			}
			return err
		}

		res.BeginResult((*tree.RollbackToSavepoint)(nil))
		if err := res.CloseResult(); err != nil {
			return err
		}
		if txnState.State() == RestartWait {
			// Reset the state to AutoRetry. We're in an "open" txn again.
			txnState.SetState(AutoRetry)
		} else {
			// We accept ROLLBACK TO SAVEPOINT even after non-retryable errors to make
			// it easy for client libraries that want to indiscriminately issue
			// ROLLBACK TO SAVEPOINT after every error and possibly follow it with a
			// ROLLBACK and also because we accept ROLLBACK TO SAVEPOINT in the Open
			// state, so this is consistent.
			// The old txn has already been rolled back; we start a new txn with the
			// same sql timestamp and isolation as the current one.
			curTs, curIso, curPri, curRo := txnState.sqlTimestamp, txnState.isolation,
				txnState.priority, txnState.readOnly
			txnState.finishSQLTxn(session)
			txnState.resetForNewSQLTxn(
				e, session,
				false /* implicitTxn */, true, /* retryIntent */
				curTs /* sqlTimestamp */, curIso /* isolation */, curPri /* priority */, curRo /* readOnly */)
		}
		// TODO(andrei/cdo): add a counter for user-directed retries.
		return nil
	default:
		if txnState.State() == RestartWait {
			err := sqlbase.NewTransactionAbortedError(
				"Expected \"ROLLBACK TO SAVEPOINT COCKROACH_RESTART\"" /* customMsg */)
			// If we were waiting for a restart, but the client failed to perform it,
			// we'll cleanup the txn. The client is not respecting the protocol, so
			// there seems to be little point in staying in RestartWait (plus,
			// higher-level code asserts that we're only in RestartWait when returning
			// retryable errors to the client).
			return txnState.updateStateAndCleanupOnErr(err, e)
		}
		return sqlbase.NewTransactionAbortedError("" /* customMsg */)
	}
}

// execStmtInCommitWaitTxn executes a statement in a txn that's in state
// CommitWait.
// Everything but COMMIT/ROLLBACK causes errors. ROLLBACK is treated like COMMIT.
func (e *Executor) execStmtInCommitWaitTxn(
	session *Session, stmt Statement, res StatementResult,
) error {
	txnState := &session.TxnState
	if txnState.State() != CommitWait {
		panic("execStmtInCommitWaitTxn called outside of an aborted txn")
	}
	e.updateStmtCounts(stmt)
	switch stmt.AST.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		// Reset the state to allow new transactions to start.
		txnState.resetStateAndTxn(NoTxn)
		res.BeginResult((*tree.CommitTransaction)(nil))
		return res.CloseResult()
	default:
		err := sqlbase.NewTransactionCommittedError()
		return err
	}
}

// sessionEventf logs an event to the current transaction context and to the
// session event log.
func sessionEventf(session *Session, format string, args ...interface{}) {
	if session.eventLog != nil {
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
// Results are written to res.
//
// The current transaction might be committed/rolled back when this returns.
// It might also transition to the aborted or RestartWait state.
//
// Args:
// session: the session to execute the statement in.
// stmt: the statement to execute.
// pinfo: the placeholders to use in the statement.
// firstInTxn: set for the first statement in a transaction. Used
//  so that nested BEGIN statements are caught.
// asOfSystemTime: set if the statement is using AS OF SYSTEM TIME.
// avoidCachedDescriptors: set if the statement execution should avoid
//  using cached descriptors.
// automaticRetryCount: increases with each retry; 0 for the first attempt.
// res: the writer to send results to.
func (e *Executor) execStmtInOpenTxn(
	session *Session,
	stmt Statement,
	pinfo *tree.PlaceholderInfo,
	firstInTxn bool,
	asOfSystemTime bool,
	avoidCachedDescriptors bool,
	automaticRetryCount int,
	res StatementResult,
) (err error) {
	txnState := &session.TxnState
	if !txnState.TxnIsOpen() {
		panic("execStmtInOpenTxn called outside of an open txn")
	}

	sessionEventf(session, "%s", stmt)

	var explicitStateTransition bool
	var transition stateTransition

	// Do not double count automatically retried transactions.
	if automaticRetryCount == 0 {
		e.updateStmtCounts(stmt)
	}

	// After the statement is executed, we might have to do state transitions.
	defer func() {
		// There's two cases to handle, depending on the two categories of
		// statements we have:
		// - Some statements (COMMIT, ROLLBACK) explicitly decide what state
		// transition they want. For them, explicitStateTransition is set and
		// transition is filled in, and so we obey their instructions.
		// - Most statements don't set explicitStateTransition. For them we do state
		// transitions based on the error they returned (if any).

		if explicitStateTransition {
			if err != nil {
				panic(fmt.Sprintf("unexpected return err when explicitStateTransition is set: %+v", err))
			}
			err = transition.err
			if transition.transitionDependentOnErrType {
				if err == nil {
					panic(fmt.Sprintf("missing err when transitionDependentOnErrType is set"))
				}
				err = txnState.updateStateAndCleanupOnErr(err, e)
			} else {
				txnState.resetStateAndTxn(transition.targetState)
			}
		} else if err != nil {
			if !txnState.TxnIsOpen() {
				panic(fmt.Sprintf("unexpected txnState when cleaning up: %v", txnState.State()))
			}
			err = txnState.updateStateAndCleanupOnErr(err, e)

			if firstInTxn && isBegin(stmt) {
				// A failed BEGIN statement that was starting a txn doesn't leave the
				// txn in the Aborted state; we transition back to NoTxn.
				//
				// TODO(andrei): BEGIN should use the explicitStateTransition mechanism.
				txnState.resetStateAndTxn(NoTxn)
			}
		}

		if err != nil {
			return
		}
	}()

	// Check if the statement is parallelized or is independent from parallel
	// execution. If neither of these cases are true, we need to synchronize
	// parallel execution by letting it drain before we can begin executing ourselves.
	parallelize := IsStmtParallelized(stmt)
	_, independentFromParallelStmts := stmt.AST.(tree.IndependentFromParallelizedPriors)
	if !(parallelize || independentFromParallelStmts) {
		if err := session.synchronizeParallelStmts(session.Ctx()); err != nil {
			if _, isCommit := stmt.AST.(*tree.CommitTransaction); isCommit {
				txnState.commitSeen = true
			}
			return err
		}
	}

	if txnState.implicitTxn && !stmtAllowedInImplicitTxn(stmt) {
		return errNoTransactionInProgress
	}

	switch s := stmt.AST.(type) {
	case *tree.BeginTransaction:
		// BeginTransaction is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		if !firstInTxn {
			return errTransactionInProgress
		}
		res.BeginResult((*tree.BeginTransaction)(nil))
		if err := session.TxnState.setTransactionModes(s.Modes); err != nil {
			return err
		}
		return res.CloseResult()

	case *tree.CommitTransaction:
		// CommitTransaction is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		transition = commitSQLTransaction(txnState, commit, res)
		explicitStateTransition = true
		return nil

	case *tree.ReleaseSavepoint:
		if err := tree.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return err
		}
		// ReleaseSavepoint is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		transition = commitSQLTransaction(txnState, release, res)
		explicitStateTransition = true
		return nil

	case *tree.RollbackTransaction:
		// RollbackTransaction is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		if err := session.tables.releaseTables(txnState.Ctx, dontBlockForDBCacheUpdate); err != nil {
			log.Warningf(txnState.Ctx, "releaseTables failed: %s", err)
		}
		transition = rollbackSQLTransaction(txnState, res)
		explicitStateTransition = true
		return nil

	case *tree.Savepoint:
		if err := tree.ValidateRestartCheckpoint(s.Name); err != nil {
			return err
		}
		// We want to disallow SAVEPOINTs to be issued after a transaction has
		// started running. The client txn's statement count indicates how many
		// statements have been executed as part of this transaction.
		if meta := txnState.mu.txn.GetTxnCoordMeta(); meta.CommandCount > 0 {
			return errors.Errorf("SAVEPOINT %s needs to be the first statement in a "+
				"transaction", tree.RestartSavepointName)
		}
		// Note that Savepoint doesn't have a corresponding plan node.
		// This here is all the execution there is.
		txnState.retryIntent = true
		res.BeginResult((*tree.Savepoint)(nil))
		return res.CloseResult()

	case *tree.RollbackToSavepoint:
		if err := tree.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return err
		}
		if !txnState.retryIntent {
			err := fmt.Errorf("SAVEPOINT %s has not been used", tree.RestartSavepointName)
			return err
		}

		res.BeginResult((*tree.Savepoint)(nil))
		if err := res.CloseResult(); err != nil {
			return err
		}

		// Move the state to AutoRetry; we're morally beginning a new transaction.
		txnState.SetState(AutoRetry)
		// If commands have already been sent through the transaction,
		// restart the client txn's proto to increment the epoch.
		if meta := txnState.mu.txn.GetTxnCoordMeta(); meta.CommandCount > 0 {
			// TODO(andrei): Should the timestamp below be e.cfg.Clock.Now(), so that
			// the transaction gets a new timestamp?
			txnState.mu.txn.Proto().Restart(
				0 /* userPriority */, 0 /* upgradePriority */, hlc.Timestamp{})
		}
		return nil

	case *tree.Prepare:
		// This must be handled here instead of the common path below
		// because we need to use the Executor reference.
		if err := e.PrepareStmt(session, s); err != nil {
			return err
		}
		res.BeginResult((*tree.Prepare)(nil))
		return res.CloseResult()

	case *tree.Execute:
		name := s.Name.String()
		ps, ok := session.PreparedStatements.Get(name)
		if !ok {
			err := pgerror.NewErrorf(
				pgerror.CodeInvalidSQLStatementNameError,
				"prepared statement %q does not exist", name,
			)
			return err
		}
		var err error
		pinfo, err = fillInPlaceholders(ps, name, s.Params, session.data.SearchPath)
		if err != nil {
			return err
		}

		stmt.AST = ps.Statement
		stmt.ExpectedTypes = ps.Columns
		stmt.AnonymizedStr = ps.AnonymizedStr

	case *tree.CopyFrom:
		// The CopyFrom statement is special. It doesn't have a planNode. Instead,
		// we use a CopyMachine and hand it control over the connection until the
		// copying is done.

		// If we're in an explicit txn, then the copying will be done within that
		// txn. Otherwise, we tell the copyMachine to manage its own transactions.
		var txnOpt copyTxnOpt
		if !txnState.implicitTxn {
			txnOpt = copyTxnOpt{
				txn:           txnState.mu.txn,
				txnTimestamp:  txnState.sqlTimestamp,
				stmtTimestamp: e.cfg.Clock.PhysicalTime(),
			}
		}
		cm, err := newCopyMachine(
			session.Ctx(), session.conn,
			s,
			txnOpt,
			&e.cfg,
			// resetPlanner
			func(p *planner, txn *client.Txn, txnTS time.Time, stmtTS time.Time) {
				session.resetPlanner(
					p, txn, txnTS, stmtTS,
					nil /* reCache */, session.statsCollector())
			},
		)
		if err != nil {
			return err
		}
		return cm.run(session.Ctx())
		// Note that we returned here without closing res (in fact, the CopyFrom
		// code doesn't touch res at all). That's OK since res.BeginResult() was
		// also not called.
	}

	var p *planner
	runInParallel := parallelize && !txnState.implicitTxn
	stmtTs := e.cfg.Clock.PhysicalTime()
	if runInParallel {
		// Create a new planner from the Session to execute the statement, since
		// we're executing in parallel.
		p = session.newPlanner(
			txnState.mu.txn, txnState.sqlTimestamp, stmtTs, e.reCache, session.statsCollector())
	} else {
		// We're not executing in parallel. We can use the cached planner on the
		// session.
		p = &session.planner
		session.resetPlanner(
			p, txnState.mu.txn, txnState.sqlTimestamp, stmtTs,
			e.reCache, session.statsCollector())
	}
	p.semaCtx.Placeholders.Assign(pinfo)
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	p.asOfSystemTime = asOfSystemTime
	p.avoidCachedDescriptors = avoidCachedDescriptors
	p.statsCollector.PhaseTimes()[plannerStartExecStmt] = timeutil.Now()
	p.stmt = &stmt
	// TODO(andrei): Ideally we'd like to fork off a context for each individual
	// statement. But the heartbeat loop in TxnCoordSender currently assumes that
	// the context of the first operation in a txn batch lasts at least as long as
	// the transaction itself. Once that sender is able to distinguish between
	// statement and transaction contexts, we should move to per-statement
	// contexts.
	p.cancelChecker = sqlbase.NewCancelChecker(txnState.Ctx)

	// constantMemAcc accounts for all constant folded values that are computed
	// prior to any rows being computed.
	constantMemAcc := p.extendedEvalCtx.Mon.MakeBoundAccount()
	p.extendedEvalCtx.ActiveMemAcc = &constantMemAcc
	defer constantMemAcc.Close(session.Ctx())

	if runInParallel {
		// Only run statements asynchronously through the parallelize queue if the
		// statements are parallelized and we're in a transaction. Parallelized
		// statements outside of a transaction are run synchronously with mocked
		// results, which has the same effect as running asynchronously but
		// immediately blocking.
		err = func() error {
			cols, err := e.execStmtInParallel(stmt, session, p)
			if err != nil {
				return err
			}
			// Produce mocked out results for the query - the "zero value" of the
			// statement's result type:
			// - tree.Rows -> an empty set of rows
			// - tree.RowsAffected -> zero rows affected
			if err := initStatementResult(res, stmt, cols); err != nil {
				return err
			}
			return res.CloseResult()
		}()
	} else {
		p.autoCommit = txnState.implicitTxn && !e.cfg.TestingKnobs.DisableAutoCommit
		err = e.execStmt(stmt, session, p, automaticRetryCount, res)
		// Zeroing the cached planner allows the GC to clean up any memory hanging
		// off the planner, which we're finished using at this point.
	}

	if err != nil {
		if independentFromParallelStmts {
			// If the statement run was independent from parallelized execution, it
			// might have been run concurrently with parallelized statements. Make
			// sure all complete before returning the error.
			_ = session.synchronizeParallelStmts(session.Ctx())
		}

		sessionEventf(session, "ERROR: %v", err)
		return err
	}

	tResult := &traceResult{tag: res.PGTag(), count: -1}
	switch res.StatementType() {
	case tree.RowsAffected, tree.Rows:
		tResult.count = res.RowsAffected()
	}
	sessionEventf(session, "%s done", tResult)
	return nil
}

// stmtAllowedInImplicitTxn returns whether the statement is allowed in an
// implicit transaction or not.
func stmtAllowedInImplicitTxn(stmt Statement) bool {
	switch stmt.AST.(type) {
	case *tree.CommitTransaction:
	case *tree.ReleaseSavepoint:
	case *tree.RollbackTransaction:
	case *tree.SetTransaction:
	case *tree.Savepoint:
	default:
		return true
	}
	return false
}

// stateTransition allows statement execution to request state transitions as a
// side-effect of their execution. Currently only specific statements use this
// mechanism; the majority rely on the handling of the errors they return to
// affect state transitions.
type stateTransition struct {
	// transitionDependentOnErrType, if set, means that the error should be
	// interpreted to decide on the state transition (e.g. retriable errors may
	// cause different transitions than non-retriable errors).
	// If set, err must also be set, and targetSet is ignored.
	transitionDependentOnErrType bool
	// err is used by statements to inform that their execution encountered an
	// error. The error will be eventually passed to pgwire, which serializes it
	// to the client.
	// The error can be a communication error (e.g. sending results to the client
	// failed); these errors will be recognized by pgwire and the connection will
	// be terminated.
	err error

	// targetState is the state to which we'll transition.
	targetState TxnStateEnum
}

// rollbackSQLTransaction executes a ROLLBACK statement. The transaction is
// rolled-back and the statement result is written to res.
//
// Returns the desired state transition. Since ROLLBACK is not concerned with
// retriable errors, stateTransition.transitionDependentOnErrType is guaranteed
// to be false.
func rollbackSQLTransaction(txnState *txnState, res StatementResult) stateTransition {
	if !txnState.TxnIsOpen() && txnState.State() != RestartWait {
		panic(fmt.Sprintf("rollbackSQLTransaction called on txn in wrong state: %s (txn: %s)",
			txnState.State(), txnState.mu.txn.Proto()))
	}
	if err := txnState.mu.txn.Rollback(txnState.Ctx); err != nil {
		log.Warningf(txnState.Ctx, "txn rollback failed: %s", err)
	}
	// We're done with this txn.
	res.BeginResult((*tree.RollbackTransaction)(nil))
	if closeErr := res.CloseResult(); closeErr != nil {
		return stateTransition{
			targetState: Aborted,
			err:         closeErr,
		}
	}
	return stateTransition{
		targetState: NoTxn,
	}
}

type commitType int

const (
	commit commitType = iota
	release
)

// commitSQLTransaction executes a COMMIT or RELEASE SAVEPOINT statement. The
// transaction is committed and the statement result is written to res.
func commitSQLTransaction(
	txnState *txnState, commitType commitType, res StatementResult,
) stateTransition {

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
		// of `txnState.commitSeen`, set above. As far as the layer interpreting our
		// stateTransition is concerned, transitionDependentOnErrType is the right
		// thing to do without concern for the special nature of COMMIT.
		return stateTransition{
			transitionDependentOnErrType: true,
			err: err,
		}
	}

	var transition stateTransition
	switch commitType {
	case release:
		// We'll now be waiting for a COMMIT.
		transition.targetState = CommitWait
	case commit:
		// We're done with this txn.
		transition.targetState = NoTxn
	}

	res.BeginResult((*tree.CommitTransaction)(nil))
	if err := res.CloseResult(); err != nil {
		transition = stateTransition{
			err: err,
			// The state after a communication error doesn't really matter, as the
			// session will not receive more statements. We're using Aborted for
			// consistency with most error code paths which don't have special
			// handling for communication errors at the Executor level.
			//
			// TODO(andrei): Introduce a dedicated state to represent broken sessions
			// that should not accept any statement anymore.
			targetState: Aborted,
		}
	}
	return transition
}

// exectDistSQL converts the current logical plan to a distributed SQL
// physical plan and runs it.
func (e *Executor) execDistSQL(
	planner *planner, plan planNode, rowResultWriter StatementResult, stmtType tree.StatementType,
) error {
	ctx := planner.EvalContext().Ctx()
	recv := makeDistSQLReceiver(
		ctx, rowResultWriter, stmtType,
		e.cfg.RangeDescriptorCache, e.cfg.LeaseHolderCache,
		planner.txn,
		func(ts hlc.Timestamp) {
			_ = e.cfg.Clock.Update(ts)
		},
	)
	e.distSQLPlanner.PlanAndRun(ctx, planner.txn, plan, recv, &planner.extendedEvalCtx)
	return rowResultWriter.Err()
}

// execLocal runs the current logical plan using the local
// (non-distributed) SQL implementation.
func (e *Executor) execLocal(planner *planner, rowResultWriter StatementResult) error {
	ctx := planner.EvalContext().Ctx()

	// Create a BoundAccount to track the memory usage of each row.
	rowAcc := planner.extendedEvalCtx.Mon.MakeBoundAccount()
	planner.extendedEvalCtx.ActiveMemAcc = &rowAcc
	defer rowAcc.Close(ctx)

	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: &planner.extendedEvalCtx,
		p:               planner,
	}

	if err := planner.curPlan.start(params); err != nil {
		return err
	}

	switch rowResultWriter.StatementType() {
	case tree.RowsAffected:
		count, err := countRowsAffected(params, planner.curPlan.plan)
		if err != nil {
			return err
		}
		rowResultWriter.IncrementRowsAffected(count)

	case tree.Rows:
		for _, c := range planner.curPlan.columns() {
			if err := checkResultType(c.Typ); err != nil {
				return err
			}
		}
		if err := forEachRow(params, planner.curPlan.plan, func(values tree.Datums) error {
			return rowResultWriter.AddRow(ctx, values)
		}); err != nil {
			return err
		}
	}
	return nil
}

// forEachRow calls the provided closure for each successful call to
// planNode.Next with planNode.Values, making sure to properly track memory
// usage.
func forEachRow(params runParams, p planNode, f func(tree.Datums) error) error {
	next, err := p.Next(params)
	for ; next; next, err = p.Next(params) {
		// If we're tracking memory, clear the previous row's memory account.
		if params.extendedEvalCtx.ActiveMemAcc != nil {
			params.extendedEvalCtx.ActiveMemAcc.Clear(params.ctx)
		}

		if err := f(p.Values()); err != nil {
			return err
		}
	}
	return err
}

// If the plan has a fast path we attempt to query that,
// otherwise we fall back to counting via plan.Next().
func countRowsAffected(params runParams, p planNode) (int, error) {
	if a, ok := p.(planNodeFastPath); ok {
		if count, res := a.FastPathResults(); res {
			return count, nil
		}
	}

	count := 0
	err := forEachRow(params, p, func(_ tree.Datums) error {
		count++
		return nil
	})
	return count, err
}

// shouldUseOptimizer determines whether we should use the experimental
// optimizer for planning.
func shouldUseOptimizer(optMode sessiondata.OptimizerMode, stmt Statement) bool {
	switch optMode {
	case sessiondata.OptimizerAlways:
		// Don't try to run SET commands with the optimizer in Always mode, or
		// else we can't switch to another mode.
		if _, setVar := stmt.AST.(*tree.SetVar); !setVar {
			return true
		}

	case sessiondata.OptimizerOn:
		// Only handle a subset of the statement types (currently read-only queries).
		switch stmt.AST.(type) {
		case *tree.ParenSelect, *tree.Select, *tree.SelectClause,
			*tree.UnionClause, *tree.ValuesClause:
			return true
		case *tree.Explain:
			// The optimizer doesn't yet support EXPLAIN but if we return false, it gets
			// executed with the old planner which can be very confusing; it's
			// preferable to error out.
			return true
		}
	}
	return false
}

// shouldUseDistSQL determines whether we should use DistSQL for the
// given logical plan, based on the session settings.
func shouldUseDistSQL(
	ctx context.Context,
	distSQLMode sessiondata.DistSQLExecMode,
	dp *DistSQLPlanner,
	planner *planner,
) (bool, error) {
	if distSQLMode == sessiondata.DistSQLOff {
		return false, nil
	}

	plan := planner.curPlan.plan

	// Don't try to run empty nodes (e.g. SET commands) with distSQL.
	if _, ok := plan.(*zeroNode); ok {
		return false, nil
	}

	// We don't support subqueries yet.
	if len(planner.curPlan.subqueryPlans) > 0 {
		if distSQLMode == sessiondata.DistSQLAlways {
			err := newQueryNotSupportedError("subqueries not supported yet")
			log.VEventf(ctx, 1, "query not supported for distSQL: %s", err)
			return false, err
		}
		return false, nil
	}

	// Trigger limit propagation.
	planner.setUnlimited(plan)

	distribute, err := dp.CheckSupport(plan)
	if err != nil {
		// If the distSQLMode is ALWAYS, reject anything but SET.
		if distSQLMode == sessiondata.DistSQLAlways && err != setNotSupportedError {
			return false, err
		}
		// Don't use distSQL for this request.
		log.VEventf(ctx, 1, "query not supported for distSQL: %s", err)
		return false, nil
	}

	if distSQLMode == sessiondata.DistSQLAuto && !distribute {
		log.VEventf(ctx, 1, "not distributing query")
		return false, nil
	}

	// In ON or ALWAYS mode, all supported queries are distributed.
	return true, nil
}

// initStatementResult initializes res according to a query.
//
// cols represents the columns of the result rows. Should be nil if
// stmt.AST.StatementType() != tree.Rows.
//
// If an error is returned, it is to be considered a query execution error.
func initStatementResult(res StatementResult, stmt Statement, cols sqlbase.ResultColumns) error {
	stmtAst := stmt.AST
	res.BeginResult(stmtAst)
	res.SetColumns(cols)
	for _, c := range cols {
		if err := checkResultType(c.Typ); err != nil {
			return err
		}
	}
	return nil
}

// execStmt executes the statement synchronously and writes the result to
// res.
func (e *Executor) execStmt(
	stmt Statement, session *Session, planner *planner, automaticRetryCount int, res StatementResult,
) error {
	ctx := session.Ctx()

	planner.statsCollector.PhaseTimes()[plannerStartLogicalPlan] = timeutil.Now()
	useOptimizer := shouldUseOptimizer(session.data.OptimizerMode, stmt)
	var err error
	if useOptimizer {
		// Experimental path (disabled by default).
		err = planner.makeOptimizerPlan(ctx, stmt)
	} else {
		err = planner.makePlan(ctx, stmt)
	}
	if err != nil {
		planner.maybeLogStatement(ctx, "exec", 0, err)
		return err
	}
	planner.statsCollector.PhaseTimes()[plannerEndLogicalPlan] = timeutil.Now()

	defer planner.curPlan.close(ctx)

	defer func() { planner.maybeLogStatement(ctx, "exec", res.RowsAffected(), res.Err()) }()

	// Prepare the result set, and determine the execution parameters.
	var cols sqlbase.ResultColumns
	if stmt.AST.StatementType() == tree.Rows {
		cols = planColumns(planner.curPlan.plan)
	}
	if err := initStatementResult(res, stmt, cols); err != nil {
		return err
	}

	useDistSQL := false
	// TODO(radu): for now, we restrict the optimizer to local execution.
	if !useOptimizer {
		var err error
		useDistSQL, err = shouldUseDistSQL(
			session.Ctx(), session.data.DistSQLMode, e.distSQLPlanner, planner,
		)
		if err != nil {
			return err
		}
	}

	// Start execution.
	if e.cfg.TestingKnobs.BeforeExecute != nil {
		e.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), false /* isParallel */)
	}

	planner.statsCollector.PhaseTimes()[plannerStartExecStmt] = timeutil.Now()
	session.setQueryExecutionMode(stmt.queryID, useDistSQL, false /* isParallel */)
	if useDistSQL {
		err = e.execDistSQL(planner, planner.curPlan.plan, res, stmt.AST.StatementType())
	} else {
		err = e.execLocal(planner, res)
	}
	// Ensure the error is saved where maybeLogStatement can find it.
	res.SetError(err)
	planner.statsCollector.PhaseTimes()[plannerEndExecStmt] = timeutil.Now()

	// Complete execution: record results and optionally run the test
	// hook.
	recordStatementSummary(
		planner, stmt, useDistSQL, automaticRetryCount, res.RowsAffected(), err, &e.EngineMetrics,
	)
	if e.cfg.TestingKnobs.AfterExecute != nil {
		e.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), err)
	}
	if err != nil {
		return err
	}
	return res.CloseResult()
}

// execStmtInParallel executes a query asynchronously: the query will wait for
// all other currently executing async queries which are not independent, and
// then it will run.
// Async queries don't produce results (apart from errors). Note that planning
// needs to be done synchronously because it's needed by the query dependency
// analysis.
// A list of columns is returned for purposes of initializing the statement
// results. This will be nil if the query's result is of type "RowsAffected".
//
// TODO(nvanbenschoten): We do not currently support parallelizing distributed SQL
// queries, so this method can only be used with local SQL execution.
func (e *Executor) execStmtInParallel(
	stmt Statement, session *Session, planner *planner,
) (sqlbase.ResultColumns, error) {
	ctx := session.Ctx()
	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: &planner.extendedEvalCtx,
		p:               planner,
	}

	if err := planner.makePlan(ctx, stmt); err != nil {
		planner.maybeLogStatement(ctx, "par-prepare", 0, err)
		return nil, err
	}
	var cols sqlbase.ResultColumns
	if stmt.AST.StatementType() == tree.Rows {
		cols = planner.curPlan.columns()
	}

	// This ensures we don't unintentionally clean up the queryMeta object when we
	// send the mock result back to the client.
	session.setQueryExecutionMode(stmt.queryID, false /* isDistributed */, true /* isParallel */)

	if err := session.parallelizeQueue.Add(params, func() error {
		// TODO(andrei): this should really be a result writer implementation that
		// does nothing.
		bufferedWriter := newBufferedWriter(session.makeBoundAccount())

		defer func() {
			params.p.maybeLogStatement(ctx, "par-exec", bufferedWriter.RowsAffected(), bufferedWriter.Err())
		}()

		err := initStatementResult(bufferedWriter, stmt, cols)
		if err != nil {
			bufferedWriter.SetError(err)
			return err
		}

		if e.cfg.TestingKnobs.BeforeExecute != nil {
			e.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), true /* isParallel */)
		}

		planner.statsCollector.PhaseTimes()[plannerStartExecStmt] = timeutil.Now()
		err = e.execLocal(planner, bufferedWriter)
		// Ensure err is saved where maybeLogStatement can find it.
		bufferedWriter.SetError(err)
		planner.statsCollector.PhaseTimes()[plannerEndExecStmt] = timeutil.Now()
		recordStatementSummary(
			planner, stmt, false, 0, bufferedWriter.RowsAffected(), err, &e.EngineMetrics)
		if e.cfg.TestingKnobs.AfterExecute != nil {
			e.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), err)
		}
		results := bufferedWriter.results()
		results.Close(ctx)
		// Deregister query from registry.
		session.removeActiveQuery(stmt.queryID)
		return err
	}); err != nil {
		planner.maybeLogStatement(ctx, "par-queue", 0, err)
		return nil, err
	}

	return cols, nil
}

// Cfg returns a reference to the ExecutorConfig.
func (e *Executor) Cfg() *ExecutorConfig {
	return &e.cfg
}

// updateStmtCounts updates metrics for the number of times the different types of SQL
// statements have been received by this node.
func (e *Executor) updateStmtCounts(stmt Statement) {
	e.QueryCount.Inc(1)
	switch stmt.AST.(type) {
	case *tree.BeginTransaction:
		e.TxnBeginCount.Inc(1)
	case *tree.Select:
		e.SelectCount.Inc(1)
	case *tree.Update:
		e.UpdateCount.Inc(1)
	case *tree.Insert:
		e.InsertCount.Inc(1)
	case *tree.Delete:
		e.DeleteCount.Inc(1)
	case *tree.CommitTransaction:
		e.TxnCommitCount.Inc(1)
	case *tree.RollbackTransaction:
		e.TxnRollbackCount.Inc(1)
	default:
		if tree.CanModifySchema(stmt.AST) {
			e.DdlCount.Inc(1)
		} else {
			e.MiscCount.Inc(1)
		}
	}
}

// generateID generates a unique ID based on the node's ID and its current HLC
// timestamp, which can be used as an ID for a query or a session.
func (e *Executor) generateID() ClusterWideID {
	return GenerateClusterWideID(e.cfg.Clock.Now(), e.cfg.NodeID.Get())
}

// golangFillQueryArguments populates the placeholder map with
// types and values from an array of Go values.
// TODO: This does not support arguments of the SQL 'Date' type, as there is not
// an equivalent type in Go's standard library. It's not currently needed by any
// of our internal tables.
func golangFillQueryArguments(pinfo *tree.PlaceholderInfo, args []interface{}) {
	pinfo.Clear()

	for i, arg := range args {
		k := strconv.Itoa(i + 1)
		if arg == nil {
			pinfo.SetValue(k, tree.DNull)
			continue
		}

		// A type switch to handle a few explicit types with special semantics:
		// - Datums are passed along as is.
		// - Time datatypes get special representation in the database.
		var d tree.Datum
		switch t := arg.(type) {
		case tree.Datum:
			d = t
		case time.Time:
			d = tree.MakeDTimestamp(t, time.Microsecond)
		case time.Duration:
			d = &tree.DInterval{Duration: duration.Duration{Nanos: t.Nanoseconds()}}
		case *apd.Decimal:
			dd := &tree.DDecimal{}
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
				d = tree.MakeDBool(tree.DBool(val.Bool()))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				d = tree.NewDInt(tree.DInt(val.Int()))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				d = tree.NewDInt(tree.DInt(val.Uint()))
			case reflect.Float32, reflect.Float64:
				d = tree.NewDFloat(tree.DFloat(val.Float()))
			case reflect.String:
				d = tree.NewDString(val.String())
			case reflect.Slice:
				// Handle byte slices.
				if val.Type().Elem().Kind() == reflect.Uint8 {
					d = tree.NewDBytes(tree.DBytes(val.Bytes()))
				}
			}
			if d == nil {
				panic(fmt.Sprintf("unexpected type %T", arg))
			}
		}
		pinfo.SetValue(k, d)
	}
}

func checkResultType(typ types.T) error {
	// Compare all types that can rely on == equality.
	switch types.UnwrapType(typ) {
	case types.Unknown:
	case types.Bool:
	case types.Int:
	case types.Float:
	case types.Decimal:
	case types.Bytes:
	case types.String:
	case types.Date:
	case types.Time:
	case types.Timestamp:
	case types.TimestampTZ:
	case types.Interval:
	case types.JSON:
	case types.UUID:
	case types.INet:
	case types.NameArray:
	case types.Oid:
	case types.RegClass:
	case types.RegNamespace:
	case types.RegProc:
	case types.RegProcedure:
	case types.RegType:
	default:
		// Compare all types that cannot rely on == equality.
		istype := typ.FamilyEqual
		switch {
		case istype(types.FamArray):
			if istype(types.UnwrapType(typ).(types.TArray).Typ) {
				return pgerror.Unimplemented("nested arrays", "arrays cannot have arrays as element type")
			}
		case istype(types.FamCollatedString):
		case istype(types.FamTuple):
		case istype(types.FamPlaceholder):
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
	evalCtx *tree.EvalContext, asOf tree.AsOfClause, max hlc.Timestamp,
) (hlc.Timestamp, error) {
	te, err := asOf.Expr.TypeCheck(nil, types.String)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	d, err := te.Eval(evalCtx)
	if err != nil {
		return hlc.Timestamp{}, err
	}

	var ts hlc.Timestamp
	var convErr error

	switch d := d.(type) {
	case *tree.DString:
		s := string(*d)
		// Allow nanosecond precision because the timestamp is only used by the
		// system and won't be returned to the user over pgwire.
		if dt, err := tree.ParseDTimestamp(s, time.Nanosecond); err == nil {
			ts.WallTime = dt.Time.UnixNano()
			break
		}
		// Attempt to parse as a decimal.
		if dec, _, err := apd.NewFromString(s); err == nil {
			ts, convErr = decimalToHLC(dec)
			break
		}
		// Attempt to parse as an interval.
		if iv, err := tree.ParseDInterval(s); err == nil {
			ts.WallTime = duration.Add(evalCtx.GetStmtTimestamp(), iv.Duration).UnixNano()
			break
		}
		convErr = errors.Errorf("AS OF SYSTEM TIME: value is neither timestamp, decimal, nor interval")
	case *tree.DInt:
		ts.WallTime = int64(*d)
	case *tree.DDecimal:
		ts, convErr = decimalToHLC(&d.Decimal)
	case *tree.DInterval:
		ts.WallTime = duration.Add(evalCtx.GetStmtTimestamp(), d.Duration).UnixNano()
	default:
		convErr = errors.Errorf("AS OF SYSTEM TIME: expected timestamp, decimal, or interval, got %s (%T)", d.ResolvedType(), d)
	}
	if convErr != nil {
		return ts, convErr
	}

	var zero hlc.Timestamp
	if ts == zero {
		return ts, errors.Errorf("AS OF SYSTEM TIME: zero timestamp is invalid")
	} else if max.Less(ts) {
		return ts, errors.Errorf("AS OF SYSTEM TIME: cannot specify timestamp in the future")
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
		return hlc.Timestamp{}, errors.Wrap(err, "AS OF SYSTEM TIME: parsing argument")
	}
	var logical int64
	if len(parts) > 1 {
		// logicalLength is the number of decimal digits expected in the
		// logical part to the right of the decimal. See the implementation of
		// cluster_logical_timestamp().
		const logicalLength = 10
		p := parts[1]
		if lp := len(p); lp > logicalLength {
			return hlc.Timestamp{}, errors.Errorf("AS OF SYSTEM TIME: logical part has too many digits")
		} else if lp < logicalLength {
			p += strings.Repeat("0", logicalLength-lp)
		}
		logical, err = strconv.ParseInt(p, 10, 32)
		if err != nil {
			return hlc.Timestamp{}, errors.Wrap(err, "AS OF SYSTEM TIME: parsing argument")
		}
	}
	return hlc.Timestamp{
		WallTime: nanos,
		Logical:  int32(logical),
	}, nil
}

// isAsOf analyzes a statement to bypass the logic in newPlan(), since
// that requires the transaction to be started already. If the returned
// timestamp is not nil, it is the timestamp to which a transaction
// should be set. The statements that will be checked are Select,
// ShowTrace (of a Select statement), and Scrub.
//
// max is a lower bound on what the transaction's timestamp will be.
// Used to check that the user didn't specify a timestamp in the future.
func isAsOf(
	stmt tree.Statement, evalCtx *tree.EvalContext, max hlc.Timestamp,
) (*hlc.Timestamp, error) {
	var asOf tree.AsOfClause
	switch s := stmt.(type) {
	case *tree.Select:
		selStmt := s.Select
		var parenSel *tree.ParenSelect
		var ok bool
		for parenSel, ok = selStmt.(*tree.ParenSelect); ok; parenSel, ok = selStmt.(*tree.ParenSelect) {
			selStmt = parenSel.Select.Select
		}

		sc, ok := selStmt.(*tree.SelectClause)
		if !ok {
			return nil, nil
		}
		if sc.From == nil || sc.From.AsOf.Expr == nil {
			return nil, nil
		}

		asOf = sc.From.AsOf
	case *tree.ShowTrace:
		return isAsOf(s.Statement, evalCtx, max)
	case *tree.Scrub:
		if s.AsOf.Expr == nil {
			return nil, nil
		}
		asOf = s.AsOf
	default:
		return nil, nil
	}

	ts, err := EvalAsOfTimestamp(evalCtx, asOf, max)
	return &ts, err
}

// isSavepoint returns true if stmt is a SAVEPOINT statement.
func isSavepoint(stmt Statement) bool {
	_, isSavepoint := stmt.AST.(*tree.Savepoint)
	return isSavepoint
}

// isBegin returns true if stmt is a BEGIN statement.
func isBegin(stmt Statement) bool {
	_, isBegin := stmt.AST.(*tree.BeginTransaction)
	return isBegin
}

// isSetTransaction returns true if stmt is a "SET TRANSACTION ..." statement.
func isSetTransaction(stmt Statement) bool {
	_, isSet := stmt.AST.(*tree.SetTransaction)
	return isSet
}

// isRollbackToSavepoint returns true if stmt is a "ROLLBACK TO SAVEPOINT"
// statement.
func isRollbackToSavepoint(stmt Statement) bool {
	_, isSet := stmt.AST.(*tree.RollbackToSavepoint)
	return isSet
}

// canStayInAutoRetryState returns true if the statement, by itself, should not
// cause the session to transition from AutoRetry->Open at the end of the
// statement's batch. In other words, if the state was AutoRetry at a certain
// time, and afterwards only statements recognized by this method run until the
// end of a batch, then the end of the batch shouldn't perform the
// AutoRetry->Open transition that ends of batches usually perform.
//
// Statements in this category may not be executed again in case of an automatic
// retry, so they need to have two properties:
// 1) They can only affect the transaction state in such a way that a
// restart would not destroy the statement's effect. So, for example, RETURNING
// NOTHING statements can't be put here without adding some replay capability.
// 2) The results and side-effects they produce must not depend on any previous
// statements ran in the transaction.
func canStayInAutoRetryState(stmt Statement) bool {
	return isBegin(stmt) ||
		isSavepoint(stmt) ||
		isSetTransaction(stmt) ||
		// ROLLBACK TO SAVEPOINT does its own state transitions; if it leaves the
		// transaction in the AutoRetriable state, don't mess with it.
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
