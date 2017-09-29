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
	"net/url"
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

// logStatementsExecuteEnabled causes the Executor to log executed
// statements and, if any, resulting errors.
var logStatementsExecuteEnabled = settings.RegisterBoolSetting(
	"sql.trace.log_statement_execute",
	"set to true to enable logging of executed statements",
	false,
)

// ClusterOrganization is the organization name.
var ClusterOrganization = settings.RegisterStringSetting(
	"cluster.organization",
	"organization name",
	"",
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

	TestingKnobs              *ExecutorTestingKnobs
	SchemaChangerTestingKnobs *SchemaChangerTestingKnobs
	// HistogramWindowInterval is (server.Context).HistogramWindowInterval.
	HistogramWindowInterval time.Duration

	// Caches updated by DistSQL.
	RangeDescriptorCache *kv.RangeDescriptorCache
	LeaseHolderCache     *kv.LeaseHolderCache
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
type StatementFilter func(context.Context, string, ResultsWriter, error) error

// ExecutorTestingKnobs is part of the context used to control parts of the
// system during testing.
type ExecutorTestingKnobs struct {
	// WaitForGossipUpdate causes metadata-mutating operations to wait
	// for the new metadata to back-propagate through gossip.
	WaitForGossipUpdate bool

	// CheckStmtStringChange causes Executor.execStmtGroup to verify that executed
	// statements are not modified during execution.
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
	AfterExecute func(
		ctx context.Context, stmt string, res StatementResult, err error,
	)

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

// DistSQLPlannerTestingKnobs is used to control internals of the distSQLPlanner
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
		distsqlrun.Version,
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
	stmt Statement, stmtStr string, session *Session, placeholderHints parser.PlaceholderTypes,
) (res *PreparedStatement, err error) {
	session.resetForBatch(e)
	sessionEventf(session, "preparing: %s", stmtStr)

	defer session.maybeRecover("preparing", stmtStr)

	prepared := &PreparedStatement{
		TypeHints:   placeholderHints,
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
	if err := placeholderHints.ProcessPlaceholderAnnotations(stmt.AST); err != nil {
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
		txn = client.NewTxn(e.cfg.DB, e.cfg.NodeID.Get())
		if err := txn.SetIsolation(session.DefaultIsolationLevel); err != nil {
			panic(fmt.Errorf("cannot set up txn for prepare %q: %v", stmtStr, err))
		}
		txn.Proto().OrigTimestamp = e.cfg.Clock.Now()
	}

	planner := session.newPlanner(e, txn)
	planner.semaCtx.Placeholders.SetTypeHints(placeholderHints)
	planner.evalCtx.PrepareOnly = true
	planner.evalCtx.ActiveMemAcc = &prepared.constantAcc

	if protoTS != nil {
		planner.avoidCachedDescriptors = true
		txn.SetFixedTimestamp(session.Ctx(), *protoTS)
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
	session *Session, stmts string, pinfo *parser.PlaceholderInfo, expectedNumResults int,
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
	session *Session, stmts string, pinfo *parser.PlaceholderInfo,
) error {
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
) error {
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
) error {
	if log.V(2) || logStatementsExecuteEnabled.Get(&e.cfg.Settings.SV) {
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
	return e.execParsed(session, stmts, pinfo, copyMsgNone)
}

// CopyData adds data to the COPY buffer and executes if there are enough rows.
func (e *Executor) CopyData(session *Session, data string) error {
	return e.execRequest(session, data, nil, copyMsgData)
}

// CopyDone executes the buffered COPY data.
func (e *Executor) CopyDone(session *Session) error {
	return e.execRequest(session, "", nil, copyMsgDone)
}

// CopyEnd ends the COPY mode. Any buffered data is discarded.
func (s *Session) CopyEnd(ctx context.Context) {
	s.copyFrom.Close(ctx)
	s.copyFrom = nil
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
) error {
	var stmts StatementList
	var err error
	txnState := &session.TxnState

	if log.V(2) || logStatementsExecuteEnabled.Get(&e.cfg.Settings.SV) {
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
		if log.V(2) || logStatementsExecuteEnabled.Get(&e.cfg.Settings.SV) {
			log.Infof(session.Ctx(), "execRequest: error: %v", err)
		}
		// A parse error occurred: we can't determine if there were multiple
		// statements or only one, so just pretend there was one.
		if txnState.mu.txn != nil {
			// Rollback the txn.
			err = txnState.updateStateAndCleanupOnErr(err, e)
		}
		return err
	}
	return e.execParsed(session, stmts, pinfo, copymsg)
}

// execParsed executes a batch of statements received as a unit from the client
// and returns query execution errors and communication errors.
func (e *Executor) execParsed(
	session *Session, stmts StatementList, pinfo *parser.PlaceholderInfo, copymsg copyMsg,
) error {
	var avoidCachedDescriptors bool
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
			if _, isBegin := stmts[0].AST.(*parser.BeginTransaction); !isBegin {
				autoCommit = true
				stmtsToExec = stmtsToExec[:1]
				// Check for AS OF SYSTEM TIME. If it is present but not detected here,
				// it will raise an error later on.
				var err error
				protoTS, err = isAsOf(session, stmtsToExec[0].AST, e.cfg.Clock.Now())
				if err != nil {
					return err
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
				autoCommit, /* implicitTxn */
				false,      /* retryIntent */
				e.cfg.Clock.PhysicalTime(), /* sqlTimestamp */
				session.DefaultIsolationLevel,
				roachpb.NormalUserPriority,
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
			protoTS, pinfo, avoidCachedDescriptors,
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

			// Release any leases the transaction(s) may have used.
			session.tables.releaseTables(session.Ctx())

			// Exec the schema changers (if the txn rolled back, the schema changers
			// will short-circuit because the corresponding descriptor mutation is not
			// found).
			if err := txnState.schemaChangers.execSchemaChanges(session.Ctx(), e, session); err != nil {
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
// returned as remainingStmts.
// txnPrefix: Set if stmtsToExec corresponds to the start of the current
// transaction. Used to trap nested BEGINs.
// autoCommit: If set, the transaction will be committed after running the
//   statement. If set, stmtsToExec can only contain a single statement.
//   If set, the transaction state will always be NoTxn when this function
//   returns, regardless of errors.
//   Errors encountered when committing are reported to the caller and are
//   indistinguishable from errors encountered while running the query.
// protoTS: If not nil, the transaction proto sets its Orig and Max timestamps
// to it each retry.
//
// Returns:
// remainingStmts: all the statements that were not executed.
// transitionToOpen: specifies if the caller should move from state AutoRetry to
// state Open. This will be false if the state is not AutoRetry when this
// returns.
// err: An error that occurred while executing the queries.
func runWithAutoRetry(
	e *Executor,
	session *Session,
	stmtsToExec StatementList,
	txnPrefix bool,
	autoCommit bool,
	protoTS *hlc.Timestamp,
	pinfo *parser.PlaceholderInfo,
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
			e, session, stmtsToExec, pinfo, origState,
			txnPrefix, avoidCachedDescriptors, automaticRetryCount, txnState.txnResults)

		// Sanity checks.
		if err != nil && txnState.TxnIsOpen() {
			log.Fatalf(session.Ctx(), "statement(s) generated error but state is %s. err: %s",
				txnState.State(), err)
		} else {
			state := txnState.State()
			if err == nil && (state == Aborted || state == RestartWait) {
				crash := true
				// SHOW TRANSACTION STATUS statements are always allowed regardless of
				// the transaction state.
				if len(stmtsToExec) > 0 {
					if _, ok := stmtsToExec[0].AST.(*parser.ShowTransactionStatus); ok {
						crash = false
					}
				}
				if crash {
					log.Fatalf(session.Ctx(), "no error, but we're in error state: %s", state)
				}
			}
		}

		// Check if we need to auto-commit. If so, we end the transaction now; the
		// transaction was only supposed to exist for the statement that we just
		// ran.
		if autoCommit {
			if err == nil {
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
					var skipCommit bool
					if e.cfg.TestingKnobs.BeforeAutoCommit != nil {
						err = e.cfg.TestingKnobs.BeforeAutoCommit(session.Ctx(), stmtsToExec[0].String())
						skipCommit = err != nil
					}
					if !skipCommit {
						err = txn.Commit(session.Ctx())
					}
					log.Eventf(session.Ctx(), "AutoCommit. err: %v\ntxn: %+v", err, txn.Proto())
					if err != nil {
						err = txnState.updateStateAndCleanupOnErr(err, e)
					}
				}
			}

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
// current transaction. Used to trap nested BEGINs.
// txnResults: used to push query results.
//
// It returns:
// remainingStmts: all the statements that were not executed.
// transitionToOpen: specifies if the caller should move from state AutoRetry to
// state Open. This will be false if the state is not AutoRetry when this
// returns.
func runTxnAttempt(
	e *Executor,
	session *Session,
	stmts StatementList,
	pinfo *parser.PlaceholderInfo,
	origState TxnStateEnum,
	txnPrefix bool,
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
			avoidCachedDescriptors, automaticRetryCount, stmtResult,
		); err != nil {
			return nil, false, err
		}

		// If the transaction is done, stop executing more statements.
		if !txnState.TxnIsOpen() {
			break
		}
	}

	// Flush the results accumulated in AutoRetry. We want possible future
	// Reset()s to not discard them since we're not going to retry the
	// statements. We also want future ResultsSentToClient() calls to return
	// false.
	if err := txnState.txnResults.Flush(session.Ctx()); err != nil {
		err = txnState.updateStateAndCleanupOnErr(err, e)
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
// avoidCachedDescriptors: Set if the statement execution should avoid
//   using cached descriptors.
// automaticRetryCount: Increases with each retry; 0 for the first attempt.
// res:                 Used to pass query results to the caller.
//
// Returns the error encountered while executing the query, if any.
func (e *Executor) execSingleStatement(
	session *Session,
	stmt Statement,
	pinfo *parser.PlaceholderInfo,
	firstInTxn bool,
	avoidCachedDescriptors bool,
	automaticRetryCount int,
	res StatementResult,
) error {
	txnState := &session.TxnState
	if txnState.State() == NoTxn {
		panic("execStmt called outside of a txn")
	}

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

	// Ignore statements that spawn jobs from SHOW QUERIES and from being cancellable
	// using CANCEL QUERY. Jobs have their own run control statements (CANCEL JOB,
	// PAUSE JOB, etc). We implement this ignore by not registering queryMeta in
	// session.mu.ActiveQueries.
	if _, ok := stmt.AST.(parser.HiddenFromShowQueries); !ok {
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
	// Run SHOW TRANSACTION STATUS in a separate code path so it is
	// always guaranteed to execute regardless of the current transaction state.
	if _, ok := stmt.AST.(*parser.ShowTransactionStatus); ok {
		err = runShowTransactionState(session, res)
	} else {
		switch txnState.State() {
		case Open, AutoRetry:
			err = e.execStmtInOpenTxn(
				session, stmt, pinfo, firstInTxn,
				avoidCachedDescriptors, automaticRetryCount, res)
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
		if err := filter(session.Ctx(), stmt.String(), session.ResultsWriter, err); err != nil {
			return err
		}
	}
	if err != nil {
		// If error contains a context cancellation error, wrap it with a
		// user-friendly query execution cancelled one.
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

// runShowTransactionState returns the state of current transaction.
func runShowTransactionState(session *Session, res StatementResult) error {
	res.BeginResult((*parser.ShowTransactionStatus)(nil))
	res.SetColumns(sqlbase.ResultColumns{{Name: "TRANSACTION STATUS", Typ: parser.TypeString}})

	state := getTransactionState(&session.TxnState)
	if err := res.AddRow(session.Ctx(), parser.Datums{parser.NewDString(state)}); err != nil {
		return err
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
	case *parser.CommitTransaction, *parser.RollbackTransaction:
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
		res.BeginResult((*parser.RollbackTransaction)(nil))
		return res.CloseResult()
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
				err = txnState.updateStateAndCleanupOnErr(err, e)
			}
			return err
		}
		if !txnState.retryIntent {
			err := fmt.Errorf("SAVEPOINT %s has not been used", parser.RestartSavepointName)
			if txnState.State() == RestartWait {
				err = txnState.updateStateAndCleanupOnErr(err, e)
			}
			return err
		}

		res.BeginResult((*parser.RollbackToSavepoint)(nil))
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
			curTs, curIso, curPri := txnState.sqlTimestamp, txnState.isolation, txnState.priority
			txnState.finishSQLTxn(session)
			txnState.resetForNewSQLTxn(
				e, session,
				false /* implicitTxn */, true, /* retryIntent */
				curTs /* sqlTimestamp */, curIso /* isolation */, curPri /* priority */)
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
	case *parser.CommitTransaction, *parser.RollbackTransaction:
		// Reset the state to allow new transactions to start.
		txnState.resetStateAndTxn(NoTxn)
		res.BeginResult((*parser.CommitTransaction)(nil))
		return res.CloseResult()
	default:
		err := sqlbase.NewTransactionCommittedError()
		return err
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
// avoidCachedDescriptors: set if the statement execution should avoid
//  using cached descriptors.
// automaticRetryCount: increases with each retry; 0 for the first attempt.
// res: the writer to send results to.
func (e *Executor) execStmtInOpenTxn(
	session *Session,
	stmt Statement,
	pinfo *parser.PlaceholderInfo,
	firstInTxn bool,
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

		if txnState.State() == AutoRetry && txnState.isSerializableRestart() {
			// If we just ran a statement synchronously, then the parallel queue
			// would have been synchronized first, so this would be a no-op.
			// However, if we just ran a statement asynchronously, then the
			// queue could still contain statements executing. So if we're in a
			// SerializableRestart state, we synchronize to drain the rest of
			// the stmts and clear the parallel batch's error-set before
			// restarting. If we did not drain the parallel stmts then the
			// txn.Reset call below might cause them to write at the wrong
			// epoch.
			//
			// TODO(nvanbenschoten): like in Session.Finish, we should try to
			// actively cancel these parallel queries instead of just waiting
			// for them to finish.
			if parErr := session.synchronizeParallelStmts(session.Ctx()); parErr != nil {
				// If synchronizing results in a non-retryable error, it takes priority.
				if _, ok := parErr.(*roachpb.HandledRetryableTxnError); !ok {
					err = parErr
					return
				}
			}

			// If we can still automatically retry the txn, and we detect that the
			// transaction won't be allowed to commit because its timestamp has been
			// pushed, we go ahead and retry now - if we'd execute further statements,
			// we probably wouldn't be allowed to retry automatically any more.
			txnState.mu.txn.Proto().Restart(
				0 /* userPriority */, 0 /* upgradePriority */, e.cfg.Clock.Now())
			// Force an auto-retry by returning a retryable error to the higher
			// levels.
			err = roachpb.NewHandledRetryableTxnError(
				"serializable transaction timestamp pushed (detected by SQL Executor)",
				txnState.mu.txn.ID(),
				// No updated transaction required; we've already manually updated our
				// client.Txn.
				roachpb.Transaction{},
			)
			err = txnState.updateStateAndCleanupOnErr(err, e)
		}
	}()

	// Check if the statement is parallelized or is independent from parallel
	// execution. If neither of these cases are true, we need to synchronize
	// parallel execution by letting it drain before we can begin executing ourselves.
	parallelize := IsStmtParallelized(stmt)
	_, independentFromParallelStmts := stmt.AST.(parser.IndependentFromParallelizedPriors)
	if !(parallelize || independentFromParallelStmts) {
		if err := session.synchronizeParallelStmts(session.Ctx()); err != nil {
			return err
		}
	}

	if txnState.implicitTxn && !stmtAllowedInImplicitTxn(stmt) {
		return errNoTransactionInProgress
	}

	switch s := stmt.AST.(type) {
	case *parser.BeginTransaction:
		if !firstInTxn {
			return errTransactionInProgress
		}

	case *parser.CommitTransaction:
		// CommitTransaction is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		transition = commitSQLTransaction(txnState, commit, res)
		explicitStateTransition = true
		return nil

	case *parser.ReleaseSavepoint:
		if err := parser.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return err
		}
		// ReleaseSavepoint is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		transition = commitSQLTransaction(txnState, release, res)
		explicitStateTransition = true
		return nil

	case *parser.RollbackTransaction:
		// Turn off test verification of metadata changes made by the
		// transaction.
		session.testingVerifyMetadataFn = nil
		// RollbackTransaction is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		transition = rollbackSQLTransaction(txnState, res)
		explicitStateTransition = true
		return nil

	case *parser.Savepoint:
		if err := parser.ValidateRestartCheckpoint(s.Name); err != nil {
			return err
		}
		// We want to disallow SAVEPOINTs to be issued after a transaction has
		// started running. The client txn's statement count indicates how many
		// statements have been executed as part of this transaction.
		if txnState.mu.txn.CommandCount() > 0 {
			return errors.Errorf("SAVEPOINT %s needs to be the first statement in a "+
				"transaction", parser.RestartSavepointName)
		}
		// Note that Savepoint doesn't have a corresponding plan node.
		// This here is all the execution there is.
		txnState.retryIntent = true
		res.BeginResult((*parser.Savepoint)(nil))
		return res.CloseResult()

	case *parser.RollbackToSavepoint:
		if err := parser.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return err
		}
		if !txnState.retryIntent {
			err := fmt.Errorf("SAVEPOINT %s has not been used", parser.RestartSavepointName)
			return err
		}

		res.BeginResult((*parser.Savepoint)(nil))
		if err := res.CloseResult(); err != nil {
			return err
		}

		// Move the state to AutoRetry; we're morally beginning a new transaction.
		txnState.SetState(AutoRetry)
		// If commands have already been sent through the transaction,
		// restart the client txn's proto to increment the epoch.
		if txnState.mu.txn.CommandCount() > 0 {
			// TODO(andrei): Should the timestamp below be e.cfg.Clock.Now(), so that
			// the transaction gets a new timestamp?
			txnState.mu.txn.Proto().Restart(
				0 /* userPriority */, 0 /* upgradePriority */, hlc.Timestamp{})
		}
		return nil

	case *parser.Prepare:
		// This must be handled here instead of the common path below
		// because we need to use the Executor reference.
		if err := e.PrepareStmt(session, s); err != nil {
			return err
		}
		res.BeginResult((*parser.Prepare)(nil))
		return res.CloseResult()

	case *parser.Execute:
		// Substitute the placeholder information and actual statement with that of
		// the saved prepared statement and pass control back to the ordinary
		// execute path.
		ps, newPInfo, err := getPreparedStatementForExecute(session, s)
		if err != nil {
			return err
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
	p.cancelChecker = sqlbase.NewCancelChecker(p.stmt.queryMeta.ctx)

	// constantMemAcc accounts for all constant folded values that are computed
	// prior to any rows being computed.
	constantMemAcc := p.evalCtx.Mon.MakeBoundAccount()
	p.evalCtx.ActiveMemAcc = &constantMemAcc
	defer constantMemAcc.Close(session.Ctx())

	if runInParallel {
		// Only run statements asynchronously through the parallelize queue if the
		// statements are parallelized and we're in a transaction. Parallelized
		// statements outside of a transaction are run synchronously with mocked
		// results, which has the same effect as running asynchronously but
		// immediately blocking.
		err = e.execStmtInParallel(stmt, p, res)
	} else {
		p.autoCommit = txnState.implicitTxn && !e.cfg.TestingKnobs.DisableAutoCommit
		err = e.execStmt(stmt, p, automaticRetryCount, res)
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
	case parser.RowsAffected, parser.Rows:
		tResult.count = res.RowsAffected()
	}
	sessionEventf(session, "%s done", tResult)
	return nil
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
	err := txnState.mu.txn.Rollback(txnState.Ctx)
	if err != nil {
		log.Warningf(txnState.Ctx, "txn rollback failed: %s", err)
	}
	// We're done with this txn.
	res.BeginResult((*parser.RollbackTransaction)(nil))
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

	res.BeginResult((*parser.CommitTransaction)(nil))
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

// exectDistSQL converts a classic plan to a distributed SQL physical plan and
// runs it.
func (e *Executor) execDistSQL(
	planner *planner, tree planNode, rowResultWriter StatementResult,
) error {
	ctx := planner.session.Ctx()
	recv, err := makeDistSQLReceiver(
		ctx, rowResultWriter,
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
	return nil
}

// execClassic runs a plan using the classic (non-distributed) SQL
// implementation.
func (e *Executor) execClassic(
	planner *planner, plan planNode, rowResultWriter StatementResult,
) error {
	ctx := planner.session.Ctx()

	// Create a BoundAccount to track the memory usage of each row.
	rowAcc := planner.evalCtx.Mon.MakeBoundAccount()
	planner.evalCtx.ActiveMemAcc = &rowAcc
	defer rowAcc.Close(ctx)

	if err := planner.startPlan(ctx, plan); err != nil {
		return err
	}

	params := runParams{
		ctx: ctx,
		p:   planner,
	}

	switch rowResultWriter.StatementType() {
	case parser.RowsAffected:
		count, err := countRowsAffected(params, plan)
		if err != nil {
			return err
		}
		rowResultWriter.IncrementRowsAffected(count)

	case parser.Rows:
		err := forEachRow(params, plan, func(values parser.Datums) error {
			for _, val := range values {
				if err := checkResultType(val.ResolvedType()); err != nil {
					return err
				}
			}
			return rowResultWriter.AddRow(ctx, values)
		})
		if err != nil {
			return err
		}
	case parser.DDL:
		if n, ok := plan.(*createTableNode); ok && n.n.As() {
			rowResultWriter.IncrementRowsAffected(n.count)
		}
	}
	return nil
}

// forEachRow calls the provided closure for each successful call to
// planNode.Next with planNode.Values, making sure to properly track memory
// usage.
func forEachRow(params runParams, p planNode, f func(parser.Datums) error) error {
	next, err := p.Next(params)
	for ; next; next, err = p.Next(params) {
		// If we're tracking memory, clear the previous row's memory account.
		if params.p.evalCtx.ActiveMemAcc != nil {
			params.p.evalCtx.ActiveMemAcc.Clear(params.ctx)
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
	err := forEachRow(params, p, func(_ parser.Datums) error {
		count++
		return nil
	})
	return count, err
}

// shouldUseDistSQL determines whether we should use DistSQL for a plan, based
// on the session settings.
func (e *Executor) shouldUseDistSQL(planner *planner, plan planNode) (bool, error) {
	distSQLMode := planner.session.DistSQLMode
	if distSQLMode == DistSQLOff {
		return false, nil
	}
	// Don't try to run empty nodes (e.g. SET commands) with distSQL.
	if _, ok := plan.(*zeroNode); ok {
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
		// If the distSQLMode is ALWAYS, reject anything but SET.
		if distSQLMode == DistSQLAlways && err != setNotSupportedError {
			return false, err
		}
		// Don't use distSQL for this request.
		log.VEventf(planner.session.Ctx(), 1, "query not supported for distSQL: %s", err)
		return false, nil
	}

	if distSQLMode == DistSQLAuto && !distribute {
		log.VEventf(planner.session.Ctx(), 1, "not distributing query")
		return false, nil
	}

	// In ON or ALWAYS mode, all supported queries are distributed.
	return true, nil
}

// initStatementResult initializes res according to a query.
func initStatementResult(res StatementResult, stmt Statement, plan planNode) error {
	stmtAst := stmt.AST
	res.BeginResult(stmtAst)
	if stmtAst.StatementType() == parser.Rows {
		columns := planColumns(plan)
		res.SetColumns(columns)
		for _, c := range columns {
			if err := checkResultType(c.Typ); err != nil {
				return err
			}
		}
	}
	return nil
}

// execStmt executes the statement synchronously and writes the result to
// res.
func (e *Executor) execStmt(
	stmt Statement, planner *planner, automaticRetryCount int, res StatementResult,
) error {
	session := planner.session
	ctx := session.Ctx()

	planner.phaseTimes[plannerStartLogicalPlan] = timeutil.Now()
	plan, err := planner.makePlan(ctx, stmt)
	planner.phaseTimes[plannerEndLogicalPlan] = timeutil.Now()
	if err != nil {
		return err
	}

	defer plan.Close(ctx)

	err = initStatementResult(res, stmt, plan)
	if err != nil {
		return err
	}

	useDistSQL, err := e.shouldUseDistSQL(planner, plan)
	if err != nil {
		return err
	}

	if e.cfg.TestingKnobs.BeforeExecute != nil {
		e.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), false /* isParallel */)
	}

	planner.phaseTimes[plannerStartExecStmt] = timeutil.Now()
	session.setQueryExecutionMode(stmt.queryID, useDistSQL, false /* isParallel */)
	if useDistSQL {
		err = e.execDistSQL(planner, plan, res)
	} else {
		err = e.execClassic(planner, plan, res)
	}
	planner.phaseTimes[plannerEndExecStmt] = timeutil.Now()
	e.recordStatementSummary(
		planner, stmt, useDistSQL, automaticRetryCount, res, err,
	)
	if e.cfg.TestingKnobs.AfterExecute != nil {
		e.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), res, err)
	}
	if err != nil {
		return err
	}
	return res.CloseResult()
}

// execStmtInParallel executes the statement asynchronously and writes mocked
// out results to res. These mocked out results will be the "zero value"
// of the statement's result type:
// - parser.Rows -> an empty set of rows
// - parser.RowsAffected -> zero rows affected
//
// TODO(nvanbenschoten): We do not currently support parallelizing distributed SQL
// queries, so this method can only be used with classical SQL.
// TODO(andrei): the mocking of results business should be done by the caller.
func (e *Executor) execStmtInParallel(
	stmt Statement, planner *planner, res StatementResult,
) (retErr error) {
	session := planner.session
	ctx := session.Ctx()
	params := runParams{
		ctx: ctx,
		p:   planner,
	}

	plan, err := planner.makePlan(ctx, stmt)
	if err != nil {
		return err
	}

	err = initStatementResult(res, stmt, plan)
	if err != nil {
		return err
	}

	// This ensures we don't unintentionally clean up the queryMeta object when we
	// send the mock result back to the client.
	session.setQueryExecutionMode(stmt.queryID, false /* isDistributed */, true /* isParallel */)

	if err := session.parallelizeQueue.Add(params, plan, func(plan planNode) error {
		// TODO(andrei): this should really be a result writer implementation that
		// does nothing.
		bufferedWriter := newBufferedWriter(session.makeBoundAccount())
		err := initStatementResult(bufferedWriter, stmt, plan)
		if err != nil {
			return err
		}

		if e.cfg.TestingKnobs.BeforeExecute != nil {
			e.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), true /* isParallel */)
		}

		planner.phaseTimes[plannerStartExecStmt] = timeutil.Now()
		err = e.execClassic(planner, plan, bufferedWriter)
		planner.phaseTimes[plannerEndExecStmt] = timeutil.Now()
		e.recordStatementSummary(planner, stmt, false, 0, bufferedWriter, err)
		if e.cfg.TestingKnobs.AfterExecute != nil {
			e.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), bufferedWriter, err)
		}
		results := bufferedWriter.results()
		results.Close(ctx)
		// Deregister query from registry.
		session.removeActiveQuery(stmt.queryID)
		return err
	}); err != nil {
		return err
	}

	return res.CloseResult()
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
	te, err := asOf.Expr.TypeCheck(nil, parser.TypeString)
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
	case *parser.DString:
		s := string(*d)
		// Allow nanosecond precision because the timestamp is only used by the
		// system and won't be returned to the user over pgwire.
		if dt, err := parser.ParseDTimestamp(s, time.Nanosecond); err == nil {
			ts.WallTime = dt.Time.UnixNano()
			break
		}
		// Attempt to parse as a decimal.
		if dec, _, err := apd.NewFromString(s); err != nil {
			// Override the error. It would be misleading to fail with a
			// DECIMAL conversion error if a user was attempting to use a
			// timestamp string and the conversion above failed.
			convErr = errors.Errorf("AS OF SYSTEM TIME: value is neither timestamp nor decimal")
		} else {
			ts, convErr = decimalToHLC(dec)
		}
	case *parser.DInt:
		ts.WallTime = int64(*d)
	case *parser.DDecimal:
		ts, convErr = decimalToHLC(&d.Decimal)
	default:
		convErr = errors.Errorf("AS OF SYSTEM TIME: expected timestamp, got %s (%T)", d.ResolvedType(), d)
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
