// Copyright 2016 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/repstream"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catsessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry/schematelemetrycontroller"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/evalcatalog"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/idxusage"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
)

// extendedEvalContext extends eval.Context with fields that are needed for
// distsql planning.
type extendedEvalContext struct {
	eval.Context

	// SessionID for this connection.
	SessionID clusterunique.ID

	// VirtualSchemas can be used to access virtual tables.
	VirtualSchemas VirtualTabler

	// Tracing provides access to the session's tracing interface. Changes to the
	// tracing state should be done through the sessionDataMutator.
	Tracing *SessionTracing

	// NodesStatusServer gives access to the NodesStatus service. Unavailable to
	// tenants.
	NodesStatusServer serverpb.OptionalNodesStatusServer

	// TenantStatusServer gives access to tenant status in the cluster.
	TenantStatusServer serverpb.TenantStatusServer

	// SQLStatusServer gives access to a subset of the serverpb.StatusServer
	// that is available to both system and non-system tenants.
	SQLStatusServer serverpb.SQLStatusServer

	// MemMetrics represent the group of metrics to which execution should
	// contribute.
	MemMetrics *MemoryMetrics

	// Tables points to the Session's table collection (& cache).
	Descs *descs.Collection

	ExecCfg *ExecutorConfig

	DistSQLPlanner *DistSQLPlanner

	TxnModesSetter txnModesSetter

	// jobs refers to jobs in extraTxnState.
	jobs *txnJobsCollection

	statsProvider *persistedsqlstats.PersistedSQLStats

	indexUsageStats *idxusage.LocalIndexUsageStats

	SchemaChangerState *SchemaChangerState

	statementPreparer statementPreparer
}

// copyFromExecCfg copies relevant fields from an ExecutorConfig.
func (evalCtx *extendedEvalContext) copyFromExecCfg(execCfg *ExecutorConfig) {
	evalCtx.ExecCfg = execCfg
	evalCtx.Settings = execCfg.Settings
	evalCtx.Codec = execCfg.Codec
	evalCtx.Tracer = execCfg.AmbientCtx.Tracer
	if execCfg.SQLLiveness != nil { // nil in some tests
		evalCtx.SQLLivenessReader = execCfg.SQLLiveness.CachedReader()
	}
	evalCtx.CompactEngineSpan = execCfg.CompactEngineSpanFunc
	evalCtx.SetCompactionConcurrency = execCfg.CompactionConcurrencyFunc
	evalCtx.TestingKnobs = execCfg.EvalContextTestingKnobs
	evalCtx.ClusterID = execCfg.NodeInfo.LogicalClusterID()
	evalCtx.ClusterName = execCfg.RPCContext.ClusterName()
	evalCtx.NodeID = execCfg.NodeInfo.NodeID
	evalCtx.Locality = execCfg.Locality
	evalCtx.OriginalLocality = execCfg.Locality
	evalCtx.NodesStatusServer = execCfg.NodesStatusServer
	evalCtx.TenantStatusServer = execCfg.TenantStatusServer
	evalCtx.SQLStatusServer = execCfg.SQLStatusServer
	evalCtx.DistSQLPlanner = execCfg.DistSQLPlanner
	evalCtx.VirtualSchemas = execCfg.VirtualSchemas
	evalCtx.KVStoresIterator = execCfg.KVStoresIterator
}

// copy returns a deep copy of ctx.
func (evalCtx *extendedEvalContext) copy() *extendedEvalContext {
	cpy := *evalCtx
	cpy.Context = *evalCtx.Context.Copy()
	return &cpy
}

// QueueJob creates a new job from record and queues it for execution after
// the transaction commits.
func (evalCtx *extendedEvalContext) QueueJob(record *jobs.Record) jobspb.JobID {
	jobID := evalCtx.ExecCfg.JobRegistry.MakeJobID()
	record.JobID = jobID
	evalCtx.jobs.addNonUniqueJobToCreate(record)
	return jobID
}

// planner is the centerpiece of SQL statement execution combining session
// state and database state with the logic for SQL execution. It is logically
// scoped to the execution of a single statement, and should not be used to
// execute multiple statements. It is not safe to use the same planner from
// multiple goroutines concurrently.
//
// planners are usually created by using the newPlanner method on a Session.
// If one needs to be created outside of a Session, use makeInternalPlanner().
type planner struct {
	schemaResolver

	txn *kv.Txn

	// internalSQLTxn corresponds to the object returned from InternalSQLTxn.
	// It is here to avoid the need to allocate another structure. The value
	// is initialized lazily. The assumption is that that method is called
	// during statement execution when the planner is in a valid state.
	// The internalSQLTxn may hold on to a stale txn reference and should
	// never be accessed directly. Nothing explicitly resets this field.
	internalSQLTxn internalTxn

	// isInternalPlanner is set to true when this planner is not bound to
	// a SQL session.
	isInternalPlanner bool

	atomic struct {
		// innerPlansMustUseLeafTxn is set to 1 if the "outer" plan is using
		// the LeafTxn forcing the "inner" plans to use the LeafTxns too. An
		// example of this is apply-join iterations when the main query has
		// concurrency.
		//
		// Note that even though the planner is not safe for concurrent usage,
		// the "outer" plan modifies this field _before_ the "inner" plans start
		// or _after_ the "inner" plans finish, so we could have avoided the
		// usage of an atomic here, but we choose to be defensive about it.
		// TODO(yuzefovich): this is a bit hacky. The problem is that the
		// incorrect txn on the planner has already been captured by the
		// planNodeToRowSource adapter before the "outer" query figured out that
		// it must use the LeafTxn. Solving that issue properly is not trivial
		// and is tracked in #41992.
		innerPlansMustUseLeafTxn uint32
	}

	monitor *mon.BytesMonitor

	// Corresponding Statement for this query.
	stmt Statement

	// StmtWithHomeRegionEnforced, if non-nil is the SQL statement for which a
	// home region is being enforced.
	StmtNoConstantsWithHomeRegionEnforced string

	// pausablePortal is set when the query is from a pausable portal.
	pausablePortal *PreparedPortal

	instrumentation instrumentationHelper

	// Contexts for different stages of planning and execution.
	semaCtx         tree.SemaContext
	extendedEvalCtx extendedEvalContext

	// sessionDataMutatorIterator is used to mutate the session variables. Read
	// access to them is provided through evalCtx.
	sessionDataMutatorIterator *sessionDataMutatorIterator

	// execCfg is used to access the server configuration for the Executor.
	execCfg *ExecutorConfig

	preparedStatements preparedStatementsAccessor

	sqlCursors sqlCursors

	createdSequences createdSequences

	// autoCommit indicates whether the plan is allowed (but not required) to
	// commit the transaction along with other KV operations. Committing the txn
	// might be beneficial because it may enable the 1PC optimization. Note that
	// autocommit may be false for implicit transactions; for example, an implicit
	// transaction is used for all the statements sent in a batch at the same
	// time.
	//
	// NOTE: plan node must be configured appropriately to actually perform an
	// auto-commit. This is dependent on information from the optimizer.
	autoCommit bool

	// cancelChecker is used by planNodes to check for cancellation of the associated
	// query.
	cancelChecker cancelchecker.CancelChecker

	// curPlan collects the properties of the current plan being prepared. This state
	// is undefined at the beginning of the planning of each new statement, and cannot
	// be reused for an old prepared statement after a new statement has been prepared.
	curPlan planTop

	// Avoid allocations by embedding commonly used objects and visitors.
	txCtx                 transform.ExprTransformContext
	nameResolutionVisitor schemaexpr.NameResolutionVisitor
	tableName             tree.TableName

	// optPlanningCtx stores the optimizer planning context, which contains
	// data structures that can be reused between queries (for efficiency).
	optPlanningCtx optPlanningCtx

	// noticeSender allows the sending of notices.
	// Do not use this object directly; use the BufferClientNotice() method
	// instead.
	noticeSender noticeSender

	queryCacheSession querycache.Session

	// evalCatalogBuiltins is used as part of the eval.Context.
	evalCatalogBuiltins evalcatalog.Builtins

	// trackDependency is used to track circular dependencies when dropping views.
	trackDependency map[catid.DescID]bool
}

// hasFlowForPausablePortal returns true if the planner is for re-executing a
// portal. We reuse the flow stored in p.pausablePortal.pauseInfo.
func (p *planner) hasFlowForPausablePortal() bool {
	return p.pausablePortal != nil && p.pausablePortal.pauseInfo != nil && p.pausablePortal.pauseInfo.resumableFlow.flow != nil
}

// resumeFlowForPausablePortal is called when re-executing a portal. We reuse
// the flow with a new receiver, without re-generating the physical plan.
func (p *planner) resumeFlowForPausablePortal(recv *DistSQLReceiver) error {
	if !p.hasFlowForPausablePortal() {
		return errors.AssertionFailedf("no flow found for pausable portal")
	}
	recv.discardRows = p.instrumentation.ShouldDiscardRows()
	recv.outputTypes = p.pausablePortal.pauseInfo.resumableFlow.outputTypes
	flow := p.pausablePortal.pauseInfo.resumableFlow.flow
	finishedSetupFn, cleanup := getFinishedSetupFn(p)
	finishedSetupFn(flow)
	defer cleanup()
	flow.Resume(recv)
	return recv.commErr
}

func (evalCtx *extendedEvalContext) setSessionID(sessionID clusterunique.ID) {
	evalCtx.SessionID = sessionID
}

// noteworthyInternalMemoryUsageBytes is the minimum size tracked by each
// internal SQL pool before the pool starts explicitly logging overall usage
// growth in the log.
var noteworthyInternalMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_INTERNAL_MEMORY_USAGE", 1<<20 /* 1 MB */)

// internalPlannerParams encapsulates configurable planner fields. The defaults
// are set in newInternalPlanner.
type internalPlannerParams struct {
	collection *descs.Collection
}

// InternalPlannerParamsOption is an option that can be passed to
// NewInternalPlanner.
type InternalPlannerParamsOption func(*internalPlannerParams)

// WithDescCollection configures the planner with the provided collection
// instead of the default (creating a new one from scratch).
func WithDescCollection(collection *descs.Collection) InternalPlannerParamsOption {
	return func(params *internalPlannerParams) {
		params.collection = collection
	}
}

// NewInternalPlanner is an exported version of newInternalPlanner. It
// returns an interface{} so it can be used outside of the sql package.
func NewInternalPlanner(
	opName string,
	txn *kv.Txn,
	user username.SQLUsername,
	memMetrics *MemoryMetrics,
	execCfg *ExecutorConfig,
	sessionData sessiondatapb.SessionData,
	opts ...InternalPlannerParamsOption,
) (interface{}, func()) {
	return newInternalPlanner(opName, txn, user, memMetrics, execCfg, sessionData, opts...)
}

// newInternalPlanner creates a new planner instance for internal usage. This
// planner is not associated with a sql session.
//
// Since it can't be reset, the planner can be used only for planning a single
// statement.
//
// Returns a cleanup function that must be called once the caller is done with
// the planner.
func newInternalPlanner(
	// TODO(yuzefovich): make this redact.RedactableString.
	opName string,
	txn *kv.Txn,
	user username.SQLUsername,
	memMetrics *MemoryMetrics,
	execCfg *ExecutorConfig,
	sessionData sessiondatapb.SessionData,
	opts ...InternalPlannerParamsOption,
) (*planner, func()) {
	// Default parameters which may be override by the supplied options.
	params := &internalPlannerParams{}
	for _, opt := range opts {
		opt(params)
	}
	callerSuppliedDescsCollection := params.collection != nil

	// We need a context that outlives all the uses of the planner (since the
	// planner captures it in the EvalCtx, and so does the cleanup function that
	// we're going to return. We just create one here instead of asking the caller
	// for a ctx with this property. This is really ugly, but the alternative of
	// asking the caller for one is hard to explain. What we need is better and
	// separate interfaces for planning and running plans, which could take
	// suitable contexts.
	ctx := logtags.AddTag(context.Background(), opName, "")

	sd := &sessiondata.SessionData{
		SessionData:   sessionData,
		SearchPath:    sessiondata.DefaultSearchPathForUser(user),
		SequenceState: sessiondata.NewSequenceState(),
		Location:      time.UTC,
	}
	if sd.SessionData.Database == "" {
		sd.SessionData.Database = "system"
	}
	sd.SessionData.UserProto = user.EncodeProto()
	sd.SessionData.Internal = true
	sds := sessiondata.NewStack(sd)

	if params.collection == nil {
		dsdp := catsessiondata.NewDescriptorSessionDataStackProvider(sds)
		params.collection = execCfg.CollectionFactory.NewCollection(
			ctx, descs.WithDescriptorSessionDataProvider(dsdp),
		)
	}

	var ts time.Time
	if txn != nil {
		readTimestamp := txn.ReadTimestamp()
		if readTimestamp.IsEmpty() {
			panic("makeInternalPlanner called with a transaction without timestamps")
		}
		ts = readTimestamp.GoTime()
	}

	p := &planner{execCfg: execCfg}

	p.txn = txn
	p.stmt = Statement{}
	p.cancelChecker.Reset(ctx)
	p.isInternalPlanner = true

	p.semaCtx = tree.MakeSemaContext()
	p.semaCtx.SearchPath = &sd.SearchPath
	p.semaCtx.TypeResolver = p
	p.semaCtx.FunctionResolver = p
	p.semaCtx.DateStyle = sd.GetDateStyle()
	p.semaCtx.IntervalStyle = sd.GetIntervalStyle()

	plannerMon := mon.NewMonitor(redact.Sprintf("internal-planner.%s.%s", user, opName),
		mon.MemoryResource,
		memMetrics.CurBytesCount, memMetrics.MaxBytesHist,
		-1, /* increment */
		noteworthyInternalMemoryUsageBytes, execCfg.Settings)
	plannerMon.StartNoReserved(ctx, execCfg.RootMemoryMonitor)
	p.monitor = plannerMon

	smi := &sessionDataMutatorIterator{
		sds: sds,
		sessionDataMutatorBase: sessionDataMutatorBase{
			defaults: SessionDefaults(map[string]string{
				"application_name": "crdb-internal",
				"database":         sd.SessionData.Database,
			}),
			settings: execCfg.Settings,
		},
		sessionDataMutatorCallbacks: sessionDataMutatorCallbacks{},
	}

	p.extendedEvalCtx = internalExtendedEvalCtx(ctx, sds, params.collection, txn, ts, ts, execCfg)
	p.extendedEvalCtx.Planner = p
	p.extendedEvalCtx.StreamManagerFactory = p
	p.extendedEvalCtx.PrivilegedAccessor = p
	p.extendedEvalCtx.SessionAccessor = p
	p.extendedEvalCtx.ClientNoticeSender = p
	p.extendedEvalCtx.Sequence = p
	p.extendedEvalCtx.Tenant = p
	p.extendedEvalCtx.Regions = p
	p.extendedEvalCtx.JoinTokenCreator = p
	p.extendedEvalCtx.Gossip = p
	p.extendedEvalCtx.ClusterID = execCfg.NodeInfo.LogicalClusterID()
	p.extendedEvalCtx.ClusterName = execCfg.RPCContext.ClusterName()
	p.extendedEvalCtx.NodeID = execCfg.NodeInfo.NodeID
	p.extendedEvalCtx.Locality = execCfg.Locality
	p.extendedEvalCtx.OriginalLocality = execCfg.Locality

	p.sessionDataMutatorIterator = smi
	p.autoCommit = false

	p.extendedEvalCtx.MemMetrics = memMetrics
	p.extendedEvalCtx.ExecCfg = execCfg
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
	p.extendedEvalCtx.Descs = params.collection

	p.queryCacheSession.Init()
	p.optPlanningCtx.init(p)
	p.createdSequences = emptyCreatedSequences{}

	p.schemaResolver.descCollection = p.Descriptors()
	p.schemaResolver.sessionDataStack = sds
	p.schemaResolver.txn = p.txn
	p.schemaResolver.authAccessor = p
	p.evalCatalogBuiltins.Init(execCfg.Codec, p.txn, p.Descriptors())

	return p, func() {
		// Note that we capture ctx here. This is only valid as long as we create
		// the context as explained at the top of the method.
		if !callerSuppliedDescsCollection {
			// The collection will accumulate descriptors read during planning as well
			// as type descriptors read during execution on the local node. Many users
			// of the internal planner do set the `skipCache` flag on the resolver but
			// this is not respected by type resolution underneath execution. That
			// subtle details means that the type descriptor used by execution may be
			// stale, but that must be okay. Correctness concerns aside, we must release
			// the leases to ensure that we don't leak a descriptor lease.
			p.Descriptors().ReleaseAll(ctx)
		}

		// Stop the memory monitor.
		plannerMon.Stop(ctx)
	}
}

// internalExtendedEvalCtx creates an evaluation context for an "internal
// planner". Since the eval context is supposed to be tied to a session and
// there's no session to speak of here, different fields are filled in here to
// keep the tests using the internal planner passing.
func internalExtendedEvalCtx(
	ctx context.Context,
	sds *sessiondata.Stack,
	tables *descs.Collection,
	txn *kv.Txn,
	txnTimestamp time.Time,
	stmtTimestamp time.Time,
	execCfg *ExecutorConfig,
) extendedEvalContext {
	evalContextTestingKnobs := execCfg.EvalContextTestingKnobs

	var indexUsageStats *idxusage.LocalIndexUsageStats
	var sqlStatsController eval.SQLStatsController
	var schemaTelemetryController eval.SchemaTelemetryController
	var indexUsageStatsController eval.IndexUsageStatsController
	if ief := execCfg.InternalDB; ief != nil {
		if ief.server != nil {
			indexUsageStats = ief.server.indexUsageStats
			sqlStatsController = ief.server.sqlStatsController
			schemaTelemetryController = ief.server.schemaTelemetryController
			indexUsageStatsController = ief.server.indexUsageStatsController
		} else {
			// If the indexUsageStats is nil from the sql.Server, we create a dummy
			// index usage stats collector. The sql.Server in the ExecutorConfig
			// is only nil during tests.
			indexUsageStats = idxusage.NewLocalIndexUsageStats(&idxusage.Config{
				Setting: execCfg.Settings,
			})
			sqlStatsController = &persistedsqlstats.Controller{}
			schemaTelemetryController = &schematelemetrycontroller.Controller{}
			indexUsageStatsController = &idxusage.Controller{}
		}
	}
	ret := extendedEvalContext{
		Context: eval.Context{
			Txn:                            txn,
			SessionDataStack:               sds,
			TxnReadOnly:                    false,
			TxnImplicit:                    true,
			TxnIsSingleStmt:                true,
			TestingKnobs:                   evalContextTestingKnobs,
			StmtTimestamp:                  stmtTimestamp,
			TxnTimestamp:                   txnTimestamp,
			SQLStatsController:             sqlStatsController,
			SchemaTelemetryController:      schemaTelemetryController,
			IndexUsageStatsController:      indexUsageStatsController,
			ConsistencyChecker:             execCfg.ConsistencyChecker,
			StmtDiagnosticsRequestInserter: execCfg.StmtDiagnosticsRecorder.InsertRequest,
			RangeStatsFetcher:              execCfg.RangeStatsFetcher,
		},
		Tracing:         &SessionTracing{},
		Descs:           tables,
		indexUsageStats: indexUsageStats,
	}
	ret.SetDeprecatedContext(ctx)
	ret.copyFromExecCfg(execCfg)
	return ret
}

// SemaCtx provides access to the planner's SemaCtx.
func (p *planner) SemaCtx() *tree.SemaContext {
	return &p.semaCtx
}

// Note: if the context will be modified, use ExtendedEvalContextCopy instead.
func (p *planner) ExtendedEvalContext() *extendedEvalContext {
	return &p.extendedEvalCtx
}

func (p *planner) ExtendedEvalContextCopy() *extendedEvalContext {
	return p.extendedEvalCtx.copy()
}

// EvalContext() provides convenient access to the planner's EvalContext().
func (p *planner) EvalContext() *eval.Context {
	return &p.extendedEvalCtx.Context
}

// Descriptors implements the PlanHookState interface.
func (p *planner) Descriptors() *descs.Collection {
	return p.extendedEvalCtx.Descs
}

// Mon is part of the eval.Planner interface.
func (p *planner) Mon() *mon.BytesMonitor {
	return p.monitor
}

// ExecCfg implements the PlanHookState interface.
func (p *planner) ExecCfg() *ExecutorConfig {
	return p.extendedEvalCtx.ExecCfg
}

// ExprEvaluator implements the PlanHookState interface.
func (p *planner) ExprEvaluator(op string) exprutil.Evaluator {
	return exprutil.MakeEvaluator(op, p.SemaCtx(), p.EvalContext())
}

// GetOrInitSequenceCache returns the sequence cache for the session.
// If the sequence cache has not been used yet, it initializes the cache
// inside the session data.
func (p *planner) GetOrInitSequenceCache() sessiondatapb.SequenceCache {
	if p.SessionData().SequenceCache == nil {
		p.sessionDataMutatorIterator.applyOnEachMutator(
			func(m sessionDataMutator) {
				m.initSequenceCache()
			},
		)
	}
	return p.SessionData().SequenceCache
}

func (p *planner) LeaseMgr() *lease.Manager {
	return p.execCfg.LeaseManager
}

func (p *planner) Txn() *kv.Txn {
	return p.txn
}

func (p *planner) InternalSQLTxn() descs.Txn {
	if p.txn == nil {
		return nil
	}

	// We lazily initialize the internalSQLTxn structure so that we don't have
	// to pay to initialize this structure if the statement being executed does
	// not execute internal sql statements.
	if p.internalSQLTxn.txn != p.txn {
		ief := p.ExecCfg().InternalDB
		ie := MakeInternalExecutor(ief.server, ief.memMetrics, ief.monitor)
		ie.SetSessionData(p.SessionData())
		ie.extraTxnState = &extraTxnState{
			txn:                p.Txn(),
			descCollection:     p.Descriptors(),
			jobs:               p.extendedEvalCtx.jobs,
			schemaChangerState: p.extendedEvalCtx.SchemaChangerState,
		}
		p.internalSQLTxn.init(p.txn, ie)
	}
	return &p.internalSQLTxn
}

func (p *planner) User() username.SQLUsername {
	return p.SessionData().User()
}

func (p *planner) TemporarySchemaName() string {
	return temporarySchemaName(p.ExtendedEvalContext().SessionID)
}

// DistSQLPlanner returns the DistSQLPlanner
func (p *planner) DistSQLPlanner() *DistSQLPlanner {
	return p.extendedEvalCtx.DistSQLPlanner
}

// MigrationJobDeps returns the upgrade.JobDeps.
func (p *planner) MigrationJobDeps() upgrade.JobDeps {
	return p.execCfg.UpgradeJobDeps
}

// SpanConfigReconciler returns the spanconfig.Reconciler.
func (p *planner) SpanConfigReconciler() spanconfig.Reconciler {
	return p.execCfg.SpanConfigReconciler
}

func (p *planner) SpanStatsConsumer() keyvisualizer.SpanStatsConsumer {
	return p.execCfg.SpanStatsConsumer
}

// GetTypeFromValidSQLSyntax implements the eval.Planner interface.
// We define this here to break the dependency from eval.go to the parser.
func (p *planner) GetTypeFromValidSQLSyntax(sql string) (*types.T, error) {
	ref, err := parser.GetTypeFromValidSQLSyntax(sql)
	if err != nil {
		return nil, err
	}
	return tree.ResolveType(context.TODO(), ref, p.semaCtx.GetTypeResolver())
}

// ResolveTableName implements the eval.DatabaseCatalog interface.
func (p *planner) ResolveTableName(ctx context.Context, tn *tree.TableName) (tree.ID, error) {
	flags := tree.ObjectLookupFlags{
		Required:          true,
		DesiredObjectKind: tree.TableObject,
	}
	_, desc, err := resolver.ResolveExistingTableObject(ctx, p, tn, flags)
	if err != nil {
		return 0, err
	}
	return tree.ID(desc.GetID()), nil
}

// CheckPrivilegeForTableID implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) CheckPrivilegeForTableID(
	ctx context.Context, tableID descpb.ID, privilege privilege.Kind,
) error {
	desc, err := p.LookupTableByID(ctx, tableID)
	if err != nil {
		return err
	}
	return p.CheckPrivilegeForUser(ctx, desc, privilege, p.User())
}

// LookupTableByID looks up a table, by the given descriptor ID. Based on the
// CommonLookupFlags, it could use or skip the Collection cache.
func (p *planner) LookupTableByID(
	ctx context.Context, tableID descpb.ID,
) (catalog.TableDescriptor, error) {
	table, err := p.byIDGetterBuilder().WithoutNonPublic().Get().Table(ctx, tableID)
	if err != nil {
		return nil, err
	}
	return table, nil
}

// SessionData is part of the PlanHookState interface.
func (p *planner) SessionData() *sessiondata.SessionData {
	return p.EvalContext().SessionData()
}

// SessionDataMutatorIterator is part of the PlanHookState interface.
func (p *planner) SessionDataMutatorIterator() *sessionDataMutatorIterator {
	return p.sessionDataMutatorIterator
}

// Ann is a shortcut for the Annotations from the eval context.
func (p *planner) Ann() *tree.Annotations {
	return p.ExtendedEvalContext().Context.Annotations
}

// ExecutorConfig implements Planner interface.
func (p *planner) ExecutorConfig() interface{} {
	return p.execCfg
}

// statementPreparer is an interface used when deserializing a session in order
// to prepare statements.
type statementPreparer interface {
	// addPreparedStmt creates a prepared statement with the given name and type
	// hints, and returns it.
	addPreparedStmt(
		ctx context.Context,
		name string,
		stmt Statement,
		placeholderHints tree.PlaceholderTypes,
		rawTypeHints []oid.Oid,
		origin PreparedStatementOrigin,
	) (*PreparedStatement, error)
}

var _ statementPreparer = &connExecutor{}

// txnModesSetter is an interface used by SQL execution to influence the current
// transaction.
type txnModesSetter interface {
	// setTransactionModes updates some characteristics of the current
	// transaction.
	// asOfTs, if not empty, is the evaluation of modes.AsOf.
	setTransactionModes(ctx context.Context, modes tree.TransactionModes, asOfTs hlc.Timestamp) error
}

// validateDescriptor is a convenience function for validating
// descriptors in the context of a planner.
func validateDescriptor(ctx context.Context, p *planner, descriptor catalog.Descriptor) error {
	return p.Descriptors().Validate(
		ctx,
		p.Txn(),
		catalog.NoValidationTelemetry,
		catalog.ValidationLevelBackReferences,
		descriptor,
	)
}

// IsActive implements the Planner interface.
func (p *planner) IsActive(ctx context.Context, key clusterversion.Key) bool {
	return p.execCfg.Settings.Version.IsActive(ctx, key)
}

// QueryRowEx executes the supplied SQL statement and returns a single row, or
// nil if no row is found, or an error if more that one row is returned.
//
// The fields set in session that are set override the respective fields if
// they have previously been set through SetSessionData().
func (p *planner) QueryRowEx(
	ctx context.Context,
	opName string,
	override sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	return p.InternalSQLTxn().QueryRowEx(ctx, opName, p.Txn(), override, stmt, qargs...)
}

// ExecEx is like Exec, but allows the caller to override some session data
// fields (e.g. the user).
func (p *planner) ExecEx(
	ctx context.Context,
	opName string,
	override sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (int, error) {
	return p.InternalSQLTxn().ExecEx(ctx, opName, p.Txn(), override, stmt, qargs...)
}

// QueryIteratorEx executes the query, returning an iterator that can be used
// to get the results. If the call is successful, the returned iterator
// *must* be closed.
//
// The fields set in session that are set override the respective fields if they
// have previously been set through SetSessionData().
func (p *planner) QueryIteratorEx(
	ctx context.Context,
	opName string,
	override sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (eval.InternalRows, error) {
	return p.InternalSQLTxn().QueryIteratorEx(ctx, opName, p.Txn(), override, stmt, qargs...)
}

// QueryBufferedEx executes the supplied SQL statement and returns the resulting
// rows (meaning all of them are buffered at once).
// The fields set in session that are set override the respective fields if they
// have previously been set through SetSessionData().
func (p *planner) QueryBufferedEx(
	ctx context.Context,
	opName string,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, error) {
	return p.InternalSQLTxn().QueryBufferedEx(ctx, opName, p.Txn(), session, stmt, qargs...)
}

// QueryRowExWithCols is like QueryRowEx, additionally returning the computed
// ResultColumns of the input query.
func (p *planner) QueryRowExWithCols(
	ctx context.Context,
	opName string,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, colinfo.ResultColumns, error) {
	return p.InternalSQLTxn().QueryRowExWithCols(ctx, opName, p.Txn(), session, stmt, qargs...)
}

// QueryBufferedExWithCols is like QueryBufferedEx, additionally returning the
// computed ResultColumns of the input query.
func (p *planner) QueryBufferedExWithCols(
	ctx context.Context,
	opName string,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, colinfo.ResultColumns, error) {
	return p.InternalSQLTxn().QueryBufferedExWithCols(ctx, opName, p.Txn(), session, stmt, qargs...)
}

func (p *planner) resetPlanner(
	ctx context.Context,
	txn *kv.Txn,
	stmtTS time.Time,
	sd *sessiondata.SessionData,
	plannerMon *mon.BytesMonitor,
) {
	p.txn = txn
	p.stmt = Statement{}
	p.instrumentation = instrumentationHelper{}
	p.monitor = plannerMon

	p.cancelChecker.Reset(ctx)

	p.semaCtx = tree.MakeSemaContext()
	p.semaCtx.SearchPath = &sd.SearchPath
	p.semaCtx.Annotations = nil
	p.semaCtx.TypeResolver = p
	p.semaCtx.FunctionResolver = p
	p.semaCtx.NameResolver = p
	p.semaCtx.DateStyle = sd.GetDateStyle()
	p.semaCtx.IntervalStyle = sd.GetIntervalStyle()

	p.autoCommit = false

	p.schemaResolver.txn = txn
	p.schemaResolver.sessionDataStack = p.EvalContext().SessionDataStack
	p.evalCatalogBuiltins.Init(p.execCfg.Codec, txn, p.Descriptors())
	p.skipDescriptorCache = false
	p.typeResolutionDbID = descpb.InvalidID
	p.pausablePortal = nil
}

// GetReplicationStreamManager returns a ReplicationStreamManager.
func (p *planner) GetReplicationStreamManager(
	ctx context.Context,
) (eval.ReplicationStreamManager, error) {
	return repstream.GetReplicationStreamManager(ctx, p.EvalContext(), p.InternalSQLTxn())
}

// GetStreamIngestManager returns a StreamIngestManager.
func (p *planner) GetStreamIngestManager(ctx context.Context) (eval.StreamIngestManager, error) {
	return repstream.GetStreamIngestManager(ctx, p.EvalContext(), p.InternalSQLTxn())
}

// SpanStats returns a stats for the given span of keys.
func (p *planner) SpanStats(
	ctx context.Context, startKey roachpb.RKey, endKey roachpb.RKey,
) (*roachpb.SpanStatsResponse, error) {
	req := &roachpb.SpanStatsRequest{
		NodeID:   "0",
		StartKey: startKey,
		EndKey:   endKey,
	}
	return p.ExecCfg().TenantStatusServer.SpanStats(ctx, req)
}

// GetDetailsForSpanStats ensures that the given database and table id exist.
// No rows will be returned for database/table ids that do not correspond to an actual
// database/table.
func (p *planner) GetDetailsForSpanStats(
	ctx context.Context, dbId int, tableId int,
) (eval.InternalRows, error) {
	query := `SELECT parent_id, table_id FROM crdb_internal.tables`
	var args []interface{}

	if tableId != 0 {
		query += ` WHERE parent_id = $1 AND table_id = $2`
		args = append(args, dbId, tableId)
	} else if dbId != 0 {
		query += ` WHERE parent_id = $1`
		args = append(args, dbId)
	} else {
		// Some tables belonging to crdb_internal.tables are not affiliated with a database
		// and have a parent_id of 0 (usually crdb_internal or pg catalog tables), which aren't useful to the user.
		query += ` WHERE parent_id != $1`
		args = append(args, dbId)
	}

	return p.QueryIteratorEx(
		ctx,
		"crdb_internal.database_span_stats",
		sessiondata.NoSessionDataOverride,
		query,
		args...,
	)
}

// MaybeReallocateAnnotations is part of the eval.Planner interface.
func (p *planner) MaybeReallocateAnnotations(numAnnotations tree.AnnotationIdx) {
	if len(p.SemaCtx().Annotations) > int(numAnnotations) {
		return
	}
	p.SemaCtx().Annotations = tree.MakeAnnotations(numAnnotations)
	p.ExtendedEvalContext().Annotations = &p.SemaCtx().Annotations
}
