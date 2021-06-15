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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// extendedEvalContext extends tree.EvalContext with fields that are needed for
// distsql planning.
type extendedEvalContext struct {
	tree.EvalContext

	SessionMutator *sessionDataMutator

	// SessionID for this connection.
	SessionID ClusterWideID

	// VirtualSchemas can be used to access virtual tables.
	VirtualSchemas VirtualTabler

	// Tracing provides access to the session's tracing interface. Changes to the
	// tracing state should be done through the sessionDataMutator.
	Tracing *SessionTracing

	// NodesStatusServer gives access to the NodesStatus service. Unavailable to
	// tenants.
	NodesStatusServer serverpb.OptionalNodesStatusServer

	// SQLStatusServer gives access to a subset of the serverpb.Status service
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

	// Jobs refers to jobs in extraTxnState. Jobs is a pointer to a jobsCollection
	// which is a slice because we need calls to resetExtraTxnState to reset the
	// jobsCollection.
	Jobs *jobsCollection

	// SchemaChangeJobCache refers to schemaChangeJobsCache in extraTxnState.
	SchemaChangeJobCache map[descpb.ID]*jobs.Job

	statsStorage sqlstats.Storage

	SchemaChangerState *SchemaChangerState
}

// copy returns a deep copy of ctx.
func (evalCtx *extendedEvalContext) copy() *extendedEvalContext {
	cpy := *evalCtx
	cpy.EvalContext = *evalCtx.EvalContext.Copy()
	return &cpy
}

// QueueJob creates a new job from record and queues it for execution after
// the transaction commits.
func (evalCtx *extendedEvalContext) QueueJob(
	ctx context.Context, record jobs.Record,
) (*jobs.Job, error) {
	jobID := evalCtx.ExecCfg.JobRegistry.MakeJobID()
	job, err := evalCtx.ExecCfg.JobRegistry.CreateJobWithTxn(
		ctx,
		record,
		jobID,
		evalCtx.Txn,
	)
	if err != nil {
		return nil, err
	}
	*evalCtx.Jobs = append(*evalCtx.Jobs, jobID)
	return job, nil
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
	txn *kv.Txn

	// isInternalPlanner is set to true when this planner is not bound to
	// a SQL session.
	isInternalPlanner bool

	// Corresponding Statement for this query.
	stmt Statement

	instrumentation instrumentationHelper

	// Contexts for different stages of planning and execution.
	semaCtx         tree.SemaContext
	extendedEvalCtx extendedEvalContext

	// sessionDataMutator is used to mutate the session variables. Read
	// access to them is provided through evalCtx.
	sessionDataMutator *sessionDataMutator

	// execCfg is used to access the server configuration for the Executor.
	execCfg *ExecutorConfig

	preparedStatements preparedStatementsAccessor

	// avoidCachedDescriptors, when true, instructs all code that
	// accesses table/view descriptors to force reading the descriptors
	// within the transaction. This is necessary to read descriptors
	// from the store for:
	// 1. Descriptors that are part of a schema change but are not
	// modified by the schema change. (reading a table in CREATE VIEW)
	// 2. Disable the use of the table cache in tests.
	avoidCachedDescriptors bool

	// If set, the planner should skip checking for the SELECT privilege when
	// initializing plans to read from a table. This should be used with care.
	skipSelectPrivilegeChecks bool

	// autoCommit indicates whether we're planning for an implicit transaction.
	// If autoCommit is true, the plan is allowed (but not required) to commit the
	// transaction along with other KV operations. Committing the txn might be
	// beneficial because it may enable the 1PC optimization.
	//
	// NOTE: plan node must be configured appropriately to actually perform an
	// auto-commit. This is dependent on information from the optimizer.
	autoCommit bool

	// cancelChecker is used by planNodes to check for cancellation of the associated
	// query.
	cancelChecker *cancelchecker.CancelChecker

	// isPreparing is true if this planner is currently preparing.
	isPreparing bool

	// curPlan collects the properties of the current plan being prepared. This state
	// is undefined at the beginning of the planning of each new statement, and cannot
	// be reused for an old prepared statement after a new statement has been prepared.
	curPlan planTop

	// Avoid allocations by embedding commonly used objects and visitors.
	txCtx                 transform.ExprTransformContext
	nameResolutionVisitor schemaexpr.NameResolutionVisitor
	tableName             tree.TableName

	// Use a common datum allocator across all the plan nodes. This separates the
	// plan lifetime from the lifetime of returned results allowing plan nodes to
	// be pool allocated.
	alloc *rowenc.DatumAlloc

	// optPlanningCtx stores the optimizer planning context, which contains
	// data structures that can be reused between queries (for efficiency).
	optPlanningCtx optPlanningCtx

	// noticeSender allows the sending of notices.
	// Do not use this object directly; use the BufferClientNotice() method
	// instead.
	noticeSender noticeSender

	queryCacheSession querycache.Session

	// contextDatabaseID is the ID of a database. It is set during some name
	// resolution processes to disallow cross database references. In particular,
	// the type resolution steps will disallow resolution of types that have a
	// parentID != contextDatabaseID when it is set.
	contextDatabaseID descpb.ID
}

func (evalCtx *extendedEvalContext) setSessionID(sessionID ClusterWideID) {
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
	user security.SQLUsername,
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
	opName string,
	txn *kv.Txn,
	user security.SQLUsername,
	memMetrics *MemoryMetrics,
	execCfg *ExecutorConfig,
	sessionData sessiondatapb.SessionData,
	opts ...InternalPlannerParamsOption,
) (*planner, func()) {
	// Default parameters which may be override by the supplied options.
	params := &internalPlannerParams{
		// The table collection used by the internal planner does not rely on the
		// deprecatedDatabaseCache and there are no subscribers to the
		// deprecatedDatabaseCache, so we can leave it uninitialized.
		// Furthermore, we're not concerned about the efficiency of querying tables
		// with user-defined types, hence the nil hydratedTables.
		collection: descs.NewCollection(
			execCfg.Settings,
			execCfg.LeaseManager,
			nil, // hydratedTables
			execCfg.VirtualSchemas,
		),
	}
	for _, opt := range opts {
		opt(params)
	}

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
	sd.SessionData.Database = "system"
	sd.SessionData.UserProto = user.EncodeProto()
	dataMutator := &sessionDataMutator{
		data: sd,
		defaults: SessionDefaults(map[string]string{
			"application_name": "crdb-internal",
			"database":         "system",
		}),
		settings:           execCfg.Settings,
		paramStatusUpdater: &noopParamStatusUpdater{},
		setCurTxnReadOnly:  func(bool) {},
	}

	var ts time.Time
	if txn != nil {
		readTimestamp := txn.ReadTimestamp()
		if readTimestamp.IsEmpty() {
			panic("makeInternalPlanner called with a transaction without timestamps")
		}
		ts = readTimestamp.GoTime()
	}

	p := &planner{execCfg: execCfg, alloc: &rowenc.DatumAlloc{}}

	p.txn = txn
	p.stmt = Statement{}
	p.cancelChecker = cancelchecker.NewCancelChecker(ctx)
	p.isInternalPlanner = true

	p.semaCtx = tree.MakeSemaContext()
	p.semaCtx.SearchPath = sd.SearchPath
	p.semaCtx.TypeResolver = p

	plannerMon := mon.NewMonitor(fmt.Sprintf("internal-planner.%s.%s", user, opName),
		mon.MemoryResource,
		memMetrics.CurBytesCount, memMetrics.MaxBytesHist,
		-1, /* increment */
		noteworthyInternalMemoryUsageBytes, execCfg.Settings)
	plannerMon.Start(ctx, execCfg.RootMemoryMonitor, mon.BoundAccount{})

	p.extendedEvalCtx = internalExtendedEvalCtx(
		ctx, sd, dataMutator, params.collection, txn, ts, ts, execCfg, plannerMon,
	)
	p.extendedEvalCtx.Planner = p
	p.extendedEvalCtx.PrivilegedAccessor = p
	p.extendedEvalCtx.SessionAccessor = p
	p.extendedEvalCtx.ClientNoticeSender = p
	p.extendedEvalCtx.Sequence = p
	p.extendedEvalCtx.Tenant = p
	p.extendedEvalCtx.JoinTokenCreator = p
	p.extendedEvalCtx.ClusterID = execCfg.ClusterID()
	p.extendedEvalCtx.ClusterName = execCfg.RPCContext.ClusterName()
	p.extendedEvalCtx.NodeID = execCfg.NodeID
	p.extendedEvalCtx.Locality = execCfg.Locality

	p.sessionDataMutator = dataMutator
	p.autoCommit = false

	p.extendedEvalCtx.MemMetrics = memMetrics
	p.extendedEvalCtx.ExecCfg = execCfg
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
	p.extendedEvalCtx.Descs = params.collection

	p.queryCacheSession.Init()
	p.optPlanningCtx.init(p)

	return p, func() {
		// Note that we capture ctx here. This is only valid as long as we create
		// the context as explained at the top of the method.

		// The collection will accumulate descriptors read during planning as well
		// as type descriptors read during execution on the local node. Many users
		// of the internal planner do set the `skipCache` flag on the resolver but
		// this is not respected by type resolution underneath execution. That
		// subtle details means that the type descriptor used by execution may be
		// stale, but that must be okay. Correctness concerns aside, we must release
		// the leases to ensure that we don't leak a descriptor lease.
		p.Descriptors().ReleaseAll(ctx)

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
	sd *sessiondata.SessionData,
	dataMutator *sessionDataMutator,
	tables *descs.Collection,
	txn *kv.Txn,
	txnTimestamp time.Time,
	stmtTimestamp time.Time,
	execCfg *ExecutorConfig,
	plannerMon *mon.BytesMonitor,
) extendedEvalContext {
	evalContextTestingKnobs := execCfg.EvalContextTestingKnobs

	var sqlStatsResetter tree.SQLStatsResetter
	if execCfg.InternalExecutor != nil {
		sqlStatsResetter = execCfg.InternalExecutor.s
	}

	return extendedEvalContext{
		EvalContext: tree.EvalContext{
			Txn:              txn,
			SessionData:      sd,
			TxnReadOnly:      false,
			TxnImplicit:      true,
			Settings:         execCfg.Settings,
			Codec:            execCfg.Codec,
			Context:          ctx,
			Mon:              plannerMon,
			TestingKnobs:     evalContextTestingKnobs,
			StmtTimestamp:    stmtTimestamp,
			TxnTimestamp:     txnTimestamp,
			InternalExecutor: execCfg.InternalExecutor,
			SQLStatsResetter: sqlStatsResetter,
		},
		SessionMutator:    dataMutator,
		VirtualSchemas:    execCfg.VirtualSchemas,
		Tracing:           &SessionTracing{},
		NodesStatusServer: execCfg.NodesStatusServer,
		Descs:             tables,
		ExecCfg:           execCfg,
		DistSQLPlanner:    execCfg.DistSQLPlanner,
	}
}

// LogicalSchemaAccessor is part of the resolver.SchemaResolver interface.
func (p *planner) Accessor() catalog.Accessor {
	return p.Descriptors()
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

// CurrentDatabase is part of the resolver.SchemaResolver interface.
func (p *planner) CurrentDatabase() string {
	return p.SessionData().Database
}

// CurrentSearchPath is part of the resolver.SchemaResolver interface.
func (p *planner) CurrentSearchPath() sessiondata.SearchPath {
	return p.SessionData().SearchPath
}

// EvalContext() provides convenient access to the planner's EvalContext().
func (p *planner) EvalContext() *tree.EvalContext {
	return &p.extendedEvalCtx.EvalContext
}

func (p *planner) Descriptors() *descs.Collection {
	return p.extendedEvalCtx.Descs
}

// ExecCfg implements the PlanHookState interface.
func (p *planner) ExecCfg() *ExecutorConfig {
	return p.extendedEvalCtx.ExecCfg
}

// GetOrInitSequenceCache returns the sequence cache for the session.
// If the sequence cache has not been used yet, it initializes the cache
// inside the session data.
func (p *planner) GetOrInitSequenceCache() sessiondata.SequenceCache {
	if p.SessionData().SequenceCache == nil {
		p.sessionDataMutator.initSequenceCache()
	}
	return p.SessionData().SequenceCache
}

func (p *planner) LeaseMgr() *lease.Manager {
	return p.execCfg.LeaseManager
}

func (p *planner) Txn() *kv.Txn {
	return p.txn
}

func (p *planner) User() security.SQLUsername {
	return p.SessionData().User()
}

func (p *planner) TemporarySchemaName() string {
	return temporarySchemaName(p.ExtendedEvalContext().SessionID)
}

// DistSQLPlanner returns the DistSQLPlanner
func (p *planner) DistSQLPlanner() *DistSQLPlanner {
	return p.extendedEvalCtx.DistSQLPlanner
}

// MigrationJobDeps returns the migration.JobDeps.
func (p *planner) MigrationJobDeps() migration.JobDeps {
	return p.execCfg.MigrationJobDeps
}

// GetTypeFromValidSQLSyntax implements the tree.EvalPlanner interface.
// We define this here to break the dependency from eval.go to the parser.
func (p *planner) GetTypeFromValidSQLSyntax(sql string) (*types.T, error) {
	ref, err := parser.GetTypeFromValidSQLSyntax(sql)
	if err != nil {
		return nil, err
	}
	return tree.ResolveType(context.TODO(), ref, p.semaCtx.GetTypeResolver())
}

// ParseQualifiedTableName implements the tree.EvalDatabase interface.
// This exists to get around a circular dependency between sql/sem/tree and
// sql/parser. sql/parser depends on tree to make objects, so tree cannot import
// ParseQualifiedTableName even though some builtins need that function.
// TODO(jordan): remove this once builtins can be moved outside of sql/sem/tree.
func (p *planner) ParseQualifiedTableName(sql string) (*tree.TableName, error) {
	return parser.ParseQualifiedTableName(sql)
}

// ResolveTableName implements the tree.EvalDatabase interface.
func (p *planner) ResolveTableName(ctx context.Context, tn *tree.TableName) (tree.ID, error) {
	flags := tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveAnyTableKind)
	_, desc, err := resolver.ResolveExistingTableObject(ctx, p, tn, flags)
	if err != nil {
		return 0, err
	}
	return tree.ID(desc.GetID()), nil
}

// LookupTableByID looks up a table, by the given descriptor ID. Based on the
// CommonLookupFlags, it could use or skip the Collection cache.
func (p *planner) LookupTableByID(
	ctx context.Context, tableID descpb.ID,
) (catalog.TableDescriptor, error) {
	const required = true // lookups by ID are always "required"
	table, err := p.Descriptors().GetImmutableTableByID(ctx, p.txn, tableID, p.ObjectLookupFlags(
		required, false /* requireMutable */))
	if err != nil {
		return nil, err
	}
	return table, nil
}

// TypeAsString enforces (not hints) that the given expression typechecks as a
// string and returns a function that can be called to get the string value
// during (planNode).Start.
// To also allow NULLs to be returned, use TypeAsStringOrNull() instead.
func (p *planner) TypeAsString(
	ctx context.Context, e tree.Expr, op string,
) (func() (string, error), error) {
	typedE, err := tree.TypeCheckAndRequire(ctx, e, &p.semaCtx, types.String, op)
	if err != nil {
		return nil, err
	}
	evalFn := p.makeStringEvalFn(typedE)
	return func() (string, error) {
		isNull, str, err := evalFn()
		if err != nil {
			return "", err
		}
		if isNull {
			return "", errors.Errorf("expected string, got NULL")
		}
		return str, nil
	}, nil
}

// TypeAsStringOrNull is like TypeAsString but allows NULLs.
func (p *planner) TypeAsStringOrNull(
	ctx context.Context, e tree.Expr, op string,
) (func() (bool, string, error), error) {
	typedE, err := tree.TypeCheckAndRequire(ctx, e, &p.semaCtx, types.String, op)
	if err != nil {
		return nil, err
	}
	return p.makeStringEvalFn(typedE), nil
}

func (p *planner) makeStringEvalFn(typedE tree.TypedExpr) func() (bool, string, error) {
	return func() (bool, string, error) {
		d, err := typedE.Eval(p.EvalContext())
		if err != nil {
			return false, "", err
		}
		if d == tree.DNull {
			return true, "", nil
		}
		str, ok := d.(*tree.DString)
		if !ok {
			return false, "", errors.Errorf("failed to cast %T to string", d)
		}
		return false, string(*str), nil
	}
}

// KVStringOptValidate indicates the requested validation of a TypeAsStringOpts
// option.
type KVStringOptValidate string

// KVStringOptValidate values
const (
	KVStringOptAny            KVStringOptValidate = `any`
	KVStringOptRequireNoValue KVStringOptValidate = `no-value`
	KVStringOptRequireValue   KVStringOptValidate = `value`
)

// evalStringOptions evaluates the KVOption values as strings and returns them
// in a map. Options with no value have an empty string.
func evalStringOptions(
	evalCtx *tree.EvalContext, opts []exec.KVOption, optValidate map[string]KVStringOptValidate,
) (map[string]string, error) {
	res := make(map[string]string, len(opts))
	for _, opt := range opts {
		k := opt.Key
		validate, ok := optValidate[k]
		if !ok {
			return nil, errors.Errorf("invalid option %q", k)
		}
		val, err := opt.Value.Eval(evalCtx)
		if err != nil {
			return nil, err
		}
		if val == tree.DNull {
			if validate == KVStringOptRequireValue {
				return nil, errors.Errorf("option %q requires a value", k)
			}
			res[k] = ""
		} else {
			if validate == KVStringOptRequireNoValue {
				return nil, errors.Errorf("option %q does not take a value", k)
			}
			str, ok := val.(*tree.DString)
			if !ok {
				return nil, errors.Errorf("expected string value, got %T", val)
			}
			res[k] = string(*str)
		}
	}
	return res, nil
}

// TypeAsStringOpts enforces (not hints) that the given expressions
// typecheck as strings, and returns a function that can be called to
// get the string value during (planNode).Start.
func (p *planner) TypeAsStringOpts(
	ctx context.Context, opts tree.KVOptions, optValidate map[string]KVStringOptValidate,
) (func() (map[string]string, error), error) {
	typed := make(map[string]tree.TypedExpr, len(opts))
	for _, opt := range opts {
		k := string(opt.Key)
		validate, ok := optValidate[k]
		if !ok {
			return nil, errors.Errorf("invalid option %q", k)
		}

		if opt.Value == nil {
			if validate == KVStringOptRequireValue {
				return nil, errors.Errorf("option %q requires a value", k)
			}
			typed[k] = nil
			continue
		}
		if validate == KVStringOptRequireNoValue {
			return nil, errors.Errorf("option %q does not take a value", k)
		}
		r, err := tree.TypeCheckAndRequire(ctx, opt.Value, &p.semaCtx, types.String, k)
		if err != nil {
			return nil, err
		}
		typed[k] = r
	}
	fn := func() (map[string]string, error) {
		res := make(map[string]string, len(typed))
		for name, e := range typed {
			if e == nil {
				res[name] = ""
				continue
			}
			d, err := e.Eval(p.EvalContext())
			if err != nil {
				return nil, err
			}
			str, ok := d.(*tree.DString)
			if !ok {
				return res, errors.Errorf("failed to cast %T to string", d)
			}
			res[name] = string(*str)
		}
		return res, nil
	}
	return fn, nil
}

// TypeAsStringArray enforces (not hints) that the given expressions all typecheck as
// strings and returns a function that can be called to get the string values
// during (planNode).Start.
func (p *planner) TypeAsStringArray(
	ctx context.Context, exprs tree.Exprs, op string,
) (func() ([]string, error), error) {
	typedExprs := make([]tree.TypedExpr, len(exprs))
	for i := range exprs {
		typedE, err := tree.TypeCheckAndRequire(ctx, exprs[i], &p.semaCtx, types.String, op)
		if err != nil {
			return nil, err
		}
		typedExprs[i] = typedE
	}
	fn := func() ([]string, error) {
		strs := make([]string, len(exprs))
		for i := range exprs {
			d, err := typedExprs[i].Eval(p.EvalContext())
			if err != nil {
				return nil, err
			}
			str, ok := d.(*tree.DString)
			if !ok {
				return strs, errors.Errorf("failed to cast %T to string", d)
			}
			strs[i] = string(*str)
		}
		return strs, nil
	}
	return fn, nil
}

// SessionData is part of the PlanHookState interface.
func (p *planner) SessionData() *sessiondata.SessionData {
	return p.EvalContext().SessionData
}

// Ann is a shortcut for the Annotations from the eval context.
func (p *planner) Ann() *tree.Annotations {
	return p.ExtendedEvalContext().EvalContext.Annotations
}

// txnModesSetter is an interface used by SQL execution to influence the current
// transaction.
type txnModesSetter interface {
	// setTransactionModes updates some characteristics of the current
	// transaction.
	// asOfTs, if not empty, is the evaluation of modes.AsOf.
	setTransactionModes(modes tree.TransactionModes, asOfTs hlc.Timestamp) error
}

// validateDescriptor is a convenience function for validating
// descriptors in the context of a planner.
func validateDescriptor(ctx context.Context, p *planner, descriptor catalog.Descriptor) error {
	bdg := catalogkv.NewOneLevelUncachedDescGetter(p.Txn(), p.ExecCfg().Codec)
	return catalog.ValidateSelfAndCrossReferences(ctx, bdg, descriptor)
}
