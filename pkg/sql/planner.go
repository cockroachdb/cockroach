// Copyright 2016 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/pkg/errors"
)

// extendedEvalContext extends tree.EvalContext with fields that are needed for
// distsql planning.
type extendedEvalContext struct {
	tree.EvalContext

	SessionMutator *sessionDataMutator

	// VirtualSchemas can be used to access virtual tables.
	VirtualSchemas VirtualTabler

	// Tracing provides access to the session's tracing interface. Changes to the
	// tracing state should be done through the sessionDataMutator.
	Tracing *SessionTracing

	// StatusServer gives access to the Status service. Used to cancel queries.
	StatusServer serverpb.StatusServer

	// MemMetrics represent the group of metrics to which execution should
	// contribute.
	MemMetrics *MemoryMetrics

	// Tables points to the Session's table collection (& cache).
	Tables *TableCollection

	ExecCfg *ExecutorConfig

	DistSQLPlanner *DistSQLPlanner

	TxnModesSetter txnModesSetter

	SchemaChangers *schemaChangerCollection

	schemaAccessors *schemaInterface
}

// schemaInterface provides access to the database and table descriptors.
// See schema_accessors.go.
type schemaInterface struct {
	physical SchemaAccessor
	logical  SchemaAccessor
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
	txn *client.Txn

	// Reference to the corresponding sql Statement for this query.
	stmt *Statement

	// Contexts for different stages of planning and execution.
	semaCtx         tree.SemaContext
	extendedEvalCtx extendedEvalContext

	// sessionDataMutator is used to mutate the session variables. Read
	// access to them is provided through evalCtx.
	sessionDataMutator *sessionDataMutator

	// execCfg is used to access the server configuration for the Executor.
	execCfg *ExecutorConfig

	preparedStatements preparedStatementsAccessor

	// statsCollector is used to collect statistics about SQL statement execution.
	statsCollector sqlStatsCollector

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
	// NOTE: This member is for internal use of the planner only. PlanNodes that
	// want to do 1PC transactions have to implement the autoCommitNode interface.
	autoCommit bool

	// discardRows is set if we want to discard any results rather than sending
	// them back to the client. Used for testing/benchmarking. Note that the
	// resulting schema or the plan are not affected.
	// See EXECUTE .. DISCARD ROWS.
	discardRows bool

	// cancelChecker is used by planNodes to check for cancellation of the associated
	// query.
	cancelChecker *sqlbase.CancelChecker

	// isPreparing is true if this planner is currently preparing.
	isPreparing bool

	// curPlan collects the properties of the current plan being prepared. This state
	// is undefined at the beginning of the planning of each new statement, and cannot
	// be reused for an old prepared statement after a new statement has been prepared.
	curPlan planTop

	// Avoid allocations by embedding commonly used objects and visitors.
	txCtx                 transform.ExprTransformContext
	subqueryVisitor       subqueryVisitor
	nameResolutionVisitor sqlbase.NameResolutionVisitor
	srfExtractionVisitor  srfExtractionVisitor
	tableName             tree.TableName

	// Use a common datum allocator across all the plan nodes. This separates the
	// plan lifetime from the lifetime of returned results allowing plan nodes to
	// be pool allocated.
	alloc sqlbase.DatumAlloc

	// optPlanningCtx stores the optimizer planning context, which contains
	// data structures that can be reused between queries (for efficiency).
	optPlanningCtx optPlanningCtx

	queryCacheSession querycache.Session
}

// noteworthyInternalMemoryUsageBytes is the minimum size tracked by each
// internal SQL pool before the pool starts explicitly logging overall usage
// growth in the log.
var noteworthyInternalMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_INTERNAL_MEMORY_USAGE", 1<<20 /* 1 MB */)

// NewInternalPlanner is an exported version of newInternalPlanner. It
// returns an interface{} so it can be used outside of the sql package.
func NewInternalPlanner(
	opName string, txn *client.Txn, user string, memMetrics *MemoryMetrics, execCfg *ExecutorConfig,
) (interface{}, func()) {
	return newInternalPlanner(opName, txn, user, memMetrics, execCfg)
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
	opName string, txn *client.Txn, user string, memMetrics *MemoryMetrics, execCfg *ExecutorConfig,
) (*planner, func()) {
	// We need a context that outlives all the uses of the planner (since the
	// planner captures it in the EvalCtx, and so does the cleanup function that
	// we're going to return. We just create one here instead of asking the caller
	// for a ctx with this property. This is really ugly, but the alternative of
	// asking the caller for one is hard to explain. What we need is better and
	// separate interfaces for planning and running plans, which could take
	// suitable contexts.
	ctx := logtags.AddTag(context.Background(), opName, "")

	sd := &sessiondata.SessionData{
		SearchPath:    sqlbase.DefaultSearchPath,
		User:          user,
		Database:      "system",
		SequenceState: sessiondata.NewSequenceState(),
		DataConversion: sessiondata.DataConversionConfig{
			Location: time.UTC,
		},
	}
	tables := &TableCollection{
		leaseMgr:      execCfg.LeaseManager,
		databaseCache: newDatabaseCache(config.NewSystemConfig()),
	}
	dataMutator := &sessionDataMutator{
		data: sd,
		defaults: SessionDefaults(map[string]string{
			"application_name": "crdb-internal",
			"database":         "system",
		}),
		settings:          execCfg.Settings,
		setCurTxnReadOnly: func(bool) {},
	}

	var ts time.Time
	if txn != nil {
		origTimestamp := txn.OrigTimestamp()
		if origTimestamp == (hlc.Timestamp{}) {
			panic("makeInternalPlanner called with a transaction without timestamps")
		}
		ts = origTimestamp.GoTime()
	}

	p := &planner{execCfg: execCfg}

	p.txn = txn
	p.stmt = nil
	p.cancelChecker = sqlbase.NewCancelChecker(ctx)

	p.semaCtx = tree.MakeSemaContext()
	p.semaCtx.Location = &sd.DataConversion.Location
	p.semaCtx.SearchPath = sd.SearchPath

	plannerMon := mon.MakeUnlimitedMonitor(ctx,
		fmt.Sprintf("internal-planner.%s.%s", user, opName),
		mon.MemoryResource,
		memMetrics.CurBytesCount, memMetrics.MaxBytesHist,
		noteworthyInternalMemoryUsageBytes, execCfg.Settings)

	p.extendedEvalCtx = internalExtendedEvalCtx(
		ctx, sd, dataMutator, tables, txn, ts, ts, execCfg, &plannerMon,
	)
	p.extendedEvalCtx.Planner = p
	p.extendedEvalCtx.SessionAccessor = p
	p.extendedEvalCtx.Sequence = p
	p.extendedEvalCtx.ClusterID = execCfg.ClusterID()
	p.extendedEvalCtx.NodeID = execCfg.NodeID.Get()
	p.extendedEvalCtx.Locality = execCfg.Locality

	p.sessionDataMutator = dataMutator
	p.autoCommit = false

	p.extendedEvalCtx.MemMetrics = memMetrics
	p.extendedEvalCtx.ExecCfg = execCfg
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	p.extendedEvalCtx.Tables = tables

	p.queryCacheSession.Init()

	return p, func() {
		// Note that we capture ctx here. This is only valid as long as we create
		// the context as explained at the top of the method.
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
	tables *TableCollection,
	txn *client.Txn,
	txnTimestamp time.Time,
	stmtTimestamp time.Time,
	execCfg *ExecutorConfig,
	plannerMon *mon.BytesMonitor,
) extendedEvalContext {
	var evalContextTestingKnobs tree.EvalContextTestingKnobs
	var statusServer serverpb.StatusServer
	evalContextTestingKnobs = execCfg.EvalContextTestingKnobs
	statusServer = execCfg.StatusServer

	return extendedEvalContext{
		EvalContext: tree.EvalContext{
			Txn:           txn,
			SessionData:   sd,
			TxnReadOnly:   false,
			TxnImplicit:   true,
			Settings:      execCfg.Settings,
			Context:       ctx,
			Mon:           plannerMon,
			TestingKnobs:  evalContextTestingKnobs,
			StmtTimestamp: stmtTimestamp,
			TxnTimestamp:  txnTimestamp,
		},
		SessionMutator:  dataMutator,
		VirtualSchemas:  execCfg.VirtualSchemas,
		Tracing:         &SessionTracing{},
		StatusServer:    statusServer,
		Tables:          tables,
		ExecCfg:         execCfg,
		schemaAccessors: newSchemaInterface(tables, execCfg.VirtualSchemas),
		DistSQLPlanner:  execCfg.DistSQLPlanner,
	}
}

func (p *planner) PhysicalSchemaAccessor() SchemaAccessor {
	return p.extendedEvalCtx.schemaAccessors.physical
}

func (p *planner) LogicalSchemaAccessor() SchemaAccessor {
	return p.extendedEvalCtx.schemaAccessors.logical
}

func (p *planner) ExtendedEvalContext() *extendedEvalContext {
	return &p.extendedEvalCtx
}

func (p *planner) CurrentDatabase() string {
	return p.SessionData().Database
}

func (p *planner) CurrentSearchPath() sessiondata.SearchPath {
	return p.SessionData().SearchPath
}

// EvalContext() provides convenient access to the planner's EvalContext().
func (p *planner) EvalContext() *tree.EvalContext {
	return &p.extendedEvalCtx.EvalContext
}

func (p *planner) Tables() *TableCollection {
	return p.extendedEvalCtx.Tables
}

// ExecCfg implements the PlanHookState interface.
func (p *planner) ExecCfg() *ExecutorConfig {
	return p.extendedEvalCtx.ExecCfg
}

func (p *planner) LeaseMgr() *LeaseManager {
	return p.Tables().leaseMgr
}

func (p *planner) Txn() *client.Txn {
	return p.txn
}

func (p *planner) User() string {
	return p.SessionData().User
}

// DistSQLPlanner returns the DistSQLPlanner
func (p *planner) DistSQLPlanner() *DistSQLPlanner {
	return p.extendedEvalCtx.DistSQLPlanner
}

// ParseType implements the tree.EvalPlanner interface.
// We define this here to break the dependency from eval.go to the parser.
func (p *planner) ParseType(sql string) (*types.T, error) {
	return parser.ParseType(sql)
}

// ParseQualifiedTableName implements the tree.EvalDatabase interface.
func (p *planner) ParseQualifiedTableName(
	ctx context.Context, sql string,
) (*tree.TableName, error) {
	return parser.ParseTableName(sql)
}

// ResolveTableName implements the tree.EvalDatabase interface.
func (p *planner) ResolveTableName(ctx context.Context, tn *tree.TableName) error {
	_, err := ResolveExistingObject(ctx, p, tn, true /*required*/, anyDescType)
	return err
}

// LookupTableByID looks up a table, by the given descriptor ID. Based on the
// CommonLookupFlags, it could use or skip the TableCollection cache. See
// TableCollection.getTableVersionByID for how it's used.
func (p *planner) LookupTableByID(ctx context.Context, tableID sqlbase.ID) (row.TableEntry, error) {
	flags := ObjectLookupFlags{CommonLookupFlags: CommonLookupFlags{avoidCached: p.avoidCachedDescriptors}}
	table, err := p.Tables().getTableVersionByID(ctx, p.txn, tableID, flags)
	if err != nil {
		if err == errTableAdding {
			return row.TableEntry{IsAdding: true}, nil
		}
		return row.TableEntry{}, err
	}
	return row.TableEntry{Desc: table}, nil
}

// TypeAsString enforces (not hints) that the given expression typechecks as a
// string and returns a function that can be called to get the string value
// during (planNode).Start.
func (p *planner) TypeAsString(e tree.Expr, op string) (func() (string, error), error) {
	typedE, err := tree.TypeCheckAndRequire(e, &p.semaCtx, types.String, op)
	if err != nil {
		return nil, err
	}
	fn := func() (string, error) {
		d, err := typedE.Eval(p.EvalContext())
		if err != nil {
			return "", err
		}
		str, ok := d.(*tree.DString)
		if !ok {
			return "", errors.Errorf("failed to cast %T to string", d)
		}
		return string(*str), nil
	}
	return fn, nil
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

// TypeAsStringOpts enforces (not hints) that the given expressions
// typecheck as strings, and returns a function that can be called to
// get the string value during (planNode).Start.
func (p *planner) TypeAsStringOpts(
	opts tree.KVOptions, optValidate map[string]KVStringOptValidate,
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
		r, err := tree.TypeCheckAndRequire(opt.Value, &p.semaCtx, types.String, k)
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
func (p *planner) TypeAsStringArray(exprs tree.Exprs, op string) (func() ([]string, error), error) {
	typedExprs := make([]tree.TypedExpr, len(exprs))
	for i := range exprs {
		typedE, err := tree.TypeCheckAndRequire(exprs[i], &p.semaCtx, types.String, op)
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

// prepareForDistSQLSupportCheck prepares p.curPlan.plan for a distSQL support
// check and does additional verification of the planner state.
func (p *planner) prepareForDistSQLSupportCheck() {
	// Trigger limit propagation.
	p.setUnlimited(p.curPlan.plan)
}

// runWithDistSQL runs a planNode tree synchronously via DistSQL, returning the
// results in a RowContainer. There's no streaming on this, so use sparingly.
// In general, you should always prefer to use the internal executor if you can.
func (p *planner) runWithDistSQL(
	ctx context.Context, plan planNode,
) (*rowcontainer.RowContainer, error) {
	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: &p.extendedEvalCtx,
		p:               p,
	}
	// Create the DistSQL plan for the input.
	planCtx := params.extendedEvalCtx.DistSQLPlanner.NewPlanningCtx(ctx, params.extendedEvalCtx, params.p.txn)
	log.VEvent(ctx, 1, "creating DistSQL plan")
	physPlan, err := planCtx.ExtendedEvalCtx.DistSQLPlanner.createPlanForNode(planCtx, plan)
	if err != nil {
		return nil, err
	}
	planCtx.ExtendedEvalCtx.DistSQLPlanner.FinalizePlan(planCtx, &physPlan)
	columns := planColumns(plan)

	// Initialize a row container for the DistSQL execution engine to write into.
	// The caller of this method will call Close on the returned RowContainer,
	// which will close this account.
	acc := planCtx.EvalContext().Mon.MakeBoundAccount()
	ci := sqlbase.ColTypeInfoFromResCols(columns)
	rows := rowcontainer.NewRowContainer(acc, ci, 0 /* rowCapacity */)
	rowResultWriter := NewRowResultWriter(rows)
	recv := MakeDistSQLReceiver(
		ctx,
		rowResultWriter,
		tree.Rows,
		p.ExecCfg().RangeDescriptorCache,
		p.ExecCfg().LeaseHolderCache,
		p.txn,
		func(ts hlc.Timestamp) {
			_ = p.ExecCfg().Clock.Update(ts)
		},
		p.extendedEvalCtx.Tracing,
	)
	defer recv.Release()

	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := p.extendedEvalCtx
	// Run the plan, writing to the row container we initialized earlier.
	p.extendedEvalCtx.DistSQLPlanner.Run(
		planCtx, p.txn, &physPlan, recv, &evalCtxCopy, nil /* finishedSetupFn */)
	if rowResultWriter.Err() != nil {
		rows.Close(ctx)
		return nil, rowResultWriter.Err()
	}
	return rows, nil
}

// txnModesSetter is an interface used by SQL execution to influence the current
// transaction.
type txnModesSetter interface {
	// setTransactionModes updates some characteristics of the current
	// transaction.
	// asOfTs, if not empty, is the evaluation of modes.AsOf.
	setTransactionModes(modes tree.TransactionModes, asOfTs hlc.Timestamp) error
}

// sqlStatsCollector is the interface used by SQL execution, through the
// planner, for recording statistics about SQL statements.
type sqlStatsCollector interface {
	// PhaseTimes returns that phaseTimes struct that measures the time spent in
	// each phase of SQL execution.
	// See executor_statement_metrics.go for details.
	PhaseTimes() *phaseTimes

	// RecordStatement record stats for one statement.
	//
	// samplePlanDescription can be nil, as these are only sampled periodically per unique fingerprint.
	RecordStatement(
		stmt *Statement,
		samplePlanDescription *roachpb.ExplainTreePlanNode,
		distSQLUsed bool,
		optUsed bool,
		automaticRetryCount int,
		numRows int,
		err error,
		parseLat, planLat, runLat, svcLat, ovhLat float64,
	)

	// SQLStats provides access to the global sqlStats object.
	SQLStats() *sqlStats

	// Reset resets this stats collector with the given phaseTimes array.
	Reset(sqlStats *sqlStats, appStats *appStats, times *phaseTimes)
}
