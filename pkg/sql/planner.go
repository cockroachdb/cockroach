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
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// extendedEvalCtx extends tree.EvalContext with fields that are just needed in
// the sql package.
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

	// asOfSystemTime indicates whether the transaction timestamp was
	// forced to a specific value (in which case that value is stored in
	// txn.mu.Proto.OrigTimestamp). If set, avoidCachedDescriptors below
	// must also be set.
	// TODO(anyone): we may want to support table readers at arbitrary
	// timestamps, so that each FROM clause can have its own
	// timestamp. In that case, the timestamp would not be set
	// globally for the entire txn and this field would not be needed.
	asOfSystemTime bool

	// avoidCachedDescriptors, when true, instructs all code that
	// accesses table/view descriptors to force reading the descriptors
	// within the transaction. This is necessary to:
	// - ensure that queries ran with AS OF SYSTEM TIME get the right
	//   version of descriptors.
	// - queries that create/update descriptors read all their dependencies
	//   in the same txn that they write new descriptors or update their
	//   dependencies, so that update/creation appears transactional
	//   to the rest of the cluster.
	// Code that sets this to true should probably also check that
	// the txn isolation level is SERIALIZABLE, and reject any update
	// if it is SNAPSHOT.
	avoidCachedDescriptors bool

	// revealNewDescriptors, when true, instructs the name resolution
	// code to also use descriptors in state ADD.
	// Used by e.g. multiple DDL inside transactions.
	revealNewDescriptors bool

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
	parser                parser.Parser
	txCtx                 transform.ExprTransformContext
	subqueryVisitor       subqueryVisitor
	nameResolutionVisitor nameResolutionVisitor
	srfExtractionVisitor  srfExtractionVisitor

	// Use a common datum allocator across all the plan nodes. This separates the
	// plan lifetime from the lifetime of returned results allowing plan nodes to
	// be pool allocated.
	alloc sqlbase.DatumAlloc
}

// noteworthyInternalMemoryUsageBytes is the minimum size tracked by each
// internal SQL pool before the pool starts explicitly logging overall usage
// growth in the log.
var noteworthyInternalMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_INTERNAL_MEMORY_USAGE", 100*1024)

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
	// init with an empty session. We can't leave this nil because too much code
	// looks in the session for the current database.
	ctx := log.WithLogTagStr(context.Background(), opName, "")

	s := &Session{
		data: sessiondata.SessionData{
			SearchPath:    sqlbase.DefaultSearchPath,
			Location:      time.UTC,
			User:          user,
			Database:      "system",
			SequenceState: sessiondata.NewSequenceState(),
		},
		TxnState: txnState{Ctx: ctx, implicitTxn: true},
		context:  ctx,
		tables: TableCollection{
			leaseMgr:      execCfg.LeaseManager,
			databaseCache: newDatabaseCache(config.SystemConfig{}),
		},
		execCfg:        execCfg,
		distSQLPlanner: execCfg.DistSQLPlanner,
	}
	s.dataMutator = sessionDataMutator{
		data: &s.data,
		defaults: sessionDefaults{
			applicationName: "crdb-internal",
			database:        "system",
		},
		settings:       execCfg.Settings,
		curTxnReadOnly: &s.TxnState.readOnly,
	}
	s.mon = mon.MakeUnlimitedMonitor(ctx,
		"internal-root",
		mon.MemoryResource,
		memMetrics.CurBytesCount, memMetrics.MaxBytesHist,
		noteworthyInternalMemoryUsageBytes, execCfg.Settings)

	s.sessionMon = mon.MakeMonitor("internal-session",
		mon.MemoryResource,
		memMetrics.SessionCurBytesCount,
		memMetrics.SessionMaxBytesHist,
		-1, noteworthyInternalMemoryUsageBytes/5, execCfg.Settings)
	s.sessionMon.Start(ctx, &s.mon, mon.BoundAccount{})

	s.TxnState.mon = mon.MakeMonitor("internal-txn",
		mon.MemoryResource,
		memMetrics.TxnCurBytesCount,
		memMetrics.TxnMaxBytesHist,
		-1, noteworthyInternalMemoryUsageBytes/5, execCfg.Settings)
	s.TxnState.mon.Start(ctx, &s.mon, mon.BoundAccount{})

	var ts time.Time
	if txn != nil {
		origTimestamp := txn.OrigTimestamp()
		if origTimestamp == (hlc.Timestamp{}) {
			panic("makeInternalPlanner called with a transaction without timestamps")
		}
		ts = origTimestamp.GoTime()
	}
	p := s.newPlanner(
		txn, ts /* txnTimestamp */, ts, /* stmtTimestamp */
		nil /* reCache */, s.statsCollector())

	p.extendedEvalCtx.MemMetrics = memMetrics
	p.extendedEvalCtx.ExecCfg = execCfg
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	p.extendedEvalCtx.Tables = &s.tables
	acc := s.mon.MakeBoundAccount()
	p.extendedEvalCtx.ActiveMemAcc = &acc

	return p, func() {
		acc.Close(ctx)
		s.TxnState.mon.Stop(ctx)
		s.sessionMon.Stop(ctx)
		s.mon.Stop(ctx)
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

// TODO(dan): This is here to implement PlanHookState, but it's not clear that
// this is the right abstraction. We could also export DistSQLPlanner, for
// example. Revisit.
func (p *planner) DistLoader() *DistLoader {
	return &DistLoader{distSQLPlanner: p.extendedEvalCtx.DistSQLPlanner}
}

// makeInternalPlan initializes a plan from a SQL statement string.
// This clobbers p.curPlan.
// p.curPlan.Close() must be called after use.
// This function changes the planner's placeholder map. It is the caller's
// responsibility to save and restore the old map if desired.
// This function is not suitable for use in the planNode constructors directly:
// the returned planNode has already been optimized.
// Consider also (*planner).delegateQuery(...).
func (p *planner) makeInternalPlan(ctx context.Context, sql string, args ...interface{}) error {
	if log.V(2) {
		log.Infof(ctx, "internal query: %s", sql)
		if len(args) > 0 {
			log.Infof(ctx, "placeholders: %q", args)
		}
	}
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		return err
	}
	golangFillQueryArguments(&p.semaCtx.Placeholders, args)
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	return p.makePlan(ctx, Statement{AST: stmt})
}

// ParseType implements the parser.EvalPlanner interface.
// We define this here to break the dependency from eval.go to the parser.
func (p *planner) ParseType(sql string) (coltypes.CastTargetType, error) {
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

// QueryRow implements the parser.EvalPlanner interface.
func (p *planner) QueryRow(
	ctx context.Context, sql string, args ...interface{},
) (tree.Datums, error) {
	origP := p
	p, cleanup := newInternalPlanner("query rows", p.Txn(), p.User(), p.ExtendedEvalContext().MemMetrics, p.ExecCfg())
	defer cleanup()
	*p.SessionData() = *origP.SessionData()
	return p.queryRow(ctx, sql, args...)
}

func (p *planner) queryRow(
	ctx context.Context, sql string, args ...interface{},
) (tree.Datums, error) {
	rows, _ /* cols */, err := p.queryRows(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	switch len(rows) {
	case 0:
		return nil, nil
	case 1:
		return rows[0], nil
	default:
		return nil, &tree.MultipleResultsError{SQL: sql}
	}
}

// queryRows executes a SQL query string where multiple result rows are returned.
func (p *planner) queryRows(
	ctx context.Context, sql string, args ...interface{},
) (rows []tree.Datums, cols sqlbase.ResultColumns, err error) {
	// makeInternalPlan() clobbers p.curplan and the placeholder info
	// map, so we have to save/restore them here.
	defer func(psave planTop, pisave tree.PlaceholderInfo) {
		p.semaCtx.Placeholders = pisave
		p.curPlan = psave
	}(p.curPlan, p.semaCtx.Placeholders)

	startTime := timeutil.Now()
	if err := p.makeInternalPlan(ctx, sql, args...); err != nil {
		p.maybeLogStatementInternal(ctx, "internal-prepare", 0, err, startTime)
		return nil, nil, err
	}
	cols = planColumns(p.curPlan.plan)
	defer p.curPlan.close(ctx)
	defer func() { p.maybeLogStatementInternal(ctx, "internal-exec", len(rows), err, startTime) }()

	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: &p.extendedEvalCtx,
		p:               p,
	}
	if err := p.curPlan.start(params); err != nil {
		return nil, nil, err
	}
	if err := forEachRow(params, p.curPlan.plan, func(values tree.Datums) error {
		if values != nil {
			valCopy := append(tree.Datums(nil), values...)
			rows = append(rows, valCopy)
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return rows, cols, nil
}

// exec executes a SQL query string and returns the number of rows
// affected.
func (p *planner) exec(
	ctx context.Context, sql string, args ...interface{},
) (numRows int, err error) {
	// makeInternalPlan() clobbers p.curplan and the placeholder info
	// map, so we have to save/restore them here.
	defer func(psave planTop, pisave tree.PlaceholderInfo) {
		p.semaCtx.Placeholders = pisave
		p.curPlan = psave
	}(p.curPlan, p.semaCtx.Placeholders)

	startTime := timeutil.Now()
	if err := p.makeInternalPlan(ctx, sql, args...); err != nil {
		p.maybeLogStatementInternal(ctx, "internal-prepare", 0, err, startTime)
		return 0, err
	}
	defer p.curPlan.close(ctx)
	defer func() { p.maybeLogStatementInternal(ctx, "internal-exec", numRows, err, startTime) }()

	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: &p.extendedEvalCtx,
		p:               p,
	}
	if err := p.curPlan.start(params); err != nil {
		return 0, err
	}
	return countRowsAffected(params, p.curPlan.plan)
}

func (p *planner) lookupFKTable(
	ctx context.Context, tableID sqlbase.ID,
) (sqlbase.TableLookup, error) {
	table, err := p.Tables().getTableVersionByID(ctx, p.txn, tableID)
	if err != nil {
		if err == errTableAdding {
			return sqlbase.TableLookup{IsAdding: true}, nil
		}
		return sqlbase.TableLookup{}, err
	}
	return sqlbase.TableLookup{Table: table}, nil
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

// TypeAsStringOpts enforces (not hints) that the given expressions
// typecheck as strings, and returns a function that can be called to
// get the string value during (planNode).Start.
func (p *planner) TypeAsStringOpts(
	opts tree.KVOptions, expectValues map[string]bool,
) (func() (map[string]string, error), error) {
	typed := make(map[string]tree.TypedExpr, len(opts))
	for _, opt := range opts {
		k := string(opt.Key)
		takesValue, ok := expectValues[k]
		if !ok {
			return nil, errors.Errorf("invalid option %q", k)
		}

		if opt.Value == nil {
			if takesValue {
				return nil, errors.Errorf("option %q requires a value", k)
			}
			typed[k] = nil
			continue
		}
		if !takesValue {
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

// txnModesSetter is an interface used by SQL execution to influence the current
// transaction.
type txnModesSetter interface {
	setTransactionModes(modes tree.TransactionModes) error
}

// sqlStatsCollector is the interface used by SQL execution, through the
// planner, for recording statistics about SQL statements.
type sqlStatsCollector interface {
	// PhaseTimes returns that phaseTimes struct that measures the time spent in
	// each phase of SQL execution.
	// See executor_statement_metrics.go for details.
	PhaseTimes() *phaseTimes

	// RecordStatement record stats for one statement.
	RecordStatement(
		stmt Statement,
		distSQLUsed bool,
		automaticRetryCount int,
		numRows int,
		err error,
		parseLat, planLat, runLat, svcLat, ovhLat float64,
	)

	// SQLStats provides access to the global sqlStats object.
	SQLStats() *sqlStats
}
