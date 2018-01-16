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
	"github.com/pkg/errors"
)

// extendedEvalCtx extends tree.EvalContext with fields that are just needed in
// the sql package.
type extendedEvalContext struct {
	tree.EvalContext

	SessionMutator sessionDataMutator

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

	// Tables points to the Session's table collection.
	Tables *TableCollection

	ExecCfg *ExecutorConfig

	DistSQLPlanner *DistSQLPlanner

	TestingVerifyMetadata testingVerifyMetadata

	TxnModesSetter txnModesSetter

	SchemaChangers *schemaChangerCollection
}

type testingVerifyMetadata interface {
	// setTestingVerifyMetadata sets a callback to be called after the Session
	// is done executing the current SQL statement. It can be used to verify
	// assumptions about how metadata will be asynchronously updated.
	// Note that this can overwrite a previous callback that was waiting to be
	// verified, which is not ideal.
	setTestingVerifyMetadata(fn func(config.SystemConfig) error)
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

	// session is the Session on whose behalf this planner is working.
	session *Session

	// preparedStatements points to the Session's collection of prepared
	// statements.
	preparedStatements *PreparedStatements

	// Reference to the corresponding sql Statement for this query.
	stmt *Statement

	// Contexts for different stages of planning and execution.
	semaCtx         tree.SemaContext
	extendedEvalCtx extendedEvalContext

	// sessionDataMutator is used to mutate the session variables. Read
	// access to them is provided through evalCtx.
	sessionDataMutator sessionDataMutator

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

	// phaseTimes helps measure the time spent in each phase of SQL execution.
	// See executor_statement_metrics.go for details.
	phaseTimes phaseTimes

	// cancelChecker is used by planNodes to check for cancellation of the associated
	// query.
	cancelChecker *sqlbase.CancelChecker

	// isPreparing is true if this planner is currently preparing.
	isPreparing bool

	// curPlan collects the properties of the current plan being prepared. This state
	// is undefined at the beginning of the planning of each new statement, and cannot
	// be reused for an old prepared statement after a new statement has been prepared.
	//
	// Note: some additional per-statement state is also stored in
	// semaCtx (placeholders).
	// TODO(jordan): investigate whether/how per-plan state like
	// placeholder data can be concentrated in a single struct.
	curPlan struct {
		// deps, if non-nil, collects the table/view dependencies for this query.
		// Any planNode constructors that resolves a table name or reference in the query
		// to a descriptor must register this descriptor into planDeps.
		// This is (currently) used by CREATE VIEW.
		// TODO(knz): Remove this in favor of a better encapsulated mechanism.
		deps planDependencies

		// cteNameEnvironment collects the mapping from common table expression alias
		// to the planNodes that represent their source.
		cteNameEnvironment cteNameEnvironment

		// hasStar collects whether any star expansion has occurred during
		// logical plan construction. This is used by CREATE VIEW until
		// #10028 is addressed.
		hasStar bool
		// hasSubqueries collects whether any subqueries expansion has
		// occurred during logical plan construction.
		hasSubqueries bool
		// plannedExecute is true if this planner has planned an EXECUTE statement.
		plannedExecute bool
	}

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

var emptyPlanner planner

// noteworthyInternalMemoryUsageBytes is the minimum size tracked by each
// internal SQL pool before the pool starts explicitly logging overall usage
// growth in the log.
var noteworthyInternalMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_INTERNAL_MEMORY_USAGE", 100*1024)

// makePlanner creates a new planner instance, referencing a dummy session.
func makeInternalPlanner(
	opName string, txn *client.Txn, user string, memMetrics *MemoryMetrics,
) *planner {
	// init with an empty session. We can't leave this nil because too much code
	// looks in the session for the current database.
	ctx := log.WithLogTagStr(context.Background(), opName, "")

	data := sessiondata.SessionData{
		Location: time.UTC,
		User:     user,
	}

	s := &Session{
		data:     data,
		TxnState: txnState{Ctx: ctx},
		context:  ctx,
		tables:   TableCollection{databaseCache: newDatabaseCache(config.SystemConfig{})},
	}
	s.dataMutator = sessionDataMutator{
		data: &s.data,
		s:    s,
		defaults: sessionDefaults{
			applicationName: "crdb-internal",
			database:        "",
		},
		settings:       nil,
		curTxnReadOnly: &s.TxnState.readOnly,
	}
	s.mon = mon.MakeUnlimitedMonitor(ctx,
		"internal-root",
		mon.MemoryResource,
		memMetrics.CurBytesCount, memMetrics.MaxBytesHist,
		noteworthyInternalMemoryUsageBytes)

	s.sessionMon = mon.MakeMonitor("internal-session",
		mon.MemoryResource,
		memMetrics.SessionCurBytesCount,
		memMetrics.SessionMaxBytesHist,
		-1, noteworthyInternalMemoryUsageBytes/5)
	s.sessionMon.Start(ctx, &s.mon, mon.BoundAccount{})

	s.TxnState.mon = mon.MakeMonitor("internal-txn",
		mon.MemoryResource,
		memMetrics.TxnCurBytesCount,
		memMetrics.TxnMaxBytesHist,
		-1, noteworthyInternalMemoryUsageBytes/5)
	s.TxnState.mon.Start(ctx, &s.mon, mon.BoundAccount{})

	var ts time.Time
	if txn != nil {
		if txn.Proto().OrigTimestamp == (hlc.Timestamp{}) {
			panic("makeInternalPlanner called with a transaction without timestamps")
		}
		ts = txn.Proto().OrigTimestamp.GoTime()
	}
	p := s.newPlanner(txn, ts /* txnTimestamp */, ts /* stmtTimestamp */, nil /* reCache */)

	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	p.extendedEvalCtx.Tables = &s.tables

	return p
}

func finishInternalPlanner(p *planner) {
	p.session.TxnState.mon.Stop(p.session.context)
	p.session.sessionMon.Stop(p.session.context)
	p.session.mon.Stop(p.session.context)
}

func (p *planner) ExtendedEvalContext() extendedEvalContext {
	return p.extendedEvalCtx
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

// makeInternalPlan initializes a planNode from a SQL statement string.
// Close() must be called on the returned planNode after use.
// This function changes the planner's placeholder map. It is the caller's
// responsibility to save and restore the old map if desired.
// This function is not suitable for use in the planNode constructors directly:
// the returned planNode has already been optimized.
// Consider also (*planner).delegateQuery(...).
func (p *planner) makeInternalPlan(
	ctx context.Context, sql string, args ...interface{},
) (planNode, error) {
	if log.V(2) {
		log.Infof(ctx, "internal query: %s", sql)
		if len(args) > 0 {
			log.Infof(ctx, "placeholders: %q", args)
		}
	}
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		return nil, err
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

// ParseTableNameWithIndex implements the parser.EvalPlanner interface.
// We define this here to break the dependency from builtins.go to the parser.
func (p *planner) ParseTableNameWithIndex(sql string) (tree.TableNameWithIndex, error) {
	return parser.ParseTableNameWithIndex(sql)
}

// QueryRow implements the parser.EvalPlanner interface.
func (p *planner) QueryRow(
	ctx context.Context, sql string, args ...interface{},
) (tree.Datums, error) {
	rows, err := p.queryRows(ctx, sql, args...)
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
) ([]tree.Datums, error) {
	oldPlaceholders := p.semaCtx.Placeholders
	defer func() { p.semaCtx.Placeholders = oldPlaceholders }()

	plan, err := p.makeInternalPlan(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer plan.Close(ctx)

	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: &p.extendedEvalCtx,
		p:               p,
	}
	if err := startPlan(params, plan); err != nil {
		return nil, err
	}
	var rows []tree.Datums
	if err = forEachRow(params, plan, func(values tree.Datums) error {
		if values != nil {
			valCopy := append(tree.Datums(nil), values...)
			rows = append(rows, valCopy)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return rows, nil
}

// exec executes a SQL query string and returns the number of rows
// affected.
func (p *planner) exec(ctx context.Context, sql string, args ...interface{}) (int, error) {
	oldPlaceholders := p.semaCtx.Placeholders
	defer func() { p.semaCtx.Placeholders = oldPlaceholders }()
	plan, err := p.makeInternalPlan(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	defer plan.Close(ctx)

	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: &p.extendedEvalCtx,
		p:               p,
	}
	if err := startPlan(params, plan); err != nil {
		return 0, err
	}
	return countRowsAffected(params, plan)
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

// isDatabaseVisible returns true if the given database is visible
// given the provided prefix.
// An empty prefix makes all databases visible.
// System databases are always visible.
// Otherwise only the database with the same name as the prefix is available.
func isDatabaseVisible(dbName, prefix, user string) bool {
	if isSystemDatabaseName(dbName) {
		return true
	} else if dbName == prefix {
		return true
	} else if prefix == "" {
		return true
	}
	return false
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
	return &p.EvalContext().SessionData
}

func (p *planner) testingVerifyMetadata() testingVerifyMetadata {
	return p.extendedEvalCtx.TestingVerifyMetadata
}

// txnModesSetter is an interface used by SQL execution to influence the current
// transaction.
type txnModesSetter interface {
	setTransactionModes(modes tree.TransactionModes) error
}
