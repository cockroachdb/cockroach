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
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"golang.org/x/net/context"
)

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

	// As the planner executes statements, it may change the current user session.
	session *Session

	// Reference to the corresponding sql Statement for this query.
	stmt *Statement

	// Contexts for different stages of planning and execution.
	semaCtx parser.SemaContext
	evalCtx parser.EvalContext

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

	// autoCommit indicates whether we're planning for a spontaneous transaction.
	// If autoCommit is true, the plan is allowed (but not required) to
	// commit the transaction along with other KV operations.
	// Note: The autoCommit parameter enables operations to enable the
	// 1PC optimization. This is a bit hackish/preliminary at present.
	autoCommit bool

	// phaseTimes helps measure the time spent in each phase of SQL execution.
	// See executor_statement_metrics.go for details.
	phaseTimes phaseTimes

	// cancelChecker is used by planNodes to check for cancellation of the associated
	// query.
	cancelChecker CancelChecker

	// planDeps, if non-nil, collects the table/view dependencies for this query.
	// Any planNode constructors that resolves a table name or reference in the query
	// to a descriptor must register this descriptor into planDeps.
	// This is (currently) used by CREATE VIEW.
	// TODO(knz): Remove this in favor of a better encapsulated mechanism.
	planDeps planDependencies

	// Avoid allocations by embedding commonly used objects and visitors.
	parser                parser.Parser
	subqueryVisitor       subqueryVisitor
	subqueryPlanVisitor   subqueryPlanVisitor
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

	s := &Session{
		Location: time.UTC,
		User:     user,
		TxnState: txnState{Ctx: ctx},
		context:  ctx,
		tables:   TableCollection{databaseCache: newDatabaseCache(config.SystemConfig{})},
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

	p := s.newPlanner(nil, txn)

	if txn != nil {
		if txn.Proto().OrigTimestamp == (hlc.Timestamp{}) {
			panic("makeInternalPlanner called with a transaction without timestamps")
		}
		ts := txn.Proto().OrigTimestamp.GoTime()
		p.evalCtx.SetTxnTimestamp(ts)
		p.evalCtx.SetStmtTimestamp(ts)
	}

	return p
}

func finishInternalPlanner(p *planner) {
	p.session.TxnState.mon.Stop(p.session.context)
	p.session.sessionMon.Stop(p.session.context)
	p.session.mon.Stop(p.session.context)
}

// ExecCfg implements the PlanHookState interface.
func (p *planner) ExecCfg() *ExecutorConfig {
	return p.session.execCfg
}

func (p *planner) LeaseMgr() *LeaseManager {
	return p.session.tables.leaseMgr
}

func (p *planner) User() string {
	return p.session.User
}

func (p *planner) EvalContext() parser.EvalContext {
	return p.evalCtx
}

// TODO(dan): This is here to implement PlanHookState, but it's not clear that
// this is the right abstraction. We could also export distSQLPlanner, for
// example. Revisit.
func (p *planner) DistLoader() *DistLoader {
	return &DistLoader{distSQLPlanner: p.session.distSQLPlanner}
}

// setTxn resets the current transaction in the planner and
// initializes the timestamps used by SQL built-in functions from
// the new txn object, if any.
func (p *planner) setTxn(txn *client.Txn) {
	p.txn = txn
	if txn != nil {
		p.evalCtx.SetClusterTimestamp(txn.OrigTimestamp())
	} else {
		p.evalCtx.SetTxnTimestamp(time.Time{})
		p.evalCtx.SetStmtTimestamp(time.Time{})
		p.evalCtx.SetClusterTimestamp(hlc.Timestamp{})
	}
}

// query initializes a planNode from a SQL statement string. Close() must be
// called on the returned planNode after use.
// This function is not suitable for use in the planNode constructors directly:
// the returned planNode has already been optimized.
// Consider also (*planner).delegateQuery(...).
func (p *planner) query(ctx context.Context, sql string, args ...interface{}) (planNode, error) {
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
	return p.makePlan(ctx, Statement{AST: stmt})
}

// QueryRow implements the parser.EvalPlanner interface.
func (p *planner) QueryRow(
	ctx context.Context, sql string, args ...interface{},
) (parser.Datums, error) {
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
		return nil, &parser.MultipleResultsError{SQL: sql}
	}
}

// queryRows executes a SQL query string where multiple result rows are returned.
func (p *planner) queryRows(
	ctx context.Context, sql string, args ...interface{},
) ([]parser.Datums, error) {
	plan, err := p.query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer plan.Close(ctx)
	if err := p.startPlan(ctx, plan); err != nil {
		return nil, err
	}
	params := runParams{
		ctx: ctx,
		p:   p,
	}
	if next, err := plan.Next(params); err != nil || !next {
		return nil, err
	}

	var rows []parser.Datums
	for {
		if values := plan.Values(); values != nil {
			valCopy := append(parser.Datums(nil), values...)
			rows = append(rows, valCopy)
		}

		next, err := plan.Next(params)
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}
	}
	return rows, nil
}

// exec executes a SQL query string and returns the number of rows
// affected.
func (p *planner) exec(ctx context.Context, sql string, args ...interface{}) (int, error) {
	plan, err := p.query(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	defer plan.Close(ctx)
	if err := p.startPlan(ctx, plan); err != nil {
		return 0, err
	}
	params := runParams{
		ctx: ctx,
		p:   p,
	}
	return countRowsAffected(params, plan)
}

func (p *planner) fillFKTableMap(ctx context.Context, m sqlbase.TableLookupsByID) error {
	for tableID := range m {
		table, err := p.session.tables.getTableVersionByID(ctx, p.txn, tableID)
		if err == errTableAdding {
			m[tableID] = sqlbase.TableLookup{IsAdding: true}
			continue
		}
		if err != nil {
			return err
		}
		m[tableID] = sqlbase.TableLookup{Table: table}
	}
	return nil
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
func (p *planner) TypeAsString(e parser.Expr, op string) (func() (string, error), error) {
	typedE, err := parser.TypeCheckAndRequire(e, &p.semaCtx, parser.TypeString, op)
	if err != nil {
		return nil, err
	}
	fn := func() (string, error) {
		d, err := typedE.Eval(&p.evalCtx)
		if err != nil {
			return "", err
		}
		return parser.AsStringWithFlags(d, parser.FmtBareStrings), nil
	}
	return fn, nil
}

// TypeAsString enforces (not hints) that the given expression typechecks as a
// string and returns a function that can be called to get the string value
// during (planNode).Start.
func (p *planner) TypeAsStringOpts(
	opts parser.KVOptions,
) (func() (map[string]string, error), error) {
	typed := make(map[string]parser.TypedExpr, len(opts))
	for _, opt := range opts {
		k := string(opt.Key)
		if opt.Value == nil {
			typed[k] = nil
			continue
		}
		r, err := parser.TypeCheckAndRequire(opt.Value, &p.semaCtx, parser.TypeString, k)
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
			d, err := e.Eval(&p.evalCtx)
			if err != nil {
				return nil, err
			}
			res[name] = parser.AsStringWithFlags(d, parser.FmtBareStrings)
		}
		return res, nil
	}
	return fn, nil
}

// TypeAsStringArray enforces (not hints) that the given expressions all typecheck as
// strings and returns a function that can be called to get the string values
// during (planNode).Start.
func (p *planner) TypeAsStringArray(
	exprs parser.Exprs, op string,
) (func() ([]string, error), error) {
	typedExprs := make([]parser.TypedExpr, len(exprs))
	for i := range exprs {
		typedE, err := parser.TypeCheckAndRequire(exprs[i], &p.semaCtx, parser.TypeString, op)
		if err != nil {
			return nil, err
		}
		typedExprs[i] = typedE
	}
	fn := func() ([]string, error) {
		strs := make([]string, len(exprs))
		for i := range exprs {
			d, err := typedExprs[i].Eval(&p.evalCtx)
			if err != nil {
				return nil, err
			}
			strs[i] = parser.AsStringWithFlags(d, parser.FmtBareStrings)
		}
		return strs, nil
	}
	return fn, nil
}
