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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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

	// Contexts for different stages of planning and execution.
	semaCtx parser.SemaContext
	evalCtx parser.EvalContext

	// If set, table descriptors will only be fetched at the time of the
	// transaction, not leased. This is used for things like AS OF SYSTEM TIME
	// queries and building query plans for views when they're created.
	// It's used in layers below the executor to modify the behavior of SELECT.
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

	// Avoid allocations by embedding commonly used objects and visitors.
	parser                parser.Parser
	subqueryVisitor       subqueryVisitor
	subqueryPlanVisitor   subqueryPlanVisitor
	nameResolutionVisitor nameResolutionVisitor
	srfExtractionVisitor  srfExtractionVisitor
}

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
		leases:   LeaseCollection{databaseCache: newDatabaseCache(config.SystemConfig{})},
	}

	s.mon = mon.MakeUnlimitedMonitor(ctx,
		"internal-root",
		memMetrics.CurBytesCount, memMetrics.MaxBytesHist,
		noteworthyInternalMemoryUsageBytes)

	s.sessionMon = mon.MakeMonitor("internal-session",
		memMetrics.SessionCurBytesCount,
		memMetrics.SessionMaxBytesHist,
		-1, noteworthyInternalMemoryUsageBytes/5)
	s.sessionMon.Start(ctx, &s.mon, mon.BoundAccount{})

	s.TxnState.mon = mon.MakeMonitor("internal-txn",
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
	return p.session.leases.leaseMgr
}

func (p *planner) User() string {
	return p.session.User
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
	return p.makePlan(ctx, stmt)
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
	if next, err := plan.Next(ctx); err != nil || !next {
		return nil, err
	}

	var rows []parser.Datums
	for {
		if values := plan.Values(); values != nil {
			valCopy := append(parser.Datums(nil), values...)
			rows = append(rows, valCopy)
		}

		next, err := plan.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}
	}
	return rows, nil
}

// queryRowsAsRoot executes a SQL query string using security.RootUser
// and multiple result rows are returned.
func (p *planner) queryRowsAsRoot(
	ctx context.Context, sql string, args ...interface{},
) ([]parser.Datums, error) {
	currentUser := p.session.User
	defer func() { p.session.User = currentUser }()
	p.session.User = security.RootUser
	return p.queryRows(ctx, sql, args...)
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
	return countRowsAffected(ctx, plan)
}

func (p *planner) fillFKTableMap(ctx context.Context, m sqlbase.TableLookupsByID) error {
	for tableID := range m {
		table, err := p.session.leases.getTableLeaseByID(ctx, p.txn, tableID)
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

// isDatabaseVisible returns true if the given database is visible to the
// current user. Only the current database and system databases are available
// to ordinary users; everything is available to root.
func (p *planner) isDatabaseVisible(dbName string) bool {
	if p.session.User == security.RootUser {
		return true
	} else if dbName == p.evalCtx.Database {
		return true
	} else if isSystemDatabaseName(dbName) {
		return true
	}
	return false
}

// TypeAsString enforces (not hints) that the given expression typechecks as a
// string and returns a function that can be called to get the string value
// during (planNode).Start.
func (p *planner) TypeAsString(e *parser.Expr) (func() string, error) {
	typedE, err := (*e).TypeCheck(&p.semaCtx, parser.TypeString)
	if err != nil {
		return nil, err
	}
	if typ := typedE.ResolvedType(); typ != parser.TypeString {
		return nil, errors.Errorf("expression '%s' did not type as a string: %s", *e, typ)
	}
	*e = typedE
	fn := func() string {
		return string(*(*e).(*parser.DString))
	}
	return fn, nil
}

// TypeAsString enforces (not hints) that the given expressions all typecheck as
// a string and returns a function that can be called to get the string values
// during (planNode).Start.
func (p *planner) TypeAsStringArray(exprs *parser.Exprs) (func() []string, error) {
	for i := range *exprs {
		typedE, err := (*exprs)[i].TypeCheck(&p.semaCtx, parser.TypeString)
		if err != nil {
			return nil, err
		}
		if typ := typedE.ResolvedType(); typ != parser.TypeString {
			return nil, errors.Errorf("expression '%s' did not type as a string: %s", (*exprs)[i], typ)
		}
		(*exprs)[i] = typedE
	}
	fn := func() []string {
		strs := make([]string, len(*exprs))
		for i := range *exprs {
			strs[i] = string(*(*exprs)[i].(*parser.DString))
		}
		return strs
	}
	return fn, nil
}
