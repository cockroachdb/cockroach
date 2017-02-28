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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

type planMaker interface {
	// newPlan starts preparing the query plan for a single SQL
	// statement.
	//
	// It performs as many early checks as possible on the structure of
	// the SQL statement, including verifying permissions and type
	// checking.  The returned plan object is not ready to execute; the
	// optimizePlan() method must be called first. See makePlan()
	// below.
	//
	// This method should not be used directly; instead prefer makePlan()
	// or prepare() below.
	newPlan(stmt parser.Statement, desiredTypes []parser.Type, autoCommit bool) (planNode, error)

	// makePlan prepares the query plan for a single SQL statement.  it
	// calls newPlan() then optimizePlan() on the result.  Execution must
	// start by calling Start() first and then iterating using Next()
	// and Values() in order to retrieve matching rows.
	//
	// makePlan starts preparing the query plan for a single SQL
	// statement.
	// It performs as many early checks as possible on the structure of
	// the SQL statement, including verifying permissions and type checking.
	// The returned plan object is ready to execute. Execution
	// must start by calling Start() first and then iterating using
	// Next() and Values() in order to retrieve matching
	// rows.
	// If autoCommit is true, the plan is allowed (but not required) to
	// commit the transaction along with other KV operations.
	// Note: The autoCommit parameter enables operations to enable the
	// 1PC optimization. This is a bit hackish/preliminary at present.
	makePlan(stmt parser.Statement, autoCommit bool) (planNode, error)

	// prepare does the same checks as makePlan but skips building some
	// data structures necessary for execution, based on the assumption
	// that the plan will never be run. A planNode built with prepare()
	// will do just enough work to check the structural validity of the
	// SQL statement and determine types for placeholders. However it is
	// not appropriate to call optimizePlan(), Next() or Values() on a plan
	// object created with prepare().
	prepare(stmt parser.Statement) (planNode, error)
}

var _ planMaker = &planner{}

// planNode defines the interface for executing a query or portion of a query.
//
// The following methods apply to planNodes and contain special cases
// for each type; they thus need to be extended when adding/removing
// planNode instances:
// - planMaker.newPlan()
// - planMaker.prepare()
// - planMaker.setNeededColumns()  (needed_columns.go)
// - planMaker.expandPlan()        (expand_plan.go)
// - planVisitor.visit()           (walk.go)
// - planNodeNames                 (walk.go)
// - planMaker.optimizeFilters()   (filter_opt.go)
// - setLimitHint()                (limit_hint.go)
//
type planNode interface {
	// Columns returns the column names and types. The length of the
	// returned slice is guaranteed to be equal to the length of the
	// tuple returned by Values().
	//
	// Available after newPlan().
	Columns() ResultColumns

	// The indexes of the columns the output is ordered by.
	//
	// Stable after optimizePlan() (or makePlan).
	// Available after newPlan(), but may change on intermediate plan
	// nodes during optimizePlan() due to index selection.
	Ordering() orderingInfo

	// MarkDebug puts the node in a special debugging mode, which allows
	// DebugValues to be used. This should be called after Start() and
	// before the first call to Next() since it may need to recurse into
	// sub-nodes created by Start().
	//
	// Available after optimizePlan().
	MarkDebug(mode explainMode)

	// Start begins the processing of the query/statement and starts
	// performing side effects for data-modifying statements. Returns an
	// error if initial processing fails.
	//
	// Note: Don't use directly. Use startPlan() instead.
	//
	// Available after optimizePlan() (or makePlan).
	Start() error

	// Next performs one unit of work, returning false if an error is
	// encountered or if there is no more work to do. For statements
	// that return a result set, the Values() method will return one row
	// of results each time that Next() returns true.
	// See executor.go: countRowsAffected() and execStmt() for an example.
	//
	// Available after Start(). It is illegal to call Next() after it returns
	// false.
	Next() (bool, error)

	// Values returns the values at the current row. The result is only valid
	// until the next call to Next().
	//
	// Available after Next().
	Values() parser.Datums

	// DebugValues returns a set of debug values, valid until the next call to
	// Next(). This is only available for nodes that have been put in a special
	// "explainDebug" mode (using MarkDebug). When the output field in the
	// result is debugValueRow, a set of values is also available through
	// Values().
	//
	// Available after Next() and MarkDebug(explainDebug), see
	// explain.go.
	DebugValues() debugValues

	// Close terminates the planNode execution and releases its resources.
	// This method should be called if the node has been used in any way (any
	// methods on it have been called) after it was constructed. Note that this
	// doesn't imply that Start() has been necessarily called.
	Close()
}

// planNodeFastPath is implemented by nodes that can perform all their
// work during Start(), possibly affecting even multiple rows. For
// example, DELETE can do this.
type planNodeFastPath interface {
	// FastPathResults returns the affected row count and true if the
	// node has no result set and has already executed when Start() completes.
	FastPathResults() (int, bool)
}

var _ planNode = &alterTableNode{}
var _ planNode = &createDatabaseNode{}
var _ planNode = &createIndexNode{}
var _ planNode = &createTableNode{}
var _ planNode = &createViewNode{}
var _ planNode = &delayedNode{}
var _ planNode = &deleteNode{}
var _ planNode = &distinctNode{}
var _ planNode = &dropDatabaseNode{}
var _ planNode = &dropIndexNode{}
var _ planNode = &dropTableNode{}
var _ planNode = &dropViewNode{}
var _ planNode = &emptyNode{}
var _ planNode = &explainDebugNode{}
var _ planNode = &explainDistSQLNode{}
var _ planNode = &explainTraceNode{}
var _ planNode = &filterNode{}
var _ planNode = &groupNode{}
var _ planNode = &indexJoinNode{}
var _ planNode = &insertNode{}
var _ planNode = &joinNode{}
var _ planNode = &limitNode{}
var _ planNode = &ordinalityNode{}
var _ planNode = &scanNode{}
var _ planNode = &renderNode{}
var _ planNode = &sortNode{}
var _ planNode = &splitNode{}
var _ planNode = &unionNode{}
var _ planNode = &updateNode{}
var _ planNode = &valueGenerator{}
var _ planNode = &valuesNode{}
var _ planNode = &windowNode{}

// makePlan implements the Planner interface.
func (p *planner) makePlan(stmt parser.Statement, autoCommit bool) (planNode, error) {
	plan, err := p.newPlan(stmt, nil, autoCommit)
	if err != nil {
		return nil, err
	}
	if err := p.semaCtx.Placeholders.AssertAllAssigned(); err != nil {
		return nil, err
	}

	needed := allColumns(plan)
	plan, err = p.optimizePlan(plan, needed)
	if err != nil {
		return nil, err
	}

	if log.V(3) {
		log.Infof(p.ctx(), "statement %s compiled to:\n%s", stmt, planToString(plan))
	}
	return plan, nil
}

// startPlan starts the plan and all its sub-query nodes.
func (p *planner) startPlan(plan planNode) error {
	if err := p.startSubqueryPlans(plan); err != nil {
		return err
	}
	if err := plan.Start(); err != nil {
		return err
	}
	// Trigger limit propagation through the plan and sub-queries.
	setUnlimited(plan)
	return nil
}

func (p *planner) maybePlanHook(stmt parser.Statement) (planNode, error) {
	// TODO(dan): This iteration makes the plan dispatch no longer constant
	// time. We could fix that with a map of `reflect.Type` but including
	// reflection in such a primary codepath is unfortunate. Instead, the
	// upcoming IR work will provide unique numeric type tags, which will
	// elegantly solve this.
	state := PlanHookState{Ctx: p.ctx(), ExecCfg: p.execCfg}
	for _, planHook := range planHooks {
		if fn, header, err := planHook(state, stmt); err != nil {
			return nil, err
		} else if fn != nil {
			return &hookFnNode{f: fn, header: header}, nil
		}
	}
	return nil, nil
}

// newPlan constructs a planNode from a statement. This is used
// recursively by the various node constructors.
func (p *planner) newPlan(
	stmt parser.Statement, desiredTypes []parser.Type, autoCommit bool,
) (planNode, error) {
	tracing.AnnotateTrace()

	// This will set the system DB trigger for transactions containing
	// DDL statements that have no effect, such as
	// `BEGIN; INSERT INTO ...; CREATE TABLE IF NOT EXISTS ...; COMMIT;`
	// where the table already exists. This will generate some false
	// refreshes, but that's expected to be quite rare in practice.
	if stmt.StatementType() == parser.DDL {
		p.txn.SetSystemConfigTrigger()
	}

	if plan, err := p.maybePlanHook(stmt); plan != nil || err != nil {
		return plan, err
	}

	switch n := stmt.(type) {
	case *parser.AlterTable:
		return p.AlterTable(n)
	case *parser.BeginTransaction:
		return p.BeginTransaction(n)
	case CopyDataBlock:
		return p.CopyData(n, autoCommit)
	case *parser.CopyFrom:
		return p.CopyFrom(n, autoCommit)
	case *parser.CreateDatabase:
		return p.CreateDatabase(n)
	case *parser.CreateIndex:
		return p.CreateIndex(n)
	case *parser.CreateTable:
		return p.CreateTable(n)
	case *parser.CreateUser:
		return p.CreateUser(n)
	case *parser.CreateView:
		return p.CreateView(n)
	case *parser.Delete:
		return p.Delete(n, desiredTypes, autoCommit)
	case *parser.DropDatabase:
		return p.DropDatabase(n)
	case *parser.DropIndex:
		return p.DropIndex(n)
	case *parser.DropTable:
		return p.DropTable(n)
	case *parser.DropView:
		return p.DropView(n)
	case *parser.Explain:
		return p.Explain(n, autoCommit)
	case *parser.Grant:
		return p.Grant(n)
	case *parser.Help:
		return p.Help(n)
	case *parser.Insert:
		return p.Insert(n, desiredTypes, autoCommit)
	case *parser.ParenSelect:
		return p.newPlan(n.Select, desiredTypes, autoCommit)
	case *parser.RenameColumn:
		return p.RenameColumn(n)
	case *parser.RenameDatabase:
		return p.RenameDatabase(n)
	case *parser.RenameIndex:
		return p.RenameIndex(n)
	case *parser.RenameTable:
		return p.RenameTable(n)
	case *parser.Revoke:
		return p.Revoke(n)
	case *parser.Select:
		return p.Select(n, desiredTypes, autoCommit)
	case *parser.SelectClause:
		return p.SelectClause(n, nil, nil, desiredTypes, publicColumns)
	case *parser.Set:
		return p.Set(n)
	case *parser.SetTimeZone:
		return p.SetTimeZone(n)
	case *parser.SetTransaction:
		return p.SetTransaction(n)
	case *parser.SetDefaultIsolation:
		return p.SetDefaultIsolation(n)
	case *parser.Show:
		return p.Show(n)
	case *parser.ShowColumns:
		return p.ShowColumns(n)
	case *parser.ShowConstraints:
		return p.ShowConstraints(n)
	case *parser.ShowCreateTable:
		return p.ShowCreateTable(n)
	case *parser.ShowCreateView:
		return p.ShowCreateView(n)
	case *parser.ShowDatabases:
		return p.ShowDatabases(n)
	case *parser.ShowGrants:
		return p.ShowGrants(n)
	case *parser.ShowIndex:
		return p.ShowIndex(n)
	case *parser.ShowTables:
		return p.ShowTables(n)
	case *parser.ShowUsers:
		return p.ShowUsers(n)
	case *parser.Split:
		return p.Split(n)
	case *parser.Truncate:
		return p.Truncate(n)
	case *parser.UnionClause:
		return p.UnionClause(n, desiredTypes, autoCommit)
	case *parser.Update:
		return p.Update(n, desiredTypes, autoCommit)
	case *parser.ValuesClause:
		return p.ValuesClause(n, desiredTypes)
	default:
		return nil, errors.Errorf("unknown statement type: %T", stmt)
	}
}

func (p *planner) prepare(stmt parser.Statement) (planNode, error) {
	if plan, err := p.maybePlanHook(stmt); plan != nil || err != nil {
		return plan, err
	}

	switch n := stmt.(type) {
	case *parser.Delete:
		return p.Delete(n, nil, false)
	case *parser.Help:
		return p.Help(n)
	case *parser.Insert:
		return p.Insert(n, nil, false)
	case *parser.Select:
		return p.Select(n, nil, false)
	case *parser.SelectClause:
		return p.SelectClause(n, nil, nil, nil, publicColumns)
	case *parser.Show:
		return p.Show(n)
	case *parser.ShowCreateTable:
		return p.ShowCreateTable(n)
	case *parser.ShowCreateView:
		return p.ShowCreateView(n)
	case *parser.ShowColumns:
		return p.ShowColumns(n)
	case *parser.ShowDatabases:
		return p.ShowDatabases(n)
	case *parser.ShowGrants:
		return p.ShowGrants(n)
	case *parser.ShowIndex:
		return p.ShowIndex(n)
	case *parser.ShowConstraints:
		return p.ShowConstraints(n)
	case *parser.ShowTables:
		return p.ShowTables(n)
	case *parser.ShowUsers:
		return p.ShowUsers(n)
	case *parser.Split:
		return p.Split(n)
	case *parser.Update:
		return p.Update(n, nil, false)
	default:
		// Other statement types do not support placeholders so there is no need
		// for any special handling here.
		return nil, nil
	}
}
