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
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/pkg/errors"
)

type planMaker interface {
	// newPlan starts preparing the query plan for a single SQL
	// statement.
	//
	// It performs as many early checks as possible on the structure of
	// the SQL statement, including verifying permissions and type
	// checking.  The returned plan object is not ready to execute; the
	// planNode.expandPlan() method must be called first. See makePlan()
	// below.
	//
	// This method should not be used directly; instead prefer makePlan()
	// or prepare() below.
	newPlan(stmt parser.Statement, desiredTypes []parser.Datum, autoCommit bool) (planNode, error)

	// makePlan prepares the query plan for a single SQL statement.  it
	// calls newPlan() then expandPlan() on the result.  Execution must
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
	// not appropriate to call expandPlan(), Next() or Values() on a plan
	// object created with prepare().
	prepare(stmt parser.Statement) (planNode, error)
}

var _ planMaker = &planner{}

// planNode defines the interface for executing a query or portion of a query.
type planNode interface {
	// ExplainTypes reports the data types involved in the node, excluding
	// the result column types.
	//
	// Available after newPlan().
	ExplainTypes(explainFn func(elem string, desc string))

	// SetLimitHint tells this node to optimize things under the assumption that
	// we will only need the first `numRows` rows.
	//
	// The special value math.MaxInt64 indicates "no limit".
	//
	// If soft is true, this is a "soft" limit and is only a hint; the node must
	// still be able to produce all results if requested.
	//
	// If soft is false, this is a "hard" limit and is a promise that Next will
	// never be called more than numRows times.
	//
	// The action of calling this method triggers limit-based query plan
	// optimizations, e.g. in selectNode.expandPlan(). The primary user
	// is limitNode.Start() after it has fully evaluated the limit and
	// offset expressions. EXPLAIN also does this, see
	// explainTypesNode.expandPlan() and explainPlanNode.expandPlan().
	//
	// TODO(radu) Arguably, this interface has room for improvement.  A
	// limitNode may have a hard limit locally which is larger than the
	// soft limit propagated up by nodes downstream. We may want to
	// improve this API to pass both the soft and hard limit.
	//
	// Available during/after newPlan().
	SetLimitHint(numRows int64, soft bool)

	// expandPlan finalizes type checking of placeholders and expands
	// the query plan to its final form, including index selection and
	// expansion of sub-queries. Returns an error if the initialization
	// fails.  The SQL "prepare" phase, as well as the EXPLAIN
	// statement, should merely build the plan node(s) and call
	// expandPlan(). This is called automatically by makePlan().
	//
	// Available after newPlan().
	expandPlan() error

	// ExplainPlan returns a name and description and a list of child nodes.
	//
	// Available after expandPlan() (or makePlan).
	ExplainPlan(verbose bool) (name, description string, children []planNode)

	// Columns returns the column names and types. The length of the
	// returned slice is guaranteed to be equal to the length of the
	// tuple returned by Values().
	//
	// Stable after expandPlan() (or makePlan).
	// Available after newPlan(), but may change on intermediate plan
	// nodes during expandPlan() due to index selection.
	Columns() []ResultColumn

	// The indexes of the columns the output is ordered by.
	//
	// Stable after expandPlan() (or makePlan).
	// Available after newPlan(), but may change on intermediate plan
	// nodes during expandPlan() due to index selection.
	Ordering() orderingInfo

	// MarkDebug puts the node in a special debugging mode, which allows
	// DebugValues to be used. This should be called after Start() and
	// before the first call to Next() since it may need to recurse into
	// sub-nodes created by Start().
	//
	// Available after expandPlan().
	MarkDebug(mode explainMode)

	// Start begins the processing of the query/statement and starts
	// performing side effects for data-modifying statements. Returns an
	// error if initial processing fails.
	//
	// Available after expandPlan() (or makePlan).
	Start() error

	// Next performs one unit of work, returning false if an error is
	// encountered or if there is no more work to do. For statements
	// that return a result set, the Value() method will return one row
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
	Values() parser.DTuple

	// DebugValues returns a set of debug values, valid until the next call to
	// Next(). This is only available for nodes that have been put in a special
	// "explainDebug" mode (using MarkDebug). When the output field in the
	// result is debugValueRow, a set of values is also available through
	// Values().
	//
	// Available after Next() and MarkDebug(explainDebug), see
	// explain.go.
	DebugValues() debugValues
}

// planNodeFastPath is implemented by nodes that can perform all their
// work during Start(), possibly affecting even multiple rows. For
// example, DELETE can do this.
type planNodeFastPath interface {
	// FastPathResults returns the affected row count and true if the
	// node has no result set and has already executed when Start() completes.
	FastPathResults() (int, bool)
}

var _ planNode = &distinctNode{}
var _ planNode = &groupNode{}
var _ planNode = &indexJoinNode{}
var _ planNode = &limitNode{}
var _ planNode = &scanNode{}
var _ planNode = &sortNode{}
var _ planNode = &valuesNode{}
var _ planNode = &selectTopNode{}
var _ planNode = &selectNode{}
var _ planNode = &unionNode{}
var _ planNode = &emptyNode{}
var _ planNode = &explainDebugNode{}
var _ planNode = &explainTraceNode{}
var _ planNode = &insertNode{}
var _ planNode = &updateNode{}
var _ planNode = &deleteNode{}
var _ planNode = &createDatabaseNode{}
var _ planNode = &createTableNode{}
var _ planNode = &createIndexNode{}
var _ planNode = &dropDatabaseNode{}
var _ planNode = &dropTableNode{}
var _ planNode = &dropIndexNode{}
var _ planNode = &alterTableNode{}
var _ planNode = &joinNode{}

// makePlan implements the Planner interface.
func (p *planner) makePlan(stmt parser.Statement, autoCommit bool) (planNode, error) {
	plan, err := p.newPlan(stmt, nil, autoCommit)
	if err != nil {
		return nil, err
	}
	if err := plan.expandPlan(); err != nil {
		return nil, err
	}
	return plan, nil
}

// newPlan constructs a planNode from a statement. This is used
// recursively by the various node constructors.
func (p *planner) newPlan(stmt parser.Statement, desiredTypes []parser.Datum, autoCommit bool) (planNode, error) {
	tracing.AnnotateTrace()

	// This will set the system DB trigger for transactions containing
	// DDL statements that have no effect, such as
	// `BEGIN; INSERT INTO ...; CREATE TABLE IF NOT EXISTS ...; COMMIT;`
	// where the table already exists. This will generate some false
	// refreshes, but that's expected to be quite rare in practice.
	if stmt.StatementType() == parser.DDL {
		p.txn.SetSystemConfigTrigger()
	}

	switch n := stmt.(type) {
	case *parser.AlterTable:
		return p.AlterTable(n)
	case *parser.BeginTransaction:
		return p.BeginTransaction(n)
	case *parser.CreateDatabase:
		return p.CreateDatabase(n)
	case *parser.CreateIndex:
		return p.CreateIndex(n)
	case *parser.CreateTable:
		return p.CreateTable(n)
	case *parser.Delete:
		return p.Delete(n, desiredTypes, autoCommit)
	case *parser.DropDatabase:
		return p.DropDatabase(n)
	case *parser.DropIndex:
		return p.DropIndex(n)
	case *parser.DropTable:
		return p.DropTable(n)
	case *parser.Explain:
		return p.Explain(n, autoCommit)
	case *parser.Grant:
		return p.Grant(n)
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
	case *parser.ShowCreateTable:
		return p.ShowCreateTable(n)
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
	switch n := stmt.(type) {
	case *parser.Delete:
		return p.Delete(n, nil, false)
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
	case *parser.Update:
		return p.Update(n, nil, false)
	default:
		// Other statement types do not support placeholders so there is no need
		// for any special handling here.
		return nil, nil
	}
}
