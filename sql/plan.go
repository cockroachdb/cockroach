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
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// planner is the centerpiece of SQL statement execution combining session
// state and database state with the logic for SQL execution.
// Create new instances using `makePlanner()`.
type planner struct {
	txn *client.Txn
	// As the planner executes statements, it may change the current user session.
	session       *Session
	user          string
	evalCtx       parser.EvalContext
	leases        []*LeaseState
	leaseMgr      *LeaseManager
	systemConfig  config.SystemConfig
	databaseCache *databaseCache

	// TODO(mjibson): remove prepareOnly in favor of a 2-step prepare-exec solution
	// that is also able to save the plan to skip work during the exec step.
	prepareOnly bool

	testingVerifyMetadata func(config.SystemConfig) error

	parser             parser.Parser
	isAggregateVisitor isAggregateVisitor
	params             parameters
	subqueryVisitor    subqueryVisitor

	// Callback used when a node wants to schedule a SchemaChanger
	// for execution at the end of the current transaction.
	schemaChangeCallback func(schemaChanger SchemaChanger)
}

func makePlanner() *planner {
	// init with an empty session. We can't leave this nil because too much code
	// looks in the session for the current database.
	return &planner{session: &Session{}}
}

func (p *planner) setTxn(txn *client.Txn, timestamp time.Time) {
	p.txn = txn
	p.evalCtx.TxnTimestamp = parser.DTimestamp{Time: timestamp}
}

func (p *planner) resetTxn() {
	p.setTxn(nil, time.Time{})
}

// makePlan creates the query plan for a single SQL statement. The returned
// plan needs to be iterated over using planNode.Next() and planNode.Values()
// in order to retrieve matching rows. If autoCommit is true, the plan is
// allowed (but not required) to commit the transaction along with other KV
// operations.
//
// Note: The autoCommit parameter enables operations to enable the 1PC
// optimization. This is a bit hackish/preliminary at present.
func (p *planner) makePlan(stmt parser.Statement, autoCommit bool) (planNode, *roachpb.Error) {
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
		pNode, err := p.BeginTransaction(n)
		return pNode, roachpb.NewError(err)
	case *parser.CommitTransaction:
		return p.CommitTransaction(n)
	case *parser.CreateDatabase:
		return p.CreateDatabase(n)
	case *parser.CreateIndex:
		return p.CreateIndex(n)
	case *parser.CreateTable:
		return p.CreateTable(n)
	case *parser.Delete:
		return p.Delete(n, autoCommit)
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
		return p.Insert(n, autoCommit)
	case *parser.ParenSelect:
		return p.makePlan(n.Select, autoCommit)
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
	case *parser.RollbackTransaction:
		return p.RollbackTransaction(n)
	case *parser.Select:
		return p.Select(n, autoCommit)
	case *parser.SelectClause:
		return p.SelectClause(n)
	case *parser.Set:
		return p.Set(n)
	case *parser.SetTimeZone:
		pNode, err := p.SetTimeZone(n)
		return pNode, roachpb.NewError(err)
	case *parser.SetTransaction:
		pNode, err := p.SetTransaction(n)
		return pNode, roachpb.NewError(err)
	case *parser.SetDefaultIsolation:
		pNode, err := p.SetDefaultIsolation(n)
		return pNode, roachpb.NewError(err)
	case *parser.Show:
		pNode, err := p.Show(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowColumns:
		return p.ShowColumns(n)
	case *parser.ShowDatabases:
		return p.ShowDatabases(n)
	case *parser.ShowGrants:
		return p.ShowGrants(n)
	case *parser.ShowIndex:
		return p.ShowIndex(n)
	case *parser.ShowTables:
		return p.ShowTables(n)
	case *parser.Truncate:
		return p.Truncate(n)
	case *parser.UnionClause:
		return p.UnionClause(n, autoCommit)
	case *parser.Update:
		return p.Update(n, autoCommit)
	case *parser.ValuesClause:
		return p.ValuesClause(n)
	default:
		return nil, roachpb.NewErrorf("unknown statement type: %T", stmt)
	}
}

func (p *planner) prepare(stmt parser.Statement) (planNode, *roachpb.Error) {
	p.prepareOnly = true
	switch n := stmt.(type) {
	case *parser.Delete:
		return p.Delete(n, false)
	case *parser.Insert:
		return p.Insert(n, false)
	case *parser.Select:
		return p.Select(n, false)
	case *parser.SelectClause:
		return p.SelectClause(n)
	case *parser.Show:
		pNode, err := p.Show(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowColumns:
		return p.ShowColumns(n)
	case *parser.ShowDatabases:
		return p.ShowDatabases(n)
	case *parser.ShowGrants:
		return p.ShowGrants(n)
	case *parser.ShowIndex:
		return p.ShowIndex(n)
	case *parser.ShowTables:
		return p.ShowTables(n)
	case *parser.Update:
		return p.Update(n, false)
	default:
		// Other statement types do not support placeholders so there is no need
		// for any special handling here.
		return nil, nil
	}
}

func (p *planner) query(sql string, args ...interface{}) (planNode, *roachpb.Error) {
	stmt, err := parser.ParseOneTraditional(sql)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	stmt, err = parser.FillArgs(stmt, golangParameters(args))
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	return p.makePlan(stmt, false)
}

func (p *planner) queryRow(sql string, args ...interface{}) (parser.DTuple, *roachpb.Error) {
	plan, err := p.query(sql, args...)
	if err != nil {
		return nil, err
	}
	if !plan.Next() {
		if pErr := plan.PErr(); pErr != nil {
			return nil, pErr
		}
		return nil, nil
	}
	values := plan.Values()
	if plan.Next() {
		return nil, roachpb.NewErrorf("%s: unexpected multiple results", sql)
	}
	if pErr := plan.PErr(); pErr != nil {
		return nil, pErr
	}
	return values, nil
}

func (p *planner) exec(sql string, args ...interface{}) (int, *roachpb.Error) {
	plan, pErr := p.query(sql, args...)
	if pErr != nil {
		return 0, pErr
	}
	return countRowsAffected(plan), plan.PErr()
}

// getAliasedTableLease looks up the table descriptor for an alias table
// expression.
func (p *planner) getAliasedTableLease(n parser.TableExpr) (*TableDescriptor, *roachpb.Error) {
	ate, ok := n.(*parser.AliasedTableExpr)
	if !ok {
		return nil, roachpb.NewErrorf("TODO(pmattis): unsupported FROM: %s", n)
	}
	table, ok := ate.Expr.(*parser.QualifiedName)
	if !ok {
		return nil, roachpb.NewErrorf("TODO(pmattis): unsupported FROM: %s", n)
	}
	desc, pErr := p.getTableLease(table)
	if pErr != nil {
		return nil, pErr
	}
	return &desc, nil
}

// notify that an outstanding schema change exists for the table.
func (p *planner) notifySchemaChange(id ID, mutationID MutationID) {
	sc := SchemaChanger{
		tableID:    id,
		mutationID: mutationID,
		nodeID:     p.evalCtx.NodeID,
		cfg:        p.systemConfig,
		leaseMgr:   p.leaseMgr,
	}
	if p.schemaChangeCallback == nil {
		panic("Schema changer encountered on planner not configured with a schemaChangeCallback")
	}
	p.schemaChangeCallback(sc)
}

func (p *planner) releaseLeases() {
	if p.leases != nil {
		for _, lease := range p.leases {
			if err := p.leaseMgr.Release(lease); err != nil {
				log.Warning(err)
			}
		}
		p.leases = nil
	}
}

// planNode defines the interface for executing a query or portion of a query.
type planNode interface {
	// Columns returns the column names and types . The length of the
	// returned slice is guaranteed to be equal to the length of the
	// tuple returned by Values().
	Columns() []ResultColumn
	// The indexes of the columns the output is ordered by.
	Ordering() orderingInfo
	// Values returns the values at the current row. The result is only valid
	// until the next call to Next().
	Values() parser.DTuple
	// DebugValues returns a set of debug values, valid until the next call to Next(). This is only
	// available for nodes that have been put in a special "explainDebug" mode (using
	// MarkDebug). When the output field in the result is debugValueRow, a set of values is
	// also available through Values().
	DebugValues() debugValues
	// Next advances to the next row, returning false if an error is encountered
	// or if there is no next row.
	Next() bool
	// PErr returns the error, if any, encountered during iteration.
	PErr() *roachpb.Error
	// ExplainPlan returns a name and description and a list of child nodes.
	ExplainPlan() (name, description string, children []planNode)
	// SetLimitHint tells this node to optimize things under the assumption that we will only need
	// the first `numRows` rows. This is only a hint; the node must still be able to produce all
	// results if requested.
	SetLimitHint(numRows int64)
	// MarkDebug puts the node in a special debugging mode, which allows DebugValues to be used.
	MarkDebug(mode explainMode)
}

var _ planNode = &distinctNode{}
var _ planNode = &groupNode{}
var _ planNode = &indexJoinNode{}
var _ planNode = &limitNode{}
var _ planNode = &scanNode{}
var _ planNode = &sortNode{}
var _ planNode = &valuesNode{}
var _ planNode = &selectNode{}
var _ planNode = &unionNode{}
var _ planNode = &emptyNode{}
var _ planNode = &explainDebugNode{}
var _ planNode = &explainTraceNode{}

// emptyNode is a planNode with no columns and either no rows (default) or a single row with empty
// results (if results is initializer to true). The former is used for nodes that have no results
// (e.g. a table for which the filtering condition has a contradiction), the latter is used by
// select statements that have no table or where we detect the filtering condition throws away all
// results.
type emptyNode struct {
	results bool
}

func (*emptyNode) Columns() []ResultColumn { return nil }
func (*emptyNode) Ordering() orderingInfo  { return orderingInfo{} }
func (*emptyNode) Values() parser.DTuple   { return nil }
func (*emptyNode) PErr() *roachpb.Error    { return nil }

func (*emptyNode) ExplainPlan() (name, description string, children []planNode) {
	return "empty", "-", nil
}

func (*emptyNode) MarkDebug(_ explainMode) {}

func (*emptyNode) DebugValues() debugValues {
	return debugValues{
		rowIdx: 0,
		key:    "",
		value:  parser.DNull.String(),
		output: debugValueRow,
	}
}

func (e *emptyNode) Next() bool {
	r := e.results
	e.results = false
	return r
}

func (*emptyNode) SetLimitHint(_ int64) {}
