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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// planner is the centerpiece of SQL statement execution combining session
// state and database state with the logic for SQL execution.
type planner struct {
	txn          *client.Txn
	session      Session
	user         string
	evalCtx      parser.EvalContext
	leases       map[ID]*LeaseState
	leaseMgr     *LeaseManager
	systemConfig config.SystemConfig
	// Keep track of tables that had schema changes that were completed.
	// The executor has the option to wait on completed schema changes
	// propagating to all nodes so that future commands use the latest
	// schema. This is not required for correctness, but it makes the UI
	// experience/tests predictable.
	//
	// TODO(vivek): Well, this is still needed for correctness as
	// long as the schema change is still executed in one transaction
	// without a staged process that takes the schema through all the state
	// changes. Once the staged process is rolled out delete this
	// TODO comment.
	completedSchemaChange []ID

	testingVerifyMetadata func(config.SystemConfig) error

	parser                   parser.Parser
	extractAggregatesVisitor extractAggregatesVisitor
	params                   parameters
	subqueryVisitor          subqueryVisitor
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
// in order to retrieve matching rows.
func (p *planner) makePlan(stmt parser.Statement) (planNode, error) {
	// This will set the system DB trigger for transactions containing
	// DDL statements that have no effect, such as
	// `BEGIN; INSERT INTO ...; CREATE TABLE IF NOT EXISTS ...; COMMIT;`
	// where the table already exists. This will generate some false
	// refreshes, but that's expected to be quite rare in practice.
	if stmt.StatementType() == parser.DDL {
		p.txn.SetSystemDBTrigger()
	}

	switch n := stmt.(type) {
	case *parser.AlterTable:
		return p.AlterTable(n)
	case *parser.BeginTransaction:
		return p.BeginTransaction(n)
	case *parser.CommitTransaction:
		return p.CommitTransaction(n)
	case *parser.CreateDatabase:
		return p.CreateDatabase(n)
	case *parser.CreateIndex:
		return p.CreateIndex(n)
	case *parser.CreateTable:
		return p.CreateTable(n)
	case *parser.Delete:
		return p.Delete(n)
	case *parser.DropDatabase:
		return p.DropDatabase(n)
	case *parser.DropIndex:
		return p.DropIndex(n)
	case *parser.DropTable:
		return p.DropTable(n)
	case *parser.Explain:
		return p.Explain(n)
	case *parser.Grant:
		return p.Grant(n)
	case *parser.Insert:
		return p.Insert(n)
	case *parser.ParenSelect:
		return p.makePlan(n.Select)
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
		return p.Select(n)
	case *parser.Set:
		return p.Set(n)
	case *parser.SetTimeZone:
		return p.SetTimeZone(n)
	case *parser.SetTransaction:
		return p.SetTransaction(n)
	case *parser.Show:
		return p.Show(n)
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
	case *parser.Update:
		return p.Update(n)
	case parser.Values:
		return p.Values(n)
	default:
		return nil, util.Errorf("unknown statement type: %T", stmt)
	}
}

func (p *planner) query(sql string, args ...interface{}) (planNode, error) {
	stmts, err := parser.ParseTraditional(sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, util.Errorf("expected single statement, found %d", len(stmts))
	}
	if err := parser.FillArgs(stmts[0], golangParameters(args)); err != nil {
		return nil, err
	}
	return p.makePlan(stmts[0])
}

func (p *planner) queryRow(sql string, args ...interface{}) (parser.DTuple, error) {
	plan, err := p.query(sql, args...)
	if err != nil {
		return nil, err
	}
	if !plan.Next() {
		if err := plan.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	values := plan.Values()
	if plan.Next() {
		return nil, util.Errorf("%s: unexpected multiple results", sql)
	}
	if err := plan.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func (p *planner) exec(sql string, args ...interface{}) (int, error) {
	plan, err := p.query(sql, args...)
	if err != nil {
		return 0, err
	}
	count := 0
	for plan.Next() {
		count++
	}
	return count, plan.Err()
}

// getAliasedTableLease looks up the table descriptor for an alias table
// expression.
func (p *planner) getAliasedTableLease(n parser.TableExpr) (*TableDescriptor, error) {
	ate, ok := n.(*parser.AliasedTableExpr)
	if !ok {
		return nil, util.Errorf("TODO(pmattis): unsupported FROM: %s", n)
	}
	table, ok := ate.Expr.(*parser.QualifiedName)
	if !ok {
		return nil, util.Errorf("TODO(pmattis): unsupported FROM: %s", n)
	}
	desc, err := p.getTableLease(table)
	if err != nil {
		return nil, err
	}
	if ate.As != "" {
		desc.Alias = string(ate.As)
	} else {
		desc.Alias = desc.Name
	}
	return desc, nil
}

// notify that the schema change is completed.
func (p *planner) notifyCompletedSchemaChange(id ID) {
	p.completedSchemaChange = append(p.completedSchemaChange, id)
}

func (p *planner) releaseLeases(db client.DB) {
	if p.leases != nil {
		for _, lease := range p.leases {
			if err := p.leaseMgr.Release(lease); err != nil {
				log.Warning(err)
			}
		}
		p.leases = nil
	}
}

type column struct {
	name string
	typ  parser.Datum
}

// planNode defines the interface for executing a query or portion of a query.
type planNode interface {
	// Columns returns the column names and types . The length of the
	// returned slice is guaranteed to be equal to the length of the
	// tuple returned by Values().
	Columns() []column
	// The indexes of the columns the output is ordered by. Indexes are 1-based
	// and negative indexes indicate descending ordering. The ordering return
	// value may be nil if no ordering has been performed. The prefix return
	// value indicates the prefix of the ordering for which an exact match has
	// been performed via filtering. This prefix may safely be ignored for
	// ordering considerations.
	Ordering() (ordering []int, prefix int)
	// Values returns the values at the current row. The result is only valid
	// until the next call to Next().
	Values() parser.DTuple
	// Next advances to the next row, returning false if an error is encountered
	// or if there is no next row.
	Next() bool
	// Err returns the error, if any, encountered during iteration.
	Err() error
	// ExplainPlan returns a name and description and a list of child nodes.
	ExplainPlan() (name, description string, children []planNode)
}

var _ planNode = &distinctNode{}
var _ planNode = &groupNode{}
var _ planNode = &indexJoinNode{}
var _ planNode = &limitNode{}
var _ planNode = &scanNode{}
var _ planNode = &sortNode{}
var _ planNode = &valuesNode{}
