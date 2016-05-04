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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// planner is the centerpiece of SQL statement execution combining session
// state and database state with the logic for SQL execution.
// A planner is generally part of a Session object. If one needs to be created
// outside of a Session, use makePlanner().
type planner struct {
	txn *client.Txn
	// As the planner executes statements, it may change the current user session.
	// TODO(andrei): see if the circular dependency between planner and Session
	// can be broken if we move the User and Database here from the Session.
	session       *Session
	evalCtx       parser.EvalContext
	leases        []*LeaseState
	leaseMgr      *LeaseManager
	systemConfig  config.SystemConfig
	databaseCache *databaseCache

	testingVerifyMetadataFn func(config.SystemConfig) error
	verifyFnCheckedOnce     bool

	parser parser.Parser
	params parameters

	// Avoid allocations by embedding commonly used visitors.
	isAggregateVisitor isAggregateVisitor
	subqueryVisitor    subqueryVisitor
	qnameVisitor       qnameVisitor

	execCtx *ExecutorContext
}

// setTestingVerifyMetadata sets a callback to be called after the planner
// is done executing the current SQL statement. It can be used to verify
// assumptions about how metadata will be asynchronously updated.
// Note that this can overwrite a previous callback that was waiting to be
// verified, which is not ideal.
func (p *planner) setTestingVerifyMetadata(fn func(config.SystemConfig) error) {
	p.testingVerifyMetadataFn = fn
	p.verifyFnCheckedOnce = false
}

// resetForBatch prepares the planner for executing a new batch of
// statements.
func (p *planner) resetForBatch(e *Executor) {
	// Update the systemConfig to a more recent copy, so that we can use tables
	// that we created in previus batches of the same transaction.
	cfg, cache := e.getSystemConfig()
	p.systemConfig = cfg
	p.databaseCache = cache

	p.params = parameters{}

	// The parser cannot be reused between batches.
	p.parser = parser.Parser{}

	p.evalCtx = parser.EvalContext{
		NodeID:   e.nodeID,
		ReCache:  e.reCache,
		Location: p.session.Location,
	}
	p.session.TxnState.schemaChangers.curGroupNum++
}

// blockConfigUpdatesMaybe will ask the Executor to block config updates,
// so that checkTestingVerifyMetadataInitialOrDie() can later be run.
// The point is to lock the system config so that no gossip updates sneak in
// under us, so that we're able to assert that the verify callback only succeeds
// after a gossip update.
//
// It returns an unblock function which can be called after
// checkTestingVerifyMetadata{Initial}OrDie() has been called.
//
// This lock does not change semantics. Even outside of tests, the planner uses
// static systemConfig for a user request, so locking the Executor's
// systemConfig cannot change the semantics of the SQL operation being performed
// under lock.
func (p *planner) blockConfigUpdatesMaybe(e *Executor) func() {
	if !e.ctx.TestingKnobs.WaitForGossipUpdate {
		return func() {}
	}
	return e.blockConfigUpdates()
}

// checkTestingVerifyMetadataInitialOrDie verifies that the metadata callback,
// if one was set, fails. This validates that we need a gossip update for it to
// eventually succeed.
// No-op if we've already done an initial check for the set callback.
// Gossip updates for the system config are assumed to be blocked when this is
// called.
func (p *planner) checkTestingVerifyMetadataInitialOrDie(
	e *Executor, stmts parser.StatementList) {
	if !p.execCtx.TestingKnobs.WaitForGossipUpdate {
		return
	}
	// If there's nothinging to verify, or we've already verified the initial
	// condition, there's nothing to do.
	if p.testingVerifyMetadataFn == nil || p.verifyFnCheckedOnce {
		return
	}
	if p.testingVerifyMetadataFn(e.systemConfig) == nil {
		panic(fmt.Sprintf(
			"expected %q (or the statements before them) to require a "+
				"gossip update, but they did not", stmts))
	}
	p.verifyFnCheckedOnce = true
}

// checkTestingVerifyMetadataOrDie verifies the metadata callback, if one was
// set.
// Gossip updates for the system config are assumed to be blocked when this is
// called.
func (p *planner) checkTestingVerifyMetadataOrDie(
	e *Executor, stmts parser.StatementList) {
	if !p.execCtx.TestingKnobs.WaitForGossipUpdate ||
		p.testingVerifyMetadataFn == nil {
		return
	}
	if !p.verifyFnCheckedOnce {
		panic("intial state of the condition to verify was not checked")
	}

	for p.testingVerifyMetadataFn(e.systemConfig) != nil {
		e.waitForConfigUpdate()
	}
	p.testingVerifyMetadataFn = nil
}

// makePlanner creates a new planner instances, referencing a dummy Session.
// Only use this internally where a Session cannot be created.
func makePlanner() *planner {
	// init with an empty session. We can't leave this nil because too much code
	// looks in the session for the current database.
	return &planner{session: &Session{Location: time.UTC}}
}

func (p *planner) setTxn(txn *client.Txn) {
	p.txn = txn
	if txn != nil {
		p.evalCtx.SetClusterTimestamp(txn.Proto.OrigTimestamp)
	} else {
		p.evalCtx.SetTxnTimestamp(time.Time{})
		p.evalCtx.SetStmtTimestamp(time.Time{})
		p.evalCtx.SetClusterTimestamp(roachpb.ZeroTimestamp)
	}
}

func (p *planner) resetTxn() {
	p.setTxn(nil)
}

// makePlan creates the query plan for a single SQL statement. The returned
// plan needs to be iterated over using planNode.Next() and planNode.Values()
// in order to retrieve matching rows. If autoCommit is true, the plan is
// allowed (but not required) to commit the transaction along with other KV
// operations.
//
// Note: The autoCommit parameter enables operations to enable the 1PC
// optimization. This is a bit hackish/preliminary at present.
func (p *planner) makePlan(stmt parser.Statement, desiredTypes []parser.Datum, autoCommit bool) (planNode, *roachpb.Error) {
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
		pNode, err := p.AlterTable(n)
		return pNode, roachpb.NewError(err)
	case *parser.BeginTransaction:
		pNode, err := p.BeginTransaction(n)
		return pNode, roachpb.NewError(err)
	case *parser.CreateDatabase:
		pNode, err := p.CreateDatabase(n)
		return pNode, roachpb.NewError(err)
	case *parser.CreateIndex:
		pNode, err := p.CreateIndex(n)
		return pNode, roachpb.NewError(err)
	case *parser.CreateTable:
		pNode, err := p.CreateTable(n)
		return pNode, roachpb.NewError(err)
	case *parser.Delete:
		pNode, err := p.Delete(n, desiredTypes, autoCommit)
		return pNode, roachpb.NewError(err)
	case *parser.DropDatabase:
		pNode, err := p.DropDatabase(n)
		return pNode, roachpb.NewError(err)
	case *parser.DropIndex:
		pNode, err := p.DropIndex(n)
		return pNode, roachpb.NewError(err)
	case *parser.DropTable:
		pNode, err := p.DropTable(n)
		return pNode, roachpb.NewError(err)
	case *parser.Explain:
		return p.Explain(n, autoCommit)
	case *parser.Grant:
		pNode, err := p.Grant(n)
		return pNode, roachpb.NewError(err)
	case *parser.Insert:
		return p.Insert(n, desiredTypes, autoCommit)
	case *parser.ParenSelect:
		return p.makePlan(n.Select, desiredTypes, autoCommit)
	case *parser.RenameColumn:
		pNode, err := p.RenameColumn(n)
		return pNode, roachpb.NewError(err)
	case *parser.RenameDatabase:
		pNode, err := p.RenameDatabase(n)
		return pNode, roachpb.NewError(err)
	case *parser.RenameIndex:
		pNode, err := p.RenameIndex(n)
		return pNode, roachpb.NewError(err)
	case *parser.RenameTable:
		pNode, err := p.RenameTable(n)
		return pNode, roachpb.NewError(err)
	case *parser.Revoke:
		pNode, err := p.Revoke(n)
		return pNode, roachpb.NewError(err)
	case *parser.Select:
		pNode, err := p.Select(n, desiredTypes, autoCommit)
		return pNode, roachpb.NewError(err)
	case *parser.SelectClause:
		pNode, err := p.SelectClause(n, desiredTypes)
		return pNode, roachpb.NewError(err)
	case *parser.Set:
		pNode, err := p.Set(n)
		return pNode, roachpb.NewError(err)
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
	case *parser.ShowCreateTable:
		pNode, err := p.ShowCreateTable(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowColumns:
		pNode, err := p.ShowColumns(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowDatabases:
		pNode, err := p.ShowDatabases(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowGrants:
		pNode, err := p.ShowGrants(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowIndex:
		pNode, err := p.ShowIndex(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowTables:
		pNode, err := p.ShowTables(n)
		return pNode, roachpb.NewError(err)
	case *parser.Truncate:
		pNode, err := p.Truncate(n)
		return pNode, roachpb.NewError(err)
	case *parser.UnionClause:
		return p.UnionClause(n, desiredTypes, autoCommit)
	case *parser.Update:
		pNode, err := p.Update(n, desiredTypes, autoCommit)
		return pNode, roachpb.NewError(err)
	case *parser.ValuesClause:
		pNode, err := p.ValuesClause(n, desiredTypes)
		return pNode, roachpb.NewError(err)
	default:
		return nil, roachpb.NewErrorf("unknown statement type: %T", stmt)
	}
}

func (p *planner) prepare(stmt parser.Statement) (planNode, *roachpb.Error) {
	switch n := stmt.(type) {
	case *parser.Delete:
		pNode, err := p.Delete(n, nil, false)
		return pNode, roachpb.NewError(err)
	case *parser.Insert:
		return p.Insert(n, nil, false)
	case *parser.Select:
		pNode, err := p.Select(n, nil, false)
		return pNode, roachpb.NewError(err)
	case *parser.SelectClause:
		pNode, err := p.SelectClause(n, nil)
		return pNode, roachpb.NewError(err)
	case *parser.Show:
		pNode, err := p.Show(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowCreateTable:
		pNode, err := p.ShowCreateTable(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowColumns:
		pNode, err := p.ShowColumns(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowDatabases:
		pNode, err := p.ShowDatabases(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowGrants:
		pNode, err := p.ShowGrants(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowIndex:
		pNode, err := p.ShowIndex(n)
		return pNode, roachpb.NewError(err)
	case *parser.ShowTables:
		pNode, err := p.ShowTables(n)
		return pNode, roachpb.NewError(err)
	case *parser.Update:
		pNode, err := p.Update(n, nil, false)
		return pNode, roachpb.NewError(err)
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
	return p.makePlan(stmt, nil, false)
}

// TODO(andrei): change to return error.
func (p *planner) queryRow(sql string, args ...interface{}) (parser.DTuple, *roachpb.Error) {
	plan, err := p.query(sql, args...)
	if err != nil {
		return nil, err
	}
	if err := plan.Start(); err != nil {
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

// TODO(andrei): change to return error.
func (p *planner) exec(sql string, args ...interface{}) (int, *roachpb.Error) {
	plan, pErr := p.query(sql, args...)
	if pErr != nil {
		return 0, pErr
	}
	if pErr := plan.Start(); pErr != nil {
		return 0, pErr
	}
	return countRowsAffected(plan), plan.PErr()
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
	p.session.TxnState.schemaChangers.queueSchemaChanger(sc)
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

func (p *planner) writeTableDesc(tableDesc *TableDescriptor) error {
	return p.txn.Put(MakeDescMetadataKey(tableDesc.GetID()), wrapDescriptor(tableDesc))
}

// planNode defines the interface for executing a query or portion of a query.
type planNode interface {
	// Columns returns the column names and types. The length of the
	// returned slice is guaranteed to be equal to the length of the
	// tuple returned by Values().
	Columns() []ResultColumn

	// The indexes of the columns the output is ordered by.
	Ordering() orderingInfo

	// Values returns the values at the current row. The result is only valid
	// until the next call to Next().
	Values() parser.DTuple

	// DebugValues returns a set of debug values, valid until the next call to
	// Next(). This is only available for nodes that have been put in a special
	// "explainDebug" mode (using MarkDebug). When the output field in the
	// result is debugValueRow, a set of values is also available through
	// Values().
	DebugValues() debugValues

	// Start begins the query/statement and initializes what needs to be
	// initialized (e.g. final type checking of placeholders, first query
	// plan). Returns an error if initialization fails.
	// The SQL "prepare" phase should merely build the plan node(s),
	// and Start/Next should be only called during "execute".
	Start() *roachpb.Error

	// Next performs one unit of work, returning false if an error is
	// encountered or if there is no more work to do. For statements
	// that return a result set, the Value() method will return one row
	// of results each time that Next() returns true.
	// See executor.go: countRowsAffected() and execStmt() for an example.
	Next() bool

	// PErr returns the error, if any, encountered during iteration.
	// TODO(andrei): change to return error
	PErr() *roachpb.Error

	// ExplainPlan returns a name and description and a list of child nodes.
	ExplainPlan(verbose bool) (name, description string, children []planNode)

	// SetLimitHint tells this node to optimize things under the assumption that
	// we will only need the first `numRows` rows.
	//
	// If soft is true, this is a "soft" limit and is only a hint; the node must
	// still be able to produce all results if requested.
	//
	// If soft is false, this is a "hard" limit and is a promise that Next will
	// never be called more than numRows times.
	SetLimitHint(numRows int64, soft bool)

	// MarkDebug puts the node in a special debugging mode, which allows
	// DebugValues to be used. This should be called after Start() and
	// before the first call to Next() since it may need to recurse into
	// sub-nodes created by Start().
	MarkDebug(mode explainMode)
}

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
var _ planNode = &selectNode{}
var _ planNode = &unionNode{}
var _ planNode = &emptyNode{}
var _ planNode = &explainDebugNode{}
var _ planNode = &explainTraceNode{}

// emptyNode is a planNode with no columns and either no rows (default) or a single row with empty
// results (if results is initialized to true). The former is used for nodes that have no results
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

func (*emptyNode) ExplainPlan(_ bool) (name, description string, children []planNode) {
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

func (e *emptyNode) Start() *roachpb.Error {
	return nil
}

func (e *emptyNode) Next() bool {
	r := e.results
	e.results = false
	return r
}

func (*emptyNode) SetLimitHint(_ int64, _ bool) {}
