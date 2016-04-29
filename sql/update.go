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
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// editNode (Base, Run) is shared between all row updating
// statements (DELETE, UPDATE, INSERT).

// editNodeBase holds the common (prepare+execute) state needed to run
// row-modifying statements.
type editNodeBase struct {
	p          *planner
	rh         returningHelper
	tableDesc  *TableDescriptor
	autoCommit bool
}

func (p *planner) makeEditNode(t parser.TableExpr, r parser.ReturningExprs, autoCommit bool, priv privilege.Kind) (editNodeBase, *roachpb.Error) {
	tableDesc, pErr := p.getAliasedTableLease(t)
	if pErr != nil {
		return editNodeBase{}, pErr
	}

	if err := p.checkPrivilege(tableDesc, priv); err != nil {
		return editNodeBase{}, roachpb.NewError(err)
	}

	rh, err := p.makeReturningHelper(r, tableDesc.Name, tableDesc.Columns)
	if err != nil {
		return editNodeBase{}, roachpb.NewError(err)
	}

	return editNodeBase{
		p:          p,
		rh:         rh,
		tableDesc:  tableDesc,
		autoCommit: autoCommit,
	}, nil
}

// editNodeRun holds the runtime (execute) state needed to run
// row-modifying statements.
type editNodeRun struct {
	rows      planNode
	pErr      *roachpb.Error
	b         *client.Batch
	resultRow parser.DTuple
	done      bool
}

func (r *editNodeRun) startEditNode(en *editNodeBase, rows planNode) {
	if isSystemConfigID(en.tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		en.p.txn.SetSystemConfigTrigger()
	}

	r.rows = rows
	r.b = en.p.txn.NewBatch()
}

func (r *editNodeRun) finalize(en *editNodeBase, convertError bool) {
	if en.autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		r.pErr = en.p.txn.CommitInBatch(r.b)
	} else {
		r.pErr = en.p.txn.Run(r.b)
	}
	if r.pErr != nil && convertError {
		// TODO(dan): Move this logic into rowInsert/rowUpdate.
		r.pErr = convertBatchError(en.tableDesc, *r.b, r.pErr)
	}

	r.done = true
	r.b = nil
}

// recordCreatorNode (Base, Run) is shared by row creating statements
// (UPDATE, INSERT).

// rowCreatorNodeBase holds the common (prepare+execute) state needed
// to run statements that create row values.
type rowCreatorNodeBase struct {
	editNodeBase
	defaultExprs []parser.Expr
}

func (p *planner) makeRowCreatorNode(en editNodeBase, cols []ColumnDescriptor) (rowCreatorNodeBase, *roachpb.Error) {
	defaultExprs, err := makeDefaultExprs(cols, &p.parser, p.evalCtx)
	if err != nil {
		return rowCreatorNodeBase{}, roachpb.NewError(err)
	}

	return rowCreatorNodeBase{
		editNodeBase: en,
		defaultExprs: defaultExprs,
	}, nil
}

type updateNode struct {
	// The following fields are populated during makePlan.
	rowCreatorNodeBase
	n *parser.Update

	ru rowUpdater

	run struct {
		// The following fields are populated during Start().
		editNodeRun
	}
}

// Update updates columns for a selection of rows from a table.
// Privileges: UPDATE and SELECT on table. We currently always use a select statement.
//   Notes: postgres requires UPDATE. Requires SELECT with WHERE clause with table.
//          mysql requires UPDATE. Also requires SELECT with WHERE clause with table.
// TODO(guanqun): need to support CHECK in UPDATE
func (p *planner) Update(n *parser.Update, autoCommit bool) (planNode, *roachpb.Error) {
	tracing.AnnotateTrace()

	en, pErr := p.makeEditNode(n.Table, n.Returning, autoCommit, privilege.UPDATE)
	if pErr != nil {
		return nil, pErr
	}

	exprs := make([]parser.UpdateExpr, len(n.Exprs))
	for i, expr := range n.Exprs {
		exprs[i] = *expr
	}

	// Determine which columns we're inserting into.
	var names parser.QualifiedNames
	for _, expr := range n.Exprs {
		// TODO(knz): We need to (attempt to) expand subqueries here already
		// so that it retrieves the column names. But then we need to do
		// it again when the placeholder values are known below.
		newExpr, epErr := p.expandSubqueries(expr.Expr, len(expr.Names))
		if epErr != nil {
			return nil, epErr
		}

		if expr.Tuple {
			n := 0
			switch t := newExpr.(type) {
			case *parser.Tuple:
				n = len(t.Exprs)
			case *parser.DTuple:
				n = len(*t)
			default:
				return nil, roachpb.NewErrorf("unsupported tuple assignment: %T", newExpr)
			}
			if len(expr.Names) != n {
				return nil, roachpb.NewUErrorf("number of columns (%d) does not match number of values (%d)",
					len(expr.Names), n)
			}
		}
		names = append(names, expr.Names...)
	}

	updateCols, err := p.processColumns(en.tableDesc, names)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	rc, rcErr := p.makeRowCreatorNode(en, updateCols)
	if rcErr != nil {
		return nil, rcErr
	}

	un := &updateNode{
		n:                  n,
		rowCreatorNodeBase: rc,
	}

	un.ru, err = makeRowUpdater(en.tableDesc, updateCols)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	// TODO(dan): Use ru.fetchCols to compute the fetch selectors.

	tracing.AnnotateTrace()

	// Generate the list of select targets. We need to select all of the columns
	// plus we select all of the update expressions in case those expressions
	// reference columns (e.g. "UPDATE t SET v = v + 1"). Note that we flatten
	// expressions for tuple assignments just as we flattened the column names
	// above. So "UPDATE t SET (a, b) = (1, 2)" translates into select targets of
	// "*, 1, 2", not "*, (1, 2)".
	// TODO(radu): we only need to select columns necessary to generate primary and
	// secondary indexes keys, and columns needed by returningHelper.
	targets := en.tableDesc.allColumnsSelector()
	i := 0
	// Remember the index where the targets for exprs start.
	exprTargetIdx := len(targets)
	for _, expr := range n.Exprs {
		if expr.Tuple {
			if t, ok := expr.Expr.(*parser.Tuple); ok {
				for _, e := range t.Exprs {
					e = fillDefault(e, i, rc.defaultExprs)
					targets = append(targets, parser.SelectExpr{Expr: e})
					i++
				}
			}
		} else {
			e := fillDefault(expr.Expr, i, rc.defaultExprs)
			targets = append(targets, parser.SelectExpr{Expr: e})
			i++
		}
	}

	// TODO(knz): Until we split the creation of the node from Start()
	// for the SelectClause too, we cannot cache this. This is because
	// this node's initSelect() method both does type checking and also
	// performs index selection. We cannot perform index selection
	// properly until the placeholder values are known.
	rows, pErr := p.SelectClause(&parser.SelectClause{
		Exprs: targets,
		From:  []parser.TableExpr{n.Table},
		Where: n.Where,
	})
	if pErr != nil {
		return nil, pErr
	}

	// ValArgs have their types populated in the above Select if they are part
	// of an expression ("SET a = 2 + $1") in the type check step where those
	// types are inferred. For the simpler case ("SET a = $1"), populate them
	// using checkColumnType. This step also verifies that the expression
	// types match the column types.
	for i, target := range rows.(*selectNode).render[exprTargetIdx:] {
		// DefaultVal doesn't implement TypeCheck
		if _, ok := target.(parser.DefaultVal); ok {
			continue
		}
		d, err := parser.PerformTypeChecking(target, p.evalCtx.Args)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		if err := checkColumnType(updateCols[i], d, p.evalCtx.Args); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	if pErr := en.rh.TypeCheck(); pErr != nil {
		return nil, pErr
	}

	return un, nil
}

func (u *updateNode) Start() *roachpb.Error {
	exprs := make([]parser.UpdateExpr, len(u.n.Exprs))
	for i, expr := range u.n.Exprs {
		exprs[i] = *expr
	}

	// Expand the sub-queries and construct the real list of targets.
	for i, expr := range exprs {
		newExpr, epErr := u.p.expandSubqueries(expr.Expr, len(expr.Names))
		if epErr != nil {
			return epErr
		}
		exprs[i].Expr = newExpr
	}

	// Really generate the list of select targets.
	// TODO(radu): we only need to select columns necessary to generate primary and
	// secondary indexes keys, and columns needed by returningHelper.
	targets := u.tableDesc.allColumnsSelector()
	i := 0
	for _, expr := range exprs {
		if expr.Tuple {
			switch t := expr.Expr.(type) {
			case *parser.Tuple:
				for _, e := range t.Exprs {
					e = fillDefault(e, i, u.defaultExprs)
					targets = append(targets, parser.SelectExpr{Expr: e})
					i++
				}
			case *parser.DTuple:
				for _, e := range *t {
					targets = append(targets, parser.SelectExpr{Expr: e})
					i++
				}
			}
		} else {
			e := fillDefault(expr.Expr, i, u.defaultExprs)
			targets = append(targets, parser.SelectExpr{Expr: e})
			i++
		}
	}

	// Create the workhorse select clause for rows that need updating.
	// TODO(knz): See comment above in Update().
	rows, pErr := u.p.SelectClause(&parser.SelectClause{
		Exprs: targets,
		From:  []parser.TableExpr{u.n.Table},
		Where: u.n.Where,
	})
	if pErr != nil {
		return pErr
	}

	if pErr := rows.Start(); pErr != nil {
		return pErr
	}

	u.run.startEditNode(&u.editNodeBase, rows)

	return nil
}

func (u *updateNode) Next() bool {
	if u.run.done || u.run.pErr != nil {
		return false
	}

	if !u.run.rows.Next() {
		u.run.finalize(&u.editNodeBase, true)
		return false
	}

	tracing.AnnotateTrace()

	oldValues := u.run.rows.Values()

	// Our updated value expressions occur immediately after the plain
	// columns in the output.
	updateValues := oldValues[len(u.tableDesc.Columns):]
	oldValues = oldValues[:len(u.tableDesc.Columns)]

	// Ensure that the values honor the specified column widths.
	for i := range updateValues {
		if err := checkValueWidth(u.ru.updateCols[i], updateValues[i]); err != nil {
			u.run.pErr = roachpb.NewError(err)
			return false
		}
	}

	// Update the row values.
	for i, col := range u.ru.updateCols {
		val := updateValues[i]
		if !col.Nullable && val == parser.DNull {
			u.run.pErr = roachpb.NewUErrorf("null value in column %q violates not-null constraint", col.Name)
			return false
		}
	}

	newValues, pErr := u.ru.updateRow(u.run.b, oldValues, updateValues)
	if pErr != nil {
		u.run.pErr = pErr
		return false
	}

	resultRow, err := u.rh.cookResultRow(newValues)
	if err != nil {
		u.run.pErr = roachpb.NewError(err)
		return false
	}
	u.run.resultRow = resultRow

	return true
}

func fillDefault(expr parser.Expr, index int, defaultExprs []parser.Expr) parser.Expr {
	switch expr.(type) {
	case parser.DefaultVal:
		return defaultExprs[index]
	}
	return expr
}

func (u *updateNode) Columns() []ResultColumn {
	return u.rh.columns
}

func (u *updateNode) Values() parser.DTuple {
	return u.run.resultRow
}

func (u *updateNode) MarkDebug(mode explainMode) {
	u.run.rows.MarkDebug(mode)
}

func (u *updateNode) DebugValues() debugValues {
	return u.run.rows.DebugValues()
}

func (u *updateNode) Ordering() orderingInfo {
	return u.run.rows.Ordering()
}

func (u *updateNode) PErr() *roachpb.Error {
	return u.run.pErr
}

func (u *updateNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	var buf bytes.Buffer
	if v {
		fmt.Fprintf(&buf, "set %s (", u.tableDesc.Name)
		for i, col := range u.ru.updateCols {
			if i > 0 {
				fmt.Fprintf(&buf, ", ")
			}
			fmt.Fprintf(&buf, "%s", col.Name)
		}
		fmt.Fprintf(&buf, ") returning (")
		for i, col := range u.rh.columns {
			if i > 0 {
				fmt.Fprintf(&buf, ", ")
			}
			fmt.Fprintf(&buf, "%s", col.Name)
		}
		fmt.Fprintf(&buf, ")")
	}
	return "update", buf.String(), []planNode{u.run.rows}
}

func (u *updateNode) SetLimitHint(numRows int64, soft bool) {}
