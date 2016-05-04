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

func (p *planner) makeEditNode(t parser.TableExpr, r parser.ReturningExprs, desiredTypes []parser.Datum, autoCommit bool, priv privilege.Kind) (editNodeBase, *roachpb.Error) {
	tableDesc, pErr := p.getAliasedTableLease(t)
	if pErr != nil {
		return editNodeBase{}, pErr
	}

	if err := p.checkPrivilege(tableDesc, priv); err != nil {
		return editNodeBase{}, roachpb.NewError(err)
	}

	rh, err := p.makeReturningHelper(r, desiredTypes, tableDesc.Name, tableDesc.Columns)
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
	rows planNode
	// TODO(andrei): change this to error
	pErr      *roachpb.Error
	tw        tableWriter
	resultRow parser.DTuple
	done      bool
}

func (r *editNodeRun) startEditNode(en *editNodeBase, rows planNode, tw tableWriter) error {
	if isSystemConfigID(en.tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		en.p.txn.SetSystemConfigTrigger()
	}

	r.rows = rows
	r.tw = tw
	return r.tw.init(en.p.txn)
}

type updateNode struct {
	// The following fields are populated during makePlan.
	editNodeBase
	defaultExprs []parser.TypedExpr
	n            *parser.Update
	desiredTypes []parser.Datum

	updateCols []ColumnDescriptor
	tw         tableUpdater

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
func (p *planner) Update(n *parser.Update, desiredTypes []parser.Datum, autoCommit bool) (planNode, *roachpb.Error) {
	tracing.AnnotateTrace()

	en, pErr := p.makeEditNode(n.Table, n.Returning, desiredTypes, autoCommit, privilege.UPDATE)
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

	defaultExprs, err := makeDefaultExprs(updateCols, &p.parser, p.evalCtx)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	ru, err := makeRowUpdater(en.tableDesc, updateCols)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	// TODO(dan): Use ru.fetchCols to compute the fetch selectors.
	tw := tableUpdater{ru: ru, autoCommit: autoCommit}

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
	desiredTypesFromSelect := make([]parser.Datum, len(targets), len(targets)+len(exprs))
	for _, expr := range n.Exprs {
		if expr.Tuple {
			if t, ok := expr.Expr.(*parser.Tuple); ok {
				for _, e := range t.Exprs {
					typ := updateCols[i].Type.toDatumType()
					e := fillDefault(e, typ, i, defaultExprs)
					targets = append(targets, parser.SelectExpr{Expr: e})
					desiredTypesFromSelect = append(desiredTypesFromSelect, typ)
					i++
				}
			}
		} else {
			typ := updateCols[i].Type.toDatumType()
			e := fillDefault(expr.Expr, typ, i, defaultExprs)
			targets = append(targets, parser.SelectExpr{Expr: e})
			desiredTypesFromSelect = append(desiredTypesFromSelect, typ)
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
	}, desiredTypesFromSelect)
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
		typedTarget, err := parser.TypeCheck(target, p.evalCtx.Args, updateCols[i].Type.toDatumType())
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		if err := checkColumnType(updateCols[i], typedTarget.ReturnType(), p.evalCtx.Args); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	if pErr := en.rh.TypeCheck(); pErr != nil {
		return nil, pErr
	}

	un := &updateNode{
		n:            n,
		editNodeBase: en,
		desiredTypes: desiredTypesFromSelect,
		defaultExprs: defaultExprs,
		updateCols:   ru.updateCols,
		tw:           tw,
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
					e = fillDefault(e, u.desiredTypes[i], i, u.defaultExprs)
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
			e := fillDefault(expr.Expr, u.desiredTypes[i], i, u.defaultExprs)
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
	}, u.desiredTypes)
	if pErr != nil {
		return pErr
	}

	if pErr := rows.Start(); pErr != nil {
		return pErr
	}

	if err := u.run.startEditNode(&u.editNodeBase, rows, &u.tw); err != nil {
		return roachpb.NewError(err)
	}

	return nil
}

func (u *updateNode) Next() bool {
	if u.run.done || u.run.pErr != nil {
		return false
	}

	if !u.run.rows.Next() {
		// We're done. Finish the batch.
		err := u.tw.finalize()
		u.run.pErr = roachpb.NewError(err)
		u.run.done = true
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
		if err := checkValueWidth(u.updateCols[i], updateValues[i]); err != nil {
			u.run.pErr = roachpb.NewError(err)
			return false
		}
	}

	// Update the row values.
	for i, col := range u.updateCols {
		val := updateValues[i]
		if !col.Nullable && val == parser.DNull {
			u.run.pErr = roachpb.NewUErrorf("null value in column %q violates not-null constraint", col.Name)
			return false
		}
	}

	newValues, err := u.tw.row(append(oldValues, updateValues...))
	if err != nil {
		u.run.pErr = roachpb.NewError(err)
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

func fillDefault(
	expr parser.Expr, desired parser.Datum, index int, defaultExprs []parser.TypedExpr,
) parser.Expr {
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
		for i, col := range u.updateCols {
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
