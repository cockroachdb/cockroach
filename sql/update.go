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

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// editNode (Base, Run) is shared between all row updating
// statements (DELETE, UPDATE, INSERT).

// editNodeBase holds the common (prepare+execute) state needed to run
// row-modifying statements.
type editNodeBase struct {
	p          *planner
	rh         returningHelper
	tableDesc  *sqlbase.TableDescriptor
	autoCommit bool
}

func (p *planner) makeEditNode(t parser.TableExpr, r parser.ReturningExprs, desiredTypes []parser.Datum, autoCommit bool, priv privilege.Kind) (editNodeBase, error) {
	tableDesc, err := p.getAliasedTableLease(t)
	if err != nil {
		return editNodeBase{}, err
	}

	if err := p.checkPrivilege(tableDesc, priv); err != nil {
		return editNodeBase{}, err
	}

	rh, err := p.makeReturningHelper(r, desiredTypes, tableDesc.Name, tableDesc.Columns)
	if err != nil {
		return editNodeBase{}, err
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
	err       error
	tw        tableWriter
	resultRow parser.DTuple
	done      bool
}

func (r *editNodeRun) buildEditNodePlan(en *editNodeBase, rows planNode, tw tableWriter) {
	if sqlbase.IsSystemConfigID(en.tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		en.p.txn.SetSystemConfigTrigger()
	}

	r.rows = rows
	r.tw = tw
}

type updateNode struct {
	// The following fields are populated during makePlan.
	editNodeBase
	defaultExprs []parser.TypedExpr
	n            *parser.Update
	desiredTypes []parser.Datum

	updateCols    []sqlbase.ColumnDescriptor
	updateColsIdx map[sqlbase.ColumnID]int // index in updateCols slice
	tw            tableUpdater
	checkHelper   checkHelper

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
func (p *planner) Update(n *parser.Update, desiredTypes []parser.Datum, autoCommit bool) (planNode, error) {
	tracing.AnnotateTrace()

	en, err := p.makeEditNode(n.Table, n.Returning, desiredTypes, autoCommit, privilege.UPDATE)
	if err != nil {
		return nil, err
	}

	exprs := make([]parser.UpdateExpr, len(n.Exprs))
	for i, expr := range n.Exprs {
		exprs[i] = *expr
	}

	// Determine which columns we're inserting into.
	names, err := p.namesForExprs(n.Exprs)
	if err != nil {
		return nil, err
	}

	updateCols, err := p.processColumns(en.tableDesc, names)
	if err != nil {
		return nil, err
	}

	defaultExprs, err := makeDefaultExprs(updateCols, &p.parser, p.evalCtx)
	if err != nil {
		return nil, err
	}

	var requestedCols []sqlbase.ColumnDescriptor
	if len(en.rh.exprs) > 0 || len(en.tableDesc.Checks) > 0 {
		// TODO(dan): This could be made tighter, just the rows needed for RETURNING
		// exprs.
		requestedCols = en.tableDesc.Columns
	}

	ru, err := makeRowUpdater(en.tableDesc, updateCols, requestedCols)
	if err != nil {
		return nil, err
	}
	tw := tableUpdater{ru: ru, autoCommit: autoCommit}

	tracing.AnnotateTrace()

	// Generate the list of select targets. We need to select all of the columns
	// plus we select all of the update expressions in case those expressions
	// reference columns (e.g. "UPDATE t SET v = v + 1"). Note that we flatten
	// expressions for tuple assignments just as we flattened the column names
	// above. So "UPDATE t SET (a, b) = (1, 2)" translates into select targets of
	// "*, 1, 2", not "*, (1, 2)".
	targets := sqlbase.ColumnsSelectors(ru.fetchCols)
	i := 0
	// Remember the index where the targets for exprs start.
	exprTargetIdx := len(targets)
	desiredTypesFromSelect := make([]parser.Datum, len(targets), len(targets)+len(exprs))
	for _, expr := range n.Exprs {
		if expr.Tuple {
			if t, ok := expr.Expr.(*parser.Tuple); ok {
				for _, e := range t.Exprs {
					typ := updateCols[i].Type.ToDatumType()
					e := fillDefault(e, typ, i, defaultExprs)
					targets = append(targets, parser.SelectExpr{Expr: e})
					desiredTypesFromSelect = append(desiredTypesFromSelect, typ)
					i++
				}
			}
		} else {
			typ := updateCols[i].Type.ToDatumType()
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
	rows, err := p.SelectClause(&parser.SelectClause{
		Exprs: targets,
		From:  []parser.TableExpr{n.Table},
		Where: n.Where,
	}, nil, nil, desiredTypesFromSelect)
	if err != nil {
		return nil, err
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
		typedTarget, err := parser.TypeCheck(target, p.evalCtx.Args, updateCols[i].Type.ToDatumType())
		if err != nil {
			return nil, err
		}
		err = sqlbase.CheckColumnType(updateCols[i], typedTarget.ReturnType(), p.evalCtx.Args)
		if err != nil {
			return nil, err
		}
	}

	if err := en.rh.TypeCheck(); err != nil {
		return nil, err
	}

	updateColsIdx := make(map[sqlbase.ColumnID]int, len(ru.updateCols))
	for i, col := range ru.updateCols {
		updateColsIdx[col.ID] = i
	}

	un := &updateNode{
		n:             n,
		editNodeBase:  en,
		desiredTypes:  desiredTypesFromSelect,
		defaultExprs:  defaultExprs,
		updateCols:    ru.updateCols,
		updateColsIdx: updateColsIdx,
		tw:            tw,
	}
	if err := un.checkHelper.init(p, en.tableDesc); err != nil {
		return nil, err
	}
	return un, nil
}

func (u *updateNode) expandPlan() error {
	exprs := make([]parser.UpdateExpr, len(u.n.Exprs))
	for i, expr := range u.n.Exprs {
		exprs[i] = *expr
	}

	// Expand the sub-queries and construct the real list of targets.
	for i, expr := range exprs {
		newExpr, eErr := u.p.expandSubqueries(expr.Expr, len(expr.Names))
		if eErr != nil {
			return eErr
		}
		exprs[i].Expr = newExpr
	}

	targets := sqlbase.ColumnsSelectors(u.tw.ru.fetchCols)
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
	rows, err := u.p.SelectClause(&parser.SelectClause{
		Exprs: targets,
		From:  []parser.TableExpr{u.n.Table},
		Where: u.n.Where,
	}, nil, nil, u.desiredTypes)
	if err != nil {
		return err
	}

	if err := rows.expandPlan(); err != nil {
		return err
	}

	u.run.buildEditNodePlan(&u.editNodeBase, rows, &u.tw)
	return nil
}

func (u *updateNode) Start() error {
	if err := u.run.rows.Start(); err != nil {
		return err
	}

	return u.run.tw.init(u.p.txn)
}

func (u *updateNode) Next() bool {
	if u.run.done || u.run.err != nil {
		return false
	}

	if !u.run.rows.Next() {
		// We're done. Finish the batch.
		err := u.tw.finalize()
		u.run.err = err
		u.run.done = true
		return false
	}

	tracing.AnnotateTrace()

	oldValues := u.run.rows.Values()

	// Our updated value expressions occur immediately after the plain
	// columns in the output.
	updateValues := oldValues[len(u.tw.ru.fetchCols):]
	oldValues = oldValues[:len(u.tw.ru.fetchCols)]

	u.checkHelper.loadRow(u.tw.ru.fetchColIDtoRowIndex, oldValues, false)
	u.checkHelper.loadRow(u.updateColsIdx, updateValues, true)
	if err := u.checkHelper.check(u.p.evalCtx); err != nil {
		u.run.err = err
		return false
	}

	// Ensure that the values honor the specified column widths.
	for i := range updateValues {
		if err := sqlbase.CheckValueWidth(u.updateCols[i], updateValues[i]); err != nil {
			u.run.err = err
			return false
		}
	}

	// Update the row values.
	for i, col := range u.updateCols {
		val := updateValues[i]
		if !col.Nullable && val == parser.DNull {
			u.run.err = newNonNullViolationError(col.Name)
			return false
		}
	}

	newValues, err := u.tw.row(append(oldValues, updateValues...))
	if err != nil {
		u.run.err = err
		return false
	}

	resultRow, err := u.rh.cookResultRow(newValues)
	if err != nil {
		u.run.err = err
		return false
	}
	u.run.resultRow = resultRow

	return true
}

// namesForExprs expands names in the tuples and subqueries in exprs.
func (p *planner) namesForExprs(exprs parser.UpdateExprs) (parser.QualifiedNames, error) {
	var names parser.QualifiedNames
	for _, expr := range exprs {
		// TODO(knz): We need to (attempt to) expand subqueries here already
		// so that it retrieves the column names. But then we need to do
		// it again when the placeholder values are known below.
		newExpr, eErr := p.expandSubqueries(expr.Expr, len(expr.Names))
		if eErr != nil {
			return nil, eErr
		}

		if expr.Tuple {
			n := 0
			switch t := newExpr.(type) {
			case *parser.Tuple:
				n = len(t.Exprs)
			case *parser.DTuple:
				n = len(*t)
			default:
				return nil, util.Errorf("unsupported tuple assignment: %T", newExpr)
			}
			if len(expr.Names) != n {
				return nil, fmt.Errorf("number of columns (%d) does not match number of values (%d)",
					len(expr.Names), n)
			}
		}
		names = append(names, expr.Names...)
	}
	return names, nil
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

func (u *updateNode) Err() error {
	return u.run.err
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

func (u *updateNode) ExplainTypes(regTypes func(string, string)) {
	cols := u.rh.columns
	for i, rexpr := range u.rh.exprs {
		regTypes(fmt.Sprintf("returning %s", cols[i].Name), parser.AsStringWithFlags(rexpr, parser.FmtShowTypes))
	}
	for i, dexpr := range u.defaultExprs {
		regTypes(fmt.Sprintf("default %d", i), parser.AsStringWithFlags(dexpr, parser.FmtShowTypes))
	}
}

func (u *updateNode) SetLimitHint(numRows int64, soft bool) {}
