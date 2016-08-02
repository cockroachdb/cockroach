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
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/pkg/errors"
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

func (p *planner) makeEditNode(t parser.TableExpr, autoCommit bool, priv privilege.Kind) (editNodeBase, error) {
	tableDesc, err := p.getAliasedTableLease(t)
	if err != nil {
		return editNodeBase{}, err
	}

	if err := p.checkPrivilege(tableDesc, priv); err != nil {
		return editNodeBase{}, err
	}

	return editNodeBase{
		p:          p,
		tableDesc:  tableDesc,
		autoCommit: autoCommit,
	}, nil
}

// editNodeRun holds the runtime (execute) state needed to run
// row-modifying statements.
type editNodeRun struct {
	rows      planNode
	tw        tableWriter
	resultRow parser.DTuple

	explain explainMode
}

func (r *editNodeRun) initEditNode(en *editNodeBase, rows planNode, re parser.ReturningExprs, desiredTypes []parser.Datum) error {
	r.rows = rows

	rh, err := en.p.makeReturningHelper(re, desiredTypes, en.tableDesc.Name, en.tableDesc.Columns)
	if err != nil {
		return err
	}
	en.rh = rh

	return nil
}

func (r *editNodeRun) expandEditNodePlan(en *editNodeBase, tw tableWriter) error {
	if err := en.rh.expandPlans(); err != nil {
		return err
	}

	if sqlbase.IsSystemConfigID(en.tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		en.p.txn.SetSystemConfigTrigger()
	}

	if err := tw.expand(); err != nil {
		return err
	}

	r.tw = tw
	return r.rows.expandPlan()
}

func (r *editNodeRun) startEditNode() error {
	if err := r.tw.start(); err != nil {
		return err
	}

	return r.rows.Start()
}

type updateNode struct {
	// The following fields are populated during makePlan.
	editNodeBase
	n             *parser.Update
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

	en, err := p.makeEditNode(n.Table, autoCommit, privilege.UPDATE)
	if err != nil {
		return nil, err
	}

	exprs := make([]*parser.UpdateExpr, len(n.Exprs))
	for i, expr := range n.Exprs {
		// Replace the sub-query nodes.
		newExpr, err := p.replaceSubqueries(expr.Expr, len(expr.Names))
		if err != nil {
			return nil, err
		}
		exprs[i] = &parser.UpdateExpr{Tuple: expr.Tuple, Expr: newExpr, Names: expr.Names}
	}

	// Determine which columns we're inserting into.
	names, err := p.namesForExprs(exprs)
	if err != nil {
		return nil, err
	}

	updateCols, err := p.processColumns(en.tableDesc, names)
	if err != nil {
		return nil, err
	}

	defaultExprs, err := makeDefaultExprs(updateCols, &p.parser, &p.evalCtx)
	if err != nil {
		return nil, err
	}

	var requestedCols []sqlbase.ColumnDescriptor
	if len(n.Returning) > 0 || len(en.tableDesc.Checks) > 0 {
		// TODO(dan): This could be made tighter, just the rows needed for RETURNING
		// exprs.
		requestedCols = en.tableDesc.Columns
	}

	fkTables := TablesNeededForFKs(*en.tableDesc, CheckUpdates)
	if err := p.fillFKTableMap(fkTables); err != nil {
		return nil, err
	}
	ru, err := makeRowUpdater(p.txn, en.tableDesc, fkTables, updateCols, requestedCols, rowUpdaterDefault)
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
	for _, expr := range exprs {
		if expr.Tuple {
			switch t := expr.Expr.(type) {
			case (*parser.Tuple):
				for _, e := range t.Exprs {
					typ := updateCols[i].Type.ToDatumType()
					e := fillDefault(e, typ, i, defaultExprs)
					targets = append(targets, parser.SelectExpr{Expr: e})
					desiredTypesFromSelect = append(desiredTypesFromSelect, typ)
					i++
				}
			default:
				return nil, fmt.Errorf("cannot use this expression to assign multiple columns: %s", expr.Expr)
			}
		} else {
			typ := updateCols[i].Type.ToDatumType()
			e := fillDefault(expr.Expr, typ, i, defaultExprs)
			targets = append(targets, parser.SelectExpr{Expr: e})
			desiredTypesFromSelect = append(desiredTypesFromSelect, typ)
			i++
		}
	}

	rows, err := p.SelectClause(&parser.SelectClause{
		Exprs: targets,
		From:  &parser.From{Tables: []parser.TableExpr{n.Table}},
		Where: n.Where,
	}, nil, nil, desiredTypesFromSelect, publicAndNonPublicColumns)
	if err != nil {
		return nil, err
	}

	// Placeholders have their types populated in the above Select if they are part
	// of an expression ("SET a = 2 + $1") in the type check step where those
	// types are inferred. For the simpler case ("SET a = $1"), populate them
	// using checkColumnType. This step also verifies that the expression
	// types match the column types.
	sel := rows.(*selectTopNode).source.(*selectNode)
	for i, target := range sel.render[exprTargetIdx:] {
		// DefaultVal doesn't implement TypeCheck
		if _, ok := target.(parser.DefaultVal); ok {
			continue
		}
		// TODO(nvanbenschoten) isn't this TypeCheck redundant with the call to SelectClause?
		typedTarget, err := parser.TypeCheck(target, &p.semaCtx, updateCols[i].Type.ToDatumType())
		if err != nil {
			return nil, err
		}
		err = sqlbase.CheckColumnType(updateCols[i], typedTarget.ReturnType(), p.semaCtx.Placeholders)
		if err != nil {
			return nil, err
		}
	}

	updateColsIdx := make(map[sqlbase.ColumnID]int, len(ru.updateCols))
	for i, col := range ru.updateCols {
		updateColsIdx[col.ID] = i
	}

	un := &updateNode{
		n:             n,
		editNodeBase:  en,
		updateCols:    ru.updateCols,
		updateColsIdx: updateColsIdx,
		tw:            tw,
	}
	if err := un.checkHelper.init(p, en.tableDesc); err != nil {
		return nil, err
	}
	if err := un.run.initEditNode(&un.editNodeBase, rows, n.Returning, desiredTypes); err != nil {
		return nil, err
	}
	return un, nil
}

func (u *updateNode) expandPlan() error {
	return u.run.expandEditNodePlan(&u.editNodeBase, &u.tw)
}

func (u *updateNode) Start() error {
	if err := u.rh.startPlans(); err != nil {
		return err
	}

	if err := u.run.startEditNode(); err != nil {
		return err
	}

	return u.run.tw.init(u.p.txn)
}

func (u *updateNode) Next() (bool, error) {
	next, err := u.run.rows.Next()
	if !next {
		if err == nil {
			// We're done. Finish the batch.
			err = u.tw.finalize(u.p.ctx())
		}
		return false, err
	}

	if u.run.explain == explainDebug {
		return true, nil
	}

	tracing.AnnotateTrace()

	oldValues := u.run.rows.Values()

	// Our updated value expressions occur immediately after the plain
	// columns in the output.
	updateValues := oldValues[len(u.tw.ru.fetchCols):]
	oldValues = oldValues[:len(u.tw.ru.fetchCols)]

	u.checkHelper.loadRow(u.tw.ru.fetchColIDtoRowIndex, oldValues, false)
	u.checkHelper.loadRow(u.updateColsIdx, updateValues, true)
	if err := u.checkHelper.check(&u.p.evalCtx); err != nil {
		return false, err
	}

	// Ensure that the values honor the specified column widths.
	for i := range updateValues {
		if err := sqlbase.CheckValueWidth(u.tw.ru.updateCols[i], updateValues[i]); err != nil {
			return false, err
		}
	}

	// Update the row values.
	for i, col := range u.tw.ru.updateCols {
		val := updateValues[i]
		if !col.Nullable && val == parser.DNull {
			return false, sqlbase.NewNonNullViolationError(col.Name)
		}
	}

	newValues, err := u.tw.row(u.p.ctx(), append(oldValues, updateValues...))
	if err != nil {
		return false, err
	}

	resultRow, err := u.rh.cookResultRow(newValues)
	if err != nil {
		return false, err
	}
	u.run.resultRow = resultRow

	return true, nil
}

// namesForExprs expands names in the tuples and subqueries in exprs.
func (p *planner) namesForExprs(exprs parser.UpdateExprs) (parser.QualifiedNames, error) {
	var names parser.QualifiedNames
	for _, expr := range exprs {
		newExpr := expr.Expr

		if expr.Tuple {
			n := 0
			if s, ok := newExpr.(*subquery); ok {
				newExpr = s.typ
			}
			switch t := newExpr.(type) {
			case *parser.Tuple:
				n = len(t.Exprs)
			case *parser.DTuple:
				n = len(*t)
			default:
				return nil, errors.Errorf("unsupported tuple assignment: %T", newExpr)
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
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	u.run.explain = mode
	u.run.rows.MarkDebug(mode)
}

func (u *updateNode) DebugValues() debugValues {
	return u.run.rows.DebugValues()
}

func (u *updateNode) Ordering() orderingInfo {
	return u.run.rows.Ordering()
}

func (u *updateNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	var buf bytes.Buffer
	if v {
		fmt.Fprintf(&buf, "set %s (", u.tableDesc.Name)
		for i, col := range u.tw.ru.updateCols {
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

	subplans := []planNode{u.run.rows}
	for _, e := range u.rh.exprs {
		subplans = u.p.collectSubqueryPlans(e, subplans)
	}

	return "update", buf.String(), subplans
}

func (u *updateNode) ExplainTypes(regTypes func(string, string)) {
	cols := u.rh.columns
	for i, rexpr := range u.rh.exprs {
		regTypes(fmt.Sprintf("returning %s", cols[i].Name), parser.AsStringWithFlags(rexpr, parser.FmtShowTypes))
	}
}

func (u *updateNode) SetLimitHint(numRows int64, soft bool) {}
