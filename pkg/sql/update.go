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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// editNode (Base, Run) is shared between all row updating
// statements (DELETE, UPDATE, INSERT).

// editNodeBase holds the common (prepare+execute) state needed to run
// row-modifying statements.
type editNodeBase struct {
	p          *planner
	rh         *returningHelper
	tableDesc  *sqlbase.TableDescriptor
	autoCommit bool
}

func (p *planner) makeEditNode(
	ctx context.Context, tn *parser.TableName, autoCommit bool, priv privilege.Kind,
) (editNodeBase, error) {
	tableDesc, err := p.session.leases.getTableLease(ctx, p.txn, p.getVirtualTabler(), tn)
	if err != nil {
		return editNodeBase{}, err
	}
	// We don't support update on views, only real tables.
	if !tableDesc.IsTable() {
		return editNodeBase{},
			errors.Errorf("cannot run %s on view %q - views are not updateable", priv, tn)
	}

	if err := p.CheckPrivilege(tableDesc, priv); err != nil {
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
	resultRow parser.Datums

	explain explainMode
}

func (r *editNodeRun) initEditNode(
	ctx context.Context,
	en *editNodeBase,
	rows planNode,
	tw tableWriter,
	re parser.ReturningClause,
	desiredTypes []parser.Type,
) error {
	r.rows = rows
	r.tw = tw

	rh, err := en.p.newReturningHelper(ctx, re, desiredTypes, en.tableDesc.Name, en.tableDesc.Columns)
	if err != nil {
		return err
	}
	en.rh = rh

	return nil
}

func (r *editNodeRun) startEditNode(ctx context.Context, en *editNodeBase) error {
	if sqlbase.IsSystemConfigID(en.tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		if err := en.p.txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
	}

	return r.rows.Start(ctx)
}

func (r *editNodeRun) collectSpans(ctx context.Context) (reads, writes roachpb.Spans, err error) {
	scanReads, scanWrites, err := r.rows.Spans(ctx)
	if err != nil {
		return nil, nil, err
	}
	if len(scanWrites) > 0 {
		return nil, nil, errors.Errorf("unexpected scan span writes: %v", scanWrites)
	}
	// TODO(nvanbenschoten) if we notice that r.rows is a ValuesClause, we
	// may be able to contrain the tableWriter Spans.
	writerReads, writerWrites, err := r.tw.spans()
	if err != nil {
		return nil, nil, err
	}
	sqReads, err := collectSubquerySpans(ctx, r.rows)
	if err != nil {
		return nil, nil, err
	}
	return append(scanReads, append(writerReads, sqReads...)...), writerWrites, nil
}

type updateNode struct {
	// The following fields are populated during makePlan.
	editNodeBase
	n             *parser.Update
	updateCols    []sqlbase.ColumnDescriptor
	updateColsIdx map[sqlbase.ColumnID]int // index in updateCols slice
	tw            tableUpdater
	checkHelper   checkHelper
	sourceSlots   []sourceSlot

	run struct {
		// The following fields are populated during Start().
		editNodeRun
	}
}

// sourceSlot abstracts the idea that our update sources can either be tuples
// or scalars. Tuples are for cases such as SET (a, b) = (1, 2) or SET (a, b) =
// (SELECT 1, 2), and scalars are for situations like SET a = b. A sourceSlot
// represents how to extract and type-check the results of the right-hand side
// of a single SET statement. We could treat everything as tuples, including
// scalars as tuples of size 1, and eliminate this indirection, but that makes
// the query plan more complex.
type sourceSlot interface {
	// extractValues returns a slice of the values this slot is responsible for,
	// as extracted from the row of results.
	extractValues(resultRow parser.Datums) parser.Datums
	// checkColumnTypes compares the types of the results that this slot refers to to the types of
	// the columns those values will be assigned to. It returns an error if those types don't match up.
	// It also populates the types of any placeholders by way of calling into sqlbase.CheckColumnType.
	checkColumnTypes(row []parser.TypedExpr, pmap *parser.PlaceholderInfo) error
}

type tupleSlot struct {
	columns     []sqlbase.ColumnDescriptor
	sourceIndex int
}

func (ts tupleSlot) extractValues(row parser.Datums) parser.Datums {
	return row[ts.sourceIndex].(*parser.DTuple).D
}

func (ts tupleSlot) checkColumnTypes(row []parser.TypedExpr, pmap *parser.PlaceholderInfo) error {
	renderedResult := row[ts.sourceIndex]
	for i, typ := range renderedResult.ResolvedType().(parser.TTuple) {
		if err := sqlbase.CheckColumnType(ts.columns[i], typ, pmap); err != nil {
			return err
		}
	}
	return nil
}

type scalarSlot struct {
	column      sqlbase.ColumnDescriptor
	sourceIndex int
}

func (ss scalarSlot) extractValues(row parser.Datums) parser.Datums {
	return row[ss.sourceIndex : ss.sourceIndex+1]
}

func (ss scalarSlot) checkColumnTypes(row []parser.TypedExpr, pmap *parser.PlaceholderInfo) error {
	renderedResult := row[ss.sourceIndex]
	typ := renderedResult.ResolvedType()
	return sqlbase.CheckColumnType(ss.column, typ, pmap)
}

// addOrMergeExpr inserts an Expr into a renderNode, attempting to reuse
// previous renders if possible by using render.addOrMergeRender, returning the
// column index at which the rendered value can be accessed.
func addOrMergeExpr(
	ctx context.Context,
	e parser.Expr,
	currentUpdateIdx int,
	updateCols []sqlbase.ColumnDescriptor,
	defaultExprs []parser.TypedExpr,
	render *renderNode,
) (colIdx int, err error) {
	e = fillDefault(e, currentUpdateIdx, defaultExprs)
	selectExpr := parser.SelectExpr{Expr: e}
	typ := updateCols[currentUpdateIdx].Type.ToDatumType()
	col, expr, err := render.planner.computeRender(ctx, selectExpr, typ,
		render.sourceInfo, render.ivarHelper)
	if err != nil {
		return -1, err
	}

	return render.addOrMergeRender(col, expr, true), nil
}

// Update updates columns for a selection of rows from a table.
// Privileges: UPDATE and SELECT on table. We currently always use a select statement.
//   Notes: postgres requires UPDATE. Requires SELECT with WHERE clause with table.
//          mysql requires UPDATE. Also requires SELECT with WHERE clause with table.
// TODO(guanqun): need to support CHECK in UPDATE
func (p *planner) Update(
	ctx context.Context, n *parser.Update, desiredTypes []parser.Type, autoCommit bool,
) (planNode, error) {
	tracing.AnnotateTrace()

	tn, err := p.getAliasedTableName(n.Table)
	if err != nil {
		return nil, err
	}

	en, err := p.makeEditNode(ctx, tn, autoCommit, privilege.UPDATE)
	if err != nil {
		return nil, err
	}

	setExprs := make([]*parser.UpdateExpr, len(n.Exprs))
	for i, expr := range n.Exprs {
		// Replace the sub-query nodes.
		newExpr, err := p.replaceSubqueries(ctx, expr.Expr, len(expr.Names))
		if err != nil {
			return nil, err
		}
		setExprs[i] = &parser.UpdateExpr{Tuple: expr.Tuple, Expr: newExpr, Names: expr.Names}
	}

	// Determine which columns we're inserting into.
	names, err := p.namesForExprs(setExprs)
	if err != nil {
		return nil, err
	}

	updateCols, err := p.processColumns(en.tableDesc, names)
	if err != nil {
		return nil, err
	}

	defaultExprs, err := sqlbase.MakeDefaultExprs(updateCols, &p.parser, &p.evalCtx)
	if err != nil {
		return nil, err
	}

	var requestedCols []sqlbase.ColumnDescriptor
	if _, retExprs := n.Returning.(*parser.ReturningExprs); retExprs || len(en.tableDesc.Checks) > 0 {
		// TODO(dan): This could be made tighter, just the rows needed for RETURNING
		// exprs.
		requestedCols = en.tableDesc.Columns
	}

	fkTables := sqlbase.TablesNeededForFKs(*en.tableDesc, sqlbase.CheckUpdates)
	if err := p.fillFKTableMap(ctx, fkTables); err != nil {
		return nil, err
	}
	ru, err := sqlbase.MakeRowUpdater(p.txn, en.tableDesc, fkTables, updateCols, requestedCols, sqlbase.RowUpdaterDefault)
	if err != nil {
		return nil, err
	}
	tw := tableUpdater{ru: ru, autoCommit: autoCommit}

	tracing.AnnotateTrace()

	// We construct a query containing the columns being updated, and then later merge the values
	// they are being updated with into that renderNode to ideally reuse some of the queries.
	rows, err := p.SelectClause(ctx, &parser.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(ru.FetchCols),
		From:  &parser.From{Tables: []parser.TableExpr{n.Table}},
		Where: n.Where,
	}, nil, nil, nil, publicAndNonPublicColumns)
	if err != nil {
		return nil, err
	}

	// This capacity is an overestimate, since subqueries only take up one sourceSlot.
	sourceSlots := make([]sourceSlot, 0, len(ru.FetchCols))

	// currentUpdateIdx is the index of the first column descriptor in updateCols
	// that is assigned to by the current setExpr.
	currentUpdateIdx := 0
	render := rows.(*renderNode)

	for _, setExpr := range setExprs {
		if setExpr.Tuple {
			switch t := setExpr.Expr.(type) {
			case *parser.Tuple:
				// The user assigned an explicit set of values to the columns. We can't
				// treat this case the same as when we have a subquery (and just evaluate
				// the tuple) because when assigning a literal tuple like this it's valid
				// to assign DEFAULT to some of the columns, which is not valid generally.
				for _, e := range t.Exprs {
					colIdx, err := addOrMergeExpr(ctx, e, currentUpdateIdx, updateCols, defaultExprs, render)
					if err != nil {
						return nil, err
					}

					sourceSlots = append(sourceSlots, scalarSlot{
						column:      updateCols[currentUpdateIdx],
						sourceIndex: colIdx,
					})

					currentUpdateIdx++
				}
			case *subquery:
				selectExpr := parser.SelectExpr{Expr: t}
				desiredTupleType := make(parser.TTuple, len(setExpr.Names))
				for i := range setExpr.Names {
					desiredTupleType[i] = updateCols[currentUpdateIdx+i].Type.ToDatumType()
				}
				col, expr, err := render.planner.computeRender(ctx, selectExpr, desiredTupleType,
					render.sourceInfo, render.ivarHelper)
				if err != nil {
					return nil, err
				}

				colIdx := render.addOrMergeRender(col, expr, false)

				sourceSlots = append(sourceSlots, tupleSlot{
					columns:     updateCols[currentUpdateIdx : currentUpdateIdx+len(setExpr.Names)],
					sourceIndex: colIdx,
				})
				currentUpdateIdx += len(setExpr.Names)
			default:
				panic(fmt.Sprintf("assigning to tuple with expression that is neither a tuple nor a subquery: %s", setExpr.Expr))
			}

		} else {
			colIdx, err := addOrMergeExpr(ctx, setExpr.Expr, currentUpdateIdx, updateCols, defaultExprs, render)
			if err != nil {
				return nil, err
			}

			sourceSlots = append(sourceSlots, scalarSlot{
				column:      updateCols[currentUpdateIdx],
				sourceIndex: colIdx,
			})
			currentUpdateIdx++
		}
	}

	// Placeholders have their types populated in the above Select if they are part
	// of an expression ("SET a = 2 + $1") in the type check step where those
	// types are inferred. For the simpler case ("SET a = $1"), populate them
	// using checkColumnType. This step also verifies that the expression
	// types match the column types.
	sel := rows.(*renderNode)
	for _, sourceSlot := range sourceSlots {
		if err := sourceSlot.checkColumnTypes(sel.render, &p.semaCtx.Placeholders); err != nil {
			return nil, err
		}
	}

	updateColsIdx := make(map[sqlbase.ColumnID]int, len(ru.UpdateCols))
	for i, col := range ru.UpdateCols {
		updateColsIdx[col.ID] = i
	}

	un := &updateNode{
		n:             n,
		editNodeBase:  en,
		updateCols:    ru.UpdateCols,
		updateColsIdx: updateColsIdx,
		tw:            tw,
		sourceSlots:   sourceSlots,
	}
	if err := un.checkHelper.init(ctx, p, tn, en.tableDesc); err != nil {
		return nil, err
	}
	if err := un.run.initEditNode(
		ctx, &un.editNodeBase, rows, &un.tw, n.Returning, desiredTypes); err != nil {
		return nil, err
	}
	return un, nil
}

func (u *updateNode) Start(ctx context.Context) error {
	if err := u.run.startEditNode(ctx, &u.editNodeBase); err != nil {
		return err
	}
	return u.run.tw.init(u.p.txn)
}

func (u *updateNode) Close(ctx context.Context) {
	u.run.rows.Close(ctx)
}

func (u *updateNode) Next(ctx context.Context) (bool, error) {
	next, err := u.run.rows.Next(ctx)
	if !next {
		if err == nil {
			// We're done. Finish the batch.
			err = u.tw.finalize(ctx)
		}
		return false, err
	}

	if u.run.explain == explainDebug {
		return true, nil
	}

	tracing.AnnotateTrace()

	entireRow := u.run.rows.Values()

	// Our updated value expressions occur immediately after the plain
	// columns in the output.
	oldValues := entireRow[:len(u.tw.ru.FetchCols)]

	updateValues := make(parser.Datums, len(u.tw.ru.UpdateCols))
	valueIdx := 0

	for _, slot := range u.sourceSlots {
		for _, value := range slot.extractValues(entireRow) {
			updateValues[valueIdx] = value
			valueIdx++
		}
	}

	if err := u.checkHelper.loadRow(u.tw.ru.FetchColIDtoRowIndex, oldValues, false); err != nil {
		return false, err
	}
	if err := u.checkHelper.loadRow(u.updateColsIdx, updateValues, true); err != nil {
		return false, err
	}
	if err := u.checkHelper.check(&u.p.evalCtx); err != nil {
		return false, err
	}

	// Ensure that the values honor the specified column widths.
	for i := range updateValues {
		if err := sqlbase.CheckValueWidth(u.tw.ru.UpdateCols[i], updateValues[i]); err != nil {
			return false, err
		}
	}

	for i, col := range u.tw.ru.UpdateCols {
		val := updateValues[i]
		if !col.Nullable && val == parser.DNull {
			return false, sqlbase.NewNonNullViolationError(col.Name)
		}
	}

	// Update the row values.
	newValues, err := u.tw.row(ctx, append(oldValues, updateValues...))
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
func (p *planner) namesForExprs(exprs parser.UpdateExprs) (parser.UnresolvedNames, error) {
	var names parser.UnresolvedNames
	for _, expr := range exprs {
		if expr.Tuple {
			n := -1
			switch t := expr.Expr.(type) {
			case *subquery:
				if tup, ok := t.typ.(parser.TTuple); ok {
					n = len(tup)
				}
			case *parser.Tuple:
				n = len(t.Exprs)
			case *parser.DTuple:
				n = len(t.D)
			}
			if n < 0 {
				return nil, errors.Errorf("unsupported tuple assignment: %T", expr.Expr)
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

func fillDefault(expr parser.Expr, index int, defaultExprs []parser.TypedExpr) parser.Expr {
	switch expr.(type) {
	case parser.DefaultVal:
		if defaultExprs == nil {
			return parser.DNull
		}
		return defaultExprs[index]
	}
	return expr
}

func (u *updateNode) Columns() ResultColumns {
	return u.rh.columns
}

func (u *updateNode) Values() parser.Datums {
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

func (u *updateNode) Ordering() orderingInfo { return orderingInfo{} }

func (u *updateNode) Spans(ctx context.Context) (reads, writes roachpb.Spans, err error) {
	return u.run.collectSpans(ctx)
}
