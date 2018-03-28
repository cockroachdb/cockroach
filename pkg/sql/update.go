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

package sql

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

var updateNodePool = sync.Pool{
	New: func() interface{} {
		return &updateNode{}
	},
}

type updateNode struct {
	// The following fields are populated during makePlan.
	editNodeBase
	n             *tree.Update
	updateCols    []sqlbase.ColumnDescriptor
	computedCols  []sqlbase.ColumnDescriptor
	updateColsIdx map[sqlbase.ColumnID]int // index in updateCols slice
	computeExprs  []tree.TypedExpr
	tw            tableUpdater
	checkHelper   *sqlbase.CheckHelper
	sourceSlots   []sourceSlot

	run        updateRun
	autoCommit autoCommitOpt
}

// updateNode implements the autoCommitNode interface.
var _ autoCommitNode = &updateNode{}

// Update updates columns for a selection of rows from a table.
// Privileges: UPDATE and SELECT on table. We currently always use a select statement.
//   Notes: postgres requires UPDATE. Requires SELECT with WHERE clause with table.
//          mysql requires UPDATE. Also requires SELECT with WHERE clause with table.
func (p *planner) Update(
	ctx context.Context, n *tree.Update, desiredTypes []types.T,
) (planNode, error) {
	if n.Where == nil && p.SessionData().SafeUpdates {
		return nil, pgerror.NewDangerousStatementErrorf("UPDATE without WHERE clause")
	}
	resetter, err := p.initWith(ctx, n.With)
	if err != nil {
		return nil, err
	}
	if resetter != nil {
		defer resetter(p)
	}

	tracing.AnnotateTrace()

	tn, alias, err := p.getAliasedTableName(n.Table)
	if err != nil {
		return nil, err
	}

	en, err := p.makeEditNode(ctx, tn, privilege.UPDATE)
	if err != nil {
		return nil, err
	}

	setExprs := make([]*tree.UpdateExpr, len(n.Exprs))
	for i, expr := range n.Exprs {
		// Analyze the sub-query nodes.
		err := p.analyzeSubqueries(ctx, expr.Expr, len(expr.Names))
		if err != nil {
			return nil, err
		}
		setExprs[i] = &tree.UpdateExpr{Tuple: expr.Tuple, Expr: expr.Expr, Names: expr.Names}
	}

	// Determine which columns we're inserting into.
	names, err := p.namesForExprs(setExprs)
	if err != nil {
		return nil, err
	}

	updateCols, err := p.processColumns(en.tableDesc, names,
		true /* ensureColumns */, false /* allowMutations */)
	if err != nil {
		return nil, err
	}

	defaultExprs, err := sqlbase.MakeDefaultExprs(
		updateCols, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	// TODO(justin): this is incorrect: we should allow this, but then it should
	// error unless we both have a VALUES clause and every value being "inserted"
	// into a computed column is DEFAULT. See #22434.
	if err := checkHasNoComputedCols(updateCols); err != nil {
		return nil, err
	}

	// We update the set of columns being updated into with any computed columns.
	updateCols, computedCols, computeExprs, err :=
		ProcessComputedColumns(ctx, updateCols, tn, en.tableDesc, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	var requestedCols []sqlbase.ColumnDescriptor
	if _, retExprs := n.Returning.(*tree.ReturningExprs); retExprs || len(en.tableDesc.Checks) > 0 {
		// TODO(dan): This could be made tighter, just the rows needed for RETURNING
		// exprs.
		// TODO(nvanbenschoten): This could be made tighter, just the rows needed for
		// the CHECK exprs.
		requestedCols = en.tableDesc.Columns
	}

	fkTables, err := sqlbase.TablesNeededForFKs(
		ctx,
		*en.tableDesc,
		sqlbase.CheckUpdates,
		p.lookupFKTable,
		p.CheckPrivilege,
		p.analyzeExpr,
	)
	if err != nil {
		return nil, err
	}
	ru, err := sqlbase.MakeRowUpdater(
		p.txn,
		en.tableDesc,
		fkTables,
		updateCols,
		requestedCols,
		sqlbase.RowUpdaterDefault,
		p.EvalContext(),
		&p.alloc,
	)
	if err != nil {
		return nil, err
	}
	tw := tableUpdater{ru: ru}

	tracing.AnnotateTrace()

	// We construct a query containing the columns being updated, and then later merge the values
	// they are being updated with into that renderNode to ideally reuse some of the queries.
	rows, err := p.SelectClause(ctx, &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(ru.FetchCols, true /* forUpdateOrDelete */),
		From:  &tree.From{Tables: []tree.TableExpr{n.Table}},
		Where: n.Where,
	}, n.OrderBy, n.Limit, nil /* with */, nil /*desiredTypes*/, publicAndNonPublicColumns)
	if err != nil {
		return nil, err
	}

	// This capacity is an overestimate, since subqueries only take up one sourceSlot.
	sourceSlots := make([]sourceSlot, 0, len(ru.FetchCols))

	// currentUpdateIdx is the index of the first column descriptor in updateCols
	// that is assigned to by the current setExpr.
	currentUpdateIdx := 0

	// We need to add renders below, for the RHS of each
	// assignment. Where can we do this? If we already have a
	// renderNode, then use that. Otherwise, add one.
	var render *renderNode
	if r, ok := rows.(*renderNode); ok {
		render = r
	} else {
		render, err = p.insertRender(ctx, rows, tn)
		if err != nil {
			return nil, err
		}
		// The new renderNode is also the new data source for the update.
		rows = render
	}

	for _, setExpr := range setExprs {
		if setExpr.Tuple {
			switch t := setExpr.Expr.(type) {
			case *tree.Tuple:
				// The user assigned an explicit set of values to the columns. We can't
				// treat this case the same as when we have a subquery (and just evaluate
				// the tuple) because when assigning a literal tuple like this it's valid
				// to assign DEFAULT to some of the columns, which is not valid generally.
				for _, e := range t.Exprs {
					colIdx, err := p.addOrMergeExpr(ctx, e, currentUpdateIdx, updateCols, defaultExprs, render)
					if err != nil {
						return nil, err
					}

					sourceSlots = append(sourceSlots, scalarSlot{
						column:      updateCols[currentUpdateIdx],
						sourceIndex: colIdx,
					})

					currentUpdateIdx++
				}
			case *tree.Subquery:
				selectExpr := tree.SelectExpr{Expr: t}
				desiredTupleType := make(types.TTuple, len(setExpr.Names))
				for i := range setExpr.Names {
					desiredTupleType[i] = updateCols[currentUpdateIdx+i].Type.ToDatumType()
				}
				col, expr, err := p.computeRender(ctx, selectExpr, desiredTupleType,
					render.sourceInfo, render.ivarHelper, autoGenerateRenderOutputName)
				if err != nil {
					return nil, err
				}

				colIdx := render.addOrReuseRender(col, expr, false)

				sourceSlots = append(sourceSlots, tupleSlot{
					columns:     updateCols[currentUpdateIdx : currentUpdateIdx+len(setExpr.Names)],
					sourceIndex: colIdx,
				})
				currentUpdateIdx += len(setExpr.Names)
			default:
				panic(fmt.Sprintf("assigning to tuple with expression that is neither a tuple nor a subquery: %s", setExpr.Expr))
			}

		} else {
			colIdx, err := p.addOrMergeExpr(ctx, setExpr.Expr, currentUpdateIdx, updateCols, defaultExprs, render)
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
	for _, sourceSlot := range sourceSlots {
		if err := sourceSlot.checkColumnTypes(render.render, &p.semaCtx.Placeholders); err != nil {
			return nil, err
		}
	}

	updateColsIdx := make(map[sqlbase.ColumnID]int, len(ru.UpdateCols))
	for i, col := range ru.UpdateCols {
		updateColsIdx[col.ID] = i
	}

	un := updateNodePool.Get().(*updateNode)
	*un = updateNode{
		n:             n,
		editNodeBase:  en,
		updateCols:    ru.UpdateCols,
		computedCols:  computedCols,
		computeExprs:  computeExprs,
		updateColsIdx: updateColsIdx,
		tw:            tw,
		sourceSlots:   sourceSlots,
		checkHelper:   fkTables[en.tableDesc.ID].CheckHelper,
	}
	if err := un.run.initEditNode(
		ctx, &un.editNodeBase, rows, &un.tw, alias, n.Returning, desiredTypes); err != nil {
		return nil, err
	}
	return un, nil
}

// updateRun contains the run-time state of updateNode during local execution.
type updateRun struct {
	// The following fields are populated during Start().
	editNodeRun
}

func (u *updateNode) startExec(params runParams) error {
	if err := u.run.startEditNode(params, &u.editNodeBase); err != nil {
		return err
	}
	return u.run.tw.init(params.p.txn, params.EvalContext())
}

func (u *updateNode) Next(params runParams) (bool, error) {
	next, err := u.run.rows.Next(params)
	if !next {
		if err == nil {
			if err := params.p.cancelChecker.Check(); err != nil {
				return false, err
			}
			// We're done. Finish the batch.
			_, err = u.tw.finalize(params.ctx, u.autoCommit, params.extendedEvalCtx.Tracing.KVTracingEnabled())
		}
		return false, err
	}

	tracing.AnnotateTrace()

	entireRow := u.run.rows.Values()

	// Our updated value expressions occur immediately after the plain
	// columns in the output.
	oldValues := entireRow[:len(u.tw.ru.FetchCols)]

	updateValues := make(tree.Datums, len(u.tw.ru.UpdateCols))
	valueIdx := 0

	for _, slot := range u.sourceSlots {
		for _, value := range slot.extractValues(entireRow) {
			updateValues[valueIdx] = value
			valueIdx++
		}
	}

	if len(u.computeExprs) > 0 {
		// Evaluate computed columns if necessary.
		newVals := make(tree.Datums, len(oldValues))
		copy(newVals, oldValues)
		for i, col := range u.tw.ru.UpdateCols {
			newVals[u.tw.ru.FetchColIDtoRowIndex[col.ID]] = updateValues[i]
		}

		iv := &rowIndexedVarContainer{newVals, u.tableDesc.Columns, u.tw.ru.FetchColIDtoRowIndex}
		params.EvalContext().IVarContainer = iv

		for i := range u.computedCols {
			d, err := u.computeExprs[i].Eval(params.EvalContext())
			if err != nil {
				return false, err
			}
			updateValues[u.updateColsIdx[u.computedCols[i].ID]] = d
		}
	}

	params.EvalContext().IVarContainer = nil

	// TODO(justin): we have actually constructed the whole row at this point and
	// thus should be able to avoid loading it separately like this now.
	if err := u.checkHelper.LoadRow(u.tw.ru.FetchColIDtoRowIndex, oldValues, false); err != nil {
		return false, err
	}
	if err := u.checkHelper.LoadRow(u.updateColsIdx, updateValues, true); err != nil {
		return false, err
	}
	if err := u.checkHelper.Check(params.EvalContext()); err != nil {
		return false, err
	}

	// Ensure that the values honor the specified column widths.
	for i := range updateValues {
		if err := sqlbase.CheckValueWidth(u.tw.ru.UpdateCols[i].Type, updateValues[i], u.tw.ru.UpdateCols[i].Name); err != nil {
			return false, err
		}
	}

	for i, col := range u.tw.ru.UpdateCols {
		val := updateValues[i]
		if !col.Nullable && val == tree.DNull {
			return false, sqlbase.NewNonNullViolationError(col.Name)
		}
	}

	// Update the row values.
	newValues, err := u.tw.row(
		params.ctx, append(oldValues, updateValues...), params.p.extendedEvalCtx.Tracing.KVTracingEnabled(),
	)
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

func (u *updateNode) Values() tree.Datums {
	return u.run.resultRow
}

// requireSpool implements the planNodeRequireSpool interface.
func (u *updateNode) requireSpool() {}

func (u *updateNode) Close(ctx context.Context) {
	u.run.rows.Close(ctx)
	u.tw.close(ctx)
	*u = updateNode{}
	updateNodePool.Put(u)
}

// enableAutoCommit is part of the autoCommitNode interface.
func (u *updateNode) enableAutoCommit() {
	u.autoCommit = autoCommitEnabled
}

// editNode (Base, Run) is shared between all row updating
// statements (DELETE, UPDATE, INSERT).

// editNodeBase holds the common (prepare+execute) state needed to run
// row-modifying statements.
type editNodeBase struct {
	p         *planner
	rh        *returningHelper
	tableDesc *sqlbase.TableDescriptor
}

func (p *planner) makeEditNode(
	ctx context.Context, tn *tree.TableName, priv privilege.Kind,
) (editNodeBase, error) {
	tableDesc, err := ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
	if err != nil {
		return editNodeBase{}, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, priv); err != nil {
		return editNodeBase{}, err
	}

	return editNodeBase{
		p:         p,
		tableDesc: tableDesc,
	}, nil
}

// editNodeRun holds the runtime (execute) state needed to run
// row-modifying statements.
type editNodeRun struct {
	rows      planNode
	tw        tableWriter
	resultRow tree.Datums
}

func (r *editNodeRun) initEditNode(
	ctx context.Context,
	en *editNodeBase,
	rows planNode,
	tw tableWriter,
	tn *tree.TableName,
	re tree.ReturningClause,
	desiredTypes []types.T,
) error {
	r.rows = rows
	r.tw = tw

	rh, err := en.p.newReturningHelper(ctx, re, desiredTypes, tn, en.tableDesc.Columns)
	if err != nil {
		return err
	}
	en.rh = rh

	return nil
}

func (r *editNodeRun) startEditNode(params runParams, en *editNodeBase) error {
	if sqlbase.IsSystemConfigID(en.tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		return en.p.txn.SetSystemConfigTrigger()
	}
	return nil
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
	extractValues(resultRow tree.Datums) tree.Datums
	// checkColumnTypes compares the types of the results that this slot refers to to the types of
	// the columns those values will be assigned to. It returns an error if those types don't match up.
	// It also populates the types of any placeholders by way of calling into sqlbase.CheckColumnType.
	checkColumnTypes(row []tree.TypedExpr, pmap *tree.PlaceholderInfo) error
}

type tupleSlot struct {
	columns     []sqlbase.ColumnDescriptor
	sourceIndex int
}

func (ts tupleSlot) extractValues(row tree.Datums) tree.Datums {
	return row[ts.sourceIndex].(*tree.DTuple).D
}

func (ts tupleSlot) checkColumnTypes(row []tree.TypedExpr, pmap *tree.PlaceholderInfo) error {
	renderedResult := row[ts.sourceIndex]
	for i, typ := range renderedResult.ResolvedType().(types.TTuple) {
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

func (ss scalarSlot) extractValues(row tree.Datums) tree.Datums {
	return row[ss.sourceIndex : ss.sourceIndex+1]
}

func (ss scalarSlot) checkColumnTypes(row []tree.TypedExpr, pmap *tree.PlaceholderInfo) error {
	renderedResult := row[ss.sourceIndex]
	typ := renderedResult.ResolvedType()
	return sqlbase.CheckColumnType(ss.column, typ, pmap)
}

// addOrMergeExpr inserts an Expr into a renderNode, attempting to reuse
// previous renders if possible by using render.addOrMergeRender, returning the
// column index at which the rendered value can be accessed.
func (p *planner) addOrMergeExpr(
	ctx context.Context,
	e tree.Expr,
	currentUpdateIdx int,
	updateCols []sqlbase.ColumnDescriptor,
	defaultExprs []tree.TypedExpr,
	render *renderNode,
) (colIdx int, err error) {
	e = fillDefault(e, currentUpdateIdx, defaultExprs)
	selectExpr := tree.SelectExpr{Expr: e}
	typ := updateCols[currentUpdateIdx].Type.ToDatumType()
	col, expr, err := p.computeRender(ctx, selectExpr, typ,
		render.sourceInfo, render.ivarHelper, autoGenerateRenderOutputName)
	if err != nil {
		return -1, err
	}

	return render.addOrReuseRender(col, expr, true), nil
}

// namesForExprs collects all the names mentioned in the LHS of the
// UpdateExprs.  That is, it will transform SET (a,b) = (1,2), b = 3,
// (a,c) = 4 into [a,b,b,a,c].
//
// It also checks that the arity of the LHS and RHS match when
// assigning tuples.
func (p *planner) namesForExprs(exprs tree.UpdateExprs) (tree.NameList, error) {
	var names tree.NameList
	for _, expr := range exprs {
		if expr.Tuple {
			n := -1
			switch t := expr.Expr.(type) {
			case *tree.Subquery:
				if tup, ok := t.ResolvedType().(types.TTuple); ok {
					n = len(tup)
				}
			case *tree.Tuple:
				n = len(t.Exprs)
			case *tree.DTuple:
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

func fillDefault(expr tree.Expr, index int, defaultExprs []tree.TypedExpr) tree.Expr {
	switch expr.(type) {
	case tree.DefaultVal:
		if defaultExprs == nil {
			return tree.DNull
		}
		return defaultExprs[index]
	}
	return expr
}
