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
	source planNode

	// columns is set if this UPDATE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns sqlbase.ResultColumns

	run updateRun
}

// updateNode implements the autoCommitNode interface.
var _ autoCommitNode = &updateNode{}

// Update updates columns for a selection of rows from a table.
// Privileges: UPDATE and SELECT on table. We currently always use a select statement.
//   Notes: postgres requires UPDATE. Requires SELECT with WHERE clause with table.
//          mysql requires UPDATE. Also requires SELECT with WHERE clause with table.
func (p *planner) Update(
	ctx context.Context, n *tree.Update, desiredTypes []types.T,
) (result planNode, resultErr error) {
	// UX friendliness safeguard.
	if n.Where == nil && p.SessionData().SafeUpdates {
		return nil, pgerror.NewDangerousStatementErrorf("UPDATE without WHERE clause")
	}

	// CTE analysis.
	resetter, err := p.initWith(ctx, n.With)
	if err != nil {
		return nil, err
	}
	if resetter != nil {
		defer func() {
			if cteErr := resetter(p); cteErr != nil && resultErr == nil {
				// If no error was found in the inner planning but a CTE error
				// is occurring during the final checks on the way back from
				// the recursion, use that error as final error for this
				// stage.
				resultErr = cteErr
				result = nil
			}
		}()
	}

	tracing.AnnotateTrace()

	// UPDATE xx AS yy - we want to know about xx (tn) because
	// that's what we get the descriptor with, and yy (alias) because
	// that's what RETURNING will use.
	tn, alias, err := p.getAliasedTableName(n.Table)
	if err != nil {
		return nil, err
	}

	// Find which table we're working on, check the permissions.
	desc, err := ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
	if err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, desc, privilege.UPDATE); err != nil {
		return nil, err
	}

	// Determine what are the foreign key tables that are involved in the update.
	fkTables, err := sqlbase.TablesNeededForFKs(
		ctx,
		*desc,
		sqlbase.CheckUpdates,
		p.LookupTableByID,
		p.CheckPrivilege,
		p.analyzeExpr,
	)
	if err != nil {
		return nil, err
	}

	// Pre-perform early subquery analysis and extraction. Usually
	// this is done by analyzeExpr(), and, in fact, analyzeExpr() is indirectly
	// called below as well. Why not call analyzeExpr() already?
	//
	// The obstacle is that there's a circular dependency here.
	//
	// - We can't call analyzeExpr() at this point because the RHS of
	//   UPDATE SET can contain the special expression "DEFAULT", and
	//   analyzeExpr() does not work with DEFAULT.
	//
	// - The substitution of DEFAULT by the actual default expression
	//   occurs below (in addOrMergeExpr), but we can't do that yet here
	//   because we first need to decompose a tuple in the LHS into
	//   multiple assignments.
	//
	// - We can't decompose the tuple in the LHS without validating it
	//   against the arity of the RHS (to properly reject mismatched
	//   arities with an error) until subquery analysis has occurred.
	//
	// So we need the subquery analysis to be done early and we can't
	// call analyzeExpr() to do so.
	//
	// TODO(knz): arguably we could do the tuple decomposition _before_
	// the arity check, in this order: decompose tuples, substitute
	// DEFAULT / run addOrMergeExpr which itself calls analyzeExpr, and
	// then check the arity. This improvement is left as an exercise for
	// the reader.
	setExprs := make([]*tree.UpdateExpr, len(n.Exprs))
	for i, expr := range n.Exprs {
		// Analyze the sub-query nodes.
		err := p.analyzeSubqueries(ctx, expr.Expr, len(expr.Names))
		if err != nil {
			return nil, err
		}
		setExprs[i] = &tree.UpdateExpr{Tuple: expr.Tuple, Expr: expr.Expr, Names: expr.Names}
	}

	// Extract all the LHS column names, and verify that the arity of
	// the LHS and RHS match when assigning tuples.
	names, err := p.namesForExprs(setExprs)
	if err != nil {
		return nil, err
	}

	// Extract the column descriptors for the column names listed
	// in the LHS operands of SET expressions. This also checks
	// that each column is assigned at most once.
	updateCols, err := p.processColumns(desc, names,
		true /* ensureColumns */, false /* allowMutations */)
	if err != nil {
		return nil, err
	}

	// Ensure that the columns being updated are not computed.
	// We do this check as early as possible to avoid doing
	// unnecessary work below in case there's an error.
	//
	// TODO(justin): this is too restrictive. It should
	// be possible to allow UPDATE foo SET x = DEFAULT
	// when x is a computed column. See #22434.
	if err := checkHasNoComputedCols(updateCols); err != nil {
		return nil, err
	}

	// Extract the pre-analyzed, pre-typed default expressions for all
	// the updated columns. There are as many defaultExprs as there are
	// updateCols.
	defaultExprs, err := sqlbase.MakeDefaultExprs(
		updateCols, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	// Extract the computed columns, if any. This extends updateCols with
	// all the computed columns. The computedCols result is an alias for the suffix
	// of updateCols that corresponds to computed columns.
	updateCols, computedCols, computeExprs, err :=
		sqlbase.ProcessComputedColumns(ctx, updateCols, tn, desc, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	// rowsNeeded will help determine whether we need to allocate a
	// rowsContainer.
	rowsNeeded := resultsNeeded(n.Returning)

	var requestedCols []sqlbase.ColumnDescriptor
	if rowsNeeded || len(desc.Checks) > 0 {
		// TODO(dan): This could be made tighter, just the rows needed for RETURNING
		// exprs.
		// TODO(nvanbenschoten): This could be made tighter, just the rows needed for
		// the CHECK exprs.
		requestedCols = desc.Columns
	}

	// Create the table updater, which does the bulk of the work.
	// As a result of MakeRowUpdater, ru.FetchCols include all the
	// columns in the table descriptor + any columns currently in the
	// process of being added.
	ru, err := sqlbase.MakeRowUpdater(
		p.txn,
		desc,
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

	tracing.AnnotateTrace()

	// We construct a query containing the columns being updated, and
	// then later merge the values they are being updated with into that
	// renderNode to ideally reuse some of the queries.
	rows, err := p.SelectClause(ctx, &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(ru.FetchCols, true /* forUpdateOrDelete */),
		From:  &tree.From{Tables: []tree.TableExpr{n.Table}},
		Where: n.Where,
	}, n.OrderBy, n.Limit, nil /* with */, nil /*desiredTypes*/, publicAndNonPublicColumns)
	if err != nil {
		return nil, err
	}

	// sourceSlots describes the RHS operands to potential tuple-wise
	// assignments to the LHS operands. See the comment on
	// updateRun.sourceSlots for details.
	//
	// UPDATE ... SET (a,b) = (...), (c,d) = (...)
	//                ^^^^^^^^^^(1)  ^^^^^^^^^^(2) len(n.Exprs) == len(sourceSlots)
	sourceSlots := make([]sourceSlot, 0, len(n.Exprs))

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

	// Capture the columns of the source, prior to the insertion of
	// extra renders. This will be the input for RETURNING, if any, and
	// this must not see the additional renders added below.
	// It also must not see the additional columns captured in FetchCols
	// but which were not in requestedCols.
	var columns sqlbase.ResultColumns
	if rowsNeeded {
		columns = planColumns(rows)
		// If rowsNeeded is set, we have requested from the source above
		// all the columns from the descriptor. However, to ensure that
		// modified rows include all columns, the construction of the
		// source has used publicAndNonVivible columns so the source may
		// contain additional columns for every newly added column not yet
		// visible.
		// We do not want these to be available for RETURNING below.
		//
		// MakeRowUpdater guarantees that the first columns of the source
		// are those specified in requestedCols, which, in the case where
		// rowsNeeded is true, is also desc.Columns. So we can truncate to
		// the length of that to only see public columns.
		columns = columns[:len(desc.Columns)]
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
				desiredTupleType := types.TTuple{Types: make([]types.T, len(setExpr.Names))}
				for i := range setExpr.Names {
					desiredTupleType.Types[i] = updateCols[currentUpdateIdx+i].Type.ToDatumType()
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

	// updateColsIdx inverts the mapping of UpdateCols to FetchCols. See
	// the explanatory comments in updateRun.
	updateColsIdx := make(map[sqlbase.ColumnID]int, len(ru.UpdateCols))
	for i, col := range ru.UpdateCols {
		updateColsIdx[col.ID] = i
	}

	un := updateNodePool.Get().(*updateNode)
	*un = updateNode{
		source:  rows,
		columns: columns,
		run: updateRun{
			tu:           tableUpdater{ru: ru},
			checkHelper:  fkTables[desc.ID].CheckHelper,
			rowsNeeded:   rowsNeeded,
			computedCols: computedCols,
			computeExprs: computeExprs,
			iVarContainerForComputedCols: sqlbase.RowIndexedVarContainer{
				CurSourceRow: make(tree.Datums, len(ru.FetchCols)),
				Cols:         desc.Columns,
				Mapping:      ru.FetchColIDtoRowIndex,
			},
			sourceSlots:   sourceSlots,
			updateValues:  make(tree.Datums, len(ru.UpdateCols)),
			updateColsIdx: updateColsIdx,
		},
	}

	// Finally, handle RETURNING, if any.
	r, err := p.Returning(ctx, un, n.Returning, desiredTypes, alias)
	if err != nil {
		// We close explicitly here to release the node to the pool.
		un.Close(ctx)
	}
	return r, err
}

// updateRun contains the run-time state of updateNode during local execution.
type updateRun struct {
	tu          tableUpdater
	checkHelper *sqlbase.CheckHelper
	rowsNeeded  bool

	// rowCount is the number of rows in the current batch.
	rowCount int

	// done informs a new call to BatchedNext() that the previous call to
	// BatchedNext() has completed the work already.
	done bool

	// rows contains the accumulated result rows if rowsNeeded is set.
	rows *sqlbase.RowContainer

	// autoCommit indicates whether the last KV batch processed by
	// this update will also commit the KV txn.
	autoCommit autoCommitOpt

	// traceKV caches the current KV tracing flag.
	traceKV bool

	// computedCols are the columns that need to be (re-)computed as
	// the result of updating some of the columns in updateCols.
	computedCols []sqlbase.ColumnDescriptor
	// computeExprs are the expressions to evaluate to re-compute the
	// columns in computedCols.
	computeExprs []tree.TypedExpr
	// iVarContainerForComputedCols is used as a temporary buffer that
	// holds the updated values for every column in the source, to
	// serve as input for indexed vars contained in the computeExprs.
	iVarContainerForComputedCols sqlbase.RowIndexedVarContainer

	// sourceSlots is the helper that maps RHS expressions to LHS targets.
	// This is necessary because there may be fewer RHS expressions than
	// LHS targets. For example, SET (a, b) = (SELECT 1,2) has:
	// - 2 targets (a, b)
	// - 1 source slot, the subquery (SELECT 1, 2).
	// Each call to extractValues() on a sourceSlot will return 1 or more
	// datums suitable for assignments. In the example above, the
	// method would return 2 values.
	sourceSlots []sourceSlot

	// updateValues will hold the new values for every column
	// mentioned in the LHS of the SET expressions, in the
	// order specified by those SET expressions (thus potentially
	// a different order than the source).
	updateValues tree.Datums

	// During the update, the expressions provided by the source plan
	// contain the columns that are being assigned in the order
	// specified by the table descriptor.
	//
	// For example, with UPDATE kv SET v=3, k=2, the source plan will
	// provide the values in the order k, v (assuming this is the order
	// the columns are defined in kv's descriptor).
	//
	// Then during the update, the columns are updated in the order of
	// the setExprs (or, equivalently, the order of the sourceSlots),
	// for the example above that would be v, k. The results
	// are stored in updateValues above.
	//
	// Then at the end of the update, the values need to be presented
	// back to the TableRowUpdater in the order of the table descriptor
	// again.
	//
	// updateVals is the buffer for this 2nd stage.
	// updateColsIdx maps the order of the 2nd stage into the order of the 3rd stage.
	// This provides the inverse mapping of sourceSlots.
	//
	updateColsIdx map[sqlbase.ColumnID]int
}

// maxUpdateBatchSize is the max number of entries in the KV batch for
// the update operation (including secondary index updates, FK
// cascading updates, etc), before the current KV batch is executed
// and a new batch is started.
const maxUpdateBatchSize = 10000

func (u *updateNode) startExec(params runParams) error {
	if err := params.p.maybeSetSystemConfig(u.run.tu.tableDesc().GetID()); err != nil {
		return err
	}

	// cache traceKV during execution, to avoid re-evaluating it for every row.
	u.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	if u.run.rowsNeeded {
		u.run.rows = sqlbase.NewRowContainer(
			params.EvalContext().Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(u.columns), 0)
	}
	return u.run.tu.init(params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (u *updateNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (u *updateNode) Values() tree.Datums { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (u *updateNode) BatchedNext(params runParams) (bool, error) {
	if u.run.done {
		return false, nil
	}

	tracing.AnnotateTrace()

	// Advance one batch. First, clear the current batch.
	u.run.rowCount = 0
	if u.run.rows != nil {
		u.run.rows.Clear(params.ctx)
	}
	// Now consume/accumulate the rows for this batch.
	lastBatch := false
	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Advance one individual row.
		if next, err := u.source.Next(params); !next {
			lastBatch = true
			if err != nil {
				return false, err
			}
			break
		}

		// Process the update for the current source row, potentially
		// accumulating the result row for later.
		if err := u.processSourceRow(params, u.source.Values()); err != nil {
			return false, err
		}

		u.run.rowCount++

		// Are we done yet with the current batch?
		if u.run.tu.curBatchSize() >= maxUpdateBatchSize {
			break
		}
	}

	if u.run.rowCount > 0 {
		if err := u.run.tu.atBatchEnd(params.ctx, u.run.traceKV); err != nil {
			return false, err
		}

		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err := u.run.tu.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		if _, err := u.run.tu.finalize(params.ctx, u.run.autoCommit, u.run.traceKV); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		u.run.done = true
	}

	return u.run.rowCount > 0, nil
}

// processSourceRow processes one row from the source for update and, if
// result rows are needed, saves it in the result row container.
func (u *updateNode) processSourceRow(params runParams, sourceVals tree.Datums) error {
	// sourceVals contains values for the columns from the table, in the order of the
	// table descriptor. (One per column in u.tw.ru.FetchCols)
	//
	// And then after that, all the extra expressions potentially added via
	// a renderNode for the RHS of the assignments.

	// oldValues is the prefix of sourceVals that corresponds to real
	// stored columns in the table, that is, excluding the RHS assignment
	// expressions.
	oldValues := sourceVals[:len(u.run.tu.ru.FetchCols)]

	// valueIdx is used in the loop below to map sourceSlots to
	// entries in updateValues.
	valueIdx := 0

	// Propagate the values computed for the RHS expressions into
	// updateValues at the right positions. The positions in
	// updateValues correspond to the columns named in the LHS
	// operands for SET.
	for _, slot := range u.run.sourceSlots {
		for _, value := range slot.extractValues(sourceVals) {
			u.run.updateValues[valueIdx] = value
			valueIdx++
		}
	}

	// At this point, we have populated updateValues with the result of
	// computing the RHS for every assignment.
	//

	if len(u.run.computeExprs) > 0 {
		// We now need to (re-)compute the computed column values, using
		// the updated values above as input.
		//
		// This needs to happen in the context of a row containing all the
		// table's columns as if they had been updated already. This is not
		// yet reflected neither by oldValues (which contain non-updated values)
		// nor updateValues (which contain only those columns mentioned in the SET LHS).
		//
		// So we need to construct a buffer that groups them together.
		// iVarContainerForComputedCols does this.
		copy(u.run.iVarContainerForComputedCols.CurSourceRow, oldValues)
		for i, col := range u.run.tu.ru.UpdateCols {
			u.run.iVarContainerForComputedCols.CurSourceRow[u.run.tu.ru.FetchColIDtoRowIndex[col.ID]] = u.run.updateValues[i]
		}

		// Now (re-)compute the computed columns.
		// Note that it's safe to do this in any order, because we currently
		// prevent computed columns from depending on other computed columns.
		params.EvalContext().PushIVarContainer(&u.run.iVarContainerForComputedCols)
		for i := range u.run.computedCols {
			d, err := u.run.computeExprs[i].Eval(params.EvalContext())
			if err != nil {
				params.EvalContext().IVarContainer = nil
				return errors.Wrapf(err,
					"computed column %s", tree.ErrString((*tree.Name)(&u.run.computedCols[i].Name)))
			}
			u.run.updateValues[u.run.updateColsIdx[u.run.computedCols[i].ID]] = d
		}
		params.EvalContext().PopIVarContainer()
	}

	// Verify the schema constraints. For consistency with INSERT/UPSERT
	// and compatibility with PostgreSQL, we must do this before
	// processing the CHECK constraints.
	for i, val := range u.run.updateValues {
		col := &u.run.tu.ru.UpdateCols[i]
		if val == tree.DNull {
			// Verify no NULL makes it to a nullable column.
			if !col.Nullable {
				return sqlbase.NewNonNullViolationError(col.Name)
			}
		} else {
			// Verify that the data width matches the column constraint.
			if err := sqlbase.CheckValueWidth(col.Type, val, &col.Name); err != nil {
				return err
			}
		}
	}

	// Run the CHECK constraints, if any.
	// TODO(justin): we have actually constructed the whole row at this point and
	// thus should be able to avoid loading it separately like this now.
	if len(u.run.checkHelper.Exprs) > 0 {
		if err := u.run.checkHelper.LoadRow(
			u.run.tu.ru.FetchColIDtoRowIndex, oldValues, false); err != nil {
			return err
		}
		if err := u.run.checkHelper.LoadRow(
			u.run.updateColsIdx, u.run.updateValues, true); err != nil {
			return err
		}
		if err := u.run.checkHelper.Check(params.EvalContext()); err != nil {
			return err
		}
	}

	// Queue the insert in the KV batch.
	newValues, err := u.run.tu.rowForUpdate(params.ctx, oldValues, u.run.updateValues, u.run.traceKV)
	if err != nil {
		return err
	}

	// If result rows need to be accumulated, do it.
	if u.run.rows != nil {
		if _, err := u.run.rows.AddRow(params.ctx, newValues); err != nil {
			return err
		}
	}

	return nil
}

// BatchedCount implements the batchedPlanNode interface.
func (u *updateNode) BatchedCount() int { return u.run.rowCount }

// BatchedCount implements the batchedPlanNode interface.
func (u *updateNode) BatchedValues(rowIdx int) tree.Datums { return u.run.rows.At(rowIdx) }

func (u *updateNode) Close(ctx context.Context) {
	u.source.Close(ctx)
	if u.run.rows != nil {
		u.run.rows.Close(ctx)
		u.run.rows = nil
	}
	u.run.tu.close(ctx)
	*u = updateNode{}
	updateNodePool.Put(u)
}

// enableAutoCommit implements the autoCommitNode interface.
func (u *updateNode) enableAutoCommit() {
	u.run.autoCommit = autoCommitEnabled
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
	for i, typ := range renderedResult.ResolvedType().(types.TTuple).Types {
		if err := sqlbase.CheckDatumTypeFitsColumnType(ts.columns[i], typ, pmap); err != nil {
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
	return sqlbase.CheckDatumTypeFitsColumnType(ss.column, typ, pmap)
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
					n = len(tup.Types)
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

func checkHasNoComputedCols(cols []sqlbase.ColumnDescriptor) error {
	for i := range cols {
		if cols[i].IsComputed() {
			return sqlbase.CannotWriteToComputedColError(&cols[i])
		}
	}
	return nil
}
