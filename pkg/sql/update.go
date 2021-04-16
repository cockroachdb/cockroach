// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
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
	columns colinfo.ResultColumns

	run updateRun
}

// updateRun contains the run-time state of updateNode during local execution.
type updateRun struct {
	tu         tableUpdater
	rowsNeeded bool

	checkOrds checkSet

	// done informs a new call to BatchedNext() that the previous call to
	// BatchedNext() has completed the work already.
	done bool

	// traceKV caches the current KV tracing flag.
	traceKV bool

	// computedCols are the columns that need to be (re-)computed as
	// the result of updating some of the columns in updateCols.
	computedCols []catalog.Column
	// computeExprs are the expressions to evaluate to re-compute the
	// columns in computedCols.
	computeExprs []tree.TypedExpr
	// iVarContainerForComputedCols is used as a temporary buffer that
	// holds the updated values for every column in the source, to
	// serve as input for indexed vars contained in the computeExprs.
	iVarContainerForComputedCols schemaexpr.RowIndexedVarContainer

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
	updateColsIdx catalog.TableColMap

	// rowIdxToRetIdx is the mapping from the columns in ru.FetchCols to the
	// columns in the resultRowBuffer. A value of -1 is used to indicate
	// that the column at that index is not part of the resultRowBuffer
	// of the mutation. Otherwise, the value at the i-th index refers to the
	// index of the resultRowBuffer where the i-th column is to be returned.
	rowIdxToRetIdx []int

	// numPassthrough is the number of columns in addition to the set of
	// columns of the target table being returned, that we must pass through
	// from the input node.
	numPassthrough int
}

func (u *updateNode) startExec(params runParams) error {
	// cache traceKV during execution, to avoid re-evaluating it for every row.
	u.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	if u.run.rowsNeeded {
		u.run.tu.rows = rowcontainer.NewRowContainer(
			params.EvalContext().Mon.MakeBoundAccount(),
			colinfo.ColTypeInfoFromResCols(u.columns),
		)
	}
	return u.run.tu.init(params.ctx, params.p.txn, params.EvalContext())
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

	// Advance one batch. First, clear the last batch.
	u.run.tu.clearLastBatch(params.ctx)

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

		// Are we done yet with the current batch?
		if u.run.tu.currentBatchSize >= u.run.tu.maxBatchSize {
			break
		}
	}

	if u.run.tu.currentBatchSize > 0 {
		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err := u.run.tu.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		if err := u.run.tu.finalize(params.ctx); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		u.run.done = true
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(
		u.run.tu.tableDesc().GetID(),
		u.run.tu.lastBatchSize,
	)

	return u.run.tu.lastBatchSize > 0, nil
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
		for i := range u.run.tu.ru.UpdateCols {
			id := u.run.tu.ru.UpdateCols[i].GetID()
			idx := u.run.tu.ru.FetchColIDtoRowIndex.GetDefault(id)
			u.run.iVarContainerForComputedCols.CurSourceRow[idx] = u.run.
				updateValues[i]
		}

		// Now (re-)compute the computed columns.
		// Note that it's safe to do this in any order, because we currently
		// prevent computed columns from depending on other computed columns.
		params.EvalContext().PushIVarContainer(&u.run.iVarContainerForComputedCols)
		for i := range u.run.computedCols {
			d, err := u.run.computeExprs[i].Eval(params.EvalContext())
			if err != nil {
				params.EvalContext().IVarContainer = nil
				name := u.run.computedCols[i].GetName()
				return errors.Wrapf(err, "computed column %s", tree.ErrString((*tree.Name)(&name)))
			}
			idx := u.run.updateColsIdx.GetDefault(u.run.computedCols[i].GetID())
			u.run.updateValues[idx] = d
		}
		params.EvalContext().PopIVarContainer()
	}

	// Verify the schema constraints. For consistency with INSERT/UPSERT
	// and compatibility with PostgreSQL, we must do this before
	// processing the CHECK constraints.
	if err := enforceLocalColumnConstraints(u.run.updateValues, u.run.tu.ru.UpdateCols); err != nil {
		return err
	}

	// Run the CHECK constraints, if any. CheckHelper will either evaluate the
	// constraints itself, or else inspect boolean columns from the input that
	// contain the results of evaluation.
	if !u.run.checkOrds.Empty() {
		checkVals := sourceVals[len(u.run.tu.ru.FetchCols)+len(u.run.tu.ru.UpdateCols)+u.run.numPassthrough:]
		if err := checkMutationInput(
			params.ctx, &params.p.semaCtx, u.run.tu.tableDesc(), u.run.checkOrds, checkVals,
		); err != nil {
			return err
		}
	}

	// Create a set of partial index IDs to not add entries or remove entries
	// from.
	var pm row.PartialIndexUpdateHelper
	if n := len(u.run.tu.tableDesc().PartialIndexes()); n > 0 {
		offset := len(u.run.tu.ru.FetchCols) + len(u.run.tu.ru.UpdateCols) + u.run.checkOrds.Len() + u.run.numPassthrough
		partialIndexVals := sourceVals[offset:]
		partialIndexPutVals := partialIndexVals[:n]
		partialIndexDelVals := partialIndexVals[n : n*2]

		err := pm.Init(partialIndexPutVals, partialIndexDelVals, u.run.tu.tableDesc())
		if err != nil {
			return err
		}
	}

	// Queue the insert in the KV batch.
	newValues, err := u.run.tu.rowForUpdate(params.ctx, oldValues, u.run.updateValues, pm, u.run.traceKV)
	if err != nil {
		return err
	}

	// If result rows need to be accumulated, do it.
	if u.run.tu.rows != nil {
		// The new values can include all columns, the construction of the
		// values has used execinfra.ScanVisibilityPublicAndNotPublic so the
		// values may contain additional columns for every newly added column
		// not yet visible. We do not want them to be available for RETURNING.
		//
		// MakeUpdater guarantees that the first columns of the new values
		// are those specified u.columns.
		resultValues := make([]tree.Datum, len(u.columns))
		largestRetIdx := -1
		for i := range u.run.rowIdxToRetIdx {
			retIdx := u.run.rowIdxToRetIdx[i]
			if retIdx >= 0 {
				if retIdx >= largestRetIdx {
					largestRetIdx = retIdx
				}
				resultValues[retIdx] = newValues[i]
			}
		}

		// At this point we've extracted all the RETURNING values that are part
		// of the target table. We must now extract the columns in the RETURNING
		// clause that refer to other tables (from the FROM clause of the update).
		if u.run.numPassthrough > 0 {
			passthroughBegin := len(u.run.tu.ru.FetchCols) + len(u.run.tu.ru.UpdateCols)
			passthroughEnd := passthroughBegin + u.run.numPassthrough
			passthroughValues := sourceVals[passthroughBegin:passthroughEnd]

			for i := 0; i < u.run.numPassthrough; i++ {
				largestRetIdx++
				resultValues[largestRetIdx] = passthroughValues[i]
			}
		}

		if _, err := u.run.tu.rows.AddRow(params.ctx, resultValues); err != nil {
			return err
		}
	}

	return nil
}

// BatchedCount implements the batchedPlanNode interface.
func (u *updateNode) BatchedCount() int { return u.run.tu.lastBatchSize }

// BatchedCount implements the batchedPlanNode interface.
func (u *updateNode) BatchedValues(rowIdx int) tree.Datums { return u.run.tu.rows.At(rowIdx) }

func (u *updateNode) Close(ctx context.Context) {
	u.source.Close(ctx)
	u.run.tu.close(ctx)
	*u = updateNode{}
	updateNodePool.Put(u)
}

func (u *updateNode) enableAutoCommit() {
	u.run.tu.enableAutoCommit()
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
	checkColumnTypes(row []tree.TypedExpr) error
}

type scalarSlot struct {
	column      catalog.Column
	sourceIndex int
}

func (ss scalarSlot) extractValues(row tree.Datums) tree.Datums {
	return row[ss.sourceIndex : ss.sourceIndex+1]
}

func (ss scalarSlot) checkColumnTypes(row []tree.TypedExpr) error {
	renderedResult := row[ss.sourceIndex]
	typ := renderedResult.ResolvedType()
	return colinfo.CheckDatumTypeFitsColumnType(ss.column, typ)
}

// enforceLocalColumnConstraints asserts the column constraints that
// do not require data validation from other sources than the row data
// itself. This includes:
// - rejecting null values in non-nullable columns;
// - checking width constraints from the column type;
// - truncating results to the requested precision (not width).
// Note: the second point is what distinguishes this operation
// from a regular SQL cast -- here widths are checked, not
// used to truncate the value silently.
//
// The row buffer is modified in-place with the result of the
// checks.
func enforceLocalColumnConstraints(row tree.Datums, cols []catalog.Column) error {
	for i, col := range cols {
		if !col.IsNullable() && row[i] == tree.DNull {
			return sqlerrors.NewNonNullViolationError(col.GetName())
		}
		outVal, err := tree.AdjustValueToType(col.GetType(), row[i])
		if err != nil {
			return err
		}
		row[i] = outVal
	}
	return nil
}
