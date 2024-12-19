// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
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
	singleInputPlanNode

	// columns is set if this UPDATE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run updateRun
}

var _ mutationPlanNode = &updateNode{}

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

	// regionLocalInfo handles erroring out the UPDATE when the
	// enforce_home_region setting is on.
	regionLocalInfo regionLocalInfoType
}

func (u *updateNode) startExec(params runParams) error {
	// cache traceKV during execution, to avoid re-evaluating it for every row.
	u.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	if u.run.rowsNeeded {
		u.run.tu.rows = rowcontainer.NewRowContainer(
			params.p.Mon().MakeBoundAccount(),
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
		if next, err := u.input.Next(params); !next {
			lastBatch = true
			if err != nil {
				return false, err
			}
			break
		}

		// Process the update for the current input row, potentially
		// accumulating the result row for later.
		if err := u.processSourceRow(params, u.input.Values()); err != nil {
			return false, err
		}

		// Are we done yet with the current batch?
		if u.run.tu.currentBatchSize >= u.run.tu.maxBatchSize ||
			u.run.tu.b.ApproximateMutationBytes() >= u.run.tu.maxBatchByteSize {
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
		u.run.tu.setRowsWrittenLimit(params.extendedEvalCtx.SessionData())
		if err := u.run.tu.finalize(params.ctx); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		u.run.done = true
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(u.run.tu.tableDesc(), u.run.tu.lastBatchSize)

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

	// The update values follow the fetch values and their order corresponds to the order of ru.UpdateCols.
	numFetchCols := len(u.run.tu.ru.FetchCols)
	numUpdateCols := len(u.run.tu.ru.UpdateCols)
	updateValues := sourceVals[numFetchCols : numFetchCols+numUpdateCols]

	// Verify the schema constraints. For consistency with INSERT/UPSERT
	// and compatibility with PostgreSQL, we must do this before
	// processing the CHECK constraints.
	if err := enforceNotNullConstraints(updateValues, u.run.tu.ru.UpdateCols); err != nil {
		return err
	}

	// Run the CHECK constraints, if any. CheckHelper will either evaluate the
	// constraints itself, or else inspect boolean columns from the input that
	// contain the results of evaluation.
	if !u.run.checkOrds.Empty() {
		checkVals := sourceVals[len(u.run.tu.ru.FetchCols)+len(u.run.tu.ru.UpdateCols)+u.run.numPassthrough:]
		if err := checkMutationInput(
			params.ctx, params.EvalContext(), &params.p.semaCtx, params.p.SessionData(),
			u.run.tu.tableDesc(), u.run.checkOrds, checkVals,
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

	// Error out the update if the enforce_home_region session setting is on and
	// the row's locality doesn't match the gateway region.
	if err := u.run.regionLocalInfo.checkHomeRegion(updateValues); err != nil {
		return err
	}

	// Queue the insert in the KV batch.
	newValues, err := u.run.tu.rowForUpdate(params.ctx, oldValues, updateValues, pm, u.run.traceKV)
	if err != nil {
		return err
	}

	// If result rows need to be accumulated, do it.
	if u.run.tu.rows != nil {
		// The new values can include all columns,  so the values may contain
		// additional columns for every newly added column not yet visible. We do
		// not want them to be available for RETURNING.
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
	u.input.Close(ctx)
	u.run.tu.close(ctx)
	*u = updateNode{}
	updateNodePool.Put(u)
}

func (u *updateNode) rowsWritten() int64 {
	return u.run.tu.rowsWritten
}

func (u *updateNode) enableAutoCommit() {
	u.run.tu.enableAutoCommit()
}

// enforceNotNullConstraints enforces NOT NULL column constraints. row and cols
// must have the same length.
func enforceNotNullConstraints(row tree.Datums, cols []catalog.Column) error {
	if len(row) != len(cols) {
		return errors.AssertionFailedf("expected length of row (%d) and columns (%d) to match",
			len(row), len(cols))
	}
	for i, col := range cols {
		if !col.IsNullable() && row[i] == tree.DNull {
			return sqlerrors.NewNonNullViolationError(col.GetName())
		}
	}
	return nil
}
