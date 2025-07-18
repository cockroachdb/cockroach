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
	mutationOutputHelper
	tu tableUpdater

	checkOrds checkSet

	// resultRowBuffer is used to prepare a result row for accumulation
	// into the row container above, when rowsNeeded is set.
	resultRowBuffer tree.Datums

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

	mustValidateOldPKValues bool

	originTimestampCPutHelper row.OriginTimestampCPutHelper
}

func (r *updateRun) init(params runParams, columns colinfo.ResultColumns) {
	if ots := params.extendedEvalCtx.SessionData().OriginTimestampForLogicalDataReplication; ots.IsSet() {
		r.originTimestampCPutHelper.OriginTimestamp = ots
	}

	if !r.rowsNeeded {
		return
	}
	r.rows = rowcontainer.NewRowContainer(
		params.p.Mon().MakeBoundAccount(),
		colinfo.ColTypeInfoFromResCols(columns),
	)
	r.resultRowBuffer = make([]tree.Datum, len(columns))
	for i := range r.resultRowBuffer {
		r.resultRowBuffer[i] = tree.DNull
	}
}

func (u *updateNode) startExec(params runParams) error {
	// cache traceKV during execution, to avoid re-evaluating it for every row.
	u.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	u.run.init(params, u.columns)

	if err := u.run.tu.init(params.ctx, params.p.txn, params.EvalContext()); err != nil {
		return err
	}

	// Run the mutation to completion.
	for {
		lastBatch, err := u.processBatch(params)
		if err != nil || lastBatch {
			return err
		}
	}
}

// Next implements the planNode interface.
func (u *updateNode) Next(_ runParams) (bool, error) {
	return u.run.next(), nil
}

// Values implements the planNode interface.
func (u *updateNode) Values() tree.Datums {
	return u.run.values()
}

func (u *updateNode) processBatch(params runParams) (lastBatch bool, err error) {
	// Consume/accumulate the rows for this batch.
	lastBatch = false
	for {
		if err = params.p.cancelChecker.Check(); err != nil {
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
		if err = u.run.processSourceRow(params, u.input.Values()); err != nil {
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
			if err = u.run.tu.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		u.run.tu.setRowsWrittenLimit(params.extendedEvalCtx.SessionData())
		if err = u.run.tu.finalize(params.ctx); err != nil {
			return false, err
		}
		// Possibly initiate a run of CREATE STATISTICS.
		params.ExecCfg().StatsRefresher.NotifyMutation(u.run.tu.tableDesc(), int(u.run.modifiedRowCount()))
	}
	return lastBatch, nil
}

// processSourceRow processes one row from the source for update and, if
// result rows are needed, saves it in the result row container.
func (r *updateRun) processSourceRow(params runParams, sourceVals tree.Datums) error {
	// sourceVals contains values for the columns from the table, in the order of the
	// table descriptor. (One per column in u.tw.ru.FetchCols)
	//
	// And then after that, all the extra expressions potentially added via
	// a renderNode for the RHS of the assignments.

	// oldValues is the prefix of sourceVals that corresponds to real
	// stored columns in the table, that is, excluding the RHS assignment
	// expressions.
	oldValues := sourceVals[:len(r.tu.ru.FetchCols)]
	sourceVals = sourceVals[len(oldValues):]

	// The update values follow the fetch values and their order corresponds to the order of ru.UpdateCols.
	updateValues := sourceVals[:len(r.tu.ru.UpdateCols)]
	sourceVals = sourceVals[len(updateValues):]

	// The passthrough values follow the update values.
	passthroughValues := sourceVals[:r.numPassthrough]
	sourceVals = sourceVals[len(passthroughValues):]

	// Verify the schema constraints. For consistency with INSERT/UPSERT
	// and compatibility with PostgreSQL, we must do this before
	// processing the CHECK constraints.
	if err := enforceNotNullConstraints(updateValues, r.tu.ru.UpdateCols); err != nil {
		return err
	}

	// Run the CHECK constraints, if any. CheckHelper will either evaluate the
	// constraints itself, or else inspect boolean columns from the input that
	// contain the results of evaluation.
	if !r.checkOrds.Empty() {
		if err := checkMutationInput(
			params.ctx, params.EvalContext(), &params.p.semaCtx, params.p.SessionData(),
			r.tu.tableDesc(), r.checkOrds, sourceVals[:r.checkOrds.Len()],
		); err != nil {
			return err
		}
		sourceVals = sourceVals[r.checkOrds.Len():]
	}

	// Create a set of partial index IDs to not add entries or remove entries
	// from. Put values are followed by del values.
	var pm row.PartialIndexUpdateHelper
	if n := len(r.tu.tableDesc().PartialIndexes()); n > 0 {
		err := pm.Init(sourceVals[:n], sourceVals[n:n*2], r.tu.tableDesc())
		if err != nil {
			return err
		}
		sourceVals = sourceVals[n*2:]
	}

	// Keep track of the vector index partitions to update, as well as the
	// quantized vectors. This information is passed to tableInserter.row below.
	// Order of column values is put partitions, quantized vectors, followed by
	// del partitions
	var vh row.VectorIndexUpdateHelper
	if n := len(r.tu.tableDesc().VectorIndexes()); n > 0 {
		vh.InitForPut(sourceVals[:n], sourceVals[n:n*2], r.tu.tableDesc())
		vh.InitForDel(sourceVals[n*2:n*3], r.tu.tableDesc())
	}

	// Error out the update if the enforce_home_region session setting is on and
	// the row's locality doesn't match the gateway region.
	if err := r.regionLocalInfo.checkHomeRegion(updateValues); err != nil {
		return err
	}

	// Queue the insert in the KV batch.
	newValues, err := r.tu.rowForUpdate(
		params.ctx, oldValues, updateValues, pm, vh, r.originTimestampCPutHelper, r.mustValidateOldPKValues, r.traceKV,
	)
	if err != nil {
		return err
	}
	// NOTE: we intentionally do not increment the modified row count if there
	// was an error. UpdateSwap can swallow a ConditionFailedError and no-op, in
	// which case the count should be left at 0.
	r.onModifiedRow()

	if !r.rowsNeeded {
		return nil
	}

	// Result rows must be accumulated.
	//
	// The new values can include all columns,  so the values may contain
	// additional columns for every newly added column not yet visible. We do
	// not want them to be available for RETURNING.
	//
	// MakeUpdater guarantees that the first columns of the new values
	// are those specified u.columns.
	largestRetIdx := -1
	for i := range r.rowIdxToRetIdx {
		retIdx := r.rowIdxToRetIdx[i]
		if retIdx >= 0 {
			if retIdx >= largestRetIdx {
				largestRetIdx = retIdx
			}
			r.resultRowBuffer[retIdx] = newValues[i]
		}
	}

	// At this point we've extracted all the RETURNING values that are part
	// of the target table. We must now extract the columns in the RETURNING
	// clause that refer to other tables (from the FROM clause of the update).
	for i := 0; i < r.numPassthrough; i++ {
		largestRetIdx++
		r.resultRowBuffer[largestRetIdx] = passthroughValues[i]
	}

	return r.addRow(params.ctx, r.resultRowBuffer)
}

func (u *updateNode) Close(ctx context.Context) {
	u.input.Close(ctx)
	u.run.close(ctx)
	*u = updateNode{}
	updateNodePool.Put(u)
}

func (u *updateNode) rowsWritten() int64 {
	return u.run.modifiedRowCount()
}

func (u *updateNode) returnsRowsAffected() bool {
	return !u.run.rowsNeeded
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
