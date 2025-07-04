// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var deleteNodePool = sync.Pool{
	New: func() interface{} {
		return &deleteNode{}
	},
}

type deleteNode struct {
	singleInputPlanNode

	// columns is set if this DELETE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run deleteRun
}

var _ mutationPlanNode = &deleteNode{}

// deleteRun contains the run-time state of deleteNode during local execution.
type deleteRun struct {
	td         tableDeleter
	rowsNeeded bool

	// done informs a new call to BatchedNext() that the previous call
	// to BatchedNext() has completed the work already.
	done bool

	// resultRowBuffer is used to prepare a result row for accumulation
	// into the row container above, when rowsNeeded is set.
	resultRowBuffer tree.Datums

	// traceKV caches the current KV tracing flag.
	traceKV bool

	// rowIdxToRetIdx is the mapping from the columns returned by the deleter
	// to the columns in the resultRowBuffer. A value of -1 is used to indicate
	// that the column at that index is not part of the resultRowBuffer
	// of the mutation. Otherwise, the value at the i-th index refers to the
	// index of the resultRowBuffer where the i-th column is to be returned.
	rowIdxToRetIdx []int

	// numPassthrough is the number of columns in addition to the set of columns
	// of the target table being returned, that must be passed through from the
	// input node.
	numPassthrough int

	mustValidateOldPKValues bool

	originTimestampCPutHelper row.OriginTimestampCPutHelper
}

func (r *deleteRun) init(params runParams, columns colinfo.ResultColumns) {
	if ots := params.extendedEvalCtx.SessionData().OriginTimestampForLogicalDataReplication; ots.IsSet() {
		r.originTimestampCPutHelper.OriginTimestamp = ots
	}

	if !r.rowsNeeded {
		return
	}

	r.td.rows = rowcontainer.NewRowContainer(
		params.p.Mon().MakeBoundAccount(),
		colinfo.ColTypeInfoFromResCols(columns),
	)
	r.resultRowBuffer = make([]tree.Datum, len(columns))
	for i := range r.resultRowBuffer {
		r.resultRowBuffer[i] = tree.DNull
	}
}

func (d *deleteNode) startExec(params runParams) error {
	// cache traceKV during execution, to avoid re-evaluating it for every row.
	d.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	d.run.init(params, d.columns)

	return d.run.td.init(params.ctx, params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (d *deleteNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (d *deleteNode) Values() tree.Datums { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (d *deleteNode) BatchedNext(params runParams) (bool, error) {
	if d.run.done {
		return false, nil
	}

	// Advance one batch. First, clear the last batch.
	d.run.td.clearLastBatch(params.ctx)
	// Now consume/accumulate the rows for this batch.
	lastBatch := false
	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Advance one individual row.
		if next, err := d.input.Next(params); !next {
			lastBatch = true
			if err != nil {
				return false, err
			}
			break
		}

		// Process the deletion of the current input row,
		// potentially accumulating the result row for later.
		if err := d.run.processSourceRow(params, d.input.Values()); err != nil {
			return false, err
		}

		// Are we done yet with the current batch?
		if d.run.td.currentBatchSize >= d.run.td.maxBatchSize ||
			d.run.td.b.ApproximateMutationBytes() >= d.run.td.maxBatchByteSize {
			break
		}
	}

	if d.run.td.currentBatchSize > 0 {
		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err := d.run.td.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		d.run.td.setRowsWrittenLimit(params.extendedEvalCtx.SessionData())
		if err := d.run.td.finalize(params.ctx); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		d.run.done = true
		// Possibly initiate a run of CREATE STATISTICS.
		params.ExecCfg().StatsRefresher.NotifyMutation(d.run.td.tableDesc(), int(d.run.td.rowsWritten))
	}

	return d.run.td.lastBatchSize > 0, nil
}

// processSourceRow processes one row from the source for deletion and, if
// result rows are needed, saves it in the result row container
func (r *deleteRun) processSourceRow(params runParams, sourceVals tree.Datums) error {
	// Remove extra columns for partial index predicate values and AFTER triggers.
	deleteVals := sourceVals[:len(r.td.rd.FetchCols)+r.numPassthrough]
	sourceVals = sourceVals[len(deleteVals):]

	// Create a set of partial index IDs to not delete from. Indexes should not
	// be deleted from when they are partial indexes and the row does not
	// satisfy the predicate and therefore do not exist in the partial index.
	// This set is passed as a argument to tableDeleter.row below.
	var pm row.PartialIndexUpdateHelper
	if n := len(r.td.tableDesc().PartialIndexes()); n > 0 {
		err := pm.Init(nil /* partialIndexPutVals */, sourceVals[:n], r.td.tableDesc())
		if err != nil {
			return err
		}
		sourceVals = sourceVals[n:]
	}

	// Keep track of the vector index partitions to update. This information is
	// passed to tableInserter.row below.
	var vh row.VectorIndexUpdateHelper
	if n := len(r.td.tableDesc().VectorIndexes()); n > 0 {
		vh.InitForDel(sourceVals[:n], r.td.tableDesc())
	}

	// Queue the deletion in the KV batch.
	if err := r.td.row(
		params.ctx, deleteVals, pm, vh, r.originTimestampCPutHelper, r.mustValidateOldPKValues, r.traceKV,
	); err != nil {
		return err
	}

	// If result rows need to be accumulated, do it.
	if r.td.rows != nil {
		// The new values can include all columns, so the values may contain
		// additional columns for every newly dropped column not visible. We do not
		// want them to be available for RETURNING.
		//
		// r.rows.NumCols() is guaranteed to only contain the requested
		// public columns.
		largestRetIdx := -1
		for i := range r.rowIdxToRetIdx {
			retIdx := r.rowIdxToRetIdx[i]
			if retIdx >= 0 {
				if retIdx >= largestRetIdx {
					largestRetIdx = retIdx
				}
				r.resultRowBuffer[retIdx] = deleteVals[i]
			}
		}

		// At this point we've extracted all the RETURNING values that are part
		// of the target table. We must now extract the columns in the RETURNING
		// clause that refer to other tables (from the USING clause of the delete).
		if r.numPassthrough > 0 {
			passthroughBegin := len(r.td.rd.FetchCols)
			passthroughEnd := passthroughBegin + r.numPassthrough
			passthroughValues := deleteVals[passthroughBegin:passthroughEnd]

			for i := 0; i < r.numPassthrough; i++ {
				largestRetIdx++
				r.resultRowBuffer[largestRetIdx] = passthroughValues[i]
			}

		}

		if _, err := r.td.rows.AddRow(params.ctx, r.resultRowBuffer); err != nil {
			return err
		}
	}

	return nil
}

// BatchedCount implements the batchedPlanNode interface.
func (d *deleteNode) BatchedCount() int { return d.run.td.lastBatchSize }

// BatchedValues implements the batchedPlanNode interface.
func (d *deleteNode) BatchedValues(rowIdx int) tree.Datums { return d.run.td.rows.At(rowIdx) }

func (d *deleteNode) Close(ctx context.Context) {
	d.input.Close(ctx)
	d.run.td.close(ctx)
	*d = deleteNode{}
	deleteNodePool.Put(d)
}

func (d *deleteNode) rowsWritten() int64 {
	return d.run.td.rowsWritten
}

func (d *deleteNode) enableAutoCommit() {
	d.run.td.enableAutoCommit()
}
