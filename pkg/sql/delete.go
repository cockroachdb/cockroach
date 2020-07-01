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

	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var deleteNodePool = sync.Pool{
	New: func() interface{} {
		return &deleteNode{}
	},
}

type deleteNode struct {
	source planNode

	// columns is set if this DELETE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns sqlbase.ResultColumns

	run deleteRun
}

// deleteRun contains the run-time state of deleteNode during local execution.
type deleteRun struct {
	td         tableDeleter
	rowsNeeded bool

	// rowCount is the number of rows in the current batch.
	rowCount int

	// done informs a new call to BatchedNext() that the previous call
	// to BatchedNext() has completed the work already.
	done bool

	// rows contains the accumulated result rows if rowsNeeded is set.
	rows *rowcontainer.RowContainer

	// traceKV caches the current KV tracing flag.
	traceKV bool

	// partialIndexDelValsOffset is the offset of partial index delete
	// indicators in the source values. It is equal to the number of fetched
	// columns.
	partialIndexDelValsOffset int

	// rowIdxToRetIdx is the mapping from the columns returned by the deleter
	// to the columns in the resultRowBuffer. A value of -1 is used to indicate
	// that the column at that index is not part of the resultRowBuffer
	// of the mutation. Otherwise, the value at the i-th index refers to the
	// index of the resultRowBuffer where the i-th column is to be returned.
	rowIdxToRetIdx []int
}

// maxDeleteBatchSize is the max number of entries in the KV batch for
// the delete operation (including secondary index updates, FK
// cascading updates, etc), before the current KV batch is executed
// and a new batch is started.
const maxDeleteBatchSize = 10000

func (d *deleteNode) startExec(params runParams) error {
	// cache traceKV during execution, to avoid re-evaluating it for every row.
	d.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	if d.run.rowsNeeded {
		d.run.rows = rowcontainer.NewRowContainer(
			params.EvalContext().Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(d.columns), 0)
	}
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

	tracing.AnnotateTrace()

	// Advance one batch. First, clear the current batch.
	d.run.rowCount = 0
	if d.run.rows != nil {
		d.run.rows.Clear(params.ctx)
	}
	// Now consume/accumulate the rows for this batch.
	lastBatch := false
	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Advance one individual row.
		if next, err := d.source.Next(params); !next {
			lastBatch = true
			if err != nil {
				return false, err
			}
			break
		}

		// Process the deletion of the current source row,
		// potentially accumulating the result row for later.
		if err := d.processSourceRow(params, d.source.Values()); err != nil {
			return false, err
		}

		d.run.rowCount++

		// Are we done yet with the current batch?
		if d.run.td.curBatchSize() >= maxDeleteBatchSize {
			break
		}
	}

	if d.run.rowCount > 0 {
		if err := d.run.td.atBatchEnd(params.ctx, d.run.traceKV); err != nil {
			return false, err
		}

		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err := d.run.td.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		if _, err := d.run.td.finalize(params.ctx, d.run.traceKV); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		d.run.done = true
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(
		d.run.td.tableDesc().ID,
		d.run.rowCount,
	)

	return d.run.rowCount > 0, nil
}

// processSourceRow processes one row from the source for deletion and, if
// result rows are needed, saves it in the result row container
func (d *deleteNode) processSourceRow(params runParams, sourceVals tree.Datums) error {
	// Create a set of index IDs to not delete from. Indexes should not be
	// deleted from when they are partial indexes and the row does not satisfy
	// the predicate and therefore do not exist in the partial index. This
	// set is passed as a argument to tableDeleter.row below.
	var ignoreIndexes util.FastIntSet
	partialIndexDelVals := sourceVals[d.run.partialIndexDelValsOffset:]
	colIdx := 0
	indexes := d.run.td.tableDesc().Indexes
	for i := range indexes {
		index := indexes[i]
		if index.IsPartial() {
			val, err := tree.GetBool(partialIndexDelVals[colIdx])
			if err != nil {
				return err
			}

			if !val {
				// If the value of the column for the index predicate expression
				// is false, the row should not be removed from the partial
				// index.
				ignoreIndexes.Add(int(index.ID))
			}

			colIdx++
			if colIdx >= len(partialIndexDelVals) {
				break
			}
		}
	}

	// Queue the deletion in the KV batch.
	if err := d.run.td.row(params.ctx, sourceVals, ignoreIndexes, d.run.traceKV); err != nil {
		return err
	}

	// If result rows need to be accumulated, do it.
	if d.run.rows != nil {
		// The new values can include all columns, the construction of the
		// values has used execinfra.ScanVisibilityPublicAndNotPublic so the
		// values may contain additional columns for every newly dropped column
		// not visible. We do not want them to be available for RETURNING.
		//
		// d.run.rows.NumCols() is guaranteed to only contain the requested
		// public columns.
		resultValues := make(tree.Datums, d.run.rows.NumCols())
		for i, retIdx := range d.run.rowIdxToRetIdx {
			if retIdx >= 0 {
				resultValues[retIdx] = sourceVals[i]
			}
		}

		if _, err := d.run.rows.AddRow(params.ctx, resultValues); err != nil {
			return err
		}
	}

	return nil
}

// BatchedCount implements the batchedPlanNode interface.
func (d *deleteNode) BatchedCount() int { return d.run.rowCount }

// BatchedCount implements the batchedPlanNode interface.
func (d *deleteNode) BatchedValues(rowIdx int) tree.Datums { return d.run.rows.At(rowIdx) }

func (d *deleteNode) Close(ctx context.Context) {
	d.source.Close(ctx)
	if d.run.rows != nil {
		d.run.rows.Close(ctx)
		d.run.rows = nil
	}
	d.run.td.close(ctx)
	*d = deleteNode{}
	deleteNodePool.Put(d)
}

func (d *deleteNode) enableAutoCommit() {
	d.run.td.enableAutoCommit()
}
