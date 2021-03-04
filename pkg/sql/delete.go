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
	source planNode

	// columns is set if this DELETE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run deleteRun
}

// deleteRun contains the run-time state of deleteNode during local execution.
type deleteRun struct {
	td         tableDeleter
	rowsNeeded bool

	// done informs a new call to BatchedNext() that the previous call
	// to BatchedNext() has completed the work already.
	done bool

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

func (d *deleteNode) startExec(params runParams) error {
	// cache traceKV during execution, to avoid re-evaluating it for every row.
	d.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	if d.run.rowsNeeded {
		d.run.td.rows = rowcontainer.NewRowContainer(
			params.EvalContext().Mon.MakeBoundAccount(),
			colinfo.ColTypeInfoFromResCols(d.columns))
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

	// Advance one batch. First, clear the last batch.
	d.run.td.clearLastBatch(params.ctx)
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

		// Are we done yet with the current batch?
		if d.run.td.currentBatchSize >= d.run.td.maxBatchSize {
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
		if err := d.run.td.finalize(params.ctx); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		d.run.done = true
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(
		d.run.td.tableDesc().GetID(),
		d.run.td.lastBatchSize,
	)

	return d.run.td.lastBatchSize > 0, nil
}

// processSourceRow processes one row from the source for deletion and, if
// result rows are needed, saves it in the result row container
func (d *deleteNode) processSourceRow(params runParams, sourceVals tree.Datums) error {
	// Create a set of partial index IDs to not delete from. Indexes should not
	// be deleted from when they are partial indexes and the row does not
	// satisfy the predicate and therefore do not exist in the partial index.
	// This set is passed as a argument to tableDeleter.row below.
	var pm row.PartialIndexUpdateHelper
	if n := len(d.run.td.tableDesc().PartialIndexes()); n > 0 {
		offset := d.run.partialIndexDelValsOffset
		partialIndexDelVals := sourceVals[offset : offset+n]

		err := pm.Init(tree.Datums{}, partialIndexDelVals, d.run.td.tableDesc())
		if err != nil {
			return err
		}

		// Truncate sourceVals so that it no longer includes partial index
		// predicate values.
		sourceVals = sourceVals[:d.run.partialIndexDelValsOffset]
	}

	// Queue the deletion in the KV batch.
	if err := d.run.td.row(params.ctx, sourceVals, pm, d.run.traceKV); err != nil {
		return err
	}

	// If result rows need to be accumulated, do it.
	if d.run.td.rows != nil {
		// The new values can include all columns, the construction of the
		// values has used execinfra.ScanVisibilityPublicAndNotPublic so the
		// values may contain additional columns for every newly dropped column
		// not visible. We do not want them to be available for RETURNING.
		//
		// d.run.rows.NumCols() is guaranteed to only contain the requested
		// public columns.
		resultValues := make(tree.Datums, d.run.td.rows.NumCols())
		for i, retIdx := range d.run.rowIdxToRetIdx {
			if retIdx >= 0 {
				resultValues[retIdx] = sourceVals[i]
			}
		}

		if _, err := d.run.td.rows.AddRow(params.ctx, resultValues); err != nil {
			return err
		}
	}

	return nil
}

// BatchedCount implements the batchedPlanNode interface.
func (d *deleteNode) BatchedCount() int { return d.run.td.lastBatchSize }

// BatchedCount implements the batchedPlanNode interface.
func (d *deleteNode) BatchedValues(rowIdx int) tree.Datums { return d.run.td.rows.At(rowIdx) }

func (d *deleteNode) Close(ctx context.Context) {
	d.source.Close(ctx)
	d.run.td.close(ctx)
	*d = deleteNode{}
	deleteNodePool.Put(d)
}

func (d *deleteNode) enableAutoCommit() {
	d.run.td.enableAutoCommit()
}
