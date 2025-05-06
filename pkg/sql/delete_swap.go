// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

var deleteSwapNodePool = sync.Pool{
	New: func() interface{} {
		return &deleteSwapNode{}
	},
}

type deleteSwapNode struct {
	// Unlike insertFastPathNode, deleteSwapNode reads from input in order to
	// support projections, which are used by some DELETE statements.
	singleInputPlanNode

	// columns is set if this DELETE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run deleteRun
}

var _ mutationPlanNode = &deleteSwapNode{}

func (d *deleteSwapNode) startExec(params runParams) error {
	// Cache traceKV during execution, to avoid re-evaluating it for every row.
	d.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	d.run.mustValidateOldPKValues = true

	d.run.init(params, d.columns)

	return d.run.td.init(params.ctx, params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (d *deleteSwapNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (d *deleteSwapNode) Values() tree.Datums { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (d *deleteSwapNode) BatchedNext(params runParams) (bool, error) {
	if d.run.done {
		return false, nil
	}

	// Delete swap does everything in one batch. There should only be a single row
	// of input, to ensure the savepoint rollback below has the correct SQL
	// semantics.

	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}

	next, err := d.input.Next(params)
	if next {
		if err := d.run.processSourceRow(params, d.input.Values()); err != nil {
			return false, err
		}
		// Verify that there was only a single row of input.
		next, err = d.input.Next(params)
		if next {
			return false, errors.AssertionFailedf("expected only 1 row as input to delete swap")
		}
	}
	if err != nil {
		return false, err
	}

	// Delete swap works by optimistically modifying every index in the same
	// batch. If the row does not actually exist, the write to the primary index
	// will fail with ConditionFailedError, but writes to some secondary indexes
	// might succeed. We use a savepoint here to undo those writes.
	sp, err := d.run.td.createSavepoint(params.ctx)
	if err != nil {
		return false, err
	}

	d.run.td.setRowsWrittenLimit(params.extendedEvalCtx.SessionData())
	if err := d.run.td.finalize(params.ctx); err != nil {
		// If this was a ConditionFailedError, it means the row did not exist in the
		// primary index. We must roll back to the savepoint above to undo writes to
		// all secondary indexes.
		if condErr := (*kvpb.ConditionFailedError)(nil); errors.As(err, &condErr) {
			// Reset the table writer so that it looks like there were no rows to
			// delete.
			d.run.td.rowsWritten = 0
			d.run.td.clearLastBatch(params.ctx)
			if err := d.run.td.rollbackToSavepoint(params.ctx, sp); err != nil {
				return false, err
			}
			return false, nil
		}
		return false, err
	}

	// Remember we're done for the next call to BatchedNext().
	d.run.done = true

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(d.run.td.tableDesc(), d.run.td.lastBatchSize)

	return d.run.td.lastBatchSize > 0, nil
}

// BatchedCount implements the batchedPlanNode interface.
func (d *deleteSwapNode) BatchedCount() int {
	return d.run.td.lastBatchSize
}

// BatchedValues implements the batchedPlanNode interface.
func (d *deleteSwapNode) BatchedValues(rowIdx int) tree.Datums {
	return d.run.td.rows.At(rowIdx)
}

func (d *deleteSwapNode) Close(ctx context.Context) {
	d.run.td.close(ctx)
	*d = deleteSwapNode{}
	deleteSwapNodePool.Put(d)
}

func (d *deleteSwapNode) rowsWritten() int64 {
	return d.run.td.rowsWritten
}
