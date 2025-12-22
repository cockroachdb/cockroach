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

	if err := d.run.td.init(params.ctx, params.p.txn, params.EvalContext(), params.p.stmt.WorkloadID); err != nil {
		return err
	}

	// Run the mutation to completion. DeleteSwap only processes one row, so no
	// need to loop.
	return d.processBatch(params)
}

// Next implements the planNode interface.
func (d *deleteSwapNode) Next(_ runParams) (bool, error) {
	return d.run.next(), nil
}

// Values implements the planNode interface.
func (d *deleteSwapNode) Values() tree.Datums {
	return d.run.values()
}

func (d *deleteSwapNode) processBatch(params runParams) error {
	// Delete swap does everything in one batch. There should only be a single row
	// of input, to ensure the savepoint rollback below has the correct SQL
	// semantics.

	if err := params.p.cancelChecker.Check(); err != nil {
		return err
	}

	next, err := d.input.Next(params)
	if next {
		if err := d.run.processSourceRow(params, d.input.Values()); err != nil {
			return err
		}
		// Verify that there was only a single row of input.
		next, err = d.input.Next(params)
		if next {
			return errors.AssertionFailedf("expected only 1 row as input to delete swap")
		}
	}
	if err != nil {
		return err
	}

	// Delete swap works by optimistically modifying every index in the same
	// batch. If the row does not actually exist, the write to the primary index
	// will fail with ConditionFailedError, but writes to some secondary indexes
	// might succeed. We use a savepoint here to undo those writes.
	sp, err := d.run.td.createSavepoint(params.ctx)
	if err != nil {
		return err
	}

	d.run.td.setRowsWrittenLimit(params.extendedEvalCtx.SessionData())
	if err := d.run.td.finalize(params.ctx); err != nil {
		// If this was a ConditionFailedError, it means the row did not exist in the
		// primary index. We must roll back to the savepoint above to undo writes to
		// all secondary indexes.
		if condErr := (*kvpb.ConditionFailedError)(nil); errors.As(err, &condErr) {
			// Reset the table writer so that it looks like there were no rows to
			// delete.
			d.run.reset(params.ctx)
			if err := d.run.td.rollbackToSavepoint(params.ctx, sp); err != nil {
				return err
			}
			return nil
		}
		return err
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(params.ctx, d.run.td.tableDesc(), int(d.run.rowsAffected()))

	return nil
}

func (d *deleteSwapNode) Close(ctx context.Context) {
	d.input.Close(ctx)
	d.run.close(ctx)
	*d = deleteSwapNode{}
	deleteSwapNodePool.Put(d)
}

func (d *deleteSwapNode) rowsWritten() int64 {
	return d.run.rowsAffected()
}

func (d *deleteSwapNode) indexRowsWritten() int64 {
	return d.run.td.indexRowsWritten
}

func (d *deleteSwapNode) indexBytesWritten() int64 {
	// No bytes counted as written for a deletion.
	return 0
}

func (d *deleteSwapNode) returnsRowsAffected() bool {
	return !d.run.rowsNeeded
}

func (d *deleteSwapNode) kvCPUTime() int64 {
	return d.run.td.kvCPUTime
}

func (d *deleteSwapNode) enableAutoCommit() {
	d.run.td.enableAutoCommit()
}
