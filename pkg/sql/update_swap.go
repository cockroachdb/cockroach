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

var updateSwapNodePool = sync.Pool{
	New: func() interface{} {
		return &updateSwapNode{}
	},
}

type updateSwapNode struct {
	// Unlike insertFastPathNode, updateSwapNode reads from input in order to
	// support projections, which are used by most UPDATE statements.
	singleInputPlanNode

	// columns is set if this UPDATE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run updateRun
}

var _ mutationPlanNode = &updateSwapNode{}

func (u *updateSwapNode) startExec(params runParams) error {
	// Cache traceKV during execution, to avoid re-evaluating it for every row.
	u.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	u.run.mustValidateOldPKValues = true

	u.run.init(params, u.columns)

	return u.run.tu.init(params.ctx, params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (u *updateSwapNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (u *updateSwapNode) Values() tree.Datums { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (u *updateSwapNode) BatchedNext(params runParams) (bool, error) {
	if u.run.done {
		return false, nil
	}

	// Update-swap does everything in one batch. There should only be a single row
	// of input, to ensure the savepoint rollback below has the correct SQL
	// semantics.

	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}

	next, err := u.input.Next(params)
	if next {
		if err := u.run.processSourceRow(params, u.input.Values()); err != nil {
			return false, err
		}
		// Verify that there was only a single row of input.
		next, err = u.input.Next(params)
		if next {
			return false, errors.AssertionFailedf("expected only 1 row as input to update swap")
		}
	}
	if err != nil {
		return false, err
	}

	// Update swap works by optimistically modifying every index in the same
	// batch. If the row does not actually exist, the write to the primary index
	// will fail with ConditionFailedError, but writes to some secondary indexes
	// might succeed. We use a savepoint here to undo those writes.
	sp, err := u.run.tu.createSavepoint(params.ctx)
	if err != nil {
		return false, err
	}

	u.run.tu.setRowsWrittenLimit(params.extendedEvalCtx.SessionData())
	if err := u.run.tu.finalize(params.ctx); err != nil {
		// If this was a ConditionFailedError, it means the row did not exist in the
		// primary index. We must roll back to the savepoint above to undo writes to
		// all secondary indexes.
		if condErr := (*kvpb.ConditionFailedError)(nil); errors.As(err, &condErr) {
			// Reset the table writer so that it looks like there were no rows to
			// update.
			u.run.tu.rowsWritten = 0
			u.run.tu.clearLastBatch(params.ctx)
			if err := u.run.tu.rollbackToSavepoint(params.ctx, sp); err != nil {
				return false, err
			}
			return false, nil
		}
		return false, err
	}

	// Remember we're done for the next call to BatchedNext().
	u.run.done = true

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(u.run.tu.tableDesc(), u.run.tu.lastBatchSize)

	return u.run.tu.lastBatchSize > 0, nil
}

// BatchedCount implements the batchedPlanNode interface.
func (u *updateSwapNode) BatchedCount() int {
	return u.run.tu.lastBatchSize
}

// BatchedValues implements the batchedPlanNode interface.
func (u *updateSwapNode) BatchedValues(rowIdx int) tree.Datums {
	return u.run.tu.rows.At(rowIdx)
}

func (u *updateSwapNode) Close(ctx context.Context) {
	u.input.Close(ctx)
	u.run.tu.close(ctx)
	*u = updateSwapNode{}
	updateSwapNodePool.Put(u)
}

func (u *updateSwapNode) rowsWritten() int64 {
	return u.run.tu.rowsWritten
}

func (u *updateSwapNode) enableAutoCommit() {
	u.run.tu.enableAutoCommit()
}
