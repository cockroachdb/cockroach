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

	if err := u.run.tu.init(params.ctx, params.p.txn, params.EvalContext(), params.p.stmt.WorkloadID); err != nil {
		return err
	}

	// Run the mutation to completion. UpdateSwap only processes one row, so no
	// need to loop.
	return u.processBatch(params)
}

// Next implements the planNode interface.
func (u *updateSwapNode) Next(_ runParams) (bool, error) {
	return u.run.next(), nil
}

// Values implements the planNode interface.
func (u *updateSwapNode) Values() tree.Datums {
	return u.run.values()
}

func (u *updateSwapNode) processBatch(params runParams) error {
	// Update-swap does everything in one batch. There should only be a single row
	// of input, to ensure the savepoint rollback below has the correct SQL
	// semantics.

	if err := params.p.cancelChecker.Check(); err != nil {
		return err
	}

	next, err := u.input.Next(params)
	if next {
		if err = u.run.processSourceRow(params, u.input.Values()); err != nil {
			return err
		}
		// Verify that there was only a single row of input.
		next, err = u.input.Next(params)
		if next {
			return errors.AssertionFailedf("expected only 1 row as input to update swap")
		}
	}
	if err != nil {
		return err
	}

	// Update swap works by optimistically modifying every index in the same
	// batch. If the row does not actually exist, the write to the primary index
	// will fail with ConditionFailedError, but writes to some secondary indexes
	// might succeed. We use a savepoint here to undo those writes.
	sp, err := u.run.tu.createSavepoint(params.ctx)
	if err != nil {
		return err
	}

	u.run.tu.setRowsWrittenLimit(params.extendedEvalCtx.SessionData())
	if err = u.run.tu.finalize(params.ctx); err != nil {
		// If this was a ConditionFailedError, it means the row did not exist in the
		// primary index. We must roll back to the savepoint above to undo writes to
		// all secondary indexes.
		if condErr := (*kvpb.ConditionFailedError)(nil); errors.As(err, &condErr) {
			// Reset the table writer so that it looks like there were no rows to
			// update.
			u.run.reset(params.ctx)
			if err = u.run.tu.rollbackToSavepoint(params.ctx, sp); err != nil {
				return err
			}
			return nil
		}
		return err
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(params.ctx, u.run.tu.tableDesc(), int(u.run.rowsAffected()))

	return nil
}

func (u *updateSwapNode) Close(ctx context.Context) {
	u.input.Close(ctx)
	u.run.close(ctx)
	*u = updateSwapNode{}
	updateSwapNodePool.Put(u)
}

func (u *updateSwapNode) rowsWritten() int64 {
	return u.run.rowsAffected()
}

func (u *updateSwapNode) indexRowsWritten() int64 {
	return u.run.tu.indexRowsWritten
}

func (u *updateSwapNode) indexBytesWritten() int64 {
	return u.run.tu.indexBytesWritten
}

func (u *updateSwapNode) returnsRowsAffected() bool {
	return !u.run.rowsNeeded
}

func (u *updateSwapNode) kvCPUTime() int64 {
	return u.run.tu.kvCPUTime
}

func (u *updateSwapNode) enableAutoCommit() {
	u.run.tu.enableAutoCommit()
}
