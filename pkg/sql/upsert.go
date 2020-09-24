// Copyright 2016 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var upsertNodePool = sync.Pool{
	New: func() interface{} {
		return &upsertNode{}
	},
}

type upsertNode struct {
	source planNode

	// columns is set if this UPDATE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns sqlbase.ResultColumns

	run upsertRun
}

// upsertRun contains the run-time state of upsertNode during local execution.
type upsertRun struct {
	tw        optTableUpserter
	checkOrds checkSet

	// insertCols are the columns being inserted/upserted into.
	insertCols []sqlbase.ColumnDescriptor

	// done informs a new call to BatchedNext() that the previous call to
	// BatchedNext() has completed the work already.
	done bool

	// traceKV caches the current KV tracing flag.
	traceKV bool
}

func (n *upsertNode) startExec(params runParams) error {
	// cache traceKV during execution, to avoid re-evaluating it for every row.
	n.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	return n.run.tw.init(params.ctx, params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (n *upsertNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (n *upsertNode) Values() tree.Datums { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (n *upsertNode) BatchedNext(params runParams) (bool, error) {
	if n.run.done {
		return false, nil
	}

	tracing.AnnotateTrace()

	// Advance one batch. First, clear the current batch.
	if n.run.tw.collectRows {
		n.run.tw.rowsUpserted.Clear(params.ctx)
	}

	// Now consume/accumulate the rows for this batch.
	lastBatch := false
	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Advance one individual row.
		if next, err := n.source.Next(params); !next {
			lastBatch = true
			if err != nil {
				return false, err
			}
			break
		}

		// Process the insertion for the current source row, potentially
		// accumulating the result row for later.
		if err := n.processSourceRow(params, n.source.Values()); err != nil {
			return false, err
		}

		// Are we done yet with the current batch?
		if n.run.tw.curBatchSize() >= n.run.tw.maxBatchSize {
			break
		}
	}

	// In Upsert, curBatchSize indicates whether "there is still work to do in this batch".
	batchSize := n.run.tw.curBatchSize()

	if batchSize > 0 {
		if err := n.run.tw.atBatchEnd(params.ctx, n.run.traceKV); err != nil {
			return false, err
		}

		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err := n.run.tw.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		if _, err := n.run.tw.finalize(params.ctx, n.run.traceKV); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		n.run.done = true
	}

	// We've just finished processing this batch, and we need to remember how
	// many rows were in it.
	n.run.tw.rowsInLastProcessedBatch = batchSize

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(
		n.run.tw.tableDesc().ID,
		n.run.tw.rowsInLastProcessedBatch,
	)

	return n.run.tw.rowsInLastProcessedBatch > 0, nil
}

// processSourceRow processes one row from the source for upsertion.
// The table writer is in charge of accumulating the result rows.
func (n *upsertNode) processSourceRow(params runParams, rowVals tree.Datums) error {
	if err := enforceLocalColumnConstraints(rowVals, n.run.insertCols); err != nil {
		return err
	}

	// Verify the CHECK constraints by inspecting boolean columns from the input that
	// contain the results of evaluation.
	if !n.run.checkOrds.Empty() {
		ord := len(n.run.insertCols) + len(n.run.tw.fetchCols) + len(n.run.tw.updateCols)
		if n.run.tw.canaryOrdinal != -1 {
			ord++
		}
		checkVals := rowVals[ord:]
		if err := checkMutationInput(n.run.tw.tableDesc(), n.run.checkOrds, checkVals); err != nil {
			return err
		}
		rowVals = rowVals[:ord]
	}

	// Process the row. This is also where the tableWriter will accumulate
	// the row for later.
	return n.run.tw.row(params.ctx, rowVals, n.run.traceKV)
}

// BatchedCount implements the batchedPlanNode interface.
func (n *upsertNode) BatchedCount() int { return n.run.tw.rowsInLastProcessedBatch }

// BatchedValues implements the batchedPlanNode interface.
func (n *upsertNode) BatchedValues(rowIdx int) tree.Datums {
	if !n.run.tw.collectRows {
		panic(errors.AssertionFailedf("return row requested but collect rows was not set"))
	}
	return n.run.tw.rowsUpserted.At(rowIdx)
}

func (n *upsertNode) Close(ctx context.Context) {
	n.source.Close(ctx)
	n.run.tw.close(ctx)
	*n = upsertNode{}
	upsertNodePool.Put(n)
}

func (n *upsertNode) enableAutoCommit() {
	n.run.tw.enableAutoCommit()
}
