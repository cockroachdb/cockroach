// Copyright 2016 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
)

var upsertNodePool = sync.Pool{
	New: func() interface{} {
		return &upsertNode{}
	},
}

type upsertNode struct {
	singleInputPlanNode

	// columns is set if this UPDATE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run upsertRun
}

var _ mutationPlanNode = &upsertNode{}

// upsertRun contains the run-time state of upsertNode during local execution.
type upsertRun struct {
	tw        tableUpserter
	checkOrds checkSet

	// insertCols are the columns being inserted/upserted into.
	insertCols []catalog.Column

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

	// Advance one batch. First, clear the last batch.
	n.run.tw.clearLastBatch(params.ctx)

	// Now consume/accumulate the rows for this batch.
	lastBatch := false
	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Advance one individual row.
		if next, err := n.input.Next(params); !next {
			lastBatch = true
			if err != nil {
				return false, err
			}
			break
		}

		// Process the insertion for the current input row, potentially
		// accumulating the result row for later.
		if err := n.run.processSourceRow(params, n.input.Values()); err != nil {
			return false, err
		}

		// Are we done yet with the current batch?
		if n.run.tw.currentBatchSize >= n.run.tw.maxBatchSize ||
			n.run.tw.b.ApproximateMutationBytes() >= n.run.tw.maxBatchByteSize {
			break
		}
	}

	if n.run.tw.currentBatchSize > 0 {
		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err := n.run.tw.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		n.run.tw.setRowsWrittenLimit(params.extendedEvalCtx.SessionData())
		if err := n.run.tw.finalize(params.ctx); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		n.run.done = true
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(n.run.tw.tableDesc(), n.run.tw.lastBatchSize)

	return n.run.tw.lastBatchSize > 0, nil
}

// processSourceRow processes one row from the source for upsertion.
// The table writer is in charge of accumulating the result rows.
func (r *upsertRun) processSourceRow(params runParams, rowVals tree.Datums) error {
	// Check for NOT NULL constraint violations.
	if r.tw.canaryOrdinal != -1 && rowVals[r.tw.canaryOrdinal] != tree.DNull {
		// When there is a canary column and its value is not NULL, then an
		// existing row is being updated, so check only the update columns for
		// NOT NULL constraint violations.
		offset := len(r.insertCols) + len(r.tw.fetchCols)
		vals := rowVals[offset : offset+len(r.tw.updateCols)]
		if err := enforceNotNullConstraints(vals, r.tw.updateCols); err != nil {
			return err
		}
	} else {
		// Otherwise, there is no canary column (i.e., canaryOrdinal is -1,
		// which is the case for "blind" upsert which overwrites existing rows
		// without performing a read) or it is NULL, indicating that a new row
		// is being inserted. In this case, check the insert columns for a NOT
		// NULL constraint violation.
		vals := rowVals[:len(r.insertCols)]
		if err := enforceNotNullConstraints(vals, r.insertCols); err != nil {
			return err
		}
	}

	lastUpsertCol := len(r.insertCols) + len(r.tw.fetchCols) + len(r.tw.updateCols)
	if r.tw.canaryOrdinal != -1 {
		lastUpsertCol++
	}
	upsertVals := rowVals[:lastUpsertCol]
	rowVals = rowVals[lastUpsertCol:]

	// Verify the CHECK constraints by inspecting boolean columns from the input that
	// contain the results of evaluation.
	if !r.checkOrds.Empty() {
		if err := checkMutationInput(
			params.ctx, params.p.EvalContext(), &params.p.semaCtx, params.p.SessionData(),
			r.tw.tableDesc(), r.checkOrds, rowVals[:r.checkOrds.Len()],
		); err != nil {
			return err
		}
		rowVals = rowVals[r.checkOrds.Len():]
	}

	// Create a set of partial index IDs to not add or remove entries from. Order is puts
	// then deletes.
	var pm row.PartialIndexUpdateHelper
	if n := len(r.tw.tableDesc().PartialIndexes()); n > 0 {
		err := pm.Init(rowVals[:n], rowVals[n:n*2], r.tw.tableDesc())
		if err != nil {
			return err
		}
		rowVals = rowVals[n*2:]
	}

	// Keep track of the vector index partitions to update, as well as the
	// quantized vectors for puts. This information is passed to tableInserter.row
	// below.
	var vh row.VectorIndexUpdateHelper
	if n := len(r.tw.tableDesc().VectorIndexes()); n > 0 {
		vh.InitForPut(rowVals[:n], rowVals[n:n*2], r.tw.tableDesc())
		vh.InitForDel(rowVals[n*2:n*3], r.tw.tableDesc())
	}

	if buildutil.CrdbTestBuild {
		// This testing knob allows us to suspend execution to force a race condition.
		if fn := params.ExecCfg().TestingKnobs.AfterArbiterRead; fn != nil {
			fn()
		}
	}

	// Process the row. This is also where the tableWriter will accumulate
	// the row for later.
	return r.tw.row(params.ctx, upsertVals, pm, vh, r.traceKV)
}

// BatchedCount implements the batchedPlanNode interface.
func (n *upsertNode) BatchedCount() int { return n.run.tw.lastBatchSize }

// BatchedValues implements the batchedPlanNode interface.
func (n *upsertNode) BatchedValues(rowIdx int) tree.Datums { return n.run.tw.rows.At(rowIdx) }

func (n *upsertNode) Close(ctx context.Context) {
	n.input.Close(ctx)
	n.run.tw.close(ctx)
	*n = upsertNode{}
	upsertNodePool.Put(n)
}

func (n *upsertNode) rowsWritten() int64 {
	return n.run.tw.rowsWritten
}

func (n *upsertNode) enableAutoCommit() {
	n.run.tw.enableAutoCommit()
}
