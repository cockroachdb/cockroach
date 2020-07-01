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

var insertNodePool = sync.Pool{
	New: func() interface{} {
		return &insertNode{}
	},
}

var tableInserterPool = sync.Pool{
	New: func() interface{} {
		return &tableInserter{}
	},
}

type insertNode struct {
	source planNode

	// columns is set if this INSERT is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns sqlbase.ResultColumns

	run insertRun
}

// insertRun contains the run-time state of insertNode during local execution.
type insertRun struct {
	ti         tableInserter
	rowsNeeded bool

	checkOrds checkSet

	// insertCols are the columns being inserted into.
	insertCols []sqlbase.ColumnDescriptor

	// rowCount is the number of rows in the current batch.
	rowCount int

	// done informs a new call to BatchedNext() that the previous call to
	// BatchedNext() has completed the work already.
	done bool

	// rows contains the accumulated result rows if rowsNeeded is set.
	rows *rowcontainer.RowContainer

	// resultRowBuffer is used to prepare a result row for accumulation
	// into the row container above, when rowsNeeded is set.
	resultRowBuffer tree.Datums

	// rowIdxToTabColIdx is the mapping from the ordering of rows in
	// insertCols to the ordering in the rows in the table, used when
	// rowsNeeded is set to populate resultRowBuffer and the row
	// container. The return index is -1 if the column for the row
	// index is not public. This is used in conjunction with tabIdxToRetIdx
	// to populate the resultRowBuffer.
	rowIdxToTabColIdx []int

	// tabColIdxToRetIdx is the mapping from the columns in the table to the
	// columns in the resultRowBuffer. A value of -1 is used to indicate
	// that the table column at that index is not part of the resultRowBuffer
	// of the mutation. Otherwise, the value at the i-th index refers to the
	// index of the resultRowBuffer where the i-th column of the table is
	// to be returned.
	tabColIdxToRetIdx []int

	// traceKV caches the current KV tracing flag.
	traceKV bool
}

func (r *insertRun) initRowContainer(
	params runParams, columns sqlbase.ResultColumns, rowCapacity int,
) {
	if !r.rowsNeeded {
		return
	}
	r.rows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(columns),
		rowCapacity,
	)

	// In some cases (e.g. `INSERT INTO t (a) ...`) the data source
	// does not provide all the table columns. However we do need to
	// produce result rows that contain values for all the table
	// columns, in the correct order.  This will be done by
	// re-ordering the data into resultRowBuffer.
	//
	// Also we need to re-order the values in the source, ordered by
	// insertCols, when writing them to resultRowBuffer, according to
	// the rowIdxToTabColIdx mapping.

	r.resultRowBuffer = make(tree.Datums, len(columns))
	for i := range r.resultRowBuffer {
		r.resultRowBuffer[i] = tree.DNull
	}

	colIDToRetIndex := make(map[sqlbase.ColumnID]int)
	cols := r.ti.tableDesc().Columns
	for i := range cols {
		colIDToRetIndex[cols[i].ID] = i
	}

	r.rowIdxToTabColIdx = make([]int, len(r.insertCols))
	for i, col := range r.insertCols {
		if idx, ok := colIDToRetIndex[col.ID]; !ok {
			// Column must be write only and not public.
			r.rowIdxToTabColIdx[i] = -1
		} else {
			r.rowIdxToTabColIdx[i] = idx
		}
	}
}

// processSourceRow processes one row from the source for insertion and, if
// result rows are needed, saves it in the result row container.
func (r *insertRun) processSourceRow(params runParams, rowVals tree.Datums) error {
	if err := enforceLocalColumnConstraints(rowVals, r.insertCols); err != nil {
		return err
	}

	// Create a set of index IDs to not write to. Indexes should not be written
	// to when they are partial indexes and the row does not satisfy the
	// predicate. This set is passed as a parameter to tableInserter.row below.
	var ignoreIndexes util.FastIntSet
	partialIndexPutVals := rowVals[len(r.insertCols)+r.checkOrds.Len():]
	colIdx := 0
	indexes := r.ti.tableDesc().Indexes
	for i := range indexes {
		index := indexes[i]
		if index.IsPartial() {
			val, err := tree.GetBool(partialIndexPutVals[colIdx])
			if err != nil {
				return err
			}

			if !val {
				// If the value of the column for the index predicate expression
				// is false, the row should not be added to the partial index.
				ignoreIndexes.Add(int(index.ID))
			}

			colIdx++
			if colIdx >= len(partialIndexPutVals) {
				break
			}

		}
	}

	// Truncate rowVals so that it no longer includes partial index predicate
	// values.
	rowVals = rowVals[:len(r.insertCols)+r.checkOrds.Len()]

	// Verify the CHECK constraint results, if any.
	if !r.checkOrds.Empty() {
		checkVals := rowVals[len(r.insertCols):]
		if err := checkMutationInput(params.ctx, &params.p.semaCtx, r.ti.tableDesc(), r.checkOrds, checkVals); err != nil {
			return err
		}
		rowVals = rowVals[:len(r.insertCols)]
	}

	// Queue the insert in the KV batch.
	if err := r.ti.row(params.ctx, rowVals, ignoreIndexes, r.traceKV); err != nil {
		return err
	}

	// If result rows need to be accumulated, do it.
	if r.rows != nil {
		for i, val := range rowVals {
			// The downstream consumer will want the rows in the order of
			// the table descriptor, not that of insertCols. Reorder them
			// and ignore non-public columns.
			if tabIdx := r.rowIdxToTabColIdx[i]; tabIdx >= 0 {
				if retIdx := r.tabColIdxToRetIdx[tabIdx]; retIdx >= 0 {
					r.resultRowBuffer[retIdx] = val
				}
			}
		}

		if _, err := r.rows.AddRow(params.ctx, r.resultRowBuffer); err != nil {
			return err
		}
	}

	return nil
}

// maxInsertBatchSize is the max number of entries in the KV batch for
// the insert operation (including secondary index updates, FK
// cascading updates, etc), before the current KV batch is executed
// and a new batch is started.
var maxInsertBatchSize = 10000

func (n *insertNode) startExec(params runParams) error {
	// Cache traceKV during execution, to avoid re-evaluating it for every row.
	n.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	n.run.initRowContainer(params, n.columns, 0 /* rowCapacity */)

	return n.run.ti.init(params.ctx, params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (n *insertNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (n *insertNode) Values() tree.Datums { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (n *insertNode) BatchedNext(params runParams) (bool, error) {
	if n.run.done {
		return false, nil
	}

	tracing.AnnotateTrace()

	// Advance one batch. First, clear the current batch.
	n.run.rowCount = 0
	if n.run.rows != nil {
		n.run.rows.Clear(params.ctx)
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
				// TODO(richardjcai): Don't like this, not sure how to check if the
				// parse error is specifically from the column undergoing the
				// alter column type schema change.

				// Intercept parse error due to ALTER COLUMN TYPE schema change.
				err = interceptAlterColumnTypeParseError(n.run.insertCols, -1, err)
				return false, err
			}
			break
		}

		// Process the insertion for the current source row, potentially
		// accumulating the result row for later.
		if err := n.run.processSourceRow(params, n.source.Values()); err != nil {
			return false, err
		}

		n.run.rowCount++

		// Are we done yet with the current batch?
		if n.run.ti.curBatchSize() >= maxInsertBatchSize {
			break
		}
	}

	if n.run.rowCount > 0 {
		if err := n.run.ti.atBatchEnd(params.ctx, n.run.traceKV); err != nil {
			return false, err
		}

		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err := n.run.ti.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		if _, err := n.run.ti.finalize(params.ctx, n.run.traceKV); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		n.run.done = true
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(n.run.ti.tableDesc().ID, n.run.rowCount)

	return n.run.rowCount > 0, nil
}

// BatchedCount implements the batchedPlanNode interface.
func (n *insertNode) BatchedCount() int { return n.run.rowCount }

// BatchedCount implements the batchedPlanNode interface.
func (n *insertNode) BatchedValues(rowIdx int) tree.Datums { return n.run.rows.At(rowIdx) }

func (n *insertNode) Close(ctx context.Context) {
	n.source.Close(ctx)
	n.run.ti.close(ctx)
	if n.run.rows != nil {
		n.run.rows.Close(ctx)
	}
	*n = insertNode{}
	insertNodePool.Put(n)
}

// See planner.autoCommit.
func (n *insertNode) enableAutoCommit() {
	n.run.ti.enableAutoCommit()
}

// TestingSetInsertBatchSize exports a constant for testing only.
func TestingSetInsertBatchSize(val int) func() {
	oldVal := maxInsertBatchSize
	maxInsertBatchSize = val
	return func() { maxInsertBatchSize = oldVal }
}
