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

	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	ti          tableInserter
	checkHelper *sqlbase.CheckHelper
	rowsNeeded  bool

	// insertCols are the columns being inserted into.
	insertCols []sqlbase.ColumnDescriptor

	// defaultExprs are the expressions used to generate default values.
	defaultExprs []tree.TypedExpr

	// computedCols are the columns that need to be (re-)computed as
	// the result of updating some of the columns in updateCols.
	computedCols []sqlbase.ColumnDescriptor
	// computeExprs are the expressions to evaluate to re-compute the
	// columns in computedCols.
	computeExprs []tree.TypedExpr
	// iVarContainerForComputedCols is used as a temporary buffer that
	// holds the updated values for every column in the source, to
	// serve as input for indexed vars contained in the computeExprs.
	iVarContainerForComputedCols sqlbase.RowIndexedVarContainer

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

// maxInsertBatchSize is the max number of entries in the KV batch for
// the insert operation (including secondary index updates, FK
// cascading updates, etc), before the current KV batch is executed
// and a new batch is started.
const maxInsertBatchSize = 10000

func (n *insertNode) startExec(params runParams) error {
	if err := params.p.maybeSetSystemConfig(n.run.ti.tableDesc().GetID()); err != nil {
		return err
	}

	// cache traceKV during execution, to avoid re-evaluating it for every row.
	n.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	if n.run.rowsNeeded {
		n.run.rows = rowcontainer.NewRowContainer(
			params.EvalContext().Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(n.columns), 0)

		// In some cases (e.g. `INSERT INTO t (a) ...`) the data source
		// does not provide all the table columns. However we do need to
		// produce result rows that contain values for all the table
		// columns, in the correct order.  This will be done by
		// re-ordering the data into resultRowBuffer.
		//
		// Also we need to re-order the values in the source, ordered by
		// insertCols, when writing them to resultRowBuffer, according to
		// the rowIdxToTabColIdx mapping.

		n.run.resultRowBuffer = make(tree.Datums, len(n.columns))
		for i := range n.run.resultRowBuffer {
			n.run.resultRowBuffer[i] = tree.DNull
		}

		colIDToRetIndex := make(map[sqlbase.ColumnID]int)
		cols := n.run.ti.tableDesc().Columns
		for i := range cols {
			colIDToRetIndex[cols[i].ID] = i
		}

		n.run.rowIdxToTabColIdx = make([]int, len(n.run.insertCols))
		for i, col := range n.run.insertCols {
			if idx, ok := colIDToRetIndex[col.ID]; !ok {
				// Column must be write only and not public.
				n.run.rowIdxToTabColIdx[i] = -1
			} else {
				n.run.rowIdxToTabColIdx[i] = idx
			}
		}
	}

	return n.run.ti.init(params.p.txn, params.EvalContext())
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
				return false, err
			}
			break
		}

		// Process the insertion for the current source row, potentially
		// accumulating the result row for later.
		if err := n.processSourceRow(params, n.source.Values()); err != nil {
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

// processSourceRow processes one row from the source for insertion and, if
// result rows are needed, saves it in the result row container.
func (n *insertNode) processSourceRow(params runParams, sourceVals tree.Datums) error {
	// Process the incoming row tuple and generate the full inserted
	// row. This fills in the defaults, computes computed columns, and
	// check the data width complies with the schema constraints.
	rowVals, err := row.GenerateInsertRow(
		n.run.defaultExprs,
		n.run.computeExprs,
		n.run.insertCols,
		n.run.computedCols,
		params.EvalContext().Copy(),
		n.run.ti.tableDesc(),
		sourceVals,
		&n.run.iVarContainerForComputedCols,
	)
	if err != nil {
		return err
	}

	// Run the CHECK constraints, if any. CheckHelper will either evaluate the
	// constraints itself, or else inspect boolean columns from the input that
	// contain the results of evaluation.
	if n.run.checkHelper != nil {
		if n.run.checkHelper.NeedsEval() {
			if err := n.run.checkHelper.LoadEvalRow(
				n.run.ti.ri.InsertColIDtoRowIndex, rowVals, false); err != nil {
				return err
			}
			if err := n.run.checkHelper.CheckEval(params.EvalContext()); err != nil {
				return err
			}
		} else {
			checkVals := rowVals[len(n.run.insertCols):]
			if err := n.run.checkHelper.CheckInput(checkVals); err != nil {
				return err
			}
			rowVals = rowVals[:len(n.run.insertCols)]
		}
	}

	// Queue the insert in the KV batch.
	if err = n.run.ti.row(params.ctx, rowVals, n.run.traceKV); err != nil {
		return err
	}

	// If result rows need to be accumulated, do it.
	if n.run.rows != nil {
		for i, val := range rowVals {
			// The downstream consumer will want the rows in the order of
			// the table descriptor, not that of insertCols. Reorder them
			// and ignore non-public columns.
			if tabIdx := n.run.rowIdxToTabColIdx[i]; tabIdx >= 0 {
				if retIdx := n.run.tabColIdxToRetIdx[tabIdx]; retIdx >= 0 {
					n.run.resultRowBuffer[retIdx] = val
				}
			}
		}

		if _, err := n.run.rows.AddRow(params.ctx, n.run.resultRowBuffer); err != nil {
			return err
		}
	}

	return nil
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
