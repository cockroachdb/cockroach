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

	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	// In contrast with the run part of insert/delete/update, the
	// upsertRun has a tableWriter interface reference instead of
	// embedding a direct struct because it can run with either the
	// fastTableUpdater or the regular tableUpdater.
	tw          batchedTableWriter
	checkHelper *sqlbase.CheckHelper

	// insertCols are the columns being inserted/upserted into.
	insertCols []sqlbase.ColumnDescriptor

	// defaultExprs are the expressions used to generate default values.
	defaultExprs []tree.TypedExpr

	// computedCols are the columns that need to be (re-)computed as
	// the result of computing some of the source rows prior to the upsert.
	computedCols []sqlbase.ColumnDescriptor
	// computeExprs are the expressions to evaluate to re-compute the
	// columns in computedCols.
	computeExprs []tree.TypedExpr
	// iVarContainerForComputedCols is used as a temporary buffer that
	// holds the updated values for every column in the source, to
	// serve as input for indexed vars contained in the computeExprs.
	iVarContainerForComputedCols sqlbase.RowIndexedVarContainer

	// done informs a new call to BatchedNext() that the previous call to
	// BatchedNext() has completed the work already.
	done bool

	// traceKV caches the current KV tracing flag.
	traceKV bool
}

func (n *upsertNode) startExec(params runParams) error {
	if err := params.p.maybeSetSystemConfig(n.run.tw.tableDesc().GetID()); err != nil {
		return err
	}

	// cache traceKV during execution, to avoid re-evaluating it for every row.
	n.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	return n.run.tw.init(params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (n *upsertNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (n *upsertNode) Values() tree.Datums { panic("not valid") }

// maxUpsertBatchSize is the max number of entries in the KV batch for
// the upsert operation (including secondary index updates, FK
// cascading updates, etc), before the current KV batch is executed
// and a new batch is started.
const maxUpsertBatchSize = 10000

// BatchedNext implements the batchedPlanNode interface.
func (n *upsertNode) BatchedNext(params runParams) (bool, error) {
	if n.run.done {
		return false, nil
	}

	tracing.AnnotateTrace()

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
		if n.run.tw.curBatchSize() >= maxUpsertBatchSize {
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

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(
		n.run.tw.tableDesc().ID,
		n.run.tw.batchedCount(),
	)

	return n.run.tw.batchedCount() > 0, nil
}

// processSourceRow processes one row from the source for upsertion.
// The table writer is in charge of accumulating the result rows.
func (n *upsertNode) processSourceRow(params runParams, sourceVals tree.Datums) error {
	// Process the incoming row tuple and generate the full inserted
	// row. This fills in the defaults, computes computed columns, and
	// checks the data width complies with the schema constraints.
	rowVals, err := row.GenerateInsertRow(
		n.run.defaultExprs,
		n.run.computeExprs,
		n.run.insertCols,
		n.run.computedCols,
		params.EvalContext().Copy(),
		n.run.tw.tableDesc(),
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
			insertColIDtoRowIndex := n.run.iVarContainerForComputedCols.Mapping
			if err := n.run.checkHelper.LoadEvalRow(insertColIDtoRowIndex, rowVals, false); err != nil {
				return err
			}
			if err := n.run.checkHelper.CheckEval(params.EvalContext()); err != nil {
				return err
			}
		} else {
			checkVals := sourceVals[len(sourceVals)-n.run.checkHelper.Count():]
			if err := n.run.checkHelper.CheckInput(checkVals); err != nil {
				return err
			}
		}
	}

	// Process the row. This is also where the tableWriter will accumulate
	// the row for later.
	return n.run.tw.row(params.ctx, rowVals, n.run.traceKV)
}

// BatchedCount implements the batchedPlanNode interface.
func (n *upsertNode) BatchedCount() int { return n.run.tw.batchedCount() }

// BatchedCount implements the batchedPlanNode interface.
func (n *upsertNode) BatchedValues(rowIdx int) tree.Datums { return n.run.tw.batchedValues(rowIdx) }

func (n *upsertNode) Close(ctx context.Context) {
	n.source.Close(ctx)
	if n.run.tw != nil {
		n.run.tw.close(ctx)
	}
	*n = upsertNode{}
	upsertNodePool.Put(n)
}

func (n *upsertNode) enableAutoCommit() {
	n.run.tw.enableAutoCommit()
}

// upsertHelper is the helper struct in charge of evaluating SQL
// expression during an UPSERT. Its main responsibilities are:
//
// - eval(): compute the UPDATE statements given the previous and new values,
//   for INSERT ... ON CONFLICT DO UPDATE SET ...
//
// - evalComputedCols(): evaluate and add the computed columns
//   to the current upserted row.
//
// - shouldUpdate(): evaluates the WHERE clause of an ON CONFLICT
//   ... DO UPDATE clause.
//
// See the tableUpsertEvaler interface definition in tablewriter.go.
type upsertHelper struct {
	// p is used to provide an evalContext.
	p *planner

	// evalExprs contains the individual columns of SET RHS in an ON
	// CONFLICT DO UPDATE clause.
	// This decomposes tuple assignments, e.g. DO UPDATE SET (a,b) = (x,y)
	// into their individual expressions, in this case x, y.
	//
	// Its entries correspond 1-to-1 to the columns in
	// tableUpserter.updateCols.
	//
	// This is the main input for eval().
	evalExprs []tree.TypedExpr

	// whereExpr is the RHS of an ON CONFLICT DO UPDATE SET ... WHERE <expr>.
	// This is the main input for shouldUpdate().
	whereExpr tree.TypedExpr

	// sourceInfo describes the columns provided by the table being
	// upserted into.
	sourceInfo *sqlbase.DataSourceInfo
	// excludedSourceInfo describes the columns provided by the values
	// being upserted. This may be fewer columns than the
	// table, for example:
	// INSERT INTO kv(k) VALUES (3) ON CONFLICT(k) DO UPDATE SET v = excluded.v;
	//        invalid, value v is not part of the source in VALUES   ^^^^^^^^^^
	excludedSourceInfo *sqlbase.DataSourceInfo

	// curSourceRow buffers the current values from the table during
	// an upsert conflict. Used as input when evaluating
	// evalExprs and whereExpr.
	curSourceRow tree.Datums
	// curExcludedRow buffers the current values being upserted
	// during an upsert conflict. Used as input when evaluating
	// evalExprs and whereExpr.
	curExcludedRow tree.Datums

	// computeExprs is the list of expressions to (re-)compute computed
	// columns.
	// This is the main input for evalComputedCols().
	computeExprs []tree.TypedExpr

	// ccIvarContainer buffers the current values after the upsert
	// conflict resolution, to serve as input while evaluating
	// computeExprs.
	ccIvarContainer sqlbase.RowIndexedVarContainer

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	_ util.NoCopy
}

var _ tableUpsertEvaler = (*upsertHelper)(nil)

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (uh *upsertHelper) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	numSourceColumns := len(uh.sourceInfo.SourceColumns)
	if idx >= numSourceColumns {
		return uh.curExcludedRow[idx-numSourceColumns].Eval(ctx)
	}
	return uh.curSourceRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (uh *upsertHelper) IndexedVarResolvedType(idx int) *types.T {
	numSourceColumns := len(uh.sourceInfo.SourceColumns)
	if idx >= numSourceColumns {
		return uh.excludedSourceInfo.SourceColumns[idx-numSourceColumns].Typ
	}
	return uh.sourceInfo.SourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (uh *upsertHelper) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	numSourceColumns := len(uh.sourceInfo.SourceColumns)
	if idx >= numSourceColumns {
		return uh.excludedSourceInfo.NodeFormatter(idx - numSourceColumns)
	}
	return uh.sourceInfo.NodeFormatter(idx)
}

func (uh *upsertHelper) walkExprs(walk func(desc string, index int, expr tree.TypedExpr)) {
	for i, evalExpr := range uh.evalExprs {
		walk("eval", i, evalExpr)
	}
}

// eval returns the values for the update case of an upsert, given the row
// that would have been inserted and the existing (conflicting) values.
func (uh *upsertHelper) eval(insertRow, existingRow, resultRow tree.Datums) (tree.Datums, error) {
	// Buffer the rows.
	uh.curSourceRow = existingRow
	uh.curExcludedRow = insertRow

	// Evaluate the update expressions.
	uh.p.EvalContext().PushIVarContainer(uh)
	defer func() { uh.p.EvalContext().PopIVarContainer() }()
	for _, evalExpr := range uh.evalExprs {
		res, err := evalExpr.Eval(uh.p.EvalContext())
		if err != nil {
			return nil, err
		}
		resultRow = append(resultRow, res)
	}

	return resultRow, nil
}

// evalComputedCols handles after we've figured out the values of all regular
// columns, what the values of any incoming computed columns should be.
// It then appends those new values to the end of a given slice.
func (uh *upsertHelper) evalComputedCols(
	updatedRow tree.Datums, appendTo tree.Datums,
) (tree.Datums, error) {
	// Buffer the row.
	uh.ccIvarContainer.CurSourceRow = updatedRow

	// Evaluate the computed columns.
	uh.p.EvalContext().PushIVarContainer(&uh.ccIvarContainer)
	defer func() { uh.p.EvalContext().PopIVarContainer() }()
	for i := range uh.computeExprs {
		res, err := uh.computeExprs[i].Eval(uh.p.EvalContext())
		if err != nil {
			return nil, err
		}
		appendTo = append(appendTo, res)
	}
	return appendTo, nil
}

// shouldUpdate returns the result of evaluating the WHERE clause of the
// ON CONFLICT ... DO UPDATE clause.
func (uh *upsertHelper) shouldUpdate(insertRow tree.Datums, existingRow tree.Datums) (bool, error) {
	// Buffer the rows.
	uh.curSourceRow = existingRow
	uh.curExcludedRow = insertRow

	// Evaluate the predicate.
	uh.p.EvalContext().PushIVarContainer(uh)
	defer func() { uh.p.EvalContext().PopIVarContainer() }()
	return sqlbase.RunFilter(uh.whereExpr, uh.p.EvalContext())
}
