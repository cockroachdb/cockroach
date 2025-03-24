// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

var updateFastPathNodePool = sync.Pool{
	New: func() interface{} {
		return &updateFastPathNode{}
	},
}

type updateFastPathNode struct {
	zeroInputPlanNode
	// input values, similar to a valuesNode.
	input [][]tree.TypedExpr

	// columns is set if this UPDATE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run updateFastPathRun
}

type updateFastPathRun struct {
	updateRun

	numInputCols int

	// inputBuf stores the evaluation result of the input rows, linearized into a
	// single slice; see inputRow(). Unfortunately we can't do everything one row
	// at a time, because we need the datums for generating error messages in case
	// an FK check fails.
	inputBuf tree.Datums
}

func (r *updateFastPathRun) inputRow(rowIdx int) tree.Datums {
	start := rowIdx * r.numInputCols
	end := start + r.numInputCols
	return r.inputBuf[start:end:end]
}

func (u *updateFastPathNode) startExec(params runParams) error {
	// cache traceKV during execution, to avoid re-evaluating it for every row.
	u.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	u.run.mustValidateOldValues = true

	u.run.initRowContainer(params, u.columns)

	if len(u.input) != 1 {
		return errors.AssertionFailedf("update fast path can only handle 1 row")
	}

	u.run.numInputCols = len(u.input[0])
	u.run.inputBuf = make(tree.Datums, len(u.input)*u.run.numInputCols)

	return u.run.tu.init(params.ctx, params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (u *updateFastPathNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (u *updateFastPathNode) Values() tree.Datums { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (u *updateFastPathNode) BatchedNext(params runParams) (bool, error) {
	if u.run.done {
		return false, nil
	}

	// The fast path node does everything in one batch.

	// There should only be a single row, to ensure the savepoint rollback below
	// has the correct SQL semantics. This is written as a loop anyway to match
	// the insert fast path.
	for rowIdx, tupleRow := range u.input {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		inputRow := u.run.inputRow(rowIdx)
		for col, typedExpr := range tupleRow {
			var err error
			inputRow[col], err = eval.Expr(params.ctx, params.EvalContext(), typedExpr)
			if err != nil {
				return false, err
			}
		}

		if err := u.run.processSourceRow(params, inputRow); err != nil {
			return false, err
		}
	}

	// create savepoint
	_, err := u.run.tu.createSavepoint(params.ctx)
	if err != nil {
		return false, err
	}

	u.run.tu.setRowsWrittenLimit(params.extendedEvalCtx.SessionData())
	if err := u.run.tu.finalize(params.ctx); err != nil {
		// if the error is ConditionFailedError, rollback to savepoint and report 0
		// rows modified
		fmt.Println(err)
		return false, err
	}
	// Remember we're done for the next call to BatchedNext().
	u.run.done = true

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(u.run.tu.tableDesc(), u.run.tu.lastBatchSize)

	return true, nil
}

// BatchedCount implements the batchedPlanNode interface.
func (u *updateFastPathNode) BatchedCount() int {
	return u.run.tu.lastBatchSize
}

// BatchedValues implements the batchedPlanNode interface.
func (u *updateFastPathNode) BatchedValues(rowIdx int) tree.Datums {
	return u.run.tu.rows.At(rowIdx)
}

func (u *updateFastPathNode) Close(ctx context.Context) {
	u.run.tu.close(ctx)
	*u = updateFastPathNode{}
	updateFastPathNodePool.Put(u)
}

func (u *updateFastPathNode) rowsWritten() int64 {
	return u.run.tu.rowsWritten
}

func (u *updateFastPathNode) enableAutoCommit() {
	u.run.tu.enableAutoCommit()
}
