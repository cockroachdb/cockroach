// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// A windowNode implements the planNode interface and handles windowing logic.
//
// windowRender will contain renders that will output the desired result
// columns (so len(windowRender) == len(columns)).
//  1. If ith render from the source node does not have any window functions,
//     then that column will be simply passed through and windowRender[i] is
//     nil. Notably, windowNode will rearrange renders in the source node so
//     that all such passed through columns are contiguous and in the beginning.
//     (This happens during extractWindowFunctions call.)
//  2. If ith render from the source node has any window functions, then the
//     render is stored in windowRender[i]. During
//     constructWindowFunctionsDefinitions all variables used in OVER clauses
//     of all window functions are being rendered, and during
//     setupWindowFunctions all arguments to all window functions are being
//     rendered (renders are reused if possible).
//
// Therefore, the schema of the source node will be changed to look as follows:
// pass through column | OVER clauses columns | arguments to window functions.
type windowNode struct {
	singleInputPlanNode
	// columns is the set of result columns.
	columns colinfo.ResultColumns

	// The window functions handled by this windowNode.
	funcs []*windowFuncHolder
}

func (n *windowNode) startExec(params runParams) error {
	panic("windowNode can't be run in local mode")
}

func (n *windowNode) Next(params runParams) (bool, error) {
	panic("windowNode can't be run in local mode")
}

func (n *windowNode) Values() tree.Datums {
	panic("windowNode can't be run in local mode")
}

func (n *windowNode) Close(ctx context.Context) {
	n.input.Close(ctx)
}

var _ tree.TypedExpr = &windowFuncHolder{}
var _ tree.VariableExpr = &windowFuncHolder{}

type windowFuncHolder struct {
	expr *tree.FuncExpr
	args []tree.Expr

	argsIdxs     []uint32 // indices of the columns that are arguments to the window function
	filterColIdx int      // optional index of filtering column, -1 if no filter
	outputColIdx int      // index of the column that the output should be put into

	partitionIdxs  []int
	columnOrdering colinfo.ColumnOrdering
	frame          *tree.WindowFrame
}

// samePartition returns whether w and other have the same PARTITION BY clause.
func (w *windowFuncHolder) samePartition(other *windowFuncHolder) bool {
	if len(w.partitionIdxs) != len(other.partitionIdxs) {
		return false
	}
	for i, p := range w.partitionIdxs {
		if p != other.partitionIdxs[i] {
			return false
		}
	}
	return true
}

func (*windowFuncHolder) Variable() {}

func (w *windowFuncHolder) Format(ctx *tree.FmtCtx) {
	// Avoid duplicating the type annotation by calling .Format directly.
	w.expr.Format(ctx)
}

func (w *windowFuncHolder) String() string { return tree.AsString(w) }

func (w *windowFuncHolder) Walk(v tree.Visitor) tree.Expr { return w }

func (w *windowFuncHolder) TypeCheck(
	_ context.Context, _ *tree.SemaContext, desired *types.T,
) (tree.TypedExpr, error) {
	return w, nil
}

func (w *windowFuncHolder) Eval(ctx context.Context, v tree.ExprEvaluator) (tree.Datum, error) {
	panic("windowFuncHolder should not be evaluated directly")
}

func (w *windowFuncHolder) ResolvedType() *types.T {
	return w.expr.ResolvedType()
}
