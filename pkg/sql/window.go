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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// A windowNode implements the planNode interface and handles windowing logic.
//
// windowRender will contain renders that will output the desired result
// columns (so len(windowRender) == len(columns)).
// 1. If ith render from the source node does not have any window functions,
//    then that column will be simply passed through and windowRender[i] is
//    nil. Notably, windowNode will rearrange renders in the source node so
//    that all such passed through columns are contiguous and in the beginning.
//    (This happens during extractWindowFunctions call.)
// 2. If ith render from the source node has any window functions, then the
//    render is stored in windowRender[i]. During
//    constructWindowFunctionsDefinitions all variables used in OVER clauses
//    of all window functions are being rendered, and during
//    setupWindowFunctions all arguments to all window functions are being
//    rendered (renders are reused if possible).
// Therefore, the schema of the source node will be changed to look as follows:
// pass through column | OVER clauses columns | arguments to window functions.
type windowNode struct {
	// The source node.
	plan planNode
	// columns is the set of result columns.
	columns colinfo.ResultColumns

	// A sparse array holding renders specific to this windowNode. This will
	// contain nil entries for renders that do not contain window functions,
	// and which therefore can be propagated directly from the "wrapped" node.
	windowRender []tree.TypedExpr

	// The window functions handled by this windowNode.
	funcs []*windowFuncHolder

	// colAndAggContainer is an IndexedVarContainer that provides indirection
	// to migrate IndexedVars and aggregate functions below the windowing level.
	colAndAggContainer windowNodeColAndAggContainer
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
	n.plan.Close(ctx)
}

var _ tree.TypedExpr = &windowFuncHolder{}
var _ tree.VariableExpr = &windowFuncHolder{}

type windowFuncHolder struct {
	window *windowNode

	expr *tree.FuncExpr
	args []tree.Expr

	argsIdxs     []uint32 // indices of the columns that are arguments to the window function
	filterColIdx int      // optional index of filtering column, -1 if no filter
	outputColIdx int      // index of the column that the output should be put into

	partitionIdxs  []int
	columnOrdering colinfo.ColumnOrdering
	frame          *tree.WindowFrame
}

// samePartition returns whether f and other have the same PARTITION BY clause.
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

func (w *windowFuncHolder) Eval(ctx *tree.EvalContext) (tree.Datum, error) {
	panic("windowFuncHolder should not be evaluated directly")
}

func (w *windowFuncHolder) ResolvedType() *types.T {
	return w.expr.ResolvedType()
}

// windowNodeColAndAggContainer is an IndexedVarContainer providing indirection
// for IndexedVars and aggregation functions found above the windowing level.
// See replaceIndexVarsAndAggFuncs.
type windowNodeColAndAggContainer struct {
	// idxMap maps the index of IndexedVars created in replaceIndexVarsAndAggFuncs
	// to the index their corresponding results in this container. It permits us to
	// add a single render to the source plan per unique expression.
	idxMap map[int]int
	// sourceInfo contains information on the IndexedVars from the
	// source plan where they were originally created.
	sourceInfo *colinfo.DataSourceInfo
	// aggFuncs maps the index of IndexedVars to their corresponding aggregate function.
	aggFuncs map[int]*tree.FuncExpr
	// startAggIdx indicates the smallest index to be used by an IndexedVar replacing
	// an aggregate function. We don't want to mix these IndexedVars with those
	// that replace "original" IndexedVars.
	startAggIdx int
}

func (c *windowNodeColAndAggContainer) IndexedVarEval(
	idx int, ctx *tree.EvalContext,
) (tree.Datum, error) {
	panic("IndexedVarEval should not be called on windowNodeColAndAggContainer")
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (c *windowNodeColAndAggContainer) IndexedVarResolvedType(idx int) *types.T {
	if idx >= c.startAggIdx {
		return c.aggFuncs[idx].ResolvedType()
	}
	return c.sourceInfo.SourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (c *windowNodeColAndAggContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	if idx >= c.startAggIdx {
		// Avoid duplicating the type annotation by calling .Format directly.
		return c.aggFuncs[idx]
	}
	// Avoid duplicating the type annotation by calling .Format directly.
	return c.sourceInfo.NodeFormatter(idx)
}
