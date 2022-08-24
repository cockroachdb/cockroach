// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execagg"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func createWindowFnSpec(
	planCtx *PlanningCtx, plan *PhysicalPlan, funcInProgress *windowFuncHolder,
) (execinfrapb.WindowerSpec_WindowFn, *types.T, error) {
	for _, argIdx := range funcInProgress.argsIdxs {
		if argIdx >= uint32(len(plan.GetResultTypes())) {
			return execinfrapb.WindowerSpec_WindowFn{}, nil, errors.Errorf("ColIdx out of range (%d)", argIdx)
		}
	}
	// Figure out which built-in to compute.
	funcSpec, err := rowexec.CreateWindowerSpecFunc(funcInProgress.expr.Func.String())
	if err != nil {
		return execinfrapb.WindowerSpec_WindowFn{}, nil, err
	}
	argTypes := make([]*types.T, len(funcInProgress.argsIdxs))
	for i, argIdx := range funcInProgress.argsIdxs {
		argTypes[i] = plan.GetResultTypes()[argIdx]
	}
	_, outputType, err := execagg.GetWindowFunctionInfo(funcSpec, argTypes...)
	if err != nil {
		return execinfrapb.WindowerSpec_WindowFn{}, outputType, err
	}
	// Populating column ordering from ORDER BY clause of funcInProgress.
	ordCols := make([]execinfrapb.Ordering_Column, 0, len(funcInProgress.columnOrdering))
	for _, column := range funcInProgress.columnOrdering {
		ordCols = append(ordCols, execinfrapb.Ordering_Column{
			ColIdx: uint32(column.ColIdx),
			// We need this -1 because encoding.Direction has extra value "_"
			// as zeroth "entry" which its proto equivalent doesn't have.
			Direction: execinfrapb.Ordering_Column_Direction(column.Direction - 1),
		})
	}
	funcInProgressSpec := execinfrapb.WindowerSpec_WindowFn{
		Func:         funcSpec,
		ArgsIdxs:     funcInProgress.argsIdxs,
		Ordering:     execinfrapb.Ordering{Columns: ordCols},
		FilterColIdx: int32(funcInProgress.filterColIdx),
		OutputColIdx: uint32(funcInProgress.outputColIdx),
	}
	if funcInProgress.frame != nil {
		// funcInProgress has a custom window frame.
		frameSpec := execinfrapb.WindowerSpec_Frame{}
		if err := frameSpec.InitFromAST(funcInProgress.frame, planCtx.EvalContext()); err != nil {
			return execinfrapb.WindowerSpec_WindowFn{}, outputType, err
		}
		funcInProgressSpec.Frame = &frameSpec
	}

	return funcInProgressSpec, outputType, nil
}

// windowers currently cannot maintain the ordering (see #36310).
var windowerMergeOrdering = execinfrapb.Ordering{}

// addRenderingOrProjection checks whether any of the window functions' outputs
// are used in another expression and, if they are, adds rendering to the plan.
// If no rendering is required, it adds a projection to remove all columns that
// were arguments to window functions or were used within OVER clauses.
func addRenderingOrProjection(n *windowNode, planCtx *PlanningCtx, plan *PhysicalPlan) error {
	// numWindowFuncsAsIs is the number of window functions output of which is
	// used directly (i.e. simply as an output column). Note: the same window
	// function might appear multiple times in the query, but its every
	// occurrence is replaced by a different windowFuncHolder. For example, on
	// query like 'SELECT avg(a) OVER (), avg(a) OVER () + 1 FROM t', only the
	// first window function is used "as is."
	numWindowFuncsAsIs := 0
	for _, render := range n.windowRender {
		if _, ok := render.(*windowFuncHolder); ok {
			numWindowFuncsAsIs++
		}
	}
	if numWindowFuncsAsIs == len(n.funcs) {
		// All window functions' outputs are used directly, so there is no
		// rendering to do and simple projection is sufficient.
		columns := make([]uint32, len(n.windowRender))
		passedThruColIdx := uint32(0)
		for i, render := range n.windowRender {
			if render == nil {
				columns[i] = passedThruColIdx
				passedThruColIdx++
			} else {
				// We have done the type introspection above, so all non-nil renders
				// are windowFuncHolders.
				holder := render.(*windowFuncHolder)
				columns[i] = uint32(holder.outputColIdx)
			}
		}
		plan.AddProjection(columns, windowerMergeOrdering)
		return nil
	}

	// windowNode contains render expressions that might contain:
	// 1) IndexedVars that refer to columns by their indices in the full table,
	// 2) IndexedVars that replaced regular aggregates that are above
	//    "windowing level."
	// The mapping of both types IndexedVars is stored in n.colAndAggContainer.
	renderExprs := make([]tree.TypedExpr, len(n.windowRender))
	visitor := replaceWindowFuncsVisitor{
		columnsMap: n.colAndAggContainer.idxMap,
	}

	// All passed through columns are contiguous and at the beginning of the
	// output schema.
	passedThruColIdx := 0
	renderTypes := make([]*types.T, 0, len(n.windowRender))
	for i, render := range n.windowRender {
		if render != nil {
			// render contains at least one reference to windowFuncHolder, so we need
			// to walk over the render and replace all windowFuncHolders and (if found)
			// IndexedVars using columnsMap and outputColIdx of windowFuncHolders.
			renderExprs[i] = visitor.replace(render)
		} else {
			// render is nil meaning that a column is being passed through.
			renderExprs[i] = tree.NewTypedOrdinalReference(passedThruColIdx, plan.GetResultTypes()[passedThruColIdx])
			passedThruColIdx++
		}
		outputType := renderExprs[i].ResolvedType()
		renderTypes = append(renderTypes, outputType)
	}
	return plan.AddRendering(renderExprs, planCtx, plan.PlanToStreamColMap, renderTypes, windowerMergeOrdering)
}

// replaceWindowFuncsVisitor is used to populate render expressions containing
// the results of window functions. It recurses into all expressions except for
// windowFuncHolders (which are replaced by the indices to the corresponding
// output columns) and IndexedVars (which are replaced using columnsMap).
type replaceWindowFuncsVisitor struct {
	columnsMap map[int]int
}

var _ tree.Visitor = &replaceWindowFuncsVisitor{}

// VisitPre satisfies the Visitor interface.
func (v *replaceWindowFuncsVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *windowFuncHolder:
		return false, tree.NewTypedOrdinalReference(t.outputColIdx, t.ResolvedType())
	case *tree.IndexedVar:
		return false, tree.NewTypedOrdinalReference(v.columnsMap[t.Idx], t.ResolvedType())
	}
	return true, expr
}

// VisitPost satisfies the Visitor interface.
func (v *replaceWindowFuncsVisitor) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

func (v *replaceWindowFuncsVisitor) replace(typedExpr tree.TypedExpr) tree.TypedExpr {
	expr, _ := tree.WalkExpr(v, typedExpr)
	return expr.(tree.TypedExpr)
}
