// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

type windowPlanState struct {
	// infos contains information about windowFuncHolders in the same order as
	// they appear in n.funcs.
	infos   []*windowFuncInfo
	n       *windowNode
	planCtx *PlanningCtx
	plan    *PhysicalPlan
}

func createWindowPlanState(
	n *windowNode, planCtx *PlanningCtx, plan *PhysicalPlan,
) *windowPlanState {
	infos := make([]*windowFuncInfo, 0, len(n.funcs))
	for _, holder := range n.funcs {
		infos = append(infos, &windowFuncInfo{holder: holder})
	}
	return &windowPlanState{
		infos:   infos,
		n:       n,
		planCtx: planCtx,
		plan:    plan,
	}
}

// windowFuncInfo contains runtime information about a window function.
// Note: we use holder.funcIdx as the index of a particular windowFuncInfo
// among windowPlanState.infos.
type windowFuncInfo struct {
	holder *windowFuncHolder
	// isProcessed indicates whether holder has already been processed. It is set
	// to true when holder is included in the set of window functions to be
	// processed by findUnprocessedWindowFnsWithSamePartition.
	isProcessed bool
	// outputColIdx indicates the index of the column that contains the result of
	// "computing" holder. It is first set right after holder is included in the
	// set of window functions to be processed and is later might be adjusted
	// with each call of adjustColumnIndices.
	outputColIdx int
}

// samePartition returns whether f and other have the same PARTITION BY clause.
func (f *windowFuncHolder) samePartition(other *windowFuncHolder) bool {
	if len(f.partitionIdxs) != len(other.partitionIdxs) {
		return false
	}
	for i, p := range f.partitionIdxs {
		if p != other.partitionIdxs[i] {
			return false
		}
	}
	return true
}

// findUnprocessedWindowFnsWithSamePartition finds a set of unprocessed window
// functions that use the same partitioning and updates their isProcessed flag
// accordingly. It returns the set of unprocessed window functions and indices
// of the columns in their PARTITION BY clause.
func (s *windowPlanState) findUnprocessedWindowFnsWithSamePartition() (samePartitionFuncs []*windowFuncHolder, partitionIdxs []uint32) {
	var windowFnToProcess *windowFuncHolder
	for _, windowFn := range s.infos {
		if !windowFn.isProcessed {
			windowFnToProcess = windowFn.holder
			break
		}
	}
	if windowFnToProcess == nil {
		panic("unexpected: no unprocessed window function")
	}

	partitionIdxs = make([]uint32, len(windowFnToProcess.partitionIdxs))
	for i, idx := range windowFnToProcess.partitionIdxs {
		partitionIdxs[i] = uint32(idx)
	}

	samePartitionFuncs = make([]*windowFuncHolder, 0, len(s.infos)-windowFnToProcess.funcIdx)
	samePartitionFuncs = append(samePartitionFuncs, windowFnToProcess)
	s.infos[windowFnToProcess.funcIdx].isProcessed = true
	for _, windowFn := range s.infos[windowFnToProcess.funcIdx+1:] {
		if windowFn.isProcessed {
			continue
		}
		if windowFnToProcess.samePartition(windowFn.holder) {
			samePartitionFuncs = append(samePartitionFuncs, windowFn.holder)
			windowFn.isProcessed = true
		}
	}

	return samePartitionFuncs, partitionIdxs
}

// adjustColumnIndices shifts all the indices due to window functions in
// funcsInProgress that add or remove columns. It maintains:
// 1. argIdxStart of yet unprocessed window functions in s.infos to correctly
//    point at the start of their arguments;
// 2. outputColIdx of already processed (including added in the current stage)
//    window function to the output column index in the output of the current
//    stage of windowers;
// 3. indices of columns referred to in PARTITION BY and ORDER BY clauses of
//    unprocessed window functions.
func (s *windowPlanState) adjustColumnIndices(funcsInProgress []*windowFuncHolder) {
	for _, funcInProgress := range funcsInProgress {
		if funcInProgress.argCount != 1 {
			argShift := 1 - funcInProgress.argCount
			// All window functions after funcInProgress need to be adjusted since
			// funcInProgress adds/removes columns. Important assumption: all window
			// functions are initially sorted by their argIdxStart, and these shifts
			// keep that order intact.
			//
			// Some edge cases for two window functions f1 and f2 (f1 appears
			// before f2 in s.infos) with f1.argIdxStart == f2.argIdxStart:
			//
			// 1. both f1 and f2 are in funcsInProgress:
			//    a) f1.argCount == 0
			//       - handled correctly because we'll insert a new column at
			//       f1.argIdxStart, and the result of f2 will be appended right
			//       after f1, i.e. f2.argIdxStart == f1.argIdxStart + 1;
			//    b) f1.argCount > 0
			//       - not possible because f2.argIdxStart would not have been
			//       equal to f1.argIdxStart.
			//
			// 2. f1 in funcsInProgress, f2 is not:
			//    a) f2 has been processed already, so we want to maintain
			//       the pointer to its output column in outputColIdx. f1
			//       shifts all columns after f1.argIdxStart by argShift,
			//       so we need to adjust column index accordingly.
			//    b) f2 will be processed later, so we want to maintain
			//       f2.argIdxStart as an index of starting argument to f2.
			//
			// 3. f1 not in funcsInProgress, f2 is:
			//    -  f2 has no influence on f1 since f1 appears before f2 in s.infos.
			for _, f := range s.infos[funcInProgress.funcIdx+1:] {
				if f.isProcessed {
					f.outputColIdx += argShift
				} else {
					f.holder.argIdxStart += argShift
				}
			}
		}
	}

	// Assumption: all PARTITION BY and ORDER BY related columns come after
	// columns-arguments to window functions, so we need to adjust their indices
	// accordingly.
	partitionOrderColShift := 0
	for _, funcInProgress := range funcsInProgress {
		partitionOrderColShift += 1 - funcInProgress.argCount
	}
	if partitionOrderColShift != 0 {
		for _, f := range s.infos {
			if !f.isProcessed {
				// If f has already been processed, we don't adjust its indices since
				// it is not necessary.
				for p := range f.holder.partitionIdxs {
					f.holder.partitionIdxs[p] += partitionOrderColShift
				}
				oldColumnOrdering := f.holder.columnOrdering
				f.holder.columnOrdering = make(sqlbase.ColumnOrdering, 0, len(oldColumnOrdering))
				for _, o := range oldColumnOrdering {
					f.holder.columnOrdering = append(f.holder.columnOrdering, sqlbase.ColumnOrderInfo{
						ColIdx:    o.ColIdx + partitionOrderColShift,
						Direction: o.Direction,
					})
				}
			}
		}
	}
}

func createWindowerSpecFunc(funcStr string) (distsqlrun.WindowerSpec_Func, error) {
	if aggBuiltin, ok := distsqlrun.AggregatorSpec_Func_value[funcStr]; ok {
		aggSpec := distsqlrun.AggregatorSpec_Func(aggBuiltin)
		return distsqlrun.WindowerSpec_Func{AggregateFunc: &aggSpec}, nil
	} else if winBuiltin, ok := distsqlrun.WindowerSpec_WindowFunc_value[funcStr]; ok {
		winSpec := distsqlrun.WindowerSpec_WindowFunc(winBuiltin)
		return distsqlrun.WindowerSpec_Func{WindowFunc: &winSpec}, nil
	} else {
		return distsqlrun.WindowerSpec_Func{}, errors.Errorf("unknown aggregate/window function %s", funcStr)
	}
}

func (s *windowPlanState) createWindowFnSpec(
	funcInProgress *windowFuncHolder,
) (distsqlrun.WindowerSpec_WindowFn, sqlbase.ColumnType, error) {
	if funcInProgress.argIdxStart+funcInProgress.argCount > len(s.plan.ResultTypes) {
		return distsqlrun.WindowerSpec_WindowFn{}, sqlbase.ColumnType{}, errors.Errorf("ColIdx out of range (%d)", funcInProgress.argIdxStart+funcInProgress.argCount-1)
	}
	// Figure out which built-in to compute.
	funcStr := strings.ToUpper(funcInProgress.expr.Func.String())
	funcSpec, err := createWindowerSpecFunc(funcStr)
	if err != nil {
		return distsqlrun.WindowerSpec_WindowFn{}, sqlbase.ColumnType{}, err
	}
	argTypes := s.plan.ResultTypes[funcInProgress.argIdxStart : funcInProgress.argIdxStart+funcInProgress.argCount]
	_, outputType, err := distsqlrun.GetWindowFunctionInfo(funcSpec, argTypes...)
	if err != nil {
		return distsqlrun.WindowerSpec_WindowFn{}, outputType, err
	}
	// Populating column ordering from ORDER BY clause of funcInProgress.
	ordCols := make([]distsqlrun.Ordering_Column, 0, len(funcInProgress.columnOrdering))
	for _, column := range funcInProgress.columnOrdering {
		ordCols = append(ordCols, distsqlrun.Ordering_Column{
			ColIdx: uint32(column.ColIdx),
			// We need this -1 because encoding.Direction has extra value "_"
			// as zeroth "entry" which its proto equivalent doesn't have.
			Direction: distsqlrun.Ordering_Column_Direction(column.Direction - 1),
		})
	}
	funcInProgressSpec := distsqlrun.WindowerSpec_WindowFn{
		Func:         funcSpec,
		ArgIdxStart:  uint32(funcInProgress.argIdxStart),
		ArgCount:     uint32(funcInProgress.argCount),
		Ordering:     distsqlrun.Ordering{Columns: ordCols},
		FilterColIdx: int32(funcInProgress.filterColIdx),
	}
	if s.n.run.windowFrames[funcInProgress.funcIdx] != nil {
		// funcInProgress has a custom window frame.
		frameSpec := distsqlrun.WindowerSpec_Frame{}
		if err := frameSpec.InitFromAST(s.n.run.windowFrames[funcInProgress.funcIdx], s.planCtx.EvalContext()); err != nil {
			return distsqlrun.WindowerSpec_WindowFn{}, outputType, err
		}
		funcInProgressSpec.Frame = &frameSpec
	}

	return funcInProgressSpec, outputType, nil
}

// addRenderingIfNecessary checks whether any of the window functions' outputs
// are used in another expression and, if they are, adds rendering to the plan.
func (s *windowPlanState) addRenderingIfNecessary() error {
	// numWindowFuncsAsIs is the number of window functions output of which is
	// used directly (i.e. simply as an output column). Note: the same window
	// function might appear multiple times in the query, but its every
	// occurrence is replaced by a different windowFuncHolder. For example, on
	// query like 'SELECT avg(a) OVER (), avg(a) OVER () + 1 FROM t', only the
	// first window function is used "as is."
	numWindowFuncsAsIs := 0
	for _, render := range s.n.windowRender {
		if _, ok := render.(*windowFuncHolder); ok {
			numWindowFuncsAsIs++
		}
	}
	if numWindowFuncsAsIs == len(s.infos) {
		// All window functions' outputs are used directly, so no rendering to do.
		return nil
	}

	// windowNode contains render expressions that might contain:
	// 1) IndexedVars that refer to columns by their indices in the full table,
	// 2) IndexedVars that replaced regular aggregates that are above
	//    "windowing level."
	// The mapping of both types IndexedVars is stored in s.n.colAndAggContainer.
	// We need to make columnsMap that maps index of an indexedVar to the column
	// in the output of windower processor.

	// maxColumnIdx is the largest column index referenced by any IndexedVar in
	// renders of windowNode.
	maxColumnIdx := -1
	for col := range s.n.colAndAggContainer.idxMap {
		if col > maxColumnIdx {
			maxColumnIdx = col
		}
	}

	// We initialize columnsMap with -1's.
	columnsMap := makePlanToStreamColMap(maxColumnIdx + 1)

	// colShift refers to the number of columns added/removed because of window
	// functions that take number of arguments other than one. IndexedVars from
	// the container point to columns after window functions-related columns, so
	// we need to shift all indices by colShift.
	colShift := 0
	for _, windowFn := range s.infos {
		colShift += 1 - windowFn.holder.argCount
	}
	for col, idx := range s.n.colAndAggContainer.idxMap {
		columnsMap[col] = idx + colShift
	}

	renderExprs := make([]tree.TypedExpr, len(s.n.windowRender))
	visitor := replaceWindowFuncsVisitor{
		infos:      s.infos,
		columnsMap: columnsMap,
	}

	renderTypes := make([]sqlbase.ColumnType, 0, len(s.n.windowRender))
	for i, render := range s.n.windowRender {
		if render != nil {
			// render contains at least one reference to windowFuncHolder, so we need
			// to walk over the render and replace all windowFuncHolders and (if found)
			// IndexedVars using columnsMap and outputColIdx of windowFuncInfos.
			renderExprs[i] = visitor.replace(render)
		} else {
			// render is nil meaning that a column is being passed through.
			renderExprs[i] = tree.NewTypedOrdinalReference(visitor.colIdx, s.plan.ResultTypes[visitor.colIdx].ToDatumType())
			visitor.colIdx++
		}
		outputType, err := sqlbase.DatumTypeToColumnType(renderExprs[i].ResolvedType())
		if err != nil {
			return err
		}
		renderTypes = append(renderTypes, outputType)
	}
	if err := s.plan.AddRendering(renderExprs, s.planCtx, s.plan.PlanToStreamColMap, renderTypes); err != nil {
		return err
	}
	s.plan.PlanToStreamColMap = identityMap(s.plan.PlanToStreamColMap, len(renderTypes))
	return nil
}

// replaceWindowFuncsVisitor is used to populate render expressions containing
// the results of window functions. It recurses into all expressions except for
// windowFuncHolders (which are replaced by the indices to the corresponding
// output columns) and IndexedVars (which are replaced using columnsMap).
type replaceWindowFuncsVisitor struct {
	infos      []*windowFuncInfo
	columnsMap []int
	// colIdx is the index of the current column in the output of last stage of
	// windowers. It is necessary to pass through correct columns that are not
	// involved in expressions that contain window functions.
	colIdx int
}

var _ tree.Visitor = &replaceWindowFuncsVisitor{}

// VisitPre satisfies the Visitor interface.
func (v *replaceWindowFuncsVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *windowFuncHolder:
		v.colIdx++
		return false, tree.NewTypedOrdinalReference(v.infos[t.funcIdx].outputColIdx, t.ResolvedType())
	case *tree.IndexedVar:
		// We don't need to increment colIdx because all IndexedVar-related columns
		// are the very end.
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
