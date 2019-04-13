// Copyright 2016 The Cockroach Authors.
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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// A windowNode implements the planNode interface and handles windowing logic.
// It "wraps" a planNode which is used to retrieve the un-windowed results.
type windowNode struct {
	// The "wrapped" node (which returns un-windowed results).
	plan planNode

	// A sparse array holding renders specific to this windowNode. This will contain
	// nil entries for renders that do not contain window functions, and which therefore
	// can be propagated directly from the "wrapped" node.
	windowRender []tree.TypedExpr

	// The window functions handled by this windowNode. computeWindows will populate
	// an entire column in windowValues for each windowFuncHolder, in order.
	funcs []*windowFuncHolder

	// colAndAggContainer is an IndexedVarContainer that provides indirection
	// to migrate IndexedVars and aggregate functions below the windowing level.
	colAndAggContainer windowNodeColAndAggContainer

	// numRendersNotToBeReused indicates the number of renders that are being used
	// as arguments to window functions plus (possibly) some columns that are simply
	// being passed through windowNode (i.e. with no window functions).
	//
	// Currently, we do not want to reuse these renders because the columns these renders
	// refer to will not be output by window processors in DistSQL once
	// the corresponding window functions have been computed (window processors
	// put the result of computing of window functions in place of the arguments
	// to window functions).
	// TODO(yuzefovich): once this is no longer necessary, remove this restriction.
	numRendersNotToBeReused int

	// numOverClausesColumns indicates the number of renders that are added
	// while constructing window definitions (arguments of PARTITION BY and of
	// ORDER BY within OVER clauses). These renders are used only during
	// processing of window functions and should be projected out afterwards.
	numOverClausesColumns int

	run windowRun
}

// window constructs a windowNode according to window function applications. This may
// adjust the render targets in the renderNode as necessary. The use of window functions
// will run with a space complexity of O(NW) (N = number of rows, W = number of windows)
// and a time complexity of O(NW) (no ordering), O(W*NlogN) (with ordering), and
// O(W*N^2) (with constant or variable sized window-frames, which are not yet supported).
//
// This code uses the following terminology throughout:
// - window:
//     the optionally-ordered subset of data over which calculations are made, defined by
//     the window definition for a given window function application.
// - built-in window functions:
//     a set of built-in functions that can only be used in the context of a window
//     through a window function application, using window function syntax.
//     Ex. row_number(), rank(), dense_rank()
// - window function application:
//     the act of applying a built-in window function or built-in aggregation function
//     over a specific window. The application performs a calculation across a set of
//     table rows that are somehow related to the current row. Unlike regular aggregate
//     functions, window function application does not cause rows to become grouped into
//     a single output row — the rows retain their separate identities.
// - window definition:
//     the defined window to apply a window function over, which is stated in a window
//     function application's OVER clause.
//     Ex. SELECT avg(x) OVER (w PARTITION BY z) FROM y
//                            ^^^^^^^^^^^^^^^^^^
// - named window specification:
//     a named window provided at the end of a SELECT clause in the WINDOW clause that
//     can be referenced by the window definition of one or more window function
//     applications. This window can be used directly as a window definition, or can be
//     overridden in a window definition.
//     Ex. used directly: SELECT avg(x) OVER w FROM y WINDOW w AS (ORDER BY z)
//                                                           ^^^^^^^^^^^^^^^^^
//     Ex. overridden: SELECT avg(x) OVER (w PARTITION BY z) FROM y WINDOW w AS (ORDER BY z)
//                                                                         ^^^^^^^^^^^^^^^^^
func (p *planner) window(
	ctx context.Context, n *tree.SelectClause, s *renderNode,
) (*windowNode, error) {
	// Determine if a window function is being applied. We use the renderNode's
	// renders to determine this because window functions may be added to the
	// renderNode by an ORDER BY clause.
	// For instance: SELECT x FROM y ORDER BY avg(x) OVER ().
	if !s.renderProps.SeenWindowApplication {
		return nil, nil
	}

	window := &windowNode{
		windowRender: make([]tree.TypedExpr, len(s.render)),
		run: windowRun{
			values: valuesNode{columns: s.columns},
		},
	}

	if err := window.extractWindowFunctions(s); err != nil {
		return nil, err
	}

	if err := p.constructWindowDefinitions(ctx, window, n, s); err != nil {
		return nil, err
	}

	window.replaceIndexVarsAndAggFuncs(s)
	acc := p.EvalContext().Mon.MakeBoundAccount()
	window.run.wrappedRenderVals = rowcontainer.NewRowContainer(
		acc, sqlbase.ColTypeInfoFromResCols(s.columns), 0,
	)

	return window, nil
}

// windowRun contains the run-time state of windowNode during local execution.
type windowRun struct {
	// The values returned by the wrapped nodes are logically split into three
	// groups of columns, although they may overlap if renders were merged:
	// - sourceVals: these values are either passed directly as rendered values of the
	//     windowNode if their corresponding expressions were not wrapped in window functions,
	//     or used as arguments to window functions to eventually create rendered values for
	//     the windowNode if their corresponding expressions were wrapped in window functions.
	//     These will always be located in wrappedRenderVals[:sourceCols].
	//     (see extractWindowFunctions)
	// - windowDefVals: these values are used to partition and order window function
	//     applications, and were added to the wrapped node from window definitions.
	//     (see constructWindowDefinitions)
	// - indexedVarVals: these values are used to buffer the IndexedVar values
	//     for each row. Unlike the renderNode, which can stream values for each IndexedVar,
	//     we need to buffer all values here while we compute window function results. We
	//     then index into these values in colContainer.IndexedVarEval and
	//     aggContainer.IndexedVarEval. (see replaceIndexVarsAndAggFuncs)
	wrappedRenderVals *rowcontainer.RowContainer

	// The populated values for this windowNode.
	values valuesNode

	windowValues [][]tree.Datum
	curRowIdx    int
	windowFrames []*tree.WindowFrame

	windowsAcc mon.BoundAccount
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
	if n.run.wrappedRenderVals != nil {
		n.run.wrappedRenderVals.Close(ctx)
		n.run.wrappedRenderVals = nil
	}
	if n.run.windowValues != nil {
		n.run.windowValues = nil
		n.run.windowsAcc.Close(ctx)
	}
	n.run.values.Close(ctx)
}

// extractWindowFunctions loops over the render expressions and extracts any window functions.
// While looping over the renders, each window function will be replaced by a separate render
// for each of its (possibly 0) arguments in the renderNode.
func (n *windowNode) extractWindowFunctions(s *renderNode) error {
	visitor := extractWindowFuncsVisitor{
		n:              n,
		aggregatesSeen: make(map[*tree.FuncExpr]struct{}),
	}

	oldRenders := s.render
	oldColumns := s.columns
	newRenders := make([]tree.TypedExpr, 0, len(oldRenders))
	newColumns := make([]sqlbase.ResultColumn, 0, len(oldColumns))
	for i := range oldRenders {
		// Add all window function applications found in oldRenders[i] to window.funcs.
		typedExpr, numFuncsAdded, err := visitor.extract(oldRenders[i])
		if err != nil {
			return err
		}
		if numFuncsAdded == 0 {
			// No window functions in render.
			newRenders = append(newRenders, oldRenders[i])
			newColumns = append(newColumns, oldColumns[i])
		} else {
			// One or more window functions in render. Create a new render in
			// renderNode for each window function argument.
			n.windowRender[i] = typedExpr
			prevWindowCount := len(n.funcs) - numFuncsAdded
			for i, funcHolder := range n.funcs[prevWindowCount:] {
				funcHolder.funcIdx = prevWindowCount + i
				funcHolder.argIdxStart = len(newRenders)
				for _, argExpr := range funcHolder.args {
					arg := argExpr.(tree.TypedExpr)
					newRenders = append(newRenders, arg)
					newColumns = append(newColumns, sqlbase.ResultColumn{
						Name: arg.String(),
						Typ:  arg.ResolvedType(),
					})
				}
			}
		}
	}
	s.resetRenderColumns(newRenders, newColumns)
	return nil
}

// constructWindowDefinitions creates window definitions for each window
// function application by combining specific window definition from a
// given window function application with referenced window specifications
// on the SelectClause.
func (p *planner) constructWindowDefinitions(
	ctx context.Context, n *windowNode, sc *tree.SelectClause, s *renderNode,
) error {
	// Process each named window specification on the select clause.
	namedWindowSpecs := make(map[string]*tree.WindowDef, len(sc.Window))
	for _, windowDef := range sc.Window {
		name := string(windowDef.Name)
		if _, ok := namedWindowSpecs[name]; ok {
			return pgerror.NewErrorf(pgerror.CodeWindowingError, "window %q is already defined", name)
		}
		namedWindowSpecs[name] = windowDef
	}

	n.run.windowFrames = make([]*tree.WindowFrame, len(n.funcs))
	n.numRendersNotToBeReused = len(s.render)

	// Construct window definitions for each window function application.
	for idx, windowFn := range n.funcs {
		windowDef, err := constructWindowDef(*windowFn.expr.WindowDef, namedWindowSpecs)
		if err != nil {
			return err
		}

		// Validate FILTER clause.
		if windowFn.expr.Filter != nil {
			filterExpr := windowFn.expr.Filter.(tree.TypedExpr)

			// We need to save and restore the previous value of the field in
			// semaCtx in case we are recursively called within a subquery
			// context.
			scalarProps := &p.semaCtx.Properties
			defer scalarProps.Restore(*scalarProps)
			scalarProps.Require("FILTER", tree.RejectSpecial)

			col, renderExpr, err := p.computeRender(ctx,
				tree.SelectExpr{Expr: filterExpr}, types.Bool, s.sourceInfo, s.ivarHelper,
				autoGenerateRenderOutputName,
			)
			if err != nil {
				return err
			}
			if renderExpr.ResolvedType().SemanticType() != types.BOOL {
				return pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError,
					"argument of FILTER must be type boolean, not type %s", renderExpr.ResolvedType(),
				)
			}
			windowFn.filterColIdx = s.addOrReuseRenderStartingFromIdx(col, renderExpr, true /* reuse */, n.numRendersNotToBeReused)
		}

		// Validate PARTITION BY clause.
		for _, partition := range windowDef.Partitions {
			if funcExpr, ok := partition.(*tree.FuncExpr); ok {
				if funcExpr.IsWindowFunctionApplication() {
					return pgerror.NewErrorf(pgerror.CodeWindowingError, "window functions are not allowed in window definitions")
				}
			}
			cols, exprs, _, err := p.computeRenderAllowingStars(ctx,
				tree.SelectExpr{Expr: partition}, types.Any, s.sourceInfo, s.ivarHelper,
				autoGenerateRenderOutputName)
			if err != nil {
				return err
			}

			colIdxs := s.addOrReuseRendersStartingFromIdx(cols, exprs, true, n.numRendersNotToBeReused)
			windowFn.partitionIdxs = append(windowFn.partitionIdxs, colIdxs...)
		}

		// Validate ORDER BY clause.
		for _, orderBy := range windowDef.OrderBy {
			if funcExpr, ok := orderBy.Expr.(*tree.FuncExpr); ok {
				if funcExpr.IsWindowFunctionApplication() {
					return pgerror.NewErrorf(pgerror.CodeWindowingError, "window functions are not allowed in window definitions")
				}
			}
			cols, exprs, _, err := p.computeRenderAllowingStars(ctx,
				tree.SelectExpr{Expr: orderBy.Expr}, types.Any, s.sourceInfo, s.ivarHelper,
				autoGenerateRenderOutputName)
			if err != nil {
				return err
			}

			direction := encoding.Ascending
			if orderBy.Direction == tree.Descending {
				direction = encoding.Descending
			}

			colIdxs := s.addOrReuseRendersStartingFromIdx(cols, exprs, true, n.numRendersNotToBeReused)
			for _, idx := range colIdxs {
				ordering := sqlbase.ColumnOrderInfo{
					ColIdx:    idx,
					Direction: direction,
				}
				windowFn.columnOrdering = append(windowFn.columnOrdering, ordering)
			}
		}

		// Validate frame of the window definition if present.
		if windowDef.Frame != nil {
			if err := windowDef.Frame.TypeCheck(&p.semaCtx, &windowDef); err != nil {
				return err
			}
			if windowDef.Frame.Mode == tree.RANGE && windowDef.Frame.Bounds.HasOffset() {
				n.funcs[idx].ordColTyp = windowDef.OrderBy[0].Expr.(tree.TypedExpr).ResolvedType()
			}
		}

		n.run.windowFrames[idx] = windowDef.Frame
	}

	n.numOverClausesColumns = len(s.render) - n.numRendersNotToBeReused
	return nil
}

// constructWindowDef constructs a WindowDef using the provided WindowDef value and the
// set of named window specifications on the current SELECT clause. If the provided
// WindowDef does not reference a named window spec, then it will simply be returned without
// modification. If the provided WindowDef does reference a named window spec, then the
// referenced spec will be overridden with any extra clauses from the WindowDef and returned.
func constructWindowDef(
	def tree.WindowDef, namedWindowSpecs map[string]*tree.WindowDef,
) (tree.WindowDef, error) {
	modifyRef := false
	var refName string
	switch {
	case def.RefName != "":
		// SELECT rank() OVER (w) FROM t WINDOW w as (...)
		// We copy the referenced window specification, and modify it if necessary.
		refName = string(def.RefName)
		modifyRef = true
	case def.Name != "":
		// SELECT rank() OVER w FROM t WINDOW w as (...)
		// We use the referenced window specification directly, without modification.
		refName = string(def.Name)
	}
	if refName == "" {
		return def, nil
	}

	referencedSpec, ok := namedWindowSpecs[refName]
	if !ok {
		return def, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError, "window %q does not exist", refName)
	}
	if !modifyRef {
		return *referencedSpec, nil
	}

	// referencedSpec.Partitions is always used.
	if len(def.Partitions) > 0 {
		return def, pgerror.NewErrorf(pgerror.CodeWindowingError, "cannot override PARTITION BY clause of window %q", refName)
	}
	def.Partitions = referencedSpec.Partitions

	// referencedSpec.OrderBy is used if set.
	if len(referencedSpec.OrderBy) > 0 {
		if len(def.OrderBy) > 0 {
			return def, pgerror.NewErrorf(pgerror.CodeWindowingError, "cannot override ORDER BY clause of window %q", refName)
		}
		def.OrderBy = referencedSpec.OrderBy
	}

	if referencedSpec.Frame != nil {
		return def, pgerror.NewErrorf(pgerror.CodeWindowingError, "cannot copy window %q because it has a frame clause", refName)
	}

	return def, nil
}

// Once the extractWindowFunctions has been run over each render, the remaining
// render expressions will either be nil or contain an expression. If one is nil,
// that means the render will not be touched by windowNode, and will be passed on
// without modification. If the render is not nil, that means that it contained
// at least one window function, and now has windowFuncHolders standing in as
// terminal expressions for each of these window function applications. These
// expressions will be evaluated after each of the window functions are run, so
// we refer to them as happening above the "windowing level". In other words, we
// let the source plan execute to completion below the windowing level, we then
// compute the results of each window function for each row, and finally we continue
// evaluating the windowRenders with each window function's respective result for
// that row above the windowing level.
//
// There is one complication here; expression types that also vary for each row
// need to be treated with special care if above this windowing level. These expression
// types are:
// - IndexedVars: window function evaluation requires completion of any wrapped
//    plan nodes, so if we did nothing here, all existing IndexedVars from source plans
//    would be pointing to their values in the last row of the underlying renderNode.
//    Clearly, we cannot use the renderNode as the IndexedVarContainer for IndexedVars
//    above the windowing level.
//   Example:
//    SELECT x + row_number() OVER () FROM t
//      is replaced by
//    SELECT @1 + row_number() OVER () FROM (SELECT x, ... FROM t)
//
// - Aggregate Functions: aggregate functions are handled by the groupNode, which
//    requires the functions to be present in its renders for proper evaluation.
//    If an aggregate function is found above the windowing level and we do not
//    migrate it below the windowing level, the aggregation will never be performed.
//   Example:
//    SELECT max(x) + row_number() OVER () FROM t
//      is replaced by
//    SELECT @1 + row_number() OVER () FROM (SELECT max(x), ... FROM t)
//
// To work around both of these cases, we perform four steps:
// 1. We add renders to the source plan for each column referenced by any existing
//    IndexedVar or any aggregate function found above the windowing level. In both
//    cases, we perform deduplication to only add a single render per unique
//    IndexedVar or aggregate function.
// 2. We replace each IndexedVar or aggregation function with a new IndexedVar that
//    uses the windowNode as an IndexedVarContainer (see windowNodeVarContainer and
//    windowNodeAggContainer).
// 3. The results are computed by the source node for the newly added renders. The
//    window node then buffers these results in the wrappedIndexedVarVals RowContainer
//    while computing window function results.
// 4. When evaluating windowRenders for each row that contain these new IndexedVars,
//    the windowNode provides the buffered column value for that row through its
//    IndexedVarContainer interface.
//
// Example:
//   SELECT a, max(b), c + max(d) + first_value(e) OVER (PARTITION BY f) FROM t
//    (for clarity, we ignore grouping rules)
//
//   After extractWindowFunctions is run:
//    source plan's renders: [a, max(b), e]
//    window plan's renders: [nil, nil, c + max(d) + first_value(@3) OVER (PARTITION BY f)]
//
//   After constructWindowDefinitions is run:
//    source plan's renders: [a, max(b), e, f]
//    window plan's renders: [nil, nil, c + max(d) + first_value(@3) OVER (PARTITION BY @4)]
//
//   After replaceIndexVarsAndAggFuncs is run:
//    source plan's renders: [a, max(b), e, f, c, max(d)]
//    window plan's renders: [nil, nil, @5 + @6 + first_value(@3) OVER (PARTITION BY @4)]
//
func (n *windowNode) replaceIndexVarsAndAggFuncs(s *renderNode) {
	n.colAndAggContainer = makeWindowNodeColAndAggContainer(&n.run, s.sourceInfo[0], s.ivarHelper.NumVars())

	// The number of aggregation functions that need to be replaced with
	// IndexedVars is unknown, so we collect them here and bind them to
	// an IndexedVarHelper later. Consequently, we cannot yet create
	// that IndexedVarHelper, so we collect new unbounded IndexedVars
	// that replace existing IndexedVars and similarly bind them later.
	// We use a map indexed by render index to leverage addOrReuseRender's
	// deduplication of identical IndexedVars and aggregate functions.
	iVars := make(map[int]*tree.IndexedVar)

	for i, render := range n.windowRender {
		if render == nil {
			continue
		}
		replaceExprsAboveWindowing := func(expr tree.Expr) (error, bool, tree.Expr) {
			switch t := expr.(type) {
			case *tree.IndexedVar:
				// We add a new render to the source renderNode for each new IndexedVar we
				// see. We also register this mapping in the idxMap.
				col := sqlbase.ResultColumn{
					Name: t.String(),
					Typ:  t.ResolvedType(),
				}
				colIdx := s.addOrReuseRenderStartingFromIdx(col, t, true, n.numRendersNotToBeReused)

				if iVar, ok := iVars[colIdx]; ok {
					// If we have already created an IndexedVar for this
					// IndexedVar, return it.
					return nil, false, iVar
				}

				// Create a new IndexedVar with index t.Idx.
				colIVar := tree.NewIndexedVar(t.Idx)
				iVars[colIdx] = colIVar
				n.colAndAggContainer.idxMap[t.Idx] = colIdx
				return nil, false, colIVar

			case *tree.FuncExpr:
				// All window function applications will have been replaced by
				// windowFuncHolders at this point, so if we see an aggregate
				// function in the window renders, it is above the windowing level.
				if t.GetAggregateConstructor() != nil {
					// We add a new render to the source renderNode for each new aggregate
					// function we see.
					col := sqlbase.ResultColumn{Name: t.String(), Typ: t.ResolvedType()}
					colIdx := s.addOrReuseRender(col, t, true)

					if iVar, ok := iVars[colIdx]; ok {
						// If we have already created an IndexedVar for this aggregate
						// function, return it.
						return nil, false, iVar
					}

					// Create a new IndexedVar with the next available index.
					idx := n.colAndAggContainer.startAggIdx + n.colAndAggContainer.numAggFuncs
					n.colAndAggContainer.numAggFuncs++
					aggIVar := tree.NewIndexedVar(idx)
					iVars[colIdx] = aggIVar
					n.colAndAggContainer.idxMap[idx] = colIdx
					n.colAndAggContainer.aggFuncs[idx] = t
					return nil, false, aggIVar
				}
				return nil, true, expr
			default:
				return nil, true, expr
			}
		}
		expr, err := tree.SimpleVisit(render, replaceExprsAboveWindowing)
		if err != nil {
			panic(err)
		}
		n.windowRender[i] = expr.(tree.TypedExpr)
	}

	colAggHelper := tree.MakeIndexedVarHelper(&n.colAndAggContainer, s.ivarHelper.NumVars()+n.colAndAggContainer.numAggFuncs)
	for _, ivar := range iVars {
		// The ivars above have been created with a nil container, and
		// therefore they are guaranteed to be modified in-place by
		// BindIfUnbound().
		if newIvar, err := colAggHelper.BindIfUnbound(ivar); err != nil {
			panic(err)
		} else if newIvar != ivar {
			panic(fmt.Sprintf("unexpected binding: %v, expected: %v", newIvar, ivar))
		}
	}
}

type extractWindowFuncsVisitor struct {
	n *windowNode

	// Avoids allocations.
	subWindowVisitor transform.ContainsWindowVisitor

	// Persisted visitor state.
	aggregatesSeen map[*tree.FuncExpr]struct{}
	windowFnCount  int
	err            error
}

var _ tree.Visitor = &extractWindowFuncsVisitor{}

func (v *extractWindowFuncsVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	switch t := expr.(type) {
	case *tree.FuncExpr:
		switch {
		case t.IsWindowFunctionApplication():
			// Check if a parent node above this window function is an aggregate.
			if len(v.aggregatesSeen) > 0 {
				v.err = sqlbase.NewWindowInAggError()
				return false, expr
			}

			// Make sure this window function does not contain another window function.
			for _, argExpr := range t.Exprs {
				if v.subWindowVisitor.ContainsWindowFunc(argExpr) {
					v.err = pgerror.NewErrorf(pgerror.CodeWindowingError, "window function calls cannot be nested")
					return false, expr
				}
			}

			f := &windowFuncHolder{
				expr:         t,
				args:         t.Exprs,
				argCount:     len(t.Exprs),
				window:       v.n,
				filterColIdx: noFilterIdx,
			}
			v.windowFnCount++
			v.n.funcs = append(v.n.funcs, f)
			return false, f

		case t.GetAggregateConstructor() != nil:
			// If we see an aggregation that is not used in a window function, we save it
			// in the visitor's seen aggregate set. The aggregate function will remain in
			// this set until the recursion into its children is complete.
			v.aggregatesSeen[t] = struct{}{}
		}
	}
	return true, expr
}

func (v *extractWindowFuncsVisitor) VisitPost(expr tree.Expr) tree.Expr {
	if fn, ok := expr.(*tree.FuncExpr); ok {
		delete(v.aggregatesSeen, fn)
	}
	return expr
}

// Extract windowFuncHolders from exprs that use window functions and check if they are valid.
// It will return the new expression tree, along with the number of window functions seen and
// added to v.n.funcs.
// A window function is valid if:
// - it is not contained in an aggregate function
// - it does not contain another window function
// - it is either the application of a built-in window function
//   or of a built-in aggregate function
//
// For example:
// Invalid: `SELECT AVG(AVG(k) OVER ()) FROM kv`
// - The avg aggregate wraps the window function.
// Valid:      `SELECT AVG(k) OVER () FROM kv`
// Also valid: `SELECT AVG(AVG(k)) OVER () FROM kv`
// - Window functions can wrap aggregates.
// Invalid:    `SELECT NOW() OVER () FROM kv`
// - NOW() is not an aggregate or a window function.
func (v extractWindowFuncsVisitor) extract(typedExpr tree.TypedExpr) (tree.TypedExpr, int, error) {
	expr, _ := tree.WalkExpr(&v, typedExpr)
	if v.err != nil {
		return nil, 0, v.err
	}
	return expr.(tree.TypedExpr), v.windowFnCount, nil
}

var _ tree.TypedExpr = &windowFuncHolder{}
var _ tree.VariableExpr = &windowFuncHolder{}

const noFilterIdx = -1

type windowFuncHolder struct {
	window *windowNode

	expr *tree.FuncExpr
	args []tree.Expr

	funcIdx      int      // index of the windowFuncHolder in window.funcs
	argIdxStart  int      // index of the window function's first arguments in window.wrappedValues
	argCount     int      // number of arguments taken by the window function
	filterColIdx int      // optional index of filtering column, -1 if no filter
	ordColTyp    *types.T // type of the ordering column, used only in RANGE mode with offsets

	partitionIdxs  []int
	columnOrdering sqlbase.ColumnOrdering
}

func (*windowFuncHolder) Variable() {}

func (w *windowFuncHolder) Format(ctx *tree.FmtCtx) {
	// Avoid duplicating the type annotation by calling .Format directly.
	w.expr.Format(ctx)
}

func (w *windowFuncHolder) String() string { return tree.AsString(w) }

func (w *windowFuncHolder) Walk(v tree.Visitor) tree.Expr { return w }

func (w *windowFuncHolder) TypeCheck(
	_ *tree.SemaContext, desired *types.T,
) (tree.TypedExpr, error) {
	return w, nil
}

func (w *windowFuncHolder) Eval(ctx *tree.EvalContext) (tree.Datum, error) {
	// Index into the windowValues computed in windowNode.computeWindows
	// to determine the Datum value to return. Evaluating this datum
	// is almost certainly the identity.
	return w.window.run.windowValues[w.window.run.curRowIdx][w.funcIdx].Eval(ctx)
}

func (w *windowFuncHolder) ResolvedType() *types.T {
	return w.expr.ResolvedType()
}

func makeWindowNodeColAndAggContainer(
	n *windowRun, sourceInfo *sqlbase.DataSourceInfo, numColVars int,
) windowNodeColAndAggContainer {
	return windowNodeColAndAggContainer{
		n:           n,
		idxMap:      make(map[int]int),
		sourceInfo:  sourceInfo,
		aggFuncs:    make(map[int]*tree.FuncExpr),
		startAggIdx: numColVars,
	}
}

// windowNodeColContainer is an IndexedVarContainer providing indirection for
// IndexedVars found above the windowing level. See replaceIndexVarsAndAggFuncs.
type windowNodeColAndAggContainer struct {
	n *windowRun

	// idxMap maps the index of IndexedVars created in replaceIndexVarsAndAggFuncs
	// to the index their corresponding results in this container. It permits us to
	// add a single render to the source plan per unique expression.
	idxMap map[int]int
	// sourceInfo contains information on the IndexedVars from the
	// source plan where they were originally created.
	sourceInfo *sqlbase.DataSourceInfo
	// aggFuncs maps the index of IndexedVars to their corresponding aggregate function.
	aggFuncs map[int]*tree.FuncExpr
	// startAggIdx indicates the smallest index to be used by an IndexedVar replacing
	// an aggregate function. We don't want to mix these IndexedVars with those
	// that replace "original" IndexedVars.
	startAggIdx int
	numAggFuncs int
}

func (c *windowNodeColAndAggContainer) IndexedVarEval(
	idx int, ctx *tree.EvalContext,
) (tree.Datum, error) {
	// Determine which row in the buffered values to evaluate.
	curRow := c.n.wrappedRenderVals.At(c.n.curRowIdx)
	// Determine which value in that row to evaluate.
	curVal := curRow[c.idxMap[idx]]
	return curVal.Eval(ctx)
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
