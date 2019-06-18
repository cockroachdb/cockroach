// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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
	columns sqlbase.ResultColumns

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

// window constructs a windowNode according to window function applications. This may
// adjust the render targets in the renderNode as necessary. The use of window functions
// will run with a space complexity of O(NW) (N = number of rows, W = number of windows)
// and a time complexity of O(NW) (no ordering), O(W*NlogN) (with ordering), and
// O(W*N^2) (with constant or variable sized window-frames).
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
		columns:      s.columns,
		windowRender: make([]tree.TypedExpr, len(s.render)),
	}

	if err := window.extractWindowFunctions(s); err != nil {
		return nil, err
	}

	if err := p.constructWindowDefinitions(ctx, window, n, s); err != nil {
		return nil, err
	}

	window.replaceIndexVarsAndAggFuncs(s)
	window.setupWindowFunctions(s)
	return window, nil
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

// extractWindowFunctions loops over the render expressions and extracts any
// window functions. All render expressions without window functions will be
// passed through.
func (n *windowNode) extractWindowFunctions(s *renderNode) error {
	visitor := extractWindowFuncsVisitor{
		n:              n,
		aggregatesSeen: make(map[*tree.FuncExpr]struct{}),
	}

	// TODO(yuzefovich): ideally, we should take in a list of columns that should
	// be passed through instead of passing all of them through.
	passedThroughRenders := make([]tree.TypedExpr, 0, len(s.render))
	passedThroughCols := make(sqlbase.ResultColumns, 0, len(s.render))
	for i, render := range s.render {
		// Add all window function applications found in render to window.funcs.
		typedExpr, numFuncsAdded, err := visitor.extract(render)
		if err != nil {
			return err
		}
		if numFuncsAdded == 0 {
			// No window functions in render, so we will simply pass it through.
			passedThroughRenders = append(passedThroughRenders, render)
			passedThroughCols = append(passedThroughCols, s.columns[i])
		} else {
			n.windowRender[i] = typedExpr
		}
	}
	s.resetRenderColumns(passedThroughRenders, passedThroughCols)
	return nil
}

// constructWindowDefinitions creates window definitions for each window
// function application by combining specific window definition from a
// given window function application with referenced window specifications
// on the SelectClause.
func (p *planner) constructWindowDefinitions(
	ctx context.Context, n *windowNode, sc *tree.SelectClause, s *renderNode,
) error {
	var containsWindowVisitor transform.ContainsWindowVisitor

	// Process each named window specification on the select clause.
	namedWindowSpecs := make(map[string]*tree.WindowDef, len(sc.Window))
	for _, windowDef := range sc.Window {
		name := string(windowDef.Name)
		if _, ok := namedWindowSpecs[name]; ok {
			return pgerror.Newf(pgcode.Windowing, "window %q is already defined", name)
		}
		namedWindowSpecs[name] = windowDef
	}

	// Construct window definitions for each window function application.
	for windowFnIdx, windowFn := range n.funcs {
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
			if renderExpr.ResolvedType().Family() != types.BoolFamily {
				return pgerror.Newf(pgcode.DatatypeMismatch,
					"argument of FILTER must be type boolean, not type %s", renderExpr.ResolvedType(),
				)
			}
			windowFn.filterColIdx = s.addOrReuseRender(col, renderExpr, true /* reuse */)
		}

		// Validate PARTITION BY clause.
		for _, partition := range windowDef.Partitions {
			if containsWindowVisitor.ContainsWindowFunc(partition) {
				return pgerror.Newf(pgcode.Windowing, "window function calls cannot be nested")
			}
			cols, exprs, _, err := p.computeRenderAllowingStars(ctx,
				tree.SelectExpr{Expr: partition}, types.Any, s.sourceInfo, s.ivarHelper,
				autoGenerateRenderOutputName)
			if err != nil {
				return err
			}

			colIdxs := s.addOrReuseRenders(cols, exprs, true)
			windowFn.partitionIdxs = append(windowFn.partitionIdxs, colIdxs...)
		}

		// Validate ORDER BY clause.
		for _, orderBy := range windowDef.OrderBy {
			if containsWindowVisitor.ContainsWindowFunc(orderBy.Expr) {
				return pgerror.Newf(pgcode.Windowing, "window function calls cannot be nested")
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

			colIdxs := s.addOrReuseRenders(cols, exprs, true)
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
				n.funcs[windowFnIdx].ordColTyp = windowDef.OrderBy[0].Expr.(tree.TypedExpr).ResolvedType()
			}
		}

		windowFn.frame = windowDef.Frame
	}

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
		// SELECT rank() OVER (w) FROM t WINDOW w AS (...)
		// We copy the referenced window specification, and modify it if necessary.
		refName = string(def.RefName)
		modifyRef = true
	case def.Name != "":
		// SELECT rank() OVER w FROM t WINDOW w AS (...)
		// Note the lack of parens around w, compared to the first case.
		// We use the referenced window specification directly, without modification.
		refName = string(def.Name)
	}
	if refName == "" {
		return def, nil
	}

	referencedSpec, ok := namedWindowSpecs[refName]
	if !ok {
		return def, pgerror.Newf(pgcode.UndefinedObject, "window %q does not exist", refName)
	}
	if !modifyRef {
		return *referencedSpec, nil
	}

	return tree.OverrideWindowDef(referencedSpec, def)
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
	n.colAndAggContainer = makeWindowNodeColAndAggContainer(s.sourceInfo[0], s.ivarHelper.NumVars())

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
		replaceExprsAboveWindowing := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, _ error) {
			switch t := expr.(type) {
			case *tree.IndexedVar:
				// We add a new render to the source renderNode for each new IndexedVar we
				// see. We also register this mapping in the idxMap.
				col := sqlbase.ResultColumn{
					Name: t.String(),
					Typ:  t.ResolvedType(),
				}
				colIdx := s.addOrReuseRender(col, t, true)

				if iVar, ok := iVars[colIdx]; ok {
					// If we have already created an IndexedVar for this
					// IndexedVar, return it.
					return false, iVar, nil
				}

				// Create a new IndexedVar with index t.Idx.
				colIVar := tree.NewIndexedVar(t.Idx)
				iVars[colIdx] = colIVar
				n.colAndAggContainer.idxMap[t.Idx] = colIdx
				return false, colIVar, nil

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
						return false, iVar, nil
					}

					// Create a new IndexedVar with the next available index.
					idx := n.colAndAggContainer.startAggIdx + n.colAndAggContainer.numAggFuncs
					n.colAndAggContainer.numAggFuncs++
					aggIVar := tree.NewIndexedVar(idx)
					iVars[colIdx] = aggIVar
					n.colAndAggContainer.idxMap[idx] = colIdx
					n.colAndAggContainer.aggFuncs[idx] = t
					return false, aggIVar, nil
				}
				return true, expr, nil
			default:
				return true, expr, nil
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

// setupWindowFunctions makes sure that arguments to all window functions are
// rendered (reusing renders whenever possible) and updates the information
// about the arguments' indices accordingly. Also, it tells each window
// function where to put its output.
func (n *windowNode) setupWindowFunctions(s *renderNode) {
	for _, funcHolder := range n.funcs {
		funcHolder.argsIdxs = make([]uint32, len(funcHolder.args))
		for argPos, argExpr := range funcHolder.args {
			arg := argExpr.(tree.TypedExpr)
			argCol := sqlbase.ResultColumn{
				Name: arg.String(),
				Typ:  arg.ResolvedType(),
			}
			funcHolder.argsIdxs[argPos] = uint32(
				s.addOrReuseRender(argCol, arg, true /* reuse */),
			)
		}
	}

	// Note: we "group" all window functions that have the same PARTITION BY
	// clause and assign output indices so that "groups" are contiguous. This is
	// done to facilitate how windowers compute the window functions - a stage of
	// windowers is responsible for computing a single "group", and windowers
	// will pass through all its input columns and will append window functions'
	// outputs after those.
	nextOutputColIdx := len(s.render)
	outputIdxSet := make([]bool, len(n.funcs))
	for funcIdx, funcHolder := range n.funcs {
		if outputIdxSet[funcIdx] {
			continue
		}
		funcHolder.outputColIdx = nextOutputColIdx
		nextOutputColIdx++
		outputIdxSet[funcIdx] = true
		for i, otherFuncHolder := range n.funcs[funcIdx+1:] {
			if funcHolder.samePartition(otherFuncHolder) {
				otherFuncHolder.outputColIdx = nextOutputColIdx
				nextOutputColIdx++
				outputIdxSet[funcIdx+i+1] = true
			}
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
					v.err = pgerror.Newf(pgcode.Windowing, "window function calls cannot be nested")
					return false, expr
				}
			}

			f := &windowFuncHolder{
				expr:         t,
				args:         t.Exprs,
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

	argsIdxs     []uint32 // indices of the columns that are arguments to the window function
	filterColIdx int      // optional index of filtering column, -1 if no filter
	ordColTyp    *types.T // type of the ordering column, used only in RANGE mode with offsets
	outputColIdx int      // index of the column that the output should be put into

	partitionIdxs  []int
	columnOrdering sqlbase.ColumnOrdering
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
	_ *tree.SemaContext, desired *types.T,
) (tree.TypedExpr, error) {
	return w, nil
}

func (w *windowFuncHolder) Eval(ctx *tree.EvalContext) (tree.Datum, error) {
	panic("windowFuncHolder should not be evaluated directly")
}

func (w *windowFuncHolder) ResolvedType() *types.T {
	return w.expr.ResolvedType()
}

func makeWindowNodeColAndAggContainer(
	sourceInfo *sqlbase.DataSourceInfo, numColVars int,
) windowNodeColAndAggContainer {
	return windowNodeColAndAggContainer{
		idxMap:      make(map[int]int),
		sourceInfo:  sourceInfo,
		aggFuncs:    make(map[int]*tree.FuncExpr),
		startAggIdx: numColVars,
	}
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
