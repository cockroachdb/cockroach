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
	"sort"
	"unsafe"

	"github.com/pkg/errors"

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

	sourceCols int

	// A sparse array holding renders specific to this windowNode. This will contain
	// nil entries for renders that do not contain window functions, and which therefore
	// can be propagated directly from the "wrapped" node.
	windowRender []tree.TypedExpr

	// The window functions handled by this windowNode. computeWindows will populate
	// an entire column in windowValues for each windowFuncHolder, in order.
	funcs []*windowFuncHolder

	// colContainer and aggContainer are IndexedVarContainers that provide indirection
	// to migrate IndexedVars and aggregate functions below the windowing level.
	colContainer windowNodeColContainer
	aggContainer windowNodeAggContainer
	ivarHelper   *tree.IndexedVarHelper

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
//     can be referenced by the window definition of of one or more window function
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
	if containsWindowFn := p.txCtx.WindowFuncInExprs(s.render); !containsWindowFn {
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
	window.sourceCols = len(s.columns)

	if err := p.constructWindowDefinitions(ctx, window, n, s); err != nil {
		return nil, err
	}

	window.replaceIndexVarsAndAggFuncs(s)

	acc := p.EvalContext().Mon.MakeBoundAccount()
	window.run.wrappedRenderVals = sqlbase.NewRowContainer(
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
	wrappedRenderVals *sqlbase.RowContainer

	// The populated values for this windowNode.
	values    valuesNode
	populated bool

	windowValues [][]tree.Datum
	curRowIdx    int

	windowsAcc mon.BoundAccount
}

func (n *windowNode) startExec(params runParams) error {
	n.run.windowsAcc = params.EvalContext().Mon.MakeBoundAccount()
	return nil
}

func (n *windowNode) Next(params runParams) (bool, error) {
	for !n.run.populated {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		next, err := n.plan.Next(params)
		if err != nil {
			return false, err
		}
		if !next {
			n.run.populated = true
			if err := n.computeWindows(params.ctx, params.EvalContext()); err != nil {
				return false, err
			}
			n.run.values.rows = sqlbase.NewRowContainer(
				params.EvalContext().Mon.MakeBoundAccount(),
				sqlbase.ColTypeInfoFromResCols(n.run.values.columns),
				n.run.wrappedRenderVals.Len(),
			)
			if err := n.populateValues(params.ctx, params.EvalContext()); err != nil {
				return false, err
			}
			break
		}

		values := n.plan.Values()
		if _, err := n.run.wrappedRenderVals.AddRow(params.ctx, values); err != nil {
			return false, err
		}
	}

	return n.run.values.Next(params)
}

func (n *windowNode) Values() tree.Datums {
	return n.run.values.Values()
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
			return errors.Errorf("window %q is already defined", name)
		}
		namedWindowSpecs[name] = windowDef
	}

	// Construct window definitions for each window function application.
	for _, windowFn := range n.funcs {
		windowDef, err := constructWindowDef(*windowFn.expr.WindowDef, namedWindowSpecs)
		if err != nil {
			return err
		}

		// Validate PARTITION BY clause.
		for _, partition := range windowDef.Partitions {
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

		windowFn.windowDef = windowDef
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
		return def, errors.Errorf("window %q does not exist", refName)
	}
	if !modifyRef {
		return *referencedSpec, nil
	}

	// referencedSpec.Partitions is always used.
	if len(def.Partitions) > 0 {
		return def, errors.Errorf("cannot override PARTITION BY clause of window %q", refName)
	}
	def.Partitions = referencedSpec.Partitions

	// referencedSpec.OrderBy is used if set.
	if len(referencedSpec.OrderBy) > 0 {
		if len(def.OrderBy) > 0 {
			return def, errors.Errorf("cannot override ORDER BY clause of window %q", refName)
		}
		def.OrderBy = referencedSpec.OrderBy
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
//    source plan's renders: [a, max(b), d, f]
//    window plan's renders: [nil, nil, c + max(d) + first_value(@3) OVER (PARTITION BY @4)]
//
//   After replaceIndexVarsAndAggFuncs is run:
//    source plan's renders: [a, max(b), d, f, c, max(d)]
//    window plan's renders: [nil, nil, @5 + @6 + first_value(@3) OVER (PARTITION BY @4)]
//
func (n *windowNode) replaceIndexVarsAndAggFuncs(s *renderNode) {
	n.colContainer = windowNodeColContainer{
		windowNodeIvarContainer: makeWindowNodeIvarContainer(&n.run),
		sourceInfo:              s.sourceInfo[0],
	}
	ivarHelper := tree.MakeIndexedVarHelper(&n.colContainer, s.ivarHelper.NumVars())
	n.ivarHelper = &ivarHelper

	n.aggContainer = windowNodeAggContainer{
		windowNodeIvarContainer: makeWindowNodeIvarContainer(&n.run),
		aggFuncs:                make(map[int]*tree.FuncExpr),
	}
	// The number of aggregation functions that need to be replaced with IndexedVars
	// is unknown, so we collect them here and bind them to an IndexedVarHelper later.
	// We use a map indexed by render index to leverage addOrMergeRender's deduplication
	// of identical aggregate functions.
	aggIVars := make(map[int]*tree.IndexedVar)

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
				colIdx := s.addOrReuseRender(col, t, true)
				n.colContainer.idxMap[t.Idx] = colIdx
				return nil, false, ivarHelper.IndexedVar(t.Idx)
			case *tree.FuncExpr:
				// All window function applications will have been replaced by
				// windowFuncHolders at this point, so if we see an aggregate
				// function in the window renders, it is above a window function.
				if t.GetAggregateConstructor() != nil {
					// We add a new render to the source renderNode for each new aggregate
					// function we see.
					col := sqlbase.ResultColumn{Name: t.String(), Typ: t.ResolvedType()}
					colIdx := s.addOrReuseRender(col, t, true)

					if iVar, ok := aggIVars[colIdx]; ok {
						// If we have already created an IndexedVar for this aggregate
						// function, return it.
						return nil, false, iVar
					}

					// Create a new IndexedVar with the next available index.
					idx := len(n.aggContainer.idxMap)
					aggIVar := tree.NewIndexedVar(idx)
					aggIVars[colIdx] = aggIVar
					n.aggContainer.idxMap[idx] = colIdx
					n.aggContainer.aggFuncs[idx] = t
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

	if len(aggIVars) > 0 {
		// Now that we know how many aggregate functions there were, we can create
		// an IndexedVarHelper and bind each of the corresponding IndexedVars to
		// the helper.
		aggHelper := tree.MakeIndexedVarHelper(&n.aggContainer, len(aggIVars))
		for _, ivar := range aggIVars {
			// The ivars above have been created with a nil container, and
			// therefore they are guaranteed to be modified in-place by
			// BindIfUnbound().
			if newIvar, err := aggHelper.BindIfUnbound(ivar); err != nil {
				panic(err)
			} else if newIvar != ivar {
				panic(fmt.Sprintf("unexpected binding: %v, expected: %v", newIvar, ivar))
			}
		}
	}
}

type partitionSorter struct {
	evalCtx       *tree.EvalContext
	rows          []tree.IndexedRow
	windowDefVals *sqlbase.RowContainer
	ordering      sqlbase.ColumnOrdering
}

// partitionSorter implements the sort.Interface interface.
func (n *partitionSorter) Len() int           { return len(n.rows) }
func (n *partitionSorter) Swap(i, j int)      { n.rows[i], n.rows[j] = n.rows[j], n.rows[i] }
func (n *partitionSorter) Less(i, j int) bool { return n.Compare(i, j) < 0 }

// partitionSorter implements the peerGroupChecker interface.
func (n *partitionSorter) InSameGroup(i, j int) bool { return n.Compare(i, j) == 0 }

func (n *partitionSorter) Compare(i, j int) int {
	ra, rb := n.rows[i], n.rows[j]
	defa, defb := n.windowDefVals.At(ra.Idx), n.windowDefVals.At(rb.Idx)
	for _, o := range n.ordering {
		da := defa[o.ColIdx]
		db := defb[o.ColIdx]
		if c := da.Compare(n.evalCtx, db); c != 0 {
			if o.Direction != encoding.Ascending {
				return -c
			}
			return c
		}
	}
	return 0
}

type allPeers struct{}

// allPeers implements the peerGroupChecker interface.
func (allPeers) InSameGroup(i, j int) bool { return true }

// peerGroupChecker can check if a pair of row indexes within a partition are
// in the same peer group.
type peerGroupChecker interface {
	InSameGroup(i, j int) bool
}

// computeWindows populates n.run.windowValues, adding a column of values to the
// 2D-slice for each window function in n.funcs. This needs to be performed
// all at once because in order to compute the result of a window function
// for any single row, we need to have access to all rows at the same time.
//
// The state shared between rows while computing all window functions for a
// single row is not easily extracted for two reasons:
// 1. window functions can define different partitioning attributes
// 2. window functions can define different column orderings within partitions
//
// The general structure is:
//   for each window function
//     compute partitions
//     for each partition
//       sort partition
//       evaluate window frame over partition per cell, keeping track of peer groups
func (n *windowNode) computeWindows(ctx context.Context, evalCtx *tree.EvalContext) error {
	rowCount := n.run.wrappedRenderVals.Len()
	if rowCount == 0 {
		return nil
	}

	windowCount := len(n.funcs)

	winValSz := uintptr(rowCount) * unsafe.Sizeof([]tree.Datum{})
	winAllocSz := uintptr(rowCount*windowCount) * unsafe.Sizeof(tree.Datum(nil))
	if err := n.run.windowsAcc.Grow(ctx, int64(winValSz+winAllocSz)); err != nil {
		return err
	}

	n.run.windowValues = make([][]tree.Datum, rowCount)
	windowAlloc := make([]tree.Datum, rowCount*windowCount)
	for i := range n.run.windowValues {
		n.run.windowValues[i] = windowAlloc[i*windowCount : (i+1)*windowCount]
	}

	var scratchBytes []byte
	var scratchDatum []tree.Datum
	for windowIdx, windowFn := range n.funcs {
		partitions := make(map[string][]tree.IndexedRow)

		if len(windowFn.partitionIdxs) == 0 {
			// If no partition indexes are included for the window function, all
			// rows are added to the same partition, which need to be pre-allocated.
			sz := int64(uintptr(rowCount) * unsafe.Sizeof(tree.IndexedRow{}))
			if err := n.run.windowsAcc.Grow(ctx, sz); err != nil {
				return err
			}
			partitions[""] = make([]tree.IndexedRow, rowCount)
		}

		if num := len(windowFn.partitionIdxs); num > cap(scratchDatum) {
			sz := int64(uintptr(num) * unsafe.Sizeof(tree.Datum(nil)))
			if err := n.run.windowsAcc.Grow(ctx, sz); err != nil {
				return err
			}
			scratchDatum = make([]tree.Datum, num)
		} else {
			scratchDatum = scratchDatum[:num]
		}

		// Partition rows into separate partitions based on hash values of the
		// window function's PARTITION BY attribute.
		//
		// TODO(nvanbenschoten): Window functions with the same window definition
		// can share partition and sorting work.
		// See Cao et al. [http://vldb.org/pvldb/vol5/p1244_yucao_vldb2012.pdf]
		for rowI := 0; rowI < rowCount; rowI++ {
			row := n.run.wrappedRenderVals.At(rowI)
			sourceVals := row[:n.sourceCols]
			entry := tree.IndexedRow{Idx: rowI, Row: sourceVals}
			if len(windowFn.partitionIdxs) == 0 {
				// If no partition indexes are included for the window function, all
				// rows are added to the same partition.
				partitions[""][rowI] = entry
			} else {
				// If the window function has partition indexes, we hash the values of each
				// of these indexes for each row, and partition based on this hashed value.
				for i, idx := range windowFn.partitionIdxs {
					scratchDatum[i] = row[idx]
				}

				encoded, err := sqlbase.EncodeDatums(scratchBytes, scratchDatum)
				if err != nil {
					return err
				}

				sz := int64(uintptr(len(encoded)) + unsafe.Sizeof(entry))
				if err := n.run.windowsAcc.Grow(ctx, sz); err != nil {
					return err
				}
				partitions[string(encoded)] = append(partitions[string(encoded)], entry)
				scratchBytes = encoded[:0]
			}
		}

		// For each partition, perform necessary sorting based on the window function's
		// ORDER BY attribute. After this, perform the window function computation for
		// each tuple and save the result in n.run.windowValues.
		//
		// TODO(nvanbenschoten)
		// - Investigate inter- and intra-partition parallelism
		// - Investigate more efficient aggregation techniques
		//   * Removable Cumulative
		//   * Segment Tree
		// See Leis et al. [http://www.vldb.org/pvldb/vol8/p1058-leis.pdf]
		for _, partition := range partitions {
			// TODO(nvanbenschoten): Handle framing here. Right now we only handle the default
			// framing option of RANGE UNBOUNDED PRECEDING. With ORDER BY, this sets the frame
			// to be all rows from the partition start up through the current row's last ORDER BY
			// peer. Without ORDER BY, all rows of the partition are included in the window frame,
			// since all rows become peers of the current row. Once we add better framing support,
			// we should flesh this logic out more.
			builtin := windowFn.expr.GetWindowConstructor()(evalCtx)
			defer builtin.Close(ctx, evalCtx)

			// Since we only support two types of window frames (see TODO above), we only
			// need two possible types of peerGroupChecker's to help determine peer groups
			// for given tuples.
			var peerGrouper peerGroupChecker
			if windowFn.columnOrdering != nil {
				// If an ORDER BY clause is provided, order the partition and use the
				// sorter as our peerGroupChecker.
				sorter := &partitionSorter{
					evalCtx:       evalCtx,
					rows:          partition,
					windowDefVals: n.run.wrappedRenderVals,
					ordering:      windowFn.columnOrdering,
				}
				// The sort needs to be deterministic because multiple window functions with
				// syntactically equivalent ORDER BY clauses in their window definitions
				// need to be guaranteed to be evaluated in the same order, even if the
				// ORDER BY *does not* uniquely determine an ordering. In the future, this
				// could be guaranteed by only performing a single pass over a sorted partition
				// for functions with syntactically equivalent PARTITION BY and ORDER BY clauses.
				sort.Sort(sorter)
				peerGrouper = sorter
			} else {
				// If no ORDER BY clause is provided, all rows in the partition are peers.
				peerGrouper = allPeers{}
			}

			// Iterate over peer groups within partition using a window frame.
			frame := tree.WindowFrame{
				Rows:        partition,
				ArgIdxStart: windowFn.argIdxStart,
				ArgCount:    windowFn.argCount,
				RowIdx:      0,
			}
			for frame.RowIdx < len(partition) {
				// Compute the size of the current peer group.
				frame.FirstPeerIdx = frame.RowIdx
				frame.PeerRowCount = 1
				for ; frame.FirstPeerIdx+frame.PeerRowCount < len(partition); frame.PeerRowCount++ {
					cur := frame.FirstPeerIdx + frame.PeerRowCount
					if !peerGrouper.InSameGroup(cur, cur-1) {
						break
					}
				}

				// Perform calculations on each row in the current peer group.
				for ; frame.RowIdx < frame.FirstPeerIdx+frame.PeerRowCount; frame.RowIdx++ {
					res, err := builtin.Compute(ctx, evalCtx, frame)
					if err != nil {
						return err
					}

					// This may overestimate, because WindowFuncs may perform internal caching.
					sz := res.Size()
					if err := n.run.windowsAcc.Grow(ctx, int64(sz)); err != nil {
						return err
					}

					// Save result into n.run.windowValues, indexed by original row index.
					valRowIdx := partition[frame.RowIdx].Idx
					n.run.windowValues[valRowIdx][windowIdx] = res
				}
			}
		}
	}
	return nil
}

// populateValues populates n.run.values with final datum values after computing
// window result values in n.run.windowValues.
func (n *windowNode) populateValues(ctx context.Context, evalCtx *tree.EvalContext) error {
	rowCount := n.run.wrappedRenderVals.Len()
	row := make(tree.Datums, len(n.windowRender))
	for i := 0; i < rowCount; i++ {
		wrappedRow := n.run.wrappedRenderVals.At(i)

		n.run.curRowIdx = i // Point all windowFuncHolders to the correct row values.
		curColIdx := 0
		curFnIdx := 0
		for j := range row {
			if curWindowRender := n.windowRender[j]; curWindowRender == nil {
				// If the windowRender at this index is nil, propagate the datum
				// directly from the wrapped planNode. It wasn't changed by windowNode.
				row[j] = wrappedRow[curColIdx]
				curColIdx++
			} else {
				// If the windowRender is not nil, ignore 0 or more columns from the wrapped
				// planNode. These were used as arguments to window functions all beneath
				// a single windowRender.
				// SELECT rank() over () from t; -> ignore 0 from wrapped values
				// SELECT (rank() over () + avg(b) over ()) from t; -> ignore 1 from wrapped values
				// SELECT (avg(a) over () + avg(b) over ()) from t; -> ignore 2 from wrapped values
				for ; curFnIdx < len(n.funcs); curFnIdx++ {
					windowFn := n.funcs[curFnIdx]
					if windowFn.argIdxStart != curColIdx {
						break
					}
					curColIdx += windowFn.argCount
				}
				// Instead, we evaluate the current window render, which depends on at least
				// one window function, at the given row.
				evalCtx.PushIVarContainer(&n.colContainer)
				res, err := curWindowRender.Eval(evalCtx)
				evalCtx.PopIVarContainer()
				if err != nil {
					return err
				}
				row[j] = res
			}
		}

		if _, err := n.run.values.rows.AddRow(ctx, row); err != nil {
			return err
		}
	}

	// Done using the output of computeWindows, release memory and clear
	// accounts.
	n.run.wrappedRenderVals.Close(ctx)
	n.run.wrappedRenderVals = nil
	n.run.windowValues = nil
	n.run.windowsAcc.Close(ctx)

	return nil
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
				v.err = errors.Errorf("aggregate function calls cannot contain window function "+
					"call %s()", &t.Func)
				return false, expr
			}

			// Make sure this window function does not contain another window function.
			for _, argExpr := range t.Exprs {
				if v.subWindowVisitor.ContainsWindowFunc(argExpr) {
					v.err = fmt.Errorf("window function calls cannot be nested under %s()", &t.Func)
					return false, expr
				}
			}

			f := &windowFuncHolder{
				expr:     t,
				args:     t.Exprs,
				argCount: len(t.Exprs),
				window:   v.n,
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

type windowFuncHolder struct {
	window *windowNode

	expr *tree.FuncExpr
	args []tree.Expr

	funcIdx     int // index of the windowFuncHolder in window.funcs
	argIdxStart int // index of the window function's first arguments in window.wrappedValues
	argCount    int // number of arguments taken by the window function

	windowDef      tree.WindowDef
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

func (w *windowFuncHolder) TypeCheck(_ *tree.SemaContext, desired types.T) (tree.TypedExpr, error) {
	return w, nil
}

func (w *windowFuncHolder) Eval(ctx *tree.EvalContext) (tree.Datum, error) {
	// Index into the windowValues computed in windowNode.computeWindows
	// to determine the Datum value to return. Evaluating this datum
	// is almost certainly the identity.
	return w.window.run.windowValues[w.window.run.curRowIdx][w.funcIdx].Eval(ctx)
}

func (w *windowFuncHolder) ResolvedType() types.T {
	return w.expr.ResolvedType()
}

// windowNodeIvarContainer is an abstract implementation of the
// tree.IndexedVarContainer interface. It handles evaluation of IndexedVars,
// but needs to be extended to handle formatting and type introspection
// for its IndexedVars.
type windowNodeIvarContainer struct {
	n *windowRun

	// idxMap maps the index of IndexedVars created in replaceIndexVarsAndAggFuncs
	// to the index their corresponding results in this container. It permits us to
	// add a single render to the source plan per unique expression.
	idxMap map[int]int
}

func makeWindowNodeIvarContainer(n *windowRun) windowNodeIvarContainer {
	return windowNodeIvarContainer{
		n:      n,
		idxMap: make(map[int]int),
	}
}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (ic *windowNodeIvarContainer) IndexedVarEval(
	idx int, ctx *tree.EvalContext,
) (tree.Datum, error) {
	// Determine which row in the buffered values to evaluate.
	curRow := ic.n.wrappedRenderVals.At(ic.n.curRowIdx)
	// Determine which value in that row to evaluate.
	curVal := curRow[ic.idxMap[idx]]
	return curVal.Eval(ctx)
}

// windowNodeColContainer is a IndexedVarContainer providing indirection for
// IndexedVars found above the windowing level. See replaceIndexVarsAndAggFuncs.
type windowNodeColContainer struct {
	windowNodeIvarContainer

	// sourceInfo contains information on the for the IndexedVars from the
	// source plan where they were originally created.
	sourceInfo *sqlbase.DataSourceInfo
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (cc *windowNodeColContainer) IndexedVarResolvedType(idx int) types.T {
	return cc.sourceInfo.SourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (cc *windowNodeColContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	// Avoid duplicating the type annotation by calling .Format directly.
	return cc.sourceInfo.NodeFormatter(idx)
}

// windowNodeAggContainer is a IndexedVarContainer providing indirection for
// aggregate functions found above the windowing level. See replaceIndexVarsAndAggFuncs.
type windowNodeAggContainer struct {
	windowNodeIvarContainer

	// aggFuncs maps the index of IndexedVars to their corresponding aggregate function.
	aggFuncs map[int]*tree.FuncExpr
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (ac *windowNodeAggContainer) IndexedVarResolvedType(idx int) types.T {
	return ac.aggFuncs[idx].ResolvedType()
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (ac *windowNodeAggContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	// Avoid duplicating the type annotation by calling .Format directly.
	return ac.aggFuncs[idx]
}
