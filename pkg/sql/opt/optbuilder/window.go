// Copyright 2019 The Cockroach Authors.
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

package optbuilder

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// windowInfo stores information about a window function call.
type windowInfo struct {
	*tree.FuncExpr

	def memo.FunctionPrivate

	// partition is the set of expressions used in the PARTITION BY clause.
	partition []tree.TypedExpr

	// orderBy is the set of expressions used in the ORDER BY clause.
	orderBy tree.OrderBy

	// col is the output column of the aggregation.
	col *scopeColumn

	// frame is the window frame this window function is computed relative to.
	frame *tree.WindowFrame
}

// Walk is part of the tree.Expr interface.
func (w *windowInfo) Walk(v tree.Visitor) tree.Expr {
	return w
}

// TypeCheck is part of the tree.Expr interface.
func (w *windowInfo) TypeCheck(ctx *tree.SemaContext, desired *types.T) (tree.TypedExpr, error) {
	if _, err := w.FuncExpr.TypeCheck(ctx, desired); err != nil {
		return nil, err
	}
	return w, nil
}

// Eval is part of the tree.TypedExpr interface.
func (w *windowInfo) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(pgerror.AssertionFailedf("windowInfo must be replaced before evaluation"))
}

var _ tree.Expr = &windowInfo{}
var _ tree.TypedExpr = &windowInfo{}

var unboundedEndBound = &tree.WindowFrameBound{BoundType: tree.UnboundedFollowing}
var unboundedStartBound = &tree.WindowFrameBound{BoundType: tree.UnboundedPreceding}
var defaultStartBound = &tree.WindowFrameBound{BoundType: tree.UnboundedPreceding}
var defaultEndBound = &tree.WindowFrameBound{BoundType: tree.CurrentRow}

// buildWindow adds any window functions on top of the expression.
func (b *Builder) buildWindow(outScope *scope, inScope *scope) {
	if len(inScope.windows) == 0 {
		return
	}
	argLists := make([][]opt.ScalarExpr, len(inScope.windows))
	partitions := make([]opt.ColSet, len(inScope.windows))
	orderings := make([]physical.OrderingChoice, len(inScope.windows))
	windowFrames := make([]tree.WindowFrame, len(inScope.windows))
	argScope := outScope.push()
	argScope.appendColumnsFromScope(outScope)
	// The arguments to a given window function need to be columns in the input
	// relation. Build a projection that produces those values to go underneath
	// the window functions.
	// TODO(justin): this is unfortunate in common cases where the arguments are
	// constant, since we'll be projecting an extra column in every row.  It
	// would be good if the windower supported being specified with constant
	// values.
	for i := range inScope.windows {
		w := inScope.windows[i].expr.(*windowInfo)
		argExprs := b.getTypedWindowArgs(w)

		argLists[i] = b.constructArgs(argExprs, i, w.def.Name, argScope, inScope)

		// PARTITION BY (a, b) => PARTITION BY a, b
		cols := flattenTuples(w.partition)
		for j, e := range cols {
			col := argScope.findExistingCol(e)
			if col == nil {
				col = b.synthesizeColumn(
					argScope,
					fmt.Sprintf("%s_%d_partition_%d", w.def.Name, i+1, j+1),
					e.ResolvedType(),
					e,
					b.buildScalar(e, inScope, nil, nil, nil),
				)
			}
			partitions[i].Add(int(col.id))
		}

		ord := make(opt.Ordering, 0, len(w.orderBy))
		for j, t := range w.orderBy {
			// ORDER BY (a, b) => ORDER BY a, b
			cols := flattenTuples([]tree.TypedExpr{t.Expr.(tree.TypedExpr)})

			for _, e := range cols {
				col := argScope.findExistingCol(e)
				if col == nil {
					col = b.synthesizeColumn(
						argScope,
						fmt.Sprintf("%s_%d_orderby_%d", w.def.Name, i+1, j+1),
						e.ResolvedType(),
						e,
						b.buildScalar(e, inScope, nil, nil, nil),
					)
				}
				ord = append(ord, opt.MakeOrderingColumn(col.id, t.Direction == tree.Descending))
			}
		}
		orderings[i].FromOrdering(ord)

		if w.frame != nil {
			windowFrames[i] = *w.frame
		}

		// Fill this in with the default so that we don't need nil checks
		// elsewhere.
		if windowFrames[i].Bounds.StartBound == nil {
			windowFrames[i].Bounds.StartBound = defaultStartBound
		}
		if windowFrames[i].Bounds.EndBound == nil {
			// Some sources appear to say that the presence of an ORDER BY changes
			// this between CURRENT ROW and UNBOUNDED FOLLOWING, but in reality, what
			// CURRENT ROW means is the *last row which is a peer of this row* (a
			// peer being a row which agrees on the ordering columns), so if there is
			// no ORDER BY, every row is a peer with every other row in its
			// partition, which means the CURRENT ROW and UNBOUNDED FOLLOWING are
			// equivalent.
			windowFrames[i].Bounds.EndBound = defaultEndBound
		}
	}

	b.constructProjectForScope(outScope, argScope)
	outScope.expr = argScope.expr

	// frames accumulates the set of distinct window frames we're computing over
	// so that we can group functions over the same partition and ordering.
	frames := make([]memo.WindowExpr, 0, len(inScope.windows))
	for i := range inScope.windows {
		w := inScope.windows[i].expr.(*windowInfo)

		frameIdx := b.findAppropriateFramePosition(&frames, partitions[i], orderings[i])

		frames[frameIdx].Windows = append(frames[frameIdx].Windows,
			memo.WindowsItem{
				Function: b.constructWindowFn(w.def.Name, argLists[i]),
				WindowsItemPrivate: memo.WindowsItemPrivate{
					Frame:      &windowFrames[i],
					ColPrivate: memo.ColPrivate{Col: w.col.id},
				},
			},
		)
	}

	for _, f := range frames {
		outScope.expr = b.factory.ConstructWindow(outScope.expr, f.Windows, &f.WindowPrivate)
	}
}

// buildAggregationAsWindow builds the aggregation operators as window functions.
// Returns the output scope for the aggregation operation.
func (b *Builder) buildAggregationAsWindow(
	groupingCols []scopeColumn, having opt.ScalarExpr, fromScope *scope,
) *scope {
	aggOutScope := fromScope.groupby.aggOutScope

	aggInfos := aggOutScope.groupby.aggs

	// Construct the aggregation operators.
	aggCols := aggOutScope.getAggregateCols()

	var groupingColSet opt.ColSet
	for i := range groupingCols {
		groupingColSet.Add(int(groupingCols[i].id))
	}
	argLists := make([][]opt.ScalarExpr, len(aggInfos))
	partitions := make([]opt.ColSet, len(aggInfos))
	orderings := make([]physical.OrderingChoice, len(aggInfos))
	windowFrames := make([]tree.WindowFrame, len(aggInfos))
	argScope := fromScope.push()
	argScope.appendColumnsFromScope(fromScope)
	for i, agg := range aggInfos {
		argExprs := b.getTypedArgs(&agg)
		argLists[i] = b.constructArgs(argExprs, i, agg.def.Name, argScope, fromScope)

		// Build appropriate partitions.
		partitions[i] = groupingColSet.Copy()

		// Build appropriate orderings.
		if !agg.IsCommutative() {
			ord := make(opt.Ordering, 0, len(agg.OrderBy))
			for j, t := range agg.OrderBy {
				te := argScope.resolveAndRequireType(t.Expr, types.Any)
				cols := flattenTuples([]tree.TypedExpr{te})

				for _, e := range cols {
					col := argScope.findExistingCol(e)
					if col == nil {
						col = b.synthesizeColumn(
							argScope,
							fmt.Sprintf("%s_%d_orderby_%d", agg.def.Name, i+1, j+1),
							te.ResolvedType(),
							te,
							b.buildScalar(e, fromScope, nil, nil, nil),
						)
					}
					ord = append(ord, opt.MakeOrderingColumn(col.id, t.Direction == tree.Descending))
				}
			}
			orderings[i].FromOrdering(ord)
		}

		// Build appropriate window frames.
		windowFrames[i].Bounds.StartBound = unboundedStartBound
		windowFrames[i].Bounds.EndBound = unboundedEndBound
	}

	b.constructProjectForScope(fromScope, argScope)
	newExpr := argScope.expr

	// frames accumulates the set of distinct window frames we're computing over
	// so that we can group functions over the same partition and ordering.
	frames := make([]memo.WindowExpr, 0, len(aggInfos))
	for i, agg := range aggInfos {
		frameIdx := b.findAppropriateFramePosition(&frames, partitions[i], orderings[i])

		frames[frameIdx].Windows = append(frames[frameIdx].Windows,
			memo.WindowsItem{
				Function: b.constructAggregate(agg.def.Name, argLists[i]),
				WindowsItemPrivate: memo.WindowsItemPrivate{
					Frame:      &windowFrames[i],
					ColPrivate: memo.ColPrivate{Col: agg.col.id},
				},
			},
		)
	}
	for _, f := range frames {
		newExpr = b.factory.ConstructWindow(newExpr, f.Windows, &f.WindowPrivate)
	}

	// Use a grouping ColSet that includes the columns the window function added.
	windowGroupingColSet := groupingColSet.Copy()
	for i := range aggCols {
		windowGroupingColSet.Add(int(aggCols[i].id))
	}

	private := memo.GroupingPrivate{GroupingCols: windowGroupingColSet}
	private.Ordering.FromOrderingWithOptCols(nil, windowGroupingColSet)
	// TODO(ridwanmsharif): Fix this. For some reason the scalar on top of the window functions
	// isn't working.
	if groupingColSet.Empty() && false {
		aggs := make(memo.AggregationsExpr, 0, len(aggCols))
		for i := range aggCols {
			aggs = append(aggs, memo.AggregationsItem{
				Agg:        b.factory.ConstructConstAgg(b.factory.ConstructVariable(aggCols[i].id)),
				ColPrivate: memo.ColPrivate{Col: aggCols[i].id},
			})
		}
		aggOutScope.expr = b.factory.ConstructScalarGroupBy(newExpr, aggs, &private)
	} else {
		aggOutScope.expr = b.factory.ConstructDistinctOn(newExpr, memo.EmptyAggregationsExpr, &private)
	}

	// Wrap with having filter if it exists.
	if having != nil {
		input := aggOutScope.expr.(memo.RelExpr)
		filters := memo.FiltersExpr{{Condition: having}}
		aggOutScope.expr = b.factory.ConstructSelect(input, filters)
	}
	return aggOutScope
}

// getTypedWindowArgs returns the arguments to the window function as
// a []tree.TypedExpr. In the case of arguments with default values, it
// fills in the values if they are missing.
// TODO(justin): this is a bit of a hack to get around the fact that we don't
// have a good way to represent optional values in the opt tree, figure out
// a better way to do this. In particular this is bad because it results in us
// projecting the default argument to some window functions when we could just
// not do that projection.
func (b *Builder) getTypedWindowArgs(w *windowInfo) []tree.TypedExpr {
	argExprs := make([]tree.TypedExpr, len(w.Exprs))
	for i, pexpr := range w.Exprs {
		argExprs[i] = pexpr.(tree.TypedExpr)
	}

	switch w.def.Name {
	// The second argument of {lead,lag} is 1 by default, and the third argument
	// is NULL by default.
	case "lead", "lag":
		if len(argExprs) < 2 {
			argExprs = append(argExprs, tree.NewDInt(1))
		}
		if len(argExprs) < 3 {
			null, err := tree.ReType(tree.DNull, argExprs[0].ResolvedType())
			if err != nil {
				panic(pgerror.NewAssertionErrorWithWrappedErrf(err, "error calling tree.ReType"))
			}
			argExprs = append(argExprs, null)
		}
	}

	return argExprs
}

// getTypedArgs returns the arguments to the aggregate function as a
// []tree.TypedExpr similar to getTypedWindowArgs.
func (b *Builder) getTypedArgs(agg *aggregateInfo) []tree.TypedExpr {
	argExprs := make([]tree.TypedExpr, len(agg.Exprs))
	for i, pexpr := range agg.Exprs {
		argExprs[i] = pexpr.(tree.TypedExpr)
	}
	return argExprs
}

// Construct the argExprs into a memo.ScalarListExpr.
func (b *Builder) constructArgs(
	argExprs []tree.TypedExpr, windowIndex int, funcName string, argScope, fromScope *scope,
) memo.ScalarListExpr {
	argList := make(memo.ScalarListExpr, len(argExprs))
	for j, a := range argExprs {
		col := argScope.findExistingCol(a)
		if col == nil {
			col = b.synthesizeColumn(
				argScope,
				fmt.Sprintf("%s_%d_arg%d", funcName, windowIndex+1, j+1),
				a.ResolvedType(),
				a,
				b.buildScalar(a, fromScope, nil, nil, nil),
			)
		}
		argList[j] = b.factory.ConstructVariable(col.id)
	}
	return argList
}

// findAppropriateFramePosition finds a frame position to which a window of the
// given partition and ordering can be added to. If no such frame is found, a
// new one is made.
func (b *Builder) findAppropriateFramePosition(
	frames *[]memo.WindowExpr, partition opt.ColSet, ordering physical.OrderingChoice,
) int {
	frameIdx := -1

	// The number of window functions is probably fairly small, so do an O(n^2)
	// loop.
	// TODO(justin): make this faster.
	// TODO(justin): consider coalescing frames with compatible orderings.
	for j := range *frames {
		if partition.Equals((*frames)[j].Partition) &&
			ordering.Equals(&(*frames)[j].Ordering) {
			frameIdx = j
			break
		}
	}

	// If we can't reuse an existing frame, make a new one.
	if frameIdx == -1 {
		*frames = append(*frames, memo.WindowExpr{
			WindowPrivate: memo.WindowPrivate{
				Partition: partition,
				Ordering:  ordering,
			},
			Windows: memo.WindowsExpr{},
		})
		frameIdx = len(*frames) - 1
	}

	return frameIdx
}
