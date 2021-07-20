// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// windowInfo stores information about a window function call.
type windowInfo struct {
	*tree.FuncExpr

	def memo.FunctionPrivate

	// col is the output column of the aggregation.
	col *scopeColumn
}

// Walk is part of the tree.Expr interface.
func (w *windowInfo) Walk(v tree.Visitor) tree.Expr {
	return w
}

// TypeCheck is part of the tree.Expr interface.
func (w *windowInfo) TypeCheck(
	ctx context.Context, semaCtx *tree.SemaContext, desired *types.T,
) (tree.TypedExpr, error) {
	if _, err := w.FuncExpr.TypeCheck(ctx, semaCtx, desired); err != nil {
		return nil, err
	}
	return w, nil
}

// Eval is part of the tree.TypedExpr interface.
func (w *windowInfo) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(errors.AssertionFailedf("windowInfo must be replaced before evaluation"))
}

var _ tree.Expr = &windowInfo{}
var _ tree.TypedExpr = &windowInfo{}

var unboundedStartBound = &tree.WindowFrameBound{BoundType: tree.UnboundedPreceding}
var unboundedEndBound = &tree.WindowFrameBound{BoundType: tree.UnboundedFollowing}
var defaultStartBound = &tree.WindowFrameBound{BoundType: tree.UnboundedPreceding}
var defaultEndBound = &tree.WindowFrameBound{BoundType: tree.CurrentRow}

// buildWindow adds any window functions on top of the expression.
func (b *Builder) buildWindow(outScope *scope, inScope *scope) {
	if len(inScope.windows) == 0 {
		return
	}

	argLists := make([][]opt.ScalarExpr, len(inScope.windows))
	partitions := make([]opt.ColSet, len(inScope.windows))
	orderings := make([]props.OrderingChoice, len(inScope.windows))
	filterCols := make([]opt.ColumnID, len(inScope.windows))
	defs := make([]*tree.WindowDef, len(inScope.windows))
	windowFrames := make([]tree.WindowFrame, len(inScope.windows))
	argScope := outScope.push()
	argScope.appendColumnsFromScope(outScope)

	// The arguments to a given window function need to be columns in the input
	// relation. Build a projection that produces those values to go underneath
	// the window functions.
	// TODO(justin): this is unfortunate in common cases where the arguments are
	// constant, since we'll be projecting an extra column in every row. It
	// would be good if the windower supported being specified with constant
	// values.
	for i := range inScope.windows {
		w := inScope.windows[i].expr.(*windowInfo)

		def := w.WindowDef
		defs[i] = def

		argExprs := b.getTypedWindowArgs(w)

		// Build the appropriate arguments.
		argLists[i] = b.buildWindowArgs(argExprs, i, w.def.Name, inScope, argScope)

		// Build appropriate partitions.
		partitions[i] = b.buildWindowPartition(def.Partitions, i, w.def.Name, inScope, argScope)

		// Build appropriate orderings.
		ord := b.buildWindowOrdering(def.OrderBy, i, w.def.Name, inScope, argScope)
		orderings[i].FromOrdering(ord)

		if def.Frame != nil {
			windowFrames[i] = *def.Frame
		}

		if w.Filter != nil {
			col := b.buildFilterCol(w.Filter, i, w.def.Name, inScope, argScope)
			filterCols[i] = col.id
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

	var referencedCols opt.ColSet
	// frames accumulates the set of distinct window frames we're computing over
	// so that we can group functions over the same partition and ordering.
	frames := make([]memo.WindowExpr, 0, len(inScope.windows))
	for i := range inScope.windows {
		w := inScope.windows[i].expr.(*windowInfo)

		frameIdx := b.findMatchingFrameIndex(&frames, partitions[i], orderings[i])

		fn := b.constructWindowFn(w.def.Name, argLists[i])

		if windowFrames[i].Bounds.StartBound.OffsetExpr != nil {
			fn = b.factory.ConstructWindowFromOffset(
				fn,
				b.buildScalar(
					w.WindowDef.Frame.Bounds.StartBound.OffsetExpr.(tree.TypedExpr),
					inScope,
					nil,
					nil,
					&referencedCols,
				),
			)
		}

		if windowFrames[i].Bounds.EndBound.OffsetExpr != nil {
			fn = b.factory.ConstructWindowToOffset(
				fn,
				b.buildScalar(
					w.WindowDef.Frame.Bounds.EndBound.OffsetExpr.(tree.TypedExpr),
					inScope,
					nil,
					nil,
					&referencedCols,
				),
			)
		}

		if !referencedCols.Empty() {
			panic(
				pgerror.Newf(
					pgcode.InvalidColumnReference,
					"argument of %s must not contain variables",
					tree.WindowModeName(windowFrames[i].Mode),
				),
			)
		}

		if filterCols[i] != 0 {
			fn = b.factory.ConstructAggFilter(
				fn,
				b.factory.ConstructVariable(filterCols[i]),
			)
		}

		frames[frameIdx].Windows = append(frames[frameIdx].Windows,
			b.factory.ConstructWindowsItem(
				fn,
				&memo.WindowsItemPrivate{
					Frame: memo.WindowFrame{
						Mode:           windowFrames[i].Mode,
						StartBoundType: windowFrames[i].Bounds.StartBound.BoundType,
						EndBoundType:   windowFrames[i].Bounds.EndBound.BoundType,
						FrameExclusion: windowFrames[i].Exclusion,
					},
					Col: w.col.id,
				},
			),
		)
	}

	for _, f := range frames {
		outScope.expr = b.factory.ConstructWindow(outScope.expr, f.Windows, &f.WindowPrivate)
	}
}

// buildAggregationAsWindow builds the aggregation operators as window functions.
// Returns the output scope for the aggregation operation.
// Consider the following query that uses an ordered aggregation:
//
// SELECT array_agg(col1 ORDER BY col1) FROM tab
//
// To support this ordering, we build the aggregate as a window function like below:
//
// scalar-group-by
//  ├── columns: array_agg:2(int[])
//  ├── window partition=() ordering=+1
//  │    ├── columns: col1:1(int!null) array_agg:2(int[])
//  │    ├── scan tab
//  │    │    └── columns: col1:1(int!null)
//  │    └── windows
//  │         └── windows-item: range from unbounded to unbounded [type=int[]]
//  │              └── array-agg [type=int[]]
//  │                   └── variable: col1 [type=int]
//  └── aggregations
//       └── const-agg [type=int[]]
//            └── variable: array_agg [type=int[]]
func (b *Builder) buildAggregationAsWindow(
	groupingColSet opt.ColSet, having opt.ScalarExpr, fromScope *scope,
) *scope {
	g := fromScope.groupby

	// Create the window frames based on the orderings and groupings specified.
	argLists := make([][]opt.ScalarExpr, len(g.aggs))
	partitions := make([]opt.ColSet, len(g.aggs))
	orderings := make([]props.OrderingChoice, len(g.aggs))
	filterCols := make([]opt.ColumnID, len(g.aggs))

	// Construct the pre-projection, which renders the grouping columns and the
	// aggregate arguments, as well as any additional order by columns.
	g.aggInScope.appendColumnsFromScope(fromScope)
	b.constructProjectForScope(fromScope, g.aggInScope)

	// Build the arguments, partitions and orderings for each aggregate.
	for i, agg := range g.aggs {
		argExprs := getTypedExprs(agg.Exprs)

		// Build the appropriate arguments.
		argLists[i] = b.buildWindowArgs(argExprs, i, agg.def.Name, fromScope, g.aggInScope)

		// Build appropriate partitions.
		partitions[i] = groupingColSet.Copy()

		// Build appropriate orderings.
		if !agg.isCommutative() {
			ord := b.buildWindowOrdering(agg.OrderBy, i, agg.def.Name, fromScope, g.aggInScope)
			orderings[i].FromOrdering(ord)
		}

		if agg.Filter != nil {
			col := b.buildFilterCol(agg.Filter, i, agg.def.Name, fromScope, g.aggInScope)
			filterCols[i] = col.id
		}
	}

	// Initialize the aggregate expression.
	aggregateExpr := g.aggInScope.expr

	// frames accumulates the set of distinct window frames we're computing over
	// so that we can group functions over the same partition and ordering.
	frames := make([]memo.WindowExpr, 0, len(g.aggs))
	for i, agg := range g.aggs {
		fn := b.constructAggregate(agg.def.Name, argLists[i])
		if filterCols[i] != 0 {
			fn = b.factory.ConstructAggFilter(
				fn,
				b.factory.ConstructVariable(filterCols[i]),
			)
		}

		frameIdx := b.findMatchingFrameIndex(&frames, partitions[i], orderings[i])

		frames[frameIdx].Windows = append(frames[frameIdx].Windows,
			b.factory.ConstructWindowsItem(
				fn,
				&memo.WindowsItemPrivate{
					Frame: windowAggregateFrame(),
					Col:   agg.col.id,
				},
			),
		)
	}

	for _, f := range frames {
		aggregateExpr = b.factory.ConstructWindow(aggregateExpr, f.Windows, &f.WindowPrivate)
	}

	// Construct a grouping so the values per group are squashed down. Each of the
	// aggregations built as window functions emit an aggregated value for each row
	// instead of each group. To rectify this, we must 'squash' the values down by
	// wrapping it with a GroupBy or ScalarGroupBy.
	g.aggOutScope.expr = b.constructWindowGroup(aggregateExpr, groupingColSet, g.aggs, g.aggOutScope)

	// Wrap with having filter if it exists.
	if having != nil {
		input := g.aggOutScope.expr.(memo.RelExpr)
		filters := memo.FiltersExpr{b.factory.ConstructFiltersItem(having)}
		g.aggOutScope.expr = b.factory.ConstructSelect(input, filters)
	}
	return g.aggOutScope
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
	argExprs := getTypedExprs(w.Exprs)

	switch w.def.Name {
	// The second argument of {lead,lag} is 1 by default, and the third argument
	// is NULL by default.
	case "lead", "lag":
		if len(argExprs) < 2 {
			argExprs = append(argExprs, tree.NewDInt(1))
		}
		if len(argExprs) < 3 {
			null := tree.ReType(tree.DNull, argExprs[0].ResolvedType())
			argExprs = append(argExprs, null)
		}
	}

	return argExprs
}

// buildWindowArgs builds the argExprs into a slice of memo.ScalarListExpr.
func (b *Builder) buildWindowArgs(
	argExprs []tree.TypedExpr, windowIndex int, funcName string, inScope, outScope *scope,
) memo.ScalarListExpr {
	argList := make(memo.ScalarListExpr, len(argExprs))
	for j, a := range argExprs {
		col := outScope.findExistingCol(a, false /* allowSideEffects */)
		if col == nil {
			// Use an anonymous name because the column cannot be referenced
			// in other expressions.
			colName := scopeColName("").WithMetadataName(
				fmt.Sprintf("%s_%d_arg%d", funcName, windowIndex+1, j+1),
			)
			col = b.synthesizeColumn(
				outScope,
				colName,
				a.ResolvedType(),
				a,
				b.buildScalar(a, inScope, nil, nil, nil),
			)
		}
		argList[j] = b.factory.ConstructVariable(col.id)
	}
	return argList
}

// buildWindowPartition builds the appropriate partitions for window functions.
func (b *Builder) buildWindowPartition(
	partitions []tree.Expr, windowIndex int, funcName string, inScope, outScope *scope,
) opt.ColSet {
	partition := make([]tree.TypedExpr, len(partitions))
	for i := range partition {
		partition[i] = partitions[i].(tree.TypedExpr)
	}

	// PARTITION BY (a, b) => PARTITION BY a, b
	var windowPartition opt.ColSet
	cols := flattenTuples(partition)
	for j, e := range cols {
		col := outScope.findExistingCol(e, false /* allowSideEffects */)
		if col == nil {
			// Use an anonymous name because the column cannot be referenced
			// in other expressions.
			colName := scopeColName("").WithMetadataName(
				fmt.Sprintf("%s_%d_partition_%d", funcName, windowIndex+1, j+1),
			)
			col = b.synthesizeColumn(
				outScope,
				colName,
				e.ResolvedType(),
				e,
				b.buildScalar(e, inScope, nil, nil, nil),
			)
		}
		windowPartition.Add(col.id)
	}
	return windowPartition
}

// buildWindowOrdering builds the appropriate orderings for window functions.
func (b *Builder) buildWindowOrdering(
	orderBy tree.OrderBy, windowIndex int, funcName string, inScope, outScope *scope,
) opt.Ordering {
	ord := make(opt.Ordering, 0, len(orderBy))
	for j, t := range orderBy {
		// ORDER BY (a, b) => ORDER BY a, b.
		te := inScope.resolveType(t.Expr, types.Any)
		cols := flattenTuples([]tree.TypedExpr{te})

		for _, e := range cols {
			col := outScope.findExistingCol(e, false /* allowSideEffects */)
			if col == nil {
				// Use an anonymous name because the column cannot be referenced
				// in other expressions.
				colName := scopeColName("").WithMetadataName(
					fmt.Sprintf("%s_%d_orderby_%d", funcName, windowIndex+1, j+1),
				)
				col = b.synthesizeColumn(
					outScope,
					colName,
					te.ResolvedType(),
					te,
					b.buildScalar(e, inScope, nil, nil, nil),
				)
			}
			ord = append(ord, opt.MakeOrderingColumn(col.id, t.Direction == tree.Descending))
		}
	}
	return ord
}

// buildFilterCol builds the filter column from the filter Expr.
func (b *Builder) buildFilterCol(
	filter tree.Expr, windowIndex int, funcName string, inScope, outScope *scope,
) *scopeColumn {
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require("FILTER", tree.RejectSpecial)

	te := inScope.resolveAndRequireType(filter, types.Bool)

	col := outScope.findExistingCol(te, false /* allowSideEffects */)
	if col == nil {
		// Use an anonymous name because the column cannot be referenced
		// in other expressions.
		colName := scopeColName("").WithMetadataName(
			fmt.Sprintf("%s_%d_filter", funcName, windowIndex+1),
		)
		col = b.synthesizeColumn(
			outScope,
			colName,
			te.ResolvedType(),
			te,
			b.buildScalar(te, inScope, nil, nil, nil),
		)
	}

	return col
}

// findMatchingFrameIndex finds a frame position to which a window of the
// given partition and ordering can be added to. If no such frame is found, a
// new one is made.
func (b *Builder) findMatchingFrameIndex(
	frames *[]memo.WindowExpr, partition opt.ColSet, ordering props.OrderingChoice,
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

// constructWindowGroup wraps the input window expression with an appropriate
// grouping so the results of each window column are squashed down.
// The expression may be wrapped with a projection so ensure the default NULL
// values of the aggregates are respected when no rows are returned.
func (b *Builder) constructWindowGroup(
	input memo.RelExpr, groupingColSet opt.ColSet, aggInfos []aggregateInfo, outScope *scope,
) memo.RelExpr {
	if groupingColSet.Empty() {
		// Construct a scalar GroupBy wrapped around the appropriate projections.
		return b.constructScalarWindowGroup(input, aggInfos, outScope)
	}

	// Construct a GroupBy using the groupingColSet. Use the ConstAgg aggregate for
	// the window columns.
	private := memo.GroupingPrivate{GroupingCols: groupingColSet}
	private.Ordering.FromOrderingWithOptCols(nil, groupingColSet)
	aggs := make(memo.AggregationsExpr, 0, len(aggInfos))
	for i := range aggInfos {
		aggs = append(aggs, b.factory.ConstructAggregationsItem(
			b.factory.ConstructConstAgg(b.factory.ConstructVariable(aggInfos[i].col.id)),
			aggInfos[i].col.id,
		))
	}
	return b.factory.ConstructGroupBy(input, aggs, &private)
}

// replaceDefaultReturn constructs a case expression to apply as a projection over
// a ScalarGroupBy expression, that replaces the default NULL value from matchVal
// to replaceVal.
func (b *Builder) replaceDefaultReturn(
	varExpr, matchVal, replaceVal opt.ScalarExpr,
) opt.ScalarExpr {
	return b.factory.ConstructCase(
		memo.TrueSingleton,
		memo.ScalarListExpr{
			b.factory.ConstructWhen(
				b.factory.ConstructIs(varExpr, matchVal),
				replaceVal,
			),
		},
		varExpr,
	)
}

// overrideDefaultNullValue checks whether the aggregate has a predefined null
// value for scalar group by when no rows are returned. The default null value
// to be applied is also returned.
func (b *Builder) overrideDefaultNullValue(agg aggregateInfo) (opt.ScalarExpr, bool) {
	switch agg.def.Name {
	case "count", "count_rows":
		return b.factory.ConstructConst(tree.NewDInt(0), types.Int), true
	default:
		return nil, false
	}
}

// constructScalarWindowGroup wraps the input window expression with a scalar
// grouping so the results of each window column are squashed down.
// The expression may be wrapped with a projection so ensure the default NULL
// values of the aggregates are respected when no rows are returned.
func (b *Builder) constructScalarWindowGroup(
	input memo.RelExpr, aggInfos []aggregateInfo, outScope *scope,
) memo.RelExpr {
	aggs := make(memo.AggregationsExpr, 0, len(aggInfos))

	// Create a projection here to replace the NULL values with pre-defined
	// default values of aggregates. The projection should be of the form:
	//
	// CASE true WHEN aggregate_result = NULL THEN default_val ELSE aggregate_result
	//
	// aggregate_result above is the column created by the window function after
	// computing an aggregate. default_val is the default value for the aggregate.
	// Example:
	//
	// CASE true WHEN count = NULL THEN 0 ELSE count

	// Create the projections expression.
	projections := make(memo.ProjectionsExpr, 0, len(aggInfos))

	// Create an appropriate passthrough for the projection.
	var passthrough opt.ColSet
	for i := range aggInfos {
		varExpr := b.factory.ConstructConstAgg(b.factory.ConstructVariable(aggInfos[i].col.id))

		// If the aggregate requires a projection to potentially set a default null value
		// a new column will be needed to be synthesized.
		defaultNullVal, requiresProjection := b.overrideDefaultNullValue(aggInfos[i])
		aggregateCol := aggInfos[i].col
		if requiresProjection {
			aggregateCol = b.synthesizeColumn(outScope, aggregateCol.name, aggregateCol.typ, aggregateCol.expr, varExpr)
		}

		aggs = append(aggs, b.factory.ConstructAggregationsItem(varExpr, aggregateCol.id))

		if requiresProjection {
			// Add projection to replace default NULL value.
			projections = append(projections, b.factory.ConstructProjectionsItem(
				b.replaceDefaultReturn(
					b.factory.ConstructVariable(aggregateCol.id),
					memo.NullSingleton,
					defaultNullVal),
				aggInfos[i].col.id,
			))
		} else {
			// Pass through the aggregate column directly.
			passthrough.Add(aggInfos[i].col.id)
		}
	}

	scalarAggExpr := b.factory.ConstructScalarGroupBy(input, aggs, &memo.GroupingPrivate{})
	if len(projections) != 0 {
		return b.factory.ConstructProject(scalarAggExpr, projections, passthrough)
	}
	return scalarAggExpr
}
