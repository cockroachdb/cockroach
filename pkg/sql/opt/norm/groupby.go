// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// CanReduceGroupingCols is true if the given GroupBy operator has one or more
// redundant grouping columns. A grouping column is redundant if it is
// functionally determined by the other grouping columns.
func (c *CustomFuncs) CanReduceGroupingCols(
	input memo.RelExpr, private *memo.GroupingPrivate,
) bool {
	fdset := input.Relational().FuncDeps
	return !fdset.ReduceCols(private.GroupingCols).Equals(private.GroupingCols)
}

// ReduceGroupingCols constructs a new GroupByDef private, based on an existing
// definition. The new GroupByDef will not retain any grouping column that is
// functionally determined by other grouping columns. CanReduceGroupingCols
// should be called before calling this method, to ensure it has work to do.
func (c *CustomFuncs) ReduceGroupingCols(
	input memo.RelExpr, private *memo.GroupingPrivate,
) *memo.GroupingPrivate {
	fdset := input.Relational().FuncDeps
	return &memo.GroupingPrivate{
		GroupingCols: fdset.ReduceCols(private.GroupingCols),
		Ordering:     private.Ordering,
	}
}

// AppendReducedGroupingCols will take columns discarded by ReduceGroupingCols
// and append them to the end of the given aggregate function list, wrapped in
// ConstAgg aggregate functions. AppendReducedGroupingCols returns a new
// Aggregations operator containing the combined set of existing aggregate
// functions and the new ConstAgg aggregate functions.
func (c *CustomFuncs) AppendReducedGroupingCols(
	input memo.RelExpr, aggs memo.AggregationsExpr, private *memo.GroupingPrivate,
) memo.AggregationsExpr {
	fdset := input.Relational().FuncDeps
	appendCols := private.GroupingCols.Difference(fdset.ReduceCols(private.GroupingCols))
	return c.AppendAggCols(aggs, opt.ConstAggOp, appendCols)
}

// AppendAggCols constructs a new Aggregations operator containing the aggregate
// functions from an existing Aggregations operator plus an additional set of
// aggregate functions, one for each column in the given set. The new functions
// are of the given aggregate operator type.
func (c *CustomFuncs) AppendAggCols(
	aggs memo.AggregationsExpr, aggOp opt.Operator, cols opt.ColSet,
) memo.AggregationsExpr {
	outAggs := make(memo.AggregationsExpr, len(aggs)+cols.Len())
	copy(outAggs, aggs)
	c.makeAggCols(aggOp, cols, outAggs[len(aggs):])
	return outAggs
}

// AppendAggCols2 constructs a new Aggregations operator containing the
// aggregate functions from an existing Aggregations operator plus an
// additional set of aggregate functions, one for each column in the given set.
// The new functions are of the given aggregate operator type.
func (c *CustomFuncs) AppendAggCols2(
	aggs memo.AggregationsExpr,
	aggOp opt.Operator,
	cols opt.ColSet,
	aggOp2 opt.Operator,
	cols2 opt.ColSet,
) memo.AggregationsExpr {
	colsLen := cols.Len()
	outAggs := make(memo.AggregationsExpr, len(aggs)+colsLen+cols2.Len())
	copy(outAggs, aggs)

	offset := len(aggs)
	c.makeAggCols(aggOp, cols, outAggs[offset:])
	offset += colsLen
	c.makeAggCols(aggOp2, cols2, outAggs[offset:])

	return outAggs
}

// makeAggCols is a helper method that constructs a new aggregate function of
// the given operator type for each column in the given set. The resulting
// aggregates are written into outElems and outColList. As an example, for
// columns (1,2) and operator ConstAggOp, makeAggCols will set the following:
//
//   outElems[0] = (ConstAggOp (Variable 1))
//   outElems[1] = (ConstAggOp (Variable 2))
//
//   outColList[0] = 1
//   outColList[1] = 2
//
func (c *CustomFuncs) makeAggCols(
	aggOp opt.Operator, cols opt.ColSet, outAggs memo.AggregationsExpr,
) {
	// Append aggregate functions wrapping a Variable reference to each column.
	i := 0
	for id, ok := cols.Next(0); ok; id, ok = cols.Next(id + 1) {
		varExpr := c.f.ConstructVariable(id)

		var outAgg opt.ScalarExpr
		switch aggOp {
		case opt.ConstAggOp:
			outAgg = c.f.ConstructConstAgg(varExpr)

		case opt.AnyNotNullAggOp:
			outAgg = c.f.ConstructAnyNotNullAgg(varExpr)

		case opt.FirstAggOp:
			outAgg = c.f.ConstructFirstAgg(varExpr)

		default:
			panic(errors.AssertionFailedf("unrecognized aggregate operator type: %v", log.Safe(aggOp)))
		}

		outAggs[i].Agg = outAgg
		outAggs[i].Col = id
		i++
	}
}

// CanRemoveAggDistinctForKeys returns true if the given aggregations contain an
// aggregation with AggDistinct where the input column together with the
// grouping columns form a key. In this case, the respective AggDistinct can be
// removed.
func (c *CustomFuncs) CanRemoveAggDistinctForKeys(
	aggs memo.AggregationsExpr, private *memo.GroupingPrivate, input memo.RelExpr,
) bool {
	inputFDs := &input.Relational().FuncDeps
	if _, hasKey := inputFDs.StrictKey(); !hasKey {
		// Fast-path for the case when the input has no keys.
		return false
	}

	for i := range aggs {
		if ok, _ := c.hasRemovableAggDistinct(aggs[i].Agg, private.GroupingCols, inputFDs); ok {
			return true
		}
	}
	return false
}

// RemoveAggDistinctForKeys rewrites aggregations to remove AggDistinct when
// the input column together with the grouping columns form a key. Returns the
// new Aggregation expression.
func (c *CustomFuncs) RemoveAggDistinctForKeys(
	aggs memo.AggregationsExpr, private *memo.GroupingPrivate, input memo.RelExpr,
) memo.AggregationsExpr {
	inputFDs := &input.Relational().FuncDeps

	newAggs := make(memo.AggregationsExpr, len(aggs))
	for i := range aggs {
		item := &aggs[i]
		if ok, v := c.hasRemovableAggDistinct(item.Agg, private.GroupingCols, inputFDs); ok {
			// Remove AggDistinct. We rely on the fact that AggDistinct must be
			// directly "under" the Aggregate.
			// TODO(radu): this will need to be revisited when we add more modifiers.
			newAggs[i].Agg = c.replaceAggInputVar(item.Agg, v)
			newAggs[i].Col = aggs[i].Col
		} else {
			newAggs[i] = *item
		}
	}

	return newAggs
}

// replaceAggInputVar swaps out the aggregated variable in an aggregate with v. In
// the case of aggregates with multiple arguments (like string_agg) the other arguments
// are kept the same.
func (c *CustomFuncs) replaceAggInputVar(agg opt.ScalarExpr, v opt.ScalarExpr) opt.ScalarExpr {
	switch agg.ChildCount() {
	case 1:
		return c.f.DynamicConstruct(agg.Op(), v).(opt.ScalarExpr)
	case 2:
		return c.f.DynamicConstruct(agg.Op(), v, agg.Child(1)).(opt.ScalarExpr)
	default:
		panic(errors.AssertionFailedf("unhandled number of aggregate children"))
	}
}

// hasRemovableAggDistinct is called with an aggregation element and returns
// true if the aggregation has AggDistinct and the grouping columns along with
// the aggregation input column form a key in the input (in which case
// AggDistinct can be elided).
// On success, the input expression to AggDistinct is also returned.
func (c *CustomFuncs) hasRemovableAggDistinct(
	agg opt.ScalarExpr, groupingCols opt.ColSet, inputFDs *props.FuncDepSet,
) (ok bool, aggDistinctVar *memo.VariableExpr) {
	if agg.ChildCount() == 0 {
		return false, nil
	}

	distinct, ok := agg.Child(0).(*memo.AggDistinctExpr)
	if !ok {
		return false, nil
	}

	v, ok := distinct.Input.(*memo.VariableExpr)
	if !ok {
		return false, nil
	}

	cols := groupingCols.Copy()
	cols.Add(v.Col)
	if !inputFDs.ColsAreStrictKey(cols) {
		return false, nil
	}

	return true, v
}

// HasNoGroupingCols returns true if the GroupingCols in the private are empty.
func (c *CustomFuncs) HasNoGroupingCols(private *memo.GroupingPrivate) bool {
	return private.GroupingCols.Empty()
}

// GroupingInputOrdering returns the Ordering in the private.
func (c *CustomFuncs) GroupingInputOrdering(private *memo.GroupingPrivate) physical.OrderingChoice {
	return private.Ordering
}

// ConstructProjectionFromDistinctOn converts a DistinctOn to a projection; this
// is correct when the input has at most one row. Note that DistinctOn can only
// have aggregations of type FirstAgg or ConstAgg.
func (c *CustomFuncs) ConstructProjectionFromDistinctOn(
	input memo.RelExpr, aggs memo.AggregationsExpr,
) memo.RelExpr {
	var passthrough opt.ColSet
	var projections memo.ProjectionsExpr
	for i := range aggs {
		varExpr := memo.ExtractVarFromAggInput(aggs[i].Agg.Child(0).(opt.ScalarExpr))
		inputCol := varExpr.Col
		outputCol := aggs[i].Col
		if inputCol == outputCol {
			passthrough.Add(inputCol)
		} else {
			projections = append(projections, memo.ProjectionsItem{
				Element:    varExpr,
				ColPrivate: aggs[i].ColPrivate,
				Typ:        aggs[i].Typ,
			})
		}
	}
	return c.f.ConstructProject(input, projections, passthrough)
}
