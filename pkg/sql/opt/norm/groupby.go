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

package norm

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// CanReduceGroupingCols is true if the given GroupBy operator has one or more
// redundant grouping columns. A grouping column is redundant if it is
// functionally determined by the other grouping columns.
func (c *CustomFuncs) CanReduceGroupingCols(input memo.GroupID, def memo.PrivateID) bool {
	groupingCols := c.f.mem.LookupPrivate(def).(*memo.GroupByDef).GroupingCols
	fdset := c.LookupLogical(input).Relational.FuncDeps
	return !fdset.ReduceCols(groupingCols).Equals(groupingCols)
}

// ReduceGroupingCols constructs a new GroupByDef private, based on an existing
// definition. The new GroupByDef will not retain any grouping column that is
// functionally determined by other grouping columns. CanReduceGroupingCols
// should be called before calling this method, to ensure it has work to do.
func (c *CustomFuncs) ReduceGroupingCols(input memo.GroupID, def memo.PrivateID) memo.PrivateID {
	groupByDef := c.f.mem.LookupPrivate(def).(*memo.GroupByDef)
	fdset := c.LookupLogical(input).Relational.FuncDeps
	return c.f.mem.InternGroupByDef(&memo.GroupByDef{
		GroupingCols: fdset.ReduceCols(groupByDef.GroupingCols),
		Ordering:     groupByDef.Ordering,
	})
}

// AppendReducedGroupingCols will take columns discarded by ReduceGroupingCols
// and append them to the end of the given aggregate function list, wrapped in
// ConstAgg aggregate functions. AppendReducedGroupingCols returns a new
// Aggregations operator containing the combined set of existing aggregate
// functions and the new ConstAgg aggregate functions.
func (c *CustomFuncs) AppendReducedGroupingCols(
	input, aggs memo.GroupID, def memo.PrivateID,
) memo.GroupID {
	groupingCols := c.f.mem.LookupPrivate(def).(*memo.GroupByDef).GroupingCols
	fdset := c.LookupLogical(input).Relational.FuncDeps
	appendCols := groupingCols.Difference(fdset.ReduceCols(groupingCols))
	return c.AppendAggCols(aggs, opt.ConstAggOp, appendCols)
}

// AppendAggCols constructs a new Aggregations operator containing the aggregate
// functions from an existing Aggregations operator plus an additional set of
// aggregate functions, one for each column in the given set. The new functions
// are of the given aggregate operator type.
func (c *CustomFuncs) AppendAggCols(
	aggs memo.GroupID, aggOp opt.Operator, cols opt.ColSet,
) memo.GroupID {
	aggsExpr := c.f.mem.NormExpr(aggs).AsAggregations()
	aggsElems := c.f.mem.LookupList(aggsExpr.Aggs())
	aggsColList := c.ExtractColList(aggsExpr.Cols())

	outElems := make([]memo.GroupID, len(aggsElems)+cols.Len())
	copy(outElems, aggsElems)
	outColList := make(opt.ColList, len(outElems))
	copy(outColList, aggsColList)

	c.makeAggCols(aggOp, cols, outElems[len(aggsElems):], outColList[len(aggsElems):])

	return c.f.ConstructAggregations(c.f.InternList(outElems), c.f.InternColList(outColList))
}

// AppendAggCols2 constructs a new Aggregations operator containing the
// aggregate functions from an existing Aggregations operator plus an
// additional set of aggregate functions, one for each column in the given set.
// The new functions are of the given aggregate operator type.
func (c *CustomFuncs) AppendAggCols2(
	aggs memo.GroupID, aggOp opt.Operator, cols opt.ColSet, aggOp2 opt.Operator, cols2 opt.ColSet,
) memo.GroupID {
	aggsExpr := c.f.mem.NormExpr(aggs).AsAggregations()
	aggsElems := c.f.mem.LookupList(aggsExpr.Aggs())
	aggsColList := c.ExtractColList(aggsExpr.Cols())

	colsLen := cols.Len()
	outElems := make([]memo.GroupID, len(aggsElems)+colsLen+cols2.Len())
	copy(outElems, aggsElems)
	outColList := make(opt.ColList, len(outElems))
	copy(outColList, aggsColList)

	offset := len(aggsElems)
	c.makeAggCols(aggOp, cols, outElems[offset:], outColList[offset:])
	offset += colsLen
	c.makeAggCols(aggOp2, cols2, outElems[offset:], outColList[offset:])

	return c.f.ConstructAggregations(c.f.InternList(outElems), c.f.InternColList(outColList))
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
	aggOp opt.Operator, cols opt.ColSet, outElems []memo.GroupID, outColList opt.ColList,
) {
	// Append aggregate functions wrapping a Variable reference to each column.
	i := 0
	for id, ok := cols.Next(0); ok; id, ok = cols.Next(id + 1) {
		varExpr := c.f.ConstructVariable(c.f.mem.InternColumnID(opt.ColumnID(id)))

		var outAgg memo.GroupID
		switch aggOp {
		case opt.ConstAggOp:
			outAgg = c.f.ConstructConstAgg(varExpr)

		case opt.AnyNotNullAggOp:
			outAgg = c.f.ConstructAnyNotNullAgg(varExpr)

		case opt.FirstAggOp:
			outAgg = c.f.ConstructFirstAgg(varExpr)

		default:
			panic(fmt.Sprintf("unrecognized aggregate operator type: %v", aggOp))
		}

		outElems[i] = outAgg
		outColList[i] = opt.ColumnID(id)
		i++
	}
}

// CanRemoveAggDistinctForKeys returns true if the given aggregations contain an
// aggregation with AggDistinct where the input column together with the
// grouping columns form a key. In this case, the respective AggDistinct can be
// removed.
func (c *CustomFuncs) CanRemoveAggDistinctForKeys(
	aggs memo.GroupID, def memo.PrivateID, input memo.GroupID,
) bool {
	inputFDs := &c.LookupLogical(input).Relational.FuncDeps
	if _, hasKey := inputFDs.Key(); !hasKey {
		// Fast-path for the case when the input has no keys.
		return false
	}

	groupingCols := c.f.mem.LookupPrivate(def).(*memo.GroupByDef).GroupingCols
	aggsExpr := c.f.mem.NormExpr(aggs).AsAggregations()
	aggsElems := c.f.mem.LookupList(aggsExpr.Aggs())
	for _, agg := range aggsElems {
		if ok, _ := c.hasRemovableAggDistinct(agg, groupingCols, inputFDs); ok {
			return true
		}
	}
	return false
}

// RemoveAggDistinctForKeys rewrites aggregations to remove AggDistinct when
// the input column together with the grouping columns form a key. Returns the
// new Aggregation expression.
func (c *CustomFuncs) RemoveAggDistinctForKeys(
	aggs memo.GroupID, def memo.PrivateID, input memo.GroupID,
) memo.GroupID {
	inputFDs := &c.LookupLogical(input).Relational.FuncDeps
	groupingCols := c.f.mem.LookupPrivate(def).(*memo.GroupByDef).GroupingCols
	aggsExpr := c.f.mem.NormExpr(aggs).AsAggregations()
	aggsElems := c.f.mem.LookupList(aggsExpr.Aggs())

	newAggElems := make([]memo.GroupID, len(aggsElems))
	for i, agg := range aggsElems {
		if ok, aggDistinctInput := c.hasRemovableAggDistinct(agg, groupingCols, inputFDs); ok {
			// Remove AggDistinct. We rely on the fact that AggDistinct must be
			// directly "under" the Aggregate.
			// TODO(radu): this will need to be revisited when we add more modifiers.
			aggExpr := c.f.mem.NormExpr(agg)
			newAggElems[i] = c.f.DynamicConstruct(
				aggExpr.Operator(),
				memo.DynamicOperands{memo.DynamicID(aggDistinctInput)},
			)
		} else {
			newAggElems[i] = agg
		}
	}

	return c.f.ConstructAggregations(c.f.InternList(newAggElems), aggsExpr.Cols())
}

// hasRemovableAggDistinct is called with an aggregation element and returns
// true if the aggregation has AggDistinct and the grouping columns along with
// the aggregation input column form a key in the input (in which case
// AggDistinct can be elided).
// On success, the input group to AggDistinct is also returned.
func (c *CustomFuncs) hasRemovableAggDistinct(
	aggregation memo.GroupID, groupingCols opt.ColSet, inputFDs *props.FuncDepSet,
) (ok bool, aggDistinctInput memo.GroupID) {
	aggExpr := c.f.mem.NormExpr(aggregation)
	if aggExpr.ChildCount() == 1 {
		argExpr := c.f.mem.NormExpr(aggExpr.ChildGroup(c.mem, 0))
		if argExpr.Operator() == opt.AggDistinctOp {
			aggDistinctInput := argExpr.ChildGroup(c.mem, 0)
			v := c.f.mem.NormExpr(aggDistinctInput)
			if v.Operator() == opt.VariableOp {
				cols := groupingCols.Copy()
				cols.Add(int(v.Private(c.mem).(opt.ColumnID)))
				if inputFDs.ColsAreStrictKey(cols) {
					return true, aggDistinctInput
				}
			}
		}
	}
	return false, 0
}

// extractAggInputColumn returns the input ColumnID of an aggregate operator.
func extractAggInputColumn(ev memo.ExprView) opt.ColumnID {
	if !ev.IsAggregate() {
		panic("not an Aggregate")
	}
	arg := ev.Child(0)
	if arg.Operator() == opt.AggDistinctOp {
		arg = arg.Child(0)
	}
	return arg.Private().(opt.ColumnID)
}
