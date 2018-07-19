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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
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
// AnyNotNull aggregate functions. AppendReducedGroupingCols returns a new
// Aggregations operator containing the combined set of existing aggregate
// functions and the new AnyNotNull aggregate functions.
func (c *CustomFuncs) AppendReducedGroupingCols(
	input, aggs memo.GroupID, def memo.PrivateID,
) memo.GroupID {
	groupingCols := c.f.mem.LookupPrivate(def).(*memo.GroupByDef).GroupingCols
	fdset := c.LookupLogical(input).Relational.FuncDeps
	appendCols := groupingCols.Difference(fdset.ReduceCols(groupingCols))
	return c.AppendAnyNotNullCols(aggs, appendCols)
}

// AppendAnyNotNullCols constructs a new Aggregations operator containing the
// given aggregate functions. Appended to those are new AnyNotNull aggregates
// that wrap the given set of appendCols. This method is useful when the
// appendCols are known to have constant values within any group; therefore, a
// value from any of the rows in the group can be used.
func (c *CustomFuncs) AppendAnyNotNullCols(aggs memo.GroupID, appendCols opt.ColSet) memo.GroupID {
	aggsExpr := c.f.mem.NormExpr(aggs).AsAggregations()
	aggsElems := c.f.mem.LookupList(aggsExpr.Aggs())
	aggsColList := c.ExtractColList(aggsExpr.Cols())

	outElems := make([]memo.GroupID, len(aggsElems), len(aggsElems)+appendCols.Len())
	copy(outElems, aggsElems)
	outColList := make(opt.ColList, len(outElems), cap(outElems))
	copy(outColList, aggsColList)

	// Append AnyNotNull aggregate functions wrapping each appendCol.
	for i, ok := appendCols.Next(0); ok; i, ok = appendCols.Next(i + 1) {
		outAgg := c.f.ConstructAnyNotNull(
			c.f.ConstructVariable(
				c.f.mem.InternColumnID(opt.ColumnID(i)),
			),
		)
		outElems = append(outElems, outAgg)
		outColList = append(outColList, opt.ColumnID(i))
	}

	return c.f.ConstructAggregations(c.f.InternList(outElems), c.f.InternColList(outColList))
}
