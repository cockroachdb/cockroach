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
