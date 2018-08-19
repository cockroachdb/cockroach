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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// RejectNullCols returns the set of columns that are candidates for NULL
// rejection filter pushdown. See the Relational.Rule.RejectNullCols comment for
// more details.
func (c *CustomFuncs) RejectNullCols(group memo.GroupID) opt.ColSet {
	return DeriveRejectNullCols(memo.MakeNormExprView(c.f.mem, group))
}

// HasNullRejectingFilter returns true if the filter causes some of the columns
// in nullRejectCols to be non-null. For example, if nullRejectCols = (x, z),
// filters such as x < 5, x = y, and z IS NOT NULL all satisfy this property.
func (c *CustomFuncs) HasNullRejectingFilter(filter memo.GroupID, nullRejectCols opt.ColSet) bool {
	filterConstraints := c.LookupLogical(filter).Scalar.Constraints
	if filterConstraints == nil {
		return false
	}

	notNullFilterCols := filterConstraints.ExtractNotNullCols(c.f.evalCtx)
	return notNullFilterCols.Intersects(nullRejectCols)
}

// NullRejectAggVar scans through the list of aggregate functions and returns
// the Variable input of the first aggregate that is not ConstAgg. Such an
// aggregate must exist, since this is only called if at least one eligible
// null-rejection column was identified by the deriveGroupByRejectNullCols
// method (see its comment for more details).
func (c *CustomFuncs) NullRejectAggVar(aggs memo.GroupID) memo.GroupID {
	aggsExpr := c.f.mem.NormExpr(aggs).AsAggregations()
	aggsElems := c.f.mem.LookupList(aggsExpr.Aggs())

	for i := len(aggsElems) - 1; i >= 0; i-- {
		agg := c.f.mem.NormExpr(aggsElems[i])
		if agg.Operator() != opt.ConstAggOp {
			// Return the input Variable operator.
			return agg.ChildGroup(c.f.mem, 0)
		}
	}
	panic("couldn't find an aggregate that is not ConstAgg")
}

// DeriveRejectNullCols returns the set of columns that are candidates for NULL
// rejection filter pushdown. See the Relational.Rule.RejectNullCols comment for
// more details.
func DeriveRejectNullCols(ev memo.ExprView) opt.ColSet {
	// Lazily calculate and store the RejectNullCols value.
	relational := ev.Logical().Relational
	if relational.IsAvailable(props.RejectNullCols) {
		return relational.Rule.RejectNullCols
	}
	relational.SetAvailable(props.RejectNullCols)

	// TODO(andyk): Add other operators to make null rejection more comprehensive.
	switch ev.Operator() {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		// Pass through null-rejecting columns from both inputs.
		relational.Rule.RejectNullCols = DeriveRejectNullCols(ev.Child(0))
		relational.Rule.RejectNullCols.UnionWith(DeriveRejectNullCols(ev.Child(1)))

	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		// Pass through null-rejection columns from left input, and request null-
		// rejection on right columns.
		relational.Rule.RejectNullCols = DeriveRejectNullCols(ev.Child(0))
		relational.Rule.RejectNullCols.UnionWith(ev.Child(1).Logical().Relational.OutputCols)

	case opt.RightJoinOp, opt.RightJoinApplyOp:
		// Pass through null-rejection columns from right input, and request null-
		// rejection on left columns.
		relational.Rule.RejectNullCols = ev.Child(0).Logical().Relational.OutputCols
		relational.Rule.RejectNullCols.UnionWith(DeriveRejectNullCols(ev.Child(1)))

	case opt.FullJoinOp, opt.FullJoinApplyOp:
		// Request null-rejection on all output columns.
		relational.Rule.RejectNullCols = relational.OutputCols

	case opt.GroupByOp, opt.ScalarGroupByOp:
		relational.Rule.RejectNullCols = deriveGroupByRejectNullCols(ev)
	}

	return relational.Rule.RejectNullCols
}

// deriveGroupByRejectNullCols returns the set of GroupBy columns that are
// eligible for null rejection. If an aggregate input column has requested null
// rejection, then pass along its request if the following criteria are met:
//
//   1. The aggregate function ignores null values, meaning that its value
//      would not change if input null values are filtered.
//
//   2. The aggregate function returns null if its input is empty. And since
//      by #1, the presence of nulls does not alter the result, the aggregate
//      function would return null if its input contains only null values.
//
//   3. No other input columns are referenced by other aggregate functions in
//      the GroupBy (all functions must refer to the same column), with the
//      possible exception of ConstAgg. A ConstAgg aggregate can be safely
//      ignored because all rows in each group must have the same value for this
//      column, so it doesn't matter which rows are filtered.
//
func deriveGroupByRejectNullCols(ev memo.ExprView) opt.ColSet {
	input := ev.Child(0)
	aggs := ev.Child(1)
	aggColList := aggs.Private().(opt.ColList)

	var rejectNullCols opt.ColSet
	var savedInColID opt.ColumnID
	for i, end := 0, aggs.ChildCount(); i < end; i++ {
		agg := aggs.Child(i)
		aggOp := agg.Operator()

		if aggOp == opt.ConstAggOp {
			continue
		}

		// Criteria #1 and #2.
		if !opt.AggregateIgnoresNulls(aggOp) || !opt.AggregateIsNullOnEmpty(aggOp) {
			// Can't reject nulls for the aggregate.
			return opt.ColSet{}
		}

		// Get column ID of aggregate's Variable operator input.
		inColID := extractAggInputColumn(agg)

		// Criteria #3.
		if savedInColID != 0 && savedInColID != inColID {
			// Multiple columns used by aggregate functions, so can't reject nulls
			// for any of them.
			return opt.ColSet{}
		}
		savedInColID = inColID

		if !DeriveRejectNullCols(input).Contains(int(inColID)) {
			// Input has not requested null rejection on the input column.
			return opt.ColSet{}
		}

		// Can possibly reject column, but keep searching, since if
		// multiple columns are used by aggregate functions, then nulls
		// can't be rejected on any column.
		rejectNullCols.Add(int(aggColList[i]))
	}
	return rejectNullCols
}
