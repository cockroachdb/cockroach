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
	"github.com/cockroachdb/errors"
)

// RejectNullCols returns the set of columns that are candidates for NULL
// rejection filter pushdown. See the Relational.Rule.RejectNullCols comment for
// more details.
func (c *CustomFuncs) RejectNullCols(in memo.RelExpr) opt.ColSet {
	return DeriveRejectNullCols(in)
}

// HasNullRejectingFilter returns true if the filter causes some of the columns
// in nullRejectCols to be non-null. For example, if nullRejectCols = (x, z),
// filters such as x < 5, x = y, and z IS NOT NULL all satisfy this property.
func (c *CustomFuncs) HasNullRejectingFilter(
	filters memo.FiltersExpr, nullRejectCols opt.ColSet,
) bool {
	for i := range filters {
		constraints := filters[i].ScalarProps().Constraints
		if constraints == nil {
			continue
		}

		notNullFilterCols := constraints.ExtractNotNullCols(c.f.evalCtx)
		if notNullFilterCols.Intersects(nullRejectCols) {
			return true
		}
	}
	return false
}

// NullRejectAggVar scans through the list of aggregate functions and returns
// the Variable input of the first aggregate that is not ConstAgg. Such an
// aggregate must exist, since this is only called if at least one eligible
// null-rejection column was identified by the deriveGroupByRejectNullCols
// method (see its comment for more details).
func (c *CustomFuncs) NullRejectAggVar(
	aggs memo.AggregationsExpr, nullRejectCols opt.ColSet,
) *memo.VariableExpr {
	for i := range aggs {
		if nullRejectCols.Contains(aggs[i].Col) {
			return memo.ExtractAggFirstVar(aggs[i].Agg)
		}
	}
	panic(errors.AssertionFailedf("expected aggregation not found"))
}

// DeriveRejectNullCols returns the set of columns that are candidates for NULL
// rejection filter pushdown. See the Relational.Rule.RejectNullCols comment for
// more details.
func DeriveRejectNullCols(in memo.RelExpr) opt.ColSet {
	// Lazily calculate and store the RejectNullCols value.
	relProps := in.Relational()
	if relProps.IsAvailable(props.RejectNullCols) {
		return relProps.Rule.RejectNullCols
	}
	relProps.SetAvailable(props.RejectNullCols)

	// TODO(andyk): Add other operators to make null rejection more comprehensive.
	switch in.Op() {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		// Pass through null-rejecting columns from both inputs.
		if in.Child(0).(memo.RelExpr).Relational().OuterCols.Empty() {
			relProps.Rule.RejectNullCols.UnionWith(DeriveRejectNullCols(in.Child(0).(memo.RelExpr)))
		}
		if in.Child(1).(memo.RelExpr).Relational().OuterCols.Empty() {
			relProps.Rule.RejectNullCols.UnionWith(DeriveRejectNullCols(in.Child(1).(memo.RelExpr)))
		}

	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		// Pass through null-rejection columns from left input, and request null-
		// rejection on right columns.
		if in.Child(0).(memo.RelExpr).Relational().OuterCols.Empty() {
			relProps.Rule.RejectNullCols.UnionWith(DeriveRejectNullCols(in.Child(0).(memo.RelExpr)))
		}
		relProps.Rule.RejectNullCols.UnionWith(in.Child(1).(memo.RelExpr).Relational().OutputCols)

	case opt.RightJoinOp:
		// Pass through null-rejection columns from right input, and request null-
		// rejection on left columns.
		relProps.Rule.RejectNullCols.UnionWith(in.Child(0).(memo.RelExpr).Relational().OutputCols)
		if in.Child(1).(memo.RelExpr).Relational().OuterCols.Empty() {
			relProps.Rule.RejectNullCols.UnionWith(DeriveRejectNullCols(in.Child(1).(memo.RelExpr)))
		}

	case opt.FullJoinOp:
		// Request null-rejection on all output columns.
		relProps.Rule.RejectNullCols.UnionWith(relProps.OutputCols)

	case opt.GroupByOp, opt.ScalarGroupByOp:
		relProps.Rule.RejectNullCols.UnionWith(deriveGroupByRejectNullCols(in))

	case opt.ProjectOp:
		// Pass through all null-rejection columns that the Project passes through.
		// The PushSelectIntoProject rule is able to push the IS NOT NULL filter
		// below the Project for these columns.
		rejectNullCols := DeriveRejectNullCols(in.Child(0).(memo.RelExpr))
		relProps.Rule.RejectNullCols = relProps.OutputCols.Intersection(rejectNullCols)
	}

	return relProps.Rule.RejectNullCols
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
func deriveGroupByRejectNullCols(in memo.RelExpr) opt.ColSet {
	input := in.Child(0).(memo.RelExpr)
	aggs := *in.Child(1).(*memo.AggregationsExpr)

	var rejectNullCols opt.ColSet
	var savedInColID opt.ColumnID
	for i := range aggs {
		agg := memo.ExtractAggFunc(aggs[i].Agg)
		aggOp := agg.Op()

		if aggOp == opt.ConstAggOp {
			continue
		}

		// Criteria #1 and #2.
		if !opt.AggregateIgnoresNulls(aggOp) || !opt.AggregateIsNullOnEmpty(aggOp) {
			// Can't reject nulls for the aggregate.
			return opt.ColSet{}
		}

		// Get column ID of aggregate's Variable operator input.
		inColID := agg.Child(0).(*memo.VariableExpr).Col

		// Criteria #3.
		if savedInColID != 0 && savedInColID != inColID {
			// Multiple columns used by aggregate functions, so can't reject nulls
			// for any of them.
			return opt.ColSet{}
		}
		savedInColID = inColID

		if !DeriveRejectNullCols(input).Contains(inColID) {
			// Input has not requested null rejection on the input column.
			return opt.ColSet{}
		}

		// Can possibly reject column, but keep searching, since if
		// multiple columns are used by aggregate functions, then nulls
		// can't be rejected on any column.
		rejectNullCols.Add(aggs[i].Col)
	}
	return rejectNullCols
}
