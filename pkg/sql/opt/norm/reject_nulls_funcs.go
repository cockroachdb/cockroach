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

// NullRejectProjections returns a Variable for the first eligible input column
// of the first projection that is referenced by nullRejectCols. A column is
// only null-rejected if:
//
//   1. It is in the RejectNullCols ColSet of the input expression (null
//      rejection has been requested)
//
//   2. A NULL in the column implies that the projection will also be NULL.
//
// NullRejectProjections panics if no such projection is found.
func (c *CustomFuncs) NullRejectProjections(
	projections memo.ProjectionsExpr, nullRejectCols, inputNullRejectCols opt.ColSet,
) opt.ScalarExpr {

	// getFirstEligibleCol recursively traverses the given expression and returns
	// the first column that is eligible for null-rejection. Returns 0 if no such
	// column is found.
	var getFirstEligibleCol func(opt.Expr) opt.ColumnID
	getFirstEligibleCol = func(expr opt.Expr) opt.ColumnID {
		if variable, ok := expr.(*memo.VariableExpr); ok {
			if inputNullRejectCols.Contains(variable.Col) {
				// Null-rejection has been requested for this column, and the projection
				// returns NULL when this column is NULL.
				return variable.Col
			}
			// Null-rejection has not been requested for this column.
			return opt.ColumnID(0)
		}
		if !opt.ScalarOperatorTransmitsNulls(expr.Op()) {
			// This operator does not return NULL when one of its inputs is NULL, so
			// we cannot null-reject through it.
			return opt.ColumnID(0)
		}
		for i, cnt := 0, expr.ChildCount(); i < cnt; i++ {
			if col := getFirstEligibleCol(expr.Child(i)); col != 0 {
				return col
			}
		}
		return opt.ColumnID(0)
	}

	for i := range projections {
		if nullRejectCols.Contains(projections[i].Col) {
			col := getFirstEligibleCol(projections[i].Element)
			if col == 0 {
				// If the ColumnID of the projection was in nullRejectCols, an input
				// column must exist that can be null-rejected.
				panic(errors.AssertionFailedf("expected column not found"))
			}
			return c.f.ConstructVariable(col)
		}
	}
	panic(errors.AssertionFailedf("expected projection not found"))
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
		relProps.Rule.RejectNullCols.UnionWith(deriveProjectRejectNullCols(in))

	case opt.ScanOp:
		relProps.Rule.RejectNullCols.UnionWith(deriveScanRejectNullCols(in))
	}

	if relProps.Rule.RejectNullCols.Intersects(relProps.NotNullCols) {
		panic(errors.AssertionFailedf("null rejection requested on non-null column"))
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

// GetNullRejectedCols returns the set of columns which are null-rejected by the
// given FiltersExpr.
func (c *CustomFuncs) GetNullRejectedCols(filters memo.FiltersExpr) opt.ColSet {
	var nullRejectedCols opt.ColSet
	for i := range filters {
		constraints := filters[i].ScalarProps().Constraints
		if constraints == nil {
			continue
		}

		nullRejectedCols.UnionWith(constraints.ExtractNotNullCols(c.f.evalCtx))
	}
	return nullRejectedCols
}

// MakeNullRejectFilters returns a FiltersExpr with a "col IS NOT NULL" conjunct
// for each column in the given ColSet.
func (c *CustomFuncs) MakeNullRejectFilters(nullRejectCols opt.ColSet) memo.FiltersExpr {
	filters := make(memo.FiltersExpr, 0, nullRejectCols.Len())
	for col, ok := nullRejectCols.Next(0); ok; col, ok = nullRejectCols.Next(col + 1) {
		filters = append(
			filters,
			c.f.ConstructFiltersItem(c.f.ConstructIsNot(c.f.ConstructVariable(col), memo.NullSingleton)),
		)
	}
	return filters
}

// deriveProjectRejectNullCols returns the set of Project output columns which
// are eligible for null rejection. All passthrough columns which are in the
// RejectNullCols set of the input can be null-rejected. In addition, projected
// columns can also be null-rejected when:
//
//   1. The projection "transmits" nulls - it returns NULL when one or more of
//      its inputs is NULL.
//
//   2. One or more of the projection's input columns are in the RejectNullCols
//      ColSet of the input expression. Note that this condition is not strictly
//      necessary in order for a null-rejecting filter to be pushed down, but it
//      ensures that filters are only pushed down when they are requested by a
//      child operator (for example, an outer join that may be simplified). This
//      prevents filters from getting in the way of other rules.
//
func deriveProjectRejectNullCols(in memo.RelExpr) opt.ColSet {
	rejectNullCols := DeriveRejectNullCols(in.Child(0).(memo.RelExpr))
	projections := *in.Child(1).(*memo.ProjectionsExpr)
	var projectionsRejectCols opt.ColSet

	// canRejectNulls recursively traverses the given projection expression and
	// returns true if the projection satisfies the above conditions.
	var canRejectNulls func(opt.Expr) bool
	canRejectNulls = func(expr opt.Expr) bool {
		switch t := expr.(type) {
		case *memo.VariableExpr:
			// Condition #2: if the column contained by this Variable is in the input
			// RejectNullCols set, the projection output column can be null-rejected.
			return rejectNullCols.Contains(t.Col)

		case *memo.ConstExpr:
			// Fall through to the child traversal.

		default:
			if !opt.ScalarOperatorTransmitsNulls(expr.Op()) {
				// In order for condition #1 to be satisfied, we require an unbroken chain
				// of null-transmitting operators from the input null-rejection column to
				// the output of the projection.
				return false
			}
			// Fall through to the child traversal.
		}
		for i, cnt := 0, expr.ChildCount(); i < cnt; i++ {
			if canRejectNulls(expr.Child(i)) {
				return true
			}
		}

		// No child expressions were found that make the projection eligible for
		// null-rejection.
		return false
	}

	// Add any projections which satisfy the conditions.
	for i := range projections {
		if canRejectNulls(projections[i].Element) {
			projectionsRejectCols.Add(projections[i].Col)
		}
	}
	return (rejectNullCols.Union(projectionsRejectCols)).Intersection(in.Relational().OutputCols)
}

// deriveScanRejectNullCols returns the set of Scan columns which are eligible
// for null rejection. Scan columns can be null-rejected only when there are
// partial indexes that have explicit "column IS NOT NULL" expressions. Creating
// null-rejecting filters is useful in this case because the filters may imply a
// partial index predicate expression, allowing a scan over the index.
func deriveScanRejectNullCols(in memo.RelExpr) opt.ColSet {
	md := in.Memo().Metadata()
	scan := in.(*memo.ScanExpr)

	var rejectNullCols opt.ColSet
	for i, n := 0, md.Table(scan.Table).IndexCount(); i < n; i++ {
		if pred, isPartialIndex := md.TableMeta(scan.Table).PartialIndexPredicate(i); isPartialIndex {
			predFilters := *pred.(*memo.FiltersExpr)
			rejectNullCols.UnionWith(isNotNullCols(predFilters))
		}
	}

	// Some of the columns may already be not-null (e.g. if the scan is
	// constrained). Requesting rejection on such columns can lead to infinite
	// rule application (see #64661).
	rejectNullCols.DifferenceWith(in.Relational().NotNullCols)

	return rejectNullCols
}

// isNotNullCols returns the set of columns with explicit, top-level IS NOT NULL
// filter conditions in the given filters. Note that And and Or expressions are
// not traversed.
func isNotNullCols(filters memo.FiltersExpr) opt.ColSet {
	var notNullCols opt.ColSet
	for i := range filters {
		c := filters[i].Condition
		isNot, ok := c.(*memo.IsNotExpr)
		if !ok {
			continue
		}
		col, ok := isNot.Left.(*memo.VariableExpr)
		if !ok {
			continue
		}
		if isNot.Right == memo.NullSingleton {
			notNullCols.Add(col.Col)
		}
	}
	return notNullCols
}
