// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// RejectNullCols returns the set of columns that are candidates for NULL
// rejection filter pushdown. See the Relational.Rule.RejectNullCols comment for
// more details.
func (c *CustomFuncs) RejectNullCols(in memo.RelExpr) opt.ColSet {
	return DeriveRejectNullCols(c.mem, in, c.f.disabledRules)
}

// HasNullRejectingFilter returns true if the filter causes some of the columns
// in nullRejectCols to be non-null. For example, if nullRejectCols = (x, z),
// filters such as x < 5, x = y, and z IS NOT NULL all satisfy this property.
func (c *CustomFuncs) HasNullRejectingFilter(
	filters memo.FiltersExpr, nullRejectCols opt.ColSet,
) bool {
	if nullRejectCols.Empty() {
		return false
	}
	for i := range filters {
		constraints := filters[i].ScalarProps().Constraints
		if constraints == nil {
			continue
		}

		notNullFilterCols := constraints.ExtractNotNullCols(c.f.ctx, c.f.evalCtx)
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
//  1. It is in the RejectNullCols ColSet of the input expression (null
//     rejection has been requested)
//
//  2. A NULL in the column implies that the projection will also be NULL.
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
//
// disabledRules is the set of rules currently disabled, only used when rules
// are randomly disabled for testing. It is used to prevent propagating the
// RejectNullCols property when the corresponding column-pruning normalization
// rule is disabled. This prevents rule cycles during testing.
func DeriveRejectNullCols(mem *memo.Memo, in memo.RelExpr, disabledRules intsets.Fast) opt.ColSet {
	// Lazily calculate and store the RejectNullCols value.
	relProps := in.Relational()
	if relProps.IsAvailable(props.RejectNullCols) {
		return relProps.Rule.RejectNullCols
	}
	relProps.SetAvailable(props.RejectNullCols)

	// TODO(andyk): Add other operators to make null rejection more comprehensive.
	switch in.Op() {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		if disabledRules.Contains(int(opt.MergeSelectInnerJoin)) ||
			disabledRules.Contains(int(opt.PushFilterIntoJoinLeft)) ||
			disabledRules.Contains(int(opt.PushFilterIntoJoinRight)) ||
			disabledRules.Contains(int(opt.MapFilterIntoJoinLeft)) ||
			disabledRules.Contains(int(opt.MapFilterIntoJoinRight)) {
			// Avoid rule cycles.
			break
		}
		// Pass through null-rejecting columns from both inputs.
		if in.Child(0).(memo.RelExpr).Relational().OuterCols.Empty() {
			relProps.Rule.RejectNullCols.UnionWith(
				DeriveRejectNullCols(mem, in.Child(0).(memo.RelExpr), disabledRules),
			)
		}
		if in.Child(1).(memo.RelExpr).Relational().OuterCols.Empty() {
			relProps.Rule.RejectNullCols.UnionWith(
				DeriveRejectNullCols(mem, in.Child(1).(memo.RelExpr), disabledRules),
			)
		}

	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		if disabledRules.Contains(int(opt.RejectNullsLeftJoin)) ||
			disabledRules.Contains(int(opt.PushSelectCondLeftIntoJoinLeftAndRight)) {
			// Avoid rule cycles.
			break
		}
		// Pass through null-rejection columns from left input, and request
		// null-rejection on right columns.
		if in.Child(0).(memo.RelExpr).Relational().OuterCols.Empty() {
			relProps.Rule.RejectNullCols.UnionWith(
				DeriveRejectNullCols(mem, in.Child(0).(memo.RelExpr), disabledRules),
			)
		}
		relProps.Rule.RejectNullCols.UnionWith(in.Child(1).(memo.RelExpr).Relational().OutputCols)

	case opt.RightJoinOp:
		if disabledRules.Contains(int(opt.RejectNullsRightJoin)) ||
			disabledRules.Contains(int(opt.CommuteRightJoin)) {
			// Avoid rule cycles.
			break
		}
		// Pass through null-rejection columns from right input, and request
		// null-rejection on left columns.
		relProps.Rule.RejectNullCols.UnionWith(in.Child(0).(memo.RelExpr).Relational().OutputCols)
		if in.Child(1).(memo.RelExpr).Relational().OuterCols.Empty() {
			relProps.Rule.RejectNullCols.UnionWith(
				DeriveRejectNullCols(mem, in.Child(1).(memo.RelExpr), disabledRules),
			)
		}

	case opt.FullJoinOp:
		if disabledRules.Contains(int(opt.RejectNullsLeftJoin)) ||
			disabledRules.Contains(int(opt.RejectNullsRightJoin)) {
			// Avoid rule cycles.
			break
		}
		// Request null-rejection on all output columns.
		relProps.Rule.RejectNullCols.UnionWith(relProps.OutputCols)

	case opt.GroupByOp, opt.ScalarGroupByOp:
		if disabledRules.Contains(int(opt.RejectNullsGroupBy)) ||
			disabledRules.Contains(int(opt.PushSelectIntoGroupBy)) {
			// Avoid rule cycles.
			break
		}
		relProps.Rule.RejectNullCols.UnionWith(deriveGroupByRejectNullCols(mem, in, disabledRules))

	case opt.ProjectOp:
		if disabledRules.Contains(int(opt.RejectNullsProject)) ||
			disabledRules.Contains(int(opt.PushSelectIntoProject)) {
			// Avoid rule cycles.
			break
		}
		relProps.Rule.RejectNullCols.UnionWith(deriveProjectRejectNullCols(mem, in, disabledRules))

	case opt.ScanOp:
		relProps.Rule.RejectNullCols.UnionWith(deriveScanRejectNullCols(mem, in))
	}

	// Don't attempt to request null-rejection for non-null cols. This can happen
	// if normalization failed to null-reject, and then exploration "uncovered"
	// the possibility for null-rejection of a column.
	relProps.Rule.RejectNullCols.DifferenceWith(relProps.NotNullCols)

	return relProps.Rule.RejectNullCols
}

// deriveGroupByRejectNullCols returns the set of GroupBy columns that are
// eligible for null rejection. If an aggregate input column has requested null
// rejection, then pass along its request if the following criteria are met:
//
//  1. The aggregate function ignores null values, meaning that its value
//     would not change if input null values are filtered.
//
//  2. The aggregate function returns null if its input is empty. And since
//     by #1, the presence of nulls does not alter the result, the aggregate
//     function would return null if its input contains only null values.
func deriveGroupByRejectNullCols(
	mem *memo.Memo, in memo.RelExpr, disabledRules intsets.Fast,
) opt.ColSet {
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

		if !DeriveRejectNullCols(mem, input, disabledRules).Contains(inColID) {
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

		nullRejectedCols.UnionWith(constraints.ExtractNotNullCols(c.f.ctx, c.f.evalCtx))
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
//  1. The projection "transmits" nulls - it returns NULL when one or more of
//     its inputs is NULL.
func deriveProjectRejectNullCols(
	mem *memo.Memo, in memo.RelExpr, disabledRules intsets.Fast,
) opt.ColSet {
	rejectNullCols := DeriveRejectNullCols(mem, in.Child(0).(memo.RelExpr), disabledRules)
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
func deriveScanRejectNullCols(mem *memo.Memo, in memo.RelExpr) opt.ColSet {
	md := mem.Metadata()
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
