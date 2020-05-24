// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// CanMapOnSetOp determines whether the filter can be mapped to either
// side of a set operator.
func (c *CustomFuncs) CanMapOnSetOp(src *memo.FiltersItem) bool {
	filterProps := src.ScalarProps()
	for i, ok := filterProps.OuterCols.Next(0); ok; i, ok = filterProps.OuterCols.Next(i + 1) {
		colType := c.f.Metadata().ColumnMeta(i).Type
		if sqlbase.HasCompositeKeyEncoding(colType) {
			return false
		}
	}
	return !filterProps.HasCorrelatedSubquery
}

// MapSetOpFilterLeft maps the filter onto the left expression by replacing
// the out columns of the filter with the appropriate corresponding columns in
// the left side of the operator.
// Useful for pushing filters to relations the set operation is composed of.
func (c *CustomFuncs) MapSetOpFilterLeft(
	filter *memo.FiltersItem, set *memo.SetPrivate,
) opt.ScalarExpr {
	colMap := makeMapFromColLists(set.OutCols, set.LeftCols)
	return c.MapFiltersItemCols(filter, colMap)
}

// MapSetOpFilterRight maps the filter onto the right expression by replacing
// the out columns of the filter with the appropriate corresponding columns in
// the right side of the operator.
// Useful for pushing filters to relations the set operation is composed of.
func (c *CustomFuncs) MapSetOpFilterRight(
	filter *memo.FiltersItem, set *memo.SetPrivate,
) opt.ScalarExpr {
	colMap := makeMapFromColLists(set.OutCols, set.RightCols)
	return c.MapFiltersItemCols(filter, colMap)
}

// makeMapFromColLists maps each column ID in src to a column ID in dst. The
// columns IDs are mapped based on their relative positions in the column lists,
// e.g. the third item in src maps to the third item in dst. The lists must be
// of equal length.
func makeMapFromColLists(src opt.ColList, dst opt.ColList) util.FastIntMap {
	if len(src) != len(dst) {
		panic(errors.AssertionFailedf("src and dst must have the same length, src: %v, dst: %v", src, dst))
	}

	var colMap util.FastIntMap
	for colIndex, outColID := range src {
		colMap.Set(int(outColID), int(dst[colIndex]))
	}
	return colMap
}

// MapFiltersItemCols maps filter expressions by replacing occurrences of
// the keys of colMap with the corresponding values. Outer columns are not
// replaced.
func (c *CustomFuncs) MapFiltersItemCols(
	filter *memo.FiltersItem, colMap util.FastIntMap,
) opt.ScalarExpr {
	// Recursively walk the scalar sub-tree looking for references to columns
	// that need to be replaced and then replace them appropriately.
	var replace ReplaceFunc
	replace = func(nd opt.Expr) opt.Expr {
		switch t := nd.(type) {
		case *memo.VariableExpr:
			dstCol, ok := colMap.Get(int(t.Col))
			if !ok {
				// It is not part of the output cols so no replacement required.
				return nd
			}
			return c.f.ConstructVariable(opt.ColumnID(dstCol))
		}
		return c.f.Replace(nd, replace)
	}

	return replace(filter.Condition).(opt.ScalarExpr)
}

// GroupingAndConstCols returns the grouping columns and ConstAgg columns (for
// which the input and output column IDs match). A filter on these columns can
// be pushed through a GroupBy.
func (c *CustomFuncs) GroupingAndConstCols(
	grouping *memo.GroupingPrivate, aggs memo.AggregationsExpr,
) opt.ColSet {
	result := grouping.GroupingCols.Copy()

	// Add any ConstAgg columns.
	for i := range aggs {
		item := &aggs[i]
		if constAgg, ok := item.Agg.(*memo.ConstAggExpr); ok {
			// Verify that the input and output column IDs match.
			if item.Col == constAgg.Input.(*memo.VariableExpr).Col {
				result.Add(item.Col)
			}
		}
	}
	return result
}

// CanConsolidateFilters returns true if there are at least two different
// filter conditions that contain the same variable, where the conditions
// have tight constraints and contain a single variable. For example,
// CanConsolidateFilters returns true with filters {x > 5, x < 10}, but false
// with {x > 5, y < 10} and {x > 5, x = y}.
func (c *CustomFuncs) CanConsolidateFilters(filters memo.FiltersExpr) bool {
	var seen opt.ColSet
	for i := range filters {
		if col, ok := c.canConsolidateFilter(&filters[i]); ok {
			if seen.Contains(col) {
				return true
			}
			seen.Add(col)
		}
	}
	return false
}

// canConsolidateFilter determines whether a filter condition can be
// consolidated. Filters can be consolidated if they have tight constraints
// and contain a single variable. Examples of such filters include x < 5 and
// x IS NULL. If the filter can be consolidated, canConsolidateFilter returns
// the column ID of the variable and ok=true. Otherwise, canConsolidateFilter
// returns ok=false.
func (c *CustomFuncs) canConsolidateFilter(filter *memo.FiltersItem) (col opt.ColumnID, ok bool) {
	if !filter.ScalarProps().TightConstraints {
		return 0, false
	}

	outerCols := c.OuterCols(filter)
	if outerCols.Len() != 1 {
		return 0, false
	}

	col, _ = outerCols.Next(0)
	return col, true
}

// ConsolidateFilters consolidates filter conditions that contain the same
// variable, where the conditions have tight constraints and contain a single
// variable. The consolidated filters are combined with a tree of nested
// And operations, and wrapped with a Range expression.
//
// See the ConsolidateSelectFilters rule for more details about why this is
// necessary.
func (c *CustomFuncs) ConsolidateFilters(filters memo.FiltersExpr) memo.FiltersExpr {
	// First find the columns that have filter conditions that can be
	// consolidated.
	var seen, seenTwice opt.ColSet
	for i := range filters {
		if col, ok := c.canConsolidateFilter(&filters[i]); ok {
			if seen.Contains(col) {
				seenTwice.Add(col)
			} else {
				seen.Add(col)
			}
		}
	}

	newFilters := make(memo.FiltersExpr, seenTwice.Len(), len(filters)-seenTwice.Len())

	// newFilters contains an empty item for each of the new Range expressions
	// that will be created below. Fill in rangeMap to track which column
	// corresponds to each item.
	var rangeMap util.FastIntMap
	i := 0
	for col, ok := seenTwice.Next(0); ok; col, ok = seenTwice.Next(col + 1) {
		rangeMap.Set(int(col), i)
		i++
	}

	// Iterate through each existing filter condition, and either consolidate it
	// into one of the new Range expressions or add it unchanged to the new
	// filters.
	for i := range filters {
		if col, ok := c.canConsolidateFilter(&filters[i]); ok && seenTwice.Contains(col) {
			// This is one of the filter conditions that can be consolidated into a
			// Range.
			cond := filters[i].Condition
			switch t := cond.(type) {
			case *memo.RangeExpr:
				// If it is already a range expression, unwrap it.
				cond = t.And
			}
			rangeIdx, _ := rangeMap.Get(int(col))
			rangeItem := &newFilters[rangeIdx]
			if rangeItem.Condition == nil {
				// This is the first condition.
				rangeItem.Condition = cond
			} else {
				// Build a left-deep tree of ANDs sorted by ID.
				rangeItem.Condition = c.mergeSortedAnds(rangeItem.Condition, cond)
			}
		} else {
			newFilters = append(newFilters, filters[i])
		}
	}

	// Construct each of the new Range operators now that we have built the
	// conjunctions.
	for i, n := 0, seenTwice.Len(); i < n; i++ {
		newFilters[i] = c.f.ConstructFiltersItem(c.f.ConstructRange(newFilters[i].Condition))
	}

	return newFilters
}

// mergeSortedAnds merges two left-deep trees of nested AndExprs sorted by ID.
// Returns a single sorted, left-deep tree of nested AndExprs, with any
// duplicate expressions eliminated.
func (c *CustomFuncs) mergeSortedAnds(left, right opt.ScalarExpr) opt.ScalarExpr {
	if right == nil {
		return left
	}
	if left == nil {
		return right
	}

	// Since both trees are left-deep, perform a merge-sort from right to left.
	nextLeft := left
	nextRight := right
	var remainingLeft, remainingRight opt.ScalarExpr
	if and, ok := left.(*memo.AndExpr); ok {
		remainingLeft = and.Left
		nextLeft = and.Right
	}
	if and, ok := right.(*memo.AndExpr); ok {
		remainingRight = and.Left
		nextRight = and.Right
	}

	if nextLeft.ID() == nextRight.ID() {
		// Eliminate duplicates.
		return c.mergeSortedAnds(left, remainingRight)
	}
	if nextLeft.ID() < nextRight.ID() {
		return c.f.ConstructAnd(c.mergeSortedAnds(left, remainingRight), nextRight)
	}
	return c.f.ConstructAnd(c.mergeSortedAnds(remainingLeft, right), nextLeft)
}

// AreFiltersSorted determines whether the expressions in a FiltersExpr are
// ordered by their expression IDs.
func (c *CustomFuncs) AreFiltersSorted(f memo.FiltersExpr) bool {
	for i, n := 0, f.ChildCount(); i < n-1; i++ {
		if f.Child(i).Child(0).(opt.ScalarExpr).ID() > f.Child(i+1).Child(0).(opt.ScalarExpr).ID() {
			return false
		}
	}
	return true
}

// SortFilters sorts a filter list by the IDs of the expressions. This has the
// effect of canonicalizing FiltersExprs which may have the same filters, but
// in a different order.
func (c *CustomFuncs) SortFilters(f memo.FiltersExpr) memo.FiltersExpr {
	result := make(memo.FiltersExpr, len(f))
	for i, n := 0, f.ChildCount(); i < n; i++ {
		fi := f.Child(i).(*memo.FiltersItem)
		result[i] = *fi
	}
	result.Sort()
	return result
}

// SimplifyFilters removes True operands from a FiltersExpr, and normalizes any
// False or Null condition to a single False condition. Null values map to False
// because FiltersExpr are only used by Select and Join, both of which treat a
// Null filter conjunct exactly as if it were false.
//
// SimplifyFilters also "flattens" any And operator child by merging its
// conditions into a new FiltersExpr list. If, after simplification, no operands
// remain, then SimplifyFilters returns an empty FiltersExpr.
//
// This method assumes that the NormalizeNestedAnds rule has already run and
// ensured a left deep And tree. If not (maybe because it's a testing scenario),
// then this rule may rematch, but it should still make forward progress).
func (c *CustomFuncs) SimplifyFilters(filters memo.FiltersExpr) memo.FiltersExpr {
	// Start by counting the number of conjuncts that will be flattened so that
	// the capacity of the FiltersExpr list can be determined.
	cnt := 0
	for _, item := range filters {
		cnt++
		condition := item.Condition
		for condition.Op() == opt.AndOp {
			cnt++
			condition = condition.(*memo.AndExpr).Left
		}
	}

	// Construct new filter list.
	newFilters := make(memo.FiltersExpr, 0, cnt)
	for _, item := range filters {
		var ok bool
		if newFilters, ok = c.addConjuncts(item.Condition, newFilters); !ok {
			return memo.FiltersExpr{c.f.ConstructFiltersItem(memo.FalseSingleton)}
		}
	}

	return newFilters
}

// IsUnsimplifiableOr returns true if this is an OR where neither side is
// NULL. SimplifyFilters simplifies ORs with a NULL on one side to its other
// side. However other ORs don't simplify. This function is used to prevent
// infinite recursion during ConstructFilterItem in SimplifyFilters. This
// function must be kept in sync with SimplifyFilters.
func (c *CustomFuncs) IsUnsimplifiableOr(item *memo.FiltersItem) bool {
	or, ok := item.Condition.(*memo.OrExpr)
	if !ok {
		return false
	}
	return or.Left.Op() != opt.NullOp && or.Right.Op() != opt.NullOp
}

// addConjuncts recursively walks a scalar expression as long as it continues to
// find nested And operators. It adds any conjuncts (ignoring True operators) to
// the given FiltersExpr and returns true. If it finds a False or Null operator,
// it propagates a false return value all the up the call stack, and
// SimplifyFilters maps that to a FiltersExpr that is always false.
func (c *CustomFuncs) addConjuncts(
	scalar opt.ScalarExpr, filters memo.FiltersExpr,
) (_ memo.FiltersExpr, ok bool) {
	switch t := scalar.(type) {
	case *memo.AndExpr:
		var ok bool
		if filters, ok = c.addConjuncts(t.Left, filters); !ok {
			return nil, false
		}
		return c.addConjuncts(t.Right, filters)

	case *memo.FalseExpr, *memo.NullExpr:
		// Filters expression evaluates to False if any operand is False or Null.
		return nil, false

	case *memo.TrueExpr:
		// Filters operator skips True operands.

	case *memo.OrExpr:
		// If NULL is on either side, take the other side.
		if t.Left.Op() == opt.NullOp {
			filters = append(filters, c.f.ConstructFiltersItem(t.Right))
		} else if t.Right.Op() == opt.NullOp {
			filters = append(filters, c.f.ConstructFiltersItem(t.Left))
		} else {
			filters = append(filters, c.f.ConstructFiltersItem(t))
		}

	default:
		filters = append(filters, c.f.ConstructFiltersItem(t))
	}
	return filters, true
}
