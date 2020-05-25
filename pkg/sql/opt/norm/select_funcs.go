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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util"
)

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
				result.Add(int(item.Col))
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
func (c *CustomFuncs) canConsolidateFilter(filter *memo.FiltersItem) (col int, ok bool) {
	if !filter.ScalarProps(c.mem).TightConstraints {
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
		rangeMap.Set(col, i)
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
			rangeIdx, _ := rangeMap.Get(col)
			rangeItem := &newFilters[rangeIdx]
			if rangeItem.Condition == nil {
				// This is the first condition.
				rangeItem.Condition = cond
			} else {
				// Build a left-deep tree of ANDs.
				rangeItem.Condition = c.f.ConstructAnd(rangeItem.Condition, cond)
			}
		} else {
			newFilters = append(newFilters, filters[i])
		}
	}

	// Construct each of the new Range operators now that we have built the
	// conjunctions.
	for i, n := 0, seenTwice.Len(); i < n; i++ {
		newFilters[i].Condition = c.f.ConstructRange(newFilters[i].Condition)
	}

	return newFilters
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
	sort.Slice(result, func(i, j int) bool {
		return result[i].Child(0).(opt.ScalarExpr).ID() < result[j].Child(0).(opt.ScalarExpr).ID()
	})
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
			return memo.FalseFilter
		}
	}

	return newFilters
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

	default:
		filters = append(filters, memo.FiltersItem{Condition: t})
	}
	return filters, true
}
