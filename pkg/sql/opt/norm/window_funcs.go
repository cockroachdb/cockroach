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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// MakeSegmentedOrdering returns an ordering choice which satisfies both
// limitOrdering and the ordering required by a window function. Returns
// ok=false if no such ordering exists. See OrderingChoice.PrefixIntersection
// for more details.
func (c *CustomFuncs) MakeSegmentedOrdering(
	input memo.RelExpr,
	prefix opt.ColSet,
	ordering props.OrderingChoice,
	limitOrdering props.OrderingChoice,
) (_ *props.OrderingChoice, ok bool) {

	// The columns in the closure of the prefix may be included in it. It's
	// beneficial to do so for a given column iff that column appears in the
	// limit's ordering.
	cl := input.Relational().FuncDeps.ComputeClosure(prefix)
	cl.IntersectionWith(limitOrdering.ColSet())
	cl.UnionWith(prefix)
	prefix = cl

	oc, ok := limitOrdering.PrefixIntersection(prefix, ordering.Columns)
	if !ok {
		return nil, false
	}
	return &oc, true
}

// AllArePrefixSafe returns whether every window function in the list satisfies
// the "prefix-safe" property.
//
// Being prefix-safe means that the computation of a window function on a given
// row does not depend on any of the rows that come after it. It's also
// precisely the property that lets us push limit operators below window
// functions:
//
//		(Limit (Window $input) n) = (Window (Limit $input n))
//
// Note that the frame affects whether a given window function is prefix-safe or not.
// rank() is prefix-safe under any frame, but avg():
//  * is not prefix-safe under RANGE BETWEEN UNBOUNDED PRECEDING TO CURRENT ROW
//    (the default), because we might cut off mid-peer group. If we can
//    guarantee that the ordering is over a key, then this becomes safe.
//  * is not prefix-safe under ROWS BETWEEN UNBOUNDED PRECEDING TO UNBOUNDED
//    FOLLOWING, because it needs to look at the entire partition.
//  * is prefix-safe under ROWS BETWEEN UNBOUNDED PRECEDING TO CURRENT ROW,
//    because it only needs to look at the rows up to any given row.
// (We don't currently handle this case).
//
// This function is best-effort. It's OK to report a function not as
// prefix-safe, even if it is.
func (c *CustomFuncs) AllArePrefixSafe(fns memo.WindowsExpr) bool {
	for i := range fns {
		if !c.isPrefixSafe(&fns[i]) {
			return false
		}
	}
	return true
}

// isPrefixSafe returns whether or not the given window function satisfies the
// "prefix-safe" property. See the comment above AllArePrefixSafe for more
// details.
func (c *CustomFuncs) isPrefixSafe(fn *memo.WindowsItem) bool {
	switch fn.Function.Op() {
	case opt.RankOp, opt.RowNumberOp, opt.DenseRankOp:
		return true
	}
	// TODO(justin): Add other cases. I think aggregates are valid here if the
	// upper bound is CURRENT ROW, and either:
	// * the mode is ROWS, or
	// * the mode is RANGE and the ordering is over a key.
	return false
}

// RemoveWindowPartitionCols returns a new window private struct with the given
// columns removed from the window partition column set.
func (c *CustomFuncs) RemoveWindowPartitionCols(
	private *memo.WindowPrivate, cols opt.ColSet,
) *memo.WindowPrivate {
	p := *private
	p.Partition = p.Partition.Difference(cols)
	return &p
}

// WindowPartition returns the set of columns that the window function uses to
// partition.
func (c *CustomFuncs) WindowPartition(priv *memo.WindowPrivate) opt.ColSet {
	return priv.Partition
}

// WindowOrdering returns the ordering used by the window function.
func (c *CustomFuncs) WindowOrdering(private *memo.WindowPrivate) props.OrderingChoice {
	return private.Ordering
}

// ExtractDeterminedConditions returns a new list of filters containing only
// those expressions from the given list which are bound by columns which
// are functionally determined by the given columns.
func (c *CustomFuncs) ExtractDeterminedConditions(
	filters memo.FiltersExpr, cols opt.ColSet, input memo.RelExpr,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, 0, len(filters))
	for i := range filters {
		if c.ColsAreDeterminedBy(filters[i].ScalarProps().OuterCols, cols, input) {
			newFilters = append(newFilters, filters[i])
		}
	}
	return newFilters
}

// ExtractUndeterminedConditions is the opposite of
// ExtractDeterminedConditions.
func (c *CustomFuncs) ExtractUndeterminedConditions(
	filters memo.FiltersExpr, cols opt.ColSet, input memo.RelExpr,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, 0, len(filters))
	for i := range filters {
		if !c.ColsAreDeterminedBy(filters[i].ScalarProps().OuterCols, cols, input) {
			newFilters = append(newFilters, filters[i])
		}
	}
	return newFilters
}

// DerefOrderingChoice returns an OrderingChoice from a pointer.
func (c *CustomFuncs) DerefOrderingChoice(result *props.OrderingChoice) props.OrderingChoice {
	return *result
}

// HasRangeFrameWithOffset returns true if w contains a WindowsItem Frame that
// has a mode of RANGE and has a specific offset, such as OffsetPreceding or
// OffsetFollowing.
func (c *CustomFuncs) HasRangeFrameWithOffset(w memo.WindowsExpr) bool {
	for i := range w {
		if w[i].Frame.Mode == tree.RANGE && w[i].Frame.HasOffset() {
			return true
		}
	}
	return false
}
