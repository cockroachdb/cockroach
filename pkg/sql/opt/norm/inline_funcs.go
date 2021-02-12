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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// FindInlinableConstants returns the set of input columns that are synthesized
// constant value expressions: ConstOp, TrueOp, FalseOp, or NullOp. Constant
// value expressions can often be inlined into referencing expressions. Only
// Project and Values operators synthesize constant value expressions.
func (c *CustomFuncs) FindInlinableConstants(input memo.RelExpr) opt.ColSet {
	var cols opt.ColSet
	if project, ok := input.(*memo.ProjectExpr); ok {
		for i := range project.Projections {
			item := &project.Projections[i]
			if opt.IsConstValueOp(item.Element) {
				cols.Add(item.Col)
			}
		}
	} else if values, ok := input.(*memo.ValuesExpr); ok && len(values.Rows) == 1 {
		tup := values.Rows[0].(*memo.TupleExpr)
		for i, scalar := range tup.Elems {
			if opt.IsConstValueOp(scalar) {
				cols.Add(values.Cols[i])
			}
		}
	}
	return cols
}

// InlineProjectionConstants recursively searches each projection expression and
// replaces any references to input columns that are constant. It returns a new
// Projections list containing the replaced expressions.
func (c *CustomFuncs) InlineProjectionConstants(
	projections memo.ProjectionsExpr, input memo.RelExpr, constCols opt.ColSet,
) memo.ProjectionsExpr {
	newProjections := make(memo.ProjectionsExpr, len(projections))
	for i := range projections {
		item := &projections[i]
		newProjections[i] = c.f.ConstructProjectionsItem(
			c.inlineConstants(item.Element, input, constCols).(opt.ScalarExpr),
			item.Col,
		)
	}
	return newProjections
}

// InlineFilterConstants recursively searches each filter expression and
// replaces any references to input columns that are constant. It returns a new
// Filters list containing the replaced expressions.
func (c *CustomFuncs) InlineFilterConstants(
	filters memo.FiltersExpr, input memo.RelExpr, constCols opt.ColSet,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, len(filters))
	for i := range filters {
		item := &filters[i]
		newFilters[i] = c.f.ConstructFiltersItem(
			c.inlineConstants(item.Condition, input, constCols).(opt.ScalarExpr),
		)
	}
	return newFilters
}

// inlineConstants recursively searches the given expression and replaces any
// references to input columns that are constant. It returns the replaced
// expression.
func (c *CustomFuncs) inlineConstants(
	e opt.Expr, input memo.RelExpr, constCols opt.ColSet,
) opt.Expr {
	var replace ReplaceFunc
	replace = func(e opt.Expr) opt.Expr {
		switch t := e.(type) {
		case *memo.VariableExpr:
			if constCols.Contains(t.Col) {
				return c.extractColumn(input, t.Col)
			}
			return t
		}
		return c.f.Replace(e, replace)
	}
	return replace(e)
}

// extractColumn searches a Project or Values input expression for the column
// having the given id. It returns the expression for that column.
func (c *CustomFuncs) extractColumn(input memo.RelExpr, col opt.ColumnID) opt.ScalarExpr {
	if project, ok := input.(*memo.ProjectExpr); ok {
		for i := range project.Projections {
			item := &project.Projections[i]
			if item.Col == col {
				return item.Element
			}
		}
	} else if values, ok := input.(*memo.ValuesExpr); ok && len(values.Rows) == 1 {
		tup := values.Rows[0].(*memo.TupleExpr)
		for i, scalar := range tup.Elems {
			if values.Cols[i] == col {
				return scalar
			}
		}
	}
	panic(errors.AssertionFailedf("could not find column to extract"))
}

// HasDuplicateRefs returns true if the target projection expressions or
// passthrough columns reference any column in the given target set more than
// one time, or if the projection expressions contain a correlated subquery.
// For example:
//
//   SELECT x+1, x+2, y FROM a
//
// HasDuplicateRefs would be true, since the x column is referenced twice.
//
// Correlated subqueries are disallowed since it introduces additional
// complexity for a case that's not too important for inlining. Also, skipping
// correlated subqueries minimizes expensive searching in deep trees.
func (c *CustomFuncs) HasDuplicateRefs(
	projections memo.ProjectionsExpr, passthrough opt.ColSet, targetCols opt.ColSet,
) bool {
	// Passthrough columns that reference a target column count as refs.
	refs := passthrough.Intersection(targetCols)
	for i := range projections {
		item := &projections[i]
		if item.ScalarProps().HasCorrelatedSubquery {
			// Don't traverse the expression tree if there is a correlated subquery.
			return true
		}

		// When a target column reference is found, add it to the refs set. If
		// the set already contains a reference to that column, then there is a
		// duplicate. findDupRefs returns true if the subtree contains at least
		// one duplicate.
		var findDupRefs func(e opt.Expr) bool
		findDupRefs = func(e opt.Expr) bool {
			switch t := e.(type) {
			case *memo.VariableExpr:
				// Ignore references to non-target columns.
				if !targetCols.Contains(t.Col) {
					return false
				}

				// Count Variable references.
				if refs.Contains(t.Col) {
					return true
				}
				refs.Add(t.Col)
				return false

			case memo.RelExpr:
				// We know that this is not a correlated subquery since
				// HasCorrelatedSubquery was already checked above. Uncorrelated
				// subqueries never have references.
				return false
			}

			for i, n := 0, e.ChildCount(); i < n; i++ {
				if findDupRefs(e.Child(i)) {
					return true
				}
			}
			return false
		}

		if findDupRefs(item.Element) {
			return true
		}
	}
	return false
}

// CanInlineProjections returns true if all projection expressions can be
// inlined. See CanInline for details.
func (c *CustomFuncs) CanInlineProjections(projections memo.ProjectionsExpr) bool {
	for i := range projections {
		if !c.CanInline(projections[i].Element) {
			return false
		}
	}
	return true
}

// CanInline returns true if the given expression consists only of "simple"
// operators like Variable, Const, Eq, and Plus. These operators are assumed to
// be relatively inexpensive to evaluate, and therefore potentially evaluating
// them multiple times is not a big concern.
func (c *CustomFuncs) CanInline(scalar opt.ScalarExpr) bool {
	switch scalar.Op() {
	case opt.AndOp, opt.OrOp, opt.NotOp, opt.TrueOp, opt.FalseOp,
		opt.EqOp, opt.NeOp, opt.LeOp, opt.LtOp, opt.GeOp, opt.GtOp,
		opt.IsOp, opt.IsNotOp, opt.InOp, opt.NotInOp,
		opt.VariableOp, opt.ConstOp, opt.NullOp,
		opt.PlusOp, opt.MinusOp, opt.MultOp:

		// Recursively verify that children are also inlinable.
		for i, n := 0, scalar.ChildCount(); i < n; i++ {
			if !c.CanInline(scalar.Child(i).(opt.ScalarExpr)) {
				return false
			}
		}
		return true
	}
	return false
}

// IndexedVirtualColumns returns the set of column IDs in the scanPrivate's
// table that are both virtual computed columns and key columns of a secondary
// index.
//
// TODO(mgartner): Include virtual computed columns that are referenced in
// partial index predicates.
func (c *CustomFuncs) IndexedVirtualColumns(scanPrivate *memo.ScanPrivate) opt.ColSet {
	md := c.mem.Metadata()
	tab := md.Table(scanPrivate.Table)
	tabMeta := md.TableMeta(scanPrivate.Table)

	virtualCols := tabMeta.VirtualComputedColumns()
	if virtualCols.Empty() {
		// Return early if there are no virtual columns to avoid unnecessarily
		// calculating the index key columns.
		return opt.ColSet{}
	}

	var indexCols opt.ColSet
	for i, n := 0, tab.IndexCount(); i < n; i++ {
		indexCols.UnionWith(tabMeta.IndexKeyColumnsMapVirtual(i))
	}

	return virtualCols.Intersection(indexCols)
}

// TryInlineSelectVirtualColumns is used by the InlineSelectVirtualColumns rule
// to find filters on virtual computed columns that can be pushed below the
// Project that produces the virtual column. The filter is pushed-down by
// inlining the virtual column expression. Only the columns in
// indexedVirtualColumns are eligible for inlining.
//
// If successful, a pair of filters is returned; the first containing filters in
// which virtual columns have been inlined, and the second containing filters
// that were unaffected by inlining. If unsuccessful, an empty
// InlinedVirtualColumnFiltersPair is returned, indicating that there is nothing
// to inline.
//
// Two separate filters are returned as a pair so that the
// InlineSelectVirtualColumns rule can push down eligible filters, but not the
// rest.
//
// For example:
//
//   CREATE TABLE t (
//     a INT,
//     b INT,
//     v INT AS (abs(a)),
//     w INT AS (abs(b)),
//     INDEX (v)
//   )
//
//   SELECT * FROM t WHERE v = 5 AND w = 10
//
// The (v = 5) and (w = 10) filters are returned in separate filters. In the
// first, v is inlined because it is indexed, so the filter is transformed to
// (abs(a) = 5). This filter is no longer dependent on the projection of v, so
// it can be pushed below the Project. The w variable is not indexed, so the
// (w = 10) filter is unchanged and remains above the Project.
//
// See the InlineSelectVirtualColumns rule for more details.
func (c *CustomFuncs) TryInlineSelectVirtualColumns(
	filters memo.FiltersExpr, projections memo.ProjectionsExpr, indexedVirtualColumns opt.ColSet,
) InlinedVirtualColumnFiltersPair {
	// Collect the projections for the indexed virtual columns.
	var inlinableCols opt.ColSet
	var inlinableProjections memo.ProjectionsExpr
	for i := range projections {
		col := projections[i].Col

		// If the projected expression is volatile, it cannot be inlined. This
		// should be impossible (computed column expressions cannot be
		// volatile), but adds additional safety in case this function is
		// expanded to inline non-virtual columns in the future.
		if projections[i].ScalarProps().VolatilitySet.HasVolatile() {
			continue
		}

		if indexedVirtualColumns.Contains(col) {
			// Initialize inlinableProjections lazily.
			if inlinableProjections == nil {
				inlinableProjections = make(memo.ProjectionsExpr, 0, len(projections)-i)
			}
			inlinableCols.Add(col)
			inlinableProjections = append(inlinableProjections, projections[i])
		}
	}

	if len(inlinableProjections) == 0 {
		// None of the projections are eligible for inlining in filters.
		return InlinedVirtualColumnFiltersPair{}
	}

	// For each filter, determine if there are any virtual columns to inline. If
	// so, inline the virtual column expressions in the filter. We keep track of
	// which filters have inlined projections in inlinedFilterIdxs so that we
	// can build a list of non-inlined filters below.
	var inlined memo.FiltersExpr
	var inlinedFilterIdxs util.FastIntSet
	for i := range filters {
		item := &filters[i]

		// Do not inline a filter if it has a correlated subquery or it does not
		// reference an inlinable virtual column.
		if item.ScalarProps().HasCorrelatedSubquery || !item.ScalarProps().OuterCols.Intersects(inlinableCols) {
			continue
		}

		// Initialize inlined lazily.
		if inlined == nil {
			inlined = make(memo.FiltersExpr, 0, len(filters)-i)
		}

		// Inline the virtual column expressions in the filter.
		filter := c.f.ConstructFiltersItem(
			c.inlineProjections(item.Condition, inlinableProjections).(opt.ScalarExpr),
		)
		inlined = append(inlined, filter)
		inlinedFilterIdxs.Add(i)
	}

	if len(inlined) == 0 {
		// No projections were inlined in filters.
		return InlinedVirtualColumnFiltersPair{}
	}

	// Collect the filters which did not have projections inlined.
	notInlined := make(memo.FiltersExpr, 0, len(filters)-inlinedFilterIdxs.Len())
	for i := range filters {
		if !inlinedFilterIdxs.Contains(i) {
			notInlined = append(notInlined, filters[i])
		}
	}

	return InlinedVirtualColumnFiltersPair{
		inlined:    inlined,
		notInlined: notInlined,
	}
}

// InlinedVirtualColumnFiltersPair is the result of
// TryInlineSelectVirtualColumns. See that function for more details.
type InlinedVirtualColumnFiltersPair struct {
	inlined    memo.FiltersExpr
	notInlined memo.FiltersExpr
}

// InlinedFilters returns the filters in the pair where virtual columns were
// inlined.
func (c *CustomFuncs) InlinedFilters(pair InlinedVirtualColumnFiltersPair) memo.FiltersExpr {
	return pair.inlined
}

// NotInlinedFilters returns the filters in the pair where virtual columns were
// not inlined.
func (c *CustomFuncs) NotInlinedFilters(pair InlinedVirtualColumnFiltersPair) memo.FiltersExpr {
	return pair.notInlined
}

// InlineSelectVirtualColumnsSucceeded returns true if the pair is not nil,
// indicating that InlineSelectVirtualColumns successfully inlined virtual
// columns into some of the filters.
func (c *CustomFuncs) InlineSelectVirtualColumnsSucceeded(
	pair InlinedVirtualColumnFiltersPair,
) bool {
	return len(pair.inlined) > 0
}

// InlineSelectProject searches the filter conditions for any variable
// references to columns from the given projections expression. Each variable is
// replaced by the corresponding inlined projection expression.
func (c *CustomFuncs) InlineSelectProject(
	filters memo.FiltersExpr, projections memo.ProjectionsExpr,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, len(filters))
	for i := range filters {
		item := &filters[i]
		newFilters[i] = c.f.ConstructFiltersItem(
			c.inlineProjections(item.Condition, projections).(opt.ScalarExpr),
		)
	}
	return newFilters
}

// InlineProjectProject searches the projection expressions for any variable
// references to columns from the given input (which must be a Project
// operator). Each variable is replaced by the corresponding inlined projection
// expression.
func (c *CustomFuncs) InlineProjectProject(
	innerProject *memo.ProjectExpr, projections memo.ProjectionsExpr, passthrough opt.ColSet,
) memo.RelExpr {
	innerProjections := innerProject.Projections

	newProjections := make(memo.ProjectionsExpr, len(projections))
	for i := range projections {
		item := &projections[i]
		newProjections[i] = c.f.ConstructProjectionsItem(
			c.inlineProjections(item.Element, innerProjections).(opt.ScalarExpr),
			item.Col,
		)
	}

	// Add any outer passthrough columns that refer to inner synthesized columns.
	newPassthrough := passthrough
	if !newPassthrough.Empty() {
		for i := range innerProjections {
			item := &innerProjections[i]
			if newPassthrough.Contains(item.Col) {
				newProjections = append(newProjections, *item)
				newPassthrough.Remove(item.Col)
			}
		}
	}

	return c.f.ConstructProject(innerProject.Input, newProjections, newPassthrough)
}

// Recursively walk the tree looking for references to projection expressions
// that need to be replaced.
func (c *CustomFuncs) inlineProjections(e opt.Expr, projections memo.ProjectionsExpr) opt.Expr {
	var replace ReplaceFunc
	replace = func(e opt.Expr) opt.Expr {
		switch t := e.(type) {
		case *memo.VariableExpr:
			for i := range projections {
				if projections[i].Col == t.Col {
					return projections[i].Element
				}
			}
			return t

		case memo.RelExpr:
			if !c.OuterCols(t).Empty() {
				// Should have prevented this in HasDuplicateRefs/HasCorrelatedSubquery.
				panic(errors.AssertionFailedf("cannot inline references within correlated subqueries"))
			}

			// No projections references possible, since there are no outer cols.
			return t
		}

		return c.f.Replace(e, replace)
	}

	return replace(e)
}

func (c *CustomFuncs) extractVarEqualsConst(
	e opt.Expr,
) (ok bool, left *memo.VariableExpr, right *memo.ConstExpr) {
	if eq, ok := e.(*memo.EqExpr); ok {
		if l, ok := eq.Left.(*memo.VariableExpr); ok {
			if r, ok := eq.Right.(*memo.ConstExpr); ok {
				return true, l, r
			}
		}
	}
	return false, nil, nil
}

// CanInlineConstVar returns true if there is an opportunity in the filters to
// inline a variable restricted to be a constant, as in:
//   SELECT * FROM foo WHERE a = 4 AND a IN (1, 2, 3, 4).
// =>
//   SELECT * FROM foo WHERE a = 4 AND 4 IN (1, 2, 3, 4).
func (c *CustomFuncs) CanInlineConstVar(f memo.FiltersExpr) bool {
	// usedIndices tracks the set of filter indices we've used to infer constant
	// values, so we don't inline into them.
	var usedIndices util.FastIntSet
	// fixedCols is the set of columns that the filters restrict to be a constant
	// value.
	var fixedCols opt.ColSet
	for i := range f {
		if ok, l, _ := c.extractVarEqualsConst(f[i].Condition); ok {
			colType := c.mem.Metadata().ColumnMeta(l.Col).Type
			if colinfo.HasCompositeKeyEncoding(colType) {
				// TODO(justin): allow inlining if the check we're doing is oblivious
				// to composite-ness.
				continue
			}
			if !fixedCols.Contains(l.Col) {
				fixedCols.Add(l.Col)
				usedIndices.Add(i)
			}
		}
	}
	for i := range f {
		if usedIndices.Contains(i) {
			continue
		}
		if f[i].ScalarProps().OuterCols.Intersects(fixedCols) {
			return true
		}
	}
	return false
}

// InlineConstVar performs the inlining detected by CanInlineConstVar.
func (c *CustomFuncs) InlineConstVar(f memo.FiltersExpr) memo.FiltersExpr {
	// usedIndices tracks the set of filter indices we've used to infer constant
	// values, so we don't inline into them.
	var usedIndices util.FastIntSet
	// fixedCols is the set of columns that the filters restrict to be a constant
	// value.
	var fixedCols opt.ColSet
	// vals maps columns which are restricted to be constant to the value they
	// are restricted to.
	vals := make(map[opt.ColumnID]opt.ScalarExpr)
	for i := range f {
		if ok, v, e := c.extractVarEqualsConst(f[i].Condition); ok {
			colType := c.mem.Metadata().ColumnMeta(v.Col).Type
			if colinfo.HasCompositeKeyEncoding(colType) {
				continue
			}
			if _, ok := vals[v.Col]; !ok {
				vals[v.Col] = e
				fixedCols.Add(v.Col)
				usedIndices.Add(i)
			}
		}
	}

	var replace ReplaceFunc
	replace = func(nd opt.Expr) opt.Expr {
		if t, ok := nd.(*memo.VariableExpr); ok {
			if e, ok := vals[t.Col]; ok {
				return e
			}
		}
		return c.f.Replace(nd, replace)
	}

	result := make(memo.FiltersExpr, len(f))
	for i := range f {
		inliningNeeded := f[i].ScalarProps().OuterCols.Intersects(fixedCols)
		// Don't inline if we used this position to infer a constant value, or if
		// the expression doesn't contain any fixed columns.
		if usedIndices.Contains(i) || !inliningNeeded {
			result[i] = f[i]
		} else {
			newCondition := replace(f[i].Condition).(opt.ScalarExpr)
			result[i] = c.f.ConstructFiltersItem(newCondition)
		}
	}
	return result
}
