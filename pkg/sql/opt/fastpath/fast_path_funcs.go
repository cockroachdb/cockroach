// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fastpath

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// CustomFuncs contains all the custom match and replace functions used by the
// fast-path rules. The unnamed norm.CustomFuncs allows CustomFuncs to provide
// a clean interface for calling functions from both the fastpath and norm
// packages using the same struct.
type CustomFuncs struct {
	norm.CustomFuncs
	fp *fastPathExplorer
}

func (c *CustomFuncs) Init(fp *fastPathExplorer) {
	*c = CustomFuncs{fp: fp}
}

const maxRowCountForPlaceholderFastPath = 10

// CheckRowCountForFastPath returns true if the estimated row-count for the
// given expression is less than the threshold for planning the placeholder
// fast path.
func (c *CustomFuncs) CheckRowCountForFastPath(root memo.RelExpr) bool {
	// We are dealing with a memo that still contains placeholders. The statistics
	// for such a memo can be wildly overestimated. Even if our plan is correct,
	// the estimated row count for a scan is passed to the execution engine which
	// uses it to make sizing decisions. Passing a very high count can affect
	// performance significantly (see #64214). So we only use the fast path if the
	// estimated row count is small; typically this will happen when we constrain
	// columns that form a key and we know there will be at most one row.
	return root.Relational().Statistics().RowCount <= maxRowCountForPlaceholderFastPath
}

// CanUseFiltersInPlaceholderScan returns true if it is possible to inline the
// given filters in a PlaceholderScan expression.
func (c *CustomFuncs) CanUseFiltersInPlaceholderScan(filters memo.FiltersExpr) bool {
	var constrainedCols opt.ColSet
	for i := range filters {
		// Each condition must be an equality between a variable and a constant
		// value or a placeholder.
		cond := filters[i].Condition
		eq, isEq := cond.(*memo.EqExpr)
		if !isEq {
			return false
		}
		v, isVar := eq.Left.(*memo.VariableExpr)
		if !isVar {
			return false
		}
		if !opt.IsConstValueOp(eq.Right) && eq.Right.Op() != opt.PlaceholderOp {
			return false
		}
		if constrainedCols.Contains(v.Col) {
			// The same variable is part of multiple equalities.
			return false
		}
		if !verifyType(c.fp.f.Memo().Metadata(), v.Col, eq.Right.DataType()) {
			// Mixed-type comparisons require rule applications.
			return false
		}
		constrainedCols.Add(v.Col)
	}
	return true
}

// FindPlaceholderScanIndex attempts to find the index that will be used to
// construct a placeholder scan. This is possible when the following conditions
// are satisfied:
//  1. The table has exactly one covering, non-partial index with a prefix
//     matching the filters' constrained columns.
//  2. There are no partial indexes that could be used by the query.
//
// If the above conditions are satisfied, using the returned index will always
// be the optimal plan.
func (c *CustomFuncs) FindPlaceholderScanIndex(
	scan *memo.ScanPrivate, filters memo.FiltersExpr,
) (idx cat.Index, ok bool) {
	numConstrained := len(filters)
	constrainedCols := c.FilterOuterCols(filters)
	md := c.fp.f.Memo().Metadata()
	tabMeta := md.TableMeta(scan.Table)
	var foundIndex cat.Index
	for ord, n := 0, tabMeta.Table.IndexCount(); ord < n; ord++ {
		index := tabMeta.Table.Index(ord)
		if index.IsInverted() {
			continue
		}

		if pred, isPartialIndex := tabMeta.PartialIndexPredicate(ord); isPartialIndex {
			// We are not equipped to handle partial indexes.
			//
			// If it is a pseudo-partial index (true predicate), treat it as a regular
			// index.
			//
			// Otherwise, check if the predicate has any columns in common with our
			// filters; if it does, bail. If it doesn't, we can safely ignore the
			// index.
			predFilters := pred.(*memo.FiltersExpr)
			for i := range *predFilters {
				if (*predFilters)[i].ScalarProps().OuterCols.Intersects(constrainedCols) {
					return nil, false
				}
			}
			if !predFilters.IsTrue() {
				continue
			}
		}

		// Verify that the constraint columns are (in some permutation) a prefix of
		// the indexed columns.
		if index.LaxKeyColumnCount() < numConstrained {
			continue
		}

		var prefixCols opt.ColSet
		for i := 0; i < numConstrained; i++ {
			ord := index.Column(i).Ordinal()
			prefixCols.Add(tabMeta.MetaID.ColumnID(ord))
		}
		if !prefixCols.Equals(constrainedCols) {
			// The index columns don't match the filters.
			continue
		}

		// Check if the index is covering.
		indexCols := prefixCols.Copy()
		for i, n := numConstrained, index.ColumnCount(); i < n; i++ {
			ord := index.Column(i).Ordinal()
			indexCols.Add(tabMeta.MetaID.ColumnID(ord))
		}
		if isCovering := scan.Cols.SubsetOf(indexCols); !isCovering {
			continue
		}

		if foundIndex != nil {
			// We found multiple candidate indexes. Choosing the best index (e.g.
			// fewer columns) requires costing.
			return nil, false
		}
		foundIndex = index
	}
	return foundIndex, foundIndex != nil
}

// MakePlaceholderScanSpan constructs the span that will be scanned by a
// PlaceholderScan expression. It will consist of constants and placeholders.
func (c *CustomFuncs) MakePlaceholderScanSpan(
	filters memo.FiltersExpr, scan *memo.ScanPrivate, index cat.Index,
) memo.ScalarListExpr {
	tabMeta := c.fp.f.Memo().Metadata().TableMeta(scan.Table)
	span := make(memo.ScalarListExpr, len(filters))
	for i := range span {
		col := tabMeta.MetaID.ColumnID(index.Column(i).Ordinal())
		for j := range filters {
			eq := filters[j].Condition.(*memo.EqExpr)
			if v := eq.Left.(*memo.VariableExpr); v.Col == col {
				span[i] = eq.Right
				break
			}
		}
		if span[i] == nil {
			// We checked above that the constrained columns match the index prefix.
			panic(errors.AssertionFailedf("no span value"))
		}
	}
	return span
}

// MakePlaceholderScanPrivate constructs the private for a PlaceholderScan.
func (c *CustomFuncs) MakePlaceholderScanPrivate(
	root memo.RelExpr, scan *memo.ScanPrivate, index cat.Index,
) *memo.ScanPrivate {
	newPrivate := *scan
	newPrivate.Cols = root.Relational().OutputCols
	newPrivate.Index = index.Ordinal()
	return &newPrivate
}

// verifyType checks that the type of the index column col matches the
// given type. We disallow mixed-type comparisons because it would result in
// incorrect encodings (See #4313 and #81315).
// TODO(rytaft): We may be able to use the placeholder fast path for
// this case if we add logic similar to UnifyComparisonTypes.
func verifyType(md *opt.Metadata, col opt.ColumnID, typ *types.T) bool {
	return typ.Family() == types.UnknownFamily || md.ColumnMeta(col).Type.Equivalent(typ)
}

// IsValuesConstantsAndPlaceholders returns true if all values in the list are
// constant, placeholders or tuples containing constants, placeholders or other
// such nested tuples.
func (c *CustomFuncs) IsValuesConstantsAndPlaceholders(values *memo.ValuesExpr) bool {
	return values.IsConstantsAndPlaceholders()
}
