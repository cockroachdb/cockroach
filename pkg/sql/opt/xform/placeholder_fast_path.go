// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/errors"
)

const maxRowCountForPlaceholderFastPath = 10

// TryPlaceholderFastPath attempts to produce a fully optimized memo with
// placeholders. This is only possible in very simple cases and involves special
// operators (PlaceholderScan) which use placeholders and resolve them at
// execbuild time.
//
// Currently we support cases where we can convert the entire tree to a single
// PlaceholderScan.
//
// If this function succeeds, the memo will be considered fully optimized.
func (o *Optimizer) TryPlaceholderFastPath() (_ opt.Expr, ok bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate internal errors without having to add
			// error checks everywhere throughout the code. This is only possible
			// because the code does not update shared state and does not manipulate
			// locks.
			if shouldCatch, e := errorutil.ShouldCatch(r); shouldCatch {
				err = e
			} else {
				// Other panic objects can't be considered "safe" and thus are
				// propagated as crashes that terminate the session.
				panic(r)
			}
		}
	}()

	root := o.mem.RootExpr().(memo.RelExpr)

	rootRelProps := root.Relational()
	// We are dealing with a memo that still contains placeholders. The statistics
	// for such a memo can be wildly overestimated. Even if our plan is correct,
	// the estimated row count for a scan is passed to the execution engine which
	// uses it to make sizing decisions. Passing a very high count can affect
	// performance significantly (see #64214). So we only use the fast path if the
	// estimated row count is small; typically this will happen when we constrain
	// columns that form a key and we know there will be at most one row.
	if rootRelProps.Stats.RowCount > maxRowCountForPlaceholderFastPath {
		return nil, false, nil
	}

	rootPhysicalProps := o.mem.RootProps()

	if !rootPhysicalProps.Ordering.Any() {
		return nil, false, nil
	}

	// TODO(radu): if we want to support more cases, we should use optgen to write
	// the rules.

	// Ignore any top-level Project that only passes through columns. It's safe to
	// remove it because the presentation property still enforces the final result
	// columns.
	expr := root
	if proj, isProject := expr.(*memo.ProjectExpr); isProject {
		if len(proj.Projections) != 0 {
			return nil, false, nil
		}
		expr = proj.Input
	}

	sel, isSelect := expr.(*memo.SelectExpr)
	if !isSelect {
		return nil, false, nil
	}
	scan, isScan := sel.Input.(*memo.ScanExpr)
	if !isScan {
		return nil, false, nil
	}

	var constrainedCols opt.ColSet
	for i := range sel.Filters {
		// Each condition must be an equality between a variable and a constant
		// value or a placeholder.
		cond := sel.Filters[i].Condition
		eq, isEq := cond.(*memo.EqExpr)
		if !isEq {
			return nil, false, nil
		}
		v, isVar := eq.Left.(*memo.VariableExpr)
		if !isVar {
			return nil, false, nil
		}
		if !opt.IsConstValueOp(eq.Right) && eq.Right.Op() != opt.PlaceholderOp {
			return nil, false, nil
		}
		if constrainedCols.Contains(v.Col) {
			// The same variable is part of multiple equalities.
			return nil, false, nil
		}
		constrainedCols.Add(v.Col)
	}
	numConstrained := len(sel.Filters)

	// Check if there is exactly one covering, non-partial index with a prefix
	// matching constrainedCols, and there are no partial indexes that could be
	// used by the query. If these conditions are satisfied, using this index is
	// always the optimal plan.

	md := o.mem.Metadata()
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
					return nil, false, nil
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
		if isCovering := scan.ScanPrivate.Cols.SubsetOf(indexCols); !isCovering {
			continue
		}

		if foundIndex != nil {
			// We found multiple candidate indexes. Choosing the best index (e.g.
			// fewer columns) requires costing.
			return nil, false, nil
		}
		foundIndex = index
	}

	if foundIndex == nil {
		return nil, false, nil
	}

	// Success!
	newPrivate := scan.ScanPrivate
	newPrivate.Cols = rootRelProps.OutputCols
	newPrivate.Index = foundIndex.Ordinal()

	span := make(memo.ScalarListExpr, numConstrained)
	for i := range span {
		col := tabMeta.MetaID.ColumnID(foundIndex.Column(i).Ordinal())
		for j := range sel.Filters {
			eq := sel.Filters[j].Condition.(*memo.EqExpr)
			if v := eq.Left.(*memo.VariableExpr); v.Col == col {
				span[i] = eq.Right
				break
			}
		}
		if span[i] == nil {
			// We checked above that the constrained columns match the index prefix.
			return nil, false, errors.AssertionFailedf("no span value")
		}
	}
	placeholderScan := &memo.PlaceholderScanExpr{
		Span:        span,
		ScanPrivate: newPrivate,
	}
	placeholderScan = o.mem.AddPlaceholderScanToGroup(placeholderScan, root)
	o.mem.SetBestProps(placeholderScan, rootPhysicalProps, &physical.Provided{}, 1.0 /* cost */)
	o.mem.SetRoot(placeholderScan, rootPhysicalProps)

	if util.CrdbTestBuild && !o.mem.IsOptimized() {
		return nil, false, errors.AssertionFailedf("IsOptimized() should be true")
	}

	return placeholderScan, true, nil
}
