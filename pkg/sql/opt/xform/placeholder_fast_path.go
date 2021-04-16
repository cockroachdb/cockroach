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
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				// Other panic objects can't be considered "safe" and thus are
				// propagated as crashes that terminate the session.
				panic(r)
			}
		}
	}()

	root := o.mem.RootExpr().(memo.RelExpr)
	rootProps := o.mem.RootProps()

	if !rootProps.Ordering.Any() {
		return nil, false, nil
	}

	rootColumns := root.Relational().OutputCols

	// TODO(radu): if we want to support more cases, we should use optgen to write
	// the rules.

	// Ignore any top-level Project that only passes through columns. It's safe to
	// remove it because the presentation property still enforces the final result
	// columns.
	expr := root
	if proj, ok := expr.(*memo.ProjectExpr); ok {
		if len(proj.Projections) != 0 {
			return nil, false, nil
		}
		expr = proj.Input
	}

	sel, ok := expr.(*memo.SelectExpr)
	if !ok {
		return nil, false, nil
	}
	var constrainedCols opt.ColSet
	for i := range sel.Filters {
		// Each condition must be an equality between a variable and a constant
		// value or a placeholder.
		cond := sel.Filters[i].Condition
		eq, ok := cond.(*memo.EqExpr)
		if !ok {
			return nil, false, nil
		}
		v, ok := eq.Left.(*memo.VariableExpr)
		if !ok {
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

	scan, ok := sel.Input.(*memo.ScanExpr)
	if !ok {
		return nil, false, nil
	}

	// Check if there is exactly one covering, non-partial index with a prefix
	// matching constrainedCols. In that case, using this index is always the
	// optimal plan.

	var iter scanIndexIter
	iter.Init(
		o.evalCtx, &o.f, o.mem, nil /* im */, &scan.ScanPrivate, nil, /* filters */
		rejectInvertedIndexes|rejectPartialIndexes,
	)

	var foundIndex cat.Index
	var foundMultiple bool
	md := o.mem.Metadata()
	tabMeta := md.TableMeta(scan.Table)
	iter.ForEach(func(index cat.Index, _ memo.FiltersExpr, _ opt.ColSet, isCovering bool, _ memo.ProjectionsExpr) {
		if !isCovering {
			return
		}
		if index.ColumnCount() < numConstrained {
			return
		}
		var prefixCols opt.ColSet
		for i := 0; i < numConstrained; i++ {
			ord := index.Column(i).Ordinal()
			prefixCols.Add(tabMeta.MetaID.ColumnID(ord))
		}
		if prefixCols.Equals(constrainedCols) {
			if foundIndex != nil {
				foundMultiple = true
			}
			foundIndex = index
		}
	})
	if foundIndex == nil || foundMultiple {
		return nil, false, nil
	}

	// Success!
	newPrivate := scan.ScanPrivate
	newPrivate.Cols = rootColumns
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
	o.mem.SetBestProps(placeholderScan, rootProps, &physical.Provided{}, 1.0 /* cost */)
	o.mem.SetRoot(placeholderScan, rootProps)

	if util.CrdbTestBuild && !o.mem.IsOptimized() {
		return nil, false, errors.AssertionFailedf("IsOptimized() should be true")
	}

	return placeholderScan, true, nil
}
