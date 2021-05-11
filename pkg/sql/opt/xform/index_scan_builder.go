// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/errors"
)

// indexScanBuilder composes a constrained, limited scan over a table index.
// Any filters are created as close to the scan as possible, and index joins can
// be used to scan a non-covering index. For example, in order to construct:
//
//   (IndexJoin
//     (Select (Scan $scanPrivate) $filters)
//     $indexJoinPrivate
//   )
//
// make the following calls:
//
//   var sb indexScanBuilder
//   sb.Init(c, tabID)
//   sb.SetScan(scanPrivate)
//   sb.AddSelect(filters)
//   sb.AddIndexJoin(cols)
//   expr := sb.Build()
//
type indexScanBuilder struct {
	c                     *CustomFuncs
	f                     *norm.Factory
	mem                   *memo.Memo
	tabID                 opt.TableID
	scanPrivate           memo.ScanPrivate
	constProjections      memo.ProjectionsExpr
	innerFilters          memo.FiltersExpr
	outerFilters          memo.FiltersExpr
	invertedFilterPrivate memo.InvertedFilterPrivate
	indexJoinPrivate      memo.IndexJoinPrivate
}

// Init initializes an indexScanBuilder.
func (b *indexScanBuilder) Init(c *CustomFuncs, tabID opt.TableID) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*b = indexScanBuilder{
		c:     c,
		f:     c.e.f,
		mem:   c.e.mem,
		tabID: tabID,
	}
}

// SetScan constructs a standalone Scan expression. As a side effect, it clears
// any expressions added during previous invocations of the builder. SetScan
// makes a copy of scanPrivate so that it doesn't escape.
func (b *indexScanBuilder) SetScan(scanPrivate *memo.ScanPrivate) {
	*b = indexScanBuilder{
		c:           b.c,
		f:           b.f,
		mem:         b.mem,
		tabID:       b.tabID,
		scanPrivate: *scanPrivate,
	}
}

// AddConstProjections wraps the input expression with a Project expression with
// the given constant projection expressions.
func (b *indexScanBuilder) AddConstProjections(proj memo.ProjectionsExpr) {
	if len(proj) != 0 {
		if b.hasConstProjections() {
			panic(errors.AssertionFailedf("cannot call AddConstProjections twice"))
		}
		if b.hasInnerFilters() || b.hasOuterFilters() {
			panic(errors.AssertionFailedf("cannot call AddConstProjections after filters are added"))
		}
		b.constProjections = proj
	}
}

// AddInvertedFilter wraps the input expression with an InvertedFilter
// expression having the given span expression.
func (b *indexScanBuilder) AddInvertedFilter(
	spanExpr *inverted.SpanExpression,
	pfState *invertedexpr.PreFiltererStateForInvertedFilterer,
	invertedCol opt.ColumnID,
) {
	if spanExpr != nil {
		if b.hasInvertedFilter() {
			panic(errors.AssertionFailedf("cannot call AddInvertedFilter twice"))
		}
		if b.hasIndexJoin() {
			panic(errors.AssertionFailedf("cannot call AddInvertedFilter after index join is added"))
		}
		b.invertedFilterPrivate = memo.InvertedFilterPrivate{
			InvertedExpression: spanExpr,
			PreFiltererState:   pfState,
			InvertedColumn:     invertedCol,
		}
	}
}

// AddSelect wraps the input expression with a Select expression having the
// given filter.
func (b *indexScanBuilder) AddSelect(filters memo.FiltersExpr) {
	if len(filters) != 0 {
		if !b.hasIndexJoin() {
			if b.hasInnerFilters() {
				panic(errors.AssertionFailedf("cannot call AddSelect methods twice before index join is added"))
			}
			b.innerFilters = filters
		} else {
			if b.hasOuterFilters() {
				panic(errors.AssertionFailedf("cannot call AddSelect methods twice after index join is added"))
			}
			b.outerFilters = filters
		}
	}
}

// AddSelectAfterSplit first splits the given filter into two parts: a filter
// that only involves columns in the given set, and a remaining filter that
// includes everything else. The filter that is bound by the columns becomes a
// Select expression that wraps the input expression, and the remaining filter
// is returned (or 0 if there is no remaining filter).
func (b *indexScanBuilder) AddSelectAfterSplit(
	filters memo.FiltersExpr, cols opt.ColSet,
) (remainingFilters memo.FiltersExpr) {
	if len(filters) == 0 {
		return nil
	}

	if b.c.FiltersBoundBy(filters, cols) {
		// Filter is fully bound by the cols, so add entire filter.
		b.AddSelect(filters)
		return nil
	}

	// Try to split filter.
	boundConditions := b.c.ExtractBoundConditions(filters, cols)
	if len(boundConditions) == 0 {
		// None of the filter conjuncts can be bound by the cols, so no expression
		// can be added.
		return filters
	}

	// Add conditions which are fully bound by the cols and return the rest.
	b.AddSelect(boundConditions)
	return b.c.ExtractUnboundConditions(filters, cols)
}

// AddIndexJoin wraps the input expression with an IndexJoin expression that
// produces the given set of columns by lookup in the primary index.
func (b *indexScanBuilder) AddIndexJoin(cols opt.ColSet) {
	if b.hasIndexJoin() {
		panic(errors.AssertionFailedf("cannot call AddIndexJoin twice"))
	}
	if b.hasOuterFilters() {
		panic(errors.AssertionFailedf("cannot call AddIndexJoin after an outer filter has been added"))
	}
	b.indexJoinPrivate = memo.IndexJoinPrivate{
		Table: b.tabID,
		Cols:  cols,
	}
}

// Build constructs the final memo expression by composing together the various
// expressions that were specified by previous calls to various add methods.
func (b *indexScanBuilder) Build(grp memo.RelExpr) {
	// 1. Only scan.
	if !b.hasConstProjections() && !b.hasInnerFilters() && !b.hasInvertedFilter() && !b.hasIndexJoin() {
		b.mem.AddScanToGroup(&memo.ScanExpr{ScanPrivate: b.scanPrivate}, grp)
		return
	}

	input := b.f.ConstructScan(&b.scanPrivate)

	// 2. Wrap input in a Project if constant projections were added.
	if b.hasConstProjections() {
		if !b.hasInnerFilters() && !b.hasInvertedFilter() && !b.hasIndexJoin() {
			b.mem.AddProjectToGroup(&memo.ProjectExpr{
				Input:       input,
				Projections: b.constProjections,
				Passthrough: b.scanPrivate.Cols,
			}, grp)
			return
		}

		input = b.f.ConstructProject(input, b.constProjections, b.scanPrivate.Cols)
	}

	// 3. Wrap input in inner filter if it was added.
	if b.hasInnerFilters() {
		if !b.hasInvertedFilter() && !b.hasIndexJoin() {
			b.mem.AddSelectToGroup(&memo.SelectExpr{Input: input, Filters: b.innerFilters}, grp)
			return
		}

		input = b.f.ConstructSelect(input, b.innerFilters)
	}

	// 4. Wrap input in inverted filter if it was added.
	if b.hasInvertedFilter() {
		if !b.hasIndexJoin() {
			invertedFilter := &memo.InvertedFilterExpr{
				Input: input, InvertedFilterPrivate: b.invertedFilterPrivate,
			}
			b.mem.AddInvertedFilterToGroup(invertedFilter, grp)
			return
		}

		input = b.f.ConstructInvertedFilter(input, &b.invertedFilterPrivate)
	}

	// 5. Wrap input in index join if it was added.
	if b.hasIndexJoin() {
		if !b.hasOuterFilters() {
			indexJoin := &memo.IndexJoinExpr{Input: input, IndexJoinPrivate: b.indexJoinPrivate}
			b.mem.AddIndexJoinToGroup(indexJoin, grp)
			return
		}

		input = b.f.ConstructIndexJoin(input, &b.indexJoinPrivate)
	}

	// 6. Wrap input in outer filter (which must exist at this point).
	if !b.hasOuterFilters() {
		// indexJoinDef == 0: outerFilters == 0 handled by #1-4 above.
		// indexJoinDef != 0: outerFilters == 0 handled by #5 above.
		panic(errors.AssertionFailedf("outer filter cannot be 0 at this point"))
	}
	b.mem.AddSelectToGroup(&memo.SelectExpr{Input: input, Filters: b.outerFilters}, grp)
}

// hasConstProjections returns true if constant projections have been added to
// the builder.
func (b *indexScanBuilder) hasConstProjections() bool {
	return len(b.constProjections) != 0
}

// hasInnerFilters returns true if inner filters have been added to the builder.
func (b *indexScanBuilder) hasInnerFilters() bool {
	return len(b.innerFilters) != 0
}

// hasOuterFilters returns true if outer filters have been added to the builder.
func (b *indexScanBuilder) hasOuterFilters() bool {
	return len(b.outerFilters) != 0
}

// hasInvertedFilter returns true if inverted filters have been added to the
// builder.
func (b *indexScanBuilder) hasInvertedFilter() bool {
	return b.invertedFilterPrivate.InvertedColumn != 0
}

// hasIndexJoin returns true if an index join has been added to the builder.
func (b *indexScanBuilder) hasIndexJoin() bool {
	return b.indexJoinPrivate.Table != 0
}
