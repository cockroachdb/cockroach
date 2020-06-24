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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
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
//   sb.init(c, tabID)
//   sb.setScan(scanPrivate)
//   sb.addSelect(filters)
//   sb.addIndexJoin(cols)
//   expr := sb.build()
//
type indexScanBuilder struct {
	c                     *CustomFuncs
	f                     *norm.Factory
	mem                   *memo.Memo
	tabID                 opt.TableID
	pkCols                opt.ColSet
	scanPrivate           memo.ScanPrivate
	innerFilters          memo.FiltersExpr
	outerFilters          memo.FiltersExpr
	invertedFilterPrivate memo.InvertedFilterPrivate
	indexJoinPrivate      memo.IndexJoinPrivate
}

func (b *indexScanBuilder) init(c *CustomFuncs, tabID opt.TableID) {
	b.c = c
	b.f = c.e.f
	b.mem = c.e.mem
	b.tabID = tabID
}

// primaryKeyCols returns the columns from the scanned table's primary index.
func (b *indexScanBuilder) primaryKeyCols() opt.ColSet {
	// Ensure that pkCols set is initialized with the primary index columns.
	if b.pkCols.Empty() {
		primaryIndex := b.c.e.mem.Metadata().Table(b.tabID).Index(cat.PrimaryIndex)
		for i, cnt := 0, primaryIndex.KeyColumnCount(); i < cnt; i++ {
			b.pkCols.Add(b.tabID.ColumnID(primaryIndex.Column(i).Ordinal))
		}
	}
	return b.pkCols
}

// setScan constructs a standalone Scan expression. As a side effect, it clears
// any expressions added during previous invocations of the builder. setScan
// makes a copy of scanPrivate so that it doesn't escape.
func (b *indexScanBuilder) setScan(scanPrivate *memo.ScanPrivate) {
	b.scanPrivate = *scanPrivate
	b.innerFilters = nil
	b.outerFilters = nil
	b.invertedFilterPrivate = memo.InvertedFilterPrivate{}
	b.indexJoinPrivate = memo.IndexJoinPrivate{}
}

// addInvertedFilter wraps the input expression with an InvertedFilter
// expression having the given span expression.
func (b *indexScanBuilder) addInvertedFilter(
	spanExpr *invertedexpr.SpanExpression, invertedCol opt.ColumnID,
) {
	if spanExpr != nil {
		if b.invertedFilterPrivate.InvertedColumn != 0 {
			panic(errors.AssertionFailedf("cannot call addInvertedFilter twice"))
		}
		if b.indexJoinPrivate.Table != 0 {
			panic(errors.AssertionFailedf("cannot add inverted filter after index join is added"))
		}
		b.invertedFilterPrivate = memo.InvertedFilterPrivate{
			InvertedExpression: spanExpr,
			InvertedColumn:     invertedCol,
		}
	}
}

// addSelect wraps the input expression with a Select expression having the
// given filter.
func (b *indexScanBuilder) addSelect(filters memo.FiltersExpr) {
	if len(filters) != 0 {
		if b.indexJoinPrivate.Table == 0 {
			if b.innerFilters != nil {
				panic(errors.AssertionFailedf("cannot call addSelect methods twice before index join is added"))
			}
			b.innerFilters = filters
		} else {
			if b.outerFilters != nil {
				panic(errors.AssertionFailedf("cannot call addSelect methods twice after index join is added"))
			}
			b.outerFilters = filters
		}
	}
}

// addSelectAfterSplit first splits the given filter into two parts: a filter
// that only involves columns in the given set, and a remaining filter that
// includes everything else. The filter that is bound by the columns becomes a
// Select expression that wraps the input expression, and the remaining filter
// is returned (or 0 if there is no remaining filter).
func (b *indexScanBuilder) addSelectAfterSplit(
	filters memo.FiltersExpr, cols opt.ColSet,
) (remainingFilters memo.FiltersExpr) {
	if len(filters) == 0 {
		return nil
	}

	if b.c.FiltersBoundBy(filters, cols) {
		// Filter is fully bound by the cols, so add entire filter.
		b.addSelect(filters)
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
	b.addSelect(boundConditions)
	return b.c.ExtractUnboundConditions(filters, cols)
}

// addIndexJoin wraps the input expression with an IndexJoin expression that
// produces the given set of columns by lookup in the primary index.
func (b *indexScanBuilder) addIndexJoin(cols opt.ColSet) {
	if b.indexJoinPrivate.Table != 0 {
		panic(errors.AssertionFailedf("cannot call addIndexJoin twice"))
	}
	if b.outerFilters != nil {
		panic(errors.AssertionFailedf("cannot add index join after an outer filter has been added"))
	}
	b.indexJoinPrivate = memo.IndexJoinPrivate{
		Table: b.tabID,
		Cols:  cols,
	}
}

// build constructs the final memo expression by composing together the various
// expressions that were specified by previous calls to various add methods.
func (b *indexScanBuilder) build(grp memo.RelExpr) {
	// 1. Only scan.
	if len(b.innerFilters) == 0 && b.indexJoinPrivate.Table == 0 {
		b.mem.AddScanToGroup(&memo.ScanExpr{ScanPrivate: b.scanPrivate}, grp)
		return
	}

	// 2. Wrap scan in inner filter if it was added.
	input := b.f.ConstructScan(&b.scanPrivate)
	if len(b.innerFilters) != 0 {
		if b.indexJoinPrivate.Table == 0 && b.invertedFilterPrivate.InvertedColumn == 0 {
			b.mem.AddSelectToGroup(&memo.SelectExpr{Input: input, Filters: b.innerFilters}, grp)
			return
		}

		input = b.f.ConstructSelect(input, b.innerFilters)
	}

	// 3. Wrap input in inverted filter if it was added.
	if b.invertedFilterPrivate.InvertedColumn != 0 {
		if b.indexJoinPrivate.Table == 0 {
			invertedFilter := &memo.InvertedFilterExpr{
				Input: input, InvertedFilterPrivate: b.invertedFilterPrivate,
			}
			b.mem.AddInvertedFilterToGroup(invertedFilter, grp)
			return
		}

		input = b.f.ConstructInvertedFilter(input, &b.invertedFilterPrivate)
	}

	// 4. Wrap input in index join if it was added.
	if b.indexJoinPrivate.Table != 0 {
		if len(b.outerFilters) == 0 {
			indexJoin := &memo.IndexJoinExpr{Input: input, IndexJoinPrivate: b.indexJoinPrivate}
			b.mem.AddIndexJoinToGroup(indexJoin, grp)
			return
		}

		input = b.f.ConstructIndexJoin(input, &b.indexJoinPrivate)
	}

	// 5. Wrap input in outer filter (which must exist at this point).
	if len(b.outerFilters) == 0 {
		// indexJoinDef == 0: outerFilters == 0 handled by #1 and #2 above.
		// indexJoinDef != 0: outerFilters == 0 handled by #3 above.
		panic(errors.AssertionFailedf("outer filter cannot be 0 at this point"))
	}
	b.mem.AddSelectToGroup(&memo.SelectExpr{Input: input, Filters: b.outerFilters}, grp)
}
