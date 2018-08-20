// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
)

// indexScanBuilder composes a constrained, limited scan over a table index.
// Any filters are created as close to the scan as possible, and index joins can
// be used to scan a non-covering index. For example, in order to construct:
//
//   (IndexJoin
//     (Select (Scan $scanDef) $filter)
//     $indexJoinDef
//   )
//
// make the following calls:
//
//   var sb indexScanBuilder
//   sb.init(c, tabID)
//   sb.setScan(scanDef)
//   sb.addSelect(filter)
//   sb.addIndexJoin(cols)
//   expr := sb.build()
//
type indexScanBuilder struct {
	c            *CustomFuncs
	f            *norm.Factory
	mem          *memo.Memo
	tabID        opt.TableID
	pkCols       opt.ColSet
	scanDef      memo.PrivateID
	innerFilter  memo.GroupID
	outerFilter  memo.GroupID
	indexJoinDef memo.PrivateID
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
		primaryIndex := b.c.e.mem.Metadata().Table(b.tabID).Index(opt.PrimaryIndex)
		for i, cnt := 0, primaryIndex.KeyColumnCount(); i < cnt; i++ {
			b.pkCols.Add(int(b.tabID.ColumnID(primaryIndex.Column(i).Ordinal)))
		}
	}
	return b.pkCols
}

// setScan constructs a standalone Scan expression. As a side effect, it clears
// any expressions added during previous invocations of the builder.
func (b *indexScanBuilder) setScan(def memo.PrivateID) {
	b.scanDef = def
	b.innerFilter = 0
	b.outerFilter = 0
	b.indexJoinDef = 0
}

// addSelect wraps the input expression with a Select expression having the
// given filter.
func (b *indexScanBuilder) addSelect(filter memo.GroupID) {
	if filter != 0 {
		if b.indexJoinDef == 0 {
			if b.innerFilter != 0 {
				panic("cannot call addSelect methods twice before index join is added")
			}
			b.innerFilter = filter
		} else {
			if b.outerFilter != 0 {
				panic("cannot call addSelect methods twice after index join is added")
			}
			b.outerFilter = filter
		}
	}
}

// addSelectAfterSplit first splits the given filter into two parts: a filter
// that only involves columns in the given set, and a remaining filter that
// includes everything else. The filter that is bound by the columns becomes a
// Select expression that wraps the input expression, and the remaining filter
// is returned (or 0 if there is no remaining filter).
func (b *indexScanBuilder) addSelectAfterSplit(
	filter memo.GroupID, cols opt.ColSet,
) (remainingFilter memo.GroupID) {
	if filter == 0 {
		return 0
	}

	filterCols := b.c.OuterCols(filter)
	if filterCols.SubsetOf(cols) {
		// Filter is fully bound by the cols, so add entire filter.
		b.addSelect(filter)
		return 0
	}

	// Try to split filter.
	conditions := b.mem.NormExpr(filter).AsFilters().Conditions()
	boundConditions := b.c.ExtractBoundConditions(conditions, cols)
	if boundConditions.Length == 0 {
		// None of the filter conjuncts can be bound by the cols, so no expression
		// can be added.
		return filter
	}

	// Add conditions which are fully bound by the cols and return the rest.
	b.addSelect(b.f.ConstructFilters(boundConditions))
	return b.f.ConstructFilters(b.c.ExtractUnboundConditions(conditions, cols))
}

// addIndexJoin wraps the input expression with an IndexJoin expression that
// produces the given set of columns by lookup in the primary index.
func (b *indexScanBuilder) addIndexJoin(cols opt.ColSet) {
	if b.indexJoinDef != 0 {
		panic("cannot call addIndexJoin twice")
	}
	if b.outerFilter != 0 {
		panic("cannot add index join after an outer filter has been added")
	}
	b.indexJoinDef = b.mem.InternIndexJoinDef(&memo.IndexJoinDef{
		Table: b.tabID,
		Cols:  cols,
	})
}

// build constructs the final memo expression by composing together the various
// expressions that were specified by previous calls to various add methods.
func (b *indexScanBuilder) build() memo.Expr {
	// 1. Only scan.
	if b.innerFilter == 0 && b.indexJoinDef == 0 {
		return memo.Expr(memo.MakeScanExpr(b.scanDef))
	}

	// 2. Wrap scan in inner filter if it was added.
	input := b.f.ConstructScan(b.scanDef)
	if b.innerFilter != 0 {
		if b.indexJoinDef == 0 {
			return memo.Expr(memo.MakeSelectExpr(input, b.innerFilter))
		}

		input = b.f.ConstructSelect(input, b.innerFilter)
	}

	// 3. Wrap input in index join if it was added.
	if b.indexJoinDef != 0 {
		if b.outerFilter == 0 {
			return memo.Expr(memo.MakeIndexJoinExpr(input, b.indexJoinDef))
		}

		input = b.f.ConstructIndexJoin(input, b.indexJoinDef)
	}

	// 4. Wrap input in outer filter (which must exist at this point).
	if b.outerFilter == 0 {
		// indexJoinDef == 0: outerFilter == 0 handled by #1 and #2 above.
		// indexJoinDef != 0: outerFilter == 0 handled by #3 above.
		panic("outer filter cannot be 0 at this point")
	}
	return memo.Expr(memo.MakeSelectExpr(input, b.outerFilter))
}
