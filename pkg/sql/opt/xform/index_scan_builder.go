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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
)

// indexScanBuilder composes a constrained, limited scan over a table index.
// Any filters are created as close the scan as possible, and index joins can
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
//   expr := sb.result
//
type indexScanBuilder struct {
	c      *CustomFuncs
	f      *norm.Factory
	mem    *memo.Memo
	tabID  opt.TableID
	result memo.Expr
	pkCols opt.ColSet
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

// setScan constructs a standalone Scan expression. It must be called before
// calling any of the add methods.
func (b *indexScanBuilder) setScan(def memo.PrivateID) {
	b.result = memo.Expr(memo.MakeScanExpr(def))
}

// addSelect constructs a new memo group for the current result expression and
// then wraps that in a new Select expression having the given filter.
func (b *indexScanBuilder) addSelect(filter memo.GroupID) {
	if filter != 0 {
		b.result = memo.Expr(memo.MakeSelectExpr(b.constructInput(), filter))
	}
}

// addSelectAfterSplit first splits the given filter into two parts: a filter
// that only involves columns in the given set, and a remainder filter that
// includes everything else. The filter that is bound by the columns becomes a
// Select expression that wraps the current result expression, and the remainder
// filter is returned.
func (b *indexScanBuilder) addSelectAfterSplit(filter memo.GroupID, cols opt.ColSet) memo.GroupID {
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

// addIndexJoin constructs a new memo group for the current result expression
// and then wraps that in a new IndexJoin expression that produces the given set
// of columns by lookup in the primary index.
func (b *indexScanBuilder) addIndexJoin(cols opt.ColSet) {
	private := b.mem.InternIndexJoinDef(&memo.IndexJoinDef{
		Table: b.tabID,
		Cols:  cols,
	})
	b.result = memo.Expr(memo.MakeIndexJoinExpr(b.constructInput(), private))
}

// constructInput constructs a new memo group from the current result expression
// and returns it.
func (b *indexScanBuilder) constructInput() memo.GroupID {
	switch b.result.Operator() {
	case opt.ScanOp:
		return b.f.ConstructScan(b.result.AsScan().Def())

	case opt.SelectOp:
		expr := b.result.AsSelect()
		return b.f.ConstructSelect(expr.Input(), expr.Filter())

	case opt.IndexJoinOp:
		expr := b.result.AsIndexJoin()
		return b.f.ConstructIndexJoin(expr.Input(), expr.Def())

	default:
		panic(fmt.Sprintf("unhandled operator: %v", b.result.Operator()))
	}
}
