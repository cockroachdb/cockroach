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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// DeriveInterestingOrderings calculates and returns the
// props.Logical.Rule.InterestingOrderings property of a relational operator.
func DeriveInterestingOrderings(ev memo.ExprView) opt.OrderingSet {
	l := ev.Logical().Relational
	if l.IsAvailable(props.InterestingOrderings) {
		return l.Rule.InterestingOrderings
	}
	l.SetAvailable(props.InterestingOrderings)

	var res opt.OrderingSet
	switch ev.Operator() {
	case opt.ScanOp:
		res = interestingOrderingsForScan(ev)

	case opt.SelectOp, opt.IndexJoinOp, opt.LookupJoinOp:
		// Pass through child orderings.
		res = DeriveInterestingOrderings(ev.Child(0))

	case opt.ProjectOp:
		res = interestingOrderingsForProject(ev)

	case opt.GroupByOp, opt.ScalarGroupByOp:
		res = interestingOrderingsForGroupBy(ev)

	case opt.LimitOp, opt.OffsetOp:
		res = interestingOrderingsForLimit(ev)

	default:
		if ev.IsJoin() {
			res = interestingOrderingsForJoin(ev)
			break
		}

		res = opt.OrderingSet{}
	}

	l.Rule.InterestingOrderings = res
	return res
}

func interestingOrderingsForScan(ev memo.ExprView) opt.OrderingSet {
	def := ev.Private().(*memo.ScanOpDef)
	md := ev.Metadata()
	tab := md.Table(def.Table)
	ord := make(opt.OrderingSet, 0, tab.IndexCount())
	for i := 0; i < tab.IndexCount(); i++ {
		index := tab.Index(i)
		if index.IsInverted() {
			continue
		}
		numIndexCols := index.KeyColumnCount()
		var o opt.Ordering
		for j := 0; j < numIndexCols; j++ {
			indexCol := index.Column(j)
			colID := def.Table.ColumnID(indexCol.Ordinal)
			if !def.Cols.Contains(int(colID)) {
				break
			}
			if o == nil {
				o = make(opt.Ordering, 0, numIndexCols)
			}
			o = append(o, opt.MakeOrderingColumn(colID, indexCol.Descending))
		}
		if o != nil {
			ord.Add(o)
		}
	}
	return ord
}

func interestingOrderingsForProject(ev memo.ExprView) opt.OrderingSet {
	inOrd := DeriveInterestingOrderings(ev.Child(0))
	passthroughCols := ev.Child(1).Private().(*memo.ProjectionsOpDef).PassthroughCols
	res := inOrd.Copy()
	res.RestrictToCols(passthroughCols)
	return res
}

func interestingOrderingsForGroupBy(ev memo.ExprView) opt.OrderingSet {
	def := ev.Private().(*memo.GroupByDef)
	if def.GroupingCols.Empty() {
		// This is a scalar group-by, returning a single row.
		return nil
	}

	res := DeriveInterestingOrderings(ev.Child(0)).Copy()
	if !def.Ordering.Any() {
		ordering := def.Ordering.ToOrdering()
		res.RestrictToPrefix(ordering)
		if len(res) == 0 {
			res.Add(ordering)
		}
	}

	// We can only keep orderings on grouping columns.
	res.RestrictToCols(def.GroupingCols)
	return res
}

func interestingOrderingsForLimit(ev memo.ExprView) opt.OrderingSet {
	res := DeriveInterestingOrderings(ev.Child(0))
	ord := ev.Private().(*props.OrderingChoice).ToOrdering()
	if ord.Empty() {
		return res
	}
	res = res.Copy()
	res.RestrictToPrefix(ord)
	if len(res) == 0 {
		res.Add(ord)
	}
	return res
}

func interestingOrderingsForJoin(ev memo.ExprView) opt.OrderingSet {
	if ev.Operator() == opt.SemiJoinOp || ev.Operator() == opt.AntiJoinOp {
		// TODO(radu): perhaps take into account right-side interesting orderings on
		// equality columns.
		return DeriveInterestingOrderings(ev.Child(0))
	}
	// For a join, we could conceivably preserve the order of one side (even with
	// hash-join, depending on which side we store).
	ordLeft := DeriveInterestingOrderings(ev.Child(0))
	ordRight := DeriveInterestingOrderings(ev.Child(1))
	ord := make(opt.OrderingSet, 0, len(ordLeft)+len(ordRight))
	ord = append(ord, ordLeft...)
	ord = append(ord, ordRight...)
	return ord
}
