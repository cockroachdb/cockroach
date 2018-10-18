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
// Relational.Rule.InterestingOrderings property of a relational operator.
func DeriveInterestingOrderings(e memo.RelExpr) opt.OrderingSet {
	l := e.Relational()
	if l.IsAvailable(props.InterestingOrderings) {
		return l.Rule.InterestingOrderings
	}
	l.SetAvailable(props.InterestingOrderings)

	var res opt.OrderingSet
	switch e.Op() {
	case opt.ScanOp:
		res = interestingOrderingsForScan(e.(*memo.ScanExpr))

	case opt.SelectOp, opt.IndexJoinOp, opt.LookupJoinOp:
		// Pass through child orderings.
		res = DeriveInterestingOrderings(e.Child(0).(memo.RelExpr))

	case opt.ProjectOp:
		res = interestingOrderingsForProject(e.(*memo.ProjectExpr))

	case opt.GroupByOp, opt.ScalarGroupByOp:
		res = interestingOrderingsForGroupBy(e)

	case opt.LimitOp, opt.OffsetOp:
		res = interestingOrderingsForLimit(e)

	default:
		if opt.IsJoinOp(e) {
			res = interestingOrderingsForJoin(e)
			break
		}

		res = opt.OrderingSet{}
	}

	l.Rule.InterestingOrderings = res
	return res
}

func interestingOrderingsForScan(scan *memo.ScanExpr) opt.OrderingSet {
	md := scan.Memo().Metadata()
	tab := md.Table(scan.Table)
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
			colID := scan.Table.ColumnID(indexCol.Ordinal)
			if !scan.Cols.Contains(int(colID)) {
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

func interestingOrderingsForProject(prj *memo.ProjectExpr) opt.OrderingSet {
	inOrd := DeriveInterestingOrderings(prj.Input)
	res := inOrd.Copy()
	res.RestrictToCols(prj.Passthrough)
	return res
}

func interestingOrderingsForGroupBy(rel memo.RelExpr) opt.OrderingSet {
	private := rel.Private().(*memo.GroupingPrivate)
	if private.GroupingCols.Empty() {
		// This is a scalar group-by, returning a single row.
		return nil
	}

	res := DeriveInterestingOrderings(rel.Child(0).(memo.RelExpr)).Copy()
	if !private.Ordering.Any() {
		ordering := private.Ordering.ToOrdering()
		res.RestrictToPrefix(ordering)
		if len(res) == 0 {
			res.Add(ordering)
		}
	}

	// We can only keep orderings on grouping columns.
	res.RestrictToCols(private.GroupingCols)
	return res
}

func interestingOrderingsForLimit(rel memo.RelExpr) opt.OrderingSet {
	res := DeriveInterestingOrderings(rel.Child(0).(memo.RelExpr))
	ord := rel.Private().(*props.OrderingChoice).ToOrdering()
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

func interestingOrderingsForJoin(rel memo.RelExpr) opt.OrderingSet {
	if rel.Op() == opt.SemiJoinOp || rel.Op() == opt.AntiJoinOp {
		// TODO(radu): perhaps take into account right-side interesting orderings on
		// equality columns.
		return DeriveInterestingOrderings(rel.Child(0).(memo.RelExpr))
	}
	// For a join, we could conceivably preserve the order of one side (even with
	// hash-join, depending on which side we store).
	ordLeft := DeriveInterestingOrderings(rel.Child(0).(memo.RelExpr))
	ordRight := DeriveInterestingOrderings(rel.Child(1).(memo.RelExpr))
	ord := make(opt.OrderingSet, 0, len(ordLeft)+len(ordRight))
	ord = append(ord, ordLeft...)
	ord = append(ord, ordRight...)
	return ord
}
