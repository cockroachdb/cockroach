// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
)

// DeriveRestrictedInterestingOrderings calculates and returns the entry of the
// Relational.Rule.RestrictedInterestingOrderings property of a relational
// operator that corresponds to the given columns.
func DeriveRestrictedInterestingOrderings(
	mem *memo.Memo, e memo.RelExpr, cols opt.ColSet,
) props.OrderingSet {
	l := e.Relational()
	fds := &l.FuncDeps
	// We follow the convention of checking if the property is available, even
	// though it is not necessary because the property is a slice. The overhead
	// of the check is basically zero.
	if l.IsAvailable(props.RestrictedInterestingOrderings) {
		for i := range l.Rule.RestrictedInterestingOrderings {
			ord := &l.Rule.RestrictedInterestingOrderings[i]
			if cols.Equals(ord.Cols) {
				return ord.OrderingSet
			}
		}
	}
	l.SetAvailable(props.RestrictedInterestingOrderings)

	// Derive the interesting orderings and restrict them to the given columns.
	orders := DeriveInterestingOrderings(mem, e).Copy()
	orders.RestrictToCols(cols, fds)

	l.Rule.RestrictedInterestingOrderings = append(l.Rule.RestrictedInterestingOrderings,
		props.RestrictedInterestingOrdering{
			OrderingSet: orders,
			Cols:        cols,
		})
	return orders
}

// DeriveInterestingOrderings calculates and returns the
// Relational.Rule.InterestingOrderings property of a relational operator.
func DeriveInterestingOrderings(mem *memo.Memo, e memo.RelExpr) props.OrderingSet {
	l := e.Relational()
	if l.IsAvailable(props.InterestingOrderings) {
		return l.Rule.InterestingOrderings
	}
	l.SetAvailable(props.InterestingOrderings)

	// We cache the interesting orderings for the entire group, so we always use
	// the normalized expression.
	e = e.FirstExpr()

	var res props.OrderingSet
	switch e.Op() {
	case opt.ScanOp:
		res = interestingOrderingsForScan(mem, e.(*memo.ScanExpr))

	case opt.SelectOp, opt.IndexJoinOp, opt.LookupJoinOp:
		res = interestingOrderingsForExpr(mem, e)

	case opt.ProjectOp:
		res = interestingOrderingsForProject(mem, e.(*memo.ProjectExpr))

	case opt.GroupByOp, opt.ScalarGroupByOp:
		res = interestingOrderingsForGroupBy(mem, e)

	case opt.LimitOp, opt.OffsetOp:
		res = interestingOrderingsForLimit(mem, e)

	default:
		if opt.IsJoinOp(e) {
			res = interestingOrderingsForJoin(mem, e)
			break
		}

		if opt.IsSetOp(e) {
			res = interestingOrderingsForSetOp(mem, e)
			break
		}

		res = props.OrderingSet{}
	}

	l.Rule.InterestingOrderings = res
	return res
}

// interestingOrderingsForScan calculates interesting orderings of a scan based
// on the indexes on underlying table.
//
// Note that partial indexes are considered here, even though they don't provide
// an interesting ordering for all values in the column. This is required in
// order to consider partial indexes for certain optimization rules, such as
// GenerateMergeJoins.
func interestingOrderingsForScan(mem *memo.Memo, scan *memo.ScanExpr) props.OrderingSet {
	md := mem.Metadata()
	tab := md.Table(scan.Table)
	var ord props.OrderingSet

	addIndexOrdering := func(indexOrd cat.IndexOrdinal, fds *props.FuncDepSet, exactPrefix int) {
		index := tab.Index(indexOrd)
		if index.Type() != idxtype.FORWARD {
			// Do not consider inverted or vector indexes.
			return
		}
		numIndexCols := index.KeyColumnCount()
		var o props.OrderingChoice
		o.Columns = make([]props.OrderingColumnChoice, 0, numIndexCols)
		for j := 0; j < numIndexCols; j++ {
			indexCol := index.Column(j)
			colID := scan.Table.ColumnID(indexCol.Ordinal())
			if j < exactPrefix {
				o.Optional.Add(colID)
			} else {
				o.AppendCol(colID, indexCol.Descending)
			}
		}
		if o.CanSimplify(fds) {
			o.Simplify(fds)
		}
		o.RestrictToCols(scan.Cols)
		if !o.Any() {
			ord.Add(&o)
		}
	}

	if scan.IsCanonical() {
		// This scan is canonical so it could be transformed into a scan over any of
		// the table's indexes. Add orderings for all of them.
		ord = make(props.OrderingSet, 0, tab.IndexCount())
		for i := 0; i < tab.IndexCount(); i++ {
			// IsCanonical implies no constraints so exactPrefix is 0.
			addIndexOrdering(i, &scan.Relational().FuncDeps, 0)
		}
	} else {
		// This scan is not canonical, so we can only use the ordering implied by
		// its index.
		ord = make(props.OrderingSet, 0, 1)
		addIndexOrdering(scan.Index, &scan.Relational().FuncDeps, scan.ExactPrefix)
	}

	return ord
}

func interestingOrderingsForExpr(mem *memo.Memo, e memo.RelExpr) props.OrderingSet {
	res := DeriveInterestingOrderings(mem, e.Child(0).(memo.RelExpr)).Copy()
	res.Simplify(&e.Relational().FuncDeps)
	return res
}

func interestingOrderingsForProject(mem *memo.Memo, prj *memo.ProjectExpr) props.OrderingSet {
	inOrd := DeriveInterestingOrderings(mem, prj.Input)
	res := inOrd.Copy()
	outCols := prj.Relational().OutputCols
	fds := prj.InternalFDs()
	res.RestrictToCols(outCols, fds)
	return res
}

func interestingOrderingsForGroupBy(mem *memo.Memo, rel memo.RelExpr) props.OrderingSet {
	private := rel.Private().(*memo.GroupingPrivate)
	if private.GroupingCols.Empty() {
		// This is a scalar group-by, returning a single row.
		return nil
	}

	res := DeriveInterestingOrderings(mem, rel.Child(0).(memo.RelExpr)).Copy()
	if !private.Ordering.Any() {
		ordering := &private.Ordering
		res.RestrictToImplies(ordering)
		if len(res) == 0 {
			res.Add(ordering)
		}
	}

	// We can only keep orderings on grouping columns.
	res.RestrictToCols(private.GroupingCols, &rel.Relational().FuncDeps)
	return res
}

func interestingOrderingsForLimit(mem *memo.Memo, rel memo.RelExpr) props.OrderingSet {
	res := DeriveInterestingOrderings(mem, rel.Child(0).(memo.RelExpr))
	ord := rel.Private().(*props.OrderingChoice)
	if ord.Any() {
		return res
	}
	res = res.Copy()
	res.RestrictToImplies(ord)
	if len(res) == 0 {
		res.Add(ord)
	}
	return res
}

func interestingOrderingsForJoin(mem *memo.Memo, rel memo.RelExpr) props.OrderingSet {
	if rel.Op() == opt.SemiJoinOp || rel.Op() == opt.AntiJoinOp {
		// TODO(radu): perhaps take into account right-side interesting orderings on
		// equality columns.
		return DeriveInterestingOrderings(mem, rel.Child(0).(memo.RelExpr))
	}
	// For a join, we could conceivably preserve the order of one side (even with
	// hash-join, depending on which side we store).
	// TODO(drewk): add logic for orderings on columns from both sides, since both
	//  lookup and merge joins can provide them.
	ordLeft := DeriveInterestingOrderings(mem, rel.Child(0).(memo.RelExpr))
	ordRight := DeriveInterestingOrderings(mem, rel.Child(1).(memo.RelExpr))
	ord := make(props.OrderingSet, 0, len(ordLeft)+len(ordRight))
	ord = append(ord, ordLeft...)
	ord = append(ord, ordRight...)
	return ord
}

func interestingOrderingsForSetOp(mem *memo.Memo, rel memo.RelExpr) props.OrderingSet {
	if rel.Op() == opt.LocalityOptimizedSearchOp {
		// LocalityOptimizedSearchOp does not support passing through orderings.
		return nil
	}
	leftChild := rel.Child(0).(memo.RelExpr)
	rightChild := rel.Child(1).(memo.RelExpr)
	ordLeft := DeriveInterestingOrderings(mem, leftChild)
	ordRight := DeriveInterestingOrderings(mem, rightChild)
	private := rel.Private().(*memo.SetPrivate)

	// We can only keep orderings on output columns.
	ordLeft.RestrictToCols(private.LeftCols.ToSet(), &leftChild.Relational().FuncDeps)
	ordRight.RestrictToCols(private.RightCols.ToSet(), &rightChild.Relational().FuncDeps)

	ordLeft = ordLeft.RemapColumns(private.LeftCols, private.OutCols)
	ordRight = ordRight.RemapColumns(private.RightCols, private.OutCols)

	ord := make(props.OrderingSet, 0, len(ordLeft)+len(ordRight))
	ord = append(ord, ordLeft...)
	for i := range ordRight {
		ord.Add(&ordRight[i])
	}

	if !private.Ordering.Any() {
		ordering := &private.Ordering
		ord.RestrictToImplies(ordering)
		if len(ord) == 0 {
			ord.Add(ordering)
		}
	}
	return ord
}
