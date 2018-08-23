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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// CustomFuncs contains all the custom match and replace functions used by
// the exploration rules. The unnamed xfunc.CustomFuncs allows
// CustomFuncs to provide a clean interface for calling functions from both the
// xform and xfunc packages using the same struct.
type CustomFuncs struct {
	norm.CustomFuncs
	e *explorer
}

// Init initializes a new CustomFuncs with the given explorer.
func (c *CustomFuncs) Init(e *explorer) {
	c.CustomFuncs.Init(e.f)
	c.e = e
}

// ----------------------------------------------------------------------
//
// Scan Rules
//   Custom match and replace functions used with scan.opt rules.
//
// ----------------------------------------------------------------------

// IsCanonicalScan returns true if the given ScanOpDef is an original unaltered
// primary index Scan operator (i.e. unconstrained and not limited).
func (c *CustomFuncs) IsCanonicalScan(def memo.PrivateID) bool {
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	return scanOpDef.Index == opt.PrimaryIndex &&
		scanOpDef.Constraint == nil &&
		scanOpDef.HardLimit == 0
}

// GenerateIndexScans enumerates all secondary indexes on the given Scan
// operator's table and generates an alternate Scan operator for each index that
// includes the set of needed columns specified in the ScanOpDef.
//
// NOTE: This does not generate index joins for non-covering indexes (except in
//       case of ForceIndex). Index joins are usually only introduced "one level
//       up", when the Scan operator is wrapped by an operator that constrains
//       or limits scan output in some way (e.g. Select, Limit, InnerJoin).
//       Index joins are only lower cost when their input does not include all
//       rows from the table. See ConstrainScans and LimitScans for cases where
//       index joins are introduced into the memo.
func (c *CustomFuncs) GenerateIndexScans(def memo.PrivateID) []memo.Expr {
	c.e.exprs = c.e.exprs[:0]
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)

	// Iterate over all secondary indexes.
	var iter scanIndexIter
	iter.init(c.e.mem, scanOpDef)
	for iter.next() {
		// Skip primary index.
		if iter.index == opt.PrimaryIndex {
			continue
		}

		// If the secondary index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if iter.isCovering() {
			newDef := *scanOpDef
			newDef.Index = iter.index
			indexScan := memo.MakeScanExpr(c.e.mem.InternScanOpDef(&newDef))
			c.e.exprs = append(c.e.exprs, memo.Expr(indexScan))
			continue
		}

		// Otherwise, if the index must be forced, then construct an IndexJoin
		// operator that provides the columns missing from the index. Note that
		// if ForceIndex=true, scanIndexIter only returns the one index that is
		// being forced, so no need to check that here.
		if !scanOpDef.Flags.ForceIndex {
			continue
		}

		var sb indexScanBuilder
		sb.init(c, scanOpDef.Table)

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newDef := *scanOpDef
		newDef.Index = iter.index
		newDef.Cols = iter.indexCols().Intersection(scanOpDef.Cols)
		newDef.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(c.e.f.InternScanOpDef(&newDef))

		sb.addIndexJoin(scanOpDef.Cols)
		c.e.exprs = append(c.e.exprs, sb.build())
	}

	return c.e.exprs
}

// ----------------------------------------------------------------------
//
// Select Rules
//   Custom match and replace functions used with select.opt rules.
//
// ----------------------------------------------------------------------

// GenerateConstrainedScans enumerates all secondary indexes on the Scan
// operator's table and tries to push the given Select filter into new
// constrained Scan operators using those indexes. Since this only needs to be
// done once per table, GenerateConstrainedScans should only be called on the
// original unaltered primary index Scan operator (i.e. not constrained or
// limited).
//
// For each secondary index that "covers" the columns needed by the scan, there
// are three cases:
//
//  - a filter that can be completely converted to a constraint over that index
//    generates a single constrained Scan operator (to be added to the same
//    group as the original Select operator):
//
//      (Scan $scanDef)
//
//  - a filter that can be partially converted to a constraint over that index
//    generates a constrained Scan operator in a new memo group, wrapped in a
//    Select operator having the remaining filter (to be added to the same group
//    as the original Select operator):
//
//      (Select (Scan $scanDef) $filter)
//
//  - a filter that cannot be converted to a constraint generates nothing
//
// And for a secondary index that does not cover the needed columns:
//
//  - a filter that can be completely converted to a constraint over that index
//    generates a single constrained Scan operator in a new memo group, wrapped
//    in an IndexJoin operator that looks up the remaining needed columns (and
//    is added to the same group as the original Select operator)
//
//      (IndexJoin (Scan $scanDef) $indexJoinDef)
//
//  - a filter that can be partially converted to a constraint over that index
//    generates a constrained Scan operator in a new memo group, wrapped in an
//    IndexJoin operator that looks up the remaining needed columns; the
//    remaining filter is distributed above and/or below the IndexJoin,
//    depending on which columns it references:
//
//      (IndexJoin
//        (Select (Scan $scanDef) $filter)
//        $indexJoinDef
//      )
//
//      (Select
//        (IndexJoin (Scan $scanDef) $indexJoinDef)
//        $filter
//      )
//
//      (Select
//        (IndexJoin
//          (Select (Scan $scanDef) $innerFilter)
//          $indexJoinDef
//        )
//        $outerFilter
//      )
//
func (c *CustomFuncs) GenerateConstrainedScans(
	scanDef memo.PrivateID, filter memo.GroupID,
) []memo.Expr {
	c.e.exprs = c.e.exprs[:0]
	scanOpDef := c.e.mem.LookupPrivate(scanDef).(*memo.ScanOpDef)

	var sb indexScanBuilder
	sb.init(c, scanOpDef.Table)

	// Iterate over all indexes.
	var iter scanIndexIter
	iter.init(c.e.mem, scanOpDef)
	for iter.next() {
		// Check whether the filter can constrain the index.
		constraint, remaining, ok := c.tryConstrainIndex(
			filter, scanOpDef.Table, iter.index, false /* isInverted */)
		if !ok {
			continue
		}

		// Construct new constrained ScanOpDef.
		newDef := *scanOpDef
		newDef.Index = iter.index
		newDef.Constraint = constraint

		// If the alternate index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if iter.isCovering() {
			sb.setScan(c.e.f.InternScanOpDef(&newDef))

			// If there is a remaining filter, then the constrained Scan operator
			// will be created in a new group, and a Select operator will be added
			// to the same group as the original operator.
			sb.addSelect(remaining)
			c.e.exprs = append(c.e.exprs, sb.build())
			continue
		}

		// Otherwise, construct an IndexJoin operator that provides the columns
		// missing from the index.
		if scanOpDef.Flags.NoIndexJoin {
			continue
		}

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newDef.Cols = iter.indexCols().Intersection(scanOpDef.Cols)
		newDef.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(c.e.f.InternScanOpDef(&newDef))

		// If remaining filter exists, split it into one part that can be pushed
		// below the IndexJoin, and one part that needs to stay above.
		remaining = sb.addSelectAfterSplit(remaining, newDef.Cols)
		sb.addIndexJoin(scanOpDef.Cols)
		sb.addSelect(remaining)

		c.e.exprs = append(c.e.exprs, sb.build())
	}

	return c.e.exprs
}

// HasInvertedIndexes returns true if at least one inverted index is defined on
// the Scan operator's table.
func (c *CustomFuncs) HasInvertedIndexes(def memo.PrivateID) bool {
	// Don't bother matching unless there's an inverted index.
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	var iter scanIndexIter
	iter.init(c.e.mem, scanOpDef)
	return iter.nextInverted()
}

// GenerateInvertedIndexScans enumerates all inverted indexes on the Scan
// operator's table and generates an alternate Scan operator for each inverted
// index that can service the query.
//
// The resulting Scan operator is pre-constrained and requires an IndexJoin to
// project columns other than the primary key columns. The reason it's pre-
// constrained is that we cannot treat an inverted index in the same way as a
// regular index, since it does not actually contain the indexed column.
func (c *CustomFuncs) GenerateInvertedIndexScans(
	def memo.PrivateID, filter memo.GroupID,
) []memo.Expr {
	c.e.exprs = c.e.exprs[:0]
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)

	var sb indexScanBuilder
	sb.init(c, scanOpDef.Table)

	// Iterate over all inverted indexes.
	var iter scanIndexIter
	iter.init(c.e.mem, scanOpDef)
	for iter.nextInverted() {
		// Check whether the filter can constrain the index.
		constraint, remaining, ok := c.tryConstrainIndex(
			filter, scanOpDef.Table, iter.index, true /* isInverted */)
		if !ok {
			continue
		}

		// Construct new ScanOpDef with the new index and constraint.
		newDef := *scanOpDef
		newDef.Index = iter.index
		newDef.Constraint = constraint

		// Though the index is marked as containing the JSONB column being
		// indexed, it doesn't actually, and it's only valid to extract the
		// primary key columns from it.
		newDef.Cols = sb.primaryKeyCols()

		// The Scan operator always goes in a new group, since it's always nested
		// underneath the IndexJoin. The IndexJoin may also go into its own group,
		// if there's a remaining filter above it.
		// TODO(justin): We might not need to do an index join in order to get the
		// correct columns, but it's difficult to tell at this point.
		sb.setScan(c.e.mem.InternScanOpDef(&newDef))

		// If remaining filter exists, split it into one part that can be pushed
		// below the IndexJoin, and one part that needs to stay above.
		remaining = sb.addSelectAfterSplit(remaining, newDef.Cols)
		sb.addIndexJoin(scanOpDef.Cols)
		sb.addSelect(remaining)

		c.e.exprs = append(c.e.exprs, sb.build())
	}

	return c.e.exprs
}

// tryConstrainIndex tries to derive a constraint for the given index from the
// specified filter. If a constraint is derived, it is returned along with any
// filter remaining after extracting the constraint. If no constraint can be
// derived, then tryConstrainIndex returns ok = false.
func (c *CustomFuncs) tryConstrainIndex(
	filter memo.GroupID, tabID opt.TableID, indexOrd int, isInverted bool,
) (constraint *constraint.Constraint, remainingFilter memo.GroupID, ok bool) {
	// Start with fast check to rule out indexes that cannot be constrained.
	if !isInverted && !c.canMaybeConstrainIndex(filter, tabID, indexOrd) {
		return nil, 0, false
	}

	// Fill out data structures needed to initialize the idxconstraint library.
	// Use LaxKeyColumnCount, since all columns <= LaxKeyColumnCount are
	// guaranteed to be part of each row's key (i.e. not stored in row's value,
	// which does not take part in an index scan). Note that the OrderingColumn
	// slice cannot be reused, as Instance.Init can use it in the constraint.
	md := c.e.mem.Metadata()
	index := md.Table(tabID).Index(indexOrd)
	columns := make([]opt.OrderingColumn, index.LaxKeyColumnCount())
	var notNullCols opt.ColSet
	for i := range columns {
		col := index.Column(i)
		colID := tabID.ColumnID(col.Ordinal)
		columns[i] = opt.MakeOrderingColumn(colID, col.Descending)
		if !col.Column.IsNullable() {
			notNullCols.Add(int(colID))
		}
	}

	// Generate index constraints.
	var ic idxconstraint.Instance
	ev := memo.MakeNormExprView(c.e.mem, filter)
	ic.Init(ev, columns, notNullCols, isInverted, c.e.evalCtx, c.e.f)
	constraint = ic.Constraint()
	if constraint.IsUnconstrained() {
		return nil, 0, false
	}

	// Return 0 if no remaining filter.
	remaining := ic.RemainingFilter()
	if c.e.mem.NormOp(remaining) == opt.TrueOp {
		remaining = 0
	}

	// Make copy of constraint so that idxconstraint instance is not referenced.
	copy := *constraint
	return &copy, remaining, true
}

// canMaybeConstrainIndex performs two checks that can quickly rule out the
// possibility that the given index can be constrained by the specified filter:
//
//   1. If the filter does not reference the first index column, then no
//      constraint can be generated.
//   2. If none of the filter's constraints start with the first index column,
//      then no constraint can be generated.
//
func (c *CustomFuncs) canMaybeConstrainIndex(
	filter memo.GroupID, tabID opt.TableID, indexOrd int,
) bool {
	md := c.e.mem.Metadata()
	index := md.Table(tabID).Index(indexOrd)

	// If the filter does not involve the first index column, we won't be able to
	// generate a constraint.
	firstIndexCol := tabID.ColumnID(index.Column(0).Ordinal)
	filterProps := c.LookupLogical(filter).Scalar
	if !filterProps.OuterCols.Contains(int(firstIndexCol)) {
		return false
	}

	// If the constraints are tight, check if there is a constraint that starts
	// with the first index column. If the constraints are not tight, it's
	// possible that index constraints can still be generated (that code currently
	// supports more expressions).
	if filterProps.TightConstraints {
		cset := filterProps.Constraints
		for i := 0; i < cset.Length(); i++ {
			firstCol := cset.Constraint(i).Columns.Get(0).ID()
			if firstCol == firstIndexCol {
				return true
			}
		}
		// None of the constraints start with firstIndexCol.
		return false
	}
	return true
}

// ----------------------------------------------------------------------
//
// Limit Rules
//   Custom match and replace functions used with limit.opt rules.
//
// ----------------------------------------------------------------------

// IsPositiveLimit is true if the given limit value is greater than zero.
func (c *CustomFuncs) IsPositiveLimit(limit memo.PrivateID) bool {
	limitVal := int64(*c.e.mem.LookupPrivate(limit).(*tree.DInt))
	return limitVal > 0
}

// LimitScanDef constructs a new ScanOpDef private value that is based on the
// given ScanOpDef. The new def's HardLimit is set to the given limit, which
// must be a constant int datum value. The other fields are inherited from the
// existing def.
func (c *CustomFuncs) LimitScanDef(def, limit, ordering memo.PrivateID) memo.PrivateID {
	// Determine the scan direction necessary to provide the required ordering.
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	required := c.e.mem.LookupPrivate(ordering).(*props.OrderingChoice)
	_, reverse := scanOpDef.CanProvideOrdering(c.e.mem.Metadata(), required)

	defCopy := *scanOpDef
	defCopy.HardLimit = memo.MakeScanLimit(int64(*c.e.mem.LookupPrivate(limit).(*tree.DInt)), reverse)
	return c.e.mem.InternScanOpDef(&defCopy)
}

// CanLimitConstrainedScan returns true if the given scan has already been
// constrained and can have a row count limit installed as well. This is only
// possible when the required ordering of the rows to be limited can be
// satisfied by the Scan operator.
//
// NOTE: Limiting unconstrained scans is done by the PushLimitIntoScan rule,
//       since that can require IndexJoin operators to be generated.
func (c *CustomFuncs) CanLimitConstrainedScan(def, ordering memo.PrivateID) bool {
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	if scanOpDef.HardLimit != 0 {
		// Don't push limit into scan if scan is already limited. This would
		// usually only happen when normalizations haven't run, as otherwise
		// redundant Limit operators would be discarded.
		return false
	}

	if scanOpDef.Constraint == nil {
		// This is not a constrained scan, so skip it. The PushLimitIntoScan rule
		// is responsible for limited unconstrained scans.
		return false
	}

	required := c.e.mem.LookupPrivate(ordering).(*props.OrderingChoice)
	ok, _ := scanOpDef.CanProvideOrdering(c.e.mem.Metadata(), required)
	return ok
}

// GenerateLimitedScans enumerates all secondary indexes on the Scan operator's
// table and tries to create new limited Scan operators from them. Since this
// only needs to be done once per table, GenerateLimitedScans should only be
// called on the original unaltered primary index Scan operator (i.e. not
// constrained or limited).
//
// For a secondary index that "covers" the columns needed by the scan, a single
// limited Scan operator is created. For a non-covering index, an IndexJoin is
// constructed to add missing columns to the limited Scan.
func (c *CustomFuncs) GenerateLimitedScans(def, limit, ordering memo.PrivateID) []memo.Expr {
	c.e.exprs = c.e.exprs[:0]
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	required := c.e.mem.LookupPrivate(ordering).(*props.OrderingChoice)
	limitVal := int64(*c.e.mem.LookupPrivate(limit).(*tree.DInt))

	var sb indexScanBuilder
	sb.init(c, scanOpDef.Table)

	// Iterate over all indexes, looking for those that can be limited.
	var iter scanIndexIter
	iter.init(c.e.mem, scanOpDef)
	for iter.next() {
		newDef := *scanOpDef
		newDef.Index = iter.index

		// If the alternate index does not conform to the ordering, then skip it.
		// If reverse=true, then the scan needs to be in reverse order to match
		// the required ordering.
		ok, reverse := newDef.CanProvideOrdering(c.e.mem.Metadata(), required)
		if !ok {
			continue
		}
		newDef.HardLimit = memo.MakeScanLimit(limitVal, reverse)

		// If the alternate index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if iter.isCovering() {
			sb.setScan(c.e.f.InternScanOpDef(&newDef))
			c.e.exprs = append(c.e.exprs, sb.build())
			continue
		}

		// Otherwise, try to construct an IndexJoin operator that provides the
		// columns missing from the index.
		if scanOpDef.Flags.NoIndexJoin {
			continue
		}

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newDef.Cols = iter.indexCols().Intersection(scanOpDef.Cols)
		newDef.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(c.e.f.InternScanOpDef(&newDef))

		// The Scan operator will go into its own group (because it projects a
		// different set of columns), and the IndexJoin operator will be added to
		// the same group as the original Limit operator.
		sb.addIndexJoin(scanOpDef.Cols)
		c.e.exprs = append(c.e.exprs, sb.build())
	}

	return c.e.exprs
}

// ----------------------------------------------------------------------
//
// Join Rules
//   Custom match and replace functions used with join.opt rules.
//
// ----------------------------------------------------------------------

// ConstructMergeJoins spawns MergeJoinOps, based on any interesting orderings.
func (c *CustomFuncs) ConstructMergeJoins(
	originalOp opt.Operator, left memo.GroupID, right memo.GroupID, on memo.GroupID,
) []memo.Expr {
	c.e.exprs = c.e.exprs[:0]
	leftProps := c.e.mem.GroupProperties(left).Relational
	rightProps := c.e.mem.GroupProperties(right).Relational

	onExpr := memo.MakeNormExprView(c.e.mem, on)

	leftEq, rightEq := harvestEqualityColumns(leftProps.OutputCols, rightProps.OutputCols, onExpr)
	n := len(leftEq)
	if n == 0 {
		return nil
	}

	// We generate MergeJoin expressions based on interesting orderings from the
	// left side. The CommuteJoin rule will ensure that we actually try both
	// sides.
	leftOrders := DeriveInterestingOrderings(memo.MakeNormExprView(c.e.mem, left)).Copy()
	leftOrders.RestrictToCols(opt.ColListToSet(leftEq))
	if len(leftOrders) == 0 {
		return nil
	}

	var colToEq util.FastIntMap
	for i := range leftEq {
		colToEq.Set(int(leftEq[i]), i)
		colToEq.Set(int(rightEq[i]), i)
	}

	for _, o := range leftOrders {
		if len(o) < n {
			// TODO(radu): we have a partial ordering on the equality columns. We
			// should augment it with the other columns (in arbitrary order) in the
			// hope that we can get the full ordering cheaply using a "streaming"
			// sort. This would not useful now since we don't support streaming sorts.
			continue
		}
		def := memo.MergeOnDef{JoinType: originalOp}
		def.LeftEq = make(opt.Ordering, n)
		def.RightEq = make(opt.Ordering, n)
		def.LeftOrdering.Columns = make([]props.OrderingColumnChoice, 0, n)
		def.RightOrdering.Columns = make([]props.OrderingColumnChoice, 0, n)
		for i := 0; i < n; i++ {
			eqIdx, _ := colToEq.Get(int(o[i].ID()))
			l, r, descending := leftEq[eqIdx], rightEq[eqIdx], o[i].Descending()
			def.LeftEq[i] = opt.MakeOrderingColumn(l, descending)
			def.RightEq[i] = opt.MakeOrderingColumn(r, descending)
			def.LeftOrdering.AppendCol(l, descending)
			def.RightOrdering.AppendCol(r, descending)
		}

		// Simplify the orderings with the corresponding FD sets.
		def.LeftOrdering.Simplify(&c.e.mem.GroupProperties(left).Relational.FuncDeps)
		def.RightOrdering.Simplify(&c.e.mem.GroupProperties(right).Relational.FuncDeps)

		// TODO(radu): simplify the ON condition (we can remove the equalities we
		// extracted).
		mergeOn := c.e.f.ConstructMergeOn(on, c.e.mem.InternMergeOnDef(&def))
		// Create a merge join expression in the same group.
		mergeJoin := memo.MakeMergeJoinExpr(left, right, mergeOn)
		c.e.exprs = append(c.e.exprs, memo.Expr(mergeJoin))
	}

	return c.e.exprs
}

// harvestEqualityColumns returns a list of pairs of columns (one from the left
// side, one from the right side) which are constrained to be equal.
func harvestEqualityColumns(
	leftCols, rightCols opt.ColSet, on memo.ExprView,
) (leftEq opt.ColList, rightEq opt.ColList) {
	if on.Operator() != opt.FiltersOp {
		return nil, nil
	}
	for i, n := 0, on.ChildCount(); i < n; i++ {
		e := on.Child(i)
		ok, left, right := isJoinEquality(leftCols, rightCols, e)
		if !ok {
			continue
		}
		// Don't allow any column to show up twice.
		// TODO(radu): need to figure out the right thing to do in cases
		// like: left.a = right.a AND left.a = right.b
		duplicate := false
		for i := range leftEq {
			if leftEq[i] == left || rightEq[i] == right {
				duplicate = true
				break
			}
		}
		if !duplicate {
			leftEq = append(leftEq, left)
			rightEq = append(rightEq, right)
		}
	}
	return leftEq, rightEq
}

func isJoinEquality(
	leftCols, rightCols opt.ColSet, expr memo.ExprView,
) (ok bool, left, right opt.ColumnID) {
	if expr.Operator() != opt.EqOp {
		return false, 0, 0
	}
	lhs, rhs := expr.Child(0), expr.Child(1)
	if lhs.Operator() != opt.VariableOp || rhs.Operator() != opt.VariableOp {
		return false, 0, 0
	}
	// Don't allow mixed types (see #22519).
	if !lhs.Logical().Scalar.Type.Equivalent(rhs.Logical().Scalar.Type) {
		return false, 0, 0
	}
	lhsCol := lhs.Private().(opt.ColumnID)
	rhsCol := rhs.Private().(opt.ColumnID)
	if leftCols.Contains(int(lhsCol)) && rightCols.Contains(int(rhsCol)) {
		return true, lhsCol, rhsCol
	}
	if leftCols.Contains(int(rhsCol)) && rightCols.Contains(int(lhsCol)) {
		return true, rhsCol, lhsCol
	}
	return false, 0, 0
}

// CanUseLookupJoin returns true if a join with the given join type can be
// converted to a lookup join. This is only possible when:
//  - the scan has no constraints, and
//  - the scan has no limit, and
//  - a prefix of the scan index columns have equality constraints.
func (c *CustomFuncs) CanUseLookupJoin(
	input memo.GroupID, scanDefID memo.PrivateID, on memo.GroupID,
) bool {
	md := c.e.mem.Metadata()
	scanDef := c.e.mem.LookupPrivate(scanDefID).(*memo.ScanOpDef)
	if scanDef.Constraint != nil || scanDef.HardLimit != 0 {
		return false
	}

	inputProps := c.e.mem.GroupProperties(input).Relational
	onExpr := memo.MakeNormExprView(c.e.mem, on)
	_, rightEq := harvestEqualityColumns(inputProps.OutputCols, scanDef.Cols, onExpr)

	// Check if the first column in the index has an equality constraint.
	idx := md.Table(scanDef.Table).Index(scanDef.Index)
	firstCol := scanDef.Table.ColumnID(idx.Column(0).Ordinal)
	for i := range rightEq {
		if rightEq[i] == firstCol {
			return true
		}
	}
	return false
}

// ConstructLookupJoin creates a lookup join expression.
func (c *CustomFuncs) ConstructLookupJoin(
	joinType opt.Operator, input memo.GroupID, scanDefID memo.PrivateID, on memo.GroupID,
) []memo.Expr {
	md := c.e.mem.Metadata()
	scanDef := c.e.mem.LookupPrivate(scanDefID).(*memo.ScanOpDef)
	inputProps := c.e.mem.GroupProperties(input).Relational
	onExpr := memo.MakeNormExprView(c.e.mem, on)
	leftEq, rightEq := harvestEqualityColumns(inputProps.OutputCols, scanDef.Cols, onExpr)
	n := len(leftEq)
	if n == 0 {
		return nil
	}

	idx := md.Table(scanDef.Table).Index(scanDef.Index)
	numIndexCols := idx.LaxKeyColumnCount()
	lookupJoinDef := memo.LookupJoinDef{
		JoinType:   joinType,
		Table:      scanDef.Table,
		Index:      scanDef.Index,
		LookupCols: scanDef.Cols,
	}
	for i := 0; i < numIndexCols; i++ {
		idxCol := scanDef.Table.ColumnID(idx.Column(i).Ordinal)
		found := -1
		for j := range rightEq {
			if rightEq[j] == idxCol {
				found = j
			}
		}
		if found == -1 {
			break
		}
		inputCol := leftEq[found]
		if lookupJoinDef.KeyCols == nil {
			lookupJoinDef.KeyCols = make(opt.ColList, 0, numIndexCols)
		}
		lookupJoinDef.KeyCols = append(lookupJoinDef.KeyCols, inputCol)
	}
	if lookupJoinDef.KeyCols == nil {
		// CanUseLookupJoin ensures this is not possible.
		panic("lookup join not possible")
	}

	// Create a lookup join expression in the same group.
	// TODO(radu): simplify the ON condition (we can remove the equalities on
	// KeyCols).
	lookupJoin := memo.MakeLookupJoinExpr(input, on, c.e.mem.InternLookupJoinDef(&lookupJoinDef))
	c.e.exprs = append(c.e.exprs[:0], memo.Expr(lookupJoin))
	return c.e.exprs
}

// HoistIndexJoinDef creates a LookupJoinDef that implements an index join which
// was hoisted above a join. See the PushJoinThroughIndexJoin rule for more
// information.
func (c *CustomFuncs) HoistIndexJoinDef(
	indexJoinDefID memo.PrivateID, newJoin memo.GroupID, joinType opt.Operator,
) memo.PrivateID {
	indexJoinDef := c.e.mem.LookupPrivate(indexJoinDefID).(*memo.IndexJoinDef)
	inputCols := c.e.mem.GroupProperties(newJoin).Relational.OutputCols
	md := c.e.mem.Metadata()

	pkIndex := md.Table(indexJoinDef.Table).Index(opt.PrimaryIndex)
	numPKCols := pkIndex.KeyColumnCount()

	// Create the lookup join expression.
	//
	// The key columns are the PK columns.
	//
	// The lookup columns are the lookup columns in the original index join,
	// without any columns that are already available in the lookup join's input
	// (i.e. newJoin).
	//
	// For example:
	//  CREATE TABLE abc (a PRIMARY KEY, b INT, c INT)
	//  CREATE TABLE xyz (x PRIMARY KEY, y INT, z INT, INDEX (y))
	//  SELECT * FROM abc JOIN xyz ON a=y
	//
	// We want to first join abc with the index on y (which provides columns y, x)
	// and then use a lookup join to retrieve column z. The newJoin will thus
	// produce columns a,b,c,x,y; the lookup columns are just z (the original
	// index join produced x,y,z).
	//
	// Note that the lookup join "sees" column IDs from the table on both "sides"
	// (in this example x,y on the left and z on the right) but there is no
	// overlap.
	//
	// We support inner and left join. For left join, the lookup join node must
	// pass-through NULL PKs that come from "outer" (null-extended) rows and
	// return a row of NULLs. Because we know that all other rows will have a
	// match in the primary index, we can achieve this by making the lookup join a
	// left join as well.
	lookupJoinDef := memo.LookupJoinDef{
		JoinType:   joinType,
		Table:      indexJoinDef.Table,
		Index:      opt.PrimaryIndex,
		KeyCols:    make(opt.ColList, numPKCols),
		LookupCols: indexJoinDef.Cols.Difference(inputCols),
	}
	for i := 0; i < numPKCols; i++ {
		lookupJoinDef.KeyCols[i] = indexJoinDef.Table.ColumnID(pkIndex.Column(i).Ordinal)
		if !inputCols.Contains(int(lookupJoinDef.KeyCols[i])) {
			panic("index join input doesn't have PK")
		}
	}
	return c.e.mem.InternLookupJoinDef(&lookupJoinDef)
}

// ----------------------------------------------------------------------
//
// GroupBy Rules
//   Custom match and replace functions used with groupby.opt rules.
//
// ----------------------------------------------------------------------

// MakeOrderingChoiceFromColumn constructs a new OrderingChoice with
// one element in the sequence: the columnID in the order defined by
// (MIN/MAX) operator. This function was originally created to be used
// with the Replace(Min|Max)WithLimit exploration rules.
func (c *CustomFuncs) MakeOrderingChoiceFromColumn(
	op opt.Operator, col memo.PrivateID,
) memo.PrivateID {
	oc := props.OrderingChoice{}
	switch op {
	case opt.MinOp:
		oc.AppendCol(c.ExtractColID(col), false /* descending */)
	case opt.MaxOp:
		oc.AppendCol(c.ExtractColID(col), true /* descending */)
	}
	return c.e.f.InternOrderingChoice(&oc)
}

// scanIndexIter is a helper struct that supports iteration over the indexes
// of a Scan operator table. For example:
//
//   var iter scanIndexIter
//   iter.init(mem, scanOpDef)
//   for iter.next() {
//     doSomething(iter.index)
//   }
//
type scanIndexIter struct {
	mem       *memo.Memo
	scanOpDef *memo.ScanOpDef
	tab       opt.Table
	index     int
	cols      opt.ColSet
}

func (it *scanIndexIter) init(mem *memo.Memo, scanOpDef *memo.ScanOpDef) {
	it.mem = mem
	it.scanOpDef = scanOpDef
	it.tab = mem.Metadata().Table(scanOpDef.Table)
	it.index = -1
}

// next advances iteration to the next index of the Scan operator's table. This
// is the primary index if it's the first time next is called, or a secondary
// index thereafter. Inverted index are skipped. If the ForceIndex flag is set,
// then all indexes except the forced index are skipped. When there are no more
// indexes to enumerate, next returns false. The current index is accessible via
// the iterator's "index" field.
func (it *scanIndexIter) next() bool {
	for {
		it.index++
		if it.index >= it.tab.IndexCount() {
			return false
		}

		if it.tab.Index(it.index).IsInverted() {
			continue
		}
		if it.scanOpDef.Flags.ForceIndex && it.scanOpDef.Flags.Index != it.index {
			// If we are forcing a specific index, ignore the others.
			continue
		}
		it.cols = opt.ColSet{}
		return true
	}
}

// nextInverted advances iteration to the next inverted index of the Scan
// operator's table. It returns false when there are no more inverted indexes to
// enumerate (or if there were none to begin with). The current index is
// accessible via the iterator's "index" field.
func (it *scanIndexIter) nextInverted() bool {
	for {
		it.index++
		if it.index >= it.tab.IndexCount() {
			return false
		}

		if !it.tab.Index(it.index).IsInverted() {
			continue
		}
		if it.scanOpDef.Flags.ForceIndex && it.scanOpDef.Flags.Index != it.index {
			// If we are forcing a specific index, ignore the others.
			continue
		}
		it.cols = opt.ColSet{}
		return true
	}
}

// indexCols returns the set of columns contained in the current index.
func (it *scanIndexIter) indexCols() opt.ColSet {
	if it.cols.Empty() {
		it.cols = it.mem.Metadata().IndexColumns(it.scanOpDef.Table, it.index)
	}
	return it.cols
}

// isCovering returns true if the current index contains all columns projected
// by the Scan operator.
func (it *scanIndexIter) isCovering() bool {
	return it.scanOpDef.Cols.SubsetOf(it.indexCols())
}
