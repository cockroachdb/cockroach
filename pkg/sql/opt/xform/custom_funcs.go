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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
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

// IsCanonicalScan returns true if the given ScanPrivate is an original
// unaltered primary index Scan operator (i.e. unconstrained and not limited).
func (c *CustomFuncs) IsCanonicalScan(scan *memo.ScanPrivate) bool {
	return scan.Index == cat.PrimaryIndex &&
		scan.Constraint == nil &&
		scan.HardLimit == 0
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
func (c *CustomFuncs) GenerateIndexScans(grp memo.RelExpr, scanPrivate *memo.ScanPrivate) {
	// Iterate over all secondary indexes.
	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.next() {
		// Skip primary index.
		if iter.indexOrdinal == cat.PrimaryIndex {
			continue
		}

		// If the secondary index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if iter.isCovering() {
			scan := memo.ScanExpr{ScanPrivate: *scanPrivate}
			scan.Index = iter.indexOrdinal
			c.e.mem.AddScanToGroup(&scan, grp)
			continue
		}

		// Otherwise, if the index must be forced, then construct an IndexJoin
		// operator that provides the columns missing from the index. Note that
		// if ForceIndex=true, scanIndexIter only returns the one index that is
		// being forced, so no need to check that here.
		if !scanPrivate.Flags.ForceIndex {
			continue
		}

		var sb indexScanBuilder
		sb.init(c, scanPrivate.Table)

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = iter.indexOrdinal
		newScanPrivate.Cols = iter.indexCols().Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(&newScanPrivate)

		sb.addIndexJoin(scanPrivate.Cols)
		sb.build(grp)
	}
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
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate, filters memo.FiltersExpr,
) {
	var sb indexScanBuilder
	sb.init(c, scanPrivate.Table)

	// Iterate over all indexes.
	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.next() {
		// Check whether the filter can constrain the index.
		constraint, remaining, ok := c.tryConstrainIndex(
			filters, scanPrivate.Table, iter.indexOrdinal, false /* isInverted */)
		if !ok {
			continue
		}

		// Construct new constrained ScanPrivate.
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = iter.indexOrdinal
		newScanPrivate.Constraint = constraint

		// If the alternate index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if iter.isCovering() {
			sb.setScan(&newScanPrivate)

			// If there are remaining filters, then the constrained Scan operator
			// will be created in a new group, and a Select operator will be added
			// to the same group as the original operator.
			sb.addSelect(remaining)
			sb.build(grp)
			continue
		}

		// Otherwise, construct an IndexJoin operator that provides the columns
		// missing from the index.
		if scanPrivate.Flags.NoIndexJoin {
			continue
		}

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newScanPrivate.Cols = iter.indexCols().Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(&newScanPrivate)

		// If remaining filter exists, split it into one part that can be pushed
		// below the IndexJoin, and one part that needs to stay above.
		remaining = sb.addSelectAfterSplit(remaining, newScanPrivate.Cols)
		sb.addIndexJoin(scanPrivate.Cols)
		sb.addSelect(remaining)

		sb.build(grp)
	}
}

// HasInvertedIndexes returns true if at least one inverted index is defined on
// the Scan operator's table.
func (c *CustomFuncs) HasInvertedIndexes(scanPrivate *memo.ScanPrivate) bool {
	// Don't bother matching unless there's an inverted index.
	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
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
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate, filters memo.FiltersExpr,
) {
	var sb indexScanBuilder
	sb.init(c, scanPrivate.Table)

	// Iterate over all inverted indexes.
	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.nextInverted() {
		// Check whether the filter can constrain the index.
		constraint, remaining, ok := c.tryConstrainIndex(
			filters, scanPrivate.Table, iter.indexOrdinal, true /* isInverted */)
		if !ok {
			continue
		}

		// Construct new ScanOpDef with the new index and constraint.
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = iter.indexOrdinal
		newScanPrivate.Constraint = constraint

		// Though the index is marked as containing the JSONB column being
		// indexed, it doesn't actually, and it's only valid to extract the
		// primary key columns from it.
		newScanPrivate.Cols = sb.primaryKeyCols()

		// The Scan operator always goes in a new group, since it's always nested
		// underneath the IndexJoin. The IndexJoin may also go into its own group,
		// if there's a remaining filter above it.
		// TODO(justin): We might not need to do an index join in order to get the
		// correct columns, but it's difficult to tell at this point.
		sb.setScan(&newScanPrivate)

		// If remaining filter exists, split it into one part that can be pushed
		// below the IndexJoin, and one part that needs to stay above.
		remaining = sb.addSelectAfterSplit(remaining, newScanPrivate.Cols)
		sb.addIndexJoin(scanPrivate.Cols)
		sb.addSelect(remaining)

		sb.build(grp)
	}
}

func (c *CustomFuncs) initIdxConstraintForIndex(
	filters memo.FiltersExpr, tabID opt.TableID, indexOrd int, isInverted bool,
) (ic *idxconstraint.Instance) {
	ic = &idxconstraint.Instance{}

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
	ic.Init(filters, columns, notNullCols, isInverted, c.e.evalCtx, c.e.f)
	return ic
}

// tryConstrainIndex tries to derive a constraint for the given index from the
// specified filter. If a constraint is derived, it is returned along with any
// filter remaining after extracting the constraint. If no constraint can be
// derived, then tryConstrainIndex returns ok = false.
func (c *CustomFuncs) tryConstrainIndex(
	filters memo.FiltersExpr, tabID opt.TableID, indexOrd int, isInverted bool,
) (constraint *constraint.Constraint, remainingFilters memo.FiltersExpr, ok bool) {
	// Start with fast check to rule out indexes that cannot be constrained.
	if !isInverted && !c.canMaybeConstrainIndex(filters, tabID, indexOrd) {
		return nil, nil, false
	}

	ic := c.initIdxConstraintForIndex(filters, tabID, indexOrd, isInverted)
	constraint = ic.Constraint()
	if constraint.IsUnconstrained() {
		return nil, nil, false
	}

	// Return 0 if no remaining filter.
	remaining := ic.RemainingFilters()

	// Make copy of constraint so that idxconstraint instance is not referenced.
	copy := *constraint
	return &copy, remaining, true
}

// allInvIndexConstraints tries to derive all constraints for the specified inverted
// index that can be derived. If no constraint is derived, then it returns ok = false,
// similar to tryConstrainIndex.
func (c *CustomFuncs) allInvIndexConstraints(
	filters memo.FiltersExpr, tabID opt.TableID, indexOrd int,
) (constraints []*constraint.Constraint, ok bool) {
	ic := c.initIdxConstraintForIndex(filters, tabID, indexOrd, true /* isInverted */)
	constraints, err := ic.AllInvertedIndexConstraints()
	if err != nil {
		return nil, false
	}
	// As long as there was no error, AllInvertedIndexConstraints is guaranteed
	// to add at least one constraint to the slice. It will be set to
	// unconstrained if no constraints could be derived for this index.
	constraint := constraints[0]
	if constraint.IsUnconstrained() {
		return constraints, false
	}

	return constraints, true
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
	filters memo.FiltersExpr, tabID opt.TableID, indexOrd int,
) bool {
	md := c.e.mem.Metadata()
	index := md.Table(tabID).Index(indexOrd)

	for i := range filters {
		filterProps := filters[i].ScalarProps(c.e.mem)

		// If the filter involves the first index column, then the index can
		// possibly be constrained.
		firstIndexCol := tabID.ColumnID(index.Column(0).Ordinal)
		if filterProps.OuterCols.Contains(int(firstIndexCol)) {
			return true
		}

		// If the constraints are not tight, then the index can possibly be
		// constrained, because index constraint generation supports more
		// expressions than filter constraint generation.
		if !filterProps.TightConstraints {
			return true
		}

		// If any constraint involves the first index column, then the index can
		// possibly be constrained.
		cset := filterProps.Constraints
		for i := 0; i < cset.Length(); i++ {
			firstCol := cset.Constraint(i).Columns.Get(0).ID()
			if firstCol == firstIndexCol {
				return true
			}
		}
	}

	return false
}

// ----------------------------------------------------------------------
//
// Limit Rules
//   Custom match and replace functions used with limit.opt rules.
//
// ----------------------------------------------------------------------

// IsPositiveLimit is true if the given limit value is greater than zero.
func (c *CustomFuncs) IsPositiveLimit(limit tree.Datum) bool {
	limitVal := int64(*limit.(*tree.DInt))
	return limitVal > 0
}

// LimitScanPrivate constructs a new ScanPrivate value that is based on the
// given ScanPrivate. The new private's HardLimit is set to the given limit,
// which must be a constant int datum value. The other fields are inherited from
// the existing private.
func (c *CustomFuncs) LimitScanPrivate(
	scanPrivate *memo.ScanPrivate, limit tree.Datum, required physical.OrderingChoice,
) *memo.ScanPrivate {
	// Determine the scan direction necessary to provide the required ordering.
	_, reverse := ordering.ScanPrivateCanProvide(c.e.mem.Metadata(), scanPrivate, &required)

	newScanPrivate := *scanPrivate
	newScanPrivate.HardLimit = memo.MakeScanLimit(int64(*limit.(*tree.DInt)), reverse)
	return &newScanPrivate
}

// CanLimitConstrainedScan returns true if the given scan has already been
// constrained and can have a row count limit installed as well. This is only
// possible when the required ordering of the rows to be limited can be
// satisfied by the Scan operator.
//
// NOTE: Limiting unconstrained scans is done by the PushLimitIntoScan rule,
//       since that can require IndexJoin operators to be generated.
func (c *CustomFuncs) CanLimitConstrainedScan(
	scanPrivate *memo.ScanPrivate, required physical.OrderingChoice,
) bool {
	if scanPrivate.HardLimit != 0 {
		// Don't push limit into scan if scan is already limited. This would
		// usually only happen when normalizations haven't run, as otherwise
		// redundant Limit operators would be discarded.
		return false
	}

	if scanPrivate.Constraint == nil {
		// This is not a constrained scan, so skip it. The PushLimitIntoScan rule
		// is responsible for limited unconstrained scans.
		return false
	}

	ok, _ := ordering.ScanPrivateCanProvide(c.e.mem.Metadata(), scanPrivate, &required)
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
func (c *CustomFuncs) GenerateLimitedScans(
	grp memo.RelExpr,
	scanPrivate *memo.ScanPrivate,
	limit tree.Datum,
	required physical.OrderingChoice,
) {
	limitVal := int64(*limit.(*tree.DInt))

	var sb indexScanBuilder
	sb.init(c, scanPrivate.Table)

	// Iterate over all indexes, looking for those that can be limited.
	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.next() {
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = iter.indexOrdinal

		// If the alternate index does not conform to the ordering, then skip it.
		// If reverse=true, then the scan needs to be in reverse order to match
		// the required ordering.
		ok, reverse := ordering.ScanPrivateCanProvide(
			c.e.mem.Metadata(), &newScanPrivate, &required,
		)
		if !ok {
			continue
		}
		newScanPrivate.HardLimit = memo.MakeScanLimit(limitVal, reverse)

		// If the alternate index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if iter.isCovering() {
			sb.setScan(&newScanPrivate)
			sb.build(grp)
			continue
		}

		// Otherwise, try to construct an IndexJoin operator that provides the
		// columns missing from the index.
		if scanPrivate.Flags.NoIndexJoin {
			continue
		}

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newScanPrivate.Cols = iter.indexCols().Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(&newScanPrivate)

		// The Scan operator will go into its own group (because it projects a
		// different set of columns), and the IndexJoin operator will be added to
		// the same group as the original Limit operator.
		sb.addIndexJoin(scanPrivate.Cols)

		sb.build(grp)
	}
}

// ----------------------------------------------------------------------
//
// Join Rules
//   Custom match and replace functions used with join.opt rules.
//
// ----------------------------------------------------------------------

// GenerateMergeJoins spawns MergeJoinOps, based on any interesting orderings.
func (c *CustomFuncs) GenerateMergeJoins(
	grp memo.RelExpr, originalOp opt.Operator, left, right memo.RelExpr, on memo.FiltersExpr,
) {
	leftProps := left.Relational()
	rightProps := right.Relational()

	leftEq, rightEq := memo.ExtractJoinEqualityColumns(
		leftProps.OutputCols, rightProps.OutputCols, on,
	)
	n := len(leftEq)
	if n == 0 {
		return
	}

	// We generate MergeJoin expressions based on interesting orderings from the
	// left side. The CommuteJoin rule will ensure that we actually try both
	// sides.
	leftOrders := DeriveInterestingOrderings(left).Copy()
	leftOrders.RestrictToCols(leftEq.ToSet())
	if len(leftOrders) == 0 {
		return
	}

	var colToEq util.FastIntMap
	for i := range leftEq {
		colToEq.Set(int(leftEq[i]), i)
		colToEq.Set(int(rightEq[i]), i)
	}

	var remainingFilters memo.FiltersExpr

	for _, o := range leftOrders {
		if len(o) < n {
			// TODO(radu): we have a partial ordering on the equality columns. We
			// should augment it with the other columns (in arbitrary order) in the
			// hope that we can get the full ordering cheaply using a "streaming"
			// sort. This would not useful now since we don't support streaming sorts.
			continue
		}

		if remainingFilters == nil {
			remainingFilters = memo.ExtractRemainingJoinFilters(on, leftEq, rightEq)
		}

		merge := memo.MergeJoinExpr{Left: left, Right: right, On: remainingFilters}
		merge.JoinType = originalOp
		merge.LeftEq = make(opt.Ordering, n)
		merge.RightEq = make(opt.Ordering, n)
		merge.LeftOrdering.Columns = make([]physical.OrderingColumnChoice, 0, n)
		merge.RightOrdering.Columns = make([]physical.OrderingColumnChoice, 0, n)
		for i := 0; i < n; i++ {
			eqIdx, _ := colToEq.Get(int(o[i].ID()))
			l, r, descending := leftEq[eqIdx], rightEq[eqIdx], o[i].Descending()
			merge.LeftEq[i] = opt.MakeOrderingColumn(l, descending)
			merge.RightEq[i] = opt.MakeOrderingColumn(r, descending)
			merge.LeftOrdering.AppendCol(l, descending)
			merge.RightOrdering.AppendCol(r, descending)
		}

		// Simplify the orderings with the corresponding FD sets.
		merge.LeftOrdering.Simplify(&leftProps.FuncDeps)
		merge.RightOrdering.Simplify(&rightProps.FuncDeps)

		c.e.mem.AddMergeJoinToGroup(&merge, grp)
	}
}

// GenerateLookupJoins looks at the possible indexes and creates lookup join
// expressions in the current group. A lookup join can be created when the ON
// condition has equality constraints on a prefix of the index columns.
//
// There are two cases:
//
//  1. The index has all the columns we need; this is the simple case, where we
//     generate a LookupJoin expression in the current group:
//
//         Join                       LookupJoin(t@idx))
//         /   \                           |
//        /     \            ->            |
//      Input  Scan(t)                   Input
//
//
//  2. The index is not covering. We have to generate an index join above the
//     lookup join. Note that this index join is also implemented as a
//     LookupJoin, because an IndexJoin can only output columns from one table,
//     whereas we also need to output columns from Input.
//
//         Join                       LookupJoin(t@primary)
//         /   \                           |
//        /     \            ->            |
//      Input  Scan(t)                LookupJoin(t@idx)
//                                         |
//                                         |
//                                       Input
//
//     For example:
//      CREATE TABLE abc (a PRIMARY KEY, b INT, c INT)
//      CREATE TABLE xyz (x PRIMARY KEY, y INT, z INT, INDEX (y))
//      SELECT * FROM abc JOIN xyz ON a=y
//
//     We want to first join abc with the index on y (which provides columns y, x)
//     and then use a lookup join to retrieve column z. The "index join" (top
//     LookupJoin) will produce columns a,b,c,x,y; the lookup columns are just z
//     (the original index join produced x,y,z).
//
//     Note that the top LookupJoin "sees" column IDs from the table on both
//     "sides" (in this example x,y on the left and z on the right) but there is
//     no overlap.
//
func (c *CustomFuncs) GenerateLookupJoins(
	grp memo.RelExpr,
	joinType opt.Operator,
	input memo.RelExpr,
	scanPrivate *memo.ScanPrivate,
	on memo.FiltersExpr,
) {
	inputProps := input.Relational()

	leftEq, rightEq := memo.ExtractJoinEqualityColumns(inputProps.OutputCols, scanPrivate.Cols, on)
	n := len(leftEq)
	if n == 0 {
		return
	}

	var pkCols opt.ColList

	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.next() {
		// Check if the first column in the index has an equality constraint.
		firstIdxCol := scanPrivate.Table.ColumnID(iter.index.Column(0).Ordinal)
		if _, ok := rightEq.Find(firstIdxCol); !ok {
			continue
		}

		lookupJoin := memo.LookupJoinExpr{Input: input, On: on}
		lookupJoin.JoinType = joinType
		lookupJoin.Table = scanPrivate.Table
		lookupJoin.Index = iter.indexOrdinal

		// Find the longest prefix of index key columns that are equality columns.
		numIndexKeyCols := iter.index.LaxKeyColumnCount()
		lookupJoin.KeyCols = make(opt.ColList, 0, numIndexKeyCols)
		rightSideCols := make(opt.ColList, 0, numIndexKeyCols)
		for j := 0; j < numIndexKeyCols; j++ {
			idxCol := scanPrivate.Table.ColumnID(iter.index.Column(j).Ordinal)
			eqIdx, ok := rightEq.Find(idxCol)
			if !ok {
				break
			}
			lookupJoin.KeyCols = append(lookupJoin.KeyCols, leftEq[eqIdx])
			rightSideCols = append(rightSideCols, idxCol)
		}

		lookupJoin.On = memo.ExtractRemainingJoinFilters(on, lookupJoin.KeyCols, rightSideCols)

		if iter.isCovering() {
			// Case 1 (see function comment).
			lookupJoin.Cols = scanPrivate.Cols.Union(inputProps.OutputCols)
			c.e.mem.AddLookupJoinToGroup(&lookupJoin, grp)
			continue
		}

		// Case 2 (see function comment).
		if scanPrivate.Flags.NoIndexJoin {
			continue
		}

		if pkCols == nil {
			pkIndex := iter.tab.Index(cat.PrimaryIndex)
			pkCols = make(opt.ColList, pkIndex.KeyColumnCount())
			for i := range pkCols {
				pkCols[i] = scanPrivate.Table.ColumnID(pkIndex.Column(i).Ordinal)
			}
		}

		// The lower LookupJoin must return all PK columns (they are needed as key
		// columns for the index join).
		indexCols := iter.indexCols()
		lookupJoin.Cols = scanPrivate.Cols.Intersection(indexCols)
		for i := range pkCols {
			lookupJoin.Cols.Add(int(pkCols[i]))
		}
		lookupJoin.Cols.UnionWith(inputProps.OutputCols)

		var indexJoin memo.LookupJoinExpr

		// onCols are the columns that the ON condition in the (lower) lookup join
		// can refer to: input columns, or columns available in the index.
		onCols := indexCols.Union(inputProps.OutputCols)
		if c.FiltersBoundBy(lookupJoin.On, onCols) {
			// The ON condition refers only to the columns available in the index.
			//
			// For LeftJoin, both LookupJoins perform a LeftJoin. A null-extended row
			// from the lower LookupJoin will not have any matches in the top
			// LookupJoin (it has NULLs on key columns) and will get null-extended
			// there as well.
			indexJoin.On = memo.TrueFilter
		} else {
			// ON has some conditions that are bound by the columns in the index (at
			// the very least, the equality conditions we used for KeyCols), and some
			// conditions that refer to other columns. We can put the former in the
			// lower LookupJoin and the latter in the index join.
			//
			// This works for InnerJoin but not for LeftJoin because of a
			// technicality: if an input (left) row has matches in the lower
			// LookupJoin but has no matches in the index join, only the columns
			// looked up by the top index join get NULL-extended.
			if joinType == opt.LeftJoinOp {
				// TODO(radu): support LeftJoin, perhaps by looking up all columns and
				// discarding columns that are already available from the lower
				// LookupJoin. This requires a projection to avoid having the same
				// ColumnIDs on both sides of the index join.
				continue
			}
			conditions := lookupJoin.On
			lookupJoin.On = c.ExtractBoundConditions(conditions, onCols)
			indexJoin.On = c.ExtractUnboundConditions(conditions, onCols)
		}

		indexJoin.Input = c.e.f.ConstructLookupJoin(
			lookupJoin.Input,
			lookupJoin.On,
			&lookupJoin.LookupJoinPrivate,
		)
		indexJoin.JoinType = joinType
		indexJoin.Table = scanPrivate.Table
		indexJoin.Index = cat.PrimaryIndex
		indexJoin.KeyCols = pkCols
		indexJoin.Cols = scanPrivate.Cols.Union(inputProps.OutputCols)

		// Create the LookupJoin for the index join in the same group.
		c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
	}
}

// eqColsForZigzag is a helper function to generate eqCol lists for the zigzag
// joiner. The zigzag joiner requires that the equality columns immediately
// follow the fixed columns in the index. Fixed here refers to columns that
// have been constrained to a constant value.
//
// There are two kinds of equality columns that this function takes care of:
// columns that have the same ColumnID on both sides (i.e. the same column),
// as well as columns that have been equated in some ON filter (i.e. they are
// contained in leftEqCols and rightEqCols at the same index).
//
// This function iterates through all columns of the indexes in order,
// skips past the fixed columns, and then generates however many eqCols
// there are that meet the above criteria.
//
// Returns a list of column ordinals for each index.
//
// See the comment in pkg/sql/distsqlrun/zigzag_joiner.go for more details
// on the role eqCols and fixed cols play in zigzag joins.
func eqColsForZigzag(
	tab cat.Table,
	tabID opt.TableID,
	leftIndex cat.Index,
	rightIndex cat.Index,
	fixedCols opt.ColSet,
	leftEqCols opt.ColList,
	rightEqCols opt.ColList,
) (leftEqPrefix, rightEqPrefix opt.ColList) {
	leftEqPrefix = make(opt.ColList, 0, len(leftEqCols))
	rightEqPrefix = make(opt.ColList, 0, len(rightEqCols))
	i, leftCnt := 0, leftIndex.ColumnCount()
	j, rightCnt := 0, rightIndex.ColumnCount()
	for ; i < leftCnt; i++ {
		colID := tabID.ColumnID(leftIndex.Column(i).Ordinal)
		if !fixedCols.Contains(int(colID)) {
			break
		}
	}
	for ; j < rightCnt; j++ {
		colID := tabID.ColumnID(rightIndex.Column(j).Ordinal)
		if !fixedCols.Contains(int(colID)) {
			break
		}
	}

	for i < leftCnt && j < rightCnt {
		leftColID := tabID.ColumnID(leftIndex.Column(i).Ordinal)
		rightColID := tabID.ColumnID(rightIndex.Column(j).Ordinal)
		i++
		j++

		if leftColID == rightColID {
			leftEqPrefix = append(leftEqPrefix, leftColID)
			rightEqPrefix = append(rightEqPrefix, rightColID)
			continue
		}
		leftIdx, leftOk := leftEqCols.Find(leftColID)
		rightIdx, rightOk := rightEqCols.Find(rightColID)
		// If both columns are at the same index in their respective
		// EqCols lists, they were equated in the filters.
		if leftOk && rightOk && leftIdx == rightIdx {
			leftEqPrefix = append(leftEqPrefix, leftColID)
			rightEqPrefix = append(rightEqPrefix, rightColID)
			continue
		} else {
			// We've reached the first non-equal column; the zigzag
			// joiner does not support non-contiguous/non-prefix equal
			// columns.
			break
		}

	}

	return leftEqPrefix, rightEqPrefix
}

// GenerateZigzagJoins generates zigzag joins for all pairs of indexes of the
// Scan table which contain one of the constant columns in the FiltersExpr as
// its prefix.
//
// Similar to the lookup join, if the selected index pair does not contain
// all the columns in the output of the scan, we wrap the zigzag join
// in another index join (implemented as a lookup join) on the primary index.
// The index join is implemented with a lookup join since the index join does
// not support arbitrary input sources that are not plain index scans.
func (c *CustomFuncs) GenerateZigzagJoins(
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate, filters memo.FiltersExpr,
) {

	// Short circuit unless zigzag joins are explicitly enabled.
	if !c.e.evalCtx.SessionData.ZigzagJoinEnabled {
		return
	}

	fixedCols := memo.ExtractConstColumns(filters, c.e.mem, c.e.evalCtx)

	if fixedCols.Len() == 0 {
		// Zigzagging isn't helpful in the absence of fixed columns.
		return
	}

	// Iterate through indexes, looking for those prefixed with fixedEq cols.
	// Efficiently finding a set of indexes that make the most efficient zigzag
	// join, with no limit on the number of indexes selected, is an instance of
	// this NP-hard problem:
	// https://en.wikipedia.org/wiki/Maximum_coverage_problem
	//
	// A formal definition would be: Suppose we have a set of fixed columns F
	// (defined as fixedCols in the code above), and a set of indexes I. The
	// "fixed prefix" of every index, in this context, refers to the longest
	// prefix of each index's columns that is in F. In other words, we stop
	// adding to the prefix when we come across the first non-fixed column
	// in an index.
	//
	// We want to find at most k = 2 indexes from I (in the future k could be
	// >= 2 when the zigzag joiner supports 2+ index zigzag joins) that cover
	// the maximum number of columns in F. An index is defined to have covered
	// a column if that column is in the index's fixed prefix.
	//
	// Since only 2-way zigzag joins are currently supported, the naive
	// approach is bounded at n^2. For now, just do that - a quadratic
	// iteration through all indexes.
	//
	// TODO(itsbilal): Implement the greedy or weighted version of the
	// algorithm laid out here:
	// https://en.wikipedia.org/wiki/Maximum_coverage_problem
	var iter, iter2 scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.next() {
		if iter.indexOrdinal == cat.PrimaryIndex {
			continue
		}
		// Short-circuit quickly if the first column in the index is not a fixed
		// column.
		if !fixedCols.Contains(int(scanPrivate.Table.ColumnID(iter.index.Column(0).Ordinal))) {
			continue
		}

		iter2.init(c.e.mem, scanPrivate)
		// Only look at indexes after this one.
		iter2.indexOrdinal = iter.indexOrdinal

		for iter2.next() {
			if !fixedCols.Contains(int(scanPrivate.Table.ColumnID(iter2.index.Column(0).Ordinal))) {
				continue
			}
			// Columns that are in both indexes are, by definition, equal.
			leftCols := iter.indexCols()
			rightCols := iter2.indexCols()
			eqCols := leftCols.Intersection(rightCols)
			eqCols.DifferenceWith(fixedCols)
			if eqCols.Len() == 0 {
				// A simple index join is more efficient in such cases.
				continue
			}

			// If there are any equalities across the columns of the two indexes,
			// push them into the zigzag join spec.
			leftEq, rightEq := memo.ExtractJoinEqualityColumns(
				leftCols, rightCols, filters,
			)
			leftEqCols, rightEqCols := eqColsForZigzag(
				iter.tab,
				scanPrivate.Table,
				iter.index,
				iter2.index,
				fixedCols,
				leftEq,
				rightEq,
			)

			if len(leftEqCols) == 0 || len(rightEqCols) == 0 {
				// One of the indexes is not sorted by any of the equality
				// columns, because the equality columns do not immediately
				// succeed the fixed columns. A zigzag join cannot be planned.
				continue
			}

			// Confirm the primary key columns are in both leftEqCols and
			// rightEqCols. The conversion of a select with filters to a
			// zigzag join requires the primary key columns to be in the output
			// for output correctness; otherwise, we could be outputting more
			// results than there should be (due to an equality on a non-unique
			// non-required value).
			pkIndex := iter.tab.Index(cat.PrimaryIndex)
			pkCols := make(opt.ColList, pkIndex.KeyColumnCount())
			pkColsFound := true
			for i := range pkCols {
				pkCols[i] = scanPrivate.Table.ColumnID(pkIndex.Column(i).Ordinal)

				if _, ok := leftEqCols.Find(pkCols[i]); !ok {
					pkColsFound = false
					break
				}
				if _, ok := rightEqCols.Find(pkCols[i]); !ok {
					pkColsFound = false
					break
				}
			}
			if !pkColsFound {
				continue
			}

			zigzagJoin := memo.ZigzagJoinExpr{
				On: filters,
				ZigzagJoinPrivate: memo.ZigzagJoinPrivate{
					LeftTable:   scanPrivate.Table,
					LeftIndex:   iter.indexOrdinal,
					RightTable:  scanPrivate.Table,
					RightIndex:  iter2.indexOrdinal,
					LeftEqCols:  leftEqCols,
					RightEqCols: rightEqCols,
				},
			}

			// Fixed values are represented as tuples consisting of the
			// fixed segment of that side's index.
			zigzagJoin.FixedVals = make(memo.ScalarListExpr, 2)
			leftVals := make(memo.ScalarListExpr, 0, fixedCols.Len())
			leftTypes := make([]types.T, 0, fixedCols.Len())
			rightVals := make(memo.ScalarListExpr, 0, fixedCols.Len())
			rightTypes := make([]types.T, 0, fixedCols.Len())

			zigzagJoin.LeftFixedCols = make(opt.ColList, 0, fixedCols.Len())
			zigzagJoin.RightFixedCols = make(opt.ColList, 0, fixedCols.Len())
			fixedValMap := memo.ExtractValuesFromFilter(filters, fixedCols)

			for i, cnt := 0, iter.index.ColumnCount(); i < cnt; i++ {
				colID := scanPrivate.Table.ColumnID(iter.index.Column(i).Ordinal)
				val, ok := fixedValMap[colID]
				if !ok {
					break
				}
				leftVals = append(leftVals, c.e.f.ConstructConstVal(val))
				leftTypes = append(leftTypes, iter.index.Column(i).Column.DatumType())
				zigzagJoin.LeftFixedCols = append(zigzagJoin.LeftFixedCols, colID)
			}
			for i, cnt := 0, iter2.index.ColumnCount(); i < cnt; i++ {
				colID := scanPrivate.Table.ColumnID(iter2.index.Column(i).Ordinal)
				val, ok := fixedValMap[colID]
				if !ok {
					break
				}
				rightVals = append(rightVals, c.e.f.ConstructConstVal(val))
				rightTypes = append(rightTypes, iter2.index.Column(i).Column.DatumType())
				zigzagJoin.RightFixedCols = append(zigzagJoin.RightFixedCols, colID)
			}
			zigzagJoin.FixedVals[0] = c.e.f.ConstructTuple(leftVals, types.TTuple{Types: leftTypes})
			zigzagJoin.FixedVals[1] = c.e.f.ConstructTuple(rightVals, types.TTuple{Types: rightTypes})

			zigzagJoin.On = memo.ExtractRemainingJoinFilters(
				filters,
				zigzagJoin.LeftEqCols,
				zigzagJoin.RightEqCols,
			)
			zigzagCols := leftCols.Copy()
			zigzagCols.UnionWith(rightCols)

			if scanPrivate.Cols.SubsetOf(zigzagCols) {
				// Case 1 (zigzagged indexes contain all requested columns).
				zigzagJoin.Cols = scanPrivate.Cols
				c.e.mem.AddZigzagJoinToGroup(&zigzagJoin, grp)
				continue
			}

			if scanPrivate.Flags.NoIndexJoin {
				continue
			}

			// Case 2 (wrap zigzag join in an index join).
			var indexJoin memo.LookupJoinExpr
			// Ensure the zigzag join returns pk columns.
			zigzagJoin.Cols = scanPrivate.Cols.Intersection(zigzagCols)
			for i := range pkCols {
				zigzagJoin.Cols.Add(int(pkCols[i]))
			}

			if c.FiltersBoundBy(zigzagJoin.On, zigzagCols) {
				// The ON condition refers only to the columns available in the zigzag
				// indices.
				indexJoin.On = memo.TrueFilter
			} else {
				// ON has some conditions that are bound by the columns in the index (at
				// the very least, the equality conditions we used for EqCols and FixedCols),
				// and some conditions that refer to other table columns. We can put
				// the former in the lower ZigzagJoin and the latter in the index join.
				conditions := zigzagJoin.On
				zigzagJoin.On = c.ExtractBoundConditions(conditions, zigzagCols)
				indexJoin.On = c.ExtractUnboundConditions(conditions, zigzagCols)
			}

			indexJoin.Input = c.e.f.ConstructZigzagJoin(
				zigzagJoin.On,
				&zigzagJoin.ZigzagJoinPrivate,
			)
			indexJoin.JoinType = opt.InnerJoinOp
			indexJoin.Table = scanPrivate.Table
			indexJoin.Index = cat.PrimaryIndex
			indexJoin.KeyCols = pkCols
			indexJoin.Cols = scanPrivate.Cols

			// Create the LookupJoin for the index join in the same group as the
			// original select.
			c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
		}
	}
}

// GenerateInvertedIndexZigzagJoins generates zigzag joins for constraints on
// inverted index. It looks for cases where one inverted index can satisfy
// two constraints, and it produces zigzag joins with the same index on both
// sides of the zigzag join for those cases, fixed on different constant values.
func (c *CustomFuncs) GenerateInvertedIndexZigzagJoins(
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate, filters memo.FiltersExpr,
) {
	// Short circuit unless zigzag joins are explicitly enabled.
	if !c.e.evalCtx.SessionData.ZigzagJoinEnabled {
		return
	}

	var sb indexScanBuilder
	sb.init(c, scanPrivate.Table)

	// Iterate over all inverted indexes.
	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.nextInverted() {
		// See if there are two or more constraints that can be satisfied
		// by this inverted index. This is possible with inverted indexes as
		// opposed to secondary indexes, because one row in the primary index
		// can often correspond to multiple rows in an inverted index. This
		// function generates all constraints it can derive for this index;
		// not all of which might get used in this function.
		constraints, ok := c.allInvIndexConstraints(
			filters, scanPrivate.Table, iter.indexOrdinal,
		)
		if !ok || len(constraints) < 2 {
			continue
		}
		// In theory, we could explore zigzag joins on all constraint pairs.
		// However, in the absence of stats on inverted indexes, we will not
		// be able to distinguish more selective constraints from less
		// selective ones anyway, so just pick the first two constraints.
		//
		// TODO(itsbilal): Use the remaining constraints to build a remaining
		// filters expression, instead of just reusing filters from the scan.
		constraint := constraints[0]
		constraint2 := constraints[1]

		minPrefix := constraint.ExactPrefix(c.e.evalCtx)
		if otherPrefix := constraint2.ExactPrefix(c.e.evalCtx); otherPrefix < minPrefix {
			minPrefix = otherPrefix
		}

		if minPrefix == 0 {
			continue
		}

		zigzagJoin := memo.ZigzagJoinExpr{
			On: filters,
			ZigzagJoinPrivate: memo.ZigzagJoinPrivate{
				LeftTable:  scanPrivate.Table,
				LeftIndex:  iter.indexOrdinal,
				RightTable: scanPrivate.Table,
				RightIndex: iter.indexOrdinal,
			},
		}

		// Get constant values from each constraint. Add them to FixedVals as
		// tuples, with associated Column IDs in both {Left,Right}FixedCols.
		leftVals := make(memo.ScalarListExpr, minPrefix)
		leftTypes := make([]types.T, minPrefix)
		rightVals := make(memo.ScalarListExpr, minPrefix)
		rightTypes := make([]types.T, minPrefix)

		zigzagJoin.LeftFixedCols = make(opt.ColList, minPrefix)
		zigzagJoin.RightFixedCols = make(opt.ColList, minPrefix)
		for i := 0; i < minPrefix; i++ {
			leftVal := constraint.Spans.Get(0).StartKey().Value(i)
			rightVal := constraint2.Spans.Get(0).StartKey().Value(i)

			leftVals[i] = c.e.f.ConstructConstVal(leftVal)
			leftTypes[i] = leftVal.ResolvedType()
			rightVals[i] = c.e.f.ConstructConstVal(rightVal)
			rightTypes[i] = rightVal.ResolvedType()
			zigzagJoin.LeftFixedCols[i] = constraint.Columns.Get(i).ID()
			zigzagJoin.RightFixedCols[i] = constraint.Columns.Get(i).ID()
		}
		zigzagJoin.FixedVals = memo.ScalarListExpr{
			c.e.f.ConstructTuple(leftVals, types.TTuple{Types: leftTypes}),
			c.e.f.ConstructTuple(rightVals, types.TTuple{Types: rightTypes}),
		}

		// Set equality columns - all remaining columns after the fixed prefix
		// need to be equal.
		eqColLen := iter.index.ColumnCount() - minPrefix
		zigzagJoin.LeftEqCols = make(opt.ColList, eqColLen)
		zigzagJoin.RightEqCols = make(opt.ColList, eqColLen)
		for i := minPrefix; i < iter.index.ColumnCount(); i++ {
			colID := scanPrivate.Table.ColumnID(iter.index.Column(i).Ordinal)
			zigzagJoin.LeftEqCols[i-minPrefix] = colID
			zigzagJoin.RightEqCols[i-minPrefix] = colID
		}
		zigzagJoin.On = filters

		// Don't output the first column (i.e. the inverted index's JSON key
		// col) from the zigzag join. It could contain partial values, so
		// presenting it in the output or checking ON conditions against
		// it makes little sense.
		zigzagCols := iter.indexCols()
		for i, cnt := 0, iter.index.KeyColumnCount(); i < cnt; i++ {
			colID := scanPrivate.Table.ColumnID(iter.index.Column(i).Ordinal)
			zigzagCols.Remove(int(colID))
		}

		pkIndex := iter.tab.Index(cat.PrimaryIndex)
		pkCols := make(opt.ColList, pkIndex.KeyColumnCount())
		for i := range pkCols {
			pkCols[i] = scanPrivate.Table.ColumnID(pkIndex.Column(i).Ordinal)
			// Ensure primary key columns are always retrieved from the zigzag
			// join.
			zigzagCols.Add(int(pkCols[i]))
		}

		// Case 1 (zigzagged indexes contain all requested columns).
		if scanPrivate.Cols.SubsetOf(zigzagCols) {
			zigzagJoin.Cols = scanPrivate.Cols
			c.e.mem.AddZigzagJoinToGroup(&zigzagJoin, grp)
			continue
		}

		if scanPrivate.Flags.NoIndexJoin {
			continue
		}

		// Case 2 (wrap zigzag join in an index join).

		var indexJoin memo.LookupJoinExpr
		// Ensure the zigzag join returns pk columns.
		zigzagJoin.Cols = scanPrivate.Cols.Intersection(zigzagCols)
		for i := range pkCols {
			zigzagJoin.Cols.Add(int(pkCols[i]))
		}

		if c.FiltersBoundBy(zigzagJoin.On, zigzagCols) {
			// The ON condition refers only to the columns available in the zigzag
			// indices.
			indexJoin.On = memo.TrueFilter
		} else {
			// ON has some conditions that are bound by the columns in the index (at
			// the very least, the equality conditions we used for EqCols and FixedCols),
			// and some conditions that refer to other table columns. We can put
			// the former in the lower ZigzagJoin and the latter in the index join.
			conditions := zigzagJoin.On
			zigzagJoin.On = c.ExtractBoundConditions(conditions, zigzagCols)
			indexJoin.On = c.ExtractUnboundConditions(conditions, zigzagCols)
		}

		indexJoin.Input = c.e.f.ConstructZigzagJoin(
			zigzagJoin.On,
			&zigzagJoin.ZigzagJoinPrivate,
		)
		indexJoin.JoinType = opt.InnerJoinOp
		indexJoin.Table = scanPrivate.Table
		indexJoin.Index = cat.PrimaryIndex
		indexJoin.KeyCols = pkCols
		indexJoin.Cols = scanPrivate.Cols

		// Create the LookupJoin for the index join in the same group as the
		// original select.
		c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
	}
}

// ----------------------------------------------------------------------
//
// GroupBy Rules
//   Custom match and replace functions used with groupby.opt rules.
//
// ----------------------------------------------------------------------

// IsCanonicalGroupBy returns true if the private is for the canonical version
// of the grouping operator.
func (c *CustomFuncs) IsCanonicalGroupBy(private *memo.GroupingPrivate) bool {
	// The canonical version always has all grouping columns as optional in the
	// internal ordering.
	return private.Ordering.Any() || private.GroupingCols.SubsetOf(private.Ordering.Optional)
}

// GenerateStreamingGroupBy generates variants of a GroupBy or DistinctOn
// expression with more specific orderings on the grouping columns, using the
// interesting orderings property. See the GenerateStreamingGroupBy rule.
func (c *CustomFuncs) GenerateStreamingGroupBy(
	grp memo.RelExpr,
	op opt.Operator,
	input memo.RelExpr,
	aggs memo.AggregationsExpr,
	private *memo.GroupingPrivate,
) {
	orders := DeriveInterestingOrderings(input)
	intraOrd := private.Ordering
	for _, o := range orders {
		// We are looking for a prefix of o that satisfies the intra-group ordering
		// if we ignore grouping columns.
		oIdx, intraIdx := 0, 0
		for ; oIdx < len(o); oIdx++ {
			oCol := o[oIdx].ID()
			if private.GroupingCols.Contains(int(oCol)) || intraOrd.Optional.Contains(int(oCol)) {
				// Grouping or optional column.
				continue
			}

			if intraIdx < len(intraOrd.Columns) &&
				intraOrd.Columns[intraIdx].Group.Contains(int(oCol)) &&
				intraOrd.Columns[intraIdx].Descending == o[oIdx].Descending() {
				// Column matches the one in the ordering.
				intraIdx++
				continue
			}
			break
		}
		if oIdx == 0 || intraIdx < len(intraOrd.Columns) {
			// No match.
			continue
		}
		o = o[:oIdx]

		var newOrd physical.OrderingChoice
		newOrd.FromOrderingWithOptCols(o, opt.ColSet{})

		// Simplify the ordering according to the input's FDs. Note that this is not
		// necessary for correctness because buildChildPhysicalProps would do it
		// anyway, but doing it here once can make things more efficient (and we may
		// generate fewer expressions if some of these orderings turn out to be
		// equivalent).
		newOrd.Simplify(&input.Relational().FuncDeps)

		switch op {
		case opt.GroupByOp:
			newExpr := memo.GroupByExpr{
				Input:        input,
				Aggregations: aggs,
				GroupingPrivate: memo.GroupingPrivate{
					GroupingCols: private.GroupingCols,
					Ordering:     newOrd,
				},
			}
			c.e.mem.AddGroupByToGroup(&newExpr, grp)

		case opt.DistinctOnOp:
			newExpr := memo.DistinctOnExpr{
				Input:        input,
				Aggregations: aggs,
				GroupingPrivate: memo.GroupingPrivate{
					GroupingCols: private.GroupingCols,
					Ordering:     newOrd,
				},
			}
			c.e.mem.AddDistinctOnToGroup(&newExpr, grp)
		}
	}
}

// MakeOrderingChoiceFromColumn constructs a new OrderingChoice with
// one element in the sequence: the columnID in the order defined by
// (MIN/MAX) operator. This function was originally created to be used
// with the Replace(Min|Max)WithLimit exploration rules.
func (c *CustomFuncs) MakeOrderingChoiceFromColumn(
	op opt.Operator, col opt.ColumnID,
) physical.OrderingChoice {
	oc := physical.OrderingChoice{}
	switch op {
	case opt.MinOp:
		oc.AppendCol(col, false /* descending */)
	case opt.MaxOp:
		oc.AppendCol(col, true /* descending */)
	}
	return oc
}

// scanIndexIter is a helper struct that supports iteration over the indexes
// of a Scan operator table. For example:
//
//   var iter scanIndexIter
//   iter.init(mem, scanOpDef)
//   for iter.next() {
//     doSomething(iter.indexOrdinal)
//   }
//
type scanIndexIter struct {
	mem          *memo.Memo
	scanPrivate  *memo.ScanPrivate
	tab          cat.Table
	indexOrdinal int
	index        cat.Index
	cols         opt.ColSet
}

func (it *scanIndexIter) init(mem *memo.Memo, scanPrivate *memo.ScanPrivate) {
	it.mem = mem
	it.scanPrivate = scanPrivate
	it.tab = mem.Metadata().Table(scanPrivate.Table)
	it.indexOrdinal = -1
	it.index = nil
}

// next advances iteration to the next index of the Scan operator's table. This
// is the primary index if it's the first time next is called, or a secondary
// index thereafter. Inverted index are skipped. If the ForceIndex flag is set,
// then all indexes except the forced index are skipped. When there are no more
// indexes to enumerate, next returns false. The current index is accessible via
// the iterator's "index" field.
func (it *scanIndexIter) next() bool {
	for {
		it.indexOrdinal++
		if it.indexOrdinal >= it.tab.IndexCount() {
			it.index = nil
			return false
		}
		it.index = it.tab.Index(it.indexOrdinal)
		if it.index.IsInverted() {
			continue
		}
		if it.scanPrivate.Flags.ForceIndex && it.scanPrivate.Flags.Index != it.indexOrdinal {
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
		it.indexOrdinal++
		if it.indexOrdinal >= it.tab.IndexCount() {
			it.index = nil
			return false
		}

		it.index = it.tab.Index(it.indexOrdinal)
		if !it.index.IsInverted() {
			continue
		}
		if it.scanPrivate.Flags.ForceIndex && it.scanPrivate.Flags.Index != it.indexOrdinal {
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
		it.cols = it.mem.Metadata().TableMeta(it.scanPrivate.Table).IndexColumns(it.indexOrdinal)
	}
	return it.cols
}

// isCovering returns true if the current index contains all columns projected
// by the Scan operator.
func (it *scanIndexIter) isCovering() bool {
	return it.scanPrivate.Cols.SubsetOf(it.indexCols())
}
