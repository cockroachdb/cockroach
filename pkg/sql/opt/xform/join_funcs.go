// Copyright 2020 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// GenerateMergeJoins spawns MergeJoinOps, based on any interesting orderings.
func (c *CustomFuncs) GenerateMergeJoins(
	grp memo.RelExpr,
	originalOp opt.Operator,
	left, right memo.RelExpr,
	on memo.FiltersExpr,
	joinPrivate *memo.JoinPrivate,
) {
	if joinPrivate.Flags.Has(memo.DisallowMergeJoin) {
		return
	}

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
	orders := DeriveInterestingOrderings(left).Copy()
	orders.RestrictToCols(leftEq.ToSet())

	if !c.NoJoinHints(joinPrivate) || c.e.evalCtx.SessionData.ReorderJoinsLimit == 0 {
		// If we are using a hint, or the join limit is set to zero, the join won't
		// be commuted. Add the orderings from the right side.
		rightOrders := DeriveInterestingOrderings(right).Copy()
		rightOrders.RestrictToCols(leftEq.ToSet())
		orders = append(orders, rightOrders...)

		// If we don't allow hash join, we must do our best to generate a merge
		// join, even if it means sorting both sides. We append an arbitrary
		// ordering, in case the interesting orderings don't result in any merge
		// joins.
		o := make(opt.Ordering, len(leftEq))
		for i := range o {
			o[i] = opt.MakeOrderingColumn(leftEq[i], false /* descending */)
		}
		orders.Add(o)
	}

	if len(orders) == 0 {
		return
	}

	var colToEq util.FastIntMap
	for i := range leftEq {
		colToEq.Set(int(leftEq[i]), i)
		colToEq.Set(int(rightEq[i]), i)
	}

	var remainingFilters memo.FiltersExpr

	for _, o := range orders {
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
		merge.JoinPrivate = *joinPrivate
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
//      CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//      CREATE TABLE xyz (x INT PRIMARY KEY, y INT, z INT, INDEX (y))
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
	joinPrivate *memo.JoinPrivate,
) {
	if joinPrivate.Flags.Has(memo.DisallowLookupJoinIntoRight) {
		return
	}
	md := c.e.mem.Metadata()
	inputProps := input.Relational()

	leftEq, rightEq := memo.ExtractJoinEqualityColumns(inputProps.OutputCols, scanPrivate.Cols, on)
	n := len(leftEq)
	if n == 0 {
		return
	}

	var pkCols opt.ColList
	var iter scanIndexIter
	iter.init(c.e.mem, &c.im, scanPrivate, on, rejectInvertedIndexes)
	iter.ForEach(func(index cat.Index, onFilters memo.FiltersExpr, indexCols opt.ColSet, isCovering bool) {
		// Find the longest prefix of index key columns that are constrained by
		// an equality with another column or a constant.
		numIndexKeyCols := index.LaxKeyColumnCount()

		var projections memo.ProjectionsExpr
		var constFilters memo.FiltersExpr

		// Check if the first column in the index has an equality constraint, or if
		// it is constrained to a constant value. This check doesn't guarantee that
		// we will find lookup join key columns, but it avoids the unnecessary work
		// in most cases.
		firstIdxCol := scanPrivate.Table.IndexColumnID(index, 0)
		if _, ok := rightEq.Find(firstIdxCol); !ok {
			if _, _, ok := c.findConstantFilter(onFilters, firstIdxCol); !ok {
				return
			}
		}

		lookupJoin := memo.LookupJoinExpr{Input: input}
		lookupJoin.JoinPrivate = *joinPrivate
		lookupJoin.JoinType = joinType
		lookupJoin.Table = scanPrivate.Table
		lookupJoin.Index = index.Ordinal()

		lookupJoin.KeyCols = make(opt.ColList, 0, numIndexKeyCols)
		rightSideCols := make(opt.ColList, 0, numIndexKeyCols)
		needProjection := false

		// All the lookup conditions must apply to the prefix of the index and so
		// the projected columns created must be created in order.
		for j := 0; j < numIndexKeyCols; j++ {
			idxCol := scanPrivate.Table.IndexColumnID(index, j)
			if eqIdx, ok := rightEq.Find(idxCol); ok {
				lookupJoin.KeyCols = append(lookupJoin.KeyCols, leftEq[eqIdx])
				rightSideCols = append(rightSideCols, idxCol)
				continue
			}

			// Try to find a filter that constrains this column to a non-NULL constant
			// value. We cannot use a NULL value because the lookup join implements
			// logic equivalent to simple equality between columns (where NULL never
			// equals anything).
			foundVal, onIdx, ok := c.findConstantFilter(onFilters, idxCol)
			if !ok || foundVal == tree.DNull {
				break
			}

			// We will project this constant value in the input to make it an equality
			// column.
			if projections == nil {
				projections = make(memo.ProjectionsExpr, 0, numIndexKeyCols-j)
				constFilters = make(memo.FiltersExpr, 0, numIndexKeyCols-j)
			}

			idxColType := c.e.f.Metadata().ColumnMeta(idxCol).Type
			constColID := c.e.f.Metadata().AddColumn(
				fmt.Sprintf("project_const_col_@%d", idxCol),
				idxColType,
			)
			projections = append(projections, c.e.f.ConstructProjectionsItem(
				c.e.f.ConstructConst(foundVal, idxColType),
				constColID,
			))

			needProjection = true
			lookupJoin.KeyCols = append(lookupJoin.KeyCols, constColID)
			rightSideCols = append(rightSideCols, idxCol)
			constFilters = append(constFilters, onFilters[onIdx])
		}

		if len(lookupJoin.KeyCols) == 0 {
			// We couldn't find equality columns which we can lookup.
			return
		}

		tableFDs := memo.MakeTableFuncDep(md, scanPrivate.Table)
		// A lookup join will drop any input row which contains NULLs, so a lax key
		// is sufficient.
		lookupJoin.LookupColsAreTableKey = tableFDs.ColsAreLaxKey(rightSideCols.ToSet())

		// Construct the projections for the constant columns.
		if needProjection {
			lookupJoin.Input = c.e.f.ConstructProject(input, projections, input.Relational().OutputCols)
		}

		// Remove the redundant filters and update the lookup condition.
		lookupJoin.On = memo.ExtractRemainingJoinFilters(onFilters, lookupJoin.KeyCols, rightSideCols)
		lookupJoin.On.RemoveCommonFilters(constFilters)
		lookupJoin.ConstFilters = constFilters

		if isCovering {
			// Case 1 (see function comment).
			lookupJoin.Cols = scanPrivate.Cols.Union(inputProps.OutputCols)
			c.e.mem.AddLookupJoinToGroup(&lookupJoin, grp)
			return
		}

		_, isPartial := index.Predicate()
		if isPartial && (joinType == opt.SemiJoinOp || joinType == opt.AntiJoinOp) {
			// Typically, the index must cover all columns in the scanPrivate in
			// order to generate a lookup join without an additional index join
			// (case 1, see function comment). However, if the index is a
			// partial index, the filters remaining after proving
			// filter-predicate implication may no longer reference some
			// columns. A lookup semi- or anti-join can be generated if the
			// columns in the new filters from the right side of the join are
			// covered by the index. Consider the example:
			//
			//   CREATE TABLE a (a INT)
			//   CREATE TABLE xy (x INT, y INT, INDEX (x) WHERE y > 0)
			//
			//   SELECT a FROM a WHERE EXISTS (SELECT 1 FROM xyz WHERE a = x AND y > 0)
			//
			// The original ON filters of the semi-join are (a = x AND y > 0).
			// The (y > 0) expression in the filter is an exact match to the
			// partial index predicate, so the remaining ON filters are (a = x).
			// Column y is no longer referenced, so a lookup semi-join can be
			// created despite the partial index not covering y.
			//
			// Note that this is a special case that only works for semi- and
			// anti-joins because they never include columns from the right side
			// in their output columns. Other joins include columns from the
			// right side in their output columns, so even if the ON filters no
			// longer reference an un-covered column, they must be fetched (case
			// 2, see function comment).
			filterColsFromRight := scanPrivate.Cols.Intersection(onFilters.OuterCols(c.e.mem))
			if filterColsFromRight.SubsetOf(indexCols) {
				lookupJoin.Cols = filterColsFromRight.Union(inputProps.OutputCols)
				c.e.mem.AddLookupJoinToGroup(&lookupJoin, grp)
				return
			}
		}

		// All code that follows is for case 2 (see function comment).

		if scanPrivate.Flags.NoIndexJoin {
			return
		}
		if joinType == opt.SemiJoinOp || joinType == opt.AntiJoinOp {
			// We cannot use a non-covering index for semi and anti join. Note that
			// since the semi/anti join doesn't pass through any columns, "non
			// covering" here means that not all columns in the ON condition are
			// available.
			//
			// TODO(radu): We could create a semi/anti join on top of an inner join if
			// the lookup columns form a key (to guarantee that input rows are not
			// duplicated by the inner join).
			return
		}

		if pkCols == nil {
			pkIndex := md.Table(scanPrivate.Table).Index(cat.PrimaryIndex)
			pkCols = make(opt.ColList, pkIndex.KeyColumnCount())
			for i := range pkCols {
				pkCols[i] = scanPrivate.Table.IndexColumnID(pkIndex, i)
			}
		}

		// The lower LookupJoin must return all PK columns (they are needed as key
		// columns for the index join).
		lookupJoin.Cols = scanPrivate.Cols.Intersection(indexCols)
		for i := range pkCols {
			lookupJoin.Cols.Add(pkCols[i])
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
				return
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
		indexJoin.LookupColsAreTableKey = true

		// Create the LookupJoin for the index join in the same group.
		c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
	})
}

// GenerateInvertedJoins is similar to GenerateLookupJoins, but instead
// of generating lookup joins with regular indexes, it generates lookup joins
// with inverted indexes. Similar to GenerateLookupJoins, there are two cases
// depending on whether or not the index is covering. See the comment above
// GenerateLookupJoins for details.
// TODO(rytaft): add support for JSON and array inverted indexes.
func (c *CustomFuncs) GenerateInvertedJoins(
	grp memo.RelExpr,
	joinType opt.Operator,
	input memo.RelExpr,
	scanPrivate *memo.ScanPrivate,
	on memo.FiltersExpr,
	joinPrivate *memo.JoinPrivate,
) {
	if joinPrivate.Flags.Has(memo.DisallowLookupJoinIntoRight) {
		return
	}

	inputCols := input.Relational().OutputCols
	var pkCols opt.ColList

	// TODO(mgartner): Use partial indexes for geolookup joins when the
	// predicate is implied by the on filter.
	var iter scanIndexIter
	iter.init(c.e.mem, &c.im, scanPrivate, nil /* filters */, rejectNonInvertedIndexes|rejectPartialIndexes)
	iter.ForEach(func(index cat.Index, _ memo.FiltersExpr, indexCols opt.ColSet, isCovering bool) {
		// Check whether the filter can constrain the index.
		invertedExpr := invertedidx.TryJoinGeoIndex(
			c.e.evalCtx.Context, c.e.f, on, scanPrivate.Table, index, inputCols,
		)
		if invertedExpr == nil {
			return
		}

		// Geospatial lookup joins are not covering, so we must wrap them in an
		// index join.
		if scanPrivate.Flags.NoIndexJoin {
			return
		}
		if joinType == opt.SemiJoinOp || joinType == opt.AntiJoinOp {
			// We cannot use a non-covering index for semi and anti join. Note that
			// since the semi/anti join doesn't pass through any columns, "non
			// covering" here means that not all columns in the ON condition are
			// available.
			//
			// For semi joins, we may still be able to generate an inverted join
			// by converting it to an inner join using the ConvertSemiToInnerJoin
			// rule. Any semi join that could use an inverted index would already be
			// transformed into an inner join by ConvertSemiToInnerJoin, so semi
			// joins can be ignored here.
			return
		}

		if pkCols == nil {
			tab := c.e.mem.Metadata().Table(scanPrivate.Table)
			pkIndex := tab.Index(cat.PrimaryIndex)
			pkCols = make(opt.ColList, pkIndex.KeyColumnCount())
			for i := range pkCols {
				pkCols[i] = scanPrivate.Table.IndexColumnID(pkIndex, i)
			}
		}

		// Though the index is marked as containing the column being indexed, it
		// doesn't actually, and it is only valid to extract the primary key
		// columns from it.
		indexCols = pkCols.ToSet()

		lookupJoin := memo.InvertedJoinExpr{Input: input}
		lookupJoin.JoinPrivate = *joinPrivate
		lookupJoin.JoinType = joinType
		lookupJoin.Table = scanPrivate.Table
		lookupJoin.Index = index.Ordinal()
		lookupJoin.InvertedExpr = invertedExpr
		lookupJoin.InvertedCol = scanPrivate.Table.IndexColumnID(index, 0)
		lookupJoin.Cols = indexCols.Union(inputCols)

		var indexJoin memo.LookupJoinExpr

		// ON may have some conditions that are bound by the columns in the index
		// and some conditions that refer to other columns. We can put the former
		// in the InvertedJoin and the latter in the index join.
		lookupJoin.On = c.ExtractBoundConditions(on, lookupJoin.Cols)
		indexJoin.On = c.ExtractUnboundConditions(on, lookupJoin.Cols)

		indexJoin.Input = c.e.f.ConstructInvertedJoin(
			lookupJoin.Input,
			lookupJoin.On,
			&lookupJoin.InvertedJoinPrivate,
		)
		indexJoin.JoinType = joinType
		indexJoin.Table = scanPrivate.Table
		indexJoin.Index = cat.PrimaryIndex
		indexJoin.KeyCols = pkCols
		indexJoin.Cols = scanPrivate.Cols.Union(inputCols)
		indexJoin.LookupColsAreTableKey = true

		// Create the LookupJoin for the index join in the same group.
		c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
	})
}

// findConstantFilter tries to find a filter that is exactly equivalent to
// constraining the given column to a constant value. Note that the constant
// value can be NULL (for an `x IS NULL` filter).
func (c *CustomFuncs) findConstantFilter(
	filters memo.FiltersExpr, col opt.ColumnID,
) (value tree.Datum, filterIdx int, ok bool) {
	for filterIdx := range filters {
		props := filters[filterIdx].ScalarProps()
		if props.TightConstraints {
			constCol, constVal, ok := props.Constraints.IsSingleColumnConstValue(c.e.evalCtx)
			if ok && constCol == col {
				return constVal, filterIdx, true
			}
		}
	}
	return nil, -1, false
}

// ShouldReorderJoins returns whether the optimizer should attempt to find
// a better ordering of inner joins. This is the case if the given expression is
// the first expression of its group, and the join tree rooted at the expression
// has not previously been reordered. This is to avoid duplicate work. In
// addition, a join cannot be reordered if it has join hints.
func (c *CustomFuncs) ShouldReorderJoins(root memo.RelExpr) bool {
	// Only match the first expression of a group to avoid duplicate work.
	if root != root.FirstExpr() {
		return false
	}

	private, ok := root.Private().(*memo.JoinPrivate)
	if !ok {
		panic(errors.AssertionFailedf("operator does not have a join private: %v", root.Op()))
	}

	// Ensure that this join expression was not added to the memo by a previous
	// reordering, as well as that the join does not have hints.
	return !private.SkipReorderJoins && c.NoJoinHints(private)
}

// ReorderJoins adds alternate orderings of the given join tree to the memo. The
// first expression of the memo group is used for construction of the join
// graph. For more information, see the comment in join_order_builder.go.
func (c *CustomFuncs) ReorderJoins(grp memo.RelExpr) memo.RelExpr {
	c.e.o.JoinOrderBuilder().Init(c.e.f, c.e.evalCtx)
	c.e.o.JoinOrderBuilder().Reorder(grp.FirstExpr())
	return grp
}

// IsSimpleEquality returns true if all of the filter conditions are equalities
// between simple data types (constants, variables, tuples and NULL).
func (c *CustomFuncs) IsSimpleEquality(filters memo.FiltersExpr) bool {
	for i := range filters {
		eqFilter, ok := filters[i].Condition.(*memo.EqExpr)
		if !ok {
			return false
		}

		left, right := eqFilter.Left, eqFilter.Right
		switch left.Op() {
		case opt.VariableOp, opt.ConstOp, opt.NullOp, opt.TupleOp:
		default:
			return false
		}

		switch right.Op() {
		case opt.VariableOp, opt.ConstOp, opt.NullOp, opt.TupleOp:
		default:
			return false
		}
	}

	return true
}

// ConvertIndexToLookupJoinPrivate constructs a new LookupJoinPrivate using the
// given IndexJoinPrivate with the given output columns.
func (c *CustomFuncs) ConvertIndexToLookupJoinPrivate(
	indexPrivate *memo.IndexJoinPrivate, outCols opt.ColSet,
) *memo.LookupJoinPrivate {
	// Retrieve an ordered list of primary key columns from the lookup table;
	// these will form the lookup key.
	md := c.e.mem.Metadata()
	primaryIndex := md.Table(indexPrivate.Table).Index(cat.PrimaryIndex)
	lookupCols := make(opt.ColList, primaryIndex.KeyColumnCount())
	for i := 0; i < primaryIndex.KeyColumnCount(); i++ {
		lookupCols[i] = indexPrivate.Table.IndexColumnID(primaryIndex, i)
	}

	return &memo.LookupJoinPrivate{
		JoinType:              opt.InnerJoinOp,
		Table:                 indexPrivate.Table,
		Index:                 cat.PrimaryIndex,
		KeyCols:               lookupCols,
		Cols:                  outCols,
		LookupColsAreTableKey: true,
		ConstFilters:          nil,
		JoinPrivate:           memo.JoinPrivate{},
	}
}

// AddWithBinding adds a With binding for the given binding expression.
// Returns the WithID associated with the binding.
func (c *CustomFuncs) AddWithBinding(expr memo.RelExpr) opt.WithID {
	withID := c.e.mem.NextWithID()
	c.e.mem.Metadata().AddWithBinding(withID, expr)
	return withID
}

// MakeWithScan constructs a WithScan expression that scans the With expression
// with the given WithID. It creates new columns in the metadata for the
// WithScan output columns.
func (c *CustomFuncs) MakeWithScan(withID opt.WithID) memo.RelExpr {
	binding := c.e.mem.Metadata().WithBinding(withID).(memo.RelExpr)
	cols := binding.Relational().OutputCols
	inCols := make(opt.ColList, cols.Len())
	outCols := make(opt.ColList, len(inCols))

	i := 0
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		colMeta := c.e.mem.Metadata().ColumnMeta(col)
		inCols[i] = col
		outCols[i] = c.e.mem.Metadata().AddColumn(colMeta.Alias, colMeta.Type)
		i++
	}

	return c.e.f.ConstructWithScan(&memo.WithScanPrivate{
		With:    withID,
		InCols:  inCols,
		OutCols: outCols,
		ID:      c.e.mem.Metadata().NextUniqueID(),
	})
}

// MakeWithScanUsingCols constructs a WithScan expression that scans the With
// expression with the given WithID. It uses the provided columns for the
// output columns of the WithScan.
func (c *CustomFuncs) MakeWithScanUsingCols(withID opt.WithID, outColSet opt.ColSet) memo.RelExpr {
	binding := c.e.mem.Metadata().WithBinding(withID).(memo.RelExpr)
	inColSet := binding.Relational().OutputCols
	if outColSet.Len() != inColSet.Len() {
		panic(errors.AssertionFailedf(
			"outColSet.Len() must match the number of output columns of the given With expression (%d != %d)",
			outColSet.Len(), inColSet.Len(),
		))
	}
	inCols := make(opt.ColList, inColSet.Len())
	outCols := make(opt.ColList, outColSet.Len())

	i := 0
	for col, ok := inColSet.Next(0); ok; col, ok = inColSet.Next(col + 1) {
		inCols[i] = col
		i++
	}

	i = 0
	for col, ok := outColSet.Next(0); ok; col, ok = outColSet.Next(col + 1) {
		outCols[i] = col
		i++
	}

	return c.e.f.ConstructWithScan(&memo.WithScanPrivate{
		With:    withID,
		InCols:  inCols,
		OutCols: outCols,
		ID:      c.e.mem.Metadata().NextUniqueID(),
	})
}

// MakeWithScanKeyEqualityFilters takes two WithScan expressions that scan the
// same With expression. It returns a filters expression that contains equality
// conditions between the primary key columns from each side. For example,
// if WithScans a and b are both scanning a With expression that has key
// columns x and y, MakeWithScanKeyEqualityFilters will return filters
// a.x = b.x AND a.y = b.y.
func (c *CustomFuncs) MakeWithScanKeyEqualityFilters(left, right opt.Expr) memo.FiltersExpr {
	leftWithScan := left.(*memo.WithScanExpr)
	rightWithScan := right.(*memo.WithScanExpr)
	if leftWithScan.With != rightWithScan.With {
		panic(errors.AssertionFailedf(
			"attempt to make equality filters between WithScans of different With expressions",
		))
	}
	if !leftWithScan.InCols.Equals(rightWithScan.InCols) {
		panic(errors.AssertionFailedf(
			"attempt to make equality filters between WithScans with different input columns",
		))
	}

	binding := c.e.mem.Metadata().WithBinding(leftWithScan.With).(memo.RelExpr)
	keyCols, ok := binding.Relational().FuncDeps.StrictKey()
	if !ok {
		panic(errors.AssertionFailedf("WithBinding has no key (was EnsureKey called?)"))
	}

	filters := make(memo.FiltersExpr, keyCols.Len())
	keyIdx := 0
	for i := 0; i < len(leftWithScan.InCols); i++ {
		if !keyCols.Contains(leftWithScan.InCols[i]) {
			continue
		}

		filters[keyIdx] = c.e.f.ConstructFiltersItem(c.e.f.ConstructEq(
			c.e.f.ConstructVariable(leftWithScan.OutCols[i]),
			c.e.f.ConstructVariable(rightWithScan.OutCols[i]),
		))
		keyIdx++
	}

	return filters
}

// MakeWithPrivate returns a WithPrivate containing the given WithID.
func (c *CustomFuncs) MakeWithPrivate(id opt.WithID) *memo.WithPrivate {
	return &memo.WithPrivate{
		ID: id,
	}
}

// ReplaceOutputCols replaces the output columns of the given expression by
// wrapping the expression in a project expression that projects each of the
// original output columns as a new column with a new ColumnID.
func (c *CustomFuncs) ReplaceOutputCols(expr memo.RelExpr) memo.RelExpr {
	srcCols := expr.Relational().OutputCols
	projections := make(memo.ProjectionsExpr, srcCols.Len())

	i := 0
	for srcCol, ok := srcCols.Next(0); ok; srcCol, ok = srcCols.Next(srcCol + 1) {
		colMeta := c.e.mem.Metadata().ColumnMeta(srcCol)
		dstCol := c.e.mem.Metadata().AddColumn(colMeta.Alias, colMeta.Type)
		projections[i] = c.e.f.ConstructProjectionsItem(c.e.f.ConstructVariable(srcCol), dstCol)
		i++
	}

	return c.e.f.ConstructProject(expr, projections, opt.ColSet{})
}

// MapFilterCols returns a new FiltersExpr with all the src column IDs in
// the input expression replaced with column IDs in dst.
//
// NOTE: Every ColumnID in src must map to the a ColumnID in dst with the same
// relative position in the ColSets. For example, if src and dst are (1, 5, 6)
// and (7, 12, 15), then the following mapping would be applied:
//
//   1 => 7
//   5 => 12
//   6 => 15
func (c *CustomFuncs) MapFilterCols(
	filters memo.FiltersExpr, src, dst opt.ColSet,
) memo.FiltersExpr {
	if src.Len() != dst.Len() {
		panic(errors.AssertionFailedf(
			"src and dst must have the same number of columns, src: %v, dst: %v",
			src,
			dst,
		))
	}

	// Map each column in src to a column in dst based on the relative position
	// of both the src and dst ColumnIDs in the ColSet.
	var colMap opt.ColMap
	dstCol, _ := dst.Next(0)
	for srcCol, ok := src.Next(0); ok; srcCol, ok = src.Next(srcCol + 1) {
		colMap.Set(int(srcCol), int(dstCol))
		dstCol, _ = dst.Next(dstCol + 1)
	}

	newFilters := c.RemapCols(&filters, colMap).(*memo.FiltersExpr)
	return *newFilters
}

// CanGenerateInvertedJoin is a best-effort check that returns true if it
// may be possible to generate an inverted join with the given right-side input
// and on conditions. It may return some false positives, but it is used to
// avoid applying certain rules such as ConvertLeftToInnerJoin in cases where
// they may not be beneficial.
func (c *CustomFuncs) CanGenerateInvertedJoin(rightInput memo.RelExpr, on memo.FiltersExpr) bool {
	// The right-side input must be either a canonical Scan or a Select wrapping a
	// canonical Scan on a table that has inverted indexes. These are the conditions
	// checked by GenerateInvertedJoins and GenerateInvertedJoinsFromSelect,
	// so they are required for generating an inverted join.
	scan, ok := rightInput.(*memo.ScanExpr)
	if !ok {
		sel, ok := rightInput.(*memo.SelectExpr)
		if !ok {
			return false
		}
		scan, ok = sel.Input.(*memo.ScanExpr)
		if !ok {
			return false
		}
	}

	if !c.IsCanonicalScan(&scan.ScanPrivate) || !c.HasInvertedIndexes(&scan.ScanPrivate) {
		return false
	}

	// Check whether any of the ON conditions contain a geospatial function or
	// operator that can be index-accelerated.
	for i := range on {
		if c.exprContainsGeoIndexRelationship(on[i].Condition) {
			return true
		}
	}

	return false
}

// exprContainsGeoIndexRelationship returns true if the given expression
// contains a geospatial function or bounding box operator that can be
// index accelerated. It is not a guarantee that an inverted join will be
// produced for the given ON condition, but it eliminates expressions that
// definitely cannot produce an inverted join.
func (c *CustomFuncs) exprContainsGeoIndexRelationship(expr opt.ScalarExpr) bool {
	switch t := expr.(type) {
	case *memo.AndExpr:
		return c.exprContainsGeoIndexRelationship(t.Left) || c.exprContainsGeoIndexRelationship(t.Right)
	case *memo.OrExpr:
		return c.exprContainsGeoIndexRelationship(t.Left) || c.exprContainsGeoIndexRelationship(t.Right)
	default:
		if _, ok := invertedidx.GetGeoIndexRelationship(expr); ok {
			return true
		}
		return false
	}
}

// EnsureNotNullColFromFilteredScan ensures that there is at least one not-null
// column in the given expression. If there is already at least one not-null
// column, EnsureNotNullColFromFilteredScan returns the expression unchanged.
// Otherwise, it calls TryAddKeyToScan, which will try to augment the
// expression with the primary key of the underlying table scan (guaranteed to
// be not-null). In order for this call to succeed, the input expression must
// be a non-virtual Scan, optionally wrapped in a Select. Otherwise,
// EnsureNotNullColFromFilteredScan will panic.
//
// Note that we cannot just project a not-null constant value because we want
// the output expression to be like the input: a non-virtual Scan, optionally
// wrapped in a Select. This is needed to ensure that GenerateInvertedJoins
// or GenerateInvertedJoinsFromSelect can match on this expression.
func (c *CustomFuncs) EnsureNotNullColFromFilteredScan(expr memo.RelExpr) memo.RelExpr {
	if !expr.Relational().NotNullCols.Empty() {
		return expr
	}
	if res, ok := c.TryAddKeyToScan(expr); ok {
		return res
	}
	panic(errors.AssertionFailedf(
		"TryAddKeyToScan failed. Input must have type Scan or Select(Scan). Actual type: %T", expr,
	))
}

// NotNullCol returns the first not-null column from the input expression.
// EnsureNotNullColFromFilteredScan must have been called previously to
// ensure that such a column exists.
func (c *CustomFuncs) NotNullCol(expr memo.RelExpr) opt.ColumnID {
	col, ok := expr.Relational().NotNullCols.Next(0)
	if !ok {
		panic(errors.AssertionFailedf(
			"NotNullCol was called on an expression with no not-null columns",
		))
	}
	return col
}
