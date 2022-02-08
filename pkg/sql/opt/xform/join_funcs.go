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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partition"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	orders := ordering.DeriveInterestingOrderings(left).Copy()
	leftCols, leftFDs := leftEq.ToSet(), &left.Relational().FuncDeps
	orders.RestrictToCols(leftCols, leftFDs)

	var mustGenerateMergeJoin bool
	if len(orders) == 0 && leftCols.SubsetOf(leftFDs.ConstantCols()) {
		// All left equality columns are constant, so we can trivially create
		// an ordering.
		mustGenerateMergeJoin = true
	}

	if !c.NoJoinHints(joinPrivate) || c.e.evalCtx.SessionData().ReorderJoinsLimit == 0 {
		// If we are using a hint, or the join limit is set to zero, the join won't
		// be commuted. Add the orderings from the right side.
		rightOrders := ordering.DeriveInterestingOrderings(right).Copy()
		rightOrders.RestrictToCols(rightEq.ToSet(), &right.Relational().FuncDeps)
		orders = append(orders, rightOrders...)

		// If we don't allow hash join, we must do our best to generate a merge
		// join, even if it means sorting both sides.
		mustGenerateMergeJoin = true
	}

	if mustGenerateMergeJoin {
		// We append an arbitrary ordering, in case the interesting orderings don't
		// result in any merge joins.
		o := make(opt.Ordering, len(leftEq))
		for i := range o {
			o[i] = opt.MakeOrderingColumn(leftEq[i], false /* descending */)
		}
		var oc props.OrderingChoice
		oc.FromOrdering(o)
		orders.Add(&oc)
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
		if remainingFilters == nil {
			remainingFilters = memo.ExtractRemainingJoinFilters(on, leftEq, rightEq)
		}

		merge := memo.MergeJoinExpr{Left: left, Right: right, On: remainingFilters}
		merge.JoinPrivate = *joinPrivate
		merge.JoinType = originalOp
		merge.LeftEq = make(opt.Ordering, 0, n)
		merge.RightEq = make(opt.Ordering, 0, n)
		merge.LeftOrdering.Columns = make([]props.OrderingColumnChoice, 0, n)
		merge.RightOrdering.Columns = make([]props.OrderingColumnChoice, 0, n)

		addCol := func(col opt.ColumnID, descending bool) {
			eqIdx, _ := colToEq.Get(int(col))
			l, r := leftEq[eqIdx], rightEq[eqIdx]
			merge.LeftEq = append(merge.LeftEq, opt.MakeOrderingColumn(l, descending))
			merge.RightEq = append(merge.RightEq, opt.MakeOrderingColumn(r, descending))
			merge.LeftOrdering.AppendCol(l, descending)
			merge.RightOrdering.AppendCol(r, descending)
		}

		// Add the required ordering columns.
		for i := range o.Columns {
			c := &o.Columns[i]
			c.Group.ForEach(func(col opt.ColumnID) {
				addCol(col, c.Descending)
			})
		}

		// Add the remaining columns in an arbitrary order.
		remaining := leftCols.Difference(merge.LeftEq.ColSet())
		remaining.ForEach(func(col opt.ColumnID) {
			addCol(col, false /* descending */)
		})

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
//         Join                       LookupJoin(t@idx)
//         /   \                           |
//        /     \            ->            |
//      Input  Scan(t)                   Input
//
//
//  2. The index is not covering, but we can fully evaluate the ON condition
//     using the index, or we are doing an InnerJoin. We have to generate
//     an index join above the lookup join. Note that this index join is also
//     implemented as a LookupJoin, because an IndexJoin can only output
//     columns from one table, whereas we also need to output columns from
//     Input.
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
//     LookupJoin) will produce columns a,b,c,x,y,z; the lookup columns are just z
//     (the original lookup join produced a,b,c,x,y).
//
//     Note that the top LookupJoin "sees" column IDs from the table on both
//     "sides" (in this example x,y on the left and z on the right) but there is
//     no overlap.
//
//  3. The index is not covering and we cannot fully evaluate the ON condition
//     using the index, and we are doing a LeftJoin/SemiJoin/AntiJoin. This is
//     handled using a lower-upper pair of joins that are further specialized
//     as paired-joins. The first (lower) join outputs a continuation column
//     that is used by the second (upper) join. Like case 2, both are lookup
//     joins, but paired-joins explicitly know their role in the pair and
//     behave accordingly.
//
//     For example, using the same tables in the example for case 2:
//      SELECT * FROM abc LEFT JOIN xyz ON a=y AND b=z
//
//     The first join will evaluate a=y and produce columns a,b,c,x,y,cont
//     where cont is the continuation column used to group together rows that
//     correspond to the same original a,b,c. The second join will fetch z from
//     the primary index, evaluate b=z, and produce columns a,b,c,x,y,z. A
//     similar approach works for anti-joins and semi-joins.
//
//
// A lookup join can be created when the ON condition or implicit filters from
// CHECK constraints and computed columns constrain a prefix of the index
// columns to non-ranging constant values. To support this, the constant values
// are cross-joined with the input and used as key columns for the parent lookup
// join.
//
// For example, consider the tables and query below.
//
//   CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//   CREATE TABLE xyz (
//     x INT PRIMARY KEY,
//     y INT,
//     z INT NOT NULL,
//     CHECK z IN (1, 2, 3),
//     INDEX (z, y)
//   )
//   SELECT a, x FROM abc JOIN xyz ON a=y
//
// GenerateLookupJoins will perform the following transformation.
//
//         Join                       LookupJoin(t@idx)
//         /   \                           |
//        /     \            ->            |
//      Input  Scan(t)                   Join
//                                       /   \
//                                      /     \
//                                    Input  Values(1, 2, 3)
//
// If a column is constrained to a single constant value, inlining normalization
// rules will reduce the cross join into a project.
//
//         Join                       LookupJoin(t@idx)
//         /   \                           |
//        /     \            ->            |
//      Input  Scan(t)                  Project
//                                         |
//                                         |
//                                       Input
//
func (c *CustomFuncs) GenerateLookupJoins(
	grp memo.RelExpr,
	joinType opt.Operator,
	input memo.RelExpr,
	scanPrivate *memo.ScanPrivate,
	on memo.FiltersExpr,
	joinPrivate *memo.JoinPrivate,
) {
	c.generateLookupJoinsImpl(
		grp, joinType,
		input,
		scanPrivate.Cols,
		opt.ColSet{}, /* projectedVirtualCols */
		scanPrivate,
		on,
		joinPrivate,
	)
}

// GenerateLookupJoinsWithVirtualCols is similar to GenerateLookupJoins but
// generates lookup joins into indexes that contain virtual columns.
//
// In a canonical plan a virtual column is produced with a Project expression on
// top of a Scan. This is necessary because virtual columns aren't stored in the
// primary index. When a virtual column is indexed, a lookup join can be
// generated that both uses the virtual column as a lookup column and produces
// the column directly from the index without a Project.
//
// For example:
//
//         Join                       LookupJoin(t@idx)
//         /   \                           |
//        /     \            ->            |
//      Input  Project                   Input
//               |
//               |
//             Scan(t)
//
// This function and its associated rule currently require that:
//
//   1. The join is an inner join.
//   2. The right side projects only virtual computed columns.
//   3. All the projected virtual columns are covered by a single index.
//
// It should be possible to support semi- and anti- joins. Left joins may be
// possible with additional complexity.
//
// It should also be possible to support cases where all the virtual columns are
// not covered by a single index by wrapping the lookup join in a Project that
// produces the non-covered virtual columns.
func (c *CustomFuncs) GenerateLookupJoinsWithVirtualCols(
	grp memo.RelExpr,
	joinType opt.Operator,
	input memo.RelExpr,
	rightCols opt.ColSet,
	projectedVirtualCols opt.ColSet,
	scanPrivate *memo.ScanPrivate,
	on memo.FiltersExpr,
	joinPrivate *memo.JoinPrivate,
) {
	c.generateLookupJoinsImpl(
		grp, joinType,
		input,
		rightCols,
		projectedVirtualCols,
		scanPrivate,
		on,
		joinPrivate,
	)
}

// generateLookupJoinsImpl is the general implementation for generating lookup
// joins. The rightCols argument must be the columns output by the right side of
// matched join expression. projectedVirtualCols is the set of virtual columns
// projected on the right side of the matched join expression.
//
// See GenerateLookupJoins and GenerateLookupJoinsWithVirtualCols for
// more details.
func (c *CustomFuncs) generateLookupJoinsImpl(
	grp memo.RelExpr,
	joinType opt.Operator,
	input memo.RelExpr,
	rightCols opt.ColSet,
	projectedVirtualCols opt.ColSet,
	scanPrivate *memo.ScanPrivate,
	on memo.FiltersExpr,
	joinPrivate *memo.JoinPrivate,
) {

	if joinPrivate.Flags.Has(memo.DisallowLookupJoinIntoRight) {
		return
	}
	md := c.e.mem.Metadata()
	inputProps := input.Relational()

	leftEq, rightEq := memo.ExtractJoinEqualityColumns(inputProps.OutputCols, rightCols, on)
	if len(leftEq) == 0 {
		return
	}
	rightEqSet := rightEq.ToSet()

	// Generate implicit filters from CHECK constraints and computed columns as
	// optional filters to help generate lookup join keys.
	optionalFilters := c.checkConstraintFilters(scanPrivate.Table)
	computedColFilters := c.computedColFilters(scanPrivate, on, optionalFilters)
	optionalFilters = append(optionalFilters, computedColFilters...)

	var pkCols opt.ColList
	var eqColMap opt.ColMap
	var iter scanIndexIter
	iter.Init(c.e.evalCtx, c.e.f, c.e.mem, &c.im, scanPrivate, on, rejectInvertedIndexes)
	iter.ForEach(func(index cat.Index, onFilters memo.FiltersExpr, indexCols opt.ColSet, _ bool, _ memo.ProjectionsExpr) {
		// Skip indexes that do no cover all virtual projection columns, if
		// there are any. This can happen when there are multiple virtual
		// columns indexed in different indexes.
		//
		// TODO(mgartner): It should be possible to plan a lookup join in this
		// case by producing the covered virtual columns from the lookup join
		// and producing the rest in a Project that wraps the join.
		if !projectedVirtualCols.SubsetOf(indexCols) {
			return
		}

		// Find the longest prefix of index key columns that are constrained by
		// an equality with another column or a constant.
		numIndexKeyCols := index.LaxKeyColumnCount()

		var constFilters memo.FiltersExpr
		allFilters := append(onFilters, optionalFilters...)

		// Check if the first column in the index either:
		//
		//   1. Has an equality constraint.
		//   2. Is a computed column for which an equality constraint can be
		//      generated.
		//   3. Is constrained to a constant value or values.
		//
		// This check doesn't guarantee that we will find lookup join key
		// columns, but it avoids unnecessary work in most cases.
		firstIdxCol := scanPrivate.Table.IndexColumnID(index, 0)
		if _, ok := rightEq.Find(firstIdxCol); !ok {
			if _, ok := c.findComputedColJoinEquality(scanPrivate.Table, firstIdxCol, rightEqSet); !ok {
				if _, _, ok := c.findJoinFilterConstants(allFilters, firstIdxCol); !ok {
					return
				}
			}
		}

		lookupJoin := memo.LookupJoinExpr{Input: input}
		lookupJoin.JoinPrivate = *joinPrivate
		lookupJoin.JoinType = joinType
		lookupJoin.Table = scanPrivate.Table
		lookupJoin.Index = index.Ordinal()

		lookupJoin.KeyCols = make(opt.ColList, 0, numIndexKeyCols)
		rightSideCols := make(opt.ColList, 0, numIndexKeyCols)
		var inputProjections memo.ProjectionsExpr

		shouldBuildMultiSpanLookupJoin := false

		// All the lookup conditions must apply to the prefix of the index and so
		// the projected columns created must be created in order.
		for j := 0; j < numIndexKeyCols; j++ {
			idxCol := scanPrivate.Table.IndexColumnID(index, j)
			if eqIdx, ok := rightEq.Find(idxCol); ok {
				lookupJoin.KeyCols = append(lookupJoin.KeyCols, leftEq[eqIdx])
				rightSideCols = append(rightSideCols, idxCol)
				continue
			}

			// If the column is computed and an equality constraint can be
			// synthesized for it, we can project a column from the join's input
			// that can be used as a key column. We create the projection here,
			// and construct a Project expression that wraps the join's input
			// below. See findComputedColJoinEquality for the requirements to
			// synthesize a computed column equality constraint.
			if expr, ok := c.findComputedColJoinEquality(scanPrivate.Table, idxCol, rightEqSet); ok {
				colMeta := md.ColumnMeta(idxCol)
				compEqCol := md.AddColumn(fmt.Sprintf("%s_eq", colMeta.Alias), colMeta.Type)

				// Lazily initialize eqColMap.
				if eqColMap.Empty() {
					for i := range rightEq {
						eqColMap.Set(int(rightEq[i]), int(leftEq[i]))
					}
				}

				// Project the computed column expression, mapping all columns
				// in rightEq to corresponding columns in leftEq.
				projection := c.e.f.ConstructProjectionsItem(c.RemapCols(expr, eqColMap), compEqCol)
				inputProjections = append(inputProjections, projection)
				lookupJoin.KeyCols = append(lookupJoin.KeyCols, compEqCol)
				rightSideCols = append(rightSideCols, idxCol)
				continue
			}

			// Try to find a filter that constrains this column to non-NULL
			// constant values. We cannot use a NULL value because the lookup
			// join implements logic equivalent to simple equality between
			// columns (where NULL never equals anything).
			foundVals, allIdx, ok := c.findJoinFilterConstants(allFilters, idxCol)
			var foundRange bool
			if !ok {
				// Also allow a limited form of range condition filters.
				allIdx, foundRange = c.findJoinFilterRange(allFilters, idxCol)
				if !foundRange {
					break
				}
			}

			if len(foundVals) > 1 {
				if joinType == opt.LeftJoinOp || joinType == opt.AntiJoinOp {
					// We cannot use the method constructJoinWithConstants to create a cross
					// join for left or anti joins, because constructing a cross join with
					// foundVals will increase the size of the input. As a result,
					// non-matching input rows will show up more than once in the output,
					// which is incorrect (see #59615).
					shouldBuildMultiSpanLookupJoin = true
					break
				}
				if j == 0 && projectedVirtualCols.Empty() && index.PartitionCount() > 1 {
					// If this is the first index column and there is more than one
					// partition, we may be able to build a locality optimized lookup
					// join. This requires a multi-span lookup join as a starting point.
					// See GenerateLocalityOptimizedLookupJoin for details.
					//
					// Note that we do not currently support locality optimized
					// lookup joins for indexes on virtual columns.
					shouldBuildMultiSpanLookupJoin = true
					break
				}
			}

			if foundRange {
				shouldBuildMultiSpanLookupJoin = true
				break
			}

			// We will join these constant values with the input to make
			// equality columns for the lookup join.
			if constFilters == nil {
				constFilters = make(memo.FiltersExpr, 0, numIndexKeyCols-j)
			}

			idxColType := c.e.f.Metadata().ColumnMeta(idxCol).Type
			constColAlias := fmt.Sprintf("lookup_join_const_col_@%d", idxCol)
			join, constColID := c.constructJoinWithConstants(
				lookupJoin.Input,
				foundVals,
				idxColType,
				constColAlias,
			)

			lookupJoin.Input = join
			lookupJoin.KeyCols = append(lookupJoin.KeyCols, constColID)
			rightSideCols = append(rightSideCols, idxCol)
			constFilters = append(constFilters, allFilters[allIdx])
		}

		if shouldBuildMultiSpanLookupJoin {
			// Some of the index columns were constrained to multiple constant values
			// or a range expression, and we did not use the method
			// constructJoinWithConstants to create a cross join as the input (either
			// because it would have been incorrect or because it would have
			// eliminated the opportunity to apply other optimizations such as
			// locality optimized search; see above).
			//
			// As an alternative, we store all the filters needed for the lookup in
			// LookupExpr, which will be used to construct spans at execution time.
			// The result is that each input row will generate multiple spans to
			// lookup in the index. For example, if the index cols are (region, id)
			// and the LookupExpr is `region in ('east', 'west') AND id = input.id`,
			// each input row will generate two spans to be scanned in the lookup:
			//   [/'east'/<id> - /'east'/<id>] [/'west'/<id> - /'west'/<id>]
			// where <id> is the value of input.id for the current input row.
			var eqFilters memo.FiltersExpr
			extractEqualityFilter := func(leftCol, rightCol opt.ColumnID) memo.FiltersItem {
				return memo.ExtractJoinEqualityFilter(
					leftCol, rightCol, inputProps.OutputCols, rightCols, on,
				)
			}
			eqFilters, constFilters, rightSideCols = c.findFiltersForIndexLookup(
				allFilters, scanPrivate.Table, index, leftEq, rightEq, extractEqualityFilter,
			)
			lookupJoin.LookupExpr = append(eqFilters, constFilters...)

			// Reset KeyCols since we're not using it anymore.
			lookupJoin.KeyCols = opt.ColList{}
		}

		if len(lookupJoin.KeyCols) == 0 && len(lookupJoin.LookupExpr) == 0 {
			// We couldn't find equality columns which we can lookup.
			return
		}

		// Wrap the input in a Project if any projections are required. The
		// lookup join will project away these synthesized columns.
		if len(inputProjections) > 0 {
			lookupJoin.Input = c.e.f.ConstructProject(
				lookupJoin.Input,
				inputProjections,
				lookupJoin.Input.Relational().OutputCols,
			)
		}

		tableFDs := memo.MakeTableFuncDep(md, scanPrivate.Table)
		// A lookup join will drop any input row which contains NULLs, so a lax key
		// is sufficient.
		lookupJoin.LookupColsAreTableKey = tableFDs.ColsAreLaxKey(rightSideCols.ToSet())

		// Remove redundant filters from the ON condition if columns were
		// constrained by equality filters or constant filters.
		lookupJoin.On = onFilters
		if len(lookupJoin.KeyCols) > 0 {
			lookupJoin.On = memo.ExtractRemainingJoinFilters(lookupJoin.On, lookupJoin.KeyCols, rightSideCols)
		}
		lookupJoin.On = lookupJoin.On.Difference(lookupJoin.LookupExpr)
		lookupJoin.On = lookupJoin.On.Difference(constFilters)
		lookupJoin.ConstFilters = constFilters

		// Add input columns and lookup expression columns, since these will be
		// needed for all join types and cases.
		lookupJoin.Cols = lookupJoin.LookupExpr.OuterCols()
		lookupJoin.Cols.UnionWith(inputProps.OutputCols)

		// At this point the filter may have been reduced by partial index
		// predicate implication and by removing parts of the filter that are
		// represented by the key columns. If there are any outer columns of the
		// filter that are not output columns of the right side of the join, we
		// skip this index.
		//
		// This is possible when GenerateLookupJoinsWithVirtualColsAndFilter
		// matches an expression on the right side of the join in the form
		// (Project (Select (Scan))). The Select's filters may reference columns
		// that are not passed through in the Project.
		//
		// TODO(mgartner): We could handle this by wrapping the lookup join in
		// an index join to fetch these columns and filter by them, then
		// wrapping the index join in a project that removes the columns.
		filterColsFromRight := lookupJoin.On.OuterCols().Difference(inputProps.OutputCols)
		if !filterColsFromRight.SubsetOf(rightCols) {
			return
		}

		isCovering := rightCols.SubsetOf(indexCols)
		if isCovering {
			// Case 1 (see function comment).
			lookupJoin.Cols.UnionWith(rightCols)

			// If some optional filters were used to build the lookup expression, we
			// may need to wrap the final expression with a project. We don't need to
			// do this for semi or anti joins, since they have an implicit projection
			// that removes all right-side columns.
			needsProject := joinType != opt.SemiJoinOp && joinType != opt.AntiJoinOp &&
				!lookupJoin.Cols.SubsetOf(grp.Relational().OutputCols)
			if !needsProject {
				c.e.mem.AddLookupJoinToGroup(&lookupJoin, grp)
				return
			}
			var project memo.ProjectExpr
			project.Input = c.e.f.ConstructLookupJoin(
				lookupJoin.Input,
				lookupJoin.On,
				&lookupJoin.LookupJoinPrivate,
			)
			project.Passthrough = grp.Relational().OutputCols
			c.e.mem.AddProjectToGroup(&project, grp)
			return
		}

		_, isPartial := index.Predicate()
		if isPartial && (joinType == opt.SemiJoinOp || joinType == opt.AntiJoinOp) {
			// Typically, the index must cover all columns from the right in
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
			filterColsFromRight := rightCols.Intersection(onFilters.OuterCols())
			if filterColsFromRight.SubsetOf(indexCols) {
				lookupJoin.Cols.UnionWith(filterColsFromRight)
				c.e.mem.AddLookupJoinToGroup(&lookupJoin, grp)
				return
			}
		}

		// All code that follows is for cases 2 and 3 (see function comment).
		// We need to generate two joins: a lower join followed by an upper join.
		// In case 3, this lower-upper pair of joins is further specialized into
		// paired-joins where we refer to the lower as first and upper as second.

		if scanPrivate.Flags.NoIndexJoin {
			return
		}
		pairedJoins := false
		continuationCol := opt.ColumnID(0)
		lowerJoinType := joinType
		if joinType == opt.SemiJoinOp {
			// Case 3: Semi joins are converted to a pair consisting of an inner
			// lookup join and semi lookup join.
			pairedJoins = true
			lowerJoinType = opt.InnerJoinOp
		} else if joinType == opt.AntiJoinOp {
			// Case 3: Anti joins are converted to a pair consisting of a left
			// lookup join and anti lookup join.
			pairedJoins = true
			lowerJoinType = opt.LeftJoinOp
		}

		if pkCols == nil {
			pkCols = c.getPkCols(scanPrivate.Table)
		}

		// The lower LookupJoin must return all PK columns (they are needed as key
		// columns for the index join).
		lookupJoin.Cols.UnionWith(rightCols.Intersection(indexCols))
		for i := range pkCols {
			lookupJoin.Cols.Add(pkCols[i])
		}

		var indexJoin memo.LookupJoinExpr

		// onCols are the columns that the ON condition in the (lower) lookup join
		// can refer to: input columns, or columns available in the index.
		onCols := indexCols.Union(inputProps.OutputCols)
		if c.FiltersBoundBy(lookupJoin.On, onCols) {
			// Case 2.
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
			// This works in a straightforward manner for InnerJoin but not for
			// LeftJoin because of a technicality: if an input (left) row has
			// matches in the lower LookupJoin but has no matches in the index join,
			// only the columns looked up by the top index join get NULL-extended.
			// Additionally if none of the lower matches are matches in the index
			// join, we want to output only one NULL-extended row. To accomplish
			// this, we need to use paired-joins.
			if joinType == opt.LeftJoinOp {
				// Case 3.
				pairedJoins = true
				// The lowerJoinType continues to be LeftJoinOp.
			}
			// We have already set pairedJoins=true for SemiJoin, AntiJoin earlier,
			// and we don't need to do that for InnerJoin. The following sets up the
			// ON conditions for both Case 2 and Case 3, when doing 2 joins that
			// will each evaluate part of the ON condition.
			conditions := lookupJoin.On
			lookupJoin.On = c.ExtractBoundConditions(conditions, onCols)
			indexJoin.On = c.ExtractUnboundConditions(conditions, onCols)
		}
		if pairedJoins {
			lookupJoin.JoinType = lowerJoinType
			continuationCol = c.constructContinuationColumnForPairedJoin()
			lookupJoin.IsFirstJoinInPairedJoiner = true
			lookupJoin.ContinuationCol = continuationCol
			lookupJoin.Cols.Add(continuationCol)
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
		indexJoin.Cols = rightCols.Union(inputProps.OutputCols)
		indexJoin.LookupColsAreTableKey = true
		if pairedJoins {
			indexJoin.IsSecondJoinInPairedJoiner = true
		}

		// If this is a semi- or anti-join, ensure the columns do not include any
		// unneeded right-side columns.
		if joinType == opt.SemiJoinOp || joinType == opt.AntiJoinOp {
			indexJoin.Cols = inputProps.OutputCols.Union(indexJoin.On.OuterCols())
		}

		// Create the LookupJoin for the index join in the same group.
		c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
	})
}

// findFiltersForIndexLookup finds the equality and constant filters in
// filters that can be used to constrain the given index.
func (c *CustomFuncs) findFiltersForIndexLookup(
	filters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
	leftEq, rightEq opt.ColList,
	extractEqualityFilter func(opt.ColumnID, opt.ColumnID) memo.FiltersItem,
) (eqFilters, constFilters memo.FiltersExpr, rightSideCols opt.ColList) {
	numIndexKeyCols := index.LaxKeyColumnCount()

	eqFilters = make(memo.FiltersExpr, 0, len(filters))
	rightSideCols = make(opt.ColList, 0, len(filters))

	// All the lookup conditions must apply to the prefix of the index.
	for j := 0; j < numIndexKeyCols; j++ {
		idxCol := tabID.IndexColumnID(index, j)
		if eqIdx, ok := rightEq.Find(idxCol); ok {
			eqFilter := extractEqualityFilter(leftEq[eqIdx], rightEq[eqIdx])
			eqFilters = append(eqFilters, eqFilter)
			rightSideCols = append(rightSideCols, idxCol)
			continue
		}

		var foundRange bool
		// Try to find a filter that constrains this column to non-NULL
		// constant values. We cannot use a NULL value because the lookup
		// join implements logic equivalent to simple equality between
		// columns (where NULL never equals anything).
		values, allIdx, ok := c.findJoinFilterConstants(filters, idxCol)
		if !ok {
			// If there's no const filters look for an inequality range.
			allIdx, foundRange = c.findJoinFilterRange(filters, idxCol)
			if !foundRange {
				break
			}
		}

		if constFilters == nil {
			constFilters = make(memo.FiltersExpr, 0, numIndexKeyCols-j)
		}

		// Ensure that the constant filter is an equality, IN or inequality
		// expression.   These are the only types of expressions currently supported
		// by the lookupJoiner for building lookup spans.
		constFilter := filters[allIdx]
		if !c.isCanonicalLookupJoinFilter(constFilter) {
			if len(values) > 0 {
				constFilter = c.makeConstFilter(idxCol, values)
			} else if foundRange {
				constFilter = c.makeRangeFilter(idxCol, constFilter)
			}
		}
		constFilters = append(constFilters, constFilter)

		// Generating additional columns after a range isn't helpful so stop here.
		if foundRange {
			break
		}
	}

	if len(eqFilters) == 0 {
		// We couldn't find equality columns which we can lookup.
		return nil, nil, nil
	}

	return eqFilters, constFilters, rightSideCols
}

// isCanonicalLookupJoinFilter returns true for the limited set of expr's that are
// supported by the lookup joiner at execution time.
func (c *CustomFuncs) isCanonicalLookupJoinFilter(filter memo.FiltersItem) bool {
	var checkExpr func(expr opt.Expr) bool
	checkExpr = func(expr opt.Expr) bool {
		switch t := expr.(type) {
		case *memo.RangeExpr:
			return checkExpr(t.And)
		case *memo.AndExpr:
			return checkExpr(t.Left) && checkExpr(t.Right)
		case *memo.GeExpr:
			return checkExpr(t.Left) && checkExpr(t.Right)
		case *memo.GtExpr:
			return checkExpr(t.Left) && checkExpr(t.Right)
		case *memo.LeExpr:
			return checkExpr(t.Left) && checkExpr(t.Right)
		case *memo.LtExpr:
			return checkExpr(t.Left) && checkExpr(t.Right)
		case *memo.VariableExpr:
			return true
		case *memo.EqExpr:
			return checkExpr(t.Left) && checkExpr(t.Right)
		case *memo.InExpr:
			return checkExpr(t.Left) && memo.CanExtractConstTuple(t.Right)
		}
		return opt.IsConstValueOp(expr)
	}
	return checkExpr(filter.Condition)
}

// makeConstFilter builds a filter that constrains the given column to the given
// set of constant values. This is performed by either constructing an equality
// expression or an IN expression.
func (c *CustomFuncs) makeConstFilter(col opt.ColumnID, values tree.Datums) memo.FiltersItem {
	if len(values) == 1 {
		return c.e.f.ConstructFiltersItem(c.e.f.ConstructEq(
			c.e.f.ConstructVariable(col),
			c.e.f.ConstructConstVal(values[0], values[0].ResolvedType()),
		))
	}
	elems := make(memo.ScalarListExpr, len(values))
	elemTypes := make([]*types.T, len(values))
	for i := range values {
		typ := values[i].ResolvedType()
		elems[i] = c.e.f.ConstructConstVal(values[i], typ)
		elemTypes[i] = typ
	}
	return c.e.f.ConstructFiltersItem(c.e.f.ConstructIn(
		c.e.f.ConstructVariable(col),
		c.e.f.ConstructTuple(elems, types.MakeTuple(elemTypes)),
	))
}

// makeRangeFilter builds a filter from a constrained column, we assume the
// column is constrained by at least 1 tight constraint. This code doesn't
// handle descending columns.
func (c *CustomFuncs) makeRangeFilter(col opt.ColumnID, filter memo.FiltersItem) memo.FiltersItem {
	props := filter.ScalarProps()
	if props.Constraints.Length() == 0 ||
		props.Constraints.Constraint(0).Spans.Count() != 1 ||
		props.Constraints.Constraint(0).Columns.Get(0).Descending() {
		panic(errors.AssertionFailedf("makeRangeFilter needs at least one ascending constraint with one span"))
	}
	span := props.Constraints.Constraint(0).Spans.Get(0)
	return c.makeRangeFilterFromSpan(col, span)
}

// makeRangeFilterFromSpan constructs a filter from a constraint.Span.
func (c *CustomFuncs) makeRangeFilterFromSpan(
	col opt.ColumnID, span *constraint.Span,
) memo.FiltersItem {
	variable := c.e.f.ConstructVariable(col)
	var left, right opt.ScalarExpr

	// Here and below we need to check for IsEmpty and IsNull because sometimes
	// Null is used for unbounded spans.  Found empirically by forcing
	// findFiltersForIndexLookup to always wrap the filters with makeRangeFilter.
	if !span.StartKey().IsEmpty() && !span.StartKey().IsNull() {
		val := span.StartKey().Value(0)
		if span.StartBoundary() == constraint.IncludeBoundary {
			left = c.e.f.ConstructGe(variable, c.e.f.ConstructConstVal(val, val.ResolvedType()))
		} else {
			left = c.e.f.ConstructGt(variable, c.e.f.ConstructConstVal(val, val.ResolvedType()))
		}
	}

	if !span.EndKey().IsEmpty() && !span.EndKey().IsNull() {
		val := span.EndKey().Value(0)
		if span.EndBoundary() == constraint.IncludeBoundary {
			right = c.e.f.ConstructLe(variable, c.e.f.ConstructConstVal(val, val.ResolvedType()))
		} else {
			right = c.e.f.ConstructLt(variable, c.e.f.ConstructConstVal(val, val.ResolvedType()))
		}
	}

	if left != nil && right != nil {
		return c.e.f.ConstructFiltersItem(c.e.f.ConstructRange(c.e.f.ConstructAnd(right, left)))
	} else if left != nil {
		return c.e.f.ConstructFiltersItem(left)
	} else if right != nil {
		return c.e.f.ConstructFiltersItem(right)
	}

	panic(errors.AssertionFailedf("Constraint needs a valid start or end key"))
}

// constructContinuationColumnForPairedJoin constructs a continuation column
// ID for the paired-joiners used for left outer/semi/anti joins when the
// first join generates false positives (due to an inverted index or
// non-covering index). The first join will be either a left outer join or
// an inner join.
func (c *CustomFuncs) constructContinuationColumnForPairedJoin() opt.ColumnID {
	return c.e.f.Metadata().AddColumn("continuation", c.BoolType())
}

// GenerateInvertedJoins is similar to GenerateLookupJoins, but instead
// of generating lookup joins with regular indexes, it generates lookup joins
// with inverted indexes. Similar to GenerateLookupJoins, there are two cases
// depending on whether or not the index is covering. See the comment above
// GenerateLookupJoins for details.
func (c *CustomFuncs) GenerateInvertedJoins(
	grp memo.RelExpr,
	joinType opt.Operator,
	input memo.RelExpr,
	scanPrivate *memo.ScanPrivate,
	on memo.FiltersExpr,
	joinPrivate *memo.JoinPrivate,
) {
	if joinPrivate.Flags.Has(memo.DisallowInvertedJoinIntoRight) {
		return
	}

	inputCols := input.Relational().OutputCols
	var pkCols opt.ColList
	var newScanPrivate *memo.ScanPrivate

	eqColsAndOptionalFiltersCalculated := false
	var leftEqCols opt.ColList
	var rightEqCols opt.ColList
	var optionalFilters memo.FiltersExpr

	var iter scanIndexIter
	iter.Init(c.e.evalCtx, c.e.f, c.e.mem, &c.im, scanPrivate, on, rejectNonInvertedIndexes)
	iter.ForEach(func(index cat.Index, onFilters memo.FiltersExpr, indexCols opt.ColSet, _ bool, _ memo.ProjectionsExpr) {
		invertedJoin := memo.InvertedJoinExpr{Input: input}
		numPrefixCols := index.NonInvertedPrefixColumnCount()

		var allFilters memo.FiltersExpr
		if numPrefixCols > 0 {
			// Only calculate the left and right equality columns and optional
			// filters if there is a multi-column inverted index.
			if !eqColsAndOptionalFiltersCalculated {
				inputProps := input.Relational()
				leftEqCols, rightEqCols = memo.ExtractJoinEqualityColumns(inputProps.OutputCols, scanPrivate.Cols, onFilters)

				// Generate implicit filters from CHECK constraints and computed
				// columns as optional filters. We build the computed column
				// optional filters from the original on filters, not the
				// filters within the context of the iter.ForEach callback. The
				// latter may be reduced during partial index implication and
				// using them here would result in a reduced set of optional
				// filters.
				optionalFilters = c.checkConstraintFilters(scanPrivate.Table)
				computedColFilters := c.computedColFilters(scanPrivate, on, optionalFilters)
				optionalFilters = append(optionalFilters, computedColFilters...)

				eqColsAndOptionalFiltersCalculated = true
			}

			// Combine the ON filters and optional filters together. This set of
			// filters will be used to attempt to constrain non-inverted prefix
			// columns of the multi-column inverted index.
			allFilters = append(onFilters, optionalFilters...)
		}

		// The non-inverted prefix columns of a multi-column inverted index must
		// be constrained in order to perform an inverted join. We attempt to
		// constrain each prefix column to non-ranging constant values. These
		// values are joined with the input to create key columns for the
		// InvertedJoin, similar to GenerateLookupJoins.
		var constFilters memo.FiltersExpr
		var rightSideCols opt.ColList
		for i := 0; i < numPrefixCols; i++ {
			prefixCol := scanPrivate.Table.IndexColumnID(index, i)

			// Check if prefixCol is constrained by an equality constraint.
			if eqIdx, ok := rightEqCols.Find(prefixCol); ok {
				invertedJoin.PrefixKeyCols = append(invertedJoin.PrefixKeyCols, leftEqCols[eqIdx])
				rightSideCols = append(rightSideCols, prefixCol)
				continue
			}

			// Try to constrain prefixCol to constant, non-ranging values.
			foundVals, allIdx, ok := c.findJoinFilterConstants(allFilters, prefixCol)
			if !ok {
				// Cannot constrain prefix column and therefore cannot generate
				// an inverted join.
				return
			}

			if len(foundVals) > 1 && (joinType == opt.LeftJoinOp || joinType == opt.AntiJoinOp) {
				// We cannot create an inverted join in this case, because constructing
				// a cross join with foundVals will increase the size of the input. As a
				// result, non-matching input rows will show up more than once in the
				// output, which is incorrect (see #59615).
				// TODO(rytaft,mgartner): find a way to create an inverted join for this
				// case.
				return
			}

			// We will join these constant values with the input to make
			// equality columns for the inverted join.
			if constFilters == nil {
				constFilters = make(memo.FiltersExpr, 0, numPrefixCols)
			}

			prefixColType := c.e.f.Metadata().ColumnMeta(prefixCol).Type
			constColAlias := fmt.Sprintf("inverted_join_const_col_@%d", prefixCol)
			join, constColID := c.constructJoinWithConstants(
				invertedJoin.Input,
				foundVals,
				prefixColType,
				constColAlias,
			)

			invertedJoin.Input = join
			invertedJoin.PrefixKeyCols = append(invertedJoin.PrefixKeyCols, constColID)
			constFilters = append(constFilters, allFilters[allIdx])
			rightSideCols = append(rightSideCols, prefixCol)
		}

		// Remove redundant filters from the ON condition if non-inverted prefix
		// columns were constrained by equality filters or constant filters.
		onFilters = memo.ExtractRemainingJoinFilters(onFilters, invertedJoin.PrefixKeyCols, rightSideCols)
		onFilters = onFilters.Difference(constFilters)
		invertedJoin.ConstFilters = constFilters

		// Check whether the filter can constrain the inverted column.
		invertedExpr := invertedidx.TryJoinInvertedIndex(
			c.e.evalCtx.Context, c.e.f, onFilters, scanPrivate.Table, index, inputCols,
		)
		if invertedExpr == nil {
			return
		}

		// All geospatial and JSON inverted joins that are currently supported
		// are not covering, so we must wrap them in an index join.
		// TODO(rytaft): Avoid adding an index join if possible for Array
		// inverted joins.
		if scanPrivate.Flags.NoIndexJoin {
			return
		}

		if pkCols == nil {
			pkCols = c.getPkCols(scanPrivate.Table)
		}

		// Though the index is marked as containing the column being indexed, it
		// doesn't actually, and it is only valid to extract the primary key
		// columns and non-inverted prefix columns from it.
		indexCols = pkCols.ToSet()
		for i, n := 0, index.NonInvertedPrefixColumnCount(); i < n; i++ {
			prefixCol := scanPrivate.Table.IndexColumnID(index, i)
			indexCols.Add(prefixCol)
		}

		// Create a new ScanPrivate, which will be used below for the inverted join.
		// Note: this must happen before the continuation column is created to ensure
		// that the continuation column will have the highest column ID.
		//
		// See the comment where this newScanPrivate is used below in mapInvertedJoin
		// for details about why it's needed.
		if newScanPrivate == nil {
			newScanPrivate = c.DuplicateScanPrivate(scanPrivate)
		}

		continuationCol := opt.ColumnID(0)
		invertedJoinType := joinType
		// Anti joins are converted to a pair consisting of a left inverted join
		// and anti lookup join.
		if joinType == opt.LeftJoinOp || joinType == opt.AntiJoinOp {
			continuationCol = c.constructContinuationColumnForPairedJoin()
			invertedJoinType = opt.LeftJoinOp
		} else if joinType == opt.SemiJoinOp {
			// Semi joins are converted to a pair consisting of an inner inverted
			// join and semi lookup join.
			continuationCol = c.constructContinuationColumnForPairedJoin()
			invertedJoinType = opt.InnerJoinOp
		}
		invertedJoin.JoinPrivate = *joinPrivate
		invertedJoin.JoinType = invertedJoinType
		invertedJoin.Table = scanPrivate.Table
		invertedJoin.Index = index.Ordinal()
		invertedJoin.InvertedExpr = invertedExpr
		invertedJoin.Cols = indexCols.Union(inputCols)
		if continuationCol != 0 {
			invertedJoin.Cols.Add(continuationCol)
			invertedJoin.IsFirstJoinInPairedJoiner = true
			invertedJoin.ContinuationCol = continuationCol
		}

		var indexJoin memo.LookupJoinExpr

		// ON may have some conditions that are bound by the columns in the index
		// and some conditions that refer to other columns. We can put the former
		// in the InvertedJoin and the latter in the index join.
		invertedJoin.On = c.ExtractBoundConditions(onFilters, invertedJoin.Cols)
		indexJoin.On = c.ExtractUnboundConditions(onFilters, invertedJoin.Cols)

		// Map the inverted join to use the new table and column IDs from the
		// newScanPrivate created above. We want to make sure that the column IDs
		// returned by the inverted join are different from the IDs that will be
		// returned by the top level index join.
		//
		// In addition to avoiding subtle bugs in the optimizer when the same
		// column ID is reused, this mapping is also essential for correct behavior
		// at execution time in the case of a left paired join. This is because a
		// row that matches in the first left join (the inverted join) might be a
		// false positive and fail to match in the second left join (the lookup
		// join). If an original left row has no matches after the second left join,
		// it must appear as a null-extended row with all right-hand columns null.
		// If one of the right-hand columns comes from the inverted join, however,
		// it might incorrectly show up as non-null (see #58892).
		c.mapInvertedJoin(&invertedJoin, indexCols, newScanPrivate)

		indexJoin.Input = c.e.f.ConstructInvertedJoin(
			invertedJoin.Input,
			invertedJoin.On,
			&invertedJoin.InvertedJoinPrivate,
		)
		indexJoin.JoinType = joinType
		indexJoin.Table = scanPrivate.Table
		indexJoin.Index = cat.PrimaryIndex
		indexJoin.KeyCols = c.getPkCols(invertedJoin.Table)
		indexJoin.Cols = scanPrivate.Cols.Union(inputCols)
		indexJoin.LookupColsAreTableKey = true
		if continuationCol != 0 {
			indexJoin.IsSecondJoinInPairedJoiner = true
		}

		// If this is a semi- or anti-join, ensure the columns do not include any
		// unneeded right-side columns.
		if joinType == opt.SemiJoinOp || joinType == opt.AntiJoinOp {
			indexJoin.Cols = inputCols.Union(indexJoin.On.OuterCols())
		}

		// Create the LookupJoin for the index join in the same group.
		c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
	})
}

// getPkCols gets the primary key columns for the given table as a ColList.
func (c *CustomFuncs) getPkCols(tabID opt.TableID) opt.ColList {
	tab := c.e.mem.Metadata().Table(tabID)
	pkIndex := tab.Index(cat.PrimaryIndex)
	pkCols := make(opt.ColList, pkIndex.KeyColumnCount())
	for i := range pkCols {
		pkCols[i] = tabID.IndexColumnID(pkIndex, i)
	}
	return pkCols
}

// mapInvertedJoin maps the given inverted join to use the table and columns
// provided in newScanPrivate. The inverted join is modified in place. indexCols
// contains the pre-calculated index columns used by the given invertedJoin.
//
// Note that columns from the input are not mapped. For example, PrefixKeyCols
// does not need to be mapped below since it only contains input columns.
func (c *CustomFuncs) mapInvertedJoin(
	invertedJoin *memo.InvertedJoinExpr, indexCols opt.ColSet, newScanPrivate *memo.ScanPrivate,
) {
	tabID := invertedJoin.Table
	newTabID := newScanPrivate.Table

	// Get the catalog index (same for both new and old tables).
	index := c.e.mem.Metadata().TableMeta(tabID).Table.Index(invertedJoin.Index)

	// Though the index is marked as containing the column being indexed, it
	// doesn't actually, and it is only valid to extract the primary key
	// columns and non-inverted prefix columns from it.
	newPkCols := c.getPkCols(newTabID)
	newIndexCols := newPkCols.ToSet()
	for i, n := 0, index.NonInvertedPrefixColumnCount(); i < n; i++ {
		prefixCol := newTabID.IndexColumnID(index, i)
		newIndexCols.Add(prefixCol)
	}

	// Get the source and destination ColSets, including the inverted source
	// columns, which will be used in the invertedExpr.
	srcCols := indexCols.Copy()
	dstCols := newIndexCols.Copy()
	ord := index.InvertedColumn().InvertedSourceColumnOrdinal()
	invertedSourceCol := tabID.ColumnID(ord)
	newInvertedSourceCol := newTabID.ColumnID(ord)
	srcCols.Add(invertedSourceCol)
	dstCols.Add(newInvertedSourceCol)

	invertedJoin.Table = newTabID
	invertedJoin.InvertedExpr = c.mapScalarExprCols(invertedJoin.InvertedExpr, srcCols, dstCols)
	invertedJoin.Cols = invertedJoin.Cols.Difference(indexCols).Union(newIndexCols)
	invertedJoin.ConstFilters = c.MapFilterCols(invertedJoin.ConstFilters, srcCols, dstCols)
	invertedJoin.On = c.MapFilterCols(invertedJoin.On, srcCols, dstCols)
}

// findComputedColJoinEquality returns the computed column expression of col and
// ok=true when a join equality constraint can be generated for the column. This
// is possible when:
//
//   1. col is non-nullable.
//   2. col is a computed column.
//   3. Columns referenced in the computed expression are a subset of columns
//      that already have equality constraints.
//
// For example, consider the table and query:
//
//   CREATE TABLE a (
//     a INT
//   )
//
//   CREATE TABLE bc (
//     b INT,
//     c INT NOT NULL AS (b + 1) STORED
//   )
//
//   SELECT * FROM a JOIN b ON a = b
//
// We can add an equality constraint for c because c is a function of b and b
// has an equality constraint in the join predicate:
//
//   SELECT * FROM a JOIN b ON a = b AND a + 1 = c
//
// Condition (1) is required to prevent generating invalid equality constraints
// for computed column expressions that can evaluate to NULL even when the
// columns referenced in the expression are non-NULL. For example, consider the
// table and query:
//
//   CREATE TABLE a (
//     a INT
//   )
//
//   CREATE TABLE bc (
//     b INT,
//     c INT AS (CASE WHEN b > 0 THEN NULL ELSE -1 END) STORED
//   )
//
//   SELECT a, b FROM a JOIN b ON a = b
//
// The following is an invalid transformation: a row such as (a=1, b=1) would no
// longer be returned because NULL=NULL is false.
//
//   SELECT a, b FROM a JOIN b ON a = b AND (CASE WHEN a > 0 THEN NULL ELSE -1 END) = c
//
// TODO(mgartner): We can relax condition (1) to allow nullable columns if it
// can be proven that the expression will never evaluate to NULL. We can use
// memo.ExprIsNeverNull to determine this, passing both NOT NULL and equality
// columns as notNullCols.
func (c *CustomFuncs) findComputedColJoinEquality(
	tabID opt.TableID, col opt.ColumnID, eqCols opt.ColSet,
) (_ opt.ScalarExpr, ok bool) {
	tabMeta := c.e.mem.Metadata().TableMeta(tabID)
	tab := c.e.mem.Metadata().Table(tabID)
	if tab.Column(tabID.ColumnOrdinal(col)).IsNullable() {
		return nil, false
	}
	expr, ok := tabMeta.ComputedColExpr(col)
	if !ok {
		return nil, false
	}
	if !c.OuterCols(expr).SubsetOf(eqCols) {
		return nil, false
	}
	return expr, true
}

// findJoinFilterConstants tries to find a filter that is exactly equivalent to
// constraining the given column to a constant value or a set of constant
// values. If successful, the constant values and the index of the constraining
// FiltersItem are returned. If multiple filters match, the one that minimizes
// the number of returned values is chosen. Note that the returned constant
// values do not contain NULL.
func (c *CustomFuncs) findJoinFilterConstants(
	filters memo.FiltersExpr, col opt.ColumnID,
) (values tree.Datums, filterIdx int, ok bool) {
	var bestValues tree.Datums
	var bestFilterIdx int
	for filterIdx := range filters {
		props := filters[filterIdx].ScalarProps()
		if props.TightConstraints {
			constCol, constVals, ok := props.Constraints.HasSingleColumnConstValues(c.e.evalCtx)
			if !ok || constCol != col {
				continue
			}
			hasNull := false
			for i := range constVals {
				if constVals[i] == tree.DNull {
					hasNull = true
					break
				}
			}
			if !hasNull && (bestValues == nil || len(bestValues) > len(constVals)) {
				bestValues = constVals
				bestFilterIdx = filterIdx
			}
		}
	}
	if bestValues == nil {
		return nil, -1, false
	}
	return bestValues, bestFilterIdx, true
}

// findJoinFilterRange tries to find an inequality range for this column.
func (c *CustomFuncs) findJoinFilterRange(
	filters memo.FiltersExpr, col opt.ColumnID,
) (filterIdx int, ok bool) {
	for filterIdx := range filters {
		props := filters[filterIdx].ScalarProps()
		if props.TightConstraints && props.Constraints.Length() > 0 {
			constraint := props.Constraints.Constraint(0)
			constraintCol := constraint.Columns.Get(0).ID()
			// See comment in findFiltersForIndexLookup for why we check filter here.
			// We only support 1 span in the execution engine so check that.
			if constraintCol != col ||
				constraint.Spans.Count() != 1 ||
				!c.isCanonicalLookupJoinFilter(filters[filterIdx]) {
				continue
			}
			return filterIdx, true
		}
	}
	return 0, false
}

// constructJoinWithConstants constructs a cross join that joins every row in
// the input with every value in vals. The cross join will be converted into a
// projection by inlining normalization rules if vals contains only a single
// value. The constructed expression and the column ID of the constant value
// column are returned.
func (c *CustomFuncs) constructJoinWithConstants(
	input memo.RelExpr, vals tree.Datums, typ *types.T, columnAlias string,
) (join memo.RelExpr, constColID opt.ColumnID) {
	constColID = c.e.f.Metadata().AddColumn(columnAlias, typ)
	tupleType := types.MakeTuple([]*types.T{typ})

	constRows := make(memo.ScalarListExpr, len(vals))
	for i := range constRows {
		constRows[i] = c.e.f.ConstructTuple(
			memo.ScalarListExpr{c.e.f.ConstructConstVal(vals[i], typ)},
			tupleType,
		)
	}

	values := c.e.f.ConstructValues(
		constRows,
		&memo.ValuesPrivate{
			Cols: opt.ColList{constColID},
			ID:   c.e.mem.Metadata().NextUniqueID(),
		},
	)

	// We purposefully do not propagate any join flags into this JoinPrivate. If
	// a LOOKUP join hint was propagated to this cross join, the cost of the
	// cross join would be artificially inflated and the lookup join would not
	// be selected as the optimal plan.
	join = c.e.f.ConstructInnerJoin(input, values, nil /* on */, &memo.JoinPrivate{})
	return join, constColID
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

// HasVolatileProjection returns true if any of the projection items of the
// ProjectionsExpr contains a volatile expression.
func (c *CustomFuncs) HasVolatileProjection(projections memo.ProjectionsExpr) bool {
	for i := range projections {
		if projections[i].ScalarProps().VolatilitySet.HasVolatile() {
			return true
		}
	}
	return false
}

// FindLeftJoinCanaryColumn tries to find a "canary" column from the right input
// of a left join. This is a column that is NULL in the join output iff the row
// is an "outer left" row that had no match in the join.
//
// Returns 0 if we couldn't find such a column.
func (c *CustomFuncs) FindLeftJoinCanaryColumn(
	right memo.RelExpr, on memo.FiltersExpr,
) opt.ColumnID {
	canaryCol, ok := right.Relational().NotNullCols.Next(0)
	if ok {
		// The right expression has a non-null column; we can use it as a canary
		// column.
		return canaryCol
	}

	// Find any column from the right which is null-rejected by the ON condition.
	// right rows where such a column is NULL will never contribute to the join
	// result.
	nullRejectedCols := memo.NullColsRejectedByFilter(c.e.evalCtx, on)
	nullRejectedCols.IntersectionWith(right.Relational().OutputCols)

	canaryCol, ok = nullRejectedCols.Next(0)
	if ok {
		return canaryCol
	}

	return 0
}

// FoundCanaryColumn returns true if the given column returned by
// FindLeftJoinCanaryColum indicates that we found a canary column.
func (c *CustomFuncs) FoundCanaryColumn(canaryCol opt.ColumnID) bool {
	return canaryCol != 0
}

// MakeProjectionsForOuterJoin takes a set of projections and wraps them in a
// conditional which overrides them to NULL whenever the canary column is NULL.
// TODO(radu): detect projections that evaluate to NULL on NULL inputs anyway,
// and leave those alone.
func (c *CustomFuncs) MakeProjectionsForOuterJoin(
	canaryCol opt.ColumnID, proj memo.ProjectionsExpr,
) memo.ProjectionsExpr {
	result := make(memo.ProjectionsExpr, len(proj))

	for i := range proj {
		// Construct "IF(canaryCol IS NULL, NULL, <projection>)".
		ifExpr := c.e.f.ConstructCase(
			c.e.f.ConstructIs(
				c.e.f.ConstructVariable(
					canaryCol,
				),
				memo.NullSingleton,
			),
			memo.ScalarListExpr{
				c.e.f.ConstructWhen(memo.TrueSingleton, c.e.f.ConstructNull(proj[i].Typ)),
			},
			proj[i].Element,
		)
		result[i] = c.e.f.ConstructProjectionsItem(ifExpr, proj[i].Col)
	}
	return result
}

// IsAntiJoin returns true if the given lookup join is an anti join.
func (c *CustomFuncs) IsAntiJoin(private *memo.LookupJoinPrivate) bool {
	return private.JoinType == opt.AntiJoinOp
}

// EmptyFiltersExpr returns an empty FiltersExpr.
func (c *CustomFuncs) EmptyFiltersExpr() memo.FiltersExpr {
	return memo.EmptyFiltersExpr
}

// CreateLocalityOptimizedLookupJoinPrivate creates a new lookup join private
// from the given private and replaces the LookupExpr and RemoteLookupExpr with
// the given filters. It also marks the private as locality optimized.
func (c *CustomFuncs) CreateLocalityOptimizedLookupJoinPrivate(
	lookupExpr, remoteLookupExpr memo.FiltersExpr, private *memo.LookupJoinPrivate,
) *memo.LookupJoinPrivate {
	newPrivate := *private
	newPrivate.LookupExpr = lookupExpr
	newPrivate.RemoteLookupExpr = remoteLookupExpr
	newPrivate.LocalityOptimized = true
	return &newPrivate
}

// GetLocalityOptimizedLookupJoinExprs returns the local and remote lookup
// expressions needed to build a locality optimized lookup join from the given
// ON conditions and lookup join private, if possible. Otherwise, it returns
// ok=false. See the comments above the GenerateLocalityOptimizedAntiJoin and
// GenerateLocalityOptimizedLookupJoin rules for more details.
func (c *CustomFuncs) GetLocalityOptimizedLookupJoinExprs(
	on memo.FiltersExpr, private *memo.LookupJoinPrivate,
) (localExpr memo.FiltersExpr, remoteExpr memo.FiltersExpr, ok bool) {
	// Respect the session setting LocalityOptimizedSearch.
	if !c.e.evalCtx.SessionData().LocalityOptimizedSearch {
		return nil, nil, false
	}

	// Check whether this lookup join has already been locality optimized.
	if private.LocalityOptimized {
		return nil, nil, false
	}

	// This lookup join cannot not be part of a paired join.
	if private.IsSecondJoinInPairedJoiner {
		return nil, nil, false
	}

	// This lookup join should have the LookupExpr filled in, indicating that one
	// or more of the join filters constrain an index column to multiple constant
	// values.
	if private.LookupExpr == nil {
		return nil, nil, false
	}

	// We can only generate a locality optimized lookup join if we know there is
	// at most one row produced for each lookup. This is always true for semi
	// joins if the ON condition is empty, but only true for inner and left joins
	// (and for semi joins with an ON condition) if private.LookupColsAreTableKey
	// is true.
	//
	// Locality optimized anti joins are implemented differently (see the
	// GenerateLocalityOptimizedAntiJoin rule), and therefore we can always
	// generate a locality optimized anti join regardless of the ON conditions or
	// value of private.LookupColsAreTableKey.
	if private.JoinType != opt.AntiJoinOp {
		if (private.JoinType != opt.SemiJoinOp || len(on) > 0) && !private.LookupColsAreTableKey {
			return nil, nil, false
		}
	}
	tabMeta := c.e.mem.Metadata().TableMeta(private.Table)
	index := tabMeta.Table.Index(private.Index)

	// The PrefixSorter has collected all the prefixes from all the different
	// partitions (remembering which ones came from local partitions), and has
	// sorted them so that longer prefixes come before shorter prefixes. For each
	// span in the scanConstraint, we will iterate through the list of prefixes
	// until we find a match, so ordering them with longer prefixes first ensures
	// that the correct match is found. The PrefixSorter is only non-nil when this
	// index has at least one local and one remote partition.
	var ps *partition.PrefixSorter
	if ps, ok = tabMeta.IndexPartitionLocality(private.Index, index, c.e.evalCtx); !ok {
		return nil, nil, false
	}

	// Find a filter that constrains the first column of the index.
	filterIdx, ok := c.getConstPrefixFilter(index, private.Table, private.LookupExpr)
	if !ok {
		return nil, nil, false
	}
	filter := private.LookupExpr[filterIdx]

	// Check whether the filter constrains the first column of the index
	// to at least two constant values. We need at least two values so that one
	// can target a local partition and one can target a remote partition.
	col, vals, ok := filter.ScalarProps().Constraints.HasSingleColumnConstValues(c.e.evalCtx)
	if !ok || len(vals) < 2 {
		return nil, nil, false
	}

	// Determine whether the values target both local and remote partitions.
	localValOrds := c.getLocalValues(vals, ps)
	if localValOrds.Len() == 0 || localValOrds.Len() == len(vals) {
		// The values target all local or all remote partitions.
		return nil, nil, false
	}

	// Split the values into local and remote sets.
	localValues, remoteValues := c.splitValues(vals, localValOrds)

	// Copy all of the filters from the LookupExpr, and replace the filter that
	// constrains the first index column with a filter targeting only local
	// partitions or only remote partitions.
	localExpr = make(memo.FiltersExpr, len(private.LookupExpr))
	copy(localExpr, private.LookupExpr)
	localExpr[filterIdx] = c.makeConstFilter(col, localValues)

	remoteExpr = make(memo.FiltersExpr, len(private.LookupExpr))
	copy(remoteExpr, private.LookupExpr)
	remoteExpr[filterIdx] = c.makeConstFilter(col, remoteValues)

	return localExpr, remoteExpr, true
}

// getConstPrefixFilter finds the position of the filter in the given slice of
// filters that constrains the first index column to one or more constant
// values. If such a filter is found, getConstPrefixFilter returns the position
// of the filter and ok=true. Otherwise, returns ok=false.
func (c CustomFuncs) getConstPrefixFilter(
	index cat.Index, table opt.TableID, filters memo.FiltersExpr,
) (pos int, ok bool) {
	idxCol := table.IndexColumnID(index, 0)
	for i := range filters {
		props := filters[i].ScalarProps()
		if !props.TightConstraints {
			continue
		}
		if props.OuterCols.Len() != 1 {
			continue
		}
		col := props.OuterCols.SingleColumn()
		if col == idxCol {
			return i, true
		}
	}
	return 0, false
}

// getLocalValues returns the indexes of the values in the given Datums slice
// that target local partitions.
func (c *CustomFuncs) getLocalValues(
	values tree.Datums, ps *partition.PrefixSorter,
) util.FastIntSet {
	// The PrefixSorter has collected all the prefixes from all the different
	// partitions (remembering which ones came from local partitions), and has
	// sorted them so that longer prefixes come before shorter prefixes. For each
	// span in the scanConstraint, we will iterate through the list of prefixes
	// until we find a match, so ordering them with longer prefixes first ensures
	// that the correct match is found.
	var localVals util.FastIntSet
	for i, val := range values {
		if match, ok := constraint.FindMatchOnSingleColumn(val, ps); ok {
			if match.IsLocal {
				localVals.Add(i)
			}
		}
	}
	return localVals
}

// splitValues splits the given slice of Datums into local and remote slices
// by putting the Datums at positions identified by localValOrds into the local
// slice, and the remaining Datums into the remote slice.
func (c *CustomFuncs) splitValues(
	values tree.Datums, localValOrds util.FastIntSet,
) (localVals, remoteVals tree.Datums) {
	localVals = make(tree.Datums, 0, localValOrds.Len())
	remoteVals = make(tree.Datums, 0, len(values)-len(localVals))
	for i, val := range values {
		if localValOrds.Contains(i) {
			localVals = append(localVals, val)
		} else {
			remoteVals = append(remoteVals, val)
		}
	}
	return localVals, remoteVals
}
