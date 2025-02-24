// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package xform

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/distribution"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/lookupjoin"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partition"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// GenerateMergeJoins spawns MergeJoinOps, based on any interesting orderings.
func (c *CustomFuncs) GenerateMergeJoins(
	grp memo.RelExpr,
	required *physical.Required,
	originalOp opt.Operator,
	left, right memo.RelExpr,
	on memo.FiltersExpr,
	joinPrivate *memo.JoinPrivate,
) {
	if joinPrivate.Flags.Has(memo.DisallowMergeJoin) ||
		!c.e.evalCtx.SessionData().OptimizerMergeJoinsEnabled {
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
	leftCols := leftEq.ToSet()
	// NOTE: leftCols cannot be mutated after this point because it is used as a
	// key to cache the restricted orderings in left's logical properties.
	orders := ordering.DeriveRestrictedInterestingOrderings(c.e.mem, left, leftCols).Copy()

	var mustGenerateMergeJoin bool
	leftFDs := &left.Relational().FuncDeps
	if len(orders) == 0 && leftCols.SubsetOf(leftFDs.ConstantCols()) {
		// All left equality columns are constant, so we can trivially create
		// an ordering.
		mustGenerateMergeJoin = true
	}

	if !c.NoJoinHints(joinPrivate) || c.e.evalCtx.SessionData().ReorderJoinsLimit == 0 {
		// If we are using a hint, or the join limit is set to zero, the join won't
		// be commuted. Add the orderings from the right side.
		rightOrders := ordering.DeriveInterestingOrderings(c.e.mem, right).Copy()
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

	getEqCols := func(col opt.ColumnID) (left, right opt.ColumnID) {
		// Assume that col is in either leftEq or rightEq.
		for eqIdx := 0; eqIdx < len(leftEq); eqIdx++ {
			if leftEq[eqIdx] == col || rightEq[eqIdx] == col {
				return leftEq[eqIdx], rightEq[eqIdx]
			}
		}
		panic(errors.AssertionFailedf("failed to find eqIdx for merge join"))
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
			l, r := getEqCols(col)
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
//     Join                       LookupJoin(t@idx)
//     /   \                           |
//     /     \            ->            |
//     Input  Scan(t)                   Input
//
//  2. The index is not covering, but we can fully evaluate the ON condition
//     using the index, or we are doing an InnerJoin. We have to generate
//     an index join above the lookup join. Note that this index join is also
//     implemented as a LookupJoin, because an IndexJoin can only output
//     columns from one table, whereas we also need to output columns from
//     Input.
//
//     Join                       LookupJoin(t@primary)
//     /   \                           |
//     /     \            ->            |
//     Input  Scan(t)                LookupJoin(t@idx)
//     |
//     |
//     Input
//
//     For example:
//     CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//     CREATE TABLE xyz (x INT PRIMARY KEY, y INT, z INT, INDEX (y))
//     SELECT * FROM abc JOIN xyz ON a=y
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
//     SELECT * FROM abc LEFT JOIN xyz ON a=y AND b=z
//
//     The first join will evaluate a=y and produce columns a,b,c,x,y,cont
//     where cont is the continuation column used to group together rows that
//     correspond to the same original a,b,c. The second join will fetch z from
//     the primary index, evaluate b=z, and produce columns a,b,c,x,y,z. A
//     similar approach works for anti-joins and semi-joins.
//
// A lookup join can be created when the ON condition or implicit filters from
// CHECK constraints and computed columns constrain a prefix of the index
// columns to non-ranging constant values. To support this, the constant values
// are cross-joined with the input and used as key columns for the parent lookup
// join.
//
// For example, consider the tables and query below.
//
//	CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//	CREATE TABLE xyz (
//	  x INT PRIMARY KEY,
//	  y INT,
//	  z INT NOT NULL,
//	  CHECK z IN (1, 2, 3),
//	  INDEX (z, y)
//	)
//	SELECT a, x FROM abc JOIN xyz ON a=y
//
// GenerateLookupJoins will perform the following transformation.
//
//	   Join                       LookupJoin(t@idx)
//	   /   \                           |
//	  /     \            ->            |
//	Input  Scan(t)                   Join
//	                                 /   \
//	                                /     \
//	                              Input  Values(1, 2, 3)
//
// If a column is constrained to a single constant value, inlining normalization
// rules will reduce the cross join into a project.
//
//	   Join                       LookupJoin(t@idx)
//	   /   \                           |
//	  /     \            ->            |
//	Input  Scan(t)                  Project
//	                                   |
//	                                   |
//	                                 Input
func (c *CustomFuncs) GenerateLookupJoins(
	grp memo.RelExpr,
	required *physical.Required,
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
//	   Join                       LookupJoin(t@idx)
//	   /   \                           |
//	  /     \            ->            |
//	Input  Project                   Input
//	         |
//	         |
//	       Scan(t)
//
// This function and its associated rule currently require that the right side
// projects only virtual computed columns and all those columns are covered by a
// single index.
//
// It should also be possible to support cases where all the virtual columns are
// not covered by a single index by wrapping the lookup join in a Project that
// produces the non-covered virtual columns.
func (c *CustomFuncs) GenerateLookupJoinsWithVirtualCols(
	grp memo.RelExpr,
	required *physical.Required,
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

// canGenerateLookupJoins makes a best-effort to filter out cases where no
// joins can be constructed based on the join's filters and flags. It may miss
// some cases that will be filtered out later.
func (c *CustomFuncs) canGenerateLookupJoins(
	input memo.RelExpr, joinFlags memo.JoinFlags, leftCols, rightCols opt.ColSet, on memo.FiltersExpr,
) bool {
	if joinFlags.Has(memo.DisallowLookupJoinIntoRight) {
		return false
	}
	if memo.HasJoinCondition(leftCols, rightCols, on, false /* inequality */) {
		// There is at least one valid equality between left and right columns.
		return true
	}
	// There are no valid equality conditions, but there may be an inequality that
	// can be used for lookups. Since the current implementation does not
	// deduplicate the resulting spans, only plan a lookup join with no equalities
	// when the input has one row, or if a lookup join is forced.
	if c.e.evalCtx.SessionData().VariableInequalityLookupJoinEnabled &&
		(input.Relational().Cardinality.IsZeroOrOne() ||
			joinFlags.Has(memo.AllowOnlyLookupJoinIntoRight)) {
		return memo.HasJoinCondition(leftCols, rightCols, on, true /* inequality */)
	}
	return false
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
	md := c.e.mem.Metadata()
	inputProps := input.Relational()

	if !c.canGenerateLookupJoins(input, joinPrivate.Flags, inputProps.OutputCols, rightCols, on) {
		return
	}

	// Initialize the constraint builder.
	c.cb.Init(
		c.e.ctx,
		c.e.f,
		md,
		c.e.evalCtx,
		scanPrivate.Table,
		inputProps.OutputCols,
		rightCols,
	)

	// Generate implicit filters from CHECK constraints and computed columns as
	// optional filters to help generate lookup join keys.
	optionalFilters := c.checkConstraintFilters(scanPrivate.Table)
	computedColFilters := c.ComputedColFilters(scanPrivate, on, optionalFilters)
	optionalFilters = append(optionalFilters, computedColFilters...)

	onClauseLookupRelStrictKeyCols, lookupRelEquijoinCols, inputRelJoinCols, lookupIsKey :=
		c.GetEquijoinStrictKeyCols(on, scanPrivate, input)

	var input2 memo.RelExpr
	var scanPrivate2 *memo.ScanPrivate
	var lookupIsKey2 bool
	var indexCols2 opt.ColSet
	// Look up strict key cols in the reverse direction for the purposes of
	// deriving a `t1.crdb_region = t2.crdb_region` term based on foreign key
	// constraints.
	if !lookupIsKey {
		if scanExpr, _, ok := c.getfilteredCanonicalScan(input); ok {
			scanPrivate2 = &scanExpr.ScanPrivate
			// The scan should already exist in the memo. We need to look it up so we
			// have a `ScanExpr` with properties fully populated.
			input2 = c.e.mem.MemoizeScan(scanPrivate)
			tabMeta := md.TableMeta(scanPrivate2.Table)
			indexCols2 = tabMeta.IndexColumns(scanPrivate2.Index)
			onClauseLookupRelStrictKeyCols, lookupRelEquijoinCols, inputRelJoinCols, lookupIsKey2 =
				c.GetEquijoinStrictKeyCols(on, scanPrivate2, input2)
		}
	}

	var pkCols opt.ColList
	var newScanPrivate *memo.ScanPrivate
	var iter scanIndexIter
	reject := rejectInvertedIndexes | rejectVectorIndexes
	iter.Init(c.e.evalCtx, c.e, c.e.mem, &c.im, scanPrivate, on, reject)
	iter.ForEach(func(index cat.Index, onFilters memo.FiltersExpr, indexCols opt.ColSet, _ bool, _ memo.ProjectionsExpr) {
		// Skip indexes that do not cover all virtual projection columns, if
		// there are any. This can happen when there are multiple virtual
		// columns indexed in different indexes.
		//
		// TODO(mgartner): It should be possible to plan a lookup join in this
		// case by producing the covered virtual columns from the lookup join
		// and producing the rest in a Project that wraps the join.
		if !projectedVirtualCols.SubsetOf(indexCols) {
			return
		}

		var derivedfkOnFilters memo.FiltersExpr
		if lookupIsKey {
			derivedfkOnFilters = c.ForeignKeyConstraintFilters(
				input, scanPrivate, indexCols, onClauseLookupRelStrictKeyCols, lookupRelEquijoinCols, inputRelJoinCols)
		} else if lookupIsKey2 {
			derivedfkOnFilters = c.ForeignKeyConstraintFilters(
				input2, scanPrivate2, indexCols2, onClauseLookupRelStrictKeyCols, lookupRelEquijoinCols, inputRelJoinCols)
		}
		lookupConstraint, foundEqualityCols := c.cb.Build(index, onFilters, optionalFilters, derivedfkOnFilters)
		if lookupConstraint.IsUnconstrained() {
			// We couldn't find equality columns or a lookup expression to
			// perform a lookup join on this index.
			return
		}
		if !foundEqualityCols && !inputProps.Cardinality.IsZeroOrOne() &&
			!joinPrivate.Flags.Has(memo.AllowOnlyLookupJoinIntoRight) {
			// Avoid planning an inequality-only lookup when the input has more than
			// one row unless the lookup join is forced (see canGenerateLookupJoins
			// for a brief explanation).
			return
		}

		lookupJoin := memo.LookupJoinExpr{Input: input}
		lookupJoin.JoinPrivate = *joinPrivate
		lookupJoin.JoinType = joinType
		lookupJoin.Table = scanPrivate.Table
		lookupJoin.Index = index.Ordinal()
		lookupJoin.Locking = scanPrivate.Locking
		lookupJoin.KeyCols = lookupConstraint.KeyCols
		lookupJoin.DerivedEquivCols = lookupConstraint.DerivedEquivCols
		lookupJoin.LookupExpr = lookupConstraint.LookupExpr
		lookupJoin.On = lookupConstraint.RemainingFilters
		lookupJoin.AllLookupFilters = lookupConstraint.AllLookupFilters

		// Wrap the input in a Project if any projections are required. The
		// lookup join will project away these synthesized columns.
		if len(lookupConstraint.InputProjections) > 0 {
			lookupJoin.Input = c.e.f.ConstructProject(
				lookupJoin.Input,
				lookupConstraint.InputProjections,
				lookupJoin.Input.Relational().OutputCols,
			)
		}

		tableFDs := memo.MakeTableFuncDep(md, scanPrivate.Table)
		// A lookup join will drop any input row which contains NULLs, so a lax key
		// is sufficient.
		rightKeyCols := lookupConstraint.RightSideCols.ToSet()
		lookupJoin.LookupColsAreTableKey = tableFDs.ColsAreLaxKey(rightKeyCols)

		// Add input columns and lookup expression columns, since these will be
		// needed for all join types and cases. Exclude synthesized projection
		// columns.
		var projectionCols opt.ColSet
		for i := range lookupConstraint.InputProjections {
			projectionCols.Add(lookupConstraint.InputProjections[i].Col)
		}
		lookupJoin.Cols = lookupJoin.LookupExpr.OuterCols().Difference(projectionCols)
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

		if joinType == opt.SemiJoinOp || joinType == opt.AntiJoinOp {
			// Typically, the index must cover all columns from the right in
			// order to generate a lookup join without an additional index join
			// (case 1, see function comment). However, there are some cases
			// where the remaining filters no longer reference some columns.
			//
			// 1. If the index is a partial index, the filters remaining after
			// proving filter-predicate implication may no longer reference some
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
			// 2. If onFilters contain a contradiction or tautology that is not
			// normalized away, then columns may no longer be referenced in the
			// remaining filters of the lookup join. Consider the example:
			//
			//   CREATE TABLE a (a INT)
			//   CREATE TABLE xyz (x INT, y INT, z INT, INDEX (x, z))
			//
			//   SELECT a FROM a WHERE a IN (
			//     SELECT z FROM xyz WHERE x = 0 OR y IN (0) AND y > 0
			//   )
			//
			// The filter x = 0 OR y IN (0) AND y > 0 contains a contradiction
			// that currently is not normalized to false, but a tight constraint
			// is created for entire filter that constrains x to 0. Because the
			// filter is tight, there is no remaining filter. Column y is no
			// longer referenced, so a lookup semi-join can be created despite
			// the secondary index not covering y.
			//
			// Note that these are special cases that only work for semi- and
			// anti-joins because they never include columns from the right side
			// in their output columns. Other joins include columns from the
			// right side in their output columns, so even if the ON filters no
			// longer reference an un-covered column, they must be fetched (case
			// 2, see function comment).
			remainingFilterCols := rightKeyCols.Union(lookupJoin.On.OuterCols())
			filterColsFromRight := rightCols.Intersection(remainingFilterCols)
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
			if !projectedVirtualCols.Empty() {
				// Virtual columns are not stored in the primary index, so the
				// upper join cannot produce them.
				// TODO(#90771): Add a Project above the upper join to produce
				// virtual columns. Take care to ensure that they are
				// null-extended correctly for left-joins. We'll also need to
				// ensure that the upper join produces all the columns
				// referenced by the virtual computed column expressions.
				return
			}

			// Create a new ScanPrivate, which will be used below for the first lookup
			// join in the pair. Note: this must happen before the continuation column
			// is created to ensure that the continuation column will have the highest
			// column ID.
			//
			// See the comment where this newScanPrivate is used below in mapLookupJoin
			// for details about why it's needed.
			if newScanPrivate == nil {
				newScanPrivate = c.DuplicateScanPrivate(scanPrivate)
			}

			lookupJoin.JoinType = lowerJoinType
			continuationCol = c.constructContinuationColumnForPairedJoin()
			lookupJoin.IsFirstJoinInPairedJoiner = true
			lookupJoin.ContinuationCol = continuationCol
			lookupJoin.Cols.Add(continuationCol)

			// Map the lookup join to use the new table and column IDs from the
			// newScanPrivate created above. We want to make sure that the column IDs
			// returned by the lookup join are different from the IDs that will be
			// returned by the top level index join.
			//
			// In addition to avoiding subtle bugs in the optimizer when the same
			// column ID is reused, this mapping is also essential for correct behavior
			// at execution time in the case of a left paired join. This is because a
			// row that matches in the first left join (the lookup join) might be a
			// false positive and fail to match in the second left join (the index
			// join). If an original left row has no matches after the second left join,
			// it must appear as a null-extended row with all right-hand columns null.
			// If one of the right-hand columns comes from the lookup join, however,
			// it might incorrectly show up as non-null (see #58892 and #81968).
			c.mapLookupJoin(&lookupJoin, indexCols, newScanPrivate)
		}

		indexJoin.Input = c.e.f.ConstructLookupJoin(
			lookupJoin.Input,
			lookupJoin.On,
			&lookupJoin.LookupJoinPrivate,
		)
		indexJoin.JoinType = joinType
		indexJoin.Table = scanPrivate.Table
		indexJoin.Index = cat.PrimaryIndex
		indexJoin.KeyCols = c.getPkCols(lookupJoin.Table)
		indexJoin.Cols = rightCols.Union(inputProps.OutputCols)
		indexJoin.LookupColsAreTableKey = true
		indexJoin.Locking = scanPrivate.Locking
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

// constructContinuationColumnForPairedJoin constructs a continuation column
// ID for the paired-joiners used for left outer/semi/anti joins when the
// first join generates false positives (due to an inverted index or
// non-covering index). The first join will be either a left outer join or
// an inner join.
func (c *CustomFuncs) constructContinuationColumnForPairedJoin() opt.ColumnID {
	return c.e.f.Metadata().AddColumn("continuation", c.BoolType())
}

// mapLookupJoin maps the given lookup join to use the table and columns
// provided in newScanPrivate. The lookup join is modified in place. indexCols
// contains the pre-calculated index columns used by the given lookupJoin.
//
// Note that columns from the input are not mapped. For example, KeyCols
// does not need to be mapped below since it only contains input columns.
func (c *CustomFuncs) mapLookupJoin(
	lookupJoin *memo.LookupJoinExpr, indexCols opt.ColSet, newScanPrivate *memo.ScanPrivate,
) {
	tabID := lookupJoin.Table
	newTabID := newScanPrivate.Table

	// Create a map from the source columns to the destination columns.
	var srcColsToDstCols opt.ColMap
	for srcCol, ok := indexCols.Next(0); ok; srcCol, ok = indexCols.Next(srcCol + 1) {
		ord := tabID.ColumnOrdinal(srcCol)
		dstCol := newTabID.ColumnID(ord)
		srcColsToDstCols.Set(int(srcCol), int(dstCol))
	}

	lookupJoin.Table = newTabID
	c.e.f.DisableOptimizationsTemporarily(func() {
		// Disable normalization rules when remapping the lookup expressions so
		// that they do not get normalized into non-canonical lookup
		// expressions.
		lookupExpr := c.e.f.RemapCols(&lookupJoin.LookupExpr, srcColsToDstCols).(*memo.FiltersExpr)
		lookupJoin.LookupExpr = *lookupExpr
		remoteLookupExpr := c.e.f.RemapCols(&lookupJoin.RemoteLookupExpr, srcColsToDstCols).(*memo.FiltersExpr)
		lookupJoin.RemoteLookupExpr = *remoteLookupExpr
	})
	lookupJoin.Cols = lookupJoin.Cols.CopyAndMaybeRemap(srcColsToDstCols)
	allLookupFilters := c.e.f.RemapCols(&lookupJoin.AllLookupFilters, srcColsToDstCols).(*memo.FiltersExpr)
	lookupJoin.AllLookupFilters = *allLookupFilters
	on := c.e.f.RemapCols(&lookupJoin.On, srcColsToDstCols).(*memo.FiltersExpr)
	lookupJoin.On = *on
	lookupJoin.DerivedEquivCols = lookupJoin.DerivedEquivCols.CopyAndMaybeRemap(srcColsToDstCols)
}

// GenerateInvertedJoins is similar to GenerateLookupJoins, but instead
// of generating lookup joins with regular indexes, it generates lookup joins
// with inverted indexes. Similar to GenerateLookupJoins, there are two cases
// depending on whether or not the index is covering. See the comment above
// GenerateLookupJoins for details.
func (c *CustomFuncs) GenerateInvertedJoins(
	grp memo.RelExpr,
	required *physical.Required,
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
	iter.Init(c.e.evalCtx, c.e, c.e.mem, &c.im, scanPrivate, on, rejectNonInvertedIndexes)
	iter.ForEach(func(index cat.Index, onFilters memo.FiltersExpr, indexCols opt.ColSet, _ bool, _ memo.ProjectionsExpr) {
		invertedJoin := memo.InvertedJoinExpr{Input: input}
		numPrefixCols := index.PrefixColumnCount()

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
				computedColFilters := c.ComputedColFilters(scanPrivate, on, optionalFilters)
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
			foundVals, allIdx, ok := lookupjoin.FindJoinFilterConstants(c.e.ctx, allFilters, prefixCol, c.e.evalCtx)
			if !ok {
				// Cannot constrain prefix column and therefore cannot generate
				// an inverted join.
				return
			}

			if len(foundVals) > 1 &&
				(joinType == opt.LeftJoinOp || joinType == opt.SemiJoinOp || joinType == opt.AntiJoinOp) {
				// We cannot create an inverted join in this case, because
				// constructing a cross join with foundVals will increase the
				// size of the input. As a result, matching input rows will show
				// up more than once in the output of a semi-join, and
				// non-matching input rows will show up more than once in the
				// output of a left or anti join, which is incorrect (see #59615
				// and #78681).
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
			c.e.ctx, c.e.f, onFilters, scanPrivate.Table, index, inputCols,
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
		for i, n := 0, index.PrefixColumnCount(); i < n; i++ {
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
		invertedJoin.Locking = scanPrivate.Locking
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
		indexJoin.Locking = scanPrivate.Locking
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
	for i, n := 0, index.PrefixColumnCount(); i < n; i++ {
		prefixCol := newTabID.IndexColumnID(index, i)
		newIndexCols.Add(prefixCol)
	}

	// Create a map from the source columns to the destination columns,
	// including the inverted source columns which will be used in the
	// invertedExpr.
	var srcColsToDstCols opt.ColMap
	for srcCol, ok := indexCols.Next(0); ok; srcCol, ok = indexCols.Next(srcCol + 1) {
		ord := tabID.ColumnOrdinal(srcCol)
		dstCol := newTabID.ColumnID(ord)
		srcColsToDstCols.Set(int(srcCol), int(dstCol))
	}
	ord := index.InvertedColumn().InvertedSourceColumnOrdinal()
	invertedSourceCol := tabID.ColumnID(ord)
	newInvertedSourceCol := newTabID.ColumnID(ord)
	srcColsToDstCols.Set(int(invertedSourceCol), int(newInvertedSourceCol))

	invertedJoin.Table = newTabID
	invertedJoin.InvertedExpr = c.e.f.RemapCols(invertedJoin.InvertedExpr, srcColsToDstCols)
	invertedJoin.Cols = invertedJoin.Cols.Difference(indexCols).Union(newIndexCols)
	constFilters := c.e.f.RemapCols(&invertedJoin.ConstFilters, srcColsToDstCols).(*memo.FiltersExpr)
	invertedJoin.ConstFilters = *constFilters
	on := c.e.f.RemapCols(&invertedJoin.On, srcColsToDstCols).(*memo.FiltersExpr)
	invertedJoin.On = *on
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
// a better ordering for a join tree. This is the case if the given expression
// is the first expression of its group, and the join tree rooted at the
// expression has not previously been reordered. This is to avoid duplicate
// work. In addition, a join cannot be reordered if it has join hints.
func (c *CustomFuncs) ShouldReorderJoins(root memo.RelExpr) bool {
	if root != root.FirstExpr() {
		// Only match the first expression of a group to avoid duplicate work.
		return false
	}
	if c.e.evalCtx.SessionData().ReorderJoinsLimit == 0 {
		// Join reordering has been disabled.
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
func (c *CustomFuncs) ReorderJoins(grp memo.RelExpr, required *physical.Required) memo.RelExpr {
	c.e.o.JoinOrderBuilder().Init(c.e.ctx, c.e.f, c.e.evalCtx)
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
		AllLookupFilters:      nil,
		Locking:               indexPrivate.Locking,
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
	nullRejectedCols := memo.NullColsRejectedByFilter(c.e.ctx, c.e.evalCtx, on)
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

// IsLocalityOptimizedJoin returns true if the given lookup join is a locality
// optimized join
func (c *CustomFuncs) IsLocalityOptimizedJoin(private *memo.LookupJoinPrivate) bool {
	return private.LocalityOptimized
}

// EmptyFiltersExpr returns an empty FiltersExpr.
func (c *CustomFuncs) EmptyFiltersExpr() memo.FiltersExpr {
	return memo.EmptyFiltersExpr
}

// CreateRemoteOnlyLookupJoinPrivate creates a new lookup join private from the
// given private and replaces the LookupExpr with the given filter. It also
// marks the private as locality optimized and as a join which only does lookups
// into remote regions.
func (c *CustomFuncs) CreateRemoteOnlyLookupJoinPrivate(
	remoteLookupExpr memo.FiltersExpr, private *memo.LookupJoinPrivate,
) *memo.LookupJoinPrivate {
	newPrivate := c.CreateLocalityOptimizedLookupJoinPrivate(remoteLookupExpr, c.e.funcs.EmptyFiltersExpr(), private)
	newPrivate.RemoteOnlyLookups = true
	return newPrivate
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
	switch private.JoinType {
	case opt.AntiJoinOp:
		// Add the filters from the LookupExpr, because we need to account for the
		// regions selected in our statistics estimation. This is only needed for
		// anti join because it is the only locality optimized join that is split
		// into two separate operators. Note that only lookupExpr is used for anti
		// joins, not remoteLookupExpr.
		newPrivate.AllLookupFilters = append(newPrivate.AllLookupFilters, lookupExpr...)
	}
	return &newPrivate
}

// CreateLocalityOptimizedLookupJoinPrivateIncludingCols is similar to
// CreateLocalityOptimizedLookupJoinPrivate. The new lookup join private
// contains both the columns in the given private and the given columns.
func (c *CustomFuncs) CreateLocalityOptimizedLookupJoinPrivateIncludingCols(
	lookupExpr, remoteLookupExpr memo.FiltersExpr, private *memo.LookupJoinPrivate, cols opt.ColSet,
) *memo.LookupJoinPrivate {
	newPrivate := c.CreateLocalityOptimizedLookupJoinPrivate(lookupExpr, remoteLookupExpr, private)
	// Make a copy of the columns to avoid mutating the original
	// LookupJoinPrivate's columns.
	newPrivate.Cols = newPrivate.Cols.Copy()
	newPrivate.Cols.UnionWith(cols)
	return newPrivate
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
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(private.Table)

	// The PrefixSorter has collected all the prefixes from all the different
	// partitions (remembering which ones came from local partitions), and has
	// sorted them so that longer prefixes come before shorter prefixes. For
	// each span in the scanConstraint, we will iterate through the list of
	// prefixes until we find a match, so ordering them with longer prefixes
	// first ensures that the correct match is found. The PrefixSorter is only
	// non-empty when this index has at least one local and one remote
	// partition.
	ps := tabMeta.IndexPartitionLocality(private.Index)
	if ps.Empty() {
		return nil, nil, false
	}

	// Find a filter that constrains the first column of the index.
	filterIdx, ok := private.GetConstPrefixFilter(c.e.mem.Metadata())
	if !ok {
		return nil, nil, false
	}
	filter := private.LookupExpr[filterIdx]

	// Check whether the filter constrains the first column of the index
	// to at least two constant values. We need at least two values so that one
	// can target a local partition and one can target a remote partition.
	idx := md.Table(private.Table).Index(private.Index)
	firstCol := private.Table.ColumnID(idx.Column(0).Ordinal())
	vals, ok := filter.ScalarProps().Constraints.ExtractSingleColumnNonNullConstValues(c.e.ctx, c.e.evalCtx, firstCol)
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
	c.e.f.DisableOptimizationsTemporarily(func() {
		// Disable normalization rules when constructing the lookup expression
		// so that it does not get normalized into a non-canonical expression.
		localExpr[filterIdx] = c.e.f.ConstructConstFilter(firstCol, localValues)
	})

	remoteExpr = make(memo.FiltersExpr, len(private.LookupExpr))
	copy(remoteExpr, private.LookupExpr)
	c.e.f.DisableOptimizationsTemporarily(func() {
		// Disable normalization rules when constructing the lookup expression
		// so that it does not get normalized into a non-canonical expression.
		remoteExpr[filterIdx] = c.e.f.ConstructConstFilter(firstCol, remoteValues)
	})

	return localExpr, remoteExpr, true
}

// getLocalValues returns the indexes of the values in the given Datums slice
// that target local partitions.
func (c *CustomFuncs) getLocalValues(values tree.Datums, ps partition.PrefixSorter) intsets.Fast {
	// The PrefixSorter has collected all the prefixes from all the different
	// partitions (remembering which ones came from local partitions), and has
	// sorted them so that longer prefixes come before shorter prefixes. For each
	// span in the scanConstraint, we will iterate through the list of prefixes
	// until we find a match, so ordering them with longer prefixes first ensures
	// that the correct match is found.
	var localVals intsets.Fast
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
	values tree.Datums, localValOrds intsets.Fast,
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

// splitDisjunctionForJoin finds the first disjunction in the ON clause that can
// be split into a pair of predicates. It returns the pair of predicates and the
// Filters item they were a part of. If a disjunction is not found, ok=false is
// returned.
//
// It is expected that the left and right inputs to joinRel have been
// pre-checked to be canonical scans, or Selects from canonical scans, and
// origLeftScan and origRightScan refer to those scans.
func (c *CustomFuncs) splitDisjunctionForJoin(
	joinRel memo.RelExpr,
	filters memo.FiltersExpr,
	origLeftScan *memo.ScanExpr,
	origRightScan *memo.ScanExpr,
) (
	leftPreds opt.ScalarExpr,
	rightPreds opt.ScalarExpr,
	itemToReplace *memo.FiltersItem,
	ok bool,
) {
	for i := range filters {
		if filters[i].Condition.Op() == opt.OrOp {
			if leftPreds, rightPreds, ok =
				c.findDisjunctionPairForJoin(joinRel, &filters[i], origLeftScan, origRightScan); ok {
				itemToReplace = &filters[i]
				return leftPreds, rightPreds, itemToReplace, true
			}
		}
	}
	return nil, nil, nil, false
}

// makeFilteredSelectForJoin takes a scanPrivate and filters and constructs a
// new SelectExpr from a ScanExpr with filter columns mapped from
// origScanPrivate to the column IDs in the new scan.
func (c *CustomFuncs) makeFilteredSelectForJoin(
	filters memo.FiltersExpr, scanPrivate *memo.ScanPrivate, origScanPrivate *memo.ScanPrivate,
) (newSelect memo.RelExpr) {
	newScan := c.e.f.ConstructScan(scanPrivate)
	newSelect =
		c.e.f.ConstructSelect(
			newScan,
			c.RemapScanColsInFilter(filters, origScanPrivate, scanPrivate),
		)
	return newSelect
}

// CanSplitJoinWithDisjuncts returns true if the given join can be split into a
// union of two joins. See SplitJoinWithDisjuncts for more details.
func (c *CustomFuncs) CanSplitJoinWithDisjuncts(
	joinRel memo.RelExpr, joinFilters memo.FiltersExpr,
) (firstOnClause, secondOnClause opt.ScalarExpr, itemToReplace *memo.FiltersItem, ok bool) {
	leftInput := joinRel.Child(0).(memo.RelExpr)
	rightInput := joinRel.Child(1).(memo.RelExpr)

	switch joinRel.Op() {
	case opt.InnerJoinOp, opt.SemiJoinOp, opt.AntiJoinOp:
		// Do nothing
	default:
		panic(errors.AssertionFailedf("expected joinRel to be inner, semi, or anti-join"))
	}

	origLeftScan, _, ok := c.getfilteredCanonicalScan(leftInput)
	if !ok {
		return nil, nil, nil, false
	}

	origRightScan, _, ok := c.getfilteredCanonicalScan(rightInput)
	if !ok {
		return nil, nil, nil, false
	}

	// Look for a disjunction of equijoin predicates.
	return c.splitDisjunctionForJoin(joinRel, joinFilters, origLeftScan, origRightScan)
}

// SplitJoinWithDisjuncts checks a join relation for a disjunction of predicates
// in an InnerJoin, SemiJoin or AntiJoin. If present, and the inputs to the join
// are canonical scans, or Selects from canonical scans, it builds two new join
// relations of the same join type as the original, but with one disjunct
// assigned to firstJoin and the remaining disjuncts assigned to secondJoin.
//
// In the case of inner join, newRelationCols contains the column ids from the
// original Scans in the left and right inputs plus primary key columns from
// both input relations. For semijoin or antijoin, newRelationCols contains the
// column ids from the original left Scan plus primary key columns from the left
// relation.
//
// aggCols contains the non-key columns of the left and right inputs.
// groupingCols contains the primary key columns of the left and right inputs,
// needed for deduplicating results.
//
// If there is no disjunction of predicates, or the join type is not one of the
// supported join types listed above, ok=false is returned.
func (c *CustomFuncs) SplitJoinWithDisjuncts(
	joinRel memo.RelExpr,
	joinFilters memo.FiltersExpr,
	firstOnClause, secondOnClause opt.ScalarExpr,
	itemToReplace *memo.FiltersItem,
) (
	firstJoin memo.RelExpr,
	secondJoin memo.RelExpr,
	newRelationCols opt.ColSet,
	aggCols opt.ColSet,
	groupingCols opt.ColSet,
) {
	joinPrivate := joinRel.Private().(*memo.JoinPrivate)
	leftInput := joinRel.Child(0).(memo.RelExpr)
	rightInput := joinRel.Child(1).(memo.RelExpr)

	switch joinRel.Op() {
	case opt.InnerJoinOp, opt.SemiJoinOp, opt.AntiJoinOp:
		// Do nothing
	default:
		panic(errors.AssertionFailedf("expected joinRel to be inner, semi, or anti-join"))
	}

	origLeftScan, leftFilters, ok := c.getfilteredCanonicalScan(leftInput)
	if !ok {
		panic(errors.AssertionFailedf("expected join left input to have canonical scan"))
	}
	origLeftScanPrivate := &origLeftScan.ScanPrivate
	origRightScan, rightFilters, ok := c.getfilteredCanonicalScan(rightInput)
	if !ok {
		panic(errors.AssertionFailedf("expected join right input to have canonical scan"))
	}
	origRightScanPrivate := &origRightScan.ScanPrivate

	// Add in the primary key columns so the caller can group by them to
	// deduplicate results.
	var leftSP *memo.ScanPrivate
	if c.PrimaryKeyCols(origLeftScanPrivate.Table).SubsetOf(origLeftScanPrivate.Cols) {
		leftSP = origLeftScanPrivate
	} else {
		leftSP = c.AddPrimaryKeyColsToScanPrivate(origLeftScanPrivate)
	}
	var rightSP *memo.ScanPrivate
	if joinRel.Op() == opt.InnerJoinOp {
		if c.PrimaryKeyCols(origRightScanPrivate.Table).SubsetOf(origRightScanPrivate.Cols) {
			rightSP = origRightScanPrivate
		} else {
			rightSP = c.AddPrimaryKeyColsToScanPrivate(origRightScanPrivate)
		}
	} else {
		rightSP = origRightScanPrivate
	}

	// Make new column ids for the new scans which are input to the two
	// new joins we're building.
	leftFirstColID, _ := leftSP.Cols.Next(0)
	rightFirstColID, _ := rightSP.Cols.Next(0)
	var newLeftScanPrivate, newRightScanPrivate,
		newLeftScanPrivate2, newRightScanPrivate2 *memo.ScanPrivate
	// The UNION ALL and join operations treat the output columns in the output
	// row as having the same order as the column id numbers. So, in order for the
	// resulting row definition to have the same columns in the same positions as
	// the join we're replacing, maintain this relative order. Follow a simple
	// rule that the new duplicated left and right tables maintain the same column
	// id order as the original scans. Scans allocate column ids in contiguous
	// chunks, so if the first column id in the left is less than the first column
	// id in the right, then all column ids in the left will be lower than all
	// right column ids.
	if leftFirstColID < rightFirstColID {
		newLeftScanPrivate = c.DuplicateScanPrivate(leftSP)
		newRightScanPrivate = c.DuplicateScanPrivate(rightSP)
		newLeftScanPrivate2 = c.DuplicateScanPrivate(leftSP)
		newRightScanPrivate2 = c.DuplicateScanPrivate(rightSP)
	} else {
		newRightScanPrivate = c.DuplicateScanPrivate(rightSP)
		newLeftScanPrivate = c.DuplicateScanPrivate(leftSP)
		newRightScanPrivate2 = c.DuplicateScanPrivate(rightSP)
		newLeftScanPrivate2 = c.DuplicateScanPrivate(leftSP)
	}

	amendedLeftOrigCols := c.ScanPrivateCols(leftSP)
	amendedRightOrigCols := c.ScanPrivateCols(rightSP)
	amendedOrigCols := c.UnionCols(amendedLeftOrigCols, amendedRightOrigCols)

	// Tell the caller what the complete set of column ids is for the new relation
	// they are building (e.g., a UNION ALL of the 2 new joins).
	if joinRel.Op() == opt.InnerJoinOp {
		newRelationCols = amendedOrigCols
	} else {
		newRelationCols = amendedLeftOrigCols
	}

	// Build the new Selects for the first new join, with mapped filter columns.
	newLeftSelect :=
		c.makeFilteredSelectForJoin(leftFilters, newLeftScanPrivate, leftSP)
	newRightSelect :=
		c.makeFilteredSelectForJoin(rightFilters, newRightScanPrivate, rightSP)

	// Assign the firstOnClause, which was built from splitting the disjunction,
	// to the first new join.
	newJoinFilters :=
		c.remapJoinColsInFilter(
			c.ReplaceFiltersItem(joinFilters, itemToReplace, firstOnClause),
			leftSP, newLeftScanPrivate, rightSP, newRightScanPrivate,
		)
	newJoinPrivate := c.e.funcs.DuplicateJoinPrivate(joinPrivate)

	// Build a new first join with an identical join type to the original join.
	switch joinRel.Op() {
	case opt.InnerJoinOp:
		firstJoin = c.e.f.ConstructInnerJoin(newLeftSelect, newRightSelect, newJoinFilters, newJoinPrivate)
	case opt.SemiJoinOp:
		firstJoin = c.e.f.ConstructSemiJoin(newLeftSelect, newRightSelect, newJoinFilters, newJoinPrivate)
	case opt.AntiJoinOp:
		firstJoin = c.e.f.ConstructAntiJoin(newLeftSelect, newRightSelect, newJoinFilters, newJoinPrivate)
	default:
		panic(errors.AssertionFailedf("Unexpected join type while splitting disjuncted join predicates: %v",
			joinRel.Op()))
	}

	// Build the new Selects for the second new join, with mapped filter columns.
	newLeftSelect2 :=
		c.makeFilteredSelectForJoin(leftFilters, newLeftScanPrivate2, leftSP)
	newRightSelect2 :=
		c.makeFilteredSelectForJoin(rightFilters, newRightScanPrivate2, rightSP)

	// Assign the secondOnClause, which was built from splitting the disjunction,
	// to the second new join.
	newJoinFilters2 :=
		c.remapJoinColsInFilter(
			c.ReplaceFiltersItem(joinFilters, itemToReplace, secondOnClause),
			leftSP, newLeftScanPrivate2, rightSP, newRightScanPrivate2,
		)
	newJoinPrivate2 := c.DuplicateJoinPrivate(joinPrivate)

	// Build a new second join with an identical join type to the original join.
	switch joinRel.Op() {
	case opt.InnerJoinOp:
		secondJoin = c.e.f.ConstructInnerJoin(newLeftSelect2, newRightSelect2, newJoinFilters2, newJoinPrivate2)
	case opt.SemiJoinOp:
		secondJoin = c.e.f.ConstructSemiJoin(newLeftSelect2, newRightSelect2, newJoinFilters2, newJoinPrivate2)
	case opt.AntiJoinOp:
		secondJoin = c.e.f.ConstructAntiJoin(newLeftSelect2, newRightSelect2, newJoinFilters2, newJoinPrivate2)
	default:
		panic(errors.AssertionFailedf("Unexpected join type while splitting disjuncted join predicates: %v",
			joinRel.Op()))
	}

	// Build the PK/rowid grouping columns to allow the caller to remove duplicates.
	switch joinRel.Op() {
	case opt.InnerJoinOp:
		allPKColsSet := c.UnionCols(
			c.PrimaryKeyCols(origLeftScanPrivate.Table),
			c.PrimaryKeyCols(origRightScanPrivate.Table))
		allJoinColsSet := c.UnionCols(
			origLeftScanPrivate.Cols,
			origRightScanPrivate.Cols)
		aggCols = c.DifferenceCols(allJoinColsSet, allPKColsSet)
		groupingCols = allPKColsSet
	case opt.SemiJoinOp, opt.AntiJoinOp:
		projectedPKColsSet := c.PrimaryKeyCols(origLeftScanPrivate.Table)
		projectedJoinColsSet := origLeftScanPrivate.Cols
		aggCols = c.DifferenceCols(projectedJoinColsSet, projectedPKColsSet)
		groupingCols = projectedPKColsSet
	default:
		panic(errors.AssertionFailedf("Unexpected join type while splitting disjuncted join predicates: %v",
			joinRel.Op()))
	}
	return firstJoin, secondJoin, newRelationCols, aggCols, groupingCols
}

// getfilteredCanonicalScan looks at a *ScanExpr or *SelectExpr "relation" and
// returns the input *ScanExpr and FiltersExpr, along with ok=true, if the Scan
// is a canonical scan. If "relation" is a different type, or if it's a
// *SelectExpr with an Input other than a *ScanExpr, ok=false is returned. Scans
// or Selects with no filters may return filters as nil.
func (c *CustomFuncs) getfilteredCanonicalScan(
	relation memo.RelExpr,
) (scanExpr *memo.ScanExpr, filters memo.FiltersExpr, ok bool) {
	var selectExpr *memo.SelectExpr
	if selectExpr, ok = relation.(*memo.SelectExpr); ok {
		if scanExpr, ok = selectExpr.Input.(*memo.ScanExpr); !ok {
			return nil, nil, false
		}
		filters = selectExpr.Filters
	} else if scanExpr, ok = relation.(*memo.ScanExpr); !ok {
		return nil, nil, false
	}
	scanPrivate := &scanExpr.ScanPrivate
	if !c.IsCanonicalScan(scanPrivate) {
		return nil, nil, false
	}
	return scanExpr, filters, true
}

// CanHoistProjectInput returns true if a projection of an expression on
// `relation` is allowed to be hoisted above a parent Join. The preconditions
// for this are if `relation` is a canonical scan or a select from a canonical
// scan, or the disable_hoist_projection_in_join_limitation session flag is
// true.
func (c *CustomFuncs) CanHoistProjectInput(relation memo.RelExpr) (ok bool) {
	if c.e.evalCtx.SessionData().DisableHoistProjectionInJoinLimitation {
		return true
	}
	_, _, ok = c.getfilteredCanonicalScan(relation)
	return ok
}

// findDisjunctionPairForJoin groups disjunction subexpressions into a pair of
// join predicates.
//
// When there are more than two predicates, the deepest leaf node in the left
// depth OrExpr tree is returned as "left" and the remaining predicates are
// built into a brand new OrExpr chain and returned as "right".
//
// It is expected that the left and right inputs to joinRel have been
// pre-checked to be canonical scans, or Selects from canonical scans, and
// leftScan and rightScan refer to those scans.
//
// findDisjunctionPairForJoin returns ok=false if joinRel is not a join
// relation.
func (c *CustomFuncs) findDisjunctionPairForJoin(
	joinRel memo.RelExpr, filter *memo.FiltersItem, leftScan *memo.ScanExpr, rightScan *memo.ScanExpr,
) (left opt.ScalarExpr, right opt.ScalarExpr, ok bool) {
	if !opt.IsJoinOp(joinRel) {
		panic(errors.AssertionFailedf("expected joinRel to be a join operation"))
	}

	// isJoinPred tests if a predicate is a join predicate which references columns
	// from both leftRelColSet and rightRelColSet, as indicated in the predicate's
	// referenced columns, predCols.
	isJoinPred := func(predCols, leftRelColSet, rightRelColSet opt.ColSet) bool {
		return leftRelColSet.Intersects(predCols) && rightRelColSet.Intersects(predCols)
	}

	var leftExprs memo.ScalarListExpr
	var rightExprs memo.ScalarListExpr
	leftColSet := c.OutputCols(leftScan)
	rightColSet := c.OutputCols(rightScan)

	// hasJoinEquality returns true if an ANDed expression has at least one
	// equality join predicate.
	var hasJoinEquality func(opt.ScalarExpr) bool
	hasJoinEquality = func(expr opt.ScalarExpr) bool {
		switch t := expr.(type) {
		case *memo.AndExpr:
			return hasJoinEquality(t.Left) || hasJoinEquality(t.Right)
		case *memo.EqExpr:
			cols := c.OuterCols(expr)
			return isJoinPred(cols, leftColSet, rightColSet)
		default:
			return false
		}
	}

	// Traverse all adjacent OrExpr.
	var collect func(opt.ScalarExpr)
	collect = func(expr opt.ScalarExpr) {
		interesting := false
		switch t := expr.(type) {
		case *memo.OrExpr:
			collect(t.Left)
			collect(t.Right)
			return
		default:
			if c.e.evalCtx.SessionData().OptimizerUseImprovedSplitDisjunctionForJoins {
				interesting = true
			} else {
				interesting = hasJoinEquality(expr)
			}
		}

		if interesting && len(leftExprs) == 0 {
			leftExprs = append(leftExprs, expr)
		} else {
			rightExprs = append(rightExprs, expr)
		}
	}

	collect(filter.Condition)
	// Return an empty pair if either of the expression lists is empty.
	if len(leftExprs) == 0 ||
		len(rightExprs) == 0 {
		return nil, nil, false
	}

	return c.constructOr(leftExprs), c.constructOr(rightExprs), true
}

// CanMaybeGenerateLocalityOptimizedSearchOfLookupJoins performs precondition
// checks to see if generating a locality-optimized search of lookup joins is
// legal, and returns the input scan if the join input is a canonical scan or
// select from a canonical scan. It also returns the input filters, if any, and
// ok=true if all checks passed.
func (c *CustomFuncs) CanMaybeGenerateLocalityOptimizedSearchOfLookupJoins(
	lookupJoinExpr *memo.LookupJoinExpr,
) (inputScan *memo.ScanExpr, inputFilters memo.FiltersExpr, ok bool) {
	// Respect the session setting LocalityOptimizedSearch.
	if !c.e.evalCtx.SessionData().LocalityOptimizedSearch {
		return nil, memo.FiltersExpr{}, false
	}
	if lookupJoinExpr.LocalityOptimized || lookupJoinExpr.ChildOfLocalityOptimizedSearch {
		// Already locality optimized. Bail out.
		return nil, memo.FiltersExpr{}, false
	}
	// Don't try to handle paired joins for now.
	if lookupJoinExpr.IsFirstJoinInPairedJoiner || lookupJoinExpr.IsSecondJoinInPairedJoiner {
		return nil, memo.FiltersExpr{}, false
	}
	// This rewrite is only designed for inner join, left join and semijoin.
	if lookupJoinExpr.JoinType != opt.InnerJoinOp &&
		lookupJoinExpr.JoinType != opt.SemiJoinOp &&
		lookupJoinExpr.JoinType != opt.LeftJoinOp {
		return nil, memo.FiltersExpr{}, false
	}
	// Only rewrite canonical scans or selects from canonical scans, which also
	// means they are not locality-optimized.
	inputScan, inputFilters, ok = c.getfilteredCanonicalScan(lookupJoinExpr.Input)
	if !ok {
		return nil, memo.FiltersExpr{}, false
	}
	return inputScan, inputFilters, true
}

// LookupsAreLocal returns true if the lookups done by the given lookup join
// are done in the local region.
func (c *CustomFuncs) LookupsAreLocal(
	lookupJoinExpr *memo.LookupJoinExpr, required *physical.Required,
) bool {
	_, provided := distribution.BuildLookupJoinLookupTableDistribution(
		c.e.ctx, c.e.f.EvalContext(), c.e.mem, lookupJoinExpr, required, c.e.o.MaybeGetBestCostRelation,
	)
	if provided.Any() || len(provided.Regions) != 1 {
		return false
	}
	var localDist physical.Distribution
	localDist.FromLocality(c.e.f.EvalContext().Locality)
	return localDist.Equals(provided)
}

// GenerateLocalityOptimizedSearchOfLookupJoins generates a locality-optimized
// search on top of a `root` lookup join when the input relation to the lookup
// join is a REGIONAL BY ROW table. The left branch reads local rows from the
// input table and the right branch reads remote rows from the input table. If
// localityOptimizedLookupJoinPrivate is non-nil, then the local branch of the
// locality-optimized search uses this version of `root`, which has been
// converted into a locality-optimized `LookupJoinPrivate` by the caller.
func (c *CustomFuncs) GenerateLocalityOptimizedSearchOfLookupJoins(
	grp memo.RelExpr,
	required *physical.Required,
	lookupJoinExpr *memo.LookupJoinExpr,
	inputScan *memo.ScanExpr,
	inputFilters memo.FiltersExpr,
	localityOptimizedLookupJoinPrivate *memo.LookupJoinPrivate,
) {
	var localSelectFilters, remoteSelectFilters memo.FiltersExpr
	if len(inputFilters) > 0 {
		// Both local and remote branches must evaluate the original filters. Make
		// sure to limit the capacity to allow appending.
		localSelectFilters = inputFilters[:len(inputFilters):len(inputFilters)]
		remoteSelectFilters = inputFilters[:len(inputFilters):len(inputFilters)]
	}
	// We should only generate a locality-optimized search if there is a limit
	// hint coming from an ancestor expression with a LIMIT, meaning that the
	// query could be answered solely from rows returned by the local branch.
	if required.LimitHint == 0 {
		return
	}
	tabMeta := c.e.mem.Metadata().TableMeta(inputScan.Table)
	table := c.e.mem.Metadata().Table(inputScan.Table)
	lookupTable := c.e.mem.Metadata().Table(lookupJoinExpr.Table)
	duplicateLookupTableFirst := lookupJoinExpr.Table.ColumnID(0) < inputScan.Table.ColumnID(0)

	index := table.Index(inputScan.Index)
	ps := tabMeta.IndexPartitionLocality(inputScan.Index)
	if ps.Empty() {
		return
	}
	// Build optional filters from check constraint and computed column filters.
	// This should include the `crdb_region` IN (_region1_, _region2_, ...)
	// predicate.
	optionalFilters, _ :=
		c.GetOptionalFiltersAndFilterColumns(localSelectFilters, &inputScan.ScanPrivate)
	if len(optionalFilters) == 0 {
		return
	}

	// The `crdb_region` ColumnID
	firstIndexCol := inputScan.ScanPrivate.Table.IndexColumnID(index, 0)
	localFiltersItem, remoteFiltersItem, ok := c.getLocalAndRemoteFilters(optionalFilters, ps, firstIndexCol)
	if !ok {
		return
	}
	// Must have local and remote filters to proceed.
	if localFiltersItem == nil || remoteFiltersItem == nil {
		return
	}
	// Add the region-distinguishing filters to the original filters, for each
	// branch of the UNION ALL.
	localSelectFilters = append(localSelectFilters, *localFiltersItem.(*memo.FiltersItem))
	remoteSelectFilters = append(remoteSelectFilters, *remoteFiltersItem.(*memo.FiltersItem))

	localLookupJoin := lookupJoinExpr
	// If the caller specified to create locality-optimized join, use that
	// specification for the local branch. For the remote branch, there is no
	// point using locality-optimized join, because that would force a fetch of
	// remote rows to the local region followed by an attempt to join to local
	// rows first. In a properly designed schema, matching rows should be in the
	// same region in both tables, so it is likely cheaper to join the remote rows
	// with all regions in a single operation instead of 2 serial UNION ALL
	// branches.
	if localityOptimizedLookupJoinPrivate != nil {
		localLookupJoin = &memo.LookupJoinExpr{
			Input:             lookupJoinExpr.Input,
			On:                lookupJoinExpr.On,
			LookupJoinPrivate: *localityOptimizedLookupJoinPrivate,
		}
	}

	var lookupJoinLookupSideCols opt.ColSet
	for i := 0; i < lookupTable.ColumnCount(); i++ {
		lookupJoinLookupSideCols.Add(lookupJoinExpr.Table.ColumnID(i))
	}
	lookupTableSP := &memo.ScanPrivate{
		Table:   lookupJoinExpr.Table,
		Index:   lookupJoinExpr.Index,
		Cols:    lookupJoinLookupSideCols,
		Locking: lookupJoinExpr.Locking,
	}
	var newLocalLookupTableSP, newRemoteLookupTableSP *memo.ScanPrivate

	// Similar to DuplicateScanPrivate, but for the lookup table of lookup join.
	duplicateLookupSide := func() *memo.ScanPrivate {
		tableID, cols := c.DuplicateColumnIDs(lookupJoinExpr.Table, lookupJoinLookupSideCols)
		newLookupTableSP := &memo.ScanPrivate{
			Table:   tableID,
			Index:   lookupJoinExpr.Index,
			Cols:    cols,
			Locking: lookupJoinExpr.Locking,
		}
		return newLookupTableSP
	}

	// A new ScanPrivate, solely used for remapping columns.
	inputScanPrivateWithCRDBRegionCol :=
		&memo.ScanPrivate{
			Table:   inputScan.Table,
			Index:   inputScan.Index,
			Cols:    inputScan.Cols.Copy(),
			Flags:   inputScan.Flags,
			Locking: inputScan.Locking,
		}
	// Make sure the crdb_region column is included in the ScanPrivate and the
	// remapping.
	inputScanPrivateWithCRDBRegionCol.Cols.Add(firstIndexCol)

	var localInputSP, remoteInputSP *memo.ScanPrivate
	// Column IDs of the mapped tables must be in the same order as in the
	// original tables.
	if duplicateLookupTableFirst {
		newLocalLookupTableSP = duplicateLookupSide()
		newRemoteLookupTableSP = duplicateLookupSide()
		localInputSP = c.DuplicateScanPrivate(inputScanPrivateWithCRDBRegionCol)
		remoteInputSP = c.DuplicateScanPrivate(inputScanPrivateWithCRDBRegionCol)
	} else {
		localInputSP = c.DuplicateScanPrivate(inputScanPrivateWithCRDBRegionCol)
		remoteInputSP = c.DuplicateScanPrivate(inputScanPrivateWithCRDBRegionCol)
		newLocalLookupTableSP = duplicateLookupSide()
		newRemoteLookupTableSP = duplicateLookupSide()
	}
	localScan := c.e.f.ConstructScan(localInputSP).(*memo.ScanExpr)
	remoteScan := c.e.f.ConstructScan(remoteInputSP).(*memo.ScanExpr)

	localSelectFilters =
		c.RemapScanColsInFilter(localSelectFilters, inputScanPrivateWithCRDBRegionCol, &localScan.ScanPrivate)
	remoteSelectFilters =
		c.RemapScanColsInFilter(remoteSelectFilters, inputScanPrivateWithCRDBRegionCol, &remoteScan.ScanPrivate)

	localInput :=
		c.e.f.ConstructSelect(
			localScan,
			localSelectFilters,
		)
	remoteInput :=
		c.e.f.ConstructSelect(
			remoteScan,
			remoteSelectFilters,
		)

	// Map referenced column ids coming from the lookup table.
	lookupJoinWithLocalInput := c.mapInputSideOfLookupJoin(localLookupJoin, localScan, localInput)
	// The remote branch already incurs a latency penalty, so don't use
	// locality-optimized join for this branch. See the definition of
	// localLookupJoin for more details.
	lookupJoinWithRemoteInput := c.mapInputSideOfLookupJoin(lookupJoinExpr, remoteScan, remoteInput)
	// For costing and enforce_home_region, indicate these joins lie under a
	// locality-optimized search.
	lookupJoinWithLocalInput.ChildOfLocalityOptimizedSearch = true
	lookupJoinWithRemoteInput.ChildOfLocalityOptimizedSearch = true

	// Map referenced column ids coming from the input table.
	c.mapLookupJoin(lookupJoinWithLocalInput, lookupJoinLookupSideCols, newLocalLookupTableSP)
	c.mapLookupJoin(lookupJoinWithRemoteInput, lookupJoinLookupSideCols, newRemoteLookupTableSP)

	// Map the local and remote output columns.
	localColMap := c.makeColMap(inputScanPrivateWithCRDBRegionCol, &localScan.ScanPrivate)
	remoteColMap := c.makeColMap(inputScanPrivateWithCRDBRegionCol, &remoteScan.ScanPrivate)
	localJoinOutputCols := grp.Relational().OutputCols.CopyAndMaybeRemap(localColMap)
	remoteJoinOutputCols := grp.Relational().OutputCols.CopyAndMaybeRemap(remoteColMap)

	localLookupTableColMap := c.makeColMap(lookupTableSP, newLocalLookupTableSP)
	remoteLookupTableColMap := c.makeColMap(lookupTableSP, newRemoteLookupTableSP)
	localJoinOutputCols = localJoinOutputCols.CopyAndMaybeRemap(localLookupTableColMap)
	remoteJoinOutputCols = remoteJoinOutputCols.CopyAndMaybeRemap(remoteLookupTableColMap)

	localBranch := c.e.f.ConstructLookupJoin(lookupJoinWithLocalInput.Input,
		lookupJoinWithLocalInput.On,
		&lookupJoinWithLocalInput.LookupJoinPrivate,
	)
	remoteBranch := c.e.f.ConstructLookupJoin(lookupJoinWithRemoteInput.Input,
		lookupJoinWithRemoteInput.On,
		&lookupJoinWithRemoteInput.LookupJoinPrivate,
	)

	// Project away columns which weren't in the original output columns.
	if !localJoinOutputCols.Equals(localBranch.Relational().OutputCols) {
		localBranch = c.e.f.ConstructProject(localBranch, memo.ProjectionsExpr{}, localJoinOutputCols)
	}
	if !remoteJoinOutputCols.Equals(remoteBranch.Relational().OutputCols) {
		remoteBranch = c.e.f.ConstructProject(remoteBranch, memo.ProjectionsExpr{}, remoteJoinOutputCols)
	}

	sp :=
		c.e.funcs.MakeSetPrivate(
			localBranch.Relational().OutputCols,
			remoteBranch.Relational().OutputCols,
			grp.Relational().OutputCols,
		)
	// Add the LocalityOptimizedSearchExpr to the same group as the original join.
	locOptSearch := memo.LocalityOptimizedSearchExpr{
		Local:      localBranch,
		Remote:     remoteBranch,
		SetPrivate: *sp,
	}
	c.e.mem.AddLocalityOptimizedSearchToGroup(&locOptSearch, grp)
}

// GenerateLocalityOptimizedSearchLOJ generates a locality optimized search on
// top of a lookup join, `root`, when the input relation to the lookup join is a
// REGIONAL BY ROW table.
func (c *CustomFuncs) GenerateLocalityOptimizedSearchLOJ(
	grp memo.RelExpr,
	required *physical.Required,
	lookupJoinExpr *memo.LookupJoinExpr,
	inputScan *memo.ScanExpr,
	inputFilters memo.FiltersExpr,
) {
	if !c.LookupsAreLocal(lookupJoinExpr, required) {
		return
	}
	c.GenerateLocalityOptimizedSearchOfLookupJoins(grp, required, lookupJoinExpr, inputScan, inputFilters, nil)
}

// mapInputSideOfLookupJoin copies a lookupJoinExpr having a `ScanExpr` as input
// or a Select from a `ScanExpr` and replaces that input with `newInputRel`.
// `newInputScan` is expected to be duplicated from the original input scan. All
// references to column IDs in the lookup join expression belonging to the
// original scan are mapped to column IDs of the duplicated scan.
func (c *CustomFuncs) mapInputSideOfLookupJoin(
	lookupJoinExpr *memo.LookupJoinExpr, newInputScan *memo.ScanExpr, newInputRel memo.RelExpr,
) (mappedLookupJoinExpr *memo.LookupJoinExpr) {
	origInputRel := lookupJoinExpr.Input
	if origSelectExpr, ok := origInputRel.(*memo.SelectExpr); ok {
		origInputRel = origSelectExpr.Input
	}
	inputScan, ok := origInputRel.(*memo.ScanExpr)
	if !ok {
		panic(errors.AssertionFailedf("expected input of lookup join to be a scan"))
	}
	colMap := c.makeColMap(&inputScan.ScanPrivate, &newInputScan.ScanPrivate)
	newJP := c.DuplicateJoinPrivate(&lookupJoinExpr.JoinPrivate)
	mappedLookupJoinExpr =
		&memo.LookupJoinExpr{
			Input: newInputRel,
		}
	mappedLookupJoinExpr.JoinType = lookupJoinExpr.JoinType
	on := c.e.f.RemapCols(&lookupJoinExpr.On, colMap).(*memo.FiltersExpr)
	mappedLookupJoinExpr.On = *on
	mappedLookupJoinExpr.JoinPrivate = *newJP
	mappedLookupJoinExpr.JoinType = lookupJoinExpr.JoinType
	mappedLookupJoinExpr.Table = lookupJoinExpr.Table
	mappedLookupJoinExpr.Index = lookupJoinExpr.Index
	mappedLookupJoinExpr.KeyCols = lookupJoinExpr.KeyCols.CopyAndMaybeRemapColumns(colMap)
	mappedLookupJoinExpr.DerivedEquivCols = lookupJoinExpr.DerivedEquivCols.CopyAndMaybeRemap(colMap)

	c.e.f.DisableOptimizationsTemporarily(func() {
		// Disable normalization rules when remapping the lookup expressions so
		// that they do not get normalized into non-canonical lookup
		// expressions.
		lookupExpr := c.e.f.RemapCols(&lookupJoinExpr.LookupExpr, colMap).(*memo.FiltersExpr)
		mappedLookupJoinExpr.LookupExpr = *lookupExpr
		remoteLookupExpr := c.e.f.RemapCols(&lookupJoinExpr.RemoteLookupExpr, colMap).(*memo.FiltersExpr)
		mappedLookupJoinExpr.RemoteLookupExpr = *remoteLookupExpr
	})
	mappedLookupJoinExpr.Cols = lookupJoinExpr.Cols.Copy().Difference(inputScan.Cols).Union(newInputScan.Cols)
	mappedLookupJoinExpr.LookupColsAreTableKey = lookupJoinExpr.LookupColsAreTableKey
	mappedLookupJoinExpr.IsFirstJoinInPairedJoiner = lookupJoinExpr.IsFirstJoinInPairedJoiner
	mappedLookupJoinExpr.IsSecondJoinInPairedJoiner = lookupJoinExpr.IsSecondJoinInPairedJoiner
	mappedLookupJoinExpr.ContinuationCol = lookupJoinExpr.ContinuationCol
	mappedLookupJoinExpr.LocalityOptimized = lookupJoinExpr.LocalityOptimized
	mappedLookupJoinExpr.ChildOfLocalityOptimizedSearch = lookupJoinExpr.ChildOfLocalityOptimizedSearch
	allLookupFilters := c.e.f.RemapCols(&lookupJoinExpr.AllLookupFilters, colMap).(*memo.FiltersExpr)
	mappedLookupJoinExpr.AllLookupFilters = *allLookupFilters
	mappedLookupJoinExpr.Locking = lookupJoinExpr.Locking
	mappedLookupJoinExpr.RemoteOnlyLookups = lookupJoinExpr.RemoteOnlyLookups
	return mappedLookupJoinExpr
}
