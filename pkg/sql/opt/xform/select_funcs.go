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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// IsLocking returns true if the ScanPrivate is configured to use a row-level
// locking mode. This can be the case either because the Scan is in the scope of
// a SELECT .. FOR [KEY] UPDATE/SHARE clause or because the Scan was configured
// as part of the row retrieval of a DELETE or UPDATE statement.
func (c *CustomFuncs) IsLocking(scan *memo.ScanPrivate) bool {
	return scan.IsLocking()
}

// GeneratePartialIndexScans generates unconstrained index scans over all
// non-inverted, partial indexes with predicates that are implied by the
// filters. Partial indexes with predicates which cannot be proven to be implied
// by the filters are disregarded.
//
// When a filter completely matches the predicate, the remaining filters are
// simplified so that they do not include the filter. A redundant filter is
// unnecessary to include in the remaining filters because a scan over the partial
// index implicitly filters the results.
//
// For every partial index that is implied by the filters, a Scan will be
// generated along with a combination of an IndexJoin and Selects. There are
// three questions to consider which determine which operators are generated.
//
//   1. Does the index "cover" the columns needed?
//   2. Are there any remaining filters to apply after the Scan?
//   3. If there are remaining filters does the index cover the referenced
//      columns?
//
// If the index covers the columns needed, no IndexJoin is need. The two
// possible generated expressions are either a lone Scan or a Scan wrapped in a
// Select that applies any remaining filters.
//
//       (Scan $scanDef)
//
//       (Select (Scan $scanDef) $remainingFilters)
//
// If the index is not covering, then an IndexJoin is required to retrieve the
// needed columns. Some or all of the remaining filters may be required to be
// applied after the IndexJoin, because they reference columns not covered by
// the index. Therefore, Selects can be constructed before, after, or both
// before and after the IndexJoin depending on the columns referenced in the
// remaining filters.
//
// If the index is not covering, then an IndexJoin is required to retrieve the
// needed columns. Some of the remaining filters may be applied in a Select
// before the IndexJoin, if all the columns referenced in the filter are covered
// by the index. Some of the remaining filters may be applied in a Select after
// the IndexJoin, if their columns are not covered. Therefore, Selects can be
// constructed before, after, or both before and after the IndexJoin.
//
//       (IndexJoin (Scan $scanDef) $indexJoinDef)
//
//       (IndexJoin
//         (Select (Scan $scanDef) $remainingFilters)
//         $indexJoinDef
//       )
//
//      (Select
//        (IndexJoin (Scan $scanDef) $indexJoinDef)
//        $outerFilter
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
func (c *CustomFuncs) GeneratePartialIndexScans(
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate, filters memo.FiltersExpr,
) {
	// Iterate over all partial indexes.
	var pkCols opt.ColSet
	var iter scanIndexIter
	iter.Init(c.e.evalCtx, c.e.f, c.e.mem, &c.im, scanPrivate, filters, rejectNonPartialIndexes|rejectInvertedIndexes)
	iter.ForEach(func(index cat.Index, remainingFilters memo.FiltersExpr, indexCols opt.ColSet, isCovering bool, constProj memo.ProjectionsExpr) {
		var sb indexScanBuilder
		sb.Init(c, scanPrivate.Table)
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = index.Ordinal()
		newScanPrivate.Cols = indexCols.Intersection(scanPrivate.Cols)

		// If index is covering, just add a Select with the remaining filters,
		// if there are any.
		if isCovering {
			sb.SetScan(&newScanPrivate)
			sb.AddConstProjections(constProj)
			sb.AddSelect(remainingFilters)
			sb.Build(grp)
			return
		}

		// Calculate the PK columns once.
		if pkCols.Empty() {
			pkCols = c.PrimaryKeyCols(scanPrivate.Table)
		}

		// If the index is not covering, scan the needed index columns plus
		// primary key columns.
		newScanPrivate.Cols.UnionWith(pkCols)
		sb.SetScan(&newScanPrivate)

		// Add a Select with any remaining filters that can be filtered before
		// the IndexJoin. If there are no remaining filters this is a no-op. If
		// all or parts of the remaining filters cannot be applied until after
		// the IndexJoin, the new value of remainingFilters will contain those
		// filters.
		remainingFilters = sb.AddSelectAfterSplit(remainingFilters, newScanPrivate.Cols)

		// Add an IndexJoin to retrieve the columns not provided by the Scan.
		sb.AddIndexJoin(scanPrivate.Cols)

		// Add a Select with any remaining filters.
		sb.AddSelect(remainingFilters)
		sb.Build(grp)
	})
}

// GenerateConstrainedScans enumerates all non-inverted secondary indexes on the
// Scan operator's table and tries to push the given Select filter into new
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
// GenerateConstrainedScans will further constrain the enumerated index scans
// by trying to use the check constraints and computed columns that apply to the
// table being scanned, as well as the partitioning defined for the index. See
// comments above checkColumnFilters, computedColFilters, and
// partitionValuesFilters for more detail.
func (c *CustomFuncs) GenerateConstrainedScans(
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate, explicitFilters memo.FiltersExpr,
) {
	var pkCols opt.ColSet
	var sb indexScanBuilder
	sb.Init(c, scanPrivate.Table)

	// Generate implicit filters from constraints and computed columns as
	// optional filters to help constrain an index scan.
	optionalFilters := c.checkConstraintFilters(scanPrivate.Table)
	computedColFilters := c.computedColFilters(scanPrivate, explicitFilters, optionalFilters)
	optionalFilters = append(optionalFilters, computedColFilters...)

	filterColumns := c.FilterOuterCols(explicitFilters)
	filterColumns.UnionWith(c.FilterOuterCols(optionalFilters))

	// Iterate over all non-inverted indexes.
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(scanPrivate.Table)
	var iter scanIndexIter
	iter.Init(c.e.evalCtx, c.e.f, c.e.mem, &c.im, scanPrivate, explicitFilters, rejectInvertedIndexes)
	iter.ForEach(func(index cat.Index, filters memo.FiltersExpr, indexCols opt.ColSet, isCovering bool, constProj memo.ProjectionsExpr) {
		// We only consider the partition values when a particular index can otherwise
		// not be constrained. For indexes that are constrained, the partitioned values
		// add no benefit as they don't really constrain anything.
		// Furthermore, if the filters don't take advantage of the index (use any of the
		// index columns), using the partition values add no benefit.
		//
		// If the index is partitioned (by list), we generate two constraints and
		// union them: the "main" constraint and the "in-between" constraint.The
		// "main" constraint restricts the index to the known partition ranges. The
		// "in-between" constraint restricts the index to the rest of the ranges
		// (i.e. everything that falls in-between the main ranges); the in-between
		// constraint is necessary for correctness (there can be rows outside of the
		// partitioned ranges).
		//
		// For both constraints, the partition-related filters are passed as
		// "optional" which guarantees that they return no remaining filters. This
		// allows us to merge the remaining filters from both constraints.
		//
		// Consider the following index and its partition:
		//
		// CREATE INDEX orders_by_seq_num
		//     ON orders (region, seq_num, id)
		//     STORING (total)
		//     PARTITION BY LIST (region)
		//         (
		//             PARTITION us_east1 VALUES IN ('us-east1'),
		//             PARTITION us_west1 VALUES IN ('us-west1'),
		//             PARTITION europe_west2 VALUES IN ('europe-west2')
		//         )
		//
		// The constraint generated for the query:
		//   SELECT sum(total) FROM orders WHERE seq_num >= 100 AND seq_num < 200
		// is:
		//   [/'europe-west2'/100 - /'europe-west2'/199]
		//   [/'us-east1'/100 - /'us-east1'/199]
		//   [/'us-west1'/100 - /'us-west1'/199]
		//
		// The spans before europe-west2, after us-west1 and in between the defined
		// partitions are missing. We must add these spans now, appropriately
		// constrained using the filters.
		//
		// It is important that we add these spans after the partition spans are generated
		// because otherwise these spans would merge with the partition spans and would
		// disallow the partition spans (and the in between ones) to be constrained further.
		// Using the partitioning example and the query above, if we added the in between
		// spans at the same time as the partitioned ones, we would end up with a span that
		// looked like:
		//   [ - /'europe-west2'/99]
		//
		// Allowing the partition spans to be constrained further and then adding
		// the spans give us a more constrained index scan as shown below:
		//   [ - /'europe-west2')
		//   [/'europe-west2'/100 - /'europe-west2'/199]
		//   [/e'europe-west2\x00'/100 - /'us-east1')
		//   [/'us-east1'/100 - /'us-east1'/199]
		//   [/e'us-east1\x00'/100 - /'us-west1')
		//   [/'us-west1'/100 - /'us-west1'/199]
		//   [/e'us-west1\x00'/100 - ]
		//
		// Notice how we 'skip' all the europe-west2 rows with seq_num < 100.
		//
		var partitionFilters, inBetweenFilters memo.FiltersExpr

		indexColumns := tabMeta.IndexKeyColumns(index.Ordinal())
		firstIndexCol := scanPrivate.Table.IndexColumnID(index, 0)
		if !filterColumns.Contains(firstIndexCol) && indexColumns.Intersects(filterColumns) {
			// Calculate any partition filters if appropriate (see below).
			partitionFilters, inBetweenFilters = c.partitionValuesFilters(scanPrivate.Table, index)
		}

		// Check whether the filter (along with any partitioning filters) can constrain the index.
		constraint, remainingFilters, ok := c.tryConstrainIndex(
			filters,
			append(optionalFilters, partitionFilters...),
			scanPrivate.Table,
			index.Ordinal(),
		)
		if !ok {
			return
		}

		if len(partitionFilters) > 0 {
			inBetweenConstraint, inBetweenRemainingFilters, ok := c.tryConstrainIndex(
				filters,
				append(optionalFilters, inBetweenFilters...),
				scanPrivate.Table,
				index.Ordinal(),
			)
			if !ok {
				panic(errors.AssertionFailedf("in-between filters didn't yield a constraint"))
			}

			constraint.UnionWith(c.e.evalCtx, inBetweenConstraint)

			// Even though the partitioned constraints and the inBetween constraints
			// were consolidated, we must make sure their Union is as well.
			constraint.ConsolidateSpans(c.e.evalCtx)

			// Add all remaining filters that need to be present in the
			// inBetween spans. Some of the remaining filters are common
			// between them, so we must deduplicate them.
			remainingFilters = c.ConcatFilters(remainingFilters, inBetweenRemainingFilters)
			remainingFilters.Sort()
			remainingFilters.Deduplicate()
		}

		// Construct new constrained ScanPrivate.
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = index.Ordinal()
		newScanPrivate.Cols = indexCols.Intersection(scanPrivate.Cols)
		newScanPrivate.SetConstraint(c.e.evalCtx, constraint)
		// Record whether we were able to use partitions to constrain the scan.
		newScanPrivate.PartitionConstrainedScan = (len(partitionFilters) > 0)

		// If the alternate index includes the set of needed columns, then
		// construct a new Scan operator using that index.
		if isCovering {
			sb.SetScan(&newScanPrivate)

			// Project constants from partial index predicate filters, if there
			// are any.
			sb.AddConstProjections(constProj)

			// If there are remaining filters, then the constrained Scan operator
			// will be created in a new group, and a Select operator will be added
			// to the same group as the original operator.
			sb.AddSelect(remainingFilters)

			sb.Build(grp)
			return
		}

		// Otherwise, construct an IndexJoin operator that provides the columns
		// missing from the index.
		if scanPrivate.Flags.NoIndexJoin {
			return
		}

		// Calculate the PK columns once.
		if pkCols.Empty() {
			pkCols = c.PrimaryKeyCols(scanPrivate.Table)
		}

		// If the index is not covering, scan the needed index columns plus
		// primary key columns.
		newScanPrivate.Cols.UnionWith(pkCols)
		sb.SetScan(&newScanPrivate)

		// If remaining filter exists, split it into one part that can be pushed
		// below the IndexJoin, and one part that needs to stay above.
		remainingFilters = sb.AddSelectAfterSplit(remainingFilters, newScanPrivate.Cols)
		sb.AddIndexJoin(scanPrivate.Cols)
		sb.AddSelect(remainingFilters)

		sb.Build(grp)
	})
}

// tryFoldComputedCol tries to reduce the computed column with the given column
// ID into a constant value, by evaluating it with respect to a set of other
// columns that are constant. If the computed column is constant, enter it into
// the constCols map and return false. Otherwise, return false.
func (c *CustomFuncs) tryFoldComputedCol(
	tabMeta *opt.TableMeta, computedColID opt.ColumnID, constCols map[opt.ColumnID]opt.ScalarExpr,
) bool {
	// Check whether computed column has already been folded.
	if _, ok := constCols[computedColID]; ok {
		return true
	}

	var replace func(e opt.Expr) opt.Expr
	replace = func(e opt.Expr) opt.Expr {
		if variable, ok := e.(*memo.VariableExpr); ok {
			// Can variable be folded?
			if constVal, ok := constCols[variable.Col]; ok {
				// Yes, so replace it with its constant value.
				return constVal
			}

			// No, but that may be because the variable refers to a dependent
			// computed column. In that case, try to recursively fold that
			// computed column. There are no infinite loops possible because the
			// dependency graph is guaranteed to be acyclic.
			if _, ok := tabMeta.ComputedCols[variable.Col]; ok {
				if c.tryFoldComputedCol(tabMeta, variable.Col, constCols) {
					return constCols[variable.Col]
				}
			}

			return e
		}
		return c.e.f.Replace(e, replace)
	}

	computedCol := tabMeta.ComputedCols[computedColID]
	replaced := replace(computedCol).(opt.ScalarExpr)

	// If the computed column is constant, enter it into the constCols map.
	if opt.IsConstValueOp(replaced) {
		constCols[computedColID] = replaced
		return true
	}
	return false
}

// inBetweenFilters returns a set of filters that are required to cover all the
// in-between spans given a set of partition values. This is required for
// correctness reasons; although values are unlikely to exist between defined
// partitions, they may exist and so the constraints of the scan must incorporate
// these spans.
//
// For example, if we have:
//   PARTITION BY LIST (a, b) (
//     PARTITION a VALUES IN ((1, 10)),
//     PARTITION b VALUES IN ((2, 20)),
//   )
// The in-between filters are:
//   (a, b) < (1, 10) OR
//   ((a, b) > (1, 10) AND (a, b) < (2, 20)) OR
//   (a, b) > (2, 20)
//
// When passed as optional filters to index constrains, these filters generate
// the desired spans:
//   [ - /1/10), (/1/10 - /2/20), (2/20 - ].
//
// TODO(radu,mgartner): technically these filters are not correct with respect
// to NULL values - we would want the tuple comparisons to treat NULLs as the
// smallest value. We compensate for this by adding an (a IS NULL) disjunct if
// column `a` is nullable; in addition, we know that these filters will only be
// used for span generation, and the span generation currently doesn't exclude
// NULLs on any columns other than the first in the tuple. This is fragile and
// would break if we improve the span generation for tuple inequalities.
func (c *CustomFuncs) inBetweenFilters(
	tabID opt.TableID, index cat.Index, partitionValues []tree.Datums,
) memo.FiltersExpr {
	var inBetween memo.ScalarListExpr

	if len(partitionValues) == 0 {
		return memo.EmptyFiltersExpr
	}

	// Sort the partitionValues lexicographically.
	sort.Slice(partitionValues, func(i, j int) bool {
		return partitionValues[i].Compare(c.e.evalCtx, partitionValues[j]) < 0
	})

	// The beginExpr created below will not include NULL values for the first
	// column. For example, with an index on (a, b) and partition values of
	// {'foo', 'bar'}, beginExpr would be (a, b) < ('foo', 'bar') which
	// evaluates to NULL if a is NULL. The index constraint span generated for
	// beginExpr expression would be (/NULL - /'foo'/'bar'), which excludes NULL
	// values for a. Any rows where a is NULL would not be scanned, producing
	// incorrect query results.
	//
	// Therefore, if the first column in the index is nullable, we add an IS
	// NULL expression so that NULL values are included in the index constraint
	// span generated later on. In the same example above, the constraint span
	// above becomes [/NULL - /'foo'/'bar'), which includes NULL values for a.
	//
	// This is only required for the first column in the index because the index
	// constraint spans built for tuple inequalities only exclude NULL values
	// for the first element in the tuple.
	firstCol := index.Column(0)
	if firstCol.IsNullable() {
		nullExpr := c.e.f.ConstructIs(
			c.e.f.ConstructVariable(tabID.ColumnID(firstCol.Ordinal())),
			memo.NullSingleton,
		)
		inBetween = append(inBetween, nullExpr)
	}

	// Add the beginning span.
	beginExpr := c.columnComparison(tabID, index, partitionValues[0], -1)
	inBetween = append(inBetween, beginExpr)

	// Add the end span.
	endExpr := c.columnComparison(tabID, index, partitionValues[len(partitionValues)-1], 1)
	inBetween = append(inBetween, endExpr)

	// Add the in-between spans.
	for i := 1; i < len(partitionValues); i++ {
		lowerPartition := partitionValues[i-1]
		higherPartition := partitionValues[i]

		// The between spans will be greater than the lower partition but smaller
		// than the higher partition.
		var largerThanLower opt.ScalarExpr
		if c.isPrefixOf(lowerPartition, higherPartition) {

			// Since the lower partition is a prefix of the higher partition, the span
			// must begin with the values defined in the lower partition. Consider the
			// partitions ('us') and ('us', 'cali'). In this case the in-between span
			// should be [/'us - /'us'/'cali').
			largerThanLower = c.columnComparison(tabID, index, lowerPartition, 0)
		} else {
			largerThanLower = c.columnComparison(tabID, index, lowerPartition, 1)
		}

		smallerThanHigher := c.columnComparison(tabID, index, higherPartition, -1)

		// Add the in-between span to the list of inBetween spans.
		betweenExpr := c.e.f.ConstructAnd(largerThanLower, smallerThanHigher)
		inBetween = append(inBetween, betweenExpr)
	}

	// Return an Or expression between all the expressions.
	return memo.FiltersExpr{c.e.f.ConstructFiltersItem(c.constructOr(inBetween))}
}

// constructOr constructs an expression that is an OR between all the
// provided conditions
func (c *CustomFuncs) constructOr(conditions memo.ScalarListExpr) opt.ScalarExpr {
	if len(conditions) == 0 {
		return c.e.f.ConstructFalse()
	}

	orExpr := conditions[0]
	for i := 1; i < len(conditions); i++ {
		orExpr = c.e.f.ConstructOr(conditions[i], orExpr)
	}

	return orExpr
}

// columnComparison returns a filter that compares the index columns to the
// given values. The comp parameter can be -1, 0 or 1 to indicate whether the
// comparison type of the filter should be a Lt, Eq or Gt.
func (c *CustomFuncs) columnComparison(
	tabID opt.TableID, index cat.Index, values tree.Datums, comp int,
) opt.ScalarExpr {
	colTypes := make([]*types.T, len(values))
	for i := range values {
		colTypes[i] = values[i].ResolvedType()
	}

	columnVariables := make(memo.ScalarListExpr, len(values))
	scalarValues := make(memo.ScalarListExpr, len(values))

	for i, val := range values {
		colID := tabID.IndexColumnID(index, i)
		columnVariables[i] = c.e.f.ConstructVariable(colID)
		scalarValues[i] = c.e.f.ConstructConstVal(val, val.ResolvedType())
	}

	colsTuple := c.e.f.ConstructTuple(columnVariables, types.MakeTuple(colTypes))
	valsTuple := c.e.f.ConstructTuple(scalarValues, types.MakeTuple(colTypes))
	if comp == 0 {
		return c.e.f.ConstructEq(colsTuple, valsTuple)
	} else if comp > 0 {
		return c.e.f.ConstructGt(colsTuple, valsTuple)
	}

	return c.e.f.ConstructLt(colsTuple, valsTuple)
}

// inPartitionFilters returns a FiltersExpr that is required to cover
// all the partition spans. For each partition defined, inPartitionFilters
// will contain a FilterItem that restricts the index columns by
// the partition values. Use inBetweenFilters to generate filters that
// cover all the spans that the partitions don't cover.
func (c *CustomFuncs) inPartitionFilters(
	tabID opt.TableID, index cat.Index, partitionValues []tree.Datums,
) memo.FiltersExpr {
	var partitions memo.ScalarListExpr

	// Sort the partition values so the most selective ones are first.
	sort.Slice(partitionValues, func(i, j int) bool {
		return len(partitionValues[i]) >= len(partitionValues[j])
	})

	// Construct all the partition filters.
	for i, partition := range partitionValues {

		// Only add this partition if a more selective partition hasn't
		// been defined on the same partition.
		partitionSeen := false
		for j, moreSelectivePartition := range partitionValues {
			if j >= i {
				break
			}

			// At this point we know whether the current partition was seen before.
			partitionSeen = c.isPrefixOf(partition, moreSelectivePartition)
			if partitionSeen {
				break
			}
		}

		// This partition is a prefix of a more selective partition and so,
		// will be taken care of by the in-between partitions.
		if partitionSeen {
			continue
		}

		// Get an expression that restricts the values of the index to the
		// partition values.
		inPartition := c.columnComparison(tabID, index, partition, 0)
		partitions = append(partitions, inPartition)
	}

	// Return an Or expression between all the expressions.
	return memo.FiltersExpr{c.e.f.ConstructFiltersItem(c.constructOr(partitions))}
}

// isPrefixOf returns whether pre is a prefix of other.
func (c *CustomFuncs) isPrefixOf(pre []tree.Datum, other []tree.Datum) bool {
	if len(pre) > len(other) {
		// Pre can't be a prefix of other as it is larger.
		return false
	}
	for i := range pre {
		if pre[i].Compare(c.e.evalCtx, other[i]) != 0 {
			return false
		}
	}

	return true
}

// partitionValuesFilters constructs filters with the purpose of
// constraining an index scan using the partition values similar to
// the filters added from the check constraints (see
// checkConstraintFilters). It returns two sets of filters, one to
// create the partition spans, and one to create the spans for all
// the in between ranges that are not part of any partitions.
//
// For example consider the following table and partitioned index:
//
// CREATE TABLE orders (
//     region STRING NOT NULL, id INT8 NOT NULL, total DECIMAL NOT NULL, seq_num INT NOT NULL,
//     PRIMARY KEY (region, id)
// )
//
// CREATE INDEX orders_by_seq_num
//     ON orders (region, seq_num, id)
//     STORING (total)
//     PARTITION BY LIST (region)
//         (
//             PARTITION us_east1 VALUES IN ('us-east1'),
//             PARTITION us_west1 VALUES IN ('us-west1'),
//             PARTITION europe_west2 VALUES IN ('europe-west2')
//         )
//
// Now consider the following query:
// SELECT sum(total) FROM orders WHERE seq_num >= 100 AND seq_num < 200
//
// Normally, the index would not be utilized but because we know what the
// partition values are for the prefix of the index, we can generate
// filters that allow us to use the index (adding the appropriate in-between
// filters to catch all the values that are not part of the partitions).
// By doing so, we get the following plan:
// scalar-group-by
//  ├── select
//  │    ├── scan orders@orders_by_seq_num
//  │    │    └── constraint: /1/4/2: [ - /'europe-west2')
//  │    │                            [/'europe-west2'/100 - /'europe-west2'/199]
//  │    │                            [/e'europe-west2\x00'/100 - /'us-east1')
//  │    │                            [/'us-east1'/100 - /'us-east1'/199]
//  │    │                            [/e'us-east1\x00'/100 - /'us-west1')
//  │    │                            [/'us-west1'/100 - /'us-west1'/199]
//  │    │                            [/e'us-west1\x00'/100 - ]
//  │    └── filters
//  │         └── (seq_num >= 100) AND (seq_num < 200)
//  └── aggregations
//       └── sum
//            └── variable: total
//
func (c *CustomFuncs) partitionValuesFilters(
	tabID opt.TableID, index cat.Index,
) (partitionFilter, inBetweenFilter memo.FiltersExpr) {

	// Find all the partition values
	partitionValues := make([]tree.Datums, 0, index.PartitionCount())
	for i, n := 0, index.PartitionCount(); i < n; i++ {
		partitionValues = append(partitionValues, index.Partition(i).PartitionByListPrefixes()...)
	}
	if len(partitionValues) == 0 {
		return partitionFilter, inBetweenFilter
	}

	// Get the in partition expressions.
	inPartition := c.inPartitionFilters(tabID, index, partitionValues)

	// Get the in between expressions.
	inBetween := c.inBetweenFilters(tabID, index, partitionValues)

	return inPartition, inBetween
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
	var pkCols opt.ColSet
	var sb indexScanBuilder
	sb.Init(c, scanPrivate.Table)
	tabMeta := c.e.mem.Metadata().TableMeta(scanPrivate.Table)

	// Generate implicit filters from constraints and computed columns as
	// optional filters to help constrain an index scan.
	optionalFilters := c.checkConstraintFilters(scanPrivate.Table)
	computedColFilters := c.computedColFilters(scanPrivate, filters, optionalFilters)
	optionalFilters = append(optionalFilters, computedColFilters...)

	// Iterate over all inverted indexes.
	var iter scanIndexIter
	iter.Init(c.e.evalCtx, c.e.f, c.e.mem, &c.im, scanPrivate, filters, rejectNonInvertedIndexes)
	iter.ForEach(func(index cat.Index, filters memo.FiltersExpr, indexCols opt.ColSet, _ bool, _ memo.ProjectionsExpr) {
		// Check whether the filter can constrain the index.
		spanExpr, constraint, remainingFilters, pfState, ok := invertedidx.TryFilterInvertedIndex(
			c.e.evalCtx, c.e.f, filters, optionalFilters, scanPrivate.Table, index, tabMeta.ComputedCols,
		)
		if !ok {
			// A span expression to constrain the inverted index could not be
			// generated.
			return
		}
		spansToRead := spanExpr.SpansToRead
		// Override the filters with remainingFilters. If the index is a
		// multi-column inverted index, the non-inverted prefix columns are
		// constrained by the constraint. In this case, it may be possible to
		// reduce the filters if the constraint fully describes some of
		// sub-expressions. The remainingFilters are the filters that are not
		// fully expressed by the constraint.
		//
		// Consider the example:
		//
		//   CREATE TABLE t (a INT, b INT, g GEOMETRY, INVERTED INDEX (b, g))
		//
		//   SELECT * FROM t WHERE a = 1 AND b = 2 AND ST_Intersects(.., g)
		//
		// The constraint would constrain b to [/2 - /2], guaranteeing that
		// the inverted index scan would only produce rows where (b = 2).
		// Reapplying the (b = 2) filter after the scan would be
		// unnecessary, so the remainingFilters in this case would be
		// (a = 1 AND ST_Intersects(.., g)).
		filters = remainingFilters

		// Construct new ScanOpDef with the new index and constraint.
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = index.Ordinal()
		newScanPrivate.SetConstraint(c.e.evalCtx, constraint)
		newScanPrivate.InvertedConstraint = spansToRead

		// Calculate the PK columns once.
		if pkCols.Empty() {
			pkCols = c.PrimaryKeyCols(scanPrivate.Table)
		}

		// We will need an inverted filter above the scan if the spanExpr might
		// produce duplicate primary keys or requires at least one UNION or
		// INTERSECTION. In this case, we must scan both the primary key columns
		// and the inverted key column.
		needInvertedFilter := !spanExpr.Unique || spanExpr.Operator != inverted.None
		newScanPrivate.Cols = pkCols.Copy()
		var invertedCol opt.ColumnID
		if needInvertedFilter {
			invertedCol = scanPrivate.Table.ColumnID(index.VirtualInvertedColumn().Ordinal())
			newScanPrivate.Cols.Add(invertedCol)
		}

		// The Scan operator always goes in a new group, since it's always nested
		// underneath the IndexJoin. The IndexJoin may also go into its own group,
		// if there's a remaining filter above it.
		// TODO(mgartner): We don't always need to create an index join. The
		// index join will be removed by EliminateIndexJoinInsideProject, but
		// it'd be more efficient to not create the index join in the first
		// place.
		sb.SetScan(&newScanPrivate)

		// Add an inverted filter if needed.
		if needInvertedFilter {
			sb.AddInvertedFilter(spanExpr, pfState, invertedCol)
		}

		// If remaining filter exists, split it into one part that can be pushed
		// below the IndexJoin, and one part that needs to stay above.
		filters = sb.AddSelectAfterSplit(filters, pkCols)
		sb.AddIndexJoin(scanPrivate.Cols)
		sb.AddSelect(filters)

		sb.Build(grp)
	})
}

// tryConstrainIndex tries to derive a constraint for the given index from the
// specified filter. If a constraint is derived, it is returned along with any
// filter remaining after extracting the constraint. If no constraint can be
// derived, then tryConstrainIndex returns ok = false.
func (c *CustomFuncs) tryConstrainIndex(
	requiredFilters, optionalFilters memo.FiltersExpr, tabID opt.TableID, indexOrd int,
) (constraint *constraint.Constraint, remainingFilters memo.FiltersExpr, ok bool) {
	// Start with fast check to rule out indexes that cannot be constrained.
	if !c.canMaybeConstrainNonInvertedIndex(requiredFilters, tabID, indexOrd) &&
		!c.canMaybeConstrainNonInvertedIndex(optionalFilters, tabID, indexOrd) {
		return nil, nil, false
	}

	ic := c.initIdxConstraintForIndex(requiredFilters, optionalFilters, tabID, indexOrd)
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

// canMaybeConstrainNonInvertedIndex returns true if we should try to constrain
// a given non-inverted index by the given filter. It returns false if it is
// impossible for the filter can constrain the scan.
//
// If any of the three following statements are true, then it is
// possible that the index can be constrained:
//
//   1. The filter references the first index column.
//   2. The constraints are not tight (see props.Scalar.TightConstraints).
//   3. Any of the filter's constraints start with the first index column.
//
func (c *CustomFuncs) canMaybeConstrainNonInvertedIndex(
	filters memo.FiltersExpr, tabID opt.TableID, indexOrd int,
) bool {
	md := c.e.mem.Metadata()
	index := md.Table(tabID).Index(indexOrd)

	for i := range filters {
		filterProps := filters[i].ScalarProps()

		// If the filter involves the first index column, then the index can
		// possibly be constrained.
		firstIndexCol := tabID.IndexColumnID(index, 0)
		if filterProps.OuterCols.Contains(firstIndexCol) {
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
	tab := c.e.mem.Metadata().Table(scanPrivate.Table)

	// Short circuit unless zigzag joins are explicitly enabled.
	if !c.e.evalCtx.SessionData.ZigzagJoinEnabled {
		return
	}

	fixedCols := memo.ExtractConstColumns(filters, c.e.evalCtx)

	if fixedCols.Len() < 2 {
		// Zigzagging requires at least 2 columns to have fixed values.
		return
	}

	// Zigzag joins aren't currently equipped to produce system columns, so
	// don't generate any if some system columns are requested.
	foundSystemCol := false
	scanPrivate.Cols.ForEach(func(colID opt.ColumnID) {
		if tab.Column(scanPrivate.Table.ColumnOrdinal(colID)).Kind() == cat.System {
			foundSystemCol = true
		}
	})
	if foundSystemCol {
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
	//
	// TODO(mgartner): We should consider primary indexes when it has multiple
	// columns and only the first is being constrained.
	var iter scanIndexIter
	iter.Init(c.e.evalCtx, c.e.f, c.e.mem, &c.im, scanPrivate, filters, rejectPrimaryIndex|rejectInvertedIndexes)
	iter.ForEach(func(leftIndex cat.Index, outerFilters memo.FiltersExpr, leftCols opt.ColSet, _ bool, _ memo.ProjectionsExpr) {
		leftFixed := c.indexConstrainedCols(leftIndex, scanPrivate.Table, fixedCols)
		// Short-circuit quickly if the first column in the index is not a fixed
		// column.
		if leftFixed.Len() == 0 {
			return
		}

		var iter2 scanIndexIter
		iter2.Init(c.e.evalCtx, c.e.f, c.e.mem, &c.im, scanPrivate, outerFilters, rejectPrimaryIndex|rejectInvertedIndexes)
		iter2.SetOriginalFilters(filters)
		iter2.ForEachStartingAfter(leftIndex.Ordinal(), func(rightIndex cat.Index, innerFilters memo.FiltersExpr, rightCols opt.ColSet, _ bool, _ memo.ProjectionsExpr) {
			rightFixed := c.indexConstrainedCols(rightIndex, scanPrivate.Table, fixedCols)
			// If neither side contributes a fixed column not contributed by the
			// other, then there's no reason to zigzag on this pair of indexes.
			if leftFixed.SubsetOf(rightFixed) || rightFixed.SubsetOf(leftFixed) {
				return
			}

			// Columns that are in both indexes are, by definition, equal.
			eqCols := leftCols.Intersection(rightCols)
			eqCols.DifferenceWith(fixedCols)
			if eqCols.Len() == 0 {
				// A simple index join is more efficient in such cases.
				return
			}

			// If there are any equalities across the columns of the two indexes,
			// push them into the zigzag join spec.
			leftEq, rightEq := memo.ExtractJoinEqualityColumns(
				leftCols, rightCols, innerFilters,
			)
			leftEqCols, rightEqCols := eqColsForZigzag(
				tab,
				scanPrivate.Table,
				leftIndex,
				rightIndex,
				fixedCols,
				leftEq,
				rightEq,
			)

			if len(leftEqCols) == 0 || len(rightEqCols) == 0 {
				// One of the indexes is not sorted by any of the equality
				// columns, because the equality columns do not immediately
				// succeed the fixed columns. A zigzag join cannot be planned.
				return
			}

			// Confirm the primary key columns are in both leftEqCols and
			// rightEqCols. The conversion of a select with filters to a
			// zigzag join requires the primary key columns to be in the output
			// for output correctness; otherwise, we could be outputting more
			// results than there should be (due to an equality on a non-unique
			// non-required value).
			pkIndex := tab.Index(cat.PrimaryIndex)
			pkCols := make(opt.ColList, pkIndex.KeyColumnCount())
			pkColsFound := true
			for i := range pkCols {
				pkCols[i] = scanPrivate.Table.IndexColumnID(pkIndex, i)

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
				return
			}

			leftFixedCols, leftVals, leftTypes := c.fixedColsForZigzag(
				leftIndex, scanPrivate.Table, innerFilters,
			)
			rightFixedCols, rightVals, rightTypes := c.fixedColsForZigzag(
				rightIndex, scanPrivate.Table, innerFilters,
			)

			// If the fixed cols have been reduced during partial index
			// implication, then a zigzag join cannot be planned. A single index
			// scan should be more efficient.
			if len(leftFixedCols) != leftFixed.Len() || len(rightFixedCols) != rightFixed.Len() {
				return
			}

			zigzagJoin := memo.ZigzagJoinExpr{
				On: innerFilters,
				ZigzagJoinPrivate: memo.ZigzagJoinPrivate{
					LeftTable:      scanPrivate.Table,
					LeftIndex:      leftIndex.Ordinal(),
					RightTable:     scanPrivate.Table,
					RightIndex:     rightIndex.Ordinal(),
					LeftEqCols:     leftEqCols,
					RightEqCols:    rightEqCols,
					LeftFixedCols:  leftFixedCols,
					RightFixedCols: rightFixedCols,
				},
			}

			leftTupleTyp := types.MakeTuple(leftTypes)
			rightTupleTyp := types.MakeTuple(rightTypes)
			zigzagJoin.FixedVals = memo.ScalarListExpr{
				c.e.f.ConstructTuple(leftVals, leftTupleTyp),
				c.e.f.ConstructTuple(rightVals, rightTupleTyp),
			}

			zigzagJoin.On = memo.ExtractRemainingJoinFilters(
				innerFilters,
				zigzagJoin.LeftEqCols,
				zigzagJoin.RightEqCols,
			)
			zigzagCols := leftCols.Copy()
			zigzagCols.UnionWith(rightCols)

			if scanPrivate.Cols.SubsetOf(zigzagCols) {
				// Case 1 (zigzagged indexes contain all requested columns).
				zigzagJoin.Cols = scanPrivate.Cols
				c.e.mem.AddZigzagJoinToGroup(&zigzagJoin, grp)
				return
			}

			if scanPrivate.Flags.NoIndexJoin {
				return
			}

			// Case 2 (wrap zigzag join in an index join).
			var indexJoin memo.LookupJoinExpr
			// Ensure the zigzag join returns pk columns.
			zigzagJoin.Cols = scanPrivate.Cols.Intersection(zigzagCols)
			for i := range pkCols {
				zigzagJoin.Cols.Add(pkCols[i])
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
			indexJoin.LookupColsAreTableKey = true

			// Create the LookupJoin for the index join in the same group as the
			// original select.
			c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
		})
	})
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
// See the comment in pkg/sql/rowexec/zigzag_joiner.go for more details
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
	// We can only zigzag on columns present in the key component of the index,
	// so use the LaxKeyColumnCount here because that's the longest prefix of the
	// columns in the index which is guaranteed to exist in the key component.
	// Using KeyColumnCount is invalid, because if we have a unique index with
	// nullable columns, the "key columns" include the primary key of the table,
	// which is only present in the key component if one of the other columns is
	// NULL.
	i, leftCnt := 0, leftIndex.LaxKeyColumnCount()
	j, rightCnt := 0, rightIndex.LaxKeyColumnCount()
	for ; i < leftCnt; i++ {
		colID := tabID.IndexColumnID(leftIndex, i)
		if !fixedCols.Contains(colID) {
			break
		}
	}
	for ; j < rightCnt; j++ {
		colID := tabID.IndexColumnID(rightIndex, j)
		if !fixedCols.Contains(colID) {
			break
		}
	}

	for i < leftCnt && j < rightCnt {
		leftColID := tabID.IndexColumnID(leftIndex, i)
		rightColID := tabID.IndexColumnID(rightIndex, j)
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

// fixedColsForZigzag is a helper function to generate FixedCols lists for the
// zigzag join expression. This function iterates through the columns of the
// specified index in order until it comes across the first column ID that is
// not constrained to a constant.
func (c *CustomFuncs) fixedColsForZigzag(
	index cat.Index, tabID opt.TableID, filters memo.FiltersExpr,
) (fixedCols opt.ColList, vals memo.ScalarListExpr, typs []*types.T) {
	for i, cnt := 0, index.ColumnCount(); i < cnt; i++ {
		colID := tabID.IndexColumnID(index, i)
		val := memo.ExtractValueForConstColumn(filters, c.e.evalCtx, colID)
		if val == nil {
			break
		}
		if vals == nil {
			vals = make(memo.ScalarListExpr, 0, cnt-i)
			typs = make([]*types.T, 0, cnt-i)
			fixedCols = make(opt.ColList, 0, cnt-i)
		}

		dt := val.ResolvedType()
		vals = append(vals, c.e.f.ConstructConstVal(val, dt))
		typs = append(typs, dt)
		fixedCols = append(fixedCols, colID)
	}
	return fixedCols, vals, typs
}

// indexConstrainedCols computes the set of columns in allFixedCols which form
// a prefix of the key columns in idx.
func (c *CustomFuncs) indexConstrainedCols(
	idx cat.Index, tab opt.TableID, allFixedCols opt.ColSet,
) opt.ColSet {
	var constrained opt.ColSet
	for i, n := 0, idx.ColumnCount(); i < n; i++ {
		col := tab.IndexColumnID(idx, i)
		if allFixedCols.Contains(col) {
			constrained.Add(col)
		} else {
			break
		}
	}
	return constrained
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
	sb.Init(c, scanPrivate.Table)

	// Iterate over all inverted indexes.
	var iter scanIndexIter
	iter.Init(c.e.evalCtx, c.e.f, c.e.mem, &c.im, scanPrivate, filters, rejectNonInvertedIndexes)
	iter.ForEach(func(index cat.Index, filters memo.FiltersExpr, indexCols opt.ColSet, _ bool, _ memo.ProjectionsExpr) {
		if index.NonInvertedPrefixColumnCount() > 0 {
			// TODO(mgartner): We don't yet support using multi-column inverted
			//  indexes with zigzag joins.
			return
		}

		// Check whether the filter can constrain the index with spans that
		// are guaranteed not to produce duplicate primary keys.
		// TODO(mgartner): Once we support multi-column inverted indexes, pass
		// optional filters generated from CHECK constraints and computed column
		// expressions to help constrain non-inverted prefix columns.
		spanExpr, _, _, _, ok := invertedidx.TryFilterInvertedIndex(
			c.e.evalCtx,
			c.e.f, filters,
			nil, /* optionalFilters */
			scanPrivate.Table,
			index,
			nil, /* computedColumns */
		)
		if !ok {
			return
		}

		// Recursively traverse the span expression to find single-value spans.
		//
		// We'll store at most two values in vals, so initialize the slice with
		// sufficient capacity.
		vals := make([]inverted.EncVal, 0, 2)
		var getVals func(invertedExpr inverted.Expression)
		getVals = func(invertedExpr inverted.Expression) {
			if len(vals) >= 2 {
				// We only need two constraints to plan a zigzag join, so don't bother
				// exploring further.
				// TODO(rytaft): use stats here to choose the two most selective
				// constraints instead of the first two.
				return
			}
			spanExprLocal, ok := invertedExpr.(*inverted.SpanExpression)
			if !ok {
				// The invertedExpr was a NonInvertedColExpression and cannot be used
				// to constrain the index. (This shouldn't ever happen, since
				// TryFilterInvertedIndex should have returned ok=false in this case,
				// but we don't want to panic if it does happen.)
				return
			}
			switch spanExprLocal.Operator {
			case inverted.SetIntersection:
				if len(spanExprLocal.FactoredUnionSpans) > 0 {
					// This is equivalent to a UNION between the FactoredUnionSpans and
					// the intersected children, so we can't build a zigzag join with
					// this subtree.
					return
				}
				getVals(spanExprLocal.Left)
				getVals(spanExprLocal.Right)
				return
			case inverted.SetUnion:
				// Don't recurse into UNIONs. We can't build a zigzag join with this
				// subtree.
				return
			}

			// Check that this span expression represents a single-key span that is
			// guaranteed not to produce duplicate primary keys.
			if spanExprLocal.Unique && len(spanExprLocal.SpansToRead) == 1 &&
				spanExprLocal.SpansToRead[0].IsSingleVal() {
				vals = append(vals, spanExprLocal.SpansToRead[0].Start)
			}
		}
		getVals(spanExpr)
		if len(vals) < 2 {
			return
		}

		// We treat the fixed values for JSON and Array as DBytes.
		leftVal := tree.DBytes(vals[0])
		rightVal := tree.DBytes(vals[1])

		zigzagJoin := memo.ZigzagJoinExpr{
			On: filters,
			ZigzagJoinPrivate: memo.ZigzagJoinPrivate{
				LeftTable:  scanPrivate.Table,
				LeftIndex:  index.Ordinal(),
				RightTable: scanPrivate.Table,
				RightIndex: index.Ordinal(),
			},
		}

		// The fixed columns include all the prefix columns and the inverted column.
		fixedColsCount := index.NonInvertedPrefixColumnCount() + 1

		// Get constant values and add them to FixedVals as tuples, with associated
		// Column IDs in both {Left,Right}FixedCols.
		leftVals := make(memo.ScalarListExpr, fixedColsCount)
		leftTypes := make([]*types.T, fixedColsCount)
		rightVals := make(memo.ScalarListExpr, fixedColsCount)
		rightTypes := make([]*types.T, fixedColsCount)
		zigzagJoin.LeftFixedCols = make(opt.ColList, fixedColsCount)
		zigzagJoin.RightFixedCols = make(opt.ColList, fixedColsCount)

		// TODO(rytaft): set types, values, and fixed columns for the prefix
		//  columns here.

		// invertedColIdx is the position of the inverted column in the inverted
		// index.
		invertedColIdx := index.NonInvertedPrefixColumnCount()
		leftVals[invertedColIdx] = c.e.f.ConstructConstVal(&leftVal, leftVal.ResolvedType())
		leftTypes[invertedColIdx] = leftVal.ResolvedType()
		rightVals[invertedColIdx] = c.e.f.ConstructConstVal(&rightVal, rightVal.ResolvedType())
		rightTypes[invertedColIdx] = rightVal.ResolvedType()
		invertedCol := scanPrivate.Table.ColumnID(index.VirtualInvertedColumn().Ordinal())
		zigzagJoin.LeftFixedCols[invertedColIdx] = invertedCol
		zigzagJoin.RightFixedCols[invertedColIdx] = invertedCol

		leftTupleTyp := types.MakeTuple(leftTypes)
		rightTupleTyp := types.MakeTuple(rightTypes)
		zigzagJoin.FixedVals = memo.ScalarListExpr{
			c.e.f.ConstructTuple(leftVals, leftTupleTyp),
			c.e.f.ConstructTuple(rightVals, rightTupleTyp),
		}

		// Set equality columns - all remaining columns after the fixed prefix
		// need to be equal.
		eqColLen := index.ColumnCount() - fixedColsCount
		zigzagJoin.LeftEqCols = make(opt.ColList, eqColLen)
		zigzagJoin.RightEqCols = make(opt.ColList, eqColLen)
		for i := fixedColsCount; i < index.ColumnCount(); i++ {
			colID := scanPrivate.Table.IndexColumnID(index, i)
			zigzagJoin.LeftEqCols[i-fixedColsCount] = colID
			zigzagJoin.RightEqCols[i-fixedColsCount] = colID
		}
		zigzagJoin.On = filters

		// Don't output the first column (i.e. the inverted index's JSON key
		// col) from the zigzag join. It could contain partial values, so
		// presenting it in the output or checking ON conditions against
		// it makes little sense.
		zigzagCols := indexCols
		for i, cnt := 0, index.KeyColumnCount(); i < cnt; i++ {
			colID := scanPrivate.Table.IndexColumnID(index, i)
			zigzagCols.Remove(colID)
		}

		tab := c.e.mem.Metadata().Table(scanPrivate.Table)
		pkIndex := tab.Index(cat.PrimaryIndex)
		pkCols := make(opt.ColList, pkIndex.KeyColumnCount())
		for i := range pkCols {
			pkCols[i] = scanPrivate.Table.IndexColumnID(pkIndex, i)
			// Ensure primary key columns are always retrieved from the zigzag
			// join.
			zigzagCols.Add(pkCols[i])
		}

		// Case 1 (zigzagged indexes contain all requested columns).
		if scanPrivate.Cols.SubsetOf(zigzagCols) {
			zigzagJoin.Cols = scanPrivate.Cols
			c.e.mem.AddZigzagJoinToGroup(&zigzagJoin, grp)
			return
		}

		if scanPrivate.Flags.NoIndexJoin {
			return
		}

		// Case 2 (wrap zigzag join in an index join).

		var indexJoin memo.LookupJoinExpr
		// Ensure the zigzag join returns pk columns.
		zigzagJoin.Cols = scanPrivate.Cols.Intersection(zigzagCols)
		for i := range pkCols {
			zigzagJoin.Cols.Add(pkCols[i])
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
		indexJoin.LookupColsAreTableKey = true

		// Create the LookupJoin for the index join in the same group as the
		// original select.
		c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
	})
}

// SplitDisjunction finds the first disjunction in the filters that can be split
// into an interesting pair of expressions. It returns the pair of expressions
// and the Filters item they were a part of. If an "interesting" disjunction is
// not found, ok=false is returned.
//
// For details on what makes an "interesting" disjunction, see
// findInterestingDisjunctionPair.
func (c *CustomFuncs) SplitDisjunction(
	sp *memo.ScanPrivate, filters memo.FiltersExpr,
) (left opt.ScalarExpr, right opt.ScalarExpr, itemToReplace *memo.FiltersItem, ok bool) {
	for i := range filters {
		if filters[i].Condition.Op() == opt.OrOp {
			if left, right, ok := c.findInterestingDisjunctionPair(sp, &filters[i]); ok {
				return left, right, &filters[i], true
			}
		}
	}
	return nil, nil, nil, false
}

// findInterestingDisjunctionPair groups disjunction sub-expressions into an
// "interesting" pair of expressions.
//
// An "interesting" pair of expressions is one where:
//
//   1. The column sets of both expressions in the pair are not
//      equal.
//   2. Two index scans can potentially be constrained by both expressions in
//      the pair.
//
// Consider the expression:
//
//   u = 1 OR v = 2
//
// If an index exists on u and another on v, an "interesting" pair exists, ("u =
// 1", "v = 1"). If both indexes do not exist, there is no "interesting" pair
// possible.
//
// Now consider the expression:
//
//   u = 1 OR u = 2
//
// There is no possible "interesting" pair here because the left and right sides
// of the disjunction share the same columns.
//
// findInterestingDisjunctionPair groups all sub-expressions adjacent to the
// input's top-level OrExpr into left and right expression groups. These two
// groups form the new filter expressions on the left and right side of the
// generated UnionAll in SplitDisjunction(AddKey).
//
// All sub-expressions with the same columns as the left-most sub-expression
// are grouped in the left group. All other sub-expressions are grouped in the
// right group.
//
// findInterestingDisjunctionPair returns an ok=false if all sub-expressions
// have the same columns. It also returns ok=false if either expression of the
// pair is likely to constrain an index scan. See canMaybeConstrainIndexWithCols
// for details on how this is determined.
func (c *CustomFuncs) findInterestingDisjunctionPair(
	sp *memo.ScanPrivate, filter *memo.FiltersItem,
) (left opt.ScalarExpr, right opt.ScalarExpr, ok bool) {
	var leftExprs memo.ScalarListExpr
	var rightExprs memo.ScalarListExpr
	var leftColSet opt.ColSet
	var rightColSet opt.ColSet

	// Traverse all adjacent OrExpr.
	var collect func(opt.ScalarExpr)
	collect = func(expr opt.ScalarExpr) {
		switch t := expr.(type) {
		case *memo.OrExpr:
			collect(t.Left)
			collect(t.Right)
			return
		}

		cols := c.OuterCols(expr)

		// Set the left-most non-Or expression as the left ColSet to match (or
		// not match) on.
		if leftColSet.Empty() {
			leftColSet = cols
		}

		// If the current expression ColSet matches leftColSet, add the expr to
		// the left group. Otherwise, add it to the right group.
		if leftColSet.Equals(cols) {
			leftExprs = append(leftExprs, expr)
		} else {
			rightColSet.UnionWith(cols)
			rightExprs = append(rightExprs, expr)
		}
	}
	collect(filter.Condition)

	// Return an empty pair if either of the groups is empty or if either the
	// left or right groups are unlikely to constrain an index scan.
	if len(leftExprs) == 0 ||
		len(rightExprs) == 0 ||
		!c.canMaybeConstrainIndexWithCols(sp, leftColSet) ||
		!c.canMaybeConstrainIndexWithCols(sp, rightColSet) {
		return nil, nil, false
	}

	return c.constructOr(leftExprs), c.constructOr(rightExprs), true
}

// canMaybeConstrainIndexWithCols returns true if any indexes on the
// ScanPrivate's table could be constrained by cols. It is a fast check for
// SplitDisjunction to avoid matching a large number of queries that won't
// obviously be improved by the rule.
//
// canMaybeConstrainIndexWithCols checks for an intersection between the input
// columns and an index's columns (both indexed columns and columns referenced
// in a partial index predicate). An intersection between column sets implies
// that cols could constrain a scan on that index. For example, the columns "a"
// would constrain a scan on an index over columns "a, b", because the "a" is a
// subset of the index columns. Likewise, the columns "a" and "b" would
// constrain a scan on an index over column "a", because "a" and "b" are a
// superset of the index columns.
//
// Notice that this function can return both false positives and false
// negatives. As an example of a false negative, consider the following table
// and query.
//
//   CREATE TABLE t (
//     k PRIMARY KEY,
//     a INT,
//     hash INT AS (a % 4) STORED,
//     INDEX hash (hash)
//   )
//
//   SELECT * FROM t WHERE a = 5
//
// The expression "a = 5" can constrain a scan over the hash index: The columns
// "hash" must be a constant value of 1 because it is dependent on column "a"
// with a constant value of 5. However, canMaybeConstrainIndexWithCols will
// return false in this case because "a" does not intersect with the index
// column, "hash".
func (c *CustomFuncs) canMaybeConstrainIndexWithCols(
	scanPrivate *memo.ScanPrivate, cols opt.ColSet,
) bool {
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(scanPrivate.Table)

	// Iterate through all indexes of the table and return true if cols
	// intersect with the index's key columns.
	for i := 0; i < tabMeta.Table.IndexCount(); i++ {
		index := tabMeta.Table.Index(i)
		for j, n := 0, index.KeyColumnCount(); j < n; j++ {
			col := index.Column(j)
			ord := col.Ordinal()
			if col.Kind() == cat.VirtualInverted {
				ord = col.InvertedSourceColumnOrdinal()
			}
			if cols.Contains(tabMeta.MetaID.ColumnID(ord)) {
				return true
			}
		}

		// If a partial index's predicate references some of cols, it may be
		// possible to generate an unconstrained partial index scan, which may
		// lead to better query plans.
		if _, isPartialIndex := index.Predicate(); isPartialIndex {
			p, ok := tabMeta.PartialIndexPredicate(i)
			if !ok {
				// A partial index predicate expression was not built for the
				// partial index. See Builder.buildScan for details on when this
				// can occur.
				continue
			}
			pred := *p.(*memo.FiltersExpr)
			if pred.OuterCols().Intersects(cols) {
				return true
			}
		}
	}
	return false
}

// MakeSetPrivate constructs a new SetPrivate with given left, right, and out
// columns.
func (c *CustomFuncs) MakeSetPrivate(left, right, out opt.ColSet) *memo.SetPrivate {
	return &memo.SetPrivate{
		LeftCols:  left.ToList(),
		RightCols: right.ToList(),
		OutCols:   out.ToList(),
	}
}

// AddPrimaryKeyColsToScanPrivate creates a new ScanPrivate that is the same as
// the input ScanPrivate, but has primary keys added to the ColSet.
func (c *CustomFuncs) AddPrimaryKeyColsToScanPrivate(sp *memo.ScanPrivate) *memo.ScanPrivate {
	keyCols := c.PrimaryKeyCols(sp.Table)
	return &memo.ScanPrivate{
		Table:   sp.Table,
		Cols:    sp.Cols.Union(keyCols),
		Flags:   sp.Flags,
		Locking: sp.Locking,
	}
}

// TableIDFromScanPrivate returns the table ID of the scan private.
func (c *CustomFuncs) TableIDFromScanPrivate(sp *memo.ScanPrivate) opt.TableID {
	return sp.Table
}
