// Copyright 2018 The Cockroach Authors.
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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partialidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// CustomFuncs contains all the custom match and replace functions used by
// the exploration rules. The unnamed xfunc.CustomFuncs allows
// CustomFuncs to provide a clean interface for calling functions from both the
// xform and xfunc packages using the same struct.
type CustomFuncs struct {
	norm.CustomFuncs
	e  *explorer
	im partialidx.Implicator
}

// Init initializes a new CustomFuncs with the given explorer.
func (c *CustomFuncs) Init(e *explorer) {
	c.CustomFuncs.Init(e.f)
	c.e = e
	c.im.Init(e.f, e.mem.Metadata(), e.evalCtx)
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
	return scan.IsCanonical()
}

// IsLocking returns true if the ScanPrivate is configured to use a row-level
// locking mode. This can be the case either because the Scan is in the scope of
// a SELECT .. FOR [KEY] UPDATE/SHARE clause or because the Scan was configured
// as part of the row retrieval of a DELETE or UPDATE statement.
func (c *CustomFuncs) IsLocking(scan *memo.ScanPrivate) bool {
	return scan.IsLocking()
}

// GenerateIndexScans enumerates all non-inverted and non-partial secondary
// indexes on the given Scan operator's table and generates an alternate Scan
// operator for each index that includes the set of needed columns specified in
// the ScanOpDef.
//
// Partial indexes do not index every row in the table and they can only be used
// in cases where a query filter implies the partial index predicate.
// GenerateIndexScans does not deal with filters. Therefore, partial indexes
// cannot be considered for non-constrained index scans.
//
// NOTE: This does not generate index joins for non-covering indexes (except in
//       case of ForceIndex). Index joins are usually only introduced "one level
//       up", when the Scan operator is wrapped by an operator that constrains
//       or limits scan output in some way (e.g. Select, Limit, InnerJoin).
//       Index joins are only lower cost when their input does not include all
//       rows from the table. See ConstrainScans and LimitScans for cases where
//       index joins are introduced into the memo.
func (c *CustomFuncs) GenerateIndexScans(grp memo.RelExpr, scanPrivate *memo.ScanPrivate) {
	// Iterate over all non-inverted and non-partial secondary indexes.
	iter := makeScanIndexIter(c.e.mem, scanPrivate, rejectPrimaryIndex|rejectInvertedIndexes|rejectPartialIndexes)
	for iter.Next() {
		// If the secondary index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if iter.IsCovering() {
			scan := memo.ScanExpr{ScanPrivate: *scanPrivate}
			scan.Index = iter.IndexOrdinal()
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
		newScanPrivate.Index = iter.IndexOrdinal()
		newScanPrivate.Cols = iter.IndexColumns().Intersection(scanPrivate.Cols)
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

// GeneratePartialIndexScans generates unconstrained index scans over all
// partial indexes with predicates that are implied by the filters. Partial
// indexes with predicates which cannot be proven to be implied by the filters
// are disregarded.
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
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(scanPrivate.Table)

	// Iterate over all partial indexes.
	iter := makeScanIndexIter(c.e.mem, scanPrivate, rejectNonPartialIndexes)
	for iter.Next() {
		pred := tabMeta.PartialIndexPredicates[iter.IndexOrdinal()]
		remainingFilters, ok := c.im.FiltersImplyPredicate(filters, pred)
		if !ok {
			// The filters do not imply the predicate, so the partial index
			// cannot be used.
			continue
		}

		var sb indexScanBuilder
		sb.init(c, scanPrivate.Table)
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = iter.IndexOrdinal()

		// If index is covering, just add a Select with the remaining filters,
		// if there are any.
		if iter.IsCovering() {
			sb.setScan(&newScanPrivate)
			sb.addSelect(remainingFilters)
			sb.build(grp)
			continue
		}

		// If the index is not covering, scan the needed index columns plus
		// primary key columns.
		newScanPrivate.Cols = iter.IndexColumns().Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(&newScanPrivate)

		// Add a Select with any remaining filters that can be filtered before
		// the IndexJoin. If there are no remaining filters this is a no-op. If
		// all or parts of the remaining filters cannot be applied until after
		// the IndexJoin, the new value of remainingFilters will contain those
		// filters.
		remainingFilters = sb.addSelectAfterSplit(remainingFilters, newScanPrivate.Cols)

		// Add an IndexJoin to retrieve the columns not provided by the Scan.
		sb.addIndexJoin(scanPrivate.Cols)

		// Add a Select with any remaining filters.
		sb.addSelect(remainingFilters)
		sb.build(grp)
	}
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
	var sb indexScanBuilder
	sb.init(c, scanPrivate.Table)

	// Generate implicit filters from constraints and computed columns and add
	// them to the list of explicit filters provided in the query.
	optionalFilters := c.checkConstraintFilters(scanPrivate.Table)
	computedColFilters := c.computedColFilters(scanPrivate.Table, explicitFilters, optionalFilters)
	optionalFilters = append(optionalFilters, computedColFilters...)

	filterColumns := c.FilterOuterCols(explicitFilters)
	filterColumns.UnionWith(c.FilterOuterCols(optionalFilters))

	// Iterate over all non-inverted indexes.
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(scanPrivate.Table)
	iter := makeScanIndexIter(c.e.mem, scanPrivate, rejectInvertedIndexes|rejectPartialIndexes)
	for iter.Next() {
		// TODO(mgartner): Generate constrained scans for partial indexes if
		// they are implied by the filter.

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

		indexColumns := tabMeta.IndexKeyColumns(iter.IndexOrdinal())
		firstIndexCol := scanPrivate.Table.ColumnID(iter.Index().Column(0).Ordinal)
		if !filterColumns.Contains(firstIndexCol) && indexColumns.Intersects(filterColumns) {
			// Calculate any partition filters if appropriate (see below).
			partitionFilters, inBetweenFilters = c.partitionValuesFilters(scanPrivate.Table, iter.Index())
		}

		// Check whether the filter (along with any partitioning filters) can constrain the index.
		constraint, remainingFilters, ok := c.tryConstrainIndex(
			explicitFilters,
			append(optionalFilters, partitionFilters...),
			scanPrivate.Table,
			iter.IndexOrdinal(),
			false, /* isInverted */
		)
		if !ok {
			continue
		}

		if len(partitionFilters) > 0 {
			inBetweenConstraint, inBetweenRemainingFilters, ok := c.tryConstrainIndex(
				explicitFilters,
				append(optionalFilters, inBetweenFilters...),
				scanPrivate.Table,
				iter.IndexOrdinal(),
				false, /* isInverted */
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
		newScanPrivate.Index = iter.IndexOrdinal()
		newScanPrivate.Constraint = constraint
		// Record whether we were able to use partitions to constrain the scan.
		newScanPrivate.PartitionConstrainedScan = (len(partitionFilters) > 0)

		// If the alternate index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if iter.IsCovering() {
			sb.setScan(&newScanPrivate)

			// If there are remaining filters, then the constrained Scan operator
			// will be created in a new group, and a Select operator will be added
			// to the same group as the original operator.
			sb.addSelect(remainingFilters)

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
		newScanPrivate.Cols = iter.IndexColumns().Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(&newScanPrivate)

		// If remaining filter exists, split it into one part that can be pushed
		// below the IndexJoin, and one part that needs to stay above.
		remainingFilters = sb.addSelectAfterSplit(remainingFilters, newScanPrivate.Cols)
		sb.addIndexJoin(scanPrivate.Cols)
		sb.addSelect(remainingFilters)

		sb.build(grp)
	}
}

// checkConstraintFilters generates all filters that we can derive from the
// check constraints. These are constraints that have been validated and are
// non-nullable. We only use non-nullable check constraints because they
// behave differently from filters on NULL. Check constraints are satisfied
// when their expression evaluates to NULL, while filters are not.
//
// For example, the check constraint a > 1 is satisfied if a is NULL but the
// equivalent filter a > 1 is not.
//
// These filters do not really filter any rows, they are rather facts or
// guarantees about the data but treating them as filters may allow some
// indexes to be constrained and used. Consider the following example:
//
// CREATE TABLE abc (
// 	a INT PRIMARY KEY,
// 	b INT NOT NULL,
// 	c STRING NOT NULL,
// 	CHECK (a < 10 AND a > 1),
// 	CHECK (b < 10 AND b > 1),
// 	CHECK (c in ('first', 'second')),
// 	INDEX secondary (b, a),
// 	INDEX tertiary (c, b, a))
//
// Now consider the query: SELECT a, b WHERE a > 5
//
// Notice that the filter provided previously wouldn't let the optimizer use
// the secondary or tertiary indexes. However, given that we can use the
// constraints on a, b and c, we can actually use the secondary and tertiary
// indexes. In fact, for the above query we can do the following:
//
// select
//  ├── columns: a:1(int!null) b:2(int!null)
//  ├── scan abc@tertiary
//  │		├── columns: a:1(int!null) b:2(int!null)
//  │		└── constraint: /3/2/1: [/'first'/2/6 - /'first'/9/9] [/'second'/2/6 - /'second'/9/9]
//  └── filters
//        └── gt [type=bool]
//            ├── variable: a [type=int]
//            └── const: 5 [type=int]
//
// Similarly, the secondary index could also be used. All such index scans
// will be added to the memo group.
func (c *CustomFuncs) checkConstraintFilters(tabID opt.TableID) memo.FiltersExpr {
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(tabID)
	if tabMeta.Constraints == nil {
		return memo.FiltersExpr{}
	}
	filters := *tabMeta.Constraints.(*memo.FiltersExpr)
	// Limit slice capacity to allow the caller to append if necessary.
	return filters[:len(filters):len(filters)]
}

// computedColFilters generates all filters that can be derived from the list of
// computed column expressions from the given table. A computed column can be
// used as a filter when it has a constant value. That is true when:
//
//   1. All other columns it references are constant, because other filters in
//      the query constrain them to be so.
//   2. All functions in the computed column expression can be folded into
//      constants (i.e. they do not have problematic side effects).
//
// Note that computed columns can depend on other computed columns; in general
// the dependencies form an acyclic directed graph. computedColFilters will
// return filters for all constant computed columns, regardless of the order of
// their dependencies.
//
// As with checkConstraintFilters, computedColFilters do not really filter any
// rows, they are rather facts or guarantees about the data. Treating them as
// filters may allow some indexes to be constrained and used. Consider the
// following example:
//
//   CREATE TABLE t (
//     k INT NOT NULL,
//     hash INT AS (k % 4) STORED,
//     PRIMARY KEY (hash, k)
//   )
//
//   SELECT * FROM t WHERE k = 5
//
// Notice that the filter provided explicitly wouldn't allow the optimizer to
// seek using the primary index (it would have to fall back to a table scan).
// However, column "hash" can be proven to have the constant value of 1, since
// it's dependent on column "k", which has the constant value of 5. This enables
// usage of the primary index:
//
//     scan t
//      ├── columns: k:1(int!null) hash:2(int!null)
//      ├── constraint: /2/1: [/1/5 - /1/5]
//      ├── key: (2)
//      └── fd: ()-->(1)
//
// The values of both columns in that index are known, enabling a single value
// constraint to be generated.
func (c *CustomFuncs) computedColFilters(
	tabID opt.TableID, requiredFilters, optionalFilters memo.FiltersExpr,
) memo.FiltersExpr {
	tabMeta := c.e.mem.Metadata().TableMeta(tabID)
	if len(tabMeta.ComputedCols) == 0 {
		return nil
	}

	// Start with set of constant columns, as derived from the list of filter
	// conditions.
	constCols := make(map[opt.ColumnID]opt.ScalarExpr)
	c.findConstantFilterCols(constCols, tabID, requiredFilters)
	c.findConstantFilterCols(constCols, tabID, optionalFilters)
	if len(constCols) == 0 {
		// No constant values could be derived from filters, so assume that there
		// are also no constant computed columns.
		return nil
	}

	// Construct a new filter condition for each computed column that is
	// constant (i.e. all of its variables are in the constCols set).
	var computedColFilters memo.FiltersExpr
	for colID := range tabMeta.ComputedCols {
		if c.tryFoldComputedCol(tabMeta, colID, constCols) {
			constVal := constCols[colID]
			// Note: Eq is not correct here because of NULLs.
			eqOp := c.e.f.ConstructIs(c.e.f.ConstructVariable(colID), constVal)
			computedColFilters = append(computedColFilters, c.e.f.ConstructFiltersItem(eqOp))
		}
	}
	return computedColFilters
}

// findConstantFilterCols adds to constFilterCols mappings from table column ID
// to the constant value of that column. It does this by iterating over the
// given lists of filters and finding expressions that constrain columns to a
// single constant value. For example:
//
//   x = 5 AND y = 'foo'
//
// This would add a mapping from x => 5 and y => 'foo', which constants can
// then be used to prove that dependent computed columns are also constant.
func (c *CustomFuncs) findConstantFilterCols(
	constFilterCols map[opt.ColumnID]opt.ScalarExpr, tabID opt.TableID, filters memo.FiltersExpr,
) {
	tab := c.e.mem.Metadata().Table(tabID)
	for i := range filters {
		// If filter constraints are not tight, then no way to derive constant
		// values.
		props := filters[i].ScalarProps()
		if !props.TightConstraints {
			continue
		}

		// Iterate over constraint conjuncts with a single column and single
		// span having a single key.
		for i, n := 0, props.Constraints.Length(); i < n; i++ {
			cons := props.Constraints.Constraint(i)
			if cons.Columns.Count() != 1 || cons.Spans.Count() != 1 {
				continue
			}

			// Skip columns with a data type that uses a composite key encoding.
			// Each of these data types can have multiple distinct values that
			// compare equal. For example, 0 == -0 for the FLOAT data type. It's
			// not safe to treat these as constant inputs to computed columns,
			// since the computed expression may differentiate between the
			// different forms of the same value.
			colID := cons.Columns.Get(0).ID()
			colTyp := tab.Column(tabID.ColumnOrdinal(colID)).DatumType()
			if sqlbase.HasCompositeKeyEncoding(colTyp) {
				continue
			}

			span := cons.Spans.Get(0)
			if !span.HasSingleKey(c.e.evalCtx) {
				continue
			}

			datum := span.StartKey().Value(0)
			if datum != tree.DNull {
				constFilterCols[colID] = c.e.f.ConstructConstVal(datum, colTyp)
			}
		}
	}
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
		colID := tabID.ColumnID(index.Column(i).Ordinal)
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
	partitionValues := index.PartitionByListPrefixes()
	if len(partitionValues) == 0 {
		return partitionFilter, inBetweenFilter
	}

	// Get the in partition expressions.
	inPartition := c.inPartitionFilters(tabID, index, partitionValues)

	// Get the in between expressions.
	inBetween := c.inBetweenFilters(tabID, index, partitionValues)

	return inPartition, inBetween
}

// HasInvertedIndexes returns true if at least one inverted index is defined on
// the Scan operator's table.
func (c *CustomFuncs) HasInvertedIndexes(scanPrivate *memo.ScanPrivate) bool {
	// Don't bother matching unless there's an inverted index.
	iter := makeScanIndexIter(c.e.mem, scanPrivate, rejectNonInvertedIndexes)
	return iter.Next()
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
	iter := makeScanIndexIter(c.e.mem, scanPrivate, rejectNonInvertedIndexes)
	for iter.Next() {
		var spanExpr *invertedexpr.SpanExpression
		var spansToRead invertedexpr.InvertedSpans
		var constraint *constraint.Constraint
		var remaining memo.FiltersExpr
		var geoOk, nonGeoOk bool

		// Check whether the filter can constrain the index.
		// TODO(rytaft): Unify these two cases so both return a spanExpr.
		spanExpr, geoOk = tryConstrainGeoIndex(
			c.e.evalCtx.Context, filters, scanPrivate.Table, iter.Index(),
		)
		if geoOk {
			// Geo index scans can never be tight, so remaining filters is always the
			// same as filters.
			remaining = filters
			spansToRead = spanExpr.SpansToRead
		} else {
			constraint, remaining, nonGeoOk = c.tryConstrainIndex(
				filters,
				nil, /* optionalFilters */
				scanPrivate.Table,
				iter.IndexOrdinal(),
				true, /* isInverted */
			)
			if !nonGeoOk {
				continue
			}
		}

		invertedCol := scanPrivate.Table.ColumnID(iter.Index().Column(0).Ordinal)

		// Construct new ScanOpDef with the new index and constraint.
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = iter.IndexOrdinal()
		newScanPrivate.Constraint = constraint
		newScanPrivate.InvertedConstraint = spansToRead

		// Though the index is marked as containing the column being indexed, it
		// doesn't actually, and it's only valid to extract the primary key columns
		// from it. However, the inverted key column is still needed if there is an
		// inverted filter.
		pkCols := sb.primaryKeyCols()
		newScanPrivate.Cols = pkCols.Copy()
		if spanExpr != nil {
			newScanPrivate.Cols.Add(invertedCol)
		}

		// The Scan operator always goes in a new group, since it's always nested
		// underneath the IndexJoin. The IndexJoin may also go into its own group,
		// if there's a remaining filter above it.
		// TODO(justin): We might not need to do an index join in order to get the
		// correct columns, but it's difficult to tell at this point.
		sb.setScan(&newScanPrivate)

		// Add an inverted filter if it exists.
		sb.addInvertedFilter(spanExpr, invertedCol)

		// If remaining filter exists, split it into one part that can be pushed
		// below the IndexJoin, and one part that needs to stay above.
		remaining = sb.addSelectAfterSplit(remaining, pkCols)
		sb.addIndexJoin(scanPrivate.Cols)
		sb.addSelect(remaining)

		sb.build(grp)
	}
}

func (c *CustomFuncs) initIdxConstraintForIndex(
	requiredFilters, optionalFilters memo.FiltersExpr,
	tabID opt.TableID,
	indexOrd int,
	isInverted bool,
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
		if !col.IsNullable() {
			notNullCols.Add(colID)
		}
	}

	// Generate index constraints.
	ic.Init(requiredFilters, optionalFilters, columns, notNullCols, isInverted, c.e.evalCtx, c.e.f)
	return ic
}

// tryConstrainIndex tries to derive a constraint for the given index from the
// specified filter. If a constraint is derived, it is returned along with any
// filter remaining after extracting the constraint. If no constraint can be
// derived, then tryConstrainIndex returns ok = false.
func (c *CustomFuncs) tryConstrainIndex(
	requiredFilters, optionalFilters memo.FiltersExpr,
	tabID opt.TableID,
	indexOrd int,
	isInverted bool,
) (constraint *constraint.Constraint, remainingFilters memo.FiltersExpr, ok bool) {
	// Start with fast check to rule out indexes that cannot be constrained.
	if !isInverted &&
		!c.canMaybeConstrainIndex(requiredFilters, tabID, indexOrd) &&
		!c.canMaybeConstrainIndex(optionalFilters, tabID, indexOrd) {
		return nil, nil, false
	}

	ic := c.initIdxConstraintForIndex(requiredFilters, optionalFilters, tabID, indexOrd, isInverted)
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
	ic := c.initIdxConstraintForIndex(filters, nil /* optionalFilters */, tabID, indexOrd, true /* isInverted */)
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

// canMaybeConstrainIndex returns true if we should try to constrain a given
// index by the given filter. It returns false if it is impossible for the
// filter can constrain the scan.
//
// If any of the three following statements are true, then it is
// possible that the index can be constrained:
//
//   1. The filter references the first index column.
//   2. The constraints are not tight (see props.Scalar.TightConstraints).
//   3. Any of the filter's constraints start with the first index column.
//
func (c *CustomFuncs) canMaybeConstrainIndex(
	filters memo.FiltersExpr, tabID opt.TableID, indexOrd int,
) bool {
	md := c.e.mem.Metadata()
	index := md.Table(tabID).Index(indexOrd)

	for i := range filters {
		filterProps := filters[i].ScalarProps()

		// If the filter involves the first index column, then the index can
		// possibly be constrained.
		firstIndexCol := tabID.ColumnID(index.Column(0).Ordinal)
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

// ----------------------------------------------------------------------
//
// Limit Rules
//   Custom match and replace functions used with limit.opt rules.
//
// ----------------------------------------------------------------------

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
// NOTE: Limiting unconstrained scans is done by the GenerateLimitedScans rule,
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
		// This is not a constrained scan, so skip it. The GenerateLimitedScans
		// rule is responsible for limited unconstrained scans.
		return false
	}

	ok, _ := ordering.ScanPrivateCanProvide(c.e.mem.Metadata(), scanPrivate, &required)
	return ok
}

// GenerateLimitedScans enumerates all non-inverted and non-partial secondary
// indexes on the Scan operator's table and tries to create new limited Scan
// operators from them. Since this only needs to be done once per table,
// GenerateLimitedScans should only be called on the original unaltered primary
// index Scan operator (i.e. not constrained or limited).
//
// For a secondary index that "covers" the columns needed by the scan, a single
// limited Scan operator is created. For a non-covering index, an IndexJoin is
// constructed to add missing columns to the limited Scan.
//
// Inverted index scans are not guaranteed to produce a specific number
// of result rows because they contain multiple entries for a single row
// indexed. Therefore, they cannot be considered for limited scans.
//
// Partial indexes do not index every row in the table and they can only be used
// in cases where a query filter implies the partial index predicate.
// GenerateLimitedScans deals with limits, but no filters. Therefore, partial
// indexes cannot be considered for limited scans.
func (c *CustomFuncs) GenerateLimitedScans(
	grp memo.RelExpr,
	scanPrivate *memo.ScanPrivate,
	limit tree.Datum,
	required physical.OrderingChoice,
) {
	limitVal := int64(*limit.(*tree.DInt))

	var sb indexScanBuilder
	sb.init(c, scanPrivate.Table)

	// Iterate over all non-inverted, non-partial indexes, looking for those
	// that can be limited.
	iter := makeScanIndexIter(c.e.mem, scanPrivate, rejectInvertedIndexes|rejectPartialIndexes)
	for iter.Next() {
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = iter.IndexOrdinal()

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
		if iter.IsCovering() {
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
		newScanPrivate.Cols = iter.IndexColumns().Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(&newScanPrivate)

		// The Scan operator will go into its own group (because it projects a
		// different set of columns), and the IndexJoin operator will be added to
		// the same group as the original Limit operator.
		sb.addIndexJoin(scanPrivate.Cols)

		sb.build(grp)
	}
}

// ScanIsConstrained returns true if the scan operator with the given
// ScanPrivate is constrained.
func (c *CustomFuncs) ScanIsConstrained(sp *memo.ScanPrivate) bool {
	return sp.Constraint != nil
}

// ScanIsLimited returns true if the scan operator with the given ScanPrivate is
// limited.
func (c *CustomFuncs) ScanIsLimited(sp *memo.ScanPrivate) bool {
	return sp.HardLimit != 0
}

// SplitScanIntoUnionScans returns a Union of Scan operators with hard limits
// that each scan over a single key from the original scan's constraints. This
// is beneficial in cases where the original scan had to scan over many rows but
// had relatively few keys to scan over.
func (c *CustomFuncs) SplitScanIntoUnionScans(
	limitOrdering physical.OrderingChoice, scan memo.RelExpr, sp *memo.ScanPrivate, limit tree.Datum,
) memo.RelExpr {
	const maxScanCount = 16
	const threshold = 4

	keyCtx := constraint.MakeKeyContext(&sp.Constraint.Columns, c.e.evalCtx)
	limitVal := int(*limit.(*tree.DInt))
	spans := sp.Constraint.Spans

	// Retrieve the number of keys in the spans.
	keyCount, ok := spans.KeyCount(&keyCtx)
	if !ok {
		return nil
	}
	if keyCount <= 1 {
		// We need more than one key in order to split the existing Scan into
		// multiple Scans.
		return nil
	}
	if int(keyCount) > maxScanCount {
		// The number of new Scans created would exceed maxScanCount.
		return nil
	}

	// Check that the number of rows scanned by the new plan will be smaller than
	// the number scanned by the old plan by at least a factor of "threshold".
	if float64(int(keyCount)*limitVal*threshold) >= scan.Relational().Stats.RowCount {
		// Splitting the scan may not be worth the overhead; creating a sequence of
		// scans unioned together is expensive, so we don't want to create the plan
		// only for the optimizer to use something else. We only want to create the
		// plan if it is likely to be used.
		return nil
	}

	// Retrieve the length of the keys. All keys are required to be the same
	// length (this will be checked later) so we can simply use the length of the
	// first key.
	keyLength := spans.Get(0).StartKey().Length()

	// If the index ordering has a prefix of columns of length keyLength followed
	// by the limitOrdering columns, the scan can be split. Otherwise, return nil.
	hasLimitOrderingSeq, reverse := indexHasOrderingSequence(
		c.e.mem.Metadata(), scan, sp, limitOrdering, keyLength)
	if !hasLimitOrderingSeq {
		return nil
	}

	// Construct a hard limit for the new scans using the result of
	// hasLimitOrderingSeq.
	newHardLimit := memo.MakeScanLimit(int64(limitVal), reverse)

	// Construct a new Spans object containing a new Span for each key in the
	// original Scan's spans.
	newSpans, ok := spans.ExtractSingleKeySpans(&keyCtx, maxScanCount)
	if !ok {
		// Single key spans could not be created.
		return nil
	}

	// Construct a new ScanExpr for each span and union them all together. We
	// output the old ColumnIDs from each union.
	oldColList := opt.ColSetToList(scan.Relational().OutputCols)
	last := c.makeNewScan(sp, newHardLimit, newSpans.Get(0))
	for i, cnt := 1, newSpans.Count(); i < cnt; i++ {
		newScan := c.makeNewScan(sp, newHardLimit, newSpans.Get(i))
		last = c.e.f.ConstructUnion(last, newScan, &memo.SetPrivate{
			LeftCols:  opt.ColSetToList(last.Relational().OutputCols),
			RightCols: opt.ColSetToList(newScan.Relational().OutputCols),
			OutCols:   oldColList,
		})
	}
	return last
}

// indexHasOrderingSequence returns whether the scan can provide a given
// ordering under the assumption that we are scanning a single-key span with the
// given keyLength (and if so, whether we need to scan it in reverse).
// For example:
//
// index: +1/-2/+3,
// limitOrdering: -2/+3,
// keyLength: 1,
// =>
// hasSequence: True, reverse: False
//
// index: +1/-2/+3,
// limitOrdering: +2/-3,
// keyLength: 1,
// =>
// hasSequence: True, reverse: True
//
// index: +1/-2/+3/+4,
// limitOrdering: +3/+4,
// keyLength: 1,
// =>
// hasSequence: False, reverse: False
//
func indexHasOrderingSequence(
	md *opt.Metadata,
	scan memo.RelExpr,
	sp *memo.ScanPrivate,
	limitOrdering physical.OrderingChoice,
	keyLength int,
) (hasSequence, reverse bool) {
	tableMeta := md.TableMeta(sp.Table)
	index := tableMeta.Table.Index(sp.Index)

	if keyLength > index.ColumnCount() {
		// The key contains more columns than the index. The limit ordering sequence
		// cannot be part of the index ordering.
		return false, false
	}

	// Create a copy of the Scan's FuncDepSet, and add the first 'keyCount'
	// columns from the index as constant columns. The columns are constant
	// because the span contains only a single key on those columns.
	var fds props.FuncDepSet
	fds.CopyFrom(&scan.Relational().FuncDeps)
	prefixCols := opt.ColSet{}
	for i := 0; i < keyLength; i++ {
		col := sp.Table.ColumnID(index.Column(i).Ordinal)
		prefixCols.Add(col)
	}
	fds.AddConstants(prefixCols)

	// Use fds to simplify a copy of the limit ordering; the prefix columns will
	// become part of the optional ColSet.
	requiredOrdering := limitOrdering.Copy()
	requiredOrdering.Simplify(&fds)

	// If the ScanPrivate can satisfy requiredOrdering, it must return columns
	// ordered by a prefix of length keyLength, followed by the columns of
	// limitOrdering.
	return ordering.ScanPrivateCanProvide(md, sp, &requiredOrdering)
}

// makeNewScan constructs a new Scan operator with a new TableID and the given
// limit and span. All ColumnIDs and references to those ColumnIDs are
// replaced with new ones from the new TableID. All other fields are simply
// copied from the old ScanPrivate.
func (c *CustomFuncs) makeNewScan(
	sp *memo.ScanPrivate, newHardLimit memo.ScanLimit, span *constraint.Span,
) memo.RelExpr {
	newScanPrivate := c.DuplicateScanPrivate(sp)

	// duplicateScanPrivate does not initialize the Constraint or HardLimit
	// fields, so we do that now.
	newScanPrivate.HardLimit = newHardLimit

	// Construct the new Constraint field with the given span and remapped
	// ordering columns.
	var newSpans constraint.Spans
	newSpans.InitSingleSpan(span)
	newConstraint := &constraint.Constraint{
		Columns: sp.Constraint.Columns.RemapColumns(sp.Table, newScanPrivate.Table),
		Spans:   newSpans,
	}
	newScanPrivate.Constraint = newConstraint

	return c.e.f.ConstructScan(newScanPrivate)
}

// ----------------------------------------------------------------------
//
// Join Rules
//   Custom match and replace functions used with join.opt rules.
//
// ----------------------------------------------------------------------

// CommuteJoinFlags returns a join private for the commuted join (where the left
// and right sides are swapped). It adjusts any join flags that are specific to
// one side.
func (c *CustomFuncs) CommuteJoinFlags(p *memo.JoinPrivate) *memo.JoinPrivate {
	if p.Flags.Empty() {
		return p
	}

	// swap is a helper function which swaps the values of two (single-bit) flags.
	swap := func(f, a, b memo.JoinFlags) memo.JoinFlags {
		// If the bits are different, flip them both.
		if f.Has(a) != f.Has(b) {
			f ^= (a | b)
		}
		return f
	}
	f := p.Flags
	f = swap(f, memo.AllowLookupJoinIntoLeft, memo.AllowLookupJoinIntoRight)
	f = swap(f, memo.AllowHashJoinStoreLeft, memo.AllowHashJoinStoreRight)
	if p.Flags == f {
		return p
	}
	res := *p
	res.Flags = f
	return &res
}

// GenerateMergeJoins spawns MergeJoinOps, based on any interesting orderings.
func (c *CustomFuncs) GenerateMergeJoins(
	grp memo.RelExpr,
	originalOp opt.Operator,
	left, right memo.RelExpr,
	on memo.FiltersExpr,
	joinPrivate *memo.JoinPrivate,
) {
	if !joinPrivate.Flags.Has(memo.AllowMergeJoin) {
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

	if !joinPrivate.Flags.Has(memo.AllowHashJoinStoreLeft) &&
		!joinPrivate.Flags.Has(memo.AllowHashJoinStoreRight) {
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
	joinPrivate *memo.JoinPrivate,
) {
	if !joinPrivate.Flags.Has(memo.AllowLookupJoinIntoRight) {
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

	// TODO(mgartner): Use partial indexes for lookup joins when the predicate
	// is implied by the on filter.
	iter := makeScanIndexIter(c.e.mem, scanPrivate, rejectInvertedIndexes|rejectPartialIndexes)
	for iter.Next() {
		// Find the longest prefix of index key columns that are constrained by
		// an equality with another column or a constant.
		numIndexKeyCols := iter.Index().LaxKeyColumnCount()

		var projections memo.ProjectionsExpr
		var constFilters memo.FiltersExpr

		// Check if the first column in the index has an equality constraint, or if
		// it is constrained to a constant value. This check doesn't guarantee that
		// we will find lookup join key columns, but it avoids the unnecessary work
		// in most cases.
		firstIdxCol := scanPrivate.Table.ColumnID(iter.Index().Column(0).Ordinal)
		if _, ok := rightEq.Find(firstIdxCol); !ok {
			if _, _, ok := c.findConstantFilter(on, firstIdxCol); !ok {
				continue
			}
		}

		lookupJoin := memo.LookupJoinExpr{Input: input}
		lookupJoin.JoinPrivate = *joinPrivate
		lookupJoin.JoinType = joinType
		lookupJoin.Table = scanPrivate.Table
		lookupJoin.Index = iter.IndexOrdinal()

		lookupJoin.KeyCols = make(opt.ColList, 0, numIndexKeyCols)
		rightSideCols := make(opt.ColList, 0, numIndexKeyCols)
		needProjection := false

		// All the lookup conditions must apply to the prefix of the index and so
		// the projected columns created must be created in order.
		for j := 0; j < numIndexKeyCols; j++ {
			idxCol := scanPrivate.Table.ColumnID(iter.Index().Column(j).Ordinal)
			if eqIdx, ok := rightEq.Find(idxCol); ok {
				lookupJoin.KeyCols = append(lookupJoin.KeyCols, leftEq[eqIdx])
				rightSideCols = append(rightSideCols, idxCol)
				continue
			}

			// Try to find a filter that constrains this column to a non-NULL constant
			// value. We cannot use a NULL value because the lookup join implements
			// logic equivalent to simple equality between columns (where NULL never
			// equals anything).
			foundVal, onIdx, ok := c.findConstantFilter(on, idxCol)
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
			constFilters = append(constFilters, on[onIdx])
		}

		if len(lookupJoin.KeyCols) == 0 {
			// We couldn't find equality columns which we can lookup.
			continue
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
		lookupJoin.On = memo.ExtractRemainingJoinFilters(on, lookupJoin.KeyCols, rightSideCols)
		lookupJoin.On.RemoveCommonFilters(constFilters)
		lookupJoin.ConstFilters = constFilters

		if iter.IsCovering() {
			// Case 1 (see function comment).
			lookupJoin.Cols = scanPrivate.Cols.Union(inputProps.OutputCols)
			c.e.mem.AddLookupJoinToGroup(&lookupJoin, grp)
			continue
		}

		// All code that follows is for case 2 (see function comment).

		if scanPrivate.Flags.NoIndexJoin {
			continue
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
			continue
		}

		if pkCols == nil {
			pkIndex := md.Table(scanPrivate.Table).Index(cat.PrimaryIndex)
			pkCols = make(opt.ColList, pkIndex.KeyColumnCount())
			for i := range pkCols {
				pkCols[i] = scanPrivate.Table.ColumnID(pkIndex.Column(i).Ordinal)
			}
		}

		// The lower LookupJoin must return all PK columns (they are needed as key
		// columns for the index join).
		indexCols := iter.IndexColumns()
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
		indexJoin.LookupColsAreTableKey = true

		// Create the LookupJoin for the index join in the same group.
		c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
	}
}

// GenerateGeoLookupJoins is similar to GenerateLookupJoins, but instead
// of generating lookup joins with regular indexes, it generates geospatial
// lookup joins with inverted geospatial indexes. Since these indexes are not
// covering, all geospatial lookup joins must be wrapped in an index join with
// the primary index of the table. See the description of Case 2 in the comment
// above GenerateLookupJoins for details about how this works.
// TODO(rytaft): generalize this function to be GenerateInvertedJoins and add
//  support for JSON and array inverted indexes.
// TODO(rytaft): handle more complicated geo-spatial expressions
//  e.g. ST_Intersects(x, y) AND ST_Covers(x, y) where y is the indexed value.
func (c *CustomFuncs) GenerateGeoLookupJoins(
	grp memo.RelExpr,
	joinType opt.Operator,
	input memo.RelExpr,
	scanPrivate *memo.ScanPrivate,
	on memo.FiltersExpr,
	joinPrivate *memo.JoinPrivate,
	fn opt.ScalarExpr,
) {
	if !joinPrivate.Flags.Has(memo.AllowLookupJoinIntoRight) {
		return
	}

	// Geospatial lookup joins are not covering, so we must wrap them in an
	// index join.
	if scanPrivate.Flags.NoIndexJoin {
		return
	}

	if !IsGeoIndexFunction(fn) {
		panic(errors.AssertionFailedf(
			"GenerateGeoLookupJoins called on a function that cannot be index-accelerated",
		))
	}

	function := fn.(*memo.FunctionExpr)
	inputProps := input.Relational()

	// Extract the the variable inputs to the geospatial function.
	if function.Args.ChildCount() < 2 {
		panic(errors.AssertionFailedf(
			"all index-accelerated geospatial functions should have at least two arguments",
		))
	}

	// The first argument should come from the input.
	variable, ok := function.Args.Child(0).(*memo.VariableExpr)
	if !ok {
		panic(errors.AssertionFailedf(
			"GenerateGeoLookupJoins called on function containing non-variable inputs",
		))
	}
	if !inputProps.OutputCols.Contains(variable.Col) {
		// TODO(rytaft): Commute the geospatial function in this case.
		//   Covers      <->  CoveredBy
		//   Intersects  <->  Intersects
		return
	}
	inputGeoCol := variable.Col

	// The second argument should be a variable corresponding to the index
	// column.
	variable, ok = function.Args.Child(1).(*memo.VariableExpr)
	if !ok {
		panic(errors.AssertionFailedf(
			"GenerateGeoLookupJoins called on function containing non-variable inputs",
		))
	}
	indexGeoCol := variable.Col

	var pkCols opt.ColList

	// TODO(mgartner): Use partial indexes for geolookup joins when the
	// predicate is implied by the on filter.
	iter := makeScanIndexIter(c.e.mem, scanPrivate, rejectNonInvertedIndexes|rejectPartialIndexes)
	for iter.Next() {
		if scanPrivate.Table.ColumnID(iter.Index().Column(0).Ordinal) != indexGeoCol {
			continue
		}

		if pkCols == nil {
			tab := c.e.mem.Metadata().Table(scanPrivate.Table)
			pkIndex := tab.Index(cat.PrimaryIndex)
			pkCols = make(opt.ColList, pkIndex.KeyColumnCount())
			for i := range pkCols {
				pkCols[i] = scanPrivate.Table.ColumnID(pkIndex.Column(i).Ordinal)
			}
		}

		// Though the index is marked as containing the geospatial column being
		// indexed, it doesn't actually, and it is only valid to extract the
		// primary key columns from it.
		indexCols := pkCols.ToSet()

		lookupJoin := memo.InvertedJoinExpr{Input: input}
		lookupJoin.JoinPrivate = *joinPrivate
		lookupJoin.JoinType = joinType
		lookupJoin.Table = scanPrivate.Table
		lookupJoin.Index = iter.IndexOrdinal()
		lookupJoin.InvertedExpr = function
		lookupJoin.InvertedCol = indexGeoCol
		lookupJoin.InputCol = inputGeoCol
		lookupJoin.Cols = indexCols.Union(inputProps.OutputCols)

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
		indexJoin.Cols = scanPrivate.Cols.Union(inputProps.OutputCols)
		indexJoin.LookupColsAreTableKey = true

		// Create the LookupJoin for the index join in the same group.
		c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
	}
}

// IsGeoIndexFunction returns true if the given function is a geospatial
// function that can be index-accelerated.
func (c *CustomFuncs) IsGeoIndexFunction(fn opt.ScalarExpr) bool {
	return IsGeoIndexFunction(fn)
}

// HasAllVariableArgs returns true if all the arguments to the given function
// are variables.
func (c *CustomFuncs) HasAllVariableArgs(fn opt.ScalarExpr) bool {
	function := fn.(*memo.FunctionExpr)
	for i, n := 0, function.Args.ChildCount(); i < n; i++ {
		if _, ok := function.Args.Child(i).(*memo.VariableExpr); !ok {
			return false
		}
	}
	return true
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
		colID := tabID.ColumnID(leftIndex.Column(i).Ordinal)
		if !fixedCols.Contains(colID) {
			break
		}
	}
	for ; j < rightCnt; j++ {
		colID := tabID.ColumnID(rightIndex.Column(j).Ordinal)
		if !fixedCols.Contains(colID) {
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

// fixedColsForZigzag is a helper function to generate FixedCols lists for the
// zigzag join expression. This function iterates through the columns of the
// specified index in order until it comes across the first column ID that is
// not constrained to a constant.
func (c *CustomFuncs) fixedColsForZigzag(
	index cat.Index, tabID opt.TableID, filters memo.FiltersExpr,
) (fixedCols opt.ColList, vals memo.ScalarListExpr, typs []*types.T) {
	for i, cnt := 0, index.ColumnCount(); i < cnt; i++ {
		colID := tabID.ColumnID(index.Column(i).Ordinal)
		val := memo.ExtractValueForConstColumn(filters, c.e.mem, c.e.evalCtx, colID)
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
	//
	// TODO(mgartner): Use partial indexes for zigzag joins when the predicate
	// is implied by the filter.
	//
	// TODO(mgartner): We should consider primary indexes when it has multiple
	// columns and only the first is being constrained.
	iter := makeScanIndexIter(c.e.mem, scanPrivate, rejectPrimaryIndex|rejectInvertedIndexes|rejectPartialIndexes)
	for iter.Next() {
		leftFixed := c.indexConstrainedCols(iter.Index(), scanPrivate.Table, fixedCols)
		// Short-circuit quickly if the first column in the index is not a fixed
		// column.
		if leftFixed.Len() == 0 {
			continue
		}

		iter2 := makeScanIndexIter(c.e.mem, scanPrivate, rejectPrimaryIndex|rejectInvertedIndexes|rejectPartialIndexes)
		// Only look at indexes after this one.
		iter2.StartAfter(iter.IndexOrdinal())

		for iter2.Next() {
			rightFixed := c.indexConstrainedCols(iter2.Index(), scanPrivate.Table, fixedCols)
			// If neither side contributes a fixed column not contributed by the
			// other, then there's no reason to zigzag on this pair of indexes.
			if leftFixed.SubsetOf(rightFixed) || rightFixed.SubsetOf(leftFixed) {
				continue
			}
			// Columns that are in both indexes are, by definition, equal.
			leftCols := iter.IndexColumns()
			rightCols := iter2.IndexColumns()
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
				tab,
				scanPrivate.Table,
				iter.Index(),
				iter2.Index(),
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
			pkIndex := tab.Index(cat.PrimaryIndex)
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
					LeftIndex:   iter.IndexOrdinal(),
					RightTable:  scanPrivate.Table,
					RightIndex:  iter2.IndexOrdinal(),
					LeftEqCols:  leftEqCols,
					RightEqCols: rightEqCols,
				},
			}

			leftFixedCols, leftVals, leftTypes := c.fixedColsForZigzag(
				iter.Index(), scanPrivate.Table, filters,
			)
			rightFixedCols, rightVals, rightTypes := c.fixedColsForZigzag(
				iter2.Index(), scanPrivate.Table, filters,
			)

			if len(leftFixedCols) != leftFixed.Len() || len(rightFixedCols) != rightFixed.Len() {
				panic(errors.AssertionFailedf("could not populate all fixed columns for zig zag join"))
			}

			zigzagJoin.LeftFixedCols = leftFixedCols
			zigzagJoin.RightFixedCols = rightFixedCols

			leftTupleTyp := types.MakeTuple(leftTypes)
			rightTupleTyp := types.MakeTuple(rightTypes)
			zigzagJoin.FixedVals = memo.ScalarListExpr{
				c.e.f.ConstructTuple(leftVals, leftTupleTyp),
				c.e.f.ConstructTuple(rightVals, rightTupleTyp),
			}

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
		}
	}
}

// indexConstrainedCols computes the set of columns in allFixedCols which form
// a prefix of the key columns in idx.
func (c *CustomFuncs) indexConstrainedCols(
	idx cat.Index, tab opt.TableID, allFixedCols opt.ColSet,
) opt.ColSet {
	var constrained opt.ColSet
	for i, n := 0, idx.ColumnCount(); i < n; i++ {
		col := tab.ColumnID(idx.Column(i).Ordinal)
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
	sb.init(c, scanPrivate.Table)

	// Iterate over all inverted indexes.
	// TODO(mgartner): Use partial indexes for inverted zigzag joins when the
	// predicate is implied by the filter.
	iter := makeScanIndexIter(c.e.mem, scanPrivate, rejectNonInvertedIndexes|rejectPartialIndexes)
	for iter.Next() {
		// See if there are two or more constraints that can be satisfied
		// by this inverted index. This is possible with inverted indexes as
		// opposed to secondary indexes, because one row in the primary index
		// can often correspond to multiple rows in an inverted index. This
		// function generates all constraints it can derive for this index;
		// not all of which might get used in this function.
		constraints, ok := c.allInvIndexConstraints(
			filters, scanPrivate.Table, iter.IndexOrdinal(),
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
				LeftIndex:  iter.IndexOrdinal(),
				RightTable: scanPrivate.Table,
				RightIndex: iter.IndexOrdinal(),
			},
		}

		// Get constant values from each constraint. Add them to FixedVals as
		// tuples, with associated Column IDs in both {Left,Right}FixedCols.
		leftVals := make(memo.ScalarListExpr, minPrefix)
		leftTypes := make([]*types.T, minPrefix)
		rightVals := make(memo.ScalarListExpr, minPrefix)
		rightTypes := make([]*types.T, minPrefix)

		zigzagJoin.LeftFixedCols = make(opt.ColList, minPrefix)
		zigzagJoin.RightFixedCols = make(opt.ColList, minPrefix)
		for i := 0; i < minPrefix; i++ {
			leftVal := constraint.Spans.Get(0).StartKey().Value(i)
			rightVal := constraint2.Spans.Get(0).StartKey().Value(i)

			leftVals[i] = c.e.f.ConstructConstVal(leftVal, leftVal.ResolvedType())
			leftTypes[i] = leftVal.ResolvedType()
			rightVals[i] = c.e.f.ConstructConstVal(rightVal, rightVal.ResolvedType())
			rightTypes[i] = rightVal.ResolvedType()
			zigzagJoin.LeftFixedCols[i] = constraint.Columns.Get(i).ID()
			zigzagJoin.RightFixedCols[i] = constraint.Columns.Get(i).ID()
		}

		leftTupleTyp := types.MakeTuple(leftTypes)
		rightTupleTyp := types.MakeTuple(rightTypes)
		zigzagJoin.FixedVals = memo.ScalarListExpr{
			c.e.f.ConstructTuple(leftVals, leftTupleTyp),
			c.e.f.ConstructTuple(rightVals, rightTupleTyp),
		}

		// Set equality columns - all remaining columns after the fixed prefix
		// need to be equal.
		eqColLen := iter.Index().ColumnCount() - minPrefix
		zigzagJoin.LeftEqCols = make(opt.ColList, eqColLen)
		zigzagJoin.RightEqCols = make(opt.ColList, eqColLen)
		for i := minPrefix; i < iter.Index().ColumnCount(); i++ {
			colID := scanPrivate.Table.ColumnID(iter.Index().Column(i).Ordinal)
			zigzagJoin.LeftEqCols[i-minPrefix] = colID
			zigzagJoin.RightEqCols[i-minPrefix] = colID
		}
		zigzagJoin.On = filters

		// Don't output the first column (i.e. the inverted index's JSON key
		// col) from the zigzag join. It could contain partial values, so
		// presenting it in the output or checking ON conditions against
		// it makes little sense.
		zigzagCols := iter.IndexColumns()
		for i, cnt := 0, iter.Index().KeyColumnCount(); i < cnt; i++ {
			colID := scanPrivate.Table.ColumnID(iter.Index().Column(i).Ordinal)
			zigzagCols.Remove(colID)
		}

		tab := c.e.mem.Metadata().Table(scanPrivate.Table)
		pkIndex := tab.Index(cat.PrimaryIndex)
		pkCols := make(opt.ColList, pkIndex.KeyColumnCount())
		for i := range pkCols {
			pkCols[i] = scanPrivate.Table.ColumnID(pkIndex.Column(i).Ordinal)
			// Ensure primary key columns are always retrieved from the zigzag
			// join.
			zigzagCols.Add(pkCols[i])
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
	}
}

// deriveJoinSize returns the number of base relations (i.e., not joins)
// being joined underneath the given relational expression.
func (c *CustomFuncs) deriveJoinSize(e memo.RelExpr) int {
	relProps := e.Relational()
	if relProps.IsAvailable(props.JoinSize) {
		return relProps.Rule.JoinSize
	}
	relProps.SetAvailable(props.JoinSize)

	switch j := e.(type) {
	case *memo.InnerJoinExpr:
		relProps.Rule.JoinSize = c.deriveJoinSize(j.Left) + c.deriveJoinSize(j.Right)
	default:
		relProps.Rule.JoinSize = 1
	}

	return relProps.Rule.JoinSize
}

// ShouldReorderJoins returns whether the optimizer should attempt to find
// a better ordering of inner joins.
func (c *CustomFuncs) ShouldReorderJoins(left, right memo.RelExpr) bool {
	// TODO(justin): referencing left and right here is a hack: ideally
	// we'd want to be able to reference the logical properties of the
	// expression being explored in this CustomFunc.
	size := c.deriveJoinSize(left) + c.deriveJoinSize(right)
	return size <= c.e.evalCtx.SessionData.ReorderJoinsLimit
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

// ----------------------------------------------------------------------
//
// GroupBy Rules
//   Custom match and replace functions used with groupby.opt rules.
//
// ----------------------------------------------------------------------

// IsCanonicalGroupBy returns true if the private is for the canonical version
// of the grouping operator. This is the operator that is built initially (and
// has all grouping columns as optional in the ordering), as opposed to variants
// generated by the GenerateStreamingGroupBy exploration rule.
func (c *CustomFuncs) IsCanonicalGroupBy(private *memo.GroupingPrivate) bool {
	return private.Ordering.Any() || private.GroupingCols.SubsetOf(private.Ordering.Optional)
}

// MakeProjectFromPassthroughAggs constructs a top-level Project operator that
// contains one output column per function in the given aggregrate list. The
// input expression is expected to return zero or one rows, and the aggregate
// functions are expected to always pass through their values in that case.
func (c *CustomFuncs) MakeProjectFromPassthroughAggs(
	grp memo.RelExpr, input memo.RelExpr, aggs memo.AggregationsExpr,
) {
	if !input.Relational().Cardinality.IsZeroOrOne() {
		panic(errors.AssertionFailedf("input expression cannot have more than one row: %v", input))
	}

	var passthrough opt.ColSet
	projections := make(memo.ProjectionsExpr, 0, len(aggs))
	for i := range aggs {
		// If aggregate remaps the column ID, need to synthesize projection item;
		// otherwise, can just pass through.
		variable := aggs[i].Agg.Child(0).(*memo.VariableExpr)
		if variable.Col == aggs[i].Col {
			passthrough.Add(variable.Col)
		} else {
			projections = append(projections, c.e.f.ConstructProjectionsItem(variable, aggs[i].Col))
		}
	}
	c.e.mem.AddProjectToGroup(&memo.ProjectExpr{
		Input:       input,
		Projections: projections,
		Passthrough: passthrough,
	}, grp)
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
			if private.GroupingCols.Contains(oCol) || intraOrd.Optional.Contains(oCol) {
				// Grouping or optional column.
				continue
			}

			if intraIdx < len(intraOrd.Columns) &&
				intraOrd.Columns[intraIdx].Group.Contains(oCol) &&
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

		newPrivate := *private
		newPrivate.Ordering = newOrd

		switch op {
		case opt.GroupByOp:
			newExpr := memo.GroupByExpr{
				Input:           input,
				Aggregations:    aggs,
				GroupingPrivate: newPrivate,
			}
			c.e.mem.AddGroupByToGroup(&newExpr, grp)

		case opt.DistinctOnOp:
			newExpr := memo.DistinctOnExpr{
				Input:           input,
				Aggregations:    aggs,
				GroupingPrivate: newPrivate,
			}
			c.e.mem.AddDistinctOnToGroup(&newExpr, grp)

		case opt.EnsureDistinctOnOp:
			newExpr := memo.EnsureDistinctOnExpr{
				Input:           input,
				Aggregations:    aggs,
				GroupingPrivate: newPrivate,
			}
			c.e.mem.AddEnsureDistinctOnToGroup(&newExpr, grp)

		case opt.UpsertDistinctOnOp:
			newExpr := memo.UpsertDistinctOnExpr{
				Input:           input,
				Aggregations:    aggs,
				GroupingPrivate: newPrivate,
			}
			c.e.mem.AddUpsertDistinctOnToGroup(&newExpr, grp)

		case opt.EnsureUpsertDistinctOnOp:
			newExpr := memo.EnsureUpsertDistinctOnExpr{
				Input:           input,
				Aggregations:    aggs,
				GroupingPrivate: newPrivate,
			}
			c.e.mem.AddEnsureUpsertDistinctOnToGroup(&newExpr, grp)
		}
	}
}

// OtherAggsAreConst returns true if all items in the given aggregate list
// contain ConstAgg functions, except for the "except" item. The ConstAgg
// functions will always return the same value, as long as there is at least
// one input row.
func (c *CustomFuncs) OtherAggsAreConst(
	aggs memo.AggregationsExpr, except *memo.AggregationsItem,
) bool {
	for i := range aggs {
		agg := &aggs[i]
		if agg == except {
			continue
		}

		switch agg.Agg.Op() {
		case opt.ConstAggOp:
			// Ensure that argument is a VariableOp.
			if agg.Agg.Child(0).Op() != opt.VariableOp {
				return false
			}

		default:
			return false
		}
	}
	return true
}

// MakeOrderingChoiceFromColumn constructs a new OrderingChoice with
// one element in the sequence: the columnID in the order defined by
// (MIN/MAX) operator. This function was originally created to be used
// with the Replace(Min|Max)WithLimit exploration rules.
//
// WARNING: The MinOp case can return a NULL value if the column allows it. This
// is because NULL values sort first in CRDB.
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

// ExprPair stores a left and right ScalarExpr. ExprPairForSplitDisjunction
// returns ExprPair, which can be deconstructed later, to avoid extra
// computation in determining the left and right expression groups.
type ExprPair struct {
	left          opt.ScalarExpr
	right         opt.ScalarExpr
	itemToReplace *memo.FiltersItem
}

// ExprPairLeft returns the left ScalarExpr in an ExprPair.
func (c *CustomFuncs) ExprPairLeft(ep ExprPair) opt.ScalarExpr {
	return ep.left
}

// ExprPairRight returns the right ScalarExpr in an ExprPair.
func (c *CustomFuncs) ExprPairRight(ep ExprPair) opt.ScalarExpr {
	return ep.right
}

// ExprPairFiltersItemToReplace returns the original FiltersItem that the
// ExprPair was generated from. This FiltersItem should be replaced by
// ExprPairLeft and ExprPairRight in the newly generated filters in
// SplitDisjunction(AddKey).
func (c *CustomFuncs) ExprPairFiltersItemToReplace(ep ExprPair) *memo.FiltersItem {
	return ep.itemToReplace
}

// ExprPairSucceeded returns true if the ExprPair is not nil.
func (c *CustomFuncs) ExprPairSucceeded(ep ExprPair) bool {
	return ep != ExprPair{}
}

// ExprPairForSplitDisjunction finds the first "interesting" ExprPair in the
// filters and returns it. If an "interesting" ExprPair is not found, an empty
// ExprPair is returned.
//
// For details on what makes an ExprPair "interesting", see
// buildExprPairForSplitDisjunction.
func (c *CustomFuncs) ExprPairForSplitDisjunction(
	sp *memo.ScanPrivate, filters memo.FiltersExpr,
) ExprPair {
	for i := range filters {
		if filters[i].Condition.Op() == opt.OrOp {
			ep := c.buildExprPairForSplitDisjunction(sp, &filters[i])
			if (ep != ExprPair{}) {
				return ep
			}
		}
	}
	return ExprPair{}
}

// buildExprPairForSplitDisjunction groups disjuction sub-expressions into an
// "interesting" ExprPair.
//
// An "interesting" ExprPair is one where:
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
// If an index exists on u and another on v, an "interesting" ExprPair exists,
// ("u = 1", "v = 1"). If both indexes do not exist, there is no "interesting"
// ExprPair possible.
//
// Now consider the expression:
//
//   u = 1 OR u = 2
//
// There is no possible "interesting" ExprPair here because the left and right
// sides of the disjunction share the same columns.
//
// buildExprPairForSplitDisjunction groups all sub-expressions adjacent to the
// input's top-level OrExpr into left and right expression groups. These two
// groups form the new filter expressions on the left and right side of the
// generated UnionAll in SplitDisjunction(AddKey).
//
// All sub-expressions with the same columns as the left-most sub-expression
// are grouped in the left group. All other sub-expressions are grouped in the
// right group.
//
// buildExprPairForSplitDisjunction returns an empty ExprPair if all
// sub-expressions have the same columns. It also returns an empty ExprPair if
// either expression in the pair found is not likely to constrain an index
// scan. See canMaybeConstrainIndexWithCols for details on how this is
// determined.
func (c *CustomFuncs) buildExprPairForSplitDisjunction(
	sp *memo.ScanPrivate, filter *memo.FiltersItem,
) ExprPair {
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
		return ExprPair{}
	}

	return ExprPair{
		left:          c.constructOr(leftExprs),
		right:         c.constructOr(rightExprs),
		itemToReplace: filter,
	}
}

// canMaybeConstrainIndexWithCols returns true if any indexes on the
// ScanPrivate's table could be constrained by cols. It is a fast check for
// SplitDisjunction to avoid matching a large number of queries that won't
// obviously be improved by the rule.
//
// canMaybeConstrainIndexWithCols checks for an intersection between the input
// columns and an index's columns. An intersection between column sets implies
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
func (c *CustomFuncs) canMaybeConstrainIndexWithCols(sp *memo.ScanPrivate, cols opt.ColSet) bool {
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(sp.Table)

	iter := makeScanIndexIter(c.e.mem, sp, rejectNoIndexes)
	for iter.Next() {
		// Iterate through all indexes of the table and return true if cols
		// intersect with the index's key columns.
		indexColumns := tabMeta.IndexKeyColumns(iter.IndexOrdinal())
		if cols.Intersects(indexColumns) {
			return true
		}
	}

	return false
}

// DuplicateScanPrivate constructs a new ScanPrivate with new table and column
// IDs. Only the Index, Flags and Locking fields are copied from the old
// ScanPrivate, so the new ScanPrivate will not have constraints even if the old
// one did.
func (c *CustomFuncs) DuplicateScanPrivate(sp *memo.ScanPrivate) *memo.ScanPrivate {
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(sp.Table)
	newTableID := md.AddTable(tabMeta.Table, &tabMeta.Alias)

	var newColIDs opt.ColSet
	cols := sp.Cols
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		ord := tabMeta.MetaID.ColumnOrdinal(col)
		newColID := newTableID.ColumnID(ord)
		newColIDs.Add(newColID)
	}

	return &memo.ScanPrivate{
		Table:   newTableID,
		Index:   sp.Index,
		Cols:    newColIDs,
		Flags:   sp.Flags,
		Locking: sp.Locking,
	}
}

// MapScanFilterCols returns a new FiltersExpr with all the src column IDs in
// the input expression replaced with column IDs in dst.
//
// NOTE: Every ColumnID in src must map to the a ColumnID in dst with the same
// relative position in the ColSets. For example, if src and dst are (1, 5, 6)
// and (7, 12, 15), then the following mapping would be applied:
//
//   1 => 7
//   5 => 12
//   6 => 15
func (c *CustomFuncs) MapScanFilterCols(
	filters memo.FiltersExpr, src *memo.ScanPrivate, dst *memo.ScanPrivate,
) memo.FiltersExpr {
	if src.Cols.Len() != dst.Cols.Len() {
		panic(errors.AssertionFailedf(
			"src and dst must have the same number of columns, src.Cols: %v, dst.Cols: %v",
			src.Cols,
			dst.Cols,
		))
	}

	// Map each column in src to a column in dst based on the relative position
	// of both the src and dst ColumnIDs in the ColSet.
	var colMap util.FastIntMap
	dstCol, _ := dst.Cols.Next(0)
	for srcCol, ok := src.Cols.Next(0); ok; srcCol, ok = src.Cols.Next(srcCol + 1) {
		colMap.Set(int(srcCol), int(dstCol))
		dstCol, _ = dst.Cols.Next(dstCol + 1)
	}

	// Map the columns of each filter in the FiltersExpr.
	newFilters := make([]memo.FiltersItem, len(filters))
	for i := range filters {
		expr := c.MapFiltersItemCols(&filters[i], colMap)
		newFilters[i] = c.e.f.ConstructFiltersItem(expr)
	}

	return newFilters
}

// MakeSetPrivateForSplitDisjunction constructs a new SetPrivate with column sets
// from the left and right ScanPrivate. We use the same ColList for the
// LeftCols and OutCols of the SetPrivate because we've used the original
// ScanPrivate column IDs for the left ScanPrivate and those are safe to use as
// output column IDs of the Union expression.
func (c *CustomFuncs) MakeSetPrivateForSplitDisjunction(
	left, right *memo.ScanPrivate,
) *memo.SetPrivate {
	leftAndOutCols := opt.ColSetToList(left.Cols)
	return &memo.SetPrivate{
		LeftCols:  leftAndOutCols,
		RightCols: opt.ColSetToList(right.Cols),
		OutCols:   leftAndOutCols,
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

// NewDatumToInvertedExpr returns a new DatumToInvertedExpr. Currently there
// is only one possible implementation returned, geoDatumToInvertedExpr.
func NewDatumToInvertedExpr(
	expr tree.TypedExpr, desc *sqlbase.IndexDescriptor,
) (invertedexpr.DatumToInvertedExpr, error) {
	if geoindex.IsEmptyConfig(&desc.GeoConfig) {
		return nil, fmt.Errorf("inverted joins are currently only supported for geospatial indexes")
	}

	return NewGeoDatumToInvertedExpr(expr, &desc.GeoConfig)
}
