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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partition"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// GenerateIndexScans enumerates all non-inverted secondary indexes on the given
// Scan operator's table and generates an alternate Scan operator for each index
// that includes the set of needed columns specified in the ScanOpDef.
//
// This transformation can only consider partial indexes that are guaranteed to
// index every row in the table. Therefore, only partial indexes with predicates
// that always evaluate to true are considered. Such an index is pseudo-partial
// in that it behaves the exactly the same as a non-partial secondary index.
//
// NOTE: This does not generate index joins for non-covering indexes (except in
//       case of ForceIndex). Index joins are usually only introduced "one level
//       up", when the Scan operator is wrapped by an operator that constrains
//       or limits scan output in some way (e.g. Select, Limit, InnerJoin).
//       Index joins are only lower cost when their input does not include all
//       rows from the table. See GenerateConstrainedScans,
//       GenerateLimitedScans, and GenerateLimitedGroupByScans for cases where
//       index joins are introduced into the memo.
func (c *CustomFuncs) GenerateIndexScans(grp memo.RelExpr, scanPrivate *memo.ScanPrivate) {
	// Iterate over all non-inverted and non-partial secondary indexes.
	var pkCols opt.ColSet
	var iter scanIndexIter
	iter.Init(c.e.evalCtx, c.e.f, c.e.mem, &c.im, scanPrivate, nil /* filters */, rejectPrimaryIndex|rejectInvertedIndexes)
	iter.ForEach(func(index cat.Index, filters memo.FiltersExpr, indexCols opt.ColSet, isCovering bool, constProj memo.ProjectionsExpr) {
		// The iterator only produces pseudo-partial indexes (the predicate is
		// true) because no filters are passed to iter.Init to imply a partial
		// index predicate. constProj is a projection of constant values based
		// on a partial index predicate. It should always be empty because a
		// pseudo-partial index cannot hold a column constant. If it is not, we
		// panic to avoid performing a logically incorrect transformation.
		if len(constProj) != 0 {
			panic(errors.AssertionFailedf("expected constProj to be empty"))
		}

		// If the secondary index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if isCovering {
			scan := memo.ScanExpr{ScanPrivate: *scanPrivate}
			scan.Index = index.Ordinal()
			c.e.mem.AddScanToGroup(&scan, grp)
			return
		}

		// Otherwise, if the index must be forced, then construct an IndexJoin
		// operator that provides the columns missing from the index. Note that
		// if ForceIndex=true, scanIndexIter only returns the one index that is
		// being forced, so no need to check that here.
		if !scanPrivate.Flags.ForceIndex {
			return
		}

		var sb indexScanBuilder
		sb.Init(c, scanPrivate.Table)

		// Calculate the PK columns once.
		if pkCols.Empty() {
			pkCols = c.PrimaryKeyCols(scanPrivate.Table)
		}

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = index.Ordinal()
		newScanPrivate.Cols = indexCols.Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(pkCols)
		sb.SetScan(&newScanPrivate)

		sb.AddIndexJoin(scanPrivate.Cols)
		sb.Build(grp)
	})
}

// CanMaybeGenerateLocalityOptimizedScan returns true if it may be possible to
// generate a locality optimized scan from the given scan private.
// CanMaybeGenerateLocalityOptimizedScan performs simple checks that are
// inexpensive to execute and can filter out cases where the optimization
// definitely cannot apply. See the comment above the
// GenerateLocalityOptimizedScan rule for details.
func (c *CustomFuncs) CanMaybeGenerateLocalityOptimizedScan(scanPrivate *memo.ScanPrivate) bool {
	// Respect the session setting LocalityOptimizedSearch.
	if !c.e.evalCtx.SessionData().LocalityOptimizedSearch {
		return false
	}

	if scanPrivate.LocalityOptimized {
		// This scan has already been locality optimized.
		return false
	}

	if scanPrivate.Constraint == nil {
		// Since we have no constraint, we must have a limit to use this
		// optimization. We also require the limit to be less than the kv batch
		// size, since it's probably better to use DistSQL once we're scanning
		// multiple batches.
		// TODO(rytaft): Revisit this when we have a more accurate cost model for
		//               data distribution.
		if scanPrivate.HardLimit == 0 || rowinfra.KeyLimit(scanPrivate.HardLimit) > rowinfra.ProductionKVBatchSize {
			return false
		}
	} else {
		// This scan should have at least two spans, or we won't be able to move one
		// of the spans to a separate remote scan.
		if scanPrivate.Constraint.Spans.Count() < 2 {
			return false
		}

		// Don't apply the rule if there are too many spans, since the rule code is
		// O(# spans * # prefixes * # datums per prefix).
		if scanPrivate.Constraint.Spans.Count() > 10000 {
			return false
		}
	}

	// There should be at least two partitions, or we won't be able to
	// differentiate between local and remote partitions.
	// This information is encapsulated in the PrefixSorter. If a PrefixSorter was
	// not created for this index, then either all partitions are local, all
	// are remote, or the index is not partitioned.
	tabMeta := c.e.mem.Metadata().TableMeta(scanPrivate.Table)
	index := tabMeta.Table.Index(scanPrivate.Index)
	if _, ok := tabMeta.IndexPartitionLocality(scanPrivate.Index, index, c.e.evalCtx); !ok {
		return false
	}
	return true
}

// GenerateLocalityOptimizedScan generates a locality optimized scan if possible
// from the given scan private. This function should only be called if
// CanMaybeGenerateLocalityOptimizedScan returns true. See the comment above the
// GenerateLocalityOptimizedScan rule for more details.
func (c *CustomFuncs) GenerateLocalityOptimizedScan(
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate,
) {
	// We can only generate a locality optimized scan if we know there is a hard
	// upper bound on the number of rows produced by the local spans. We use the
	// kv batch size as the limit, since it's probably better to use DistSQL once
	// we're scanning multiple batches.
	// TODO(rytaft): Revisit this when we have a more accurate cost model for data
	// distribution.
	maxRows := rowinfra.KeyLimit(grp.Relational().Cardinality.Max)
	if maxRows > rowinfra.ProductionKVBatchSize {
		return
	}

	tabMeta := c.e.mem.Metadata().TableMeta(scanPrivate.Table)
	index := tabMeta.Table.Index(scanPrivate.Index)

	// The PrefixSorter has collected all the prefixes from all the different
	// partitions (remembering which ones came from local partitions), and has
	// sorted them so that longer prefixes come before shorter prefixes. For each
	// span in the scanConstraint, we will iterate through the list of prefixes
	// until we find a match, so ordering them with longer prefixes first ensures
	// that the correct match is found. The PrefixSorter is only non-nil when this
	// index has at least one local and one remote partition.
	var ps *partition.PrefixSorter
	var ok bool
	if ps, ok = tabMeta.IndexPartitionLocality(scanPrivate.Index, index, c.e.evalCtx); !ok {
		return
	}

	// If the Scan has no Constraint, retrieve the constraint of the form
	// 'part_col IN (<part_1>, <part_2> ... <part_n>)' plus an expression
	// representing any gaps between defined partitions (if any), all combined
	// in a single constraint.
	// It is expected that this constraint covers all rows in the table, so it is
	// equivalent to a nil Constraint.
	idxConstraint := scanPrivate.Constraint
	if idxConstraint == nil {
		if idxConstraint, ok = c.buildAllPartitionsConstraint(tabMeta, index, ps, scanPrivate); !ok {
			return
		}
	}
	localSpans := c.getLocalSpans(idxConstraint, ps)
	if localSpans.Len() == 0 || localSpans.Len() == idxConstraint.Spans.Count() {
		// The spans target all local or all remote partitions.
		return
	}

	// Split the spans into local and remote sets.
	localConstraint, remoteConstraint := c.splitSpans(idxConstraint, localSpans)

	// Create the local scan.
	localScanPrivate := c.DuplicateScanPrivate(scanPrivate)
	localScanPrivate.LocalityOptimized = true
	localConstraint.Columns = localConstraint.Columns.RemapColumns(scanPrivate.Table, localScanPrivate.Table)
	localScanPrivate.SetConstraint(c.e.evalCtx, &localConstraint)
	localScanPrivate.HardLimit = scanPrivate.HardLimit
	localScan := c.e.f.ConstructScan(localScanPrivate)

	// Create the remote scan.
	remoteScanPrivate := c.DuplicateScanPrivate(scanPrivate)
	remoteScanPrivate.LocalityOptimized = true
	remoteConstraint.Columns = remoteConstraint.Columns.RemapColumns(scanPrivate.Table, remoteScanPrivate.Table)
	remoteScanPrivate.SetConstraint(c.e.evalCtx, &remoteConstraint)
	remoteScanPrivate.HardLimit = scanPrivate.HardLimit
	remoteScan := c.e.f.ConstructScan(remoteScanPrivate)

	// Add the LocalityOptimizedSearchExpr to the same group as the original scan.
	locOptSearch := memo.LocalityOptimizedSearchExpr{
		Local:  localScan,
		Remote: remoteScan,
		SetPrivate: memo.SetPrivate{
			LeftCols:  localScan.Relational().OutputCols.ToList(),
			RightCols: remoteScan.Relational().OutputCols.ToList(),
			OutCols:   grp.Relational().OutputCols.ToList(),
		},
	}
	c.e.mem.AddLocalityOptimizedSearchToGroup(&locOptSearch, grp)
}

// buildAllPartitionsConstraint retrieves the partition filters and in between
// filters for the "index" belonging to the table described by "tabMeta", and
// builds the full set of spans covering both defined partitions and rows
// belonging to no defined partition (or partitions defined as DEFAULT). If a
// Constraint fails to be built or if the Constraint is unconstrained, this
// function returns (nil, false).
// Partition spans that are 100% local will not be merged with other spans. Note
// that if the partitioning columns have no CHECK constraint defined, suboptimal
// spans may be produced which don't maximize the number of rows accessed as a
// 100% local operation.
// For example:
//    CREATE TABLE abc_part (
//       r STRING NOT NULL ,
//       t INT NOT NULL,
//       a INT PRIMARY KEY,
//       b INT,
//       c INT,
//       d INT,
//       UNIQUE INDEX c_idx (r, t, c) PARTITION BY LIST (r, t) (
//         PARTITION west VALUES IN (('west', 1), ('east', 4)),
//         PARTITION east VALUES IN (('east', DEFAULT), ('east', 2)),
//         PARTITION default VALUES IN (DEFAULT)
//       )
//    );
//    ALTER PARTITION "east" OF INDEX abc_part@c_idx CONFIGURE ZONE USING
//     num_voters = 5,
//     voter_constraints = '{+region=east: 2}',
//     lease_preferences = '[[+region=east]]'
//
//    ALTER PARTITION "west" OF INDEX abc_part@c_idx CONFIGURE ZONE USING
//     num_voters = 5,
//     voter_constraints = '{+region=west: 2}',
//     lease_preferences = '[[+region=west]]'
//
//    ALTER PARTITION "default" OF INDEX abc_part@c_idx CONFIGURE ZONE USING
//     num_voters = 5,
//     lease_preferences = '[[+region=central]]';
//
//    EXPLAIN SELECT c FROM abc_part@c_idx LIMIT 3;
//                         info
//    ----------------------------------------------
//      distribution: local
//      vectorized: true
//
//      • union all
//      │ limit: 3
//      │
//      ├── • scan
//      │     missing stats
//      │     table: abc_part@c_idx
//      │     spans: [/'east'/2 - /'east'/3]
//      │     limit: 3
//      │
//      └── • scan
//            missing stats
//            table: abc_part@c_idx
//            spans: [ - /'east'/1] [/'east'/4 - ]
//            limit: 3
//
// Because of the partial-default east partition, ('east', DEFAULT), the spans
// in the local (left) branch of the union all should be
// [/'east' - /'east'/3] [/'east'/5 - /'east']. Adding in the following check
// constraint achieves this: CHECK (r IN ('east', 'west', 'central'))
func (c *CustomFuncs) buildAllPartitionsConstraint(
	tabMeta *opt.TableMeta, index cat.Index, ps *partition.PrefixSorter, sp *memo.ScanPrivate,
) (*constraint.Constraint, bool) {
	var ok bool
	var remainingFilters memo.FiltersExpr
	var combinedConstraint *constraint.Constraint

	// CHECK constraint and computed column filters
	optionalFilters, filterColumns :=
		c.GetOptionalFiltersAndFilterColumns(nil /* explicitFilters */, sp)

	if _, remainingFilters, combinedConstraint, ok = c.MakeCombinedFiltersConstraint(
		tabMeta, index, sp, ps,
		nil /* explicitFilters */, optionalFilters, filterColumns,
	); !ok {
		return nil, false
	}

	// All partitionFilters are expected to build constraints. If they don't,
	// let's not hide the problem by still generating a locality-optimized search
	// that doesn't fully cover local spans.
	if remainingFilters != nil && len(remainingFilters) > 0 {
		return nil, false
	}

	return combinedConstraint, true
}

// getLocalSpans returns the indexes of the spans from the given constraint that
// target local partitions.
func (c *CustomFuncs) getLocalSpans(
	scanConstraint *constraint.Constraint, ps *partition.PrefixSorter,
) util.FastIntSet {
	// Iterate through the spans and determine whether each one matches
	// with a prefix from a local partition.
	var localSpans util.FastIntSet
	for i, n := 0, scanConstraint.Spans.Count(); i < n; i++ {
		span := scanConstraint.Spans.Get(i)
		if match, ok := constraint.FindMatch(span, ps); ok {
			if match.IsLocal {
				localSpans.Add(i)
			}
		}
	}
	return localSpans
}

// splitSpans splits the original constraint into a local and remote constraint
// by putting the spans at positions identified by localSpanOrds into the local
// constraint, and the remaining spans into the remote constraint.
func (c *CustomFuncs) splitSpans(
	origConstraint *constraint.Constraint, localSpanOrds util.FastIntSet,
) (localConstraint, remoteConstraint constraint.Constraint) {
	allSpansCount := origConstraint.Spans.Count()
	localSpansCount := localSpanOrds.Len()
	var localSpans, remoteSpans constraint.Spans
	localSpans.Alloc(localSpansCount)
	remoteSpans.Alloc(allSpansCount - localSpansCount)
	for i := 0; i < allSpansCount; i++ {
		span := origConstraint.Spans.Get(i)
		if localSpanOrds.Contains(i) {
			localSpans.Append(span)
		} else {
			remoteSpans.Append(span)
		}
	}
	keyCtx := constraint.MakeKeyContext(&origConstraint.Columns, c.e.evalCtx)
	localConstraint.Init(&keyCtx, &localSpans)
	remoteConstraint.Init(&keyCtx, &remoteSpans)
	return localConstraint, remoteConstraint
}
