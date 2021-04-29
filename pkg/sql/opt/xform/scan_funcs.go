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
//       rows from the table. See ConstrainScans and LimitScans for cases where
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

const regionKey = "region"

// CanMaybeGenerateLocalityOptimizedScan returns true if it may be possible to
// generate a locality optimized scan from the given scan private.
// CanMaybeGenerateLocalityOptimizedScan performs simple checks that are
// inexpensive to execute and can filter out cases where the optimization
// definitely cannot apply. See the comment above the
// GenerateLocalityOptimizedScan rule for details.
func (c *CustomFuncs) CanMaybeGenerateLocalityOptimizedScan(scanPrivate *memo.ScanPrivate) bool {
	// Respect the session setting LocalityOptimizedSearch.
	if !c.e.evalCtx.SessionData.LocalityOptimizedSearch {
		return false
	}

	if scanPrivate.LocalityOptimized {
		// This scan has already been locality optimized.
		return false
	}

	if scanPrivate.HardLimit != 0 {
		// This optimization doesn't apply to limited scans.
		return false
	}

	// This scan should have at least two spans, or we won't be able to move one
	// of the spans to a separate remote scan.
	if scanPrivate.Constraint == nil || scanPrivate.Constraint.Spans.Count() < 2 {
		return false
	}

	// Don't apply the rule if there are too many spans, since the rule code is
	// O(# spans * # prefixes * # datums per prefix).
	if scanPrivate.Constraint.Spans.Count() > 10000 {
		return false
	}

	// There should be at least two partitions, or we won't be able to
	// differentiate between local and remote partitions.
	tabMeta := c.e.mem.Metadata().TableMeta(scanPrivate.Table)
	index := tabMeta.Table.Index(scanPrivate.Index)
	if index.PartitionCount() < 2 {
		return false
	}

	// The local region must be set, or we won't be able to determine which
	// partitions are local.
	_, found := c.e.evalCtx.Locality.Find(regionKey)
	return found
}

// GenerateLocalityOptimizedScan generates a locality optimized scan if possible
// from the given scan private. This function should only be called if
// CanMaybeGenerateLocalityOptimizedScan returns true. See the comment above the
// GenerateLocalityOptimizedScan rule for more details.
func (c *CustomFuncs) GenerateLocalityOptimizedScan(
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate,
) {
	// We can only generate a locality optimized scan if we know there is at
	// most one row produced by the local spans.
	// TODO(rytaft): We may be able to expand this to allow any number of rows,
	// as long as there is a hard upper bound.
	if !grp.Relational().Cardinality.IsZeroOrOne() {
		return
	}

	tabMeta := c.e.mem.Metadata().TableMeta(scanPrivate.Table)
	index := tabMeta.Table.Index(scanPrivate.Index)

	// We already know that a local region exists from calling
	// CanMaybeGenerateLocalityOptimizedScan.
	localRegion, _ := c.e.evalCtx.Locality.Find(regionKey)

	// Determine whether the index has both local and remote partitions, and
	// if so, which spans target local partitions.
	var localPartitions util.FastIntSet
	for i, n := 0, index.PartitionCount(); i < n; i++ {
		part := index.Partition(i)
		if isZoneLocal(part.Zone(), localRegion) {
			localPartitions.Add(i)
		}
	}
	if localPartitions.Len() == 0 || localPartitions.Len() == index.PartitionCount() {
		// The partitions are either all local or all remote.
		return
	}

	localSpans := c.getLocalSpans(index, localPartitions, scanPrivate.Constraint)
	if localSpans.Len() == 0 || localSpans.Len() == scanPrivate.Constraint.Spans.Count() {
		// The spans target all local or all remote partitions.
		return
	}

	// Split the spans into local and remote sets.
	localConstraint, remoteConstraint := c.splitSpans(scanPrivate.Constraint, localSpans)

	// Create the local scan.
	localScanPrivate := c.DuplicateScanPrivate(scanPrivate)
	localScanPrivate.LocalityOptimized = true
	localConstraint.Columns = localConstraint.Columns.RemapColumns(scanPrivate.Table, localScanPrivate.Table)
	localScanPrivate.SetConstraint(c.e.evalCtx, &localConstraint)
	localScan := c.e.f.ConstructScan(localScanPrivate)

	// Create the remote scan.
	remoteScanPrivate := c.DuplicateScanPrivate(scanPrivate)
	remoteScanPrivate.LocalityOptimized = true
	remoteConstraint.Columns = remoteConstraint.Columns.RemapColumns(scanPrivate.Table, remoteScanPrivate.Table)
	remoteScanPrivate.SetConstraint(c.e.evalCtx, &remoteConstraint)
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

// getLocalSpans returns the indexes of the spans from the given constraint that
// target local partitions.
func (c *CustomFuncs) getLocalSpans(
	index cat.Index, localPartitions util.FastIntSet, constraint *constraint.Constraint,
) util.FastIntSet {
	// Collect all the prefixes from all the different partitions (remembering
	// which ones came from local partitions), and sort them so that longer
	// prefixes come before shorter prefixes. For each span in the constraint, we
	// will iterate through the list of prefixes until we find a match, so
	// ordering them with longer prefixes first ensures that the correct match is
	// found.
	allPrefixes := getSortedPrefixes(index, localPartitions)

	// Now iterate through the spans and determine whether each one matches
	// with a prefix from a local partition.
	// TODO(rytaft): Sort the prefixes by key in addition to length, and use
	// binary search here.
	var localSpans util.FastIntSet
	for i, n := 0, constraint.Spans.Count(); i < n; i++ {
		span := constraint.Spans.Get(i)
		spanPrefix := span.Prefix(c.e.evalCtx)
		for j := range allPrefixes {
			prefix := allPrefixes[j].prefix
			isLocal := allPrefixes[j].isLocal
			if len(prefix) > spanPrefix {
				continue
			}
			matches := true
			for k, datum := range prefix {
				if span.StartKey().Value(k).Compare(c.e.evalCtx, datum) != 0 {
					matches = false
					break
				}
			}
			if matches {
				if isLocal {
					localSpans.Add(i)
				}
				break
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
