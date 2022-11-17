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
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/distribution"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partition"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
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
//
//	case of ForceIndex). Index joins are usually only introduced "one level
//	up", when the Scan operator is wrapped by an operator that constrains
//	or limits scan output in some way (e.g. Select, Limit, InnerJoin).
//	Index joins are only lower cost when their input does not include all
//	rows from the table. See GenerateConstrainedScans,
//	GenerateLimitedScans, and GenerateLimitedGroupByScans for cases where
//	index joins are introduced into the memo.
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
		// AllowUnconstrainedNonCoveringIndexScan allows all index access paths to
		// be explored, even when non-covering and unconstrained, without forcing a
		// particular index.
		if !scanPrivate.Flags.ForceIndex && !c.e.mem.AllowUnconstrainedNonCoveringIndexScan() {
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
	// This information is encapsulated in the PrefixSorter. If a non-empty
	// PrefixSorter was not created for this index, then either all partitions
	// are local, all are remote, or the index is not partitioned.
	tabMeta := c.e.mem.Metadata().TableMeta(scanPrivate.Table)
	if ps := tabMeta.IndexPartitionLocality(scanPrivate.Index); ps.Empty() {
		return false
	}
	return true
}

// LocalityOptimizedScanBelowMaxCardinality returns true if the cardinality of
// `relExpr` is above the upper limit allowed for generation of a
// locality-optimized scan.
func (c *CustomFuncs) LocalityOptimizedScanAboveMaxCardinality(relExpr memo.RelExpr) bool {
	// We can only generate a locality optimized scan if we know there is a hard
	// upper bound on the number of rows produced by the local spans. We use the
	// kv batch size as the limit, since it's probably better to use DistSQL once
	// we're scanning multiple batches.
	// TODO(rytaft): Revisit this when we have a more accurate cost model for data
	// distribution.
	maxRows := rowinfra.KeyLimit(relExpr.Relational().Cardinality.Max)
	return maxRows > rowinfra.ProductionKVBatchSize
}

// GenerateLocalityOptimizedScan generates a locality optimized scan if possible
// from the given scan private. This function should only be called if
// CanMaybeGenerateLocalityOptimizedScan returns true. See the comment above the
// GenerateLocalityOptimizedScan rule for more details.
func (c *CustomFuncs) GenerateLocalityOptimizedScan(
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate,
) {
	if c.LocalityOptimizedScanAboveMaxCardinality(grp) {
		return
	}

	tabMeta := c.e.mem.Metadata().TableMeta(scanPrivate.Table)
	index := tabMeta.Table.Index(scanPrivate.Index)

	// The PrefixSorter has collected all the prefixes from all the different
	// partitions (remembering which ones came from local partitions), and has
	// sorted them so that longer prefixes come before shorter prefixes. For
	// each span in the scanConstraint, we will iterate through the list of
	// prefixes until we find a match, so ordering them with longer prefixes
	// first ensures that the correct match is found. The PrefixSorter is only
	// non-empty when this index has at least one local and one remote
	// partition.
	ps := tabMeta.IndexPartitionLocality(scanPrivate.Index)
	if ps.Empty() {
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
		var ok bool
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
	if scanPrivate.InvertedConstraint != nil {
		localScanPrivate.InvertedConstraint = make(inverted.Spans, len(scanPrivate.InvertedConstraint))
		copy(localScanPrivate.InvertedConstraint, scanPrivate.InvertedConstraint)
	}
	localScan := c.e.f.ConstructScan(localScanPrivate)
	if scanPrivate.HardLimit != 0 {
		// If the local scan could never reach the hard limit, we will always have
		// to read into remote regions, so there is no point in using
		// locality-optimized scan.
		if scanPrivate.HardLimit > memo.ScanLimit(localScan.Relational().Cardinality.Max) {
			return
		}
	} else {
		// When the max cardinality of the original scan is greater than the max
		// cardinality of the local scan, a remote scan will always be required.
		// IgnoreUniqueWithoutIndexKeys is true when we're performing a scan
		// during an insert to verify there are no duplicates violating the
		// uniqueness constraint. This could cause the check below to return, but
		// by design we want to use locality-optimized search for these duplicate
		// checks. So avoid returning if that flag is set.
		if localScan.Relational().Cardinality.Max <
			grp.Relational().Cardinality.Max && !tabMeta.IgnoreUniqueWithoutIndexKeys {
			return
		}
	}

	// Create the remote scan.
	remoteScanPrivate := c.DuplicateScanPrivate(scanPrivate)
	remoteScanPrivate.LocalityOptimized = true
	remoteConstraint.Columns = remoteConstraint.Columns.RemapColumns(scanPrivate.Table, remoteScanPrivate.Table)
	remoteScanPrivate.SetConstraint(c.e.evalCtx, &remoteConstraint)
	remoteScanPrivate.HardLimit = scanPrivate.HardLimit
	if scanPrivate.InvertedConstraint != nil {
		remoteScanPrivate.InvertedConstraint = make(inverted.Spans, len(scanPrivate.InvertedConstraint))
		copy(remoteScanPrivate.InvertedConstraint, scanPrivate.InvertedConstraint)
	}
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

// makeColMap builds a map used to remap column IDs that are in the OutputCols
// of `src` to refer to corresponding column IDs in `dst`. `src` and `dst` must
// refer to the same underlying table.
func (c *CustomFuncs) makeColMap(src, dst *memo.ScanPrivate) (colMap opt.ColMap) {
	for srcCol, ok := src.Cols.Next(0); ok; srcCol, ok = src.Cols.Next(srcCol + 1) {
		ord := src.Table.ColumnOrdinal(srcCol)
		dstCol := dst.Table.ColumnID(ord)
		colMap.Set(int(srcCol), int(dstCol))
	}
	return colMap
}

// getLocalAndRemoteFilters returns the filters on the `crdb_region` column
// which target only local partitions, and those which target only remote
// partitions. It is expected that `firstIndexCol` is the ColumnID of the
// `crdb_region` column.
func (c *CustomFuncs) getLocalAndRemoteFilters(
	filters memo.FiltersExpr, ps partition.PrefixSorter, firstIndexCol opt.ColumnID,
) (localFilters opt.ScalarExpr, remoteFilters opt.ScalarExpr, ok bool) {
	var localFiltersItem, remoteFiltersItem memo.FiltersItem
	for _, filter := range filters {
		constraints := filter.ScalarProps().Constraints
		if constraints == nil || constraints.Length() == 0 {
			continue
		}
		if constraints.Length() != 1 || !filter.ScalarProps().TightConstraints {
			continue
		}
		if constraints.Constraint(0).IsContradiction() ||
			constraints.Constraint(0).IsUnconstrained() {
			continue
		}
		if constraints.Constraint(0).Columns.Get(0).ID() != firstIndexCol {
			continue
		}
		numSpans := constraints.Constraint(0).Spans.Count()
		if numSpans < 1 {
			continue
		}
		// We expect there to be a single local region.
		localRegions := make(tree.Datums, 0, 1)
		// We expect all other IN list items to be remote regions.
		remoteRegions := make(tree.Datums, 0, numSpans)

		for i := 0; i < numSpans; i++ {
			val := constraints.Constraint(0).Spans.Get(i).StartKey().Value(0)
			match, ok := constraint.FindMatchOnSingleColumn(val, ps)
			if ok && match.IsLocal {
				// Found a match and it is local.
				localRegions = append(localRegions, val)
			} else {
				// Either found a match and it is not local, or we didn't find a match,
				// in which case we categorize it as non-local.
				remoteRegions = append(remoteRegions, val)
			}
		}
		if len(localRegions) > 0 || len(remoteRegions) > 0 {
			if len(localRegions) > 0 {
				// Found a match and it is local.
				localFiltersItem = c.e.f.ConstructConstFilter(firstIndexCol, localRegions)
				localFilters = &localFiltersItem
			}
			if len(remoteRegions) > 0 {
				remoteFiltersItem = c.e.f.ConstructConstFilter(firstIndexCol, remoteRegions)
				remoteFilters = &remoteFiltersItem
			}
			// localFilters is an equality expression matching the local region
			// (should be only one) and remoteFilters is an IN list with values which
			// do not match the local region. For example:
			// localFilters:  crdb_region = 'us-west1'
			// remoteFilters: crdb_region IN ('us-east1', 'eu-west1', 'ap-southeast1')
			return localFilters, remoteFilters, true
		}
	}
	return nil, nil, false
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
func (c *CustomFuncs) LookupsAreLocal(lookupJoinExpr *memo.LookupJoinExpr) bool {
	provided := distribution.BuildLookupJoinLookupTableDistribution(c.e.ctx, c.e.f.EvalContext(), lookupJoinExpr)
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
	lookupJoinExpr *memo.LookupJoinExpr,
	inputScan *memo.ScanExpr,
	inputFilters memo.FiltersExpr,
	localityOptimizedLookupJoinPrivate *memo.LookupJoinPrivate,
) {
	var localSelectFilters, remoteSelectFilters memo.FiltersExpr
	if len(inputFilters) > 0 {
		// Both local and remote branches must evaluate the original filters.
		localSelectFilters = inputFilters
		remoteSelectFilters = inputFilters
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
	lookupJoinExpr *memo.LookupJoinExpr,
	inputScan *memo.ScanExpr,
	inputFilters memo.FiltersExpr,
) {
	c.GenerateLocalityOptimizedSearchOfLookupJoins(grp, lookupJoinExpr, inputScan, inputFilters, nil)
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
	constFilters := c.e.f.RemapCols(&lookupJoinExpr.ConstFilters, colMap).(*memo.FiltersExpr)
	mappedLookupJoinExpr.ConstFilters = *constFilters
	mappedLookupJoinExpr.Locking = lookupJoinExpr.Locking
	return mappedLookupJoinExpr
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
//
//	CREATE TABLE abc_part (
//	   r STRING NOT NULL ,
//	   t INT NOT NULL,
//	   a INT PRIMARY KEY,
//	   b INT,
//	   c INT,
//	   d INT,
//	   UNIQUE INDEX c_idx (r, t, c) PARTITION BY LIST (r, t) (
//	     PARTITION west VALUES IN (('west', 1), ('east', 4)),
//	     PARTITION east VALUES IN (('east', DEFAULT), ('east', 2)),
//	     PARTITION default VALUES IN (DEFAULT)
//	   )
//	);
//	ALTER PARTITION "east" OF INDEX abc_part@c_idx CONFIGURE ZONE USING
//	 num_voters = 5,
//	 voter_constraints = '{+region=east: 2}',
//	 lease_preferences = '[[+region=east]]'
//
//	ALTER PARTITION "west" OF INDEX abc_part@c_idx CONFIGURE ZONE USING
//	 num_voters = 5,
//	 voter_constraints = '{+region=west: 2}',
//	 lease_preferences = '[[+region=west]]'
//
//	ALTER PARTITION "default" OF INDEX abc_part@c_idx CONFIGURE ZONE USING
//	 num_voters = 5,
//	 lease_preferences = '[[+region=central]]';
//
//	EXPLAIN SELECT c FROM abc_part@c_idx LIMIT 3;
//	                     info
//	----------------------------------------------
//	  distribution: local
//	  vectorized: true
//
//	  • union all
//	  │ limit: 3
//	  │
//	  ├── • scan
//	  │     missing stats
//	  │     table: abc_part@c_idx
//	  │     spans: [/'east'/2 - /'east'/3]
//	  │     limit: 3
//	  │
//	  └── • scan
//	        missing stats
//	        table: abc_part@c_idx
//	        spans: [ - /'east'/1] [/'east'/4 - ]
//	        limit: 3
//
// Because of the partial-default east partition, ('east', DEFAULT), the spans
// in the local (left) branch of the union all should be
// [/'east' - /'east'/3] [/'east'/5 - /'east']. Adding in the following check
// constraint achieves this: CHECK (r IN ('east', 'west', 'central'))
func (c *CustomFuncs) buildAllPartitionsConstraint(
	tabMeta *opt.TableMeta, index cat.Index, ps partition.PrefixSorter, sp *memo.ScanPrivate,
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
	//lint:ignore S1009 grandfathered
	if remainingFilters != nil && len(remainingFilters) > 0 {
		return nil, false
	}

	return combinedConstraint, true
}

// getLocalSpans returns the indexes of the spans from the given constraint that
// target local partitions.
func (c *CustomFuncs) getLocalSpans(
	scanConstraint *constraint.Constraint, ps partition.PrefixSorter,
) intsets.Fast {
	// Iterate through the spans and determine whether each one matches
	// with a prefix from a local partition.
	var localSpans intsets.Fast
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
	origConstraint *constraint.Constraint, localSpanOrds intsets.Fast,
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

// ScanPrivateCols returns the ColSet of a ScanPrivate.
func (c *CustomFuncs) ScanPrivateCols(sp *memo.ScanPrivate) opt.ColSet {
	return sp.Cols
}

// IsRegionalByRowTableScanOrSelect returns true if `input` is a scan or select
// from a REGIONAL BY ROW table.
func (c *CustomFuncs) IsRegionalByRowTableScanOrSelect(input memo.RelExpr) bool {
	if selectExpr, ok := input.(*memo.SelectExpr); ok {
		input = selectExpr.Input
	}
	scanExpr, ok := input.(*memo.ScanExpr)
	if !ok {
		return false
	}
	table := scanExpr.Memo().Metadata().Table(scanExpr.Table)
	return table.IsRegionalByRow()
}

// IsSelectFromRemoteTableRowsOnly returns true if `input` is a select from a
// REGIONAL BY TABLE or REGIONAL BY ROW table which only reads rows in a remote
// region without bounded staleness. Bounded staleness would allow local
// replicas to be used for the scan.
func (c *CustomFuncs) IsSelectFromRemoteTableRowsOnly(input memo.RelExpr) bool {
	scanExpr, inputFilters, ok := c.getfilteredCanonicalScan(input)
	if !ok {
		return false
	}
	if !ok {
		return false
	}
	if c.e.evalCtx.BoundedStaleness() {
		return false
	}
	table := scanExpr.Memo().Metadata().Table(scanExpr.Table)
	if table.IsRegionalByRow() {
		tabMeta := c.e.mem.Metadata().TableMeta(scanExpr.Table)
		index := table.Index(scanExpr.Index)
		ps := tabMeta.IndexPartitionLocality(scanExpr.Index)
		if ps.Empty() {
			// Can't tell if anything is local; treat all rows as remote
			return true
		}
		firstIndexCol := scanExpr.ScanPrivate.Table.IndexColumnID(index, 0)
		localFiltersItem, _, ok :=
			c.getLocalAndRemoteFilters(inputFilters, ps, firstIndexCol)
		if !ok {
			// There is no filter on crdb_region, so all regions may be read.
			return false
		}
		if localFiltersItem == nil {
			return true
		}
		return false
	} else if tableHomeRegion, ok := table.HomeRegion(); ok {
		gatewayRegion, foundLocalRegion := c.e.evalCtx.Locality.Find("region")
		if !foundLocalRegion {
			// Found no gateway region, so can't tell if only remote rows are read.
			return false
		}
		return gatewayRegion != tableHomeRegion
	}
	return false
}
