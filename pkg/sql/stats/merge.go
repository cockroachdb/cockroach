// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// MergedStatistics returns merged stats per single-column full stat by taking
// the latest full statistic as a base and applying all newer partial statistics
// for that column in chronological order. Partial statistics that cannot be
// merged with the full statistic are skipped, and if no partials can be merged
// with a full, we don't include that statistic in the result.
func MergedStatistics(
	ctx context.Context, stats []*TableStatistic, st *cluster.Settings,
) []*TableStatistic {
	// Map the ColumnIDs to the latest full table statistic,
	// and map the keys to the number of columns in the set.
	// It relies on the fact that the first full statistic
	// is the latest.
	fullStatsMap := make(map[descpb.ColumnID]*TableStatistic)
	for _, fullStat := range stats {
		if fullStat.IsPartial() || len(fullStat.ColumnIDs) != 1 {
			continue
		}
		col := fullStat.ColumnIDs[0]
		_, ok := fullStatsMap[col]
		if !ok {
			fullStatsMap[col] = fullStat
		}
	}

	mergeStats := func(base, partial *TableStatistic) (*TableStatistic, error) {
		if partial.FullStatisticID != 0 {
			// This is a partial statistic USING EXTREMES.
			return mergeExtremesStatistic(ctx, base, partial, st)
		}
		// This is a partial statistic with WHERE.
		return mergePartialStatistic(ctx, base, partial, st)
	}

	mergedStatsMap := make(map[descpb.ColumnID]*TableStatistic)
	// Iterate through the stats in reverse order so that we merge the latest
	// partial statistics last.
	for i := len(stats) - 1; i >= 0; i-- {
		partialStat := stats[i]
		if !partialStat.IsPartial() || len(partialStat.ColumnIDs) != 1 {
			continue
		}
		col := partialStat.ColumnIDs[0]
		if fullStat, ok := fullStatsMap[col]; ok && partialStat.CreatedAt.After(fullStat.CreatedAt) {
			baseStat := fullStat
			if mStat, ok := mergedStatsMap[col]; ok {
				baseStat = mStat
			}

			merged, err := mergeStats(baseStat, partialStat)
			if err != nil {
				log.VEventf(ctx, 2, "could not merge statistics for table %v columns %s: %v",
					fullStat.TableID, redact.Safe(col), err)
				continue
			}
			mergedStatsMap[col] = merged
		}
	}

	colIDs := make([]descpb.ColumnID, 0, len(mergedStatsMap))
	for colID := range mergedStatsMap {
		colIDs = append(colIDs, colID)
	}
	sort.Slice(colIDs, func(i, j int) bool { return colIDs[i] < colIDs[j] })

	mergedStats := make([]*TableStatistic, 0, len(colIDs))
	for _, c := range colIDs {
		mergedStats = append(mergedStats, mergedStatsMap[c])
	}
	return mergedStats
}

// stripOuterBuckets removes the outer buckets from a histogram without a
// leading NULL bucket.
func stripOuterBuckets(
	ctx context.Context, evalCtx *eval.Context, histogram []cat.HistogramBucket,
) []cat.HistogramBucket {
	if len(histogram) == 0 {
		return histogram
	}
	startIdx := 0
	endIdx := len(histogram)
	if histogram[0].UpperBound.IsMin(ctx, evalCtx) && histogram[0].NumEq == 0 {
		startIdx = 1
		// Set the first range counts to zero to counteract range counts added by
		// addOuterBuckets.
		histogram[startIdx].NumRange = 0
		histogram[startIdx].DistinctRange = 0
	}
	if histogram[len(histogram)-1].UpperBound.IsMax(ctx, evalCtx) && histogram[len(histogram)-1].NumEq == 0 {
		endIdx = len(histogram) - 1
	}
	return histogram[startIdx:endIdx]
}

// mergeExtremesStatistic merges a full table statistic with a partial table
// statistic and returns a new full table statistic. It does this by prepending
// the partial histogram buckets with UpperBound less than the first bucket of
// the full histogram and appending the partial histogram buckets with
// UpperBounds that are greater than the last full histogram bucket to the end
// of the full histogram. It then recalculates the counts for the new merged
// statistic, using the new combined histogram. The createdAt time is set to
// that of the latest partial statistic, and the statistic is named the string
// assigned to jobspb.MergedStatsName.
//
// For example, consider this case:
// Full Statistic: {row: 7, dist: 4, null: 4, size: 1}
// CreatedAt: 2022-01-02
// Histogram (format is: {NumEq, NumRange, DistinctRange, UpperBound}):
// [{1, 0, 0, 2}, {1, 0, 0, 3}, {1, 0, 0, 4}]
//
// Partial Statistic: {row: 8, dist: 4, null: 0, size: 1}
// CreatedAt: 2022-01-03
// Histogram: [{2, 0, 0, 0}, {2, 0, 0, 1}, {2, 0, 0, 5}, {2, 0, 0, 6}]
//
// Merged Statistic: {row: 15, dist: 7, null: 4, size: 1}
// CreatedAt: 2022-01-03
// Histogram: [{2, 0, 0, 0}, {2, 0, 0, 1}, {1, 0, 0, 2}, {1, 0, 0, 3},
// {1, 0, 0, 4}, {2, 0, 0, 5}, {2, 0, 0, 6}]
//
// Alternatively, consider this case where the new values are added at
// the upper extreme of the index:
// Full Statistic: {row: 12, dist: 6, null: 0, size: 3}
// CreatedAt: 2021-02-01
// Histogram: [{3, 3, 2, 8}, {3, 3, 2, 15}]
//
// Partial Statistic: {row: 18, dist: 11, null: 0, size: 8}
// CreatedAt: 2022-02-02
// Histogram: [{4, 4, 4, 19}, {5, 5, 5, 21}]
//
// Merged Statistic: {row: 30, dist: 17, null: 0, size: 4}
// CreatedAt: 2022-02-03
// Histogram: [{3, 3, 2, 8}, {3, 3, 2, 15}, {4, 4, 4, 19}, {5, 5, 5, 21}]
//
// In the case where there is no partial histogram, the full statistic
// is returned but with the created_at time of the partial statistic,
// with the statistic renamed to the string assigned to.
// jobspb.MergedStatsName.
func mergeExtremesStatistic(
	ctx context.Context, fullStat *TableStatistic, partialStat *TableStatistic, st *cluster.Settings,
) (*TableStatistic, error) {
	fullStatColKey := MakeSortedColStatKey(fullStat.ColumnIDs)
	partialStatColKey := MakeSortedColStatKey(partialStat.ColumnIDs)
	if fullStatColKey != partialStatColKey {
		return fullStat, errors.AssertionFailedf("column sets for full table statistics and partial table statistics column sets do not match")
	}

	// Merge the histograms
	// Currently, since we don't merge multi-column statistics, each
	// statistic passed through should have a histogram.
	// TODO (faizaanmadhani): Add support for multi-column partial statistics.
	fullHistogram := fullStat.Histogram
	partialHistogram := partialStat.Histogram

	if len(fullHistogram) == 0 {
		return nil, errors.New("the full statistic histogram does not exist")
	}

	if partialStat.FullStatisticID != fullStat.StatisticID {
		return nil, errors.New("partial statistic not derived from latest full statistic")
	}

	// If the partial histogram was empty this means that there
	// were no new values at the extremes. We update the
	// createdAt time and rename it.
	if len(partialHistogram) == 0 {
		mergedStat := *fullStat
		mergedStat.Name = jobspb.MergedStatsName
		mergedStat.CreatedAt = partialStat.CreatedAt
		return &mergedStat, nil
	}

	mergedHistogram := make([]cat.HistogramBucket, 0, len(fullHistogram)+len(partialHistogram))

	fullHistogram = fullStat.nonNullHistogram().buckets
	partialHistogram = partialStat.nonNullHistogram().buckets

	var cmpCtx *eval.Context

	// Remove the outer buckets from the ends of the histograms if they exist.
	// This is done to avoid overlapping buckets when merging the histograms.
	fullHistogram = stripOuterBuckets(ctx, cmpCtx, fullHistogram)
	partialHistogram = stripOuterBuckets(ctx, cmpCtx, partialHistogram)

	i := 0
	// Merge partial stats to prior full statistics.
	for i < len(partialHistogram) {
		if val, err := partialHistogram[i].UpperBound.Compare(ctx, cmpCtx, fullHistogram[0].UpperBound); err == nil {
			if val == 0 {
				return nil, errors.New("the lowerbound of the full statistic histogram overlaps with the partial statistic histogram")
			}
			if val == -1 {
				mergedHistogram = append(mergedHistogram, partialHistogram[i])
				i++
			} else {
				break
			}
		} else {
			return nil, err
		}
	}

	// Iterate through the rest of the full histogram and append it.
	for _, fullHistBucket := range fullHistogram {
		if i < len(partialHistogram) {
			if val, err := partialHistogram[i].UpperBound.Compare(ctx, cmpCtx, fullHistBucket.UpperBound); err == nil {
				if val <= 0 {
					return nil, errors.New("the upperbound of the full statistic histogram overlaps with the partial statistic histogram")
				}
			} else {
				return nil, err
			}
		}
		mergedHistogram = append(mergedHistogram, fullHistBucket)
	}

	// iterate through the remaining partial histogram and append it.
	for i < len(partialHistogram) {
		mergedHistogram = append(mergedHistogram, partialHistogram[i])
		i++
	}

	// Since partial statistics at the extremes will always scan over
	// the NULL rows at the lowerbound, we don't include the NULL count
	// of the full statistic.
	mergedRowCount := (fullStat.RowCount - fullStat.NullCount) + (partialStat.RowCount)
	mergedDistinctCount := fullStat.DistinctCount + partialStat.DistinctCount
	// Avoid double counting the NULL distinct value.
	if fullStat.NullCount > 0 {
		mergedDistinctCount -= 1
	}
	mergedNullCount := partialStat.NullCount

	mergedNonNullRowCount := mergedRowCount - mergedNullCount
	mergedNonNullDistinctCount := mergedDistinctCount
	if mergedNullCount > 0 {
		mergedNonNullDistinctCount--
	}

	mergedAvgSize := (partialStat.AvgSize*partialStat.RowCount + fullStat.AvgSize*fullStat.RowCount) / mergedRowCount

	mergedTableStatistic := &TableStatistic{
		TableStatisticProto: TableStatisticProto{
			TableID:         fullStat.TableID,
			StatisticID:     0, // TODO (faizaanmadhani): Add support for SHOW HISTOGRAM.
			Name:            jobspb.MergedStatsName,
			ColumnIDs:       fullStat.ColumnIDs,
			CreatedAt:       partialStat.CreatedAt,
			RowCount:        mergedRowCount,
			DistinctCount:   mergedDistinctCount,
			NullCount:       mergedNullCount,
			AvgSize:         mergedAvgSize,
			FullStatisticID: 0,
		},
	}

	hist := histogram{
		buckets: mergedHistogram,
	}
	hist.adjustCounts(ctx, cmpCtx, fullStat.HistogramData.ColumnType, float64(mergedNonNullRowCount), float64(mergedNonNullDistinctCount))
	histData, err := hist.toHistogramData(ctx, fullStat.HistogramData.ColumnType, st)
	if err != nil {
		return nil, err
	}
	mergedTableStatistic.HistogramData = &histData
	mergedTableStatistic.setHistogramBuckets(hist)

	return mergedTableStatistic, nil
}

// mergePartialStatistic merges a full table statistic with a partial table
// statistic and returns a new full table statistic. It does this by pouring?
// counts from the partial stat’s buckets into overlapping buckets in the full
// stat, and creating new buckets where full stat buckets don’t exist. This
// function assumes that the partial statistic is a single continuous histogram
// without gaps.
//
// The approach is similar to a merge join, where we iterate over both
// sorted histograms. We start by creating new buckets for each partial stat
// bucket that comes before the first full stat bucket. Next, we merge each full
// stat bucket by overwriting the parts of each bucket that overlap with a
// partial stat bucket, assuming that values are uniformly distributed across
// the bucket. Finally, we create new buckets for each partial stat bucket that
// comes after the last full stat bucket.
//
// This example illustrates the merging process for overlapping buckets, the
// histogram format is {NumEq, NumRange, DistinctRange, UpperBound}:
//
// Full stat histogram:    [{1, 0, 0, 10},                {1, 4, 2, 20}]
// Partial stat histogram: [{2, 0, 0, 10}, {1, 3, 3, 15}]
// Merged histogram:       [{2, 0, 0, 10},                {1, 6, 5, 20}]
//
// The first partial stat bucket completely overlaps with the first full stat
// bucket, so we overwrite its NumEq count with that of the partial bucket. The
// second partial stat bucket overlaps with half of the second full stat
// bucket's range, so we assume that values are uniformly distribution within
// buckets and combine half of the full stat's NumRange and DistinctRange with
// that of the partial stat bucket. The full stat bucket's NumEq is unchanged
// since the partial stat bucket does not overlap with its upper bound.
func mergePartialStatistic(
	ctx context.Context, fullStat *TableStatistic, partialStat *TableStatistic, st *cluster.Settings,
) (*TableStatistic, error) {
	fullStatColKey := MakeSortedColStatKey(fullStat.ColumnIDs)
	partialStatColKey := MakeSortedColStatKey(partialStat.ColumnIDs)
	if fullStatColKey != partialStatColKey {
		return fullStat, errors.AssertionFailedf("column sets for full table statistics and partial table statistics column sets do not match")
	}

	fullHistogram := fullStat.Histogram
	partialHistogram := partialStat.Histogram

	if len(fullHistogram) == 0 {
		return nil, errors.New("the full statistic histogram does not exist")
	}

	// An empty partial histogram means that there were no values in the scanned
	// spans. We can do better here by subtracting from the full histogram based
	// on the stat predicate, but return the full statistic for simplicity since
	// overestimates are generally safe.
	if len(partialHistogram) == 0 {
		mergedStat := *fullStat
		mergedStat.Name = jobspb.MergedStatsName
		mergedStat.CreatedAt = partialStat.CreatedAt
		return &mergedStat, nil
	}

	mergedHistogram := make([]cat.HistogramBucket, 0, len(fullHistogram)+len(partialHistogram))

	fullHistogram = fullStat.nonNullHistogram().buckets
	partialHistogram = partialStat.nonNullHistogram().buckets

	var cmpCtx *eval.Context

	// Remove the outer buckets from the ends of the histograms if they exist.
	// This is done to avoid overlapping buckets when merging the histograms.
	fullHistogram = stripOuterBuckets(ctx, cmpCtx, fullHistogram)
	partialHistogram = stripOuterBuckets(ctx, cmpCtx, partialHistogram)

	var pHist, fHist props.Histogram
	colID := opt.ColumnID(fullStat.ColumnIDs[0])
	pHist.Init(cmpCtx, colID, partialHistogram)
	fHist.Init(cmpCtx, colID, fullHistogram)

	var pIter, fIter props.HistogramIter
	pIter.Init(&pHist, false)
	fIter.Init(&fHist, false)

	var cols constraint.Columns
	cols.InitSingle(opt.MakeOrderingColumn(colID, false))
	keyCtx := constraint.KeyContext{Ctx: ctx, EvalCtx: cmpCtx, Columns: cols}

	var fullSb, partialSb, overlappingSb props.SpanBuilder
	var prefix []tree.Datum
	fullSb.Init(prefix)
	partialSb.Init(prefix)
	overlappingSb.Init(prefix)

	fullStatMin := fullHistogram[0].UpperBound
	fullStatMax := fullHistogram[len(fullHistogram)-1].UpperBound

	// Step 1: Emit partial stat buckets before the first full stat bucket.
	// Example:
	//   Full:                |-------|--...
	//   Partial:  |--|---|-------|------...
	//             ^^^^^^^^^^^^
	var beforeFullSpan constraint.Span
	beforeFullSpan.Init(
		constraint.EmptyKey, constraint.IncludeBoundary,
		constraint.MakeKey(fullStatMin), constraint.IncludeBoundary,
	)
	for ; pIter.Idx < len(partialHistogram); pIter.Next() {
		pBucketSpan := partialSb.MakeSpanFromBucket(ctx, &pIter)

		overlappingSpan := beforeFullSpan
		if overlaps := overlappingSpan.TryIntersectWith(&keyCtx, &pBucketSpan); !overlaps {
			// No overlap, stop looking for partial buckets before the full buckets.
			break
		}
		overlappingSpan.PreferInclusive(&keyCtx)
		filteredPartialBucket := props.GetFilteredBucket(&pIter, &keyCtx, &overlappingSpan, 0)
		mergedHistogram = append(mergedHistogram, filteredPartialBucket)

		// The first full stat bucket will have 0 numRange and distinctRange, so we
		// advance the full stat iterator after overwriting this bucket above.
		if cmp := overlappingSpan.CompareEnds(&keyCtx, &beforeFullSpan); cmp == 0 {
			fIter.Next()
		}

		// Break to avoid advancing the pIter if the partial bucket ends after the
		// beginning of the full stat since we haven't exhausted the current partial
		// bucket.
		if cmp := pBucketSpan.CompareEnds(&keyCtx, &beforeFullSpan); cmp > 0 {
			break
		}
	}

	// Step 2: Emit merged buckets within the range of the full stat buckets.
	// Example:
	//   Full:          |-------|----|--|----|
	//   Partial:  ...------|------|------|----|--...
	//                   ^^^^^^^^^^^^^^^^^^^^^
	for ; fIter.Idx < len(fullHistogram); fIter.Next() {
		fBucketSpan := fullSb.MakeSpanFromBucket(ctx, &fIter)

		// Start with the full bucket's counts, but we'll overwrite the parts that
		// overlap with partial buckets.
		mergedBucket := cat.HistogramBucket{
			UpperBound:    fIter.B.UpperBound,
			NumEq:         fIter.B.NumEq,
			NumRange:      fIter.B.NumRange,
			DistinctRange: fIter.B.DistinctRange,
		}

		for pIter.Idx < len(partialHistogram) {
			pBucketSpan := partialSb.MakeSpanFromBucket(ctx, &pIter)

			overlappingSpan := overlappingSb.MakeSpanFromBucket(ctx, &pIter)
			if overlaps := overlappingSpan.TryIntersectWith(&keyCtx, &fBucketSpan); !overlaps {
				// No overlap, continue to next full bucket.
				break
			}

			overlappingSpan.PreferInclusive(&keyCtx)
			filteredPartialBucket := props.GetFilteredBucket(&pIter, &keyCtx, &overlappingSpan, 0)
			filteredFullBucket := props.GetFilteredBucket(&fIter, &keyCtx, &overlappingSpan, 0)

			// Advance the partial stat iterator if the current one is fully consumed.
			if cmp := overlappingSpan.CompareEnds(&keyCtx, &pBucketSpan); cmp == 0 {
				pIter.Next()
			}

			// Merge the filtered partial bucket into the merged bucket by overwriting
			// the overlapping counts.
			mergedBucket.NumRange =
				mergedBucket.NumRange - filteredFullBucket.NumRange + filteredPartialBucket.NumRange
			mergedBucket.DistinctRange =
				mergedBucket.DistinctRange - filteredFullBucket.DistinctRange + filteredPartialBucket.DistinctRange
			if cmp := overlappingSpan.CompareEnds(&keyCtx, &fBucketSpan); cmp == 0 {
				// Use the partial bucket's NumEq if it overlaps with the full bucket's
				// upper bound.
				mergedBucket.NumEq = filteredPartialBucket.NumEq
				// We've fully consumed the full bucket.
				break
			} else {
				if filteredPartialBucket.NumEq != 0 {
					// The partial bucket ends before the full bucket, so we need to
					// account for the partial bucket's upper bound value.
					mergedBucket.DistinctRange += 1
					mergedBucket.NumRange += filteredPartialBucket.NumEq
				}
			}
		}

		mergedHistogram = append(mergedHistogram, mergedBucket)
	}

	// Step 3: Emit partial stat buckets after the last full stat bucket.
	// Example:
	//   Full:     ...--|
	//   Partial:  ...----|--|---|
	//                   ^^^^^^^^^
	var afterFullSpan constraint.Span
	afterFullSpan.Init(
		constraint.MakeKey(fullStatMax), constraint.ExcludeBoundary,
		constraint.EmptyKey, constraint.IncludeBoundary,
	)
	for ; pIter.Idx < len(partialHistogram); pIter.Next() {
		pBucketSpan := partialSb.MakeSpanFromBucket(ctx, &pIter)

		overlappingSpan := afterFullSpan
		if overlaps := overlappingSpan.TryIntersectWith(&keyCtx, &pBucketSpan); !overlaps {
			return nil, errors.AssertionFailedf(
				"expected overlap between %s and %s", pBucketSpan, overlappingSpan)
		}
		overlappingSpan.PreferInclusive(&keyCtx)
		filteredPartialBucket := props.GetFilteredBucket(&pIter, &keyCtx, &overlappingSpan, 0)
		mergedHistogram = append(mergedHistogram, filteredPartialBucket)
	}

	var mergedNonNullRowCount, mergedNonNullDistinctCount float64
	for _, bucket := range mergedHistogram {
		mergedNonNullRowCount += bucket.NumEq + bucket.NumRange
		mergedNonNullDistinctCount += bucket.DistinctRange
		if bucket.NumEq > 0 {
			mergedNonNullDistinctCount += 1
		}
	}

	// A zero null count in the partial statistic could either mean that the
	// filter excluded nulls or that the filter included nulls, but there were no
	// nulls found. We make a best effort to determine if the filter included
	// nulls by only overwriting the null count of the full statistic if the
	// partial statistic has a non-zero null count.
	mergedNullCount := fullStat.NullCount
	if partialStat.NullCount > 0 {
		mergedNullCount = partialStat.NullCount
	}

	mergedRowCount := uint64(math.Round(mergedNonNullRowCount)) + mergedNullCount
	mergedDistinctCount := uint64(math.Round(mergedNonNullDistinctCount))
	if mergedNullCount > 0 {
		mergedDistinctCount += 1
	}

	mergedAvgSize := fullStat.AvgSize
	if mergedRowCount > 0 {
		mergedAvgSize = (fullStat.AvgSize*(mergedRowCount-partialStat.RowCount) +
			partialStat.AvgSize*partialStat.RowCount) / mergedRowCount
	}

	mergedTableStatistic := &TableStatistic{
		TableStatisticProto: TableStatisticProto{
			TableID:         fullStat.TableID,
			StatisticID:     0,
			Name:            jobspb.MergedStatsName,
			ColumnIDs:       fullStat.ColumnIDs,
			CreatedAt:       partialStat.CreatedAt,
			RowCount:        mergedRowCount,
			DistinctCount:   mergedDistinctCount,
			NullCount:       mergedNullCount,
			AvgSize:         mergedAvgSize,
			FullStatisticID: 0,
		},
	}

	hist := histogram{
		buckets: mergedHistogram,
	}
	hist.adjustCounts(ctx, cmpCtx, fullStat.HistogramData.ColumnType, mergedNonNullRowCount, mergedNonNullDistinctCount)
	histData, err := hist.toHistogramData(ctx, fullStat.HistogramData.ColumnType, st)
	if err != nil {
		return nil, err
	}
	mergedTableStatistic.HistogramData = &histData
	mergedTableStatistic.setHistogramBuckets(hist)

	return mergedTableStatistic, nil
}
