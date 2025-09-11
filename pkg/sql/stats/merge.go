// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/datumrange"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// MergedStatistics returns merged stats per single-column full stat by merging
// them with all newer partial stats for that column in chronological order.
// Partial statistics that fail to merge are ignored, and full statistics
// without any successful merges are excluded from the result.
func MergedStatistics(
	ctx context.Context, stats []*TableStatistic, st *cluster.Settings,
) []*TableStatistic {
	var cmpCtx *eval.Context
	// Map the ColumnIDs to the latest full table statistic.
	// It relies on the fact that the first full statistic
	// is the latest.
	fullStatsMap := make(map[descpb.ColumnID]*TableStatistic)
	// Map from full statistic ID to its lower bound. This is used to determine
	// where to split histograms for partial statistics at extremes when merging.
	extremesBoundMap := make(map[uint64]tree.Datum)
	for _, fullStat := range stats {
		if fullStat.IsPartial() || len(fullStat.ColumnIDs) != 1 {
			continue
		}
		col := fullStat.ColumnIDs[0]
		_, ok := fullStatsMap[col]
		if !ok {
			fullStatsMap[col] = fullStat
		}
		if len(fullStat.Histogram) > 0 {
			fh := fullStat.nonNullHistogram().buckets
			fh = stripOuterBuckets(ctx, cmpCtx, fh)
			if len(fh) > 0 {
				extremesBoundMap[fullStat.StatisticID] = fh[0].UpperBound
			}
		}
	}

	var mergedColIDs catalog.TableColSet
	// Iterate over the stats in reverse order so that we merge the latest partial
	// statistics last.
	for i := len(stats) - 1; i >= 0; i-- {
		partialStat := stats[i]
		if !partialStat.IsPartial() || len(partialStat.ColumnIDs) != 1 {
			continue
		}
		col := partialStat.ColumnIDs[0]
		if fullStat, ok := fullStatsMap[col]; ok && partialStat.CreatedAt.After(fullStat.CreatedAt) {
			atExtremes := partialStat.FullStatisticID != 0
			extremesBound := extremesBoundMap[partialStat.FullStatisticID]
			merged, err := mergePartialStatistic(ctx, cmpCtx, fullStat, partialStat,
				st, atExtremes, extremesBound)
			if err != nil {
				log.VEventf(ctx, 2, "could not merge statistics for table %v columns %s: %v",
					fullStat.TableID, redact.Safe(col), err)
				continue
			}
			mergedColIDs.Add(col)
			fullStatsMap[col] = merged
		}
	}

	mergedStats := make([]*TableStatistic, 0, mergedColIDs.Len())
	for _, colID := range mergedColIDs.Ordered() {
		mergedStats = append(mergedStats, fullStatsMap[colID])
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

// mergePartialStatistic merges a full statistic with a more recent partial
// statistic, and returns a new merged statistic. It does this by merging their
// histograms (see mergeHistograms), and then recalculating the counts for the
// new merged statistic, using the new combined histogram. The createdAt time
// is set to that of the partial statistic, and the statistic is named the
// string assigned to jobspb.MergedStatsName.
//
// In the case where there is no partial histogram, the full statistic
// is returned but with the created_at time of the partial statistic,
// with the statistic renamed to the string assigned to jobspb.MergedStatsName.
func mergePartialStatistic(
	ctx context.Context,
	evalCtx *eval.Context,
	fullStat *TableStatistic,
	partialStat *TableStatistic,
	st *cluster.Settings,
	atExtremes bool,
	extremesBound tree.Datum,
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

	fullHistogram = fullStat.nonNullHistogram().buckets
	partialHistogram = partialStat.nonNullHistogram().buckets

	// Note that we still merge if the partial histogram has only NULL values
	// since we can create a merged statistic with an updated NULL count.
	if len(fullHistogram) == 0 {
		return nil, errors.New("the full statistic histogram has only NULL values")
	}

	fullHistogram = stripOuterBuckets(ctx, evalCtx, fullHistogram)
	partialHistogram = stripOuterBuckets(ctx, evalCtx, partialHistogram)

	mergedHistogram := fullHistogram
	if atExtremes {
		if extremesBound == nil {
			return nil, errors.New("can't merge partial statistic at extremes without an extremes bound")
		}
		// Find the index separating the lower and upper extreme partial buckets.
		i := 0
		for i < len(partialHistogram) {
			if val, err := partialHistogram[i].UpperBound.Compare(ctx, evalCtx, extremesBound); err == nil {
				if val == 0 {
					return nil, errors.New("the lowerbound of the full statistic histogram overlaps with the partial statistic histogram")
				}
				if val == -1 {
					i++
				} else {
					break
				}
			} else {
				return nil, err
			}
		}

		lowerExtremeBuckets := partialHistogram[:i]
		upperExtremeBuckets := partialHistogram[i:]

		var err error
		if len(lowerExtremeBuckets) > 0 {
			mergedHistogram, err = mergeHistograms(ctx, evalCtx, mergedHistogram, lowerExtremeBuckets)
			if err != nil {
				return nil, err
			}
		}
		if len(upperExtremeBuckets) > 0 {
			mergedHistogram, err = mergeHistograms(ctx, evalCtx, mergedHistogram, upperExtremeBuckets)
			if err != nil {
				return nil, err
			}
		}
	} else {
		var err error
		mergedHistogram, err = mergeHistograms(ctx, evalCtx, mergedHistogram, partialHistogram)
		if err != nil {
			return nil, err
		}
	}

	var mergedNonNullRowCount, mergedNonNullDistinctCount float64
	for _, bucket := range mergedHistogram {
		mergedNonNullRowCount += bucket.NumEq + bucket.NumRange
		mergedNonNullDistinctCount += bucket.DistinctRange
		if bucket.NumEq > 0 {
			mergedNonNullDistinctCount += 1
		}
	}

	// A zero null count in non-extreme partial statistics could either mean that
	// the filter excluded nulls or that the filter included nulls, but there were
	// no nulls found. We make a best effort to determine if the filter included
	// nulls by only overwriting the null count of the full statistic if the
	// partial statistic has a non-zero null count. Partial statistics at extremes
	// always include nulls, so we always overwrite the null count in that case.
	mergedNullCount := fullStat.NullCount
	if partialStat.NullCount > 0 || atExtremes {
		mergedNullCount = partialStat.NullCount
	}

	mergedRowCount := uint64(math.Round(mergedNonNullRowCount)) + mergedNullCount
	mergedDistinctCount := uint64(math.Round(mergedNonNullDistinctCount))
	if mergedNullCount > 0 {
		mergedDistinctCount += 1
	}

	// mergedAvgSize is a weighted average of the rows that remain from the full
	// stat and the rows contributed by the partial stat.
	mergedAvgSize := fullStat.AvgSize
	if mergedRowCount > 0 {
		mergedAvgSize = (fullStat.AvgSize*(mergedRowCount-partialStat.RowCount) +
			partialStat.AvgSize*partialStat.RowCount) / mergedRowCount
	}

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
	hist.adjustCounts(ctx, evalCtx, fullStat.HistogramData.ColumnType, mergedNonNullRowCount, mergedNonNullDistinctCount)
	histData, err := hist.toHistogramData(ctx, fullStat.HistogramData.ColumnType, st)
	if err != nil {
		return nil, err
	}
	mergedTableStatistic.HistogramData = &histData
	mergedTableStatistic.setHistogramBuckets(hist)

	return mergedTableStatistic, nil
}

type boundedBucket struct {
	cat.HistogramBucket
	// lowerBound is the inclusive lower bound of the bucket. It is the same as
	// HistogramBucket.UpperBound for the first bucket.
	lowerBound tree.Datum
}

func getBoundedBucket(
	ctx context.Context, evalCtx *eval.Context, histogram []cat.HistogramBucket, idx int,
) (b boundedBucket) {
	b = boundedBucket{
		HistogramBucket: histogram[idx],
		lowerBound:      histogram[idx].UpperBound,
	}
	if idx > 0 {
		b.lowerBound = histogram[idx-1].UpperBound
		// Make the lower bound inclusive if possible.
		if inclusiveLB, ok := histogram[idx-1].UpperBound.Next(ctx, evalCtx); ok {
			b.lowerBound = inclusiveLB
		}
	}
	return b
}

// tryOverlap checks if two buckets overlap, returning the overlapping lower and
// upper bounds if they do.
func tryOverlap(
	ctx context.Context, evalCtx *eval.Context, a, b boundedBucket,
) (ok bool, lower, upper tree.Datum, err error) {
	var cmp int
	cmp, err = a.lowerBound.Compare(ctx, evalCtx, b.lowerBound)
	if err != nil {
		return false, nil, nil, err
	}
	if cmp < 0 {
		lower = b.lowerBound
	} else {
		lower = a.lowerBound
	}
	cmp, err = a.UpperBound.Compare(ctx, evalCtx, b.UpperBound)
	if err != nil {
		return false, nil, nil, err
	}
	if cmp < 0 {
		upper = a.UpperBound
	} else {
		upper = b.UpperBound
	}
	cmp, err = lower.Compare(ctx, evalCtx, upper)
	if err != nil {
		return false, nil, nil, err
	}
	if cmp > 0 {
		return false, nil, nil, nil
	}
	return true, lower, upper, nil
}

// filterBucket estimates the range and distinct counts in a bucket that fall
// within the range between filterLower and filterUpper. It assumes that values
// are uniformly distributed within the bucket.
func filterBucket(
	bucket boundedBucket, filterLower, filterUpper tree.Datum,
) (numRange, distinctRange float64) {
	rangeBefore, rangeAfter, ok := datumrange.GetRangesBeforeAndAfter(
		bucket.lowerBound, bucket.UpperBound, filterLower, filterUpper, false /* swap */)

	if ok && rangeBefore > 0 {
		numRange = bucket.NumRange * rangeAfter / rangeBefore
	} else {
		numRange = 0.5 * bucket.NumRange
	}
	if bucket.NumRange > 0 {
		distinctRange = bucket.DistinctRange * numRange / bucket.NumRange
	}
	return numRange, distinctRange
}

// mergeHistograms merges the histograms from a full statistic and a more recent
// partial statistic, returning a new histogram that combines the information
// from both. It does this by updating counts in the full histogram's buckets
// based on overlapping buckets in the partial histogram, and creating new
// buckets outside the full histogram's range. This function requires that the
// partial histogram is a single continuous histogram without gaps, and neither
// histogram has a NULL bucket.
//
// The approach is similar to a merge join, where we iterate over both
// sorted histograms. We start by creating new buckets for each partial bucket
// that comes before the first full bucket. Next, we append each full bucket
// after overwriting parts that overlap with partial buckets, assuming that
// values are uniformly distributed across both buckets. Finally, we create new
// buckets for each partial stat bucket that comes after the last full bucket.
//
// This example illustrates the merging process for overlapping buckets, where
// the histogram format is {NumEq, NumRange, DistinctRange, UpperBound}:
//
// Full histogram:    [{1, 0, 0, 10},                {1, 4, 2, 20}]
// Partial histogram: [{2, 0, 0, 10}, {1, 3, 3, 15}]
// Merged histogram:  [{2, 0, 0, 10},                {1, 6, 5, 20}]
//
// The first partial bucket completely overlaps with the first full bucket, so
// we produce a bucket with the partial bucket's counts. The second partial
// bucket overlaps with half of the second full bucket's range, so we assume
// that values are uniformly distributed within buckets and produce a merged
// bucket that combines half of the full bucket's NumRange and DistinctRange
// with those of the partial bucket. The full bucket's NumEq is unchanged since
// the partial bucket does not overlap with its upper bound.
func mergeHistograms(
	ctx context.Context, evalCtx *eval.Context, full, partial []cat.HistogramBucket,
) ([]cat.HistogramBucket, error) {
	merged := make([]cat.HistogramBucket, 0, len(full)+len(partial))
	var i int
	var j int

	// Step 1: Emit partial stat buckets before the first full stat bucket.
	// Example:
	//   Full:                |-------|--...
	//   Partial:  |--|---|-------|------...
	//             ^^^^^^^^^^^^
	fLowerBound := full[0].UpperBound
	for ; j < len(partial); j++ {
		pBucket := getBoundedBucket(ctx, evalCtx, partial, j)
		cmp, err := pBucket.UpperBound.Compare(ctx, evalCtx, fLowerBound)
		if err != nil {
			return nil, err
		}

		if cmp <= 0 {
			// The partial bucket ends before or at the first full bucket, so we can
			// emit it as is.
			merged = append(merged, partial[j])
			if cmp == 0 {
				// Advance i to avoid re-emitting the full bucket since it is
				// overwritten by the partial bucket.
				i++
			}
		} else {
			cmp, err = pBucket.lowerBound.Compare(ctx, evalCtx, fLowerBound)
			if err != nil {
				return nil, err
			}

			if cmp < 0 {
				// The partial bucket partially overlaps with the first full bucket,
				// so we only emit the non-overlapping part.
				numRange, distinctRange := filterBucket(pBucket, pBucket.lowerBound, fLowerBound)

				merged = append(merged, cat.HistogramBucket{
					NumEq:         full[0].NumEq,
					NumRange:      numRange,
					DistinctRange: distinctRange,
					UpperBound:    fLowerBound,
				})
				// Advance i to avoid re-emitting the full bucket since it is
				// overwritten by the partial bucket.
				i++
			}
			// Break to avoid advancing j since we haven't exhausted the current
			// partial bucket.
			break
		}
	}

	// Step 2: Emit merged buckets within the range of the full stat buckets.
	// Example:
	//   Full:          |-------|----|--|----|
	//   Partial:  ...------|------|------|----|--...
	//                   ^^^^^^^^^^^^^^^^^^^^^
	for ; i < len(full); i++ {
		// Start with the full bucket's counts, but we'll overwrite the parts that
		// overlap with partial buckets.
		b := full[i]
		fBucket := getBoundedBucket(ctx, evalCtx, full, i)
		for ; j < len(partial); j++ {
			pBucket := getBoundedBucket(ctx, evalCtx, partial, j)

			overlaps, overlapLower, overlapUpper, err := tryOverlap(ctx, evalCtx,
				pBucket, fBucket)
			if err != nil {
				return nil, err
			}
			if !overlaps {
				// No overlap, continue to next full bucket.
				break
			}

			pNumRange, pDistinctRange := filterBucket(pBucket, overlapLower, overlapUpper)
			fNumRange, fDistinctRange := filterBucket(fBucket, overlapLower, overlapUpper)

			// Merge the filtered partial bucket into the merged bucket by overwriting
			// the overlapping counts.
			b.NumRange = b.NumRange - fNumRange + pNumRange
			b.DistinctRange = b.DistinctRange - fDistinctRange + pDistinctRange

			if cmp, err := pBucket.UpperBound.Compare(ctx, evalCtx, fBucket.UpperBound); err == nil {
				if cmp == 0 {
					// Use the partial bucket's NumEq if the upper bounds are the same.
					b.NumEq = partial[j].NumEq
				} else if cmp < 0 {
					if partial[j].NumEq > 0 {
						// The partial bucket ends before the full bucket, so we need to
						// account for the partial bucket's upper bound value.
						b.DistinctRange += 1
						b.NumRange += partial[j].NumEq
					}
				} else {
					// The partial bucket ends after the full bucket, so we continue to
					// the next full bucket.
					break
				}
			} else {
				return nil, err
			}
		}
		merged = append(merged, b)
	}

	// Step 3: Emit partial stat buckets after the last full stat bucket.
	// Example:
	//   Full:     ...--|
	//   Partial:  ...----|--|---|
	//                   ^^^^^^^^^
	fUpperBound := full[len(full)-1].UpperBound
	for ; j < len(partial); j++ {
		pBucket := getBoundedBucket(ctx, evalCtx, partial, j)
		if cmp, err := pBucket.lowerBound.Compare(ctx, evalCtx, fUpperBound); err == nil {
			if cmp < 0 {
				// The partial bucket overlaps with the last full bucket, so we only
				// emit the non-overlapping part.
				numRange, distinctRange := filterBucket(pBucket, fUpperBound, pBucket.UpperBound)

				merged = append(merged, cat.HistogramBucket{
					NumEq:         partial[j].NumEq,
					NumRange:      numRange,
					DistinctRange: distinctRange,
					UpperBound:    partial[j].UpperBound,
				})
			} else {
				merged = append(merged, partial[j])
			}
		} else {
			return nil, err
		}
	}

	return merged, nil
}
