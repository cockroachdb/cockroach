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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// MergeHistograms is the same as opt/props.MergeHistograms but is injected in order
// to avoid an import cycle.
var MergeHistograms = func(
	ctx context.Context,
	evalCtx *eval.Context,
	fullHistogram, partialHistogram []cat.HistogramBucket,
	columnID descpb.ColumnID,
) ([]cat.HistogramBucket, error) {
	return nil, errors.New("MergeHistograms is not injected")
}

// MergedStatistics returns merged stats per single-column full stat by merging
// them with all newer partial stats for that column in chronological order.
// Partial statistics that fail to merge are ignored, and full statistics
// without any successful merges are excluded from the result.
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

	mergedStatsMap := make(map[descpb.ColumnID]*TableStatistic)
	// Iterate over the stats in reverse order so that we merge the latest partial
	// statistics last.
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

			atExtremes := partialStat.FullStatisticID != 0
			merged, err := mergePartialStatistic(ctx, baseStat, partialStat, st, atExtremes)
			if err != nil {
				log.VEventf(ctx, 2, "could not merge statistics for table %v columns %s: %v",
					fullStat.TableID, redact.Safe(col), err)
				continue
			}
			mergedStatsMap[col] = merged
		}
	}

	var colIDs catalog.TableColSet
	for colID := range mergedStatsMap {
		colIDs.Add(colID)
	}
	mergedStats := make([]*TableStatistic, 0, colIDs.Len())
	for _, c := range colIDs.Ordered() {
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

// mergePartialStatistic merges a full statistic with a more recent partial
// statistic, and returns a new merged statistic. It does this by merging their
// histograms (see props.MergeHistograms), and then recalculating the counts for
// the new merged statistic, using the new combined histogram. The createdAt
// time is set to that of the partial statistic, and the statistic is named the
// string assigned to jobspb.MergedStatsName.
//
// In the case where there is no partial histogram, the full statistic
// is returned but with the created_at time of the partial statistic,
// with the statistic renamed to the string assigned to.
// jobspb.MergedStatsName.
func mergePartialStatistic(
	ctx context.Context,
	fullStat *TableStatistic,
	partialStat *TableStatistic,
	st *cluster.Settings,
	atExtremes bool,
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

	var cmpCtx *eval.Context

	fullHistogram = stripOuterBuckets(ctx, cmpCtx, fullHistogram)
	partialHistogram = stripOuterBuckets(ctx, cmpCtx, partialHistogram)

	mergedHistogram := fullHistogram
	if atExtremes {
		var lowerExtremeBuckets, upperExtremeBuckets []cat.HistogramBucket
		i := 0
		// Get the lower extreme buckets from the partial histogram.
		for i < len(partialHistogram) {
			if val, err := partialHistogram[i].UpperBound.Compare(ctx, cmpCtx, fullHistogram[0].UpperBound); err == nil {
				if val == 0 {
					return nil, errors.New("the lowerbound of the full statistic histogram overlaps with the partial statistic histogram")
				}
				if val == -1 {
					lowerExtremeBuckets = append(lowerExtremeBuckets, partialHistogram[i])
					i++
				} else {
					break
				}
			} else {
				return nil, err
			}
		}

		// Get the upper extreme buckets from the partial histogram.
		for i < len(partialHistogram) {
			upperExtremeBuckets = append(upperExtremeBuckets, partialHistogram[i])
			i++
		}

		var err error
		if len(lowerExtremeBuckets) > 0 {
			mergedHistogram, err = MergeHistograms(ctx, cmpCtx, mergedHistogram, lowerExtremeBuckets, fullStat.ColumnIDs[0])
			if err != nil {
				return nil, err
			}
		}
		if len(upperExtremeBuckets) > 0 {
			mergedHistogram, err = MergeHistograms(ctx, cmpCtx, mergedHistogram, upperExtremeBuckets, fullStat.ColumnIDs[0])
			if err != nil {
				return nil, err
			}
		}
	} else {
		var err error
		mergedHistogram, err = MergeHistograms(ctx, cmpCtx, mergedHistogram, partialHistogram, fullStat.ColumnIDs[0])
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
	hist.adjustCounts(ctx, cmpCtx, fullStat.HistogramData.ColumnType, mergedNonNullRowCount, mergedNonNullDistinctCount)
	histData, err := hist.toHistogramData(ctx, fullStat.HistogramData.ColumnType, st)
	if err != nil {
		return nil, err
	}
	mergedTableStatistic.HistogramData = &histData
	mergedTableStatistic.setHistogramBuckets(hist)

	return mergedTableStatistic, nil
}
