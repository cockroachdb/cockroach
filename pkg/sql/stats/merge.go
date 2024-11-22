// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// MergedStatistics returns an array of table statistics that
// are the merged combinations of the latest full statistic and the latest partial
// statistic for a specific column set. If merging a partial stat with the full
// stat is not possible, we don't include that statistic as part of the resulting array.
func MergedStatistics(
	ctx context.Context, stats []*TableStatistic, st *cluster.Settings,
) []*TableStatistic {
	// Map the ColumnIDs to the latest full table statistic,
	// and map the keys to the number of columns in the set.
	// It relies on the fact that the first full statistic
	// is the latest.
	fullStatsMap := make(map[descpb.ColumnID]*TableStatistic)
	for _, stat := range stats {
		if stat.IsPartial() || len(stat.ColumnIDs) != 1 {
			continue
		}
		col := stat.ColumnIDs[0]
		_, ok := fullStatsMap[col]
		if !ok {
			fullStatsMap[col] = stat
		}
	}

	mergedStats := make([]*TableStatistic, 0, len(fullStatsMap))
	var seenCols intsets.Fast
	for _, partialStat := range stats {
		if !partialStat.IsPartial() || len(partialStat.ColumnIDs) != 1 {
			continue
		}
		col := partialStat.ColumnIDs[0]
		if !seenCols.Contains(int(col)) {
			seenCols.Add(int(col))
			var merged *TableStatistic
			var err error
			if fullStat, ok := fullStatsMap[col]; ok && partialStat.CreatedAt.After(fullStat.CreatedAt) {
				merged, err = mergeExtremesStatistic(ctx, fullStat, partialStat, st)
				if err != nil {
					log.VEventf(ctx, 2, "could not merge statistics for table %v columns %s: %v", fullStat.TableID, redact.Safe(col), err)
					continue
				}
				mergedStats = append(mergedStats, merged)
			}
		}
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
		return fullStat, errors.New("the full statistic histogram does not exist")
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

	// Remove the NULL bucket from the front
	// of both histograms if it exists.
	if fullHistogram[0].UpperBound == tree.DNull {
		fullHistogram = fullHistogram[1:]
	}
	if partialHistogram[0].UpperBound == tree.DNull {
		partialHistogram = partialHistogram[1:]
	}

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
