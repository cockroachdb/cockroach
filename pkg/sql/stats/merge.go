// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

// mergeExtremesStatistic merges a full table statistic with a partial table statistic
// and returns a new full table statistic. It does this by prepending the partial
// histogram buckets that are less than the first histogram bucket to start of the
// histogram and appending the partial histogram buckets that are greater than the
// full histogram bucket to the end of the histogram, and recalculating the counts
// for the new merged statistic.
//
// For example, consider this case:
// Full Statistic:
// Counts: {row: 14, dist: 9, null: 4, size: 1}
// Histogram (format is: {NumEq, NumRange, DistRange, UpperBound}):
// {}
//
//
//
//
//
//

func mergeExtremesStatistic(
	fullStat *TableStatistic, partialStat *TableStatistic,
) (*TableStatistic, error) {
	fullStatColKey := MakeSortedColStatKey(fullStat.ColumnIDs)
	partialStatColKey := MakeSortedColStatKey(partialStat.ColumnIDs)
	if fullStatColKey != partialStatColKey {
		return fullStat, pgerror.New(pgcode.ObjectNotInPrerequisiteState, "Column sets for full table statistics and partial table statistics column sets do not match")
	}

	// Merge the histograms
	// Currently, since we don't merge multi-column statistics, each
	// statistic passed through should have a histogram.
	// TODO (faizaanmadhani): Add support for multi-column partial statistics.
	fullHistogram := fullStat.Histogram
	partialHistogram := partialStat.Histogram

	if len(fullHistogram) == 0 {
		return fullStat, pgerror.New(pgcode.ObjectNotInPrerequisiteState, "The full statistic histogram does not exist")
	}
	if len(partialHistogram) == 0 {
		return partialStat, pgerror.New(pgcode.ObjectNotInPrerequisiteState, "The partial histogram does not exist")
	}

	mergedHistogram := make([]cat.HistogramBucket, 0, len(fullHistogram)+len(partialHistogram))

	// Remove the NULL bucket from the front
	// of both histograms if it exists.
	if fullHistogram[0].UpperBound == tree.DNull {
		fullHistogram = fullHistogram[1:]
	}
	var partialNullCount uint64
	if partialHistogram[0].UpperBound == tree.DNull {
		partialNullCount = uint64(partialHistogram[0].NumEq)
		partialHistogram = partialHistogram[1:]
	}

	var cmpCtx *eval.Context

	var numOriginalRows uint64

	i := 0
	j := 0
	// Merge partial stats to prior full statistics.
	for i < len(partialHistogram) && j < len(fullHistogram) {
		if val, err := partialHistogram[i].UpperBound.CompareError(cmpCtx, fullHistogram[j].UpperBound); err == nil {
			if val == -1 {
				mergedHistogram = append(mergedHistogram, partialHistogram[i])
				numOriginalRows += uint64(partialHistogram[i].NumRange + partialHistogram[i].NumEq)

				i++
			} else {
				mergedHistogram = append(mergedHistogram, fullHistogram[j])
				numOriginalRows += uint64(fullHistogram[j].NumRange + fullHistogram[j].NumEq)
				j++
			}
		} else {
			return nil, err
		}
	}

	// Iterate through the rest of the full histogram and append it.
	for j < len(fullHistogram) {
		mergedHistogram = append(mergedHistogram, fullHistogram[j])
		numOriginalRows += uint64(fullHistogram[j].NumRange + fullHistogram[j].NumEq)
		j++
	}

	// iterate through the remaining partial histogram and append it.
	for i < len(partialHistogram) {
		mergedHistogram = append(mergedHistogram, partialHistogram[i])
		numOriginalRows += uint64(partialHistogram[i].NumRange + partialHistogram[i].NumEq)
		i++
	}

	var mergedRowCount uint64
	var mergedDistinctCount uint64
	mergedNullCount := partialNullCount
	for _, bucket := range mergedHistogram {
		mergedRowCount += uint64(bucket.NumEq + bucket.NumRange)
		mergedDistinctCount += uint64(bucket.DistinctRange)
		if bucket.NumEq > 0 {
			mergedDistinctCount += 1
		}
	}
	mergedRowCount += mergedNullCount

	mergedAvgSize := (partialStat.AvgSize*partialStat.RowCount + fullStat.AvgSize*numOriginalRows) / (partialStat.RowCount + numOriginalRows)

	mergedTableStatistic := &TableStatistic{
		TableStatisticProto: TableStatisticProto{
			TableID:       fullStat.TableID,
			StatisticID:   0, // TODO (faizaanmadhani): Add support for SHOW HISTOGRAM.
			Name:          jobspb.MergedStatsName,
			ColumnIDs:     fullStat.ColumnIDs,
			CreatedAt:     partialStat.CreatedAt,
			RowCount:      mergedRowCount,
			DistinctCount: mergedDistinctCount,
			NullCount:     mergedNullCount,
			AvgSize:       mergedAvgSize,
		},
	}

	hist := histogram{
		buckets: mergedHistogram,
	}
	histData, err := hist.toHistogramData(fullStat.HistogramData.ColumnType)
	if err != nil {
		return nil, err
	}
	mergedTableStatistic.HistogramData = &histData
	mergedTableStatistic.setHistogramBuckets(hist)

	return mergedTableStatistic, nil
}

// CreateMergedStatistics returns an array of table statistics that
// are the merged combinations of the latest full statistic and the latest partial
// statistic for a specific column set. If merging a partial stat with the full
// stat is not possible, we don't include that statistic as part of the resulting array.
func CreateMergedStatistics(
	ctx context.Context, partialStatsList []*TableStatistic, fullStatsList []*TableStatistic,
) []*TableStatistic {
	// Iterate through partialTableStats and construct a map
	// mapping the columnID set to the latest partial table stat.
	// It relies on the fact that the first partial stat for that
	// column is the latest one as they sorted by descending
	// createdAt time.
	partialStatsMap := make(map[descpb.ColumnID]*TableStatistic)
	for _, stat := range partialStatsList {
		col := stat.ColumnIDs[0]
		if _, ok := partialStatsMap[col]; !ok {
			partialStatsMap[col] = stat
		}
	}
	// Map the ColumnIDs to the latest full table statistic,
	// and map the keys to the amount of columns in the set.
	// It relies on the fact that the first full statistic
	// is the latest.
	fullStatsMap := make(map[descpb.ColumnID]*TableStatistic)
	for _, stat := range fullStatsList {
		col := stat.ColumnIDs[0]
		if len(stat.ColumnIDs) == 1 {
			_, ok := fullStatsMap[col]
			if !ok {
				fullStatsMap[col] = stat
			}
		}
	}

	mergedStats := make([]*TableStatistic, 0, len(fullStatsMap))
	for col, fullStat := range fullStatsMap {
		var merged *TableStatistic
		var err error
		if val, ok := partialStatsMap[col]; ok && val.CreatedAt.After(fullStatsMap[col].CreatedAt) {
			partialStat := partialStatsMap[col]
			merged, err = mergeExtremesStatistic(fullStat, partialStat)
			if err != nil {
				log.VEventf(ctx, 2, "could not merge statistics for table %v columns %s: %v", fullStat.TableID, redact.Safe(col), err)
				continue
			}
			mergedStats = append(mergedStats, merged)
		}
	}
	return mergedStats
}
