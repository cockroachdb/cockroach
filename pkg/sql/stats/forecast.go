// Copyright 2022 The Cockroach Authors.
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
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// We require at least 3 observations to produce a forecast. Forecasts based on
// 1 or 2 observations will always have perfect R² (goodness of fit) regardless
// of the accuracy of the forecast.
const minObservationsForForecast = 3

// We only use a predicted value in a forecast if the R² (goodness of fit)
// measurement of the predictive model is high enough. Otherwise we use the
// latest observed value. This means our forecasts are a mixture of predicted
// parts and observed parts.
const minGoodnessOfFit = 0.95

// To use a forecast (rather than using the latest observed statistics) it must
// have a certain number of predicted parts. Some predicted parts are more
// valuable than others. We call the weighted number of predicted parts the
// score of a forecast.
const minForecastScore = 1

// ForecastTableStatistics produces a forecast of the next statistics for a
// table, given the observed statistics. If a forecast cannot be produced (or is
// not worth using) an error is returned.
//
// If a forecast is produced, it will include statistics for each columnset in
// the latest observed statistics. The forecasted row counts for these
// columnsets will all be the same, assuming row counts were the same across
// columnsets in observed statistics.
//
// ForecastTableStatistics is deterministic, given the same observations.
func ForecastTableStatistics(
	ctx context.Context, evalCtx *eval.Context, observed []*TableStatistic,
) ([]*TableStatistic, error) {
	// Early sanity check. We have to check this again after grouping by column set.
	if len(observed) < minObservationsForForecast {
		return nil, errors.Errorf("not enough observations to forecast statistics")
	}

	// Find the time of the latest observed statistics. If we produce a forecast,
	// it will be for each columnset in the latest observed statistics.
	tableID := observed[0].TableID
	latest := observed[0].CreatedAt
	for _, stats := range observed {
		if stats.TableID != tableID {
			return nil, errors.AssertionFailedf(
				"statistics were not all from table %d: %d", tableID, stats.TableID,
			)
		}
		if stats.CreatedAt.IsZero() {
			return nil, errors.AssertionFailedf("statistics with CreatedAt = 0")
		}
		if stats.CreatedAt.After(latest) {
			latest = stats.CreatedAt
		}
	}

	// Group observed statistics by columnset.
	var forecastCols []string
	observedByCols := make(map[string][]*TableStatistic)
	for _, stats := range observed {
		cols := tableStatisticColumnsString(stats)
		if stats.CreatedAt.Equal(latest) {
			forecastCols = append(forecastCols, cols)
		}
		observedByCols[cols] = append(observedByCols[cols], stats)
	}

	// To produce a forecast we need all the latest columnsets to be present in
	// the most recent N observed statistics, where N >=
	// minObservationsForForecast. So we sort each bucket by CreatedAt descending.
	for _, cols := range forecastCols {
		observedCol := observedByCols[cols]
		sort.Slice(observedCol, func(i, j int) bool {
			return observedCol[i].CreatedAt.After(observedCol[j].CreatedAt)
		})
	}
	var n int
	observedCol0 := observedByCols[forecastCols[0]]
NLoop:
	for ; n < len(observedCol0); n++ {
		collectionTime := observedCol0[n].CreatedAt
		for i := 1; i < len(forecastCols); i++ {
			observedCol := observedByCols[forecastCols[i]]
			if len(observedCol) <= n || !observedCol[n].CreatedAt.Equal(collectionTime) {
				break NLoop
			}
		}
	}

	if n < minObservationsForForecast {
		return nil, errors.Errorf("not enough observations to forecast statistics")
	}

	// To make forecasts deterministic, we must choose a time to forecast at based
	// on only the observed statistics. We choose the time of the latest
	// statistics + the average time between collections, which should be roughly
	// when the next stats collection will occur.
	at := latest.Add(avgRefreshTime(observedCol0[:n]))

	// If any columnset has a valuable forecast, then we must also use forecasts
	// for the rest of the columnsets in the table (even if those forecasts are
	// mostly copies of the latest observed statistics). So we forecast statistics
	// for all columnsets, and then figure out if any of them were valuable
	// afterward. Forecasted RowCounts will be the same for all columnsets
	// assuming that all observations have the same RowCounts, by virtue of
	// passing the same minGoodnessOfFit each time.
	var totalScore float64
	forecasts := make([]*TableStatistic, len(forecastCols))
	for i := range forecastCols {
		observedCol := observedByCols[forecastCols[i]]
		forecast, score := forecastColumnStatistics(ctx, evalCtx, observedCol, at, n, minGoodnessOfFit)
		forecasts[i] = forecast
		totalScore += score
	}
	if totalScore < minForecastScore {
		return nil, errors.Errorf("forecast not worth using")
	}
	return forecasts, nil
}

func tableStatisticColumnsString(stat *TableStatistic) string {
	cols := strconv.FormatUint(uint64(stat.ColumnIDs[0]), 10)
	for i := 1; i < len(stat.ColumnIDs); i++ {
		cols += " " + strconv.FormatUint(uint64(stat.ColumnIDs[i]), 10)
	}
	return cols
}

// forecastColumnStatistics produces a statistics forecast for a columnset at
// the given time, based on the first N observed statistics. Observed statistics
// must be sorted by CreatedAt descending (latest observation first).
//
// To create a statistics forecast, we first construct a predictive model for
// each part (row count, null count, distinct count, avg row size, histogram,
// etc.). If the model for a part is a good fit (i.e. has R² >= minRequiredFit)
// then we use the model to predict the value for that part. If the model is a
// poor fit then we simply copy the value for that part from the latest observed
// statistics. At the end we adjust all values for consistency.
//
// A score is also returned, indicating how valuable we think the forecast
// is. If every part of the forecast is copied from the latest observed
// statistics instead of predicted, the score will be 0.
//
// forecastColumnStatistics is deterministic, given the same observations,
// forecast time, n, and minRequiredFit.
func forecastColumnStatistics(
	ctx context.Context,
	evalCtx *eval.Context,
	observed []*TableStatistic,
	at time.Time,
	n int,
	minRequiredFit float64,
) (forecast *TableStatistic, score float64) {
	forecastAt := float64(at.Unix())
	tableID := observed[0].TableID
	columnIDs := observed[0].ColumnIDs
	log.VEventf(ctx, 2, "forecasting statistics for table %v columns %v", tableID, columnIDs)

	createdAts := make([]float64, n)
	rowCounts := make([]float64, n)
	distinctCounts := make([]float64, n)
	nullCounts := make([]float64, n)
	avgSizes := make([]float64, n)
	for i := 0; i < n; i++ {
		createdAts[i] = float64(observed[i].CreatedAt.Unix())
		rowCounts[i] = float64(observed[i].RowCount)
		distinctCounts[i] = float64(observed[i].DistinctCount)
		nullCounts[i] = float64(observed[i].NullCount)
		avgSizes[i] = float64(observed[i].AvgSize)
	}

	rowCount, r2 := float64SimpleLinearRegression(createdAts, rowCounts, forecastAt)
	log.VEventf(
		ctx, 2, "forecast for table %v columns %v rowCount %v r2 %v",
		tableID, columnIDs, rowCount, r2,
	)
	if r2 < minRequiredFit {
		rowCount = float64(observed[0].RowCount)
	} else {
		rowCount = clampPosInt64(rowCount)
	}

	distinctCount, r2 := float64SimpleLinearRegression(createdAts, distinctCounts, forecastAt)
	log.VEventf(
		ctx, 2, "forecast for table %v columns %v distinctCount %v r2 %v",
		tableID, columnIDs, distinctCount, r2,
	)
	if r2 < minRequiredFit {
		distinctCount = float64(observed[0].DistinctCount)
	} else {
		distinctCount = clampPosInt64(distinctCount)
	}

	nullCount, r2 := float64SimpleLinearRegression(createdAts, nullCounts, forecastAt)
	log.VEventf(
		ctx, 2, "forecast for table %v columns %v nullCount %v r2 %v",
		tableID, columnIDs, nullCount, r2,
	)
	if r2 < minRequiredFit {
		nullCount = float64(observed[0].NullCount)
	} else {
		nullCount = clampPosInt64(nullCount)
	}

	avgSize, r2 := float64SimpleLinearRegression(createdAts, avgSizes, forecastAt)
	log.VEventf(
		ctx, 2, "forecast for table %v columns %v avgSize %v r2 %v",
		tableID, columnIDs, avgSize, r2,
	)
	if r2 < minRequiredFit {
		avgSize = float64(observed[0].AvgSize)
	} else {
		avgSize = clampPosInt64(avgSize)
	}

	var histData = observed[0].HistogramData
	var hist histogram
	var predictedHist bool
	var colType *types.T
	if histData != nil {
		colType = histData.ColumnType
		if canMakeQuantile(colType) {
			var quantiles []quantile
			for i := 0; i < n; i++ {
				if observed[i].HistogramData == nil {
					break
				}
				if observed[i].HistogramData.Version > 1 {
					break
				}
				if !observed[i].HistogramData.ColumnType.Equivalent(colType) {
					break
				}
				q, err := makeQuantile(observed[i].nonNullHistogram(), rowCounts[i]-nullCounts[i])
				if err != nil {
					log.VEventf(
						ctx, 2, "forecast for table %v columns %v makeQuantile(histogram at %v) err: %v",
						tableID, columnIDs, observed[i].CreatedAt, err,
					)
					break
				}
				quantiles = append(quantiles, q)
			}
			if len(quantiles) == n {
				q, r2 := quantileSimpleLinearRegression(createdAts, quantiles, forecastAt)
				q = q.fixMalformed()
				log.VEventf(
					ctx, 2, "forecast for table %v columns %v quantile %v r2 %v",
					tableID, columnIDs, q, r2,
				)
				if r2 >= minRequiredFit {
					h, err := q.toHistogram(evalCtx, colType, rowCount-nullCount)
					if err != nil {
						log.VErrEventf(
							ctx, 2, "forecast for table %v columns %v quantile.toHistogram err: %v",
							tableID, columnIDs, err,
						)
					} else {
						hist = h
						predictedHist = true
					}
				}
			}
		}
		// If we did not successfully predict a histogram then copy the latest
		// histogram so we can adjust it below.
		if !predictedHist {
			hist.buckets = append(hist.buckets, observed[0].nonNullHistogram().buckets...)
		}
	}

	// Enforce consistency between parts. Notably we *don't* change rowCount here,
	// which helps ensure that all forecasted statistics for the same table have
	// the same row count.
	if nullCount > rowCount {
		nullCount = rowCount
	}
	minDistinctCount := float64(0)
	maxDistinctCount := rowCount - nullCount
	if rowCount-nullCount > 0 {
		minDistinctCount++
	}
	if nullCount > 0 {
		minDistinctCount++
		maxDistinctCount++
	}
	if distinctCount < minDistinctCount {
		distinctCount = minDistinctCount
	}
	if distinctCount > maxDistinctCount {
		distinctCount = maxDistinctCount
	}
	// TODO(michae2): Make avgSize prediction smarter by looking at colType.
	minAvgSize := float64(0)
	maxAvgSize := float64(math.MaxInt64)
	if nullCount == 0 {
		minAvgSize = 1
	}
	if rowCount-nullCount == 0 {
		minAvgSize = 0
		maxAvgSize = 0
	}
	if avgSize < minAvgSize {
		avgSize = minAvgSize
	}
	if avgSize > maxAvgSize {
		avgSize = maxAvgSize
	}
	if histData != nil {
		nnRowCount := rowCount - nullCount
		nnDistinctCount := distinctCount
		if nullCount > 0 {
			nnDistinctCount--
		}
		hist.adjustCounts(evalCtx, nnRowCount, nnDistinctCount)
		hd, err := hist.toHistogramData(colType)
		if err != nil {
			log.VErrEventf(
				ctx, 2, "forecast for table %v columns %v toHistogramData err: %v",
				tableID, columnIDs, err,
			)
			// We failed to convert the adjusted histogram to HistogramData. Revert
			// back to the latest observed histogram for consistency with latest
			// observed HistogramData. It won't be properly adjusted to match row
			// count and distinct counts, but there's nothing we can do at this
			// point. (This failure to convert to HistogramData should be very rare,
			// anyway.)
			hist = observed[0].nonNullHistogram()
		} else {
			histData = &hd
			if predictedHist {
				score++
			}
		}
	}

	forecast = &TableStatistic{
		TableStatisticProto: TableStatisticProto{
			TableID:       tableID,
			StatisticID:   0, // TODO(michae2): figure out whether this needs to be unique
			Name:          jobspb.ForecastStatsName,
			ColumnIDs:     columnIDs,
			CreatedAt:     at,
			RowCount:      uint64(rowCount),
			DistinctCount: uint64(distinctCount),
			NullCount:     uint64(nullCount),
			HistogramData: histData,
			AvgSize:       uint64(avgSize),
		},
	}
	if histData != nil {
		forecast.setHistogramBuckets(hist)
	}
	return
}

func clampPosInt64(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > math.MaxInt64 {
		return math.MaxInt64
	}
	return math.Round(x)
}
