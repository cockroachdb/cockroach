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
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// UseStatisticsForecasts controls whether statistics forecasts are generated in
// the stats cache.
var UseStatisticsForecasts = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.stats.forecasts.enabled",
	"when true, enables generation of statistics forecasts by default for all tables",
	true,
).WithPublic()

// minObservationsForForecast is the minimum number of observed statistics
// required to produce a statistics forecast. Forecasts based on 1 or 2
// observations will always have R² = 1 (perfect goodness of fit) regardless of
// the accuracy of the forecast.
const minObservationsForForecast = 3

// minGoodnessOfFit is the minimum R² (goodness of fit) measurement all
// predictive models in a forecast must have for us to use the forecast.
const minGoodnessOfFit = 0.95

// ForecastTableStatistics produces zero or more statistics forecasts based on
// the given observed statistics. The observed statistics must be ordered by
// collection time descending, with the latest observed statistics first. The
// observed statistics may be a mixture of statistics for different sets of
// columns. Partial statistics will be skipped.
//
// Whether a forecast is produced for a set of columns depends on how well the
// observed statistics for that set of columns fit a linear regression model.
// This means a forecast will not necessarily be produced for every set of
// columns in the table. Any forecasts produced will have the same CreatedAt
// time, which will be after the latest observed statistics (and could be in the
// past, present, or future relative to the current time). Any forecasts
// produced will not necessarily have the same RowCount or be consistent with
// the other forecasts produced. (For example, DistinctCount in the forecast for
// columns {a, b} could very well end up less than DistinctCount in the forecast
// for column {a}.)
//
// ForecastTableStatistics is deterministic: given the same observations it will
// return the same forecasts.
func ForecastTableStatistics(ctx context.Context, observed []*TableStatistic) []*TableStatistic {
	// Group observed statistics by column set, skipping over partial statistics
	// and statistics with inverted histograms.
	var latest time.Time
	var forecastCols []string
	observedByCols := make(map[string][]*TableStatistic)
	for _, stat := range observed {
		if stat.IsPartial() {
			continue
		}
		// We don't have a good way to detect inverted statistics right now, so skip
		// all statistics with histograms of type BYTES. This means we cannot
		// forecast statistics for normal BYTES columns.
		// TODO(michae2): Improve this when issue #50655 is fixed.
		if stat.HistogramData != nil && stat.HistogramData.ColumnType != nil &&
			stat.HistogramData.ColumnType.Family() == types.BytesFamily {
			continue
		}
		if latest.IsZero() {
			latest = stat.CreatedAt
		}
		colKey := MakeSortedColStatKey(stat.ColumnIDs)
		obs, ok := observedByCols[colKey]
		if !ok {
			forecastCols = append(forecastCols, colKey)
		}
		observedByCols[colKey] = append(obs, stat)
	}

	// To make forecasts deterministic, we must choose a time to forecast at based
	// on only the observed statistics. We choose the time of the latest full
	// statistics + the average time between automatic stats collections, which
	// should be roughly when the next automatic stats collection will occur.
	if latest.IsZero() {
		// No suitable stats.
		return nil
	}
	at := latest.Add(avgFullRefreshTime(observed))

	forecasts := make([]*TableStatistic, 0, len(forecastCols))
	for _, colKey := range forecastCols {
		forecast, err := forecastColumnStatistics(ctx, observedByCols[colKey], at, minGoodnessOfFit)
		if err != nil {
			log.VEventf(
				ctx, 2, "could not forecast statistics for table %v columns %s: %v",
				observed[0].TableID, redact.SafeString(colKey), err,
			)
			continue
		}
		forecasts = append(forecasts, forecast)
	}
	return forecasts
}

// forecastColumnStatistics produces a statistics forecast at the given time,
// based on the given observed statistics. The observed statistics must all be
// for the same set of columns, must all be full statistics, must not contain
// any inverted histograms, must have a single observation per collection time,
// and must be ordered by collection time descending with the latest observed
// statistics first. The given time to forecast at can be in the past, present,
// or future.
//
// To create a forecast, we construct a linear regression model over time for
// each statistic (row count, null count, distinct count, average row size, and
// histogram). If all models are good fits (i.e. have R² >= minRequiredFit) then
// we use them to predict the value of each statistic at the given time. If any
// model except the histogram model is a poor fit (i.e. has R² < minRequiredFit)
// then we return an error instead of a forecast. If the histogram model is a
// poor fit we adjust the latest observed histogram to match predicted values.
//
// forecastColumnStatistics is deterministic: given the same observations and
// forecast time, it will return the same forecast.
func forecastColumnStatistics(
	ctx context.Context, observed []*TableStatistic, at time.Time, minRequiredFit float64,
) (forecast *TableStatistic, err error) {
	if len(observed) < minObservationsForForecast {
		return nil, errors.New("not enough observations to forecast statistics")
	}

	forecastAt := float64(at.Unix())
	tableID := observed[0].TableID
	columnIDs := observed[0].ColumnIDs

	// Gather inputs for our regression models.
	createdAts := make([]float64, len(observed))
	rowCounts := make([]float64, len(observed))
	nullCounts := make([]float64, len(observed))
	// For distinct counts and avg sizes, we skip over empty table stats and
	// only-null stats to avoid skew.
	nonEmptyCreatedAts := make([]float64, 0, len(observed))
	distinctCounts := make([]float64, 0, len(observed))
	avgSizes := make([]float64, 0, len(observed))
	for i, stat := range observed {
		// Guard against multiple observations with the same collection time, to
		// avoid skewing the regression models.
		if i > 0 && observed[i].CreatedAt.Equal(observed[i-1].CreatedAt) {
			return nil, errors.Newf(
				"multiple observations with the same collection time %v", observed[i].CreatedAt,
			)
		}
		createdAts[i] = float64(stat.CreatedAt.Unix())
		rowCounts[i] = float64(stat.RowCount)
		nullCounts[i] = float64(stat.NullCount)
		if stat.RowCount-stat.NullCount > 0 {
			nonEmptyCreatedAts = append(nonEmptyCreatedAts, float64(stat.CreatedAt.Unix()))
			distinctCounts = append(distinctCounts, float64(stat.DistinctCount))
			avgSizes = append(avgSizes, float64(stat.AvgSize))
		}
	}

	// predict tries to predict the value of the given statistic at forecast time.
	predict := func(name redact.SafeString, y, createdAts []float64) (float64, error) {
		yₙ, r2 := float64SimpleLinearRegression(createdAts, y, forecastAt)
		log.VEventf(
			ctx, 3, "forecast for table %v columns %v predicted %s %v R² %v",
			tableID, columnIDs, name, yₙ, r2,
		)
		if r2 < minRequiredFit {
			return yₙ, errors.Newf(
				"predicted %v R² %v below min required R² %v", name, r2, minRequiredFit,
			)
		}
		// Clamp the predicted value to [0, MaxInt64] and round to nearest integer.
		if yₙ < 0 {
			return 0, nil
		}
		if yₙ > math.MaxInt64 {
			return math.MaxInt64, nil
		}
		return math.Round(yₙ), nil
	}

	rowCount, err := predict("RowCount", rowCounts, createdAts)
	if err != nil {
		return nil, err
	}
	nullCount, err := predict("NullCount", nullCounts, createdAts)
	if err != nil {
		return nil, err
	}
	var distinctCount, avgSize float64
	if len(nonEmptyCreatedAts) > 0 {
		distinctCount, err = predict("DistinctCount", distinctCounts, nonEmptyCreatedAts)
		if err != nil {
			return nil, err
		}
		avgSize, err = predict("AvgSize", avgSizes, nonEmptyCreatedAts)
		if err != nil {
			return nil, err
		}
	}

	// Adjust predicted statistics for consistency.
	if nullCount > rowCount {
		nullCount = rowCount
	}
	nonNullRowCount := rowCount - nullCount

	minDistinctCount := float64(0)
	maxDistinctCount := nonNullRowCount
	if nonNullRowCount > 0 {
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
	nonNullDistinctCount := distinctCount
	if nullCount > 0 {
		nonNullDistinctCount--
	}

	forecast = &TableStatistic{
		TableStatisticProto: TableStatisticProto{
			TableID:       tableID,
			StatisticID:   0, // TODO(michae2): Add support for SHOW HISTOGRAM.
			Name:          jobspb.ForecastStatsName,
			ColumnIDs:     columnIDs,
			CreatedAt:     at,
			RowCount:      uint64(rowCount),
			DistinctCount: uint64(distinctCount),
			NullCount:     uint64(nullCount),
			AvgSize:       uint64(avgSize),
		},
	}

	// Try to predict a histogram if there was one in the latest observed
	// stats. If we cannot predict a histogram, we will use the latest observed
	// histogram. NOTE: If any of the observed histograms were for inverted
	// indexes this will produce an incorrect histogram.
	if observed[0].HistogramData != nil && observed[0].HistogramData.ColumnType != nil {
		hist, err := predictHistogram(ctx, observed, forecastAt, minRequiredFit, nonNullRowCount)
		if err != nil {
			// If we did not successfully predict a histogram then copy the latest
			// histogram so we can adjust it.
			log.VEventf(
				ctx, 3, "forecast for table %v columns %v: could not predict histogram due to: %v",
				tableID, columnIDs, err,
			)
			hist.buckets = append([]cat.HistogramBucket{}, observed[0].nonNullHistogram().buckets...)
		}

		// Now adjust for consistency. We don't use any session data for operations
		// on upper bounds, so a nil *eval.Context works as our tree.CompareContext.
		var compareCtx *eval.Context
		hist.adjustCounts(compareCtx, observed[0].HistogramData.ColumnType, nonNullRowCount, nonNullDistinctCount)

		// Finally, convert back to HistogramData.
		histData, err := hist.toHistogramData(observed[0].HistogramData.ColumnType)
		if err != nil {
			return nil, err
		}
		forecast.HistogramData = &histData
		forecast.setHistogramBuckets(hist)
	}

	return forecast, nil
}

// predictHistogram tries to predict the histogram at forecast time.
func predictHistogram(
	ctx context.Context,
	observed []*TableStatistic,
	forecastAt float64,
	minRequiredFit float64,
	nonNullRowCount float64,
) (histogram, error) {
	if observed[0].HistogramData == nil || observed[0].HistogramData.ColumnType == nil {
		return histogram{}, errors.New("latest observed stat missing histogram")
	}

	// Empty table case.
	if nonNullRowCount < 1 {
		return histogram{buckets: make([]cat.HistogramBucket, 0)}, nil
	}

	tableID := observed[0].TableID
	columnIDs := observed[0].ColumnIDs
	colType := observed[0].HistogramData.ColumnType

	// Convert histograms to quantile functions. We don't need every observation
	// to have a histogram, but we do need at least minObservationsForForecast
	// histograms.
	createdAts := make([]float64, 0, len(observed))
	quantiles := make([]quantile, 0, len(observed))
	for _, stat := range observed {
		if stat.HistogramData == nil {
			continue
		}
		if stat.HistogramData.ColumnType == nil || !stat.HistogramData.ColumnType.Equivalent(colType) {
			continue
		}
		if !canMakeQuantile(stat.HistogramData.Version, stat.HistogramData.ColumnType) {
			continue
		}
		// Skip empty table stats and only-null stats to avoid skew.
		if stat.RowCount-stat.NullCount < 1 {
			continue
		}
		q, err := makeQuantile(stat.nonNullHistogram(), float64(stat.RowCount-stat.NullCount))
		if err != nil {
			return histogram{}, err
		}
		createdAts = append(createdAts, float64(stat.CreatedAt.Unix()))
		quantiles = append(quantiles, q)
	}

	if len(quantiles) < minObservationsForForecast {
		return histogram{}, errors.New("not enough observations to forecast histogram")
	}

	// Construct a linear regression model of quantile functions over time, and
	// use it to predict a quantile function at the given time.
	yₙ, r2 := quantileSimpleLinearRegression(createdAts, quantiles, forecastAt)
	yₙ = yₙ.fixMalformed()
	log.VEventf(
		ctx, 3, "forecast for table %v columns %v predicted quantile %v R² %v",
		tableID, columnIDs, yₙ, r2,
	)
	if r2 < minRequiredFit {
		return histogram{}, errors.Newf(
			"predicted histogram R² %v below min required R² %v", r2, minRequiredFit,
		)
	}

	// Finally, convert the predicted quantile function back to a histogram.
	return yₙ.toHistogram(colType, nonNullRowCount)
}
