// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sentryutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// UseStatisticsForecasts controls whether statistics forecasts are generated in
// the stats cache.
var UseStatisticsForecasts = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.forecasts.enabled",
	"when true, enables generation of statistics forecasts by default for all tables",
	true,
	settings.WithPublic)

// minObservationsForForecast is the minimum number of observed statistics
// required to produce a statistics forecast. Forecasts based on 1 or 2
// observations will always have R² = 1 (perfect goodness of fit) regardless of
// the accuracy of the forecast.
var minObservationsForForecast = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.forecasts.min_observations",
	"the mimimum number of observed statistics required to produce a statistics forecast",
	3,
	settings.WithPublic,
	settings.IntInRange(1, math.MaxInt),
)

// minGoodnessOfFit is the minimum R² (goodness of fit) measurement all
// predictive models in a forecast must have for us to use the forecast.
var minGoodnessOfFit = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"sql.stats.forecasts.min_goodness_of_fit",
	"the minimum R² (goodness of fit) measurement required from all predictive models to use a "+
		"forecast",
	0.95,
	settings.WithPublic,
	settings.Fraction,
)

// maxDecrease is the minimum ratio of a prediction to the lowest prior
// observation that we allow. Predictions falling below this will be clamped to
// the lower bound calculated from this ratio. This lower bound is needed to
// prevent forecasting zero rows for downward-trending statistics, which can
// cause bad plans when the forecast is initially used.
//
// This happens to be the same as unknownFilterSelectivity, but there's not a
// strong theoretical reason for it.
var maxDecrease = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"sql.stats.forecasts.max_decrease",
	"the most a prediction is allowed to decrease, expressed as the minimum ratio of the prediction "+
		"to the lowest prior observation",
	1.0/3.0,
	settings.WithPublic,
	settings.Fraction,
)

// TODO(michae2): Consider whether we need a corresponding maxIncrease.

// ForecastTableStatistics produces zero or more statistics forecasts based on
// the given observed statistics. The observed statistics must be ordered by
// collection time descending, with the latest observed statistics first. The
// observed statistics may be a mixture of statistics for different sets of
// columns. Partial statistics will be skipped.
//
// Whether a forecast is produced for a set of columns depends on how well the
// observed statistics for that set of columns fit a linear regression model.
// This means a forecast will not necessarily be produced for every set of
// columns in the table. Any forecasts produced will not necessarily have the
// same CreatedAt time (and could be in the past, present, or future relative to
// the current time). Any forecasts produced will not necessarily have the same
// RowCount or be consistent with the other forecasts produced. (For example,
// DistinctCount in the forecast for columns {a, b} could very well end up less
// than DistinctCount in the forecast for column {a}.)
//
// ForecastTableStatistics is deterministic: given the same observations it will
// return the same forecasts.
func ForecastTableStatistics(
	ctx context.Context, st *cluster.Settings, observed []*TableStatistic,
) []*TableStatistic {
	// Group observed statistics by column set, skipping over partial statistics
	// and statistics with inverted histograms.
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
		colKey := MakeSortedColStatKey(stat.ColumnIDs)
		obs, ok := observedByCols[colKey]
		if !ok {
			forecastCols = append(forecastCols, colKey)
		}
		observedByCols[colKey] = append(obs, stat)
	}

	if len(observedByCols) == 0 {
		// No suitable stats.
		return nil
	}
	avgRefresh := avgFullRefreshTime(observed)

	forecasts := make([]*TableStatistic, 0, len(forecastCols))
	for _, colKey := range forecastCols {
		// To make forecasts deterministic, we must choose a time to forecast at
		// based on only the observed statistics. We choose the time of the latest
		// statistics + the average time between automatic stats collections, which
		// should be roughly when the next automatic stats collection will
		// occur. This will be the same time for all columns that had the same
		// latest collection time.
		latest := observedByCols[colKey][0].CreatedAt
		at := latest.Add(avgRefresh)

		forecast, err := forecastColumnStatistics(
			ctx, st, observedByCols[colKey], at, minGoodnessOfFit.Get(&st.SV),
		)
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
	ctx context.Context,
	st *cluster.Settings,
	observed []*TableStatistic,
	at time.Time,
	minRequiredFit float64,
) (forecast *TableStatistic, err error) {
	if len(observed) < int(minObservationsForForecast.Get(&st.SV)) {
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
		// Clamp the predicted value to [lowerBound, MaxInt64] and round to nearest
		// integer. In general, it is worse to under-estimate counts than to
		// over-estimate counts, so we pick a very conservative lowerBound of the
		// prior lowest observation times maxDecrease to avoid prematurely
		// estimating zero rows for downward-trending statistics.
		lowerBound := math.Round(slices.Min(y) * maxDecrease.Get(&st.SV))
		lowerBound = math.Max(0, lowerBound)
		if yₙ < lowerBound {
			return lowerBound, nil
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
		hist, err := predictHistogram(ctx, st, observed, forecastAt, minRequiredFit, nonNullRowCount)
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
		hist.adjustCounts(ctx, compareCtx, observed[0].HistogramData.ColumnType, nonNullRowCount, nonNullDistinctCount)

		// Finally, convert back to HistogramData.
		histData, err := hist.toHistogramData(ctx, observed[0].HistogramData.ColumnType, st)
		if err != nil {
			return nil, err
		}
		forecast.HistogramData = &histData
		forecast.setHistogramBuckets(hist)

		// Verify that the first two buckets (the initial NULL bucket and the first
		// non-NULL bucket) both have NumRange=0 and DistinctRange=0. (We must check
		// this after calling setHistogramBuckets to account for rounding.) See
		// #93892.
		for _, bucket := range forecast.Histogram {
			if bucket.NumRange != 0 || bucket.DistinctRange != 0 {
				// Build a JSON representation of the first several buckets in each
				// observed histogram so that we can figure out what happened.
				const debugBucketCount = 5
				jsonStats := make([]*JSONStatistic, 0, len(observed))

				addStat := func(stat *TableStatistic) {
					jsonStat := &JSONStatistic{
						Name:          stat.Name,
						CreatedAt:     stat.CreatedAt.String(),
						Columns:       []string{strconv.FormatInt(int64(stat.ColumnIDs[0]), 10)},
						RowCount:      stat.RowCount,
						DistinctCount: stat.DistinctCount,
						NullCount:     stat.NullCount,
						AvgSize:       stat.AvgSize,
					}
					if err := jsonStat.SetHistogram(stat.HistogramData); err == nil &&
						len(jsonStat.HistogramBuckets) > debugBucketCount {
						// Limit the histogram to the first several buckets.
						jsonStat.HistogramBuckets = jsonStat.HistogramBuckets[0:debugBucketCount]
					}
					// Replace UpperBounds with a hash.
					for i := range jsonStat.HistogramBuckets {
						hash := md5.Sum([]byte(jsonStat.HistogramBuckets[i].UpperBound))
						jsonStat.HistogramBuckets[i].UpperBound = fmt.Sprintf("_%x", hash)
					}
					jsonStats = append(jsonStats, jsonStat)
				}
				addStat(forecast)
				for i := range observed {
					addStat(observed[i])
				}
				var debugging redact.SafeString
				if j, err := json.Marshal(jsonStats); err == nil {
					debugging = redact.SafeString(j)
				}
				err := errorutil.UnexpectedWithIssueErrorf(
					93892,
					"forecasted histogram had first bucket with non-zero NumRange or DistinctRange: %s",
					debugging,
				)
				sentryutil.SendReport(ctx, &st.SV, err)
				return nil, err
			}
			if bucket.UpperBound != tree.DNull {
				// Stop checking after the first non-NULL bucket.
				break
			}
		}
	}

	return forecast, nil
}

// predictHistogram tries to predict the histogram at forecast time.
func predictHistogram(
	ctx context.Context,
	st *cluster.Settings,
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

	if len(quantiles) < int(minObservationsForForecast.Get(&st.SV)) {
		return histogram{}, errors.New("not enough observations to forecast histogram")
	}

	// Construct a linear regression model of quantile functions over time, and
	// use it to predict a quantile function at the given time.
	yₙ, r2 := quantileSimpleLinearRegression(createdAts, quantiles, forecastAt)
	if yₙ.isInvalid() {
		return histogram{}, errors.Newf("predicted histogram contains overflow values")
	}
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
	return yₙ.toHistogram(ctx, colType, nonNullRowCount)
}
