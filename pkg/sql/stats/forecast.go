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

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
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

// makeQuantile and quantile.toHistogramData will need to be changed if we
// introduce a new histogram version.
const _ uint = 1 - uint(histVersion)

// A quantile function describes a probability distribution as a function from p
// to v, where p is the probability that the value will be <= v. It is the
// inverse of a cumulative distribution function. We use quantile functions
// within our modeling for a few reasons:
// * Unlike histograms, quantile functions are independent of the absolute
//   counts. They are a "shape" not a "size".
// * The math is easier over quantile functions than over cumulative
//   distribution functions. For example, we can take the definite integral of
//   quantile functions, because they always run from (0,1].
//
// Type quantile represents a piecewise quantile function as a series of points
// from p=0 (exclusive) to p=1 (inclusive). A well-formed quantile is
// non-decreasing in both p and v. The first point must have p=0, and the last
// point must have p=1. The pieces of the quantile function are line segments
// between subsequent points (exclusive and inclusive, respectively).
//
// Subsequent points may have the same p (a vertical line, or discontinuity),
// meaning the probability of finding a value > v₁ and <= v₂ is zero. Subsequent
// points may have the same v (a horizontal line), meaning the probability of
// finding exactly that v is p₂ - p₁. To put it in terms of our histograms:
// NumRange = 0 becomes a vertical line, NumRange > 0 becomes a slanted line,
// NumEq = 0 goes away, and NumEq > 0 becomes a horizontal line.
type quantile []quantilePoint

// Type quantilePoint represents one endpoint of a piece (line segment) in a
// piecewise quantile function.
type quantilePoint struct {
	p, v float64
}

// Type quantileIndex represents the ordinal position of a quantilePoint within
// a quantile.
type quantileIndex = int

// The zeroQuantile is what we use for empty tables.
var zeroQuantile = quantile{{p: 0, v: 0}, {p: 1, v: 0}}

// ForecastTableStatistics produces a forecast of the next statistics for a
// table, given the observed statistics. If a forecast cannot be produced (or is
// not worth using) an error is returned. ForecastTableStatistics is
// deterministic, given the same observations.
func ForecastTableStatistics(
	ctx context.Context, observed []*TableStatisticProto,
) ([]*TableStatisticProto, error) {
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
	observedByCols := make(map[string][]*TableStatisticProto)
	for _, stats := range observed {
		cols := tableStatisticProtoColumnsString(stats)
		if stats.CreatedAt.Equal(latest) {
			forecastCols = append(forecastCols, cols)
		}
		observedByCols[cols] = append(observedByCols[cols], stats)
	}

	// To produce a forecast we need all the latest columnsets to be present in
	// the most recent N observed statistics, where N >=
	// minObservationsForForecast. So we sort by CreatedAt descending.
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
	at := latest.Add(avgStatTime(observedCol0[:n]))

	// If any columnset has a valuable forecast, then we must also use forecasts
	// for the rest of the columnsets in the table (even if those forecasts are
	// mostly copies of the latest observed statistics). So we forecast statistics
	// for all columnsets, and then figure out if any of them were valuable
	// afterward. Forecasted RowCounts will be the same for all columnsets
	// assuming that all observations have the same RowCounts, by virtue of
	// passing the same minGoodnessOfFit each time.
	var totalScore float64
	forecasts := make([]*TableStatisticProto, len(forecastCols))
	for i := range forecastCols {
		observedCol := observedByCols[forecastCols[i]]
		forecast, score := forecastColumnStatistics(ctx, observedCol, at, n, minGoodnessOfFit)
		forecasts[i] = forecast
		totalScore += score
	}
	if totalScore < minForecastScore {
		return nil, errors.Errorf("forecast not worth using")
	}
	return forecasts, nil
}

func tableStatisticProtoColumnsString(stat *TableStatisticProto) string {
	cols := strconv.FormatUint(uint64(stat.ColumnIDs[0]), 10)
	for i := 1; i < len(stat.ColumnIDs); i++ {
		cols += " " + strconv.FormatUint(uint64(stat.ColumnIDs[i]), 10)
	}
	return cols
}

// forecastColumnStatistics produces a statistics forecast for a columnset at
// the given time, based on the observed statistics. Parts of the forecast with
// R² (goodness of fit) < minRequiredFit use the equivalent part from the latest
// observed statistics instead of the prediction. A score is also returned,
// indicating how valuable we think the forecast is. If every part of the
// forecast is copied from the latest observed statistics instead of predicted,
// the score will be 0.
func forecastColumnStatistics(
	ctx context.Context, observed []*TableStatisticProto, at time.Time, n int, minRequiredFit float64,
) (forecast *TableStatisticProto, score float64) {
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

	rowCount, r2 := scalarLinearRegression(createdAts, rowCounts, forecastAt)
	log.VEventf(
		ctx, 2, "forecast for table %v columns %v rowCount %v r2 %v",
		tableID, columnIDs, rowCount, r2,
	)
	if r2 < minRequiredFit {
		rowCount = float64(observed[0].RowCount)
	} else {
		rowCount = math.Round(rowCount)
	}
	if rowCount < 0 {
		rowCount = 0
	}

	distinctCount, r2 := scalarLinearRegression(createdAts, distinctCounts, forecastAt)
	log.VEventf(
		ctx, 2, "forecast for table %v columns %v distinctCount %v r2 %v",
		tableID, columnIDs, distinctCount, r2,
	)
	if r2 < minRequiredFit {
		distinctCount = float64(observed[0].DistinctCount)
	} else {
		distinctCount = math.Round(distinctCount)
	}
	if distinctCount < 0 {
		distinctCount = 0
	}
	if distinctCount > rowCount {
		distinctCount = rowCount
	}

	nullCount, r2 := scalarLinearRegression(createdAts, nullCounts, forecastAt)
	log.VEventf(
		ctx, 2, "forecast for table %v columns %v nullCount %v r2 %v",
		tableID, columnIDs, nullCount, r2,
	)
	if r2 < minRequiredFit {
		nullCount = float64(observed[0].NullCount)
	} else {
		nullCount = math.Round(nullCount)
	}
	if nullCount < 0 {
		nullCount = 0
	}
	if nullCount > rowCount {
		nullCount = rowCount
	}

	avgSize, r2 := scalarLinearRegression(createdAts, avgSizes, forecastAt)
	log.VEventf(
		ctx, 2, "forecast for table %v columns %v avgSize %v r2 %v",
		tableID, columnIDs, avgSize, r2,
	)
	if r2 < minRequiredFit {
		avgSize = float64(observed[0].AvgSize)
	} else {
		avgSize = math.Round(avgSize)
	}
	if avgSize < 0 {
		avgSize = 0
	}

	histogram := observed[0].HistogramData
	if histogram != nil {
		colType := histogram.ColumnType
		var quantiles []quantile
		for i := 0; i < n; i++ {
			if observed[i].HistogramData == nil {
				break
			}
			if !observed[i].HistogramData.ColumnType.Equivalent(colType) {
				break
			}
			q, err := makeQuantile(observed[i].HistogramData, rowCounts[i])
			if err != nil {
				log.VEventf(
					ctx, 2, "forecast for table %v columns %v makeQuantile(histogram at %v) err %v",
					tableID, columnIDs, observed[i].CreatedAt, err,
				)
				break
			}
			if q == nil {
				break
			}
			quantiles = append(quantiles, q)
		}
		if len(quantiles) == n {
			quantile, r2 := quantileLinearRegression(createdAts, quantiles, forecastAt)
			log.VEventf(
				ctx, 2, "forecast for table %v columns %v quantile %v r2 %v",
				tableID, columnIDs, quantile, r2,
			)
			if r2 >= minRequiredFit {
				hist, err := quantile.toHistogramData(colType, rowCount, distinctCount)
				if err != nil {
					log.VErrEventf(
						ctx, 2, "forecast for table %v columns %v toHistogramData err %v",
						tableID, columnIDs, err,
					)
				} else {
					histogram = &hist
					score += 1
				}
			}
		}
	}

	forecast = &TableStatisticProto{
		TableID:       tableID,
		StatisticID:   0, // TODO(michae2): figure out whether this needs to be unique
		Name:          jobspb.ForecastStatsName,
		ColumnIDs:     columnIDs,
		CreatedAt:     at,
		RowCount:      uint64(rowCount),
		DistinctCount: uint64(distinctCount),
		NullCount:     uint64(nullCount),
		HistogramData: histogram,
		AvgSize:       uint64(avgSize),
	}
	return
}

// Given a set of observations of x and y, scalarLinearRegression fits a simple
// linear model y = α + βx to the observations, and uses this model to
// predict ŷₙ given an xₙ. The R² (goodness of fit) measurement of the model is
// also returned.
func scalarLinearRegression(x, y []float64, xₙ float64) (yₙ, r2 float64) {
	// We use the ordinary least squares method to find the best fitting
	// model. This allows us to solve for α̂ and β̂ directly:
	//
	//   α̂ = y̅ - β̂x̅
	//
	//       ∑ (xᵢ - x̅) (yᵢ - y̅)
	//   β̂ = ---------------------
	//          ∑ (xᵢ - x̅)²
	//
	//            ∑ (yᵢ - ŷᵢ)²
	//   R² = 1 - ------------
	//            ∑ (yᵢ - y̅)²
	//
	// where:
	// * all ∑ are from i=0 to i=n-1
	// * x̅ = (1/n) ∑ xᵢ
	// * y̅ = (1/n) ∑ yᵢ
	// * ŷᵢ = α̂ + β̂xᵢ

	n := len(x)

	// Calculate x̅ and y̅ (means of x and y).
	var Σx, Σy float64
	for i := 0; i < n; i++ {
		Σx += x[i]
		Σy += y[i]
	}
	xM, yM := Σx/float64(n), Σy/float64(n)

	// Estimate α and β given our observations.
	var Σxx, Σxy, Σyy float64
	for i := 0; i < n; i++ {
		xD, yD := x[i]-xM, y[i]-yM
		Σxx += xD * xD
		Σxy += xD * yD
		Σyy += yD * yD
	}
	var α, β float64
	// Σxx could be zero if our x's are all the same. In this case Σxy is also
	// zero, and more generally y is not dependent on x, so β̂ is zero.
	if Σxx != 0 {
		β = Σxy / Σxx
	}
	α = yM - β*xM

	// Now use the model to calculate R² and predict ŷₙ given xₙ.
	var Σεε float64
	for i := 0; i < n; i++ {
		yHat := α + β*x[i]
		ε := y[i] - yHat
		Σεε += ε * ε
	}
	yₙ = α + β*xₙ
	// Σyy could be zero if our y's are all the same. In this case we will have a
	// perfect fit and Σεε will also be zero.
	if Σyy == 0 {
		r2 = 1
	} else {
		r2 = 1 - Σεε/Σyy
	}
	return
}

// Given a set of observations of x and y(t), quantileLinearRegression fits a
// linear model y(t) = α(t) + β(t)x to the observations, and uses this model to
// predict ŷₙ(t) given an xₙ. The R² (goodness of fit) measurement of the model
// is also returned.
func quantileLinearRegression(x []float64, y []quantile, xₙ float64) (yₙ quantile, r2 float64) {
	// We use the ordinary least squares method to find the best fitting
	// model. This allows us to solve for α̂(t) and β̂(t) directly. The equations
	// are similar to those used in scalar least squares, but with definite
	// integrals in a few places to turn quantile functions into scalars:
	//
	//   α̂(t) = y̅(t) - β̂(t)x̅
	//
	//          ∑ (xᵢ - x̅) (yᵢ(t) - y̅(t))
	//   β̂(t) = ---------------------
	//             ∑ (xᵢ - x̅)²
	//
	//            ∑ ∫ (yᵢ(t) - ŷᵢ(t))² dt
	//   R² = 1 - -----------------------
	//            ∑ ∫ (yᵢ(t) - y̅(t))² dt
	//
	// where:
	// * all ∑ are from i=0 to i=n-1
	// * all ∫ are definite integrals w.r.t. t from 0 to 1
	// * x̅ = (1/n) ∑ xᵢ
	// * y̅(t) = (1/n) ∑ yᵢ(t)
	// * ŷᵢ(t) = α̂(t) + β̂(t)xᵢ
	//
	// This approach comes from "Ordinary Least Squares for Histogram Data Based
	// on Wasserstein Distance" by Verde and Irpino, 2010. We use a slightly
	// different model than Verde and Irpino to accommodate scalar x's, but the
	// method of using quantile functions and Wasserstein Distance is the same.

	n := len(x)

	// Calculate x̅ and y̅(t) (means of x and y).
	var Σx float64
	var Σy quantile = zeroQuantile
	for i := 0; i < n; i++ {
		Σx += x[i]
		Σy = Σy.add(y[i])
	}
	xM, yM := Σx/float64(n), Σy.scale(1/float64(n))

	// Estimate α(t) and β(t) given our observations.
	var Σxx, Σyy float64
	var Σxy quantile = zeroQuantile
	for i := 0; i < n; i++ {
		xD, yD := x[i]-xM, y[i].sub(yM)
		Σxx += xD * xD
		Σxy = Σxy.add(yD.scale(xD))
		Σyy += yD.integrateSquared()
	}
	var α quantile = zeroQuantile
	var β quantile = zeroQuantile
	// Σxx could be zero if our x's are all the same. In this case Σxy is the zero
	// quantile, and more generally y(t) is not dependent on x, so β̂(t) is the
	// zero quantile.
	if Σxx != 0 {
		β = Σxy.scale(1 / Σxx)
	}
	α = yM.sub(β.scale(xM))
	// Note that at this point α̂(t) and β̂(t) could be malformed quantiles, which
	// is ok. We'll fix all the outputs ŷᵢ(t) to not be malformed.

	// Now use the model to calculate R² and predict ŷₙ(t) given xₙ.
	var Σεε float64
	for i := 0; i < n; i++ {
		yHat := α.add(β.scale(x[i])).fix()
		ε := y[i].sub(yHat)
		Σεε += ε.integrateSquared()
	}
	yₙ = α.add(β.scale(xₙ)).fix()
	// Σyy could be zero if our y(t)'s are all the same. In this case we will have a
	// perfect fit, and Σεε will also be zero.
	if Σyy == 0 {
		r2 = 1
	} else {
		r2 = 1 - Σεε/Σyy
	}
	return
}

// makeQuantile converts a histogram into a quantile function, or returns an
// error if it cannot.
func makeQuantile(histogram *HistogramData, rowCount float64) (quantile, error) {
	if histogram == nil {
		return nil, nil
	}
	if histogram.Version > 1 {
		return nil, errors.Errorf("cannot make quantile for histogram version %d", histogram.Version)
	}

	// We only create quantiles for types that make sense converted to float64.
	switch histogram.ColumnType.Family() {
	case types.IntFamily,
		types.FloatFamily,
		types.DecimalFamily,
		types.DateFamily,
		types.TimeFamily,
		types.TimeTZFamily,
		types.TimestampFamily,
		types.TimestampTZFamily,
		types.IntervalFamily:
		break
	default:
		return nil, errors.Errorf("cannot make quantile for type %s", histogram.ColumnType.Name())
	}

	// Empty table cases.
	if histogram.Buckets == nil || len(histogram.Buckets) == 0 || rowCount < 1 {
		return zeroQuantile, nil
	}

	// To produce a quantile with an exclusive first point at p=0 we need the
	// first bucket to have NumRange == 0.
	if histogram.Buckets[0].NumRange != 0 {
		return nil, errors.AssertionFailedf("histogram with non-zero NumRange in first bucket")
	}

	var a tree.DatumAlloc
	var prevVal float64 = math.Inf(-1)
	var qLen quantileIndex
	var p float64
	var q quantile

	for i := range histogram.Buckets {
		if histogram.Buckets[i].NumRange < 0 || histogram.Buckets[i].NumEq < 0 {
			return nil, errors.AssertionFailedf("histogram bucket with negative row count")
		}
		val, err := quantileValue(&a, histogram.ColumnType, histogram.Buckets[i].UpperBound)
		if err != nil {
			return nil, err
		}
		if val < prevVal {
			return nil, errors.AssertionFailedf("decreasing quantile values")
		}
		prevVal = val

		p += float64(histogram.Buckets[i].NumRange) / rowCount
		// Fix any floating point errors or histogram errors (e.g. sum of bucket row
		// counts > total row count) causing p to go over 1.0.
		if p > 1 {
			p = 1
		}
		q = append(q, quantilePoint{p: p, v: val})
		if histogram.Buckets[i].NumRange > 0 {
			qLen = len(q)
		}

		if histogram.Buckets[i].NumEq == 0 {
			// Small optimization: skip adding a duplicate point.
			continue
		}
		p += float64(histogram.Buckets[i].NumEq) / rowCount
		if p > 1 {
			p = 1
		}
		q = append(q, quantilePoint{p: p, v: val})
		qLen = len(q)
	}

	if qLen == 0 {
		// In the unlikely case that every bucket had zero rows we should simply
		// return the zeroQuantile.
		q = zeroQuantile
	} else {
		// Fix any floating point errors or histogram errors causing p to go under 1.0
		// at the end of the quantile function. Also chop off any zero-row buckets at
		// the end.
		q[qLen-1].p = 1
		q = q[:qLen]
	}
	return q, nil
}

// toHistogramData converts a quantile into a histogram, using the provided type
// and row count. It returns an error if the conversion fails.
func (q quantile) toHistogramData(
	colType *types.T, rowCount, distinctCount float64,
) (HistogramData, error) {
	var h histogram
	if rowCount < 1 {
		return h.toHistogramData(colType)
	}

	// The first bucket must always have NumRange = 0. Sometimes we'll emit a
	// bucket that also has NumEq = 0 to make this true.
	prevUpper, err := histogramValue(colType, q[0].v)
	if err != nil {
		return HistogramData{}, err
	}
	h.buckets = append(h.buckets, cat.HistogramBucket{
		NumEq:         0,
		NumRange:      0,
		DistinctRange: 0,
		UpperBound:    prevUpper,
	})

	var eqP float64

	for i := 1; i < len(q); i++ {
		upper, err := histogramValue(colType, q[i].v)
		if err != nil {
			return HistogramData{}, err
		}
		cmp, err := upper.CompareError(nil, prevUpper)
		if err != nil {
			return HistogramData{}, err
		}
		if cmp < 0 {
			return HistogramData{}, errors.AssertionFailedf("decreasing histogram values")
		}
		if cmp == 0 {
			eqP += q[i].p - q[i-1].p
		} else {
			// Close the current bucket.
			numEq := eqP * rowCount
			if math.IsNaN(numEq) || math.IsInf(numEq, 0) {
				return HistogramData{}, errors.AssertionFailedf("invalid histogram NumEq: %v", numEq)
			}
			if numEq < 1 && h.buckets[len(h.buckets)-1].NumRange+numEq >= 1 {
				// Steal from NumRange so that NumEq is at least 1. This makes the
				// histogram look more like something EquiDepthHistogram would produce.
				h.buckets[len(h.buckets)-1].NumRange -= 1 - numEq
				numEq = 1
			}
			h.buckets[len(h.buckets)-1].NumEq = numEq
			eqP = 0

			// Start a new bucket.
			lessP := q[i].p - q[i-1].p
			numRange := lessP * rowCount
			if math.IsNaN(numRange) || math.IsInf(numRange, 0) {
				return HistogramData{}, errors.AssertionFailedf("invalid histogram NumRange: %v", numRange)
			}
			lowerBound := getNextLowerBound(nil, prevUpper)
			distinctRange := estimatedDistinctValuesInRange(nil, numRange, lowerBound, upper)
			if math.IsNaN(distinctRange) || math.IsInf(distinctRange, 0) {
				return HistogramData{}, errors.AssertionFailedf(
					"invalid histogram DistinctRange: %v", distinctRange,
				)
			}
			h.buckets = append(h.buckets, cat.HistogramBucket{
				NumEq:         0,
				NumRange:      numRange,
				DistinctRange: distinctRange,
				UpperBound:    upper,
			})
			prevUpper = upper
		}
		// Skip any extra p=1 points instead of emitting trailing zero buckets.
		if q[i].p == 1 {
			break
		}
	}

	// Close the last bucket.
	numEq := eqP * rowCount
	if math.IsNaN(numEq) || math.IsInf(numEq, 0) {
		return HistogramData{}, errors.AssertionFailedf("invalid histogram NumEq: %v", numEq)
	}
	if numEq < 1 && h.buckets[len(h.buckets)-1].NumRange+numEq >= 1 {
		// Steal from NumRange so that NumEq is at least 1. This makes the
		// histogram look more like something EquiDepthHistogram would produce.
		h.buckets[len(h.buckets)-1].NumRange -= 1 - numEq
		numEq = 1
	}
	h.buckets[len(h.buckets)-1].NumEq = numEq

	h.adjustCounts(nil, rowCount, distinctCount)
	return h.toHistogramData(colType)
}

// quantileValue converts from an encoded datum to a float suitable for use in a
// quantile function. This is a little different from tree.PerformCast. We allow
// some conversions that are not legal casts (e.g. TIME to FLOAT) and we error
// on NaN or infinite values because they will break our model.
func quantileValue(a *tree.DatumAlloc, colType *types.T, encoded []byte) (float64, error) {
	d, _, err := keyside.Decode(a, colType, encoded, encoding.Ascending)
	if err != nil {
		return 0, err
	}
	switch v := d.(type) {
	case *tree.DInt:
		return float64(*v), nil
	case *tree.DFloat:
		if math.IsNaN(float64(*v)) || math.IsInf(float64(*v), 0) {
			return 0, tree.ErrFloatOutOfRange
		} else {
			return float64(*v), nil
		}
	case *tree.DDecimal:
		if v.Form != apd.Finite {
			return 0, tree.ErrFloatOutOfRange
		} else {
			return v.Float64()
		}
	case *tree.DDate:
		if !v.IsFinite() {
			return 0, tree.ErrFloatOutOfRange
		} else {
			// We use PG epoch instead of Unix epoch so that we can easily clamp when
			// converting back.
			return float64(v.PGEpochDays()), nil
		}
	case *tree.DTime:
		return float64((*timeofday.TimeOfDay)(v).ToTime().UnixMicro()), nil
	case *tree.DTimeTZ:
		// These microseconds will be in UTC regardless of the stored timezone.
		return float64(v.ToTime().UnixMicro()), nil
	case *tree.DTimestamp:
		if v.Equal(pgdate.TimeInfinity) || v.Equal(pgdate.TimeNegativeInfinity) {
			return 0, tree.ErrFloatOutOfRange
		}
		return float64(v.UnixMicro()), nil
	case *tree.DTimestampTZ:
		if v.Equal(pgdate.TimeInfinity) || v.Equal(pgdate.TimeNegativeInfinity) {
			return 0, tree.ErrFloatOutOfRange
		}
		// These microseconds will be in UTC (TimestampTZ is always stored in UTC).
		return float64(v.UnixMicro()), nil
	case *tree.DInterval:
		return v.AsFloat64(), nil
	default:
		return 0, errors.Errorf("cannot make quantile value from type %s", colType.Name())
	}
}

// histogramValue converts from a quantile value back to a datum suitable for
// use in a histogram. In general this is more permissive than
// tree.PerformCast. We allow some conversions that are not legal casts
// (e.g. FLOAT to TIME). On overflow and underflow we prefer to clamp values
// rather than fail the conversion (and thus the entire histogram).
func histogramValue(colType *types.T, val float64) (tree.Datum, error) {
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return nil, tree.ErrFloatOutOfRange
	}
	switch colType.Family() {
	case types.IntFamily:
		i := math.Round(val)
		// Clamp instead of truncating.
		switch colType.Width() {
		case 16:
			if i <= math.MinInt16 {
				return tree.NewDInt(tree.DInt(math.MinInt16)), nil
			}
			if i >= math.MaxInt16 {
				return tree.NewDInt(tree.DInt(math.MaxInt16)), nil
			}
		case 32:
			if i <= math.MinInt32 {
				return tree.NewDInt(tree.DInt(math.MinInt32)), nil
			}
			if i >= math.MaxInt16 {
				return tree.NewDInt(tree.DInt(math.MaxInt32)), nil
			}
		default:
			if i <= math.MinInt64 {
				return tree.NewDInt(tree.DInt(math.MinInt64)), nil
			}
			if i >= math.MaxInt64 {
				return tree.NewDInt(tree.DInt(math.MaxInt64)), nil
			}
		}
		return tree.NewDInt(tree.DInt(i)), nil
	case types.FloatFamily:
		if colType.Width() == 32 {
			val = float64(float32(val))
		}
		return tree.NewDFloat(tree.DFloat(val)), nil
	case types.DecimalFamily:
		var dd tree.DDecimal
		_, err := dd.SetFloat64(val)
		if err != nil {
			return nil, err
		}
		// Clamp instead of erring.
		dMin, err := tree.MinDecimalForWidth(int(colType.Precision()), int(colType.Scale()))
		if err != nil {
			return nil, err
		}
		if dd.Cmp(dMin) <= 0 {
			dd.Set(dMin)
			return &dd, nil
		}
		dMax, err := tree.MaxDecimalForWidth(int(colType.Precision()), int(colType.Scale()))
		if err != nil {
			return nil, err
		}
		if dd.Cmp(dMax) >= 0 {
			dd.Set(dMax)
			return &dd, nil
		}
		err = tree.LimitDecimalWidth(&dd.Decimal, int(colType.Precision()), int(colType.Scale()))
		if err != nil {
			return nil, err
		}
		return &dd, nil
	case types.DateFamily:
		days := math.Round(val)
		if days <= math.MinInt32 {
			days = math.MinInt32
		} else if days >= math.MaxInt32 {
			days = math.MaxInt32
		}
		return tree.NewDDate(pgdate.MakeDateFromPGEpochClamp(int32(days))), nil
	case types.TimeFamily:
		micros := math.Round(val)
		// This does a modulo instead of a clamp, but I think intuitively that makes
		// sense for something cyclical like time of day.
		// TODO(michae2): We need to do the modulo before fixing the quantile, not
		// here. If it happens here it will simply fail in quantile.toHistogramData.
		// (Clamp doesn't have this problem.)
		t := timeofday.FromFloat(micros)
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(colType.Precision())
		return tree.MakeDTime(t.Round(roundTo)), nil
	case types.TimeTZFamily:
		micros := math.Round(val)
		// Same comment about modulo instead of clamp.
		// TODO(michae2): Same comment about modulo before fixing the quantile.
		t := timeofday.FromFloat(micros)
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(colType.Precision())
		// Offset 0 means the TimeTZ will always have UTC as the timestamp.
		return tree.NewDTimeTZFromOffset(t.Round(roundTo), 0), nil
	case types.TimestampFamily:
		micros := int64(math.Round(val))
		t := time.UnixMicro(micros).UTC()
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(colType.Precision())
		// TODO(michae2): This clamping is broken (it clamps to infinities).
		return tree.MakeDTimestampClamp(t, roundTo), nil
	case types.TimestampTZFamily:
		micros := int64(math.Round(val))
		t := time.UnixMicro(micros).UTC()
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(colType.Precision())
		// TODO(michae2): This clamping is broken (it clamps to infinities).
		return tree.MakeDTimestampTZClamp(t, roundTo), nil
	case types.IntervalFamily:
		itm, err := colType.IntervalTypeMetadata()
		if err != nil {
			return nil, err
		}
		return tree.NewDInterval(duration.FromFloat64(val), itm), nil
	default:
		return nil, errors.Errorf("cannot convert quantile value to type %s", colType.Name())
	}
}

// Each continuous piece of the quantile function is a line segment between two
// points, which can be represented with the equation: v = mp + b. This must
// not be called on a discontinuity (vertical line where p1 = p2).
func quantilePiece(c, d quantilePoint) (m, b float64) {
	m = (d.v - c.v) / (d.p - c.p)
	b = c.v - m*c.p
	return
}

// Create a new quantile function which represents the sum of q and r. The new
// quantile function will usually have more points than either q or r, depending
// on how their points align. The new quantile function might be malformed if
// either q or r were malformed.
func (q quantile) add(r quantile) quantile {
	sum := make(quantile, 0, len(q)+len(r))
	// Both q and r must have p=0 and p=1 as their first and last points,
	// respectively, so we always start with p=0 at the first point of both
	// functions, and end with the == case at the last point of both functions. We
	// are guaranteed to not call quantilePiece over any dicontinuities because at
	// a discontinuity, the discontinuous quantile function will always have p <=
	// the other function's p.
	var i, j quantileIndex
	sum = append(sum, quantilePoint{p: q[i].p, v: q[i].v + r[j].v})
	for i, j = 1, 1; i < len(q) && j < len(r); {
		if q[i].p < r[j].p {
			// Find the value of r within the line segment at q[i].p.
			m, b := quantilePiece(r[j-1], r[j])
			rv := m*q[i].p + b
			sum = append(sum, quantilePoint{p: q[i].p, v: q[i].v + rv})
			i++
		} else if r[j].p < q[i].p {
			// Find the value of q within the line segment at r[j].p.
			m, b := quantilePiece(q[i-1], q[i])
			qv := m*r[j].p + b
			sum = append(sum, quantilePoint{p: r[j].p, v: qv + r[j].v})
			j++
		} else {
			sum = append(sum, quantilePoint{p: q[i].p, v: q[i].v + r[j].v})
			i++
			j++
		}
	}
	// Handle any trailing p=1 points.
	for ; i < len(q); i++ {
		sum = append(sum, quantilePoint{p: q[i].p, v: q[i].v + r[j-1].v})
	}
	for ; j < len(r); j++ {
		sum = append(sum, quantilePoint{p: r[j].p, v: q[i-1].v + r[j].v})
	}
	return sum
}

// Create a new quantile function which represents q minus r. The new quantile
// function will usually have more points than either q or r, depending on how
// their points align. The new quantile function might be malformed, even if
// both q and r were well-formed.
func (q quantile) sub(r quantile) quantile {
	diff := make(quantile, 0, len(q)+len(r))
	// Both q and r must have p=0 and p=1 as their first and last points,
	// respectively, so we always start with p=0 at the first point of both
	// functions, and end with the == case at the last point of both functions. We
	// are guaranteed to not call quantilePiece over any dicontinuities because at
	// a discontinuity, the discontinuous quantile function will always have p <=
	// the other function's p.
	var i, j quantileIndex
	diff = append(diff, quantilePoint{p: q[i].p, v: q[i].v - r[j].v})
	for i, j = 1, 1; i < len(q) && j < len(r); {
		if q[i].p < r[j].p {
			// Find the value of r within the line segment at q[i].p.
			m, b := quantilePiece(r[j-1], r[j])
			rv := m*q[i].p + b
			diff = append(diff, quantilePoint{p: q[i].p, v: q[i].v - rv})
			i++
		} else if r[j].p < q[i].p {
			// Find the value of q within the line segment at r[j].p.
			m, b := quantilePiece(q[i-1], q[i])
			qv := m*r[j].p + b
			diff = append(diff, quantilePoint{p: r[j].p, v: qv - r[j].v})
			j++
		} else {
			diff = append(diff, quantilePoint{p: q[i].p, v: q[i].v - r[j].v})
			i++
			j++
		}
	}
	// Handle any trailing p=1 points.
	for ; i < len(q); i++ {
		diff = append(diff, quantilePoint{p: q[i].p, v: q[i].v - r[j-1].v})
	}
	for ; j < len(r); j++ {
		diff = append(diff, quantilePoint{p: r[j].p, v: q[i-1].v - r[j].v})
	}
	return diff
}

// Create a new quantile function which represents q multiplied by a
// constant. The new quantile function will be malformed if c is negative.
func (q quantile) scale(c float64) quantile {
	prod := make(quantile, len(q))
	for i := range q {
		prod[i] = quantilePoint{p: q[i].p, v: q[i].v * c}
	}
	return prod
}

// Calculate the definite integral w.r.t. p from p=0 to p=1 of the quantile
// function squared.
func (q quantile) integrateSquared() float64 {
	var area float64
	for i := 1; i < len(q); i++ {
		if q[i].p == q[i-1].p {
			// Skip over the discontinuity.
			continue
		}
		// Each continuous piece of the quantile function is a line segment
		// described by:
		//   v = mp + b
		// We're trying to integrate the square of this from p₁ to p₂:
		//   p₂
		//    ∫ (mp + b)² dp
		//   p₁
		// Which is equivalent to solving this at both p₁ and p₂:
		//    1            |p₂
		//   --- (mp + b)³ |
		//   3m            |p₁
		// In some cases m will be 0, so then we're solving:
		//   p₂
		//    ∫ b² dp
		//   p₁
		// Which is equivalent to solving this at both p₁ and p₂:
		//       |p₂
		//   b²p |
		//       |p₁
		m, b := quantilePiece(q[i-1], q[i])
		if m == 0 {
			a2 := math.Pow(b, 2) * q[i].p
			a1 := math.Pow(b, 2) * q[i-1].p
			area += a2 - a1
		} else {
			a2 := math.Pow(m*q[i].p+b, 3) / 3 / m
			a1 := math.Pow(m*q[i-1].p+b, 3) / 3 / m
			area += a2 - a1
		}
	}
	return area
}

// After a few operations we might have a malformed quantile function with
// negative slope in places. (I.e. subsequent points with decreasing v.) These
// places represent the quantile function "folding backward" over parts of the
// histogram it has already described.
//
// This function fixes malformed quantiles by "moving" p from the negative or
// overlapping parts to where it should be. No p is lost in the making of these
// calculations. The resulting quantile function is well-formed (that is,
// nondecreasing in both p and v).
func (q quantile) fix() quantile {
	// To fix a malformed quantile, we recalculate the p for each point by summing
	// the total p wherever the function <= the value of that point. We do this by
	// drawing a line across the quantile function at v = the value of that point
	// ("val"), finding all of the intersections, and adding the distances between
	// intersections. (This resembles the even-odd algorithm used to solve the
	// point-in-polygon problem.) For example, for this malformed quantile:
	//
	//   {{0, 100}, {0.25, 200}, {0.5, 100}, {0.75, 0}, {1, 100}}
	//
	// when calculating p for v = 100, we are adding these intervals (the first
	// interval has a length of 0).
	//
	//     +           +-----------+
	//
	// 200 |     *
	//     |  /     \
	// 100 *           *           *
	//     |             \      /
	//   0 + - - - - - - - - * - - -
	//     0   0.25   0.5   0.75   1

	fixed := make(quantile, 0, len(q))

	// Sort quantile points by value. We'll fix them in this order, which allows
	// for an optimization described below.
	pointsByValue := make([]quantileIndex, len(q))
	for i := range pointsByValue {
		pointsByValue[i] = i
	}
	sort.SliceStable(pointsByValue, func(i, j int) bool {
		return q[pointsByValue[i]].v < q[pointsByValue[j]].v
	})

	var prevCrossingPieces []quantileIndex

	for i := 0; i < len(pointsByValue); {
		val := q[pointsByValue[i]].v

		// Left point of pieces which have one endpoint <= val and one endpoint >
		// val.
		var crossingPieces []quantileIndex
		// Left point of flat pieces which have both endpoints = val.
		var eqPieces []quantileIndex

		// As an optimization, we do not need to check every piece to find
		// intersections with v = val. We only need to check (a) pieces that crossed
		// the previous val (prevCrossingPieces), and (b) pieces that have the
		// current val as an endpoint. A short proof: every piece that crosses the
		// current val will have one endpoint <= val and one endpoint > val. All
		// pieces with one endpoint < val and one endpoint > val also crossed the
		// previous val and are in (a). All pieces with one endpoint == val and one
		// endpoint > val have the current val as an endpoint. ∎

		// Check pieces that crossed the previous val.
		for _, qi := range prevCrossingPieces {
			// We know one endpoint <= prev val < current val, so we just need to
			// check whether one endpoint > val.
			if q[qi].v > val || q[qi+1].v > val {
				crossingPieces = append(crossingPieces, qi)
			}
		}
		// Check all pieces that have the current val as an endpoint. Note that this
		// also moves i past all points with the same val.
		for ; i < len(pointsByValue) && q[pointsByValue[i]].v == val; i++ {
			qi := pointsByValue[i]
			// We know q[qi].v == val, so we just need to check whether the points to
			// the left and right are > val.
			if qi > 0 && q[qi-1].v > val {
				crossingPieces = append(crossingPieces, qi-1)
			}
			if qi+1 < len(q) && q[qi+1].v > val {
				crossingPieces = append(crossingPieces, qi)
			}
			// Also look for flat pieces = val.
			if qi+1 < len(q) && q[qi+1].v == val {
				eqPieces = append(eqPieces, qi)
			}
		}

		sort.Ints(crossingPieces)
		prevCrossingPieces = crossingPieces

		// Find exact p values of the intersections with all crossing pieces.
		intersectionPs := make([]float64, 0, len(crossingPieces)+2)

		// If the first point in the quantile function is > val, we need to add a
		// starting intersection at 0.
		if q[0].v > val {
			intersectionPs = append(intersectionPs, 0)
		}
		for _, qj := range crossingPieces {
			if q[qj].p == q[qj+1].p || q[qj].v == val {
				// At a discontinuity we simply use the p shared by both points.
				intersectionPs = append(intersectionPs, q[qj].p)
			} else if q[qj+1].v == val {
				intersectionPs = append(intersectionPs, q[qj+1].p)
			} else {
				// Crossing pieces will never have m = 0 (flat lines) because they have
				// one point <= val and one point > val.
				m, b := quantilePiece(q[qj], q[qj+1])
				intersectionPs = append(intersectionPs, (val-b)/m)
			}
		}
		// If the last point in the quantile function is <= val, we need to add an
		// ending intersection at 1.
		if q[len(q)-1].v <= val {
			intersectionPs = append(intersectionPs, 1)
		}

		lessEqP := intersectionPs[0]
		for j := 1; j < len(intersectionPs); j += 2 {
			lessEqP += intersectionPs[j+1] - intersectionPs[j]
		}
		var eqP float64
		for _, qi := range eqPieces {
			eqP += q[qi+1].p - q[qi].p
		}
		lessP := lessEqP - eqP

		fixed = append(fixed, quantilePoint{p: lessP, v: val})
		if eqP != 0 {
			fixed = append(fixed, quantilePoint{p: lessEqP, v: val})
		}
	}
	return fixed
}
