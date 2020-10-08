// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testmodel

import (
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

type aggFunc func(DataSeries) float64
type fillFunc func(DataSeries, DataSeries, int64) DataSeries

// AggregateSum returns the sum value of all points in the provided data series.
func AggregateSum(data DataSeries) float64 {
	total := 0.0
	for _, dp := range data {
		total += dp.Value
	}
	return total
}

// AggregateAverage returns the average value of the points in the provided data
// series.
func AggregateAverage(data DataSeries) float64 {
	if len(data) == 0 {
		return 0.0
	}
	return AggregateSum(data) / float64(len(data))
}

// AggregateMax returns the maximum value of any point in the provided data
// series.
func AggregateMax(data DataSeries) float64 {
	max := -math.MaxFloat64
	for _, dp := range data {
		if dp.Value > max {
			max = dp.Value
		}
	}
	return max
}

// AggregateMin returns the minimum value of any point in the provided data
// series.
func AggregateMin(data DataSeries) float64 {
	min := math.MaxFloat64
	for _, dp := range data {
		if dp.Value < min {
			min = dp.Value
		}
	}
	return min
}

// AggregateFirst returns the first value in the provided data series.
func AggregateFirst(data DataSeries) float64 {
	return data[0].Value
}

// AggregateLast returns the last value in the provided data series.
func AggregateLast(data DataSeries) float64 {
	return data[len(data)-1].Value
}

// AggregateVariance returns the variance of the provided data series. The returned
// variance is the sample variance, not the population variance.
func AggregateVariance(data DataSeries) float64 {
	mean := 0.0
	meanSquaredDist := 0.0
	if len(data) < 2 {
		return 0
	}
	for i, dp := range data {
		// Welford's algorithm for computing variance.
		delta := dp.Value - mean
		mean += delta / float64(i+1)
		delta2 := dp.Value - mean
		meanSquaredDist += delta * delta2
	}
	return meanSquaredDist / float64(len(data))
}

// getAggFunction is a convenience method used to process an aggregator option
// from our time series query protobuffer format.
func getAggFunction(agg tspb.TimeSeriesQueryAggregator) aggFunc {
	switch agg {
	case tspb.TimeSeriesQueryAggregator_AVG:
		return AggregateAverage
	case tspb.TimeSeriesQueryAggregator_SUM:
		return AggregateSum
	case tspb.TimeSeriesQueryAggregator_MAX:
		return AggregateMax
	case tspb.TimeSeriesQueryAggregator_MIN:
		return AggregateMin
	case tspb.TimeSeriesQueryAggregator_FIRST:
		return AggregateFirst
	case tspb.TimeSeriesQueryAggregator_LAST:
		return AggregateLast
	case tspb.TimeSeriesQueryAggregator_VARIANCE:
		return AggregateVariance
	}

	// The model should not be called with an invalid aggregator option.
	panic(fmt.Sprintf("unknown aggregator option specified: %v", agg))
}

func fillFuncLinearInterpolate(before DataSeries, after DataSeries, resolution int64) DataSeries {
	start := before[len(before)-1]
	end := after[0]

	// compute interpolation step
	step := (end.Value - start.Value) / float64(end.TimestampNanos-start.TimestampNanos)

	result := make(DataSeries, (end.TimestampNanos-start.TimestampNanos)/resolution-1)
	for i := range result {
		result[i] = dp(
			start.TimestampNanos+(resolution*int64(i+1)),
			start.Value+(step*float64(i+1)*float64(resolution)),
		)
	}
	return result
}
