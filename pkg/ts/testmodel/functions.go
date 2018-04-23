// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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

// AggregateLast returns the last value in the provided data series.
func AggregateLast(data DataSeries) float64 {
	return data[len(data)-1].Value
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
