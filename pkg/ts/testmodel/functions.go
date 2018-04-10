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

func aggFuncSum(data DataSeries) float64 {
	total := 0.0
	for _, dp := range data {
		total += dp.value
	}
	return total
}

func aggFuncLast(data DataSeries) float64 {
	return data[len(data)-1].value
}

func aggFuncAvg(data DataSeries) float64 {
	if len(data) == 0 {
		return 0.0
	}
	return aggFuncSum(data) / float64(len(data))
}

func aggFuncMax(data DataSeries) float64 {
	max := -math.MaxFloat64
	for _, dp := range data {
		if dp.value > max {
			max = dp.value
		}
	}
	return max
}

func aggFuncMin(data DataSeries) float64 {
	min := math.MaxFloat64
	for _, dp := range data {
		if dp.value < min {
			min = dp.value
		}
	}
	return min
}

// getAggFunction is a convenience method used to process an aggregator option
// from our time series query protobuffer format.
func getAggFunction(agg tspb.TimeSeriesQueryAggregator) aggFunc {
	switch agg {
	case tspb.TimeSeriesQueryAggregator_AVG:
		return aggFuncAvg
	case tspb.TimeSeriesQueryAggregator_SUM:
		return aggFuncSum
	case tspb.TimeSeriesQueryAggregator_MAX:
		return aggFuncMax
	case tspb.TimeSeriesQueryAggregator_MIN:
		return aggFuncMin
	}

	// The model should not be called with an invalid aggregator option.
	panic(fmt.Sprintf("unknown aggregator option specified: %v", agg))
}

func fillFuncLinearInterpolate(before DataSeries, after DataSeries, resolution int64) DataSeries {
	start := before[len(before)-1]
	end := after[0]

	// compute interpolation step
	step := (end.value - start.value) / float64(end.timestamp-start.timestamp)

	result := make(DataSeries, (end.timestamp-start.timestamp)/resolution-1)
	for i := range result {
		result[i] = dp(
			start.timestamp+(resolution*int64(i+1)),
			start.value+(step*float64(i+1)*float64(resolution)),
		)
	}
	return result
}
