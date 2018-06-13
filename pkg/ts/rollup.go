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

package ts

import (
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

type rollupDatapoint struct {
	timestampNanos int64
	first          float64
	last           float64
	min            float64
	max            float64
	sum            float64
	count          uint32
	variance       float64
}

type rollupData struct {
	name       string
	source     string
	datapoints []rollupDatapoint
}

func (rd *rollupData) toInternal(
	keyDuration, sampleDuration int64,
) ([]roachpb.InternalTimeSeriesData, error) {
	if err := tspb.VerifySlabAndSampleDuration(keyDuration, sampleDuration); err != nil {
		return nil, err
	}

	// This slice must be preallocated to avoid reallocation on `append` because
	// we maintain pointers to its elements in the map below.
	result := make([]roachpb.InternalTimeSeriesData, 0, len(rd.datapoints))
	// Pointers because they need to mutate the stuff in the slice above.
	resultByKeyTime := make(map[int64]*roachpb.InternalTimeSeriesData)

	for _, dp := range rd.datapoints {
		// Determine which InternalTimeSeriesData this datapoint belongs to,
		// creating if it has not already been created for a previous sample.
		keyTime := normalizeToPeriod(dp.timestampNanos, keyDuration)
		itsd, ok := resultByKeyTime[keyTime]
		if !ok {
			result = append(result, roachpb.InternalTimeSeriesData{
				StartTimestampNanos: keyTime,
				SampleDurationNanos: sampleDuration,
			})
			itsd = &result[len(result)-1]
			resultByKeyTime[keyTime] = itsd
		}

		itsd.Offset = append(itsd.Offset, itsd.OffsetForTimestamp(dp.timestampNanos))
		itsd.Last = append(itsd.Last, dp.last)
		itsd.First = append(itsd.First, dp.first)
		itsd.Min = append(itsd.Min, dp.min)
		itsd.Max = append(itsd.Max, dp.max)
		itsd.Count = append(itsd.Count, dp.count)
		itsd.Sum = append(itsd.Sum, dp.sum)
		itsd.Variance = append(itsd.Variance, dp.variance)
	}

	return result, nil
}

func computeRollupsFromData(data tspb.TimeSeriesData, rollupPeriodNanos int64) rollupData {
	rollup := rollupData{
		name:   data.Name,
		source: data.Source,
	}

	createRollupPoint := func(timestamp int64, dataSlice []tspb.TimeSeriesDatapoint) {
		result := rollupDatapoint{
			timestampNanos: timestamp,
			max:            -math.MaxFloat64,
			min:            math.MaxFloat64,
		}
		mean := 0.0
		meanSquaredDist := 0.0
		for i, dp := range dataSlice {
			if i == 0 {
				result.first = dp.Value
			}
			result.last = dp.Value
			result.max = math.Max(result.max, dp.Value)
			result.min = math.Min(result.min, dp.Value)
			result.count++
			result.sum += dp.Value

			// Welford's algorithm for computing variance.
			delta := dp.Value - mean
			mean = mean + delta/float64(result.count)
			delta2 := dp.Value - mean
			meanSquaredDist = meanSquaredDist + delta*delta2
		}

		result.variance = meanSquaredDist / float64(result.count)
		rollup.datapoints = append(rollup.datapoints, result)
	}

	dps := data.Datapoints
	for len(dps) > 0 {
		rollupTimestamp := normalizeToPeriod(dps[0].TimestampNanos, rollupPeriodNanos)
		endIdx := sort.Search(len(dps), func(i int) bool {
			return normalizeToPeriod(dps[i].TimestampNanos, rollupPeriodNanos) > rollupTimestamp
		})
		createRollupPoint(rollupTimestamp, dps[:endIdx])
		dps = dps[endIdx:]
	}

	return rollup
}
