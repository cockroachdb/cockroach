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
	"math"
	"sort"
)

// DataPoint represents a single point in a time series. It is a timestamp/value
// pair.
type DataPoint struct {
	timestamp int64
	value     float64
}

// dp is a shorthand function for constructing a DataPoint, used for convenience
// in tests.
func dp(timestamp int64, value float64) DataPoint {
	return DataPoint{
		timestamp: timestamp,
		value:     value,
	}
}

// DataSeries represents a series of data points ordered by timestamp.
type DataSeries []DataPoint

func (data DataSeries) Len() int           { return len(data) }
func (data DataSeries) Swap(i, j int)      { data[i], data[j] = data[j], data[i] }
func (data DataSeries) Less(i, j int) bool { return data[i].timestamp < data[j].timestamp }

func normalizeTime(time, resolution int64) int64 {
	return time - time%resolution
}

// timeSlice returns the set of the dataPoints from the supplied series with
// timestamps that fall in the interval [start, end) (not inclusive of end
// timestamp).
func (data DataSeries) timeSlice(start, end int64) DataSeries {
	startIdx := sort.Search(len(data), func(i int) bool {
		return data[i].timestamp >= start
	})
	endIdx := sort.Search(len(data), func(i int) bool {
		return end <= data[i].timestamp
	})

	result := data[startIdx:endIdx]
	if len(result) == 0 {
		return nil
	}
	return result
}

// groupByResolution aggregates data points in the given series into time
// buckets based on the provided resolution.
func (data DataSeries) groupByResolution(resolution int64, aggFunc aggFunc) DataSeries {
	if len(data) == 0 {
		return nil
	}

	start := normalizeTime(data[0].timestamp, resolution)
	end := normalizeTime(data[len(data)-1].timestamp, resolution)
	result := make(DataSeries, 0, (end-start)/resolution+1)

	for len(data) > 0 {
		bucketTime := normalizeTime(data[0].timestamp, resolution)
		// Grab the index of the first data point which does not belong to the same
		// bucket as the start data point.
		bucketEndIdx := sort.Search(len(data), func(idx int) bool {
			return normalizeTime(data[idx].timestamp, resolution) > bucketTime
		})
		// Compute the next point as an aggregate of all underlying points which
		// go in the same bucket.
		result = append(result, dp(bucketTime, aggFunc(data[:bucketEndIdx])))
		data = data[bucketEndIdx:]
	}

	return result
}

// fillForResolution is used to fill in gaps in the provided data based on the
// provided resolution and fill function; any gaps longer than the resolution
// size will be eligible for fill. This is intended to be called on data sets
// that have been generated using groupByResolution, and may have unexpected
// results otherwise.
func (data DataSeries) fillForResolution(resolution int64, fillFunc fillFunc) DataSeries {
	if len(data) < 2 {
		return data
	}

	result := make(DataSeries, 0, len(data))
	result = append(result, data[0])
	for i := 1; i < len(data); i++ {
		if data[i].timestamp-data[i-1].timestamp > resolution {
			result = append(result, fillFunc(data[:i], data[i:], resolution)...)
		}
		result = append(result, data[i])
	}

	return result
}

// rateOfChange returns the rate of change (over the supplied period) for each
// point in the supplied series, which is defined as:
//         (value - valuePrev) / ((time - timePrev) / period)
// The returned series will be shorter than the original series by one, since
// the rate of change for the first datapoint cannot be computed in this
// fashion.
func (data DataSeries) rateOfChange(period int64) DataSeries {
	if len(data) < 2 {
		return nil
	}

	result := make(DataSeries, len(data)-1)
	for i := 1; i < len(data); i++ {
		result[i-1] = dp(
			data[i].timestamp,
			(data[i].value-data[i-1].value)/float64((data[i].timestamp-data[i-1].timestamp)/period),
		)
	}
	return result
}

// nonNegative replaces any values less than zero with a zero.
func (data DataSeries) nonNegative() DataSeries {
	result := make(DataSeries, len(data))
	for i := 0; i < len(data); i++ {
		if data[i].value >= 0 {
			result[i] = data[i]
		} else {
			result[i] = dp(data[i].timestamp, 0)
		}
	}
	return result
}

// groupSeriesByTimestamp returns a single DataSeries by aggregating DataPoints
// with matching timestamps from the supplied set of series.
func groupSeriesByTimestamp(datas []DataSeries, aggFunc aggFunc) DataSeries {
	if len(datas) == 0 {
		return nil
	}

	results := make(DataSeries, 0)
	dataPointsToAggregate := make(DataSeries, 0, len(datas))
	for {
		// Filter empty data series.
		origDatas := datas
		datas = datas[:0]
		for _, data := range origDatas {
			if len(data) > 0 {
				datas = append(datas, data)
			}
		}
		if len(datas) == 0 {
			break
		}

		// Create a slice of datapoints which share the earliest timestamp of any
		// datapoint across all collections. If the data series are all perfectly
		// aligned (same length and timestamps), then this will just be he first
		// data point in each series.
		earliestTime := int64(math.MaxInt64)
		for _, data := range datas {
			if data[0].timestamp < earliestTime {
				// New earliest timestamp found, discard any points which were
				// previously in the collection.
				dataPointsToAggregate = dataPointsToAggregate[:0]
				earliestTime = data[0].timestamp
			}
			if data[0].timestamp == earliestTime {
				// Data point matches earliest timestamp, add it to current datapoint
				// collection.
				dataPointsToAggregate = append(dataPointsToAggregate, data[0])
			}
		}
		results = append(results, dp(earliestTime, aggFunc(dataPointsToAggregate)))
		for i := range datas {
			if datas[i][0].timestamp == earliestTime {
				datas[i] = datas[i][1:]
			}
		}
	}

	return results
}
