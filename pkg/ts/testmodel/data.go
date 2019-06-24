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
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

// dp is a shorthand function for constructing a TimeSeriesDatapoint, used for
// convenience in tests.
func dp(timestamp int64, value float64) tspb.TimeSeriesDatapoint {
	return tspb.TimeSeriesDatapoint{
		TimestampNanos: timestamp,
		Value:          value,
	}
}

// DataSeries represents a series of data points ordered by timestamp.
type DataSeries []tspb.TimeSeriesDatapoint

func (data DataSeries) Len() int           { return len(data) }
func (data DataSeries) Swap(i, j int)      { data[i], data[j] = data[j], data[i] }
func (data DataSeries) Less(i, j int) bool { return data[i].TimestampNanos < data[j].TimestampNanos }

func normalizeTime(time, resolution int64) int64 {
	return time - time%resolution
}

// TimeSlice returns the set of the dataPoints from the supplied series with
// timestamps that fall in the interval [start, end) (not inclusive of end
// timestamp).
func (data DataSeries) TimeSlice(start, end int64) DataSeries {
	startIdx := sort.Search(len(data), func(i int) bool {
		return data[i].TimestampNanos >= start
	})
	endIdx := sort.Search(len(data), func(i int) bool {
		return end <= data[i].TimestampNanos
	})

	result := data[startIdx:endIdx]
	if len(result) == 0 {
		return nil
	}
	return result
}

// GroupByResolution aggregates data points in the given series into time
// buckets based on the provided resolution.
func (data DataSeries) GroupByResolution(resolution int64, aggFunc aggFunc) DataSeries {
	if len(data) == 0 {
		return nil
	}

	result := make(DataSeries, 0)

	for len(data) > 0 {
		bucketTime := normalizeTime(data[0].TimestampNanos, resolution)
		// Grab the index of the first data point which does not belong to the same
		// bucket as the start data point.
		bucketEndIdx := sort.Search(len(data), func(idx int) bool {
			return normalizeTime(data[idx].TimestampNanos, resolution) > bucketTime
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
		if data[i].TimestampNanos-data[i-1].TimestampNanos > resolution {
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
			data[i].TimestampNanos,
			(data[i].Value-data[i-1].Value)/(float64(data[i].TimestampNanos-data[i-1].TimestampNanos)/float64(period)),
		)
	}
	return result
}

// nonNegative replaces any values less than zero with a zero.
func (data DataSeries) nonNegative() DataSeries {
	result := make(DataSeries, len(data))
	for i := range data {
		if data[i].Value >= 0 {
			result[i] = data[i]
		} else {
			result[i] = dp(data[i].TimestampNanos, 0)
		}
	}
	return result
}

// adjustTimestamps adjusts all timestamps in the series by the provided offset.
func (data DataSeries) adjustTimestamps(offset int64) DataSeries {
	result := make(DataSeries, len(data))
	for i := range data {
		result[i] = data[i]
		result[i].TimestampNanos += offset
	}
	return result
}

// removeDuplicates removes any duplicate timestamps from the given sorted
// series, keeping the last duplicate datapoint as the only value.
func (data DataSeries) removeDuplicates() DataSeries {
	if len(data) < 2 {
		return data
	}

	result := make(DataSeries, len(data))
	result[0] = data[0]
	for i, j := 1, 0; i < len(data); i++ {
		if result[j].TimestampNanos == data[i].TimestampNanos {
			// Duplicate timestamp, keep last value only and shrink result output.
			result[j].Value = data[i].Value
			result = result[:len(result)-1]
		} else {
			j++
			result[j] = data[i]
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
			if data[0].TimestampNanos < earliestTime {
				// New earliest timestamp found, discard any points which were
				// previously in the collection.
				dataPointsToAggregate = dataPointsToAggregate[:0]
				earliestTime = data[0].TimestampNanos
			}
			if data[0].TimestampNanos == earliestTime {
				// Data point matches earliest timestamp, add it to current datapoint
				// collection.
				dataPointsToAggregate = append(dataPointsToAggregate, data[0])
			}
		}
		results = append(results, dp(earliestTime, aggFunc(dataPointsToAggregate)))
		for i := range datas {
			if datas[i][0].TimestampNanos == earliestTime {
				datas[i] = datas[i][1:]
			}
		}
	}

	return results
}

// intersectTimestamps returns all data points for which a matching timestamp is
// found in any of the supplied data series. This is used to emulate an existing
// behavior of CockroachDB's Time Series system, where a data point is not
// interpolated if no aggregated series has a real timestamp at that point.
func (data DataSeries) intersectTimestamps(datas ...DataSeries) DataSeries {
	seenTimestamps := make(map[int64]struct{})
	for _, ds := range datas {
		for _, dp := range ds {
			seenTimestamps[dp.TimestampNanos] = struct{}{}
		}
	}

	result := make(DataSeries, 0, len(data))
	for _, dp := range data {
		if _, ok := seenTimestamps[dp.TimestampNanos]; ok {
			result = append(result, dp)
		}
	}
	return result
}

const floatTolerance float64 = 0.0001

func floatEquals(a, b float64) bool {
	if (a-b) < floatTolerance && (b-a) < floatTolerance {
		return true
	}
	return false
}

// DataSeriesEquivalent returns true if the provided data series are roughly
// equivalent. This is useful primarily to work around floating point errors
// which occur if the order of computation differs between the model and the
// real system.
func DataSeriesEquivalent(a, b DataSeries) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].TimestampNanos != b[i].TimestampNanos {
			return false
		}
		if !floatEquals(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}
