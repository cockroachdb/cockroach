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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

// ModelDB is a purely in-memory model of CockroachDB's time series database,
// where time series can be stored and queried.
type ModelDB struct {
	data                    map[string]DataSeries
	metricNameToDataSources map[string]map[string]struct{}
}

// NewModelDB instantiates a new ModelDB instance.
func NewModelDB() *ModelDB {
	return &ModelDB{
		data: make(map[string]DataSeries),
		metricNameToDataSources: make(map[string]map[string]struct{}),
	}
}

// Record stores the given data series for the supplied metric and data source,
// merging it with any previously recorded data for the same series.
func (mdb *ModelDB) Record(metricName, dataSource string, data DataSeries) {
	dataSources, ok := mdb.metricNameToDataSources[metricName]
	if !ok {
		dataSources = make(map[string]struct{})
		mdb.metricNameToDataSources[metricName] = dataSources
	}
	dataSources[dataSource] = struct{}{}

	seriesName := seriesName(metricName, dataSource)
	mdb.data[seriesName] = append(mdb.data[seriesName], data...)
	sort.Sort(mdb.data[seriesName])
}

// Query retrieves aggregated data from the model database in the same way that
// data is currently queried from CockroachDB's time series database. Each query
// has a named metric, an optional set of sources, and a number of specified
// aggregation options:
//
// + A downsampler function, which is used to group series by resolution
// + An aggregation function, which is used to group multiples series by
// timestamp
// + A derivative option, which transforms the returned series into a rate of
// change.
//
// Each query has a sample duration (determines the length of the group-by-time
// interval), a start and end time, and an 'interpolation limit' which is a
// maximum gap size above which missing data is not filled. When fills are
// performed, linear interpolation is always used.
func (mdb *ModelDB) Query(
	name string,
	sources []string,
	downsample, agg *tspb.TimeSeriesQueryAggregator,
	derivative *tspb.TimeSeriesQueryDerivative,
	sampleDuration, start, end, interpolationLimit int64,
) DataSeries {
	// If explicit sources were not specified, use every source currently
	// available for this particular metric.
	if len(sources) == 0 {
		sourceMap, ok := mdb.metricNameToDataSources[name]
		if !ok {
			return nil
		}
		sources = make([]string, 0, len(sourceMap))
		for k := range sourceMap {
			sources = append(sources, k)
		}
	}

	queryData := make([]DataSeries, 0, len(sources))
	for _, source := range sources {
		queryData = append(queryData, mdb.getSeriesData(name, source))
	}

	// Process data according to query parameters.
	adjustedStart := normalizeTime(start, sampleDuration) - interpolationLimit
	adjustedEnd := normalizeTime(end, sampleDuration) + interpolationLimit
	for i := range queryData {
		data := queryData[i]

		// Slice to relevant period.
		data = data.timeSlice(adjustedStart, adjustedEnd)

		// Group by resolution according to the provided sampleDuration.
		data = data.groupByResolution(sampleDuration, getAggFunction(*downsample))

		// Fill in missing data points using linear interpolation.
		data = data.fillForResolution(
			sampleDuration,
			func(before DataSeries, after DataSeries, res int64) DataSeries {
				// Do not fill if this gap exceeds the interpolation limit.
				start := before[len(before)-1]
				end := after[0]
				if end.timestamp-start.timestamp > interpolationLimit {
					return nil
				}

				return fillFuncLinearInterpolate(before, after, res)
			},
		)

		// Convert series to its rate-of-change if specified.
		if *derivative != tspb.TimeSeriesQueryDerivative_NONE {
			data = data.rateOfChange(time.Second.Nanoseconds())
			if *derivative == tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE {
				data = data.nonNegative()
			}
		}

		queryData[i] = data
	}

	return groupSeriesByTimestamp(queryData, getAggFunction(*agg)).timeSlice(start, end)
}

func (mdb *ModelDB) getSeriesData(metricName, dataSource string) []DataPoint {
	seriesName := seriesName(metricName, dataSource)
	data, ok := mdb.data[seriesName]
	if !ok {
		return nil
	}
	return data
}

func seriesName(metricName, dataSource string) string {
	return fmt.Sprintf("%s$$%s", metricName, dataSource)
}
