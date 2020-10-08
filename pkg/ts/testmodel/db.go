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
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

// ModelDB is a purely in-memory model of CockroachDB's time series database,
// where time series can be stored and queried.
type ModelDB struct {
	data                    map[string]DataSeries
	metricNameToDataSources map[string]map[string]struct{}
	seenDataSources         map[string]struct{}
}

// NewModelDB instantiates a new ModelDB instance.
func NewModelDB() *ModelDB {
	return &ModelDB{
		data:                    make(map[string]DataSeries),
		metricNameToDataSources: make(map[string]map[string]struct{}),
		seenDataSources:         make(map[string]struct{}),
	}
}

type seriesVisitor func(string, string, DataSeries) (DataSeries, bool)

// UniqueSourceCount returns the total number of unique data sources that have been
// encountered by queries.
func (mdb *ModelDB) UniqueSourceCount() int64 {
	return int64(len(mdb.seenDataSources))
}

// VisitAllSeries calls the provided visitor function on every recorded
// series. The visitor function can optionally return a DataSeries which will
// completely replace the data series present in the model for that series.
func (mdb *ModelDB) VisitAllSeries(visitor seriesVisitor) {
	for k := range mdb.data {
		metricName, dataSource := splitSeriesName(k)
		replacement, replace := visitor(metricName, dataSource, mdb.data[k])
		if replace {
			mdb.data[k] = replacement
		}
	}
}

// VisitSeries calls the provided visitor function for all series with the given
// metric name. The visitor function can optionally return a DataSeries which
// will completely replace the data series present in the model for that series.
func (mdb *ModelDB) VisitSeries(name string, visitor seriesVisitor) {
	sourceMap, ok := mdb.metricNameToDataSources[name]
	if !ok {
		return
	}

	for source := range sourceMap {
		replacement, replace := visitor(name, source, mdb.getSeriesData(name, source))
		if replace {
			mdb.data[seriesName(name, source)] = replacement
		}
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
	mdb.seenDataSources[dataSource] = struct{}{}

	seriesName := seriesName(metricName, dataSource)
	mdb.data[seriesName] = append(mdb.data[seriesName], data...)
	sort.Stable(mdb.data[seriesName])
	mdb.data[seriesName] = mdb.data[seriesName].removeDuplicates()
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
// interval), a slab duration (used to emulate certain effects of CockroachDB's
// current time series tests), a start and end time, and an 'interpolation
// limit' which is a maximum gap size above which missing data is not filled.
// When fills are performed, linear interpolation is always used.
func (mdb *ModelDB) Query(
	name string,
	sources []string,
	downsample, agg tspb.TimeSeriesQueryAggregator,
	derivative tspb.TimeSeriesQueryDerivative,
	slabDuration, sampleDuration, start, end, interpolationLimit, now int64,
) DataSeries {
	start = normalizeTime(start, sampleDuration)
	// Check query bounds against the provided current time. Queries in the future
	// are disallowed; "future" is considered to be at or later than the sample
	// period containing the current system time (represented by the "now"
	// timestamp)
	cutoff := now - sampleDuration
	if start > cutoff {
		return nil
	}
	if end > cutoff {
		end = cutoff
	}

	// Add one nanosecond to end because the real CockroachDB system is currently
	// inclusive of end boundary.
	end++

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

	// BeforeFill keeps a list of data series before interpolation. This is used
	// to emulate an odd property of the current CockroachDB time series model:
	// see intersectTimestamps method for details.
	beforeFill := make([]DataSeries, len(queryData))

	// Process data according to query parameters.
	// The adjusted start and end are needed in order to simulate the slabbing
	// behavior of the real CockroachDB system; it affects how interpolation will
	// behave in existing tests with an interpolation limit of 0. TODO(mrtracy):
	// Remove tests with interpolation limit 0.
	adjustedStart := normalizeTime(start-interpolationLimit, slabDuration)
	adjustedEnd := normalizeTime(end+interpolationLimit-1, slabDuration) + slabDuration
	for i := range queryData {
		data := queryData[i]

		// Slice to relevant period.
		data = data.TimeSlice(adjustedStart, adjustedEnd)

		// Group by resolution according to the provided sampleDuration.
		data = data.GroupByResolution(sampleDuration, getAggFunction(downsample))

		// Save snapshot of data before filling.
		beforeFill[i] = data

		// Fill in missing data points using linear interpolation. To match existing
		// behavior, do not interpolate if there is only a single series.
		if len(queryData) > 1 {
			data = data.fillForResolution(
				sampleDuration,
				func(before DataSeries, after DataSeries, res int64) DataSeries {
					// Do not fill if this gap exceeds the interpolation limit.
					start := before[len(before)-1]
					end := after[0]
					if interpolationLimit > 0 && end.TimestampNanos-start.TimestampNanos > interpolationLimit {
						return nil
					}

					return fillFuncLinearInterpolate(before, after, res)
				},
			)
		}

		// Convert series to its rate-of-change if specified.
		if derivative != tspb.TimeSeriesQueryDerivative_NONE {
			data = data.rateOfChange(time.Second.Nanoseconds())
			if derivative == tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE {
				data = data.nonNegative()
			}
		}

		queryData[i] = data
	}

	result := groupSeriesByTimestamp(queryData, getAggFunction(agg))
	result = result.TimeSlice(start, end)
	result = result.intersectTimestamps(beforeFill...)
	return result
}

func (mdb *ModelDB) getSeriesData(metricName, dataSource string) DataSeries {
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

func splitSeriesName(seriesName string) (string, string) {
	split := strings.Split(seriesName, "$$")
	if len(split) != 2 {
		panic(fmt.Sprintf("attempt to split invalid series name %s", seriesName))
	}
	return split[0], split[1]
}
