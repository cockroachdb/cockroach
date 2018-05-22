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
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

// timeSeriesSpan represents a queryed time span for a single time series. This
// is reprented as an ordered slice of data slabs, where each slab contains
// samples.
type timeSeriesSpan []roachpb.InternalTimeSeriesData

// timeSeriesSpanIterator is used to iterate over a timeSeriesSpan. An iterator
// is helpful because a multi-level index is required to iterate over the structure.
type timeSeriesSpanIterator struct {
	span      timeSeriesSpan
	total     int
	outer     int
	inner     int
	timestamp int64
	length    int
}

// makeTimeSeriesSpanIterator constructs a new iterator for the supplied
// timeSeriesSpan, initialized at index 0.
func makeTimeSeriesSpanIterator(span timeSeriesSpan) timeSeriesSpanIterator {
	iterator := timeSeriesSpanIterator{
		span: span,
	}
	iterator.computeLength()
	iterator.computeTimestamp()
	return iterator
}

// computeLength recomputes the total length of the span.
func (tsi *timeSeriesSpanIterator) computeLength() {
	tsi.length = 0
	for _, data := range tsi.span {
		tsi.length += len(data.Samples)
	}
}

// computeTimestamp computes the timestamp of the sample at the current index.
// It is automatically called internally whenever the iterator is moved.
func (tsi *timeSeriesSpanIterator) computeTimestamp() {
	if !tsi.isValid() {
		tsi.timestamp = 0
		return
	}
	data := tsi.span[tsi.outer]
	tsi.timestamp = data.StartTimestampNanos + data.SampleDurationNanos*int64(data.Samples[tsi.inner].Offset)
}

// forward moves the iterator forward one sample. The maximum index is equal
// to the length of the span, which is one index beyond the last sample.
func (tsi *timeSeriesSpanIterator) forward() {
	if !tsi.isValid() {
		return
	}
	tsi.total++
	tsi.inner++
	if tsi.inner >= len(tsi.span[tsi.outer].Samples) {
		tsi.inner = 0
		tsi.outer++
	}
	tsi.computeTimestamp()
}

// backward moves the iterator back one sample. The iterator can not be moved
// earlier than the first index.
func (tsi *timeSeriesSpanIterator) backward() {
	if tsi.outer == 0 && tsi.inner == 0 {
		return
	}
	tsi.total--
	if tsi.inner == 0 {
		tsi.outer--
		tsi.inner = len(tsi.span[tsi.outer].Samples) - 1
	} else {
		tsi.inner--
	}
	tsi.computeTimestamp()
}

// seekIndex sets the iterator to the supplied index in the span. The index
// cannot be set greater than the length of the span or less than zero.
func (tsi *timeSeriesSpanIterator) seekIndex(index int) {
	if index >= tsi.length {
		tsi.total = tsi.length
		tsi.inner = 0
		tsi.outer = len(tsi.span)
		tsi.timestamp = 0
		return
	}

	if index < 0 {
		index = 0
	}

	remaining := index
	newOuter := 0
	for len(tsi.span) > newOuter && remaining >= len(tsi.span[newOuter].Samples) {
		remaining -= len(tsi.span[newOuter].Samples)
		newOuter++
	}
	tsi.inner = remaining
	tsi.outer = newOuter
	tsi.total = index
	tsi.computeTimestamp()
}

// seekTimestamp sets the iterator to the earliest sample index with a timestamp
// greater than or equal to the supplied timestamp.
func (tsi *timeSeriesSpanIterator) seekTimestamp(timestamp int64) {
	seeker := *tsi
	index := sort.Search(tsi.length, func(i int) bool {
		seeker.seekIndex(i)
		return seeker.timestamp >= timestamp
	})
	tsi.seekIndex(index)
}

// value returns the value of the sample at the iterators index, according to
// the provided downsampler operation.
func (tsi *timeSeriesSpanIterator) value(downsampler tspb.TimeSeriesQueryAggregator) float64 {
	if !tsi.isValid() {
		return 0
	}
	switch downsampler {
	case tspb.TimeSeriesQueryAggregator_AVG:
		return tsi.data().Average()
	case tspb.TimeSeriesQueryAggregator_MAX:
		return tsi.data().Maximum()
	case tspb.TimeSeriesQueryAggregator_MIN:
		return tsi.data().Minimum()
	case tspb.TimeSeriesQueryAggregator_SUM:
		return tsi.data().Summation()
	}

	panic(fmt.Sprintf("unknown downsampler option encountered: %v", downsampler))
}

// valueAtTimestamp returns the value of the span at the provided timestamp,
// according to the current position of the iterator. If the provided timestamp
// is not exactly equal to the iterator's current timestamp, but is in between
// the iterator's timestamp and the previous timestamp, then the value is
// interpolated using linear interpolation.
//
// However, a maximum interpolation limit is passed - if the distance between
// the current timestamp and the previous timestamp is greater than this limit,
// then interpolation will not be attempted.
func (tsi *timeSeriesSpanIterator) valueAtTimestamp(
	timestamp int64, interpolationLimitNanos int64, downsampler tspb.TimeSeriesQueryAggregator,
) (float64, bool) {
	if !tsi.isValid() {
		return 0, false
	}
	if tsi.timestamp == timestamp {
		return tsi.value(downsampler), true
	}

	// Cannot interpolate before the first index.
	if tsi.total == 0 {
		return 0, false
	}

	prev := *tsi
	prev.backward()
	// Only interpolate if the timestamp is in between this point and the previous.
	if timestamp > tsi.timestamp || timestamp <= prev.timestamp {
		return 0, false
	}
	// Respect the interpolation limit. Note that an interpolation limit of zero
	// is a special case still needed for legacy tests.
	// TODO(mrtracy): remove test cases with interpolation limit zero.
	if interpolationLimitNanos > 0 && tsi.timestamp-prev.timestamp > interpolationLimitNanos {
		return 0, false
	}

	deriv, valid := tsi.derivative(downsampler)
	if !valid {
		return 0, false
	}
	return prev.value(downsampler) + deriv*float64((timestamp-prev.timestamp)/tsi.samplePeriod()), true
}

// derivative returns the current rate of change of the iterator, computed by
// considering the value at the current position and the value at the previous
// position of the iterator. The derivative is expressed per sample period.
func (tsi *timeSeriesSpanIterator) derivative(
	downsampler tspb.TimeSeriesQueryAggregator,
) (float64, bool) {
	if !tsi.isValid() {
		return 0, false
	}

	// Cannot compute rate of change for the first index.
	if tsi.total == 0 {
		return 0, false
	}

	prev := *tsi
	prev.backward()
	rateOfChange := (tsi.value(downsampler) - prev.value(downsampler)) / float64((tsi.timestamp-prev.timestamp)/tsi.samplePeriod())
	return rateOfChange, true
}

// samplePeriod returns the sample period duration for this iterator.
func (tsi *timeSeriesSpanIterator) samplePeriod() int64 {
	return tsi.span[0].SampleDurationNanos
}

// data returns the actual sample at the current iterator position.
func (tsi *timeSeriesSpanIterator) data() roachpb.InternalTimeSeriesSample {
	if !tsi.isValid() {
		panic("call of data() on an invalid timeSeriesSpanIterator")
	}
	return tsi.span[tsi.outer].Samples[tsi.inner]
}

// dataSlice returns a slice of one element containing the sample at the current
// iterator position. This is used for mutating the sample at this index.
func (tsi *timeSeriesSpanIterator) dataSlice() []roachpb.InternalTimeSeriesSample {
	if !tsi.isValid() {
		panic("call of dataSlice() on an invalid timeSeriesSpanIterator")
	}
	return tsi.span[tsi.outer].Samples[tsi.inner : tsi.inner+1]
}

// isValid returns true if the iterator currently points to a valid sample.
func (tsi *timeSeriesSpanIterator) isValid() bool {
	return tsi.total < tsi.length
}

// Query processes the supplied query over the supplied timespan and on-disk
// resolution, while respecting the provided limitations on memory usage.
func (db *DB) Query(
	ctx context.Context,
	query tspb.Query,
	diskResolution Resolution,
	timespan QueryTimespan,
	mem QueryMemoryContext,
) ([]tspb.TimeSeriesDatapoint, []string, error) {
	timespan.normalize()

	// Validate incoming parameters.
	if err := timespan.verifyBounds(); err != nil {
		return nil, nil, err
	}
	if err := timespan.verifyDiskResolution(diskResolution); err != nil {
		return nil, nil, err
	}
	if err := verifySourceAggregator(query.GetSourceAggregator()); err != nil {
		return nil, nil, err
	}
	if err := verifyDownsampler(query.GetDownsampler()); err != nil {
		return nil, nil, err
	}

	// Adjust timespan based on the current time.
	if err := timespan.adjustForCurrentTime(diskResolution); err != nil {
		return nil, nil, err
	}

	var result []tspb.TimeSeriesDatapoint

	// Create sourceSet, which tracks unique sources seen while querying.
	sourceSet := make(map[string]struct{})

	// Compute the maximum timespan width which can be queried for this resolution
	// without exceeding the memory budget.
	maxTimespanWidth, err := mem.GetMaxTimespan(diskResolution)
	if err != nil {
		return nil, nil, err
	}

	if maxTimespanWidth > timespan.width() {
		if err := db.queryChunk(
			ctx, query, diskResolution, timespan, mem, &result, sourceSet,
		); err != nil {
			return nil, nil, err
		}
	} else {
		// Break up the timespan into "chunks" where each chunk will fit into the
		// memory budget. Query and process each chunk individually, appending
		// results to the same output collection.
		chunkTime := timespan
		chunkTime.EndNanos = chunkTime.StartNanos + maxTimespanWidth
		for ; chunkTime.StartNanos < timespan.EndNanos; chunkTime.moveForward(maxTimespanWidth + timespan.SampleDurationNanos) {
			if chunkTime.EndNanos > timespan.EndNanos {
				// Final chunk may be a smaller window.
				chunkTime.EndNanos = timespan.EndNanos
			}
			if err := db.queryChunk(
				ctx, query, diskResolution, chunkTime, mem, &result, sourceSet,
			); err != nil {
				return nil, nil, err
			}
		}
	}

	// Convert the unique sources seen into a slice.
	sources := make([]string, 0, len(sourceSet))
	for source := range sourceSet {
		sources = append(sources, source)
	}

	return result, sources, nil
}

// queryChunk processes a chunk of a query; this will read the necessary data
// from disk and apply the desired processing operations to generate a result.
func (db *DB) queryChunk(
	ctx context.Context,
	query tspb.Query,
	diskResolution Resolution,
	timespan QueryTimespan,
	mem QueryMemoryContext,
	dest *[]tspb.TimeSeriesDatapoint,
	sourceSet map[string]struct{},
) error {
	acc := mem.workerMonitor.MakeBoundAccount()
	defer acc.Close(ctx)

	// Actual queried data should include the interpolation limit on either side.
	diskTimespan := timespan
	diskTimespan.expand(mem.InterpolationLimitNanos)

	var data []client.KeyValue
	var err error
	if len(query.Sources) == 0 {
		data, err = db.readAllSourcesFromDatabase(ctx, query.Name, diskResolution, diskTimespan)
	} else {
		data, err = db.readFromDatabase(ctx, query.Name, diskResolution, diskTimespan, query.Sources)
	}

	if err != nil {
		return err
	}

	// Assemble data into an ordered timeSeriesSpan for each source.
	sourceSpans := make(map[string]timeSeriesSpan)
	for _, row := range data {
		var data roachpb.InternalTimeSeriesData
		if err := row.ValueProto(&data); err != nil {
			return err
		}
		_, source, _, _, err := DecodeDataKey(row.Key)
		if err != nil {
			return err
		}
		if err := acc.Grow(
			ctx, sizeOfSample*int64(len(data.Samples))+sizeOfTimeSeriesData,
		); err != nil {
			return err
		}
		sourceSpans[source] = append(sourceSpans[source], data)
	}
	if len(sourceSpans) == 0 {
		return nil
	}

	if timespan.SampleDurationNanos != diskResolution.SampleDuration() {
		downsampleSpans(sourceSpans, timespan.SampleDurationNanos)
	}

	// Aggregate spans, increasing our memory usage if the destination slice is
	// expanded.
	oldCap := cap(*dest)
	aggregateSpansToDatapoints(sourceSpans, query, timespan, mem.InterpolationLimitNanos, dest)
	if oldCap > cap(*dest) {
		if err := mem.resultAccount.Grow(ctx, sizeOfDataPoint*int64(cap(*dest)-oldCap)); err != nil {
			return err
		}
	}

	// Add unique sources to the supplied source set.
	for k := range sourceSpans {
		sourceSet[k] = struct{}{}
	}
	return nil
}

// downsampleSpans downsamples the provided timeSeriesSpans in place, without
// allocating additional memory.
func downsampleSpans(spans map[string]timeSeriesSpan, duration int64) {
	// Downsample data in place.
	for k, span := range spans {
		nextInsert := makeTimeSeriesSpanIterator(span)
		for start, end := nextInsert, nextInsert; start.isValid(); start = end {
			var (
				sampleTimestamp = normalizeToPeriod(start.timestamp, duration)
				max             = -math.MaxFloat64
				min             = math.MaxFloat64
				count           uint32
				sum             float64
			)
			for end.isValid() && normalizeToPeriod(end.timestamp, duration) == sampleTimestamp {
				data := end.data()
				count += data.Count
				sum += data.Sum
				if data.Max != nil {
					max = math.Max(max, *data.Max)
				} else {
					max = math.Max(max, data.Sum)
				}
				if data.Min != nil {
					min = math.Min(min, *data.Min)
				} else {
					min = math.Min(min, data.Sum)
				}
				end.forward()
			}
			nextInsert.dataSlice()[0] = roachpb.InternalTimeSeriesSample{
				Offset: int32((sampleTimestamp - nextInsert.span[nextInsert.outer].StartTimestampNanos) / nextInsert.span[nextInsert.outer].SampleDurationNanos),
				Count:  count,
				Sum:    sum,
				Max:    proto.Float64(max),
				Min:    proto.Float64(min),
			}
			nextInsert.forward()
		}

		// Trim span using nextInsert, which is where the next value would be inserted
		// and is thus the first unnneeded value.
		var outerExtent int
		if nextInsert.inner == 0 {
			outerExtent = nextInsert.outer
		} else {
			outerExtent = nextInsert.outer + 1
		}
		spans[k] = span[:outerExtent]
		// Reclaim memory from unused slabs.
		unused := span[outerExtent:]
		for i := range unused {
			unused[i] = roachpb.InternalTimeSeriesData{}
		}

		if nextInsert.inner != 0 {
			spans[k][len(spans[k])-1].Samples = spans[k][len(spans[k])-1].Samples[:nextInsert.inner]
		}
	}
}

// aggregateSpansToDatapoints aggregates the supplied set of data spans into
// a single result time series, by aggregating data points from different spans
// which share the same timestamp. For each timestamp in the query range, a
// value is extracted from each span using the supplied downsampling function.
// If a span is missing a value at a specific timestamp, the missing value will
// be interpolated under certain circumstances. The values from the different
// spans are then combined into a single value using the specified source
// aggregator.
func aggregateSpansToDatapoints(
	spans map[string]timeSeriesSpan,
	query tspb.Query,
	timespan QueryTimespan,
	interpolationLimitNanos int64,
	dest *[]tspb.TimeSeriesDatapoint,
) {
	// Aggregate into reserved result slice (filter points missing from component slices)
	iterators := make([]timeSeriesSpanIterator, 0, len(spans))
	for _, span := range spans {
		iter := makeTimeSeriesSpanIterator(span)
		iter.seekTimestamp(timespan.StartNanos)
		iterators = append(iterators, iter)
	}

	var lowestTimestamp int64
	computeLowest := func() {
		lowestTimestamp = math.MaxInt64
		for _, iter := range iterators {
			if !iter.isValid() {
				continue
			}
			if iter.timestamp < lowestTimestamp {
				lowestTimestamp = iter.timestamp
			}
		}
	}

	aggregateValues := make([]float64, len(iterators))
	for computeLowest(); lowestTimestamp <= timespan.EndNanos; computeLowest() {
		aggregateValues = aggregateValues[:0]
		for i, iter := range iterators {
			var value float64
			var valid bool
			switch query.GetDerivative() {
			case tspb.TimeSeriesQueryDerivative_DERIVATIVE:
				value, valid = iter.derivative(query.GetDownsampler())
				// Convert derivative to seconds.
				value *= float64(time.Second.Nanoseconds()) / float64(timespan.SampleDurationNanos)
			case tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE:
				value, valid = iter.derivative(query.GetDownsampler())
				if value < 0 {
					value = 0
				} else {
					// Convert derivative to seconds.
					value *= float64(time.Second.Nanoseconds()) / float64(timespan.SampleDurationNanos)
				}
			default:
				value, valid = iter.valueAtTimestamp(
					lowestTimestamp, interpolationLimitNanos, query.GetDownsampler(),
				)
			}

			if valid {
				aggregateValues = append(aggregateValues, value)
			}
			if iter.timestamp == lowestTimestamp {
				iterators[i].forward()
			}
		}
		if len(aggregateValues) == 0 {
			continue
		}

		// Filters data points near the current moment which are "incomplete". Any
		// data point in the sufficiently-recent past is required to have a valid
		// contribution from all sources being aggregated.
		//
		// A detailed explanation of why this is done: New time series data points
		// are, in typical usage, always added at the current time; however, due to
		// the curiosities of clock skew, it is a common occurrence for the most
		// recent data point to be available for some sources, but not from others.
		// For queries which aggregate from multiple sources, this can lead to a
		// situation where a persistent and precipitous dip appears at the very end
		// of data graphs. This happens because the most recent point only
		// represents the aggregation of a subset of sources, even though the
		// missing sources are not actually offline, they are simply slightly
		// delayed in reporting.
		//
		// Linear interpolation can gaps in the middle of data, but it does not work
		// in this case as the current time is later than any data available from
		// the missing sources.
		//
		// In this case, we can assume that a missing data point will be added soon,
		// and instead do *not* return the partially aggregated data point to the
		// client.
		if lowestTimestamp > timespan.NowNanos-timespan.SampleDurationNanos {
			if len(aggregateValues) < len(iterators) {
				continue
			}
		}

		*dest = append(*dest, tspb.TimeSeriesDatapoint{
			TimestampNanos: lowestTimestamp,
			Value:          aggregate(query.GetSourceAggregator(), aggregateValues),
		})
	}
}

// aggSum returns the sum value of all points in the provided slice.
func aggSum(data []float64) float64 {
	total := 0.0
	for _, dp := range data {
		total += dp
	}
	return total
}

// aggAvg returns the average value of the points in the provided slice.
func aggAvg(data []float64) float64 {
	if len(data) == 0 {
		return 0.0
	}
	return aggSum(data) / float64(len(data))
}

// aggMax returns the maximum value of any point in the provided slice.
func aggMax(data []float64) float64 {
	max := -math.MaxFloat64
	for _, dp := range data {
		if dp > max {
			max = dp
		}
	}
	return max
}

// aggMin returns the minimum value of any point in the provided slice.
func aggMin(data []float64) float64 {
	min := math.MaxFloat64
	for _, dp := range data {
		if dp < min {
			min = dp
		}
	}
	return min
}

// aggregate computes a single float64 value from the given slice of float64s
// using the specified aggregation function.
func aggregate(agg tspb.TimeSeriesQueryAggregator, values []float64) float64 {
	switch agg {
	case tspb.TimeSeriesQueryAggregator_AVG:
		return aggAvg(values)
	case tspb.TimeSeriesQueryAggregator_SUM:
		return aggSum(values)
	case tspb.TimeSeriesQueryAggregator_MAX:
		return aggMax(values)
	case tspb.TimeSeriesQueryAggregator_MIN:
		return aggMin(values)
	}

	panic(fmt.Sprintf("unknown aggregator option encountered: %v", agg))
}

// readFromDatabase retrieves data for the given series name, at the given disk
// resolution, across the supplied time span, for only the given list of
// sources.
func (db *DB) readFromDatabase(
	ctx context.Context,
	seriesName string,
	diskResolution Resolution,
	timespan QueryTimespan,
	sources []string,
) ([]client.KeyValue, error) {
	// Iterate over all key timestamps which may contain data for the given
	// sources, based on the given start/end time and the resolution.
	b := &client.Batch{}
	startTimestamp := diskResolution.normalizeToSlab(timespan.StartNanos)
	kd := diskResolution.SlabDuration()
	for currentTimestamp := startTimestamp; currentTimestamp <= timespan.EndNanos; currentTimestamp += kd {
		for _, source := range sources {
			key := MakeDataKey(seriesName, source, diskResolution, currentTimestamp)
			b.Get(key)
		}
	}
	if err := db.db.Run(ctx, b); err != nil {
		return nil, err
	}
	var rows []client.KeyValue
	for _, result := range b.Results {
		row := result.Rows[0]
		if row.Value == nil {
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// readAllSourcesFromDatabase retrieves data for the given series name, at the
// given disk resolution, across the supplied time span, for all sources.
func (db *DB) readAllSourcesFromDatabase(
	ctx context.Context, seriesName string, diskResolution Resolution, timespan QueryTimespan,
) ([]client.KeyValue, error) {
	// Based on the supplied timestamps and resolution, construct start and
	// end keys for a scan that will return every key with data relevant to
	// the query. Query slightly before and after the actual queried range
	// to allow interpolation of points at the start and end of the range.
	startKey := MakeDataKey(
		seriesName, "" /* source */, diskResolution, timespan.StartNanos,
	)
	endKey := MakeDataKey(
		seriesName, "" /* source */, diskResolution, timespan.EndNanos,
	).PrefixEnd()
	b := &client.Batch{}
	b.Scan(startKey, endKey)

	if err := db.db.Run(ctx, b); err != nil {
		return nil, err
	}
	return b.Results[0].Rows, nil
}

func verifySourceAggregator(agg tspb.TimeSeriesQueryAggregator) error {
	switch agg {
	case tspb.TimeSeriesQueryAggregator_AVG:
		return nil
	case tspb.TimeSeriesQueryAggregator_SUM:
		return nil
	case tspb.TimeSeriesQueryAggregator_MIN:
		return nil
	case tspb.TimeSeriesQueryAggregator_MAX:
		return nil
	}
	return errors.Errorf("query specified unknown time series aggregator %s", agg.String())
}

func verifyDownsampler(downsampler tspb.TimeSeriesQueryAggregator) error {
	switch downsampler {
	case tspb.TimeSeriesQueryAggregator_AVG:
		return nil
	case tspb.TimeSeriesQueryAggregator_SUM:
		return nil
	case tspb.TimeSeriesQueryAggregator_MIN:
		return nil
	case tspb.TimeSeriesQueryAggregator_MAX:
		return nil
	}
	return errors.Errorf("query specified unknown time series downsampler %s", downsampler.String())
}
