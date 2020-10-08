// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
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
		tsi.length += data.SampleCount()
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
	tsi.timestamp = data.StartTimestampNanos + data.SampleDurationNanos*int64(tsi.offset())
}

// forward moves the iterator forward one sample. The maximum index is equal
// to the length of the span, which is one index beyond the last sample.
func (tsi *timeSeriesSpanIterator) forward() {
	if !tsi.isValid() {
		return
	}
	tsi.total++
	tsi.inner++
	if tsi.inner >= tsi.span[tsi.outer].SampleCount() {
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
		tsi.inner = tsi.span[tsi.outer].SampleCount() - 1
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
	for len(tsi.span) > newOuter && remaining >= tsi.span[newOuter].SampleCount() {
		remaining -= tsi.span[newOuter].SampleCount()
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

func (tsi *timeSeriesSpanIterator) isColumnar() bool {
	return tsi.span[tsi.outer].IsColumnar()
}

func (tsi *timeSeriesSpanIterator) isRollup() bool {
	return tsi.span[tsi.outer].IsRollup()
}

func (tsi *timeSeriesSpanIterator) offset() int32 {
	data := tsi.span[tsi.outer]
	if tsi.isColumnar() {
		return data.Offset[tsi.inner]
	}
	return data.Samples[tsi.inner].Offset
}

func (tsi *timeSeriesSpanIterator) count() uint32 {
	data := tsi.span[tsi.outer]
	if tsi.isColumnar() {
		if tsi.isRollup() {
			return data.Count[tsi.inner]
		}
		return 1
	}
	return data.Samples[tsi.inner].Count
}

func (tsi *timeSeriesSpanIterator) sum() float64 {
	data := tsi.span[tsi.outer]
	if tsi.isColumnar() {
		if tsi.isRollup() {
			return data.Sum[tsi.inner]
		}
		return data.Last[tsi.inner]
	}
	return data.Samples[tsi.inner].Sum
}

func (tsi *timeSeriesSpanIterator) max() float64 {
	data := tsi.span[tsi.outer]
	if tsi.isColumnar() {
		if tsi.isRollup() {
			return data.Max[tsi.inner]
		}
		return data.Last[tsi.inner]
	}
	if max := data.Samples[tsi.inner].Max; max != nil {
		return *max
	}
	return data.Samples[tsi.inner].Sum
}

func (tsi *timeSeriesSpanIterator) min() float64 {
	data := tsi.span[tsi.outer]
	if tsi.isColumnar() {
		if tsi.isRollup() {
			return data.Min[tsi.inner]
		}
		return data.Last[tsi.inner]
	}
	if min := data.Samples[tsi.inner].Min; min != nil {
		return *min
	}
	return data.Samples[tsi.inner].Sum
}

func (tsi *timeSeriesSpanIterator) first() float64 {
	data := tsi.span[tsi.outer]
	if tsi.isColumnar() {
		if tsi.isRollup() {
			return data.First[tsi.inner]
		}
		return data.Last[tsi.inner]
	}

	// First was not recorded in the planned row-format rollups, but since these
	// rollups were never actually generated we can safely use sum.
	return data.Samples[tsi.inner].Sum
}

func (tsi *timeSeriesSpanIterator) last() float64 {
	data := tsi.span[tsi.outer]
	if tsi.isColumnar() {
		return data.Last[tsi.inner]
	}

	// Last was not recorded in the planned row-format rollups, but since these
	// rollups were never actually generated we can safely use sum.
	return data.Samples[tsi.inner].Sum
}

func (tsi *timeSeriesSpanIterator) variance() float64 {
	data := tsi.span[tsi.outer]
	if tsi.isColumnar() {
		if tsi.isRollup() {
			return data.Variance[tsi.inner]
		}
		return 0
	}

	// Variance was not recorded in the planned row-format rollups, but since
	// these rollups were never actually generated we can safely return 0.
	return 0
}

func (tsi *timeSeriesSpanIterator) average() float64 {
	return tsi.sum() / float64(tsi.count())
}

func (tsi *timeSeriesSpanIterator) setOffset(value int32) {
	data := tsi.span[tsi.outer]
	if tsi.isColumnar() {
		data.Offset[tsi.inner] = value
		return
	}
	data.Samples[tsi.inner].Offset = value
}

func (tsi *timeSeriesSpanIterator) setSingleValue(value float64) {
	data := tsi.span[tsi.outer]
	if tsi.isColumnar() {
		data.Last[tsi.inner] = value
		return
	}
	data.Samples[tsi.inner].Sum = value
	data.Samples[tsi.inner].Count = 1
	data.Samples[tsi.inner].Min = nil
	data.Samples[tsi.inner].Max = nil
}

// truncateSpan truncates the span underlying this iterator to the current
// iterator, *not including* the current position. That is, the logical
// underlying span is truncated to [0, current).
func (tsi *timeSeriesSpanIterator) truncateSpan() {
	var outerExtent int
	if tsi.inner == 0 {
		outerExtent = tsi.outer
	} else {
		outerExtent = tsi.outer + 1
	}

	// Reclaim memory from unused slabs.
	unused := tsi.span[outerExtent:]
	tsi.span = tsi.span[:outerExtent]
	for i := range unused {
		unused[i] = roachpb.InternalTimeSeriesData{}
	}

	if tsi.inner != 0 {
		data := tsi.span[tsi.outer]
		size := tsi.inner
		if data.IsColumnar() {
			data.Offset = data.Offset[:size]
			data.Last = data.Last[:size]
			if data.IsRollup() {
				data.First = data.First[:size]
				data.Min = data.Min[:size]
				data.Max = data.Max[:size]
				data.Count = data.Count[:size]
				data.Sum = data.Sum[:size]
				data.Variance = data.Variance[:size]
			}
		} else {
			data.Samples = data.Samples[:size]
		}
		tsi.span[tsi.outer] = data
	}

	tsi.computeLength()
	tsi.computeTimestamp()
}

// Convert the underlying span to single-valued by removing all optional columns
// from any columnar spans.
func convertToSingleValue(span timeSeriesSpan) {
	for i := range span {
		if span[i].IsColumnar() {
			span[i].Count = nil
			span[i].Sum = nil
			span[i].Min = nil
			span[i].Max = nil
			span[i].First = nil
			span[i].Variance = nil
		}
	}
}

// value returns the value of the sample at the iterators index, according to
// the provided downsampler operation.
func (tsi *timeSeriesSpanIterator) value(downsampler tspb.TimeSeriesQueryAggregator) float64 {
	if !tsi.isValid() {
		return 0
	}
	switch downsampler {
	case tspb.TimeSeriesQueryAggregator_AVG:
		return tsi.sum() / float64(tsi.count())
	case tspb.TimeSeriesQueryAggregator_MAX:
		return tsi.max()
	case tspb.TimeSeriesQueryAggregator_MIN:
		return tsi.min()
	case tspb.TimeSeriesQueryAggregator_SUM:
		return tsi.sum()
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
	if !tsi.validAtTimestamp(timestamp, interpolationLimitNanos) {
		return 0, false
	}
	if tsi.timestamp == timestamp {
		return tsi.value(downsampler), true
	}

	deriv, valid := tsi.derivative(downsampler)
	if !valid {
		return 0, false
	}
	return tsi.value(downsampler) - deriv*float64((tsi.timestamp-timestamp)/tsi.samplePeriod()), true
}

// validAtTimestamp returns true if the iterator can return a valid value for
// the provided timestamp. This is true either if the iterators current position
// is the current timestamp, *or* if the provided timestamp is between the
// iterators current and previous positions *and* the gap between the current
// and previous positions is less than the provided interpolation limit.
func (tsi *timeSeriesSpanIterator) validAtTimestamp(timestamp, interpolationLimitNanos int64) bool {
	if !tsi.isValid() {
		return false
	}
	if tsi.timestamp == timestamp {
		return true
	}
	// Cannot interpolate before the first index.
	if tsi.total == 0 {
		return false
	}
	prev := *tsi
	prev.backward()

	// Only interpolate if the timestamp is in between this point and the previous.
	if timestamp > tsi.timestamp || timestamp <= prev.timestamp {
		return false
	}
	// Respect the interpolation limit. Note that an interpolation limit of zero
	// is a special case still needed for legacy tests.
	// TODO(mrtracy): remove test cases with interpolation limit zero.
	if interpolationLimitNanos > 0 && tsi.timestamp-prev.timestamp > interpolationLimitNanos {
		return false
	}
	return true
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

	resolutions := []Resolution{diskResolution}
	if rollupResolution, ok := diskResolution.TargetRollupResolution(); ok {
		if timespan.verifyDiskResolution(rollupResolution) == nil {
			resolutions = []Resolution{rollupResolution, diskResolution}
		}
	}

	for _, resolution := range resolutions {
		// Compute the maximum timespan width which can be queried for this resolution
		// without exceeding the memory budget.
		maxTimespanWidth, err := mem.GetMaxTimespan(resolution)
		if err != nil {
			return nil, nil, err
		}

		if maxTimespanWidth > timespan.width() {
			if err := db.queryChunk(
				ctx, query, resolution, timespan, mem, &result, sourceSet,
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
					ctx, query, resolution, chunkTime, mem, &result, sourceSet,
				); err != nil {
					return nil, nil, err
				}
			}
		}

		// If results were returned and there are multiple resolutions, determine
		// if we have satisfied the entire query. If not, determine where the query
		// for the next resolution should begin.
		if len(resolutions) > 1 && len(result) > 0 {
			lastTime := result[len(result)-1].TimestampNanos
			if lastTime >= timespan.EndNanos {
				break
			}
			timespan.StartNanos = lastTime
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

	var data []kv.KeyValue
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
	sourceSpans, err := convertKeysToSpans(ctx, data, &acc)
	if err != nil {
		return err
	}
	if len(sourceSpans) == 0 {
		return nil
	}

	if timespan.SampleDurationNanos != diskResolution.SampleDuration() {
		downsampleSpans(sourceSpans, timespan.SampleDurationNanos, query.GetDownsampler())
		// downsampleSpans always produces single-valued spans. At the time of
		// writing, all downsamplers are the identity on single-valued spans, but
		// that may not be true forever (consider for instance a variance
		// downsampler). Therefore, before continuing to the aggregation step we
		// convert the downsampler to SUM, which is equivalent to identify for a
		// single-valued span.
		query.Downsampler = tspb.TimeSeriesQueryAggregator_SUM.Enum()
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
// allocating additional memory. The output data from downsampleSpans is
// single-valued, without rollups; unused rollup data will be discarded.
func downsampleSpans(
	spans map[string]timeSeriesSpan, duration int64, downsampler tspb.TimeSeriesQueryAggregator,
) {
	// Downsample data in place.
	for k, span := range spans {
		nextInsert := makeTimeSeriesSpanIterator(span)
		for start, end := nextInsert, nextInsert; start.isValid(); start = end {
			sampleTimestamp := normalizeToPeriod(start.timestamp, duration)

			switch downsampler {
			case tspb.TimeSeriesQueryAggregator_MAX:
				max := -math.MaxFloat64
				for ; end.isValid() && normalizeToPeriod(end.timestamp, duration) == sampleTimestamp; end.forward() {
					max = math.Max(max, end.max())
				}
				nextInsert.setSingleValue(max)
			case tspb.TimeSeriesQueryAggregator_MIN:
				min := math.MaxFloat64
				for ; end.isValid() && normalizeToPeriod(end.timestamp, duration) == sampleTimestamp; end.forward() {
					min = math.Min(min, end.min())
				}
				nextInsert.setSingleValue(min)
			case tspb.TimeSeriesQueryAggregator_AVG:
				count, sum := uint32(0), 0.0
				for ; end.isValid() && normalizeToPeriod(end.timestamp, duration) == sampleTimestamp; end.forward() {
					count += end.count()
					sum += end.sum()
				}
				nextInsert.setSingleValue(sum / float64(count))
			case tspb.TimeSeriesQueryAggregator_SUM:
				sum := 0.0
				for ; end.isValid() && normalizeToPeriod(end.timestamp, duration) == sampleTimestamp; end.forward() {
					sum += end.sum()
				}
				nextInsert.setSingleValue(sum)
			}

			nextInsert.setOffset(span[nextInsert.outer].OffsetForTimestamp(sampleTimestamp))
			nextInsert.forward()
		}

		// Trim span using nextInsert, which is where the next value would be
		// inserted and is thus the first unneeded value.
		nextInsert.truncateSpan()
		span = nextInsert.span
		convertToSingleValue(span)
		spans[k] = span
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
				valid = iter.validAtTimestamp(lowestTimestamp, interpolationLimitNanos)
				if valid {
					value, valid = iter.derivative(query.GetDownsampler())
					// Convert derivative to seconds.
					value *= float64(time.Second.Nanoseconds()) / float64(iter.samplePeriod())
				}
			case tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE:
				valid = iter.validAtTimestamp(lowestTimestamp, interpolationLimitNanos)
				if valid {
					value, valid = iter.derivative(query.GetDownsampler())
					if value < 0 {
						value = 0
					} else {
						// Convert derivative to seconds.
						value *= float64(time.Second.Nanoseconds()) / float64(iter.samplePeriod())
					}
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
) ([]kv.KeyValue, error) {
	// Iterate over all key timestamps which may contain data for the given
	// sources, based on the given start/end time and the resolution.
	b := &kv.Batch{}
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
	var rows []kv.KeyValue
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
// given disk resolution, across the supplied time span, for all sources. The
// optional limit is used when memory usage is being limited by the number of
// keys, rather than by timespan.
func (db *DB) readAllSourcesFromDatabase(
	ctx context.Context, seriesName string, diskResolution Resolution, timespan QueryTimespan,
) ([]kv.KeyValue, error) {
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
	b := &kv.Batch{}
	b.Scan(startKey, endKey)

	if err := db.db.Run(ctx, b); err != nil {
		return nil, err
	}
	return b.Results[0].Rows, nil
}

// convertKeysToSpans converts a batch of KeyValues queried from disk into a
// map of data spans organized by source.
func convertKeysToSpans(
	ctx context.Context, data []kv.KeyValue, acc *mon.BoundAccount,
) (map[string]timeSeriesSpan, error) {
	sourceSpans := make(map[string]timeSeriesSpan)
	for _, row := range data {
		var data roachpb.InternalTimeSeriesData
		if err := row.ValueProto(&data); err != nil {
			return nil, err
		}
		_, source, _, _, err := DecodeDataKey(row.Key)
		if err != nil {
			return nil, err
		}
		sampleSize := sizeOfSample
		if data.IsColumnar() {
			sampleSize = sizeOfInt32 + sizeOfFloat64
		}
		if err := acc.Grow(
			ctx, sampleSize*int64(data.SampleCount())+sizeOfTimeSeriesData,
		); err != nil {
			return nil, err
		}
		sourceSpans[source] = append(sourceSpans[source], data)
	}
	return sourceSpans, nil
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
	case tspb.TimeSeriesQueryAggregator_FIRST,
		tspb.TimeSeriesQueryAggregator_LAST,
		tspb.TimeSeriesQueryAggregator_VARIANCE:
		return errors.Errorf("aggregator %s is not yet supported", agg.String())
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
	case tspb.TimeSeriesQueryAggregator_FIRST,
		tspb.TimeSeriesQueryAggregator_LAST,
		tspb.TimeSeriesQueryAggregator_VARIANCE:
		return errors.Errorf("downsampler %s is not yet supported", downsampler.String())
	}
	return errors.Errorf("query specified unknown time series downsampler %s", downsampler.String())
}
