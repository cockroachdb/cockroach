// Copyright 2015 The Cockroach Authors.
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
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package ts

import (
	"container/heap"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
)

// calibratedData is used to calibrate an InternalTimeSeriesData object for
// use in a dataSpan.  This is accomplished by computing a constant offset
// adjustment which adjusts each Sample offset to be relative to the start time
// of the dataSpan, rather than the start time of the InternalTimeSeriesData.
type calibratedData struct {
	roachpb.InternalTimeSeriesData
	offsetAdjustment int32
}

// offsetAt returns the calibrated offset for the Sample at the supplied index
// in the InternalTimeSeriesData's Samples collection.
func (rdc *calibratedData) offsetAt(idx int) int32 {
	return rdc.Samples[idx].Offset + rdc.offsetAdjustment
}

// dataSpan is used to construct  monolithic view of a single time series over
// an arbitrary time span. The actual data in a span may be stored in multiple
// instances of InternalTimeSeriesData.
type dataSpan struct {
	startNanos  int64
	sampleNanos int64
	datas       []calibratedData
}

// timestampForOffset returns an appropriate timestamp for the given offset
// within a dataSpan. Because each offset represents a duration of time and not
// an exact time, the returned timestamp will fall exactly in the middle of the
// time slot represented by the offset.
func (ds *dataSpan) timestampForOffset(offset int32) int64 {
	return ds.startNanos + (int64(offset) * ds.sampleNanos) + (ds.sampleNanos / 2)
}

// addData adds an InternalTimeSeriesData object into this dataSpan, normalizing
// it to a calibratedData object in the process.
func (ds *dataSpan) addData(data roachpb.InternalTimeSeriesData) error {
	if data.SampleDurationNanos != ds.sampleNanos {
		return util.Errorf("data added to dataSpan with mismatched sample duration period")
	}

	// Reject data if there are no samples.
	if len(data.Samples) == 0 {
		return nil
	}

	// Calculate an offset adjustment which normalizes the supplied data into
	// the dataSpan's time period.
	adjustment := (data.StartTimestampNanos - ds.startNanos) / ds.sampleNanos
	rd := calibratedData{
		InternalTimeSeriesData: data,
		offsetAdjustment:       int32(adjustment),
	}

	// If all samples in the supplied data set are before calibrated offset 0,
	// do not include it.
	if rd.offsetAt(len(data.Samples)-1) < 0 {
		return nil
	}

	// If the supplied data does not occur after all previously added data,
	// return an error.
	if len(ds.datas) > 0 {
		last := ds.datas[len(ds.datas)-1]
		if rd.offsetAt(0) <= last.offsetAt(len(last.Samples)-1) {
			return util.Errorf("data must be added to dataSpan in chronological order")
		}
	}

	ds.datas = append(ds.datas, rd)
	return nil
}

// downsampleFn is a function which extracts a float64 value from a time series
// sample.
type downsampleFn func(roachpb.InternalTimeSeriesSample) float64

// dataSpanIterator is used to iterate through the samples in a dataSpan.
// Samples are spread across multiple InternalTimeSeriesData objects; this
// iterator thus maintains a two-level index to point to a unique sample.
type dataSpanIterator struct {
	*dataSpan
	offset       int32        // The calibrated offset of the current sample within the dataSpan
	dataIdx      int          // Index of InternalTimeSeriesData which contains current Sample
	sampleIdx    int          // Index of current Sample within InternalTimeSeriesData
	valid        bool         // True if this iterator points to a valid Sample
	downsampleFn downsampleFn // Function to extract float64 values from samples
}

// value returns a float64 value by applying downsampleFn to the
// InternalTimeSeriesSample value currently pointed to by this iterator.
func (dsi *dataSpanIterator) value() float64 {
	if !dsi.valid {
		panic(fmt.Sprintf("value called on invalid dataSpanIterator: %v", dsi))
	}
	return dsi.downsampleFn(dsi.datas[dsi.dataIdx].Samples[dsi.sampleIdx])
}

// advance moves the iterator to point to the next Sample.
func (dsi *dataSpanIterator) advance() {
	if !dsi.valid {
		return
	}
	data := dsi.datas[dsi.dataIdx]
	switch {
	case dsi.sampleIdx+1 < len(data.Samples):
		dsi.sampleIdx++
	case dsi.dataIdx+1 < len(dsi.datas):
		dsi.dataIdx++
		data = dsi.datas[dsi.dataIdx]
		dsi.sampleIdx = 0
	default:
		dsi.valid = false
	}
	dsi.offset = data.offsetAt(dsi.sampleIdx)
}

// interpolatingIterator is used to iterate over offsets within a dataSpan. The
// iterator can provide sample values for any offset, even if there is no actual
// sample in the dataSpan at that offset.
//
// Values for missing offsets are computed using linear interpolation from the
// nearest real samples preceding and following the missing offset.
type interpolatingIterator struct {
	offset   int32            // Current offset within dataSpan
	nextReal dataSpanIterator // Next sample with an offset >= iterator's offset
	prevReal dataSpanIterator // Prev sample with offset < iterator's offset
}

// advanceTo advances the iterator to the supplied offset.
func (ii *interpolatingIterator) advanceTo(offset int32) {
	ii.offset = offset
	// Advance real iterators until nextReal has offset >= the interpolated
	// offset.
	for ii.nextReal.valid && ii.nextReal.offset < ii.offset {
		ii.prevReal = ii.nextReal
		ii.nextReal.advance()
	}
}

// isValid returns true if this interpolatingIterator still points to valid data.
func (ii *interpolatingIterator) isValid() bool {
	return ii.nextReal.valid
}

// value returns the value at the current offset of this iterator.
func (ii *interpolatingIterator) value() float64 {
	if !ii.isValid() {
		return 0
	}
	if ii.nextReal.offset == ii.offset {
		return ii.nextReal.value()
	}
	// Cannot interpolate if previous value is invalid.
	if !ii.prevReal.valid {
		return 0
	}

	// Linear interpolation of value at the current offset.
	off := float64(ii.offset)
	nextAvg := ii.nextReal.value()
	nextOff := float64(ii.nextReal.offset)
	prevAvg := ii.prevReal.value()
	prevOff := float64(ii.prevReal.offset)
	return prevAvg + (nextAvg-prevAvg)*(off-prevOff)/(nextOff-prevOff)
}

// newIterator returns an interpolating iterator for the given dataSpan. The
// iterator is initialized to position startOffset, which should be 0 when
// querying non-derivatives and -1 when querying a derivative. Values returned
// by the iterator will be generated from samples using the supplied
// downsampleFn.
func (ds *dataSpan) newIterator(startOffset int32, downsampleFn downsampleFn) interpolatingIterator {
	if len(ds.datas) == 0 {
		return interpolatingIterator{}
	}

	// The first data index necessarily contains the positive offset closest to
	// 0, along with 0 itself and negative offsets. Use a binary search to
	// find the lowest offset greater than or equal to startOffset.
	data := ds.datas[0]
	innerIdx := sort.Search(len(data.Samples), func(i int) bool {
		return data.offsetAt(i) >= startOffset
	})

	iterator := interpolatingIterator{
		offset: startOffset,
		nextReal: dataSpanIterator{
			dataSpan:     ds,
			dataIdx:      0,
			sampleIdx:    innerIdx,
			offset:       data.offsetAt(innerIdx),
			valid:        true,
			downsampleFn: downsampleFn,
		},
	}

	// If innerIdx > 0, then we can compute a "previous" iterator as well; this
	// will let us interpolate a 0 value if it is not actually present in the
	// data.
	if innerIdx > 0 {
		iterator.prevReal = dataSpanIterator{
			dataSpan:     ds,
			dataIdx:      0,
			sampleIdx:    innerIdx - 1,
			offset:       data.offsetAt(innerIdx - 1),
			valid:        true,
			downsampleFn: downsampleFn,
		}
	}

	return iterator
}

// A unionIterator jointly advances multiple interpolatingIterators, visiting
// precisely those offsets for which at least one of the underlying
// interpolating iterators has a real (that is, non-interpolated) value.
//
// All valid iterators in the set will have the same offset at all times. During
// advancement, the next offset is chosen by finding the individual iterator
// with the lowest value of nextReal.offset; in other words, the iteratorSet
// will visit each possible offset in sequence, skipping offsets for which *no*
// iterators have real data.  If even a single iterator has real data at an
// offset, that offset will eventually be visited.
//
// In order to facilitate finding the lowest value of nextReal.offset, the set is
// organized as a min heap using Go's heap package.
type unionIterator []interpolatingIterator

// Len returns the length of the iteratorSet; needed by heap.Interface.
func (is unionIterator) Len() int {
	return len(is)
}

// Swap swaps the values at the two given indices; needed by heap.Interface.
func (is unionIterator) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

// Less determines if the iterator at the first supplied index in the
// iteratorSet is "Less" than the iterator at the second index; need by
// heap.Interface.
//
// An iterator is less than another if its nextReal iterator points to an
// earlier offset.
func (is unionIterator) Less(i, j int) bool {
	thisNext, otherNext := is[i].nextReal, is[j].nextReal
	if !(thisNext.valid || otherNext.valid) {
		return false
	}
	if !thisNext.valid {
		return false
	}
	if !otherNext.valid {
		return true
	}
	return thisNext.offset < otherNext.offset
}

// Push pushes an element into the iteratorSet heap; needed by heap.Interface
func (is *unionIterator) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*is = append(*is, x.(interpolatingIterator))
}

// Pop removes the minimum element from the iteratorSet heap; needed by
// heap.Interface.
func (is *unionIterator) Pop() interface{} {
	old := *is
	n := len(old)
	x := old[n-1]
	*is = old[0 : n-1]
	return x
}

// isValid returns true if at least one iterator in the set is still valid. This
// method only works if init() has already been called on the set.
func (is unionIterator) isValid() bool {
	return len(is) > 0 && is[0].isValid()
}

// init initializes the iteratorSet. This method moves all iterators to the
// first offset for which *any* iterator in the set has real data.
func (is unionIterator) init() {
	heap.Init(&is)
	if !is.isValid() {
		return
	}
	if is[0].nextReal.offset > 0 {
		is.advance()
	}
}

// advance advances each iterator in the set to the next value for which *any*
// interpolatingIterator has a real value.
func (is unionIterator) advance() {
	if !is.isValid() {
		return
	}

	// All iterators in the set currently point to the same offset. Advancement
	// begins by pre-advancing any iterators that have a real value for the
	// current offset.
	current := is[0].offset
	for is[0].offset == current {
		is[0].advanceTo(current + 1)
		heap.Fix(&is, 0)
	}

	// It is possible that all iterators are now invalid.
	if !is.isValid() {
		return
	}

	// The iterator in position zero now has the lowest value for
	// nextReal.offset - advance all iterators to that offset.
	min := is[0].nextReal.offset
	for i := range is {
		is[i].advanceTo(min)
	}
	heap.Init(&is)
}

// timestamp returns a timestamp for the current offset of the iterators in this
// set. Offsets are converted into timestamps before returning them as part of a
// query result.
func (is unionIterator) timestamp() int64 {
	if !is.isValid() {
		return 0
	}
	return is[0].nextReal.timestampForOffset(is[0].offset)
}

// offset returns the current offset of the iterator.
func (is unionIterator) offset() int32 {
	if !is.isValid() {
		return 0
	}
	return is[0].offset
}

// sum returns the sum of the current values in the iterator.
func (is unionIterator) sum() float64 {
	var sum float64
	for i := range is {
		sum = sum + is[i].value()
	}
	return sum
}

// avg returns the average of the current values in the iterator.
func (is unionIterator) avg() float64 {
	return is.sum() / float64(len(is))
}

// max return the maximum value of the current values in the iterator.
func (is unionIterator) max() float64 {
	max := is[0].value()
	for i := range is[1:] {
		val := is[i+1].value()
		if val > max {
			max = val
		}
	}
	return max
}

// min return the minimum value of the current values in the iterator.
func (is unionIterator) min() float64 {
	min := is[0].value()
	for i := range is[1:] {
		val := is[i+1].value()
		if val < min {
			min = val
		}
	}
	return min
}

// Query returns datapoints for the named time series during the supplied time
// span.  Data is returned as a series of consecutive data points.
//
// Data is queried only at the Resolution supplied: if data for the named time
// series is not stored at the given resolution, an empty result will be
// returned.
//
// All data stored on the server is downsampled to some degree; the data points
// returned represent the average value within a sample period. Each datapoint's
// timestamp falls in the middle of the sample period it represents.
//
// If data for the named time series was collected from multiple sources, each
// returned datapoint will represent the sum of datapoints from all sources at
// the same time. The returned string slices contains a list of all sources for
// the metric which were aggregated to produce the result.
func (db *DB) Query(query Query, r Resolution, startNanos, endNanos int64) ([]TimeSeriesDatapoint, []string, error) {
	// Normalize startNanos and endNanos the nearest SampleDuration boundary.
	startNanos -= startNanos % r.SampleDuration()

	var rows []client.KeyValue
	if len(query.Sources) == 0 {
		// Based on the supplied timestamps and resolution, construct start and end
		// keys for a scan that will return every key with data relevant to the
		// query.
		startKey := MakeDataKey(query.Name, "" /* source */, r, startNanos)
		endKey := MakeDataKey(query.Name, "" /* source */, r, endNanos).PrefixEnd()
		var pErr *roachpb.Error
		rows, pErr = db.db.ScanInconsistent(startKey, endKey, 0)
		if pErr != nil {
			return nil, nil, pErr.GoError()
		}
	} else {
		b := db.db.NewBatch()
		b.ReadConsistency = roachpb.INCONSISTENT
		// Iterate over all key timestamps which may contain data for the given
		// sources, based on the given start/end time and the resolution.
		kd := r.KeyDuration()
		startKeyNanos := startNanos - (startNanos % kd)
		endKeyNanos := endNanos - (endNanos % kd)
		for currentTimestamp := startKeyNanos; currentTimestamp <= endKeyNanos; currentTimestamp += kd {
			for _, source := range query.Sources {
				key := MakeDataKey(query.Name, source, r, currentTimestamp)
				b.Get(key)
			}
		}
		pErr := db.db.Run(b)
		if pErr != nil {
			return nil, nil, pErr.GoError()
		}
		for _, result := range b.Results {
			row := result.Rows[0]
			if row.Value == nil {
				continue
			}
			rows = append(rows, row)
		}
	}

	// Convert the queried source data into a set of data spans, one for each
	// source.
	sourceSpans, err := makeDataSpans(rows, startNanos)
	if err != nil {
		return nil, nil, err
	}

	// Compute a downsample function which will be used to return values from
	// each source for each sample period.
	downsampler, err := getDownsampleFunction(query.GetDownsampler())
	if err != nil {
		return nil, nil, err
	}

	// If we are returning a derivative, iteration needs to start at offset -1
	// (in order to correctly compute the rate of change at offset 0).
	var startOffset int32
	isDerivative := query.GetDerivative() != TimeSeriesQueryDerivative_NONE
	if isDerivative {
		startOffset = -1
	}

	// Create an interpolatingIterator for each dataSpan, adding each iterator
	// into a unionIterator collection. This is also where we compute a list of
	// all sources with data present in the query.
	sources := make([]string, 0, len(sourceSpans))
	iters := make(unionIterator, 0, len(sourceSpans))
	for name, span := range sourceSpans {
		sources = append(sources, name)
		iters = append(iters, span.newIterator(startOffset, downsampler))
	}

	// Choose an aggregation function to use when taking values from the
	// unionIterator.
	var valueFn func() float64
	switch query.GetSourceAggregator() {
	case TimeSeriesQueryAggregator_SUM:
		valueFn = iters.sum
	case TimeSeriesQueryAggregator_AVG:
		valueFn = iters.avg
	case TimeSeriesQueryAggregator_MAX:
		valueFn = iters.max
	case TimeSeriesQueryAggregator_MIN:
		valueFn = iters.min
	}

	// Iterate over all requested offsets, recording a value from the
	// unionIterator at each offset encountered. If the query is requesting a
	// derivative, a rate of change is recorded instead of the actual values.
	iters.init()
	var last TimeSeriesDatapoint
	if isDerivative {
		last = TimeSeriesDatapoint{
			TimestampNanos: iters.timestamp(),
			Value:          valueFn(),
		}
		// For derivatives, the iterator was initialized at offset -1 in order
		// to calculate the rate of change at offset zero. However, in some
		// cases (such as the very first value recorded) offset -1 is not
		// available. In this case, we treat the rate-of-change at the first
		// offset as zero.
		if iters.offset() < 0 {
			iters.advance()
		}
	}
	var responseData []TimeSeriesDatapoint
	for iters.isValid() && iters.timestamp() <= endNanos {
		current := TimeSeriesDatapoint{
			TimestampNanos: iters.timestamp(),
			Value:          valueFn(),
		}
		response := current
		if isDerivative {
			dTime := (current.TimestampNanos - last.TimestampNanos) / time.Second.Nanoseconds()
			if dTime == 0 {
				response.Value = 0
			} else {
				response.Value = (current.Value - last.Value) / float64(dTime)
			}
			if response.Value < 0 &&
				query.GetDerivative() == TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE {
				response.Value = 0
			}
		}
		responseData = append(responseData, response)
		last = current
		iters.advance()
	}

	return responseData, sources, nil
}

// makeDataSpans constructs a new dataSpan for each distinct source encountered
// in the query. Each dataspan will contain all data queried from a single
// source.
func makeDataSpans(rows []client.KeyValue, startNanos int64) (map[string]*dataSpan, error) {
	sourceSpans := make(map[string]*dataSpan)
	for _, row := range rows {
		var data roachpb.InternalTimeSeriesData
		if err := row.ValueProto(&data); err != nil {
			return nil, err
		}
		_, source, _, _, err := DecodeDataKey(row.Key)
		if err != nil {
			return nil, err
		}
		if _, ok := sourceSpans[source]; !ok {
			sourceSpans[source] = &dataSpan{
				startNanos:  startNanos,
				sampleNanos: data.SampleDurationNanos,
				datas:       make([]calibratedData, 0, 1),
			}
		}
		if err := sourceSpans[source].addData(data); err != nil {
			return nil, err
		}
	}
	return sourceSpans, nil
}

// getDownsampleFunction returns
func getDownsampleFunction(agg TimeSeriesQueryAggregator) (downsampleFn, error) {
	switch agg {
	case TimeSeriesQueryAggregator_AVG:
		return (roachpb.InternalTimeSeriesSample).Average, nil
	case TimeSeriesQueryAggregator_SUM:
		return (roachpb.InternalTimeSeriesSample).Summation, nil
	case TimeSeriesQueryAggregator_MAX:
		return (roachpb.InternalTimeSeriesSample).Maximum, nil
	case TimeSeriesQueryAggregator_MIN:
		return (roachpb.InternalTimeSeriesSample).Minimum, nil
	}
	return nil, util.Errorf("query specified unknown time series aggregator %s", agg.String())
}
