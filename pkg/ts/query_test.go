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
	"reflect"
	"sort"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

var (
	// testSeries1 has Data at timestamps [0, 50, 70, 90 and 140]. Data is split
	// across two InternalTimeSeriesData collections.
	testSeries1 = []roachpb.InternalTimeSeriesData{
		{
			StartTimestampNanos: 0,
			SampleDurationNanos: 10,
			Samples: []roachpb.InternalTimeSeriesSample{
				{
					Offset: 0,
					Count:  1,
					Sum:    1,
				},
				{
					Offset: 5,
					Count:  1,
					Sum:    5,
				},
				{
					Offset: 7,
					Count:  1,
					Sum:    10,
				},
			},
		},
		{
			StartTimestampNanos: 80,
			SampleDurationNanos: 10,
			Samples: []roachpb.InternalTimeSeriesSample{
				{
					Offset: 1,
					Count:  3,
					Sum:    60,
					Max:    proto.Float64(30),
					Min:    proto.Float64(10),
				},
				{
					Offset: 5,
					Count:  1,
					Sum:    0,
					Max:    proto.Float64(0),
					Min:    proto.Float64(0),
				},
				{
					Offset: 9,
					Count:  2,
					Sum:    80,
					Max:    proto.Float64(50),
					Min:    proto.Float64(30),
				},
			},
		},
	}
	// testSeries2 has Data at timestamps [30, 60, 70, 90 and 180].
	testSeries2 = []roachpb.InternalTimeSeriesData{
		{
			StartTimestampNanos: 30,
			SampleDurationNanos: 10,
			Samples: []roachpb.InternalTimeSeriesSample{
				{
					Offset: 0,
					Count:  1,
					Sum:    1,
				},
				{
					Offset: 3,
					Count:  5,
					Sum:    50,
					Max:    proto.Float64(10),
					Min:    proto.Float64(1),
				},
				{
					Offset: 4,
					Count:  1,
					Sum:    25,
				},
				{
					Offset: 6,
					Count:  3,
					Sum:    60,
					Max:    proto.Float64(30),
					Min:    proto.Float64(10),
				},
				{
					Offset: 15,
					Count:  2,
					Sum:    112,
					Max:    proto.Float64(100),
					Min:    proto.Float64(12),
				},
			},
		},
	}
)

// generateSimpleData generates an InternalTimeSeriesData object with multiple
// datapoints. Each datapoint has a value equal to its timestamp in nanos.
// Supplied timestamps are expected to be absolute, not relative to the given
// startTimestamp.
func generateSimpleData(startTimestamp int64, timestamps ...int64) roachpb.InternalTimeSeriesData {
	result := roachpb.InternalTimeSeriesData{
		StartTimestampNanos: startTimestamp,
		SampleDurationNanos: 10,
		Samples:             make([]roachpb.InternalTimeSeriesSample, len(timestamps)),
	}
	for i, ts := range timestamps {
		result.Samples[i] = roachpb.InternalTimeSeriesSample{
			Offset: int32((ts - startTimestamp) / 10),
			Count:  1,
			Sum:    float64(ts),
		}
	}
	return result
}

// completeDatapoint is a structure used by tests to quickly describe time
// series data points.
type completeDatapoint struct {
	timestamp int64
	sum       float64
	count     uint32
	min       float64
	max       float64
}

// generateComplexData generates more complicated InternalTimeSeriesData, where
// each contained point may have an explicit max and min.
func generateComplexData(
	startTimestamp, sampleDuration int64, dps []completeDatapoint,
) roachpb.InternalTimeSeriesData {
	result := roachpb.InternalTimeSeriesData{
		StartTimestampNanos: startTimestamp,
		SampleDurationNanos: sampleDuration,
		Samples:             make([]roachpb.InternalTimeSeriesSample, len(dps)),
	}
	for i, dp := range dps {
		result.Samples[i] = roachpb.InternalTimeSeriesSample{
			Offset: int32((dp.timestamp - startTimestamp) / sampleDuration),
			Count:  dp.count,
			Sum:    dp.sum,
			Max:    proto.Float64(dp.max),
			Min:    proto.Float64(dp.min),
		}
	}
	return result
}

func TestDataSpanIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ds := dataSpan{
		startNanos:  30,
		sampleNanos: 10,
	}
	testData := []roachpb.InternalTimeSeriesData{
		generateSimpleData(0, 0, 50, 70),
		generateSimpleData(80, 90, 140),
	}
	for _, data := range testData {
		if err := ds.addData(data); err != nil {
			t.Fatal(err)
		}
	}

	var iter dataSpanIterator
	checkNum := 0
	verifyIter := func(
		expectedValid bool, expectedOffset int32, expectedTimestamp int64, expectedValue float64,
	) {
		checkNum++
		if a, e := iter.isValid(), expectedValid; a != e {
			t.Fatalf("check %d: expected iterator valid to be %t, was %t", checkNum, e, a)
		}
		if !expectedValid {
			return
		}
		if a, e := iter.offset(), int32(expectedOffset); a != e {
			t.Fatalf("check %d: expected iterator to have offset %d, was %d", checkNum, e, a)
		}
		if a, e := iter.timestamp(), int64(expectedTimestamp); a != e {
			t.Fatalf(
				"check %d: expected iterator to have timestamp %d, was %d", checkNum, e, a,
			)
		}
		if a, e := iter.value(), expectedValue; a != e {
			t.Fatalf(
				"check %d: expected iterator to have value %f, was %f", checkNum, e, a,
			)
		}
	}

	// Corner cases we are looking to verify:
	// + Create new Iterator:
	//   - @offset with no real point (should be set to next point)
	//   - @offset before first point (should be set to first point)
	//   - @offset after last point (should be invalid)
	//   - @offset with point in a data set other than the first
	// + Advance iterator:
	//   - Normally from one point to next
	//   - Normally across data set boundary
	//   - From invalid state before first point
	//   - To invalid state at end of data
	// + Retreat iterator:
	//   - Normally from one point to previous
	//   - Normally across data set boundary
	//   - From invalid state after last point
	//   - To valid state at beginning of data

	// Create iterator at offset 0. Dataspan starts at 30, so this should
	// be set to the real datapoint at 50.
	iter = newDataSpanIterator(ds, 0, (roachpb.InternalTimeSeriesSample).Average)
	verifyIter(true, 2, 50, 50)

	// Retreat iterator: should be at offset -3, real timestamp 0.
	iter.retreat()
	verifyIter(true, -3, 0, 0)

	// Retreat iterator: should be invalid.
	iter.retreat()
	verifyIter(false, 0, 0, 0)

	// Advance iterator: should be at first point again.
	iter.advance()
	verifyIter(true, -3, 0, 0)

	// Advance iterator: should be at original point again.
	iter.advance()
	verifyIter(true, 2, 50, 50)

	// Advance iterator twice; should be set to point in next data set.
	iter.advance()
	iter.advance()
	verifyIter(true, 6, 90, 90)

	// Retreat iterator: should point to be set to point in previous span.
	iter.retreat()
	verifyIter(true, 4, 70, 70)

	// Advance iterator past last point.
	iter.advance()
	iter.advance()
	iter.advance()
	verifyIter(false, 0, 0, 0)

	// Retreat iterator: should be at the last data point in the set.
	iter.retreat()
	verifyIter(true, 11, 140, 140)

	// Create iterator at offset -100. Should be set to first point.
	iter = newDataSpanIterator(ds, -100, (roachpb.InternalTimeSeriesSample).Average)
	verifyIter(true, -3, 0, 0)

	// Create new iterator beyond the end of the scope: should be invalid.
	iter = newDataSpanIterator(ds, 1000, (roachpb.InternalTimeSeriesSample).Average)
	verifyIter(false, 0, 0, 0)
	iter.retreat()
	verifyIter(true, 11, 140, 140)

	// Create new iterator directly into second span.
	iter = newDataSpanIterator(ds, 6, (roachpb.InternalTimeSeriesSample).Average)
	verifyIter(true, 6, 90, 90)

	// Verify extraction functions on complex data: Avg, Sum, Min, Max
	ds = dataSpan{
		startNanos:  0,
		sampleNanos: 1,
	}
	testData = []roachpb.InternalTimeSeriesData{
		generateComplexData(0, 1, []completeDatapoint{
			{
				timestamp: 0,
				count:     2,
				sum:       10,
				max:       8,
				min:       2,
			},
			{
				timestamp: 5,
				count:     3,
				sum:       30,
				max:       15,
				min:       2,
			},
		}),
	}
	for _, data := range testData {
		if err := ds.addData(data); err != nil {
			t.Fatal(err)
		}
	}
	iter = newDataSpanIterator(ds, 0, (roachpb.InternalTimeSeriesSample).Average)
	verifyIter(true, 0, 0, 5)
	iter.advance()
	verifyIter(true, 5, 5, 10)

	iter = newDataSpanIterator(ds, 0, (roachpb.InternalTimeSeriesSample).Summation)
	verifyIter(true, 0, 0, 10)
	iter.advance()
	verifyIter(true, 5, 5, 30)

	iter = newDataSpanIterator(ds, 0, (roachpb.InternalTimeSeriesSample).Maximum)
	verifyIter(true, 0, 0, 8)
	iter.advance()
	verifyIter(true, 5, 5, 15)

	iter = newDataSpanIterator(ds, 0, (roachpb.InternalTimeSeriesSample).Minimum)
	verifyIter(true, 0, 0, 2)
	iter.advance()
	verifyIter(true, 5, 5, 2)
}

func TestDownsamplingIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ds := dataSpan{
		startNanos:  30,
		sampleNanos: 10,
	}
	testData := []roachpb.InternalTimeSeriesData{
		generateSimpleData(0, 0, 10, 20, 30, 50, 60),
		generateSimpleData(70, 70, 90, 100, 150, 170),
	}
	for _, data := range testData {
		if err := ds.addData(data); err != nil {
			t.Fatal(err)
		}
	}

	var iter downsamplingIterator
	checkNum := 0
	verifyIter := func(
		expectedValid bool, expectedOffset int32, expectedTimestamp int64, expectedValue float64,
	) {
		checkNum++
		if a, e := iter.isValid(), expectedValid; a != e {
			t.Fatalf("check %d: expected iterator valid to be %t, was %t", checkNum, e, a)
		}
		if !expectedValid {
			return
		}
		if a, e := iter.offset(), int32(expectedOffset); a != e {
			t.Fatalf("check %d: expected iterator to have offset %d, was %d", checkNum, e, a)
		}
		if a, e := iter.timestamp(), int64(expectedTimestamp); a != e {
			t.Fatalf(
				"check %d: expected iterator to have timestamp %d, was %d", checkNum, e, a,
			)
		}
		if a, e := iter.value(), float64(expectedValue); a != e {
			t.Fatalf(
				"check %d: expected iterator to have value %f, was %f", checkNum, e, a,
			)
		}
	}

	// Initialize iterator to offset zero, which represents timestamp 30.
	iter = newDownsamplingIterator(ds, 0, 30, (roachpb.InternalTimeSeriesSample).Average, downsampleAvg)
	verifyIter(true, 0, 30, 40)

	// Retreat iterator - should point to offset -1.
	iter.retreat()
	verifyIter(true, -1, 0, 10)

	// Retreat iterator - should be invalid.
	iter.retreat()
	verifyIter(false, -1, 0, 10)

	// Advance iterator - should be valid again.
	iter.advance()
	verifyIter(true, -1, 0, 10)

	// Advance iterator twice - this offset straddles two datasets in the
	// underlying data.
	iter.advance()
	iter.advance()
	verifyIter(true, 1, 60, 65)

	// Advance and retreat iterator, make sure it still straddles correctly.
	iter.advance()
	iter.retreat()
	verifyIter(true, 1, 60, 65)

	// Retreat to ensure that iterator can retreat from straddled position.
	iter.retreat()
	verifyIter(true, 0, 30, 40)

	// Advance iterator thrice, should be at offset 4 (no points at 3)
	iter.advance()
	iter.advance()
	iter.advance()
	verifyIter(true, 4, 150, 160)

	// Advance iterator; should be invalid.
	iter.advance()
	verifyIter(false, 0, 0, 0)

	// Retreat iterator, should be valid again.
	iter.retreat()
	verifyIter(true, 4, 150, 160)

	// Initialize iterator at high offset.
	iter = newDownsamplingIterator(ds, 5, 30, (roachpb.InternalTimeSeriesSample).Average, downsampleAvg)
	verifyIter(false, 0, 0, 0)
	iter.retreat()
	verifyIter(true, 4, 150, 160)

	// Initialize iterator at low offset. Should be set to first point.
	iter = newDownsamplingIterator(ds, -3, 30, (roachpb.InternalTimeSeriesSample).Average, downsampleAvg)
	verifyIter(true, -1, 0, 10)

	// Initialize iterator at staggered offset.
	iter = newDownsamplingIterator(ds, 1, 30, (roachpb.InternalTimeSeriesSample).Average, downsampleAvg)
	verifyIter(true, 1, 60, 65)

	// Verify extraction and downsampling functions on complex data: Avg, Sum,
	// Min, Max.
	ds = dataSpan{
		startNanos:  0,
		sampleNanos: 1,
	}
	testData = []roachpb.InternalTimeSeriesData{
		generateComplexData(0, 1, []completeDatapoint{
			{
				timestamp: 0,
				count:     2,
				sum:       10,
				max:       8,
				min:       2,
			},
			{
				timestamp: 5,
				count:     3,
				sum:       30,
				max:       15,
				min:       2,
			},
			{
				timestamp: 10,
				count:     3,
				sum:       30,
				max:       15,
				min:       2,
			},
			{
				timestamp: 15,
				count:     2,
				sum:       100,
				max:       55,
				min:       45,
			},
		}),
	}
	for _, data := range testData {
		if err := ds.addData(data); err != nil {
			t.Fatal(err)
		}
	}
	iter = newDownsamplingIterator(ds, 0, 10, (roachpb.InternalTimeSeriesSample).Average, downsampleAvg)
	verifyIter(true, 0, 0, 8)
	iter.advance()
	verifyIter(true, 1, 10, 26)

	iter = newDownsamplingIterator(ds, 0, 10, (roachpb.InternalTimeSeriesSample).Summation, downsampleSum)
	verifyIter(true, 0, 0, 40)
	iter.advance()
	verifyIter(true, 1, 10, 130)

	iter = newDownsamplingIterator(ds, 0, 10, (roachpb.InternalTimeSeriesSample).Maximum, downsampleMax)
	verifyIter(true, 0, 0, 15)
	iter.advance()
	verifyIter(true, 1, 10, 55)

	iter = newDownsamplingIterator(ds, 0, 10, (roachpb.InternalTimeSeriesSample).Minimum, downsampleMin)
	verifyIter(true, 0, 0, 2)
	iter.advance()
	verifyIter(true, 1, 10, 45)
}

// TestInterpolation verifies the interpolated average values of a single interpolatingIterator.
func TestInterpolation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ds := dataSpan{
		startNanos:  30,
		sampleNanos: 10,
	}
	for _, data := range testSeries1 {
		if err := ds.addData(data); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		expected   []float64
		derivative tspb.TimeSeriesQueryDerivative
		extractFn  extractFn
	}{
		{
			[]float64{3.4, 4.2, 5, 7.5, 10, 15, 20, 15, 10, 5, 0, 10, 20, 30, 40, 0},
			tspb.TimeSeriesQueryDerivative_NONE,
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Average()
			},
		},
		{
			[]float64{3.4, 4.2, 5, 7.5, 10, 20, 30, 22.5, 15, 7.5, 0, 12.5, 25, 37.5, 50, 0},
			tspb.TimeSeriesQueryDerivative_NONE,
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Maximum()
			},
		},
		{
			[]float64{3.4, 4.2, 5, 7.5, 10, 35, 60, 45, 30, 15, 0, 20, 40, 60, 80, 0},
			tspb.TimeSeriesQueryDerivative_NONE,
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Sum
			},
		},
		{
			[]float64{0.8, 0.8, 0.8, 2.5, 2.5, 5, 5, -5, -5, -5, -5, 10, 10, 10, 10, 0},
			tspb.TimeSeriesQueryDerivative_DERIVATIVE,
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Average()
			},
		},
		{
			[]float64{0.8, 0.8, 0.8, 2.5, 2.5, 5, 5, 0, 0, 0, 0, 10, 10, 10, 10, 0},
			tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE,
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Average()
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			actual := make([]float64, 0, len(tc.expected))
			iter := newInterpolatingIterator(ds, 0, 10, tc.extractFn, downsampleSum, tc.derivative)
			for i := 0; i < len(tc.expected); i++ {
				iter.advanceTo(int32(i))
				actual = append(actual, iter.value())
			}
			if !reflect.DeepEqual(actual, tc.expected) {
				t.Fatalf("interpolated values: %v, expected values: %v", actual, tc.expected)
			}
		})
	}
}

// TestAggregation verifies the behavior of an iteratorSet, which
// advances multiple interpolatingIterators together.
func TestAggregation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dataSpan1 := dataSpan{
		startNanos:  30,
		sampleNanos: 10,
	}
	dataSpan2 := dataSpan{
		startNanos:  30,
		sampleNanos: 10,
	}
	for _, data := range testSeries1 {
		if err := dataSpan1.addData(data); err != nil {
			t.Fatal(err)
		}
	}
	for _, data := range testSeries2 {
		if err := dataSpan2.addData(data); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		expected []float64
		aggFunc  func(ui aggregatingIterator) float64
	}{
		{
			[]float64{4.4, 12, 17.5, 35, 40, 36, 92, 56},
			func(ui aggregatingIterator) float64 {
				return ui.sum()
			},
		},
		{
			[]float64{3.4, 7, 10, 25, 20, 36, 52, 56},
			func(ui aggregatingIterator) float64 {
				return ui.max()
			},
		},
		{
			[]float64{1, 5, 7.5, 10, 20, 0, 40, 0},
			func(ui aggregatingIterator) float64 {
				return ui.min()
			},
		},
		{
			[]float64{2.2, 6, 8.75, 17.5, 20, 18, 46, 28},
			func(ui aggregatingIterator) float64 {
				return ui.avg()
			},
		},
	}

	extractFn := func(s roachpb.InternalTimeSeriesSample) float64 {
		return s.Average()
	}
	for i, tc := range testCases {
		actual := make([]float64, 0, len(tc.expected))
		iters := aggregatingIterator{
			newInterpolatingIterator(
				dataSpan1, 0, 10, extractFn, downsampleSum, tspb.TimeSeriesQueryDerivative_NONE,
			),
			newInterpolatingIterator(
				dataSpan2, 0, 10, extractFn, downsampleSum, tspb.TimeSeriesQueryDerivative_NONE,
			),
		}
		iters.init()
		for iters.isValid() {
			actual = append(actual, tc.aggFunc(iters))
			iters.advance()
		}
		if !reflect.DeepEqual(actual, tc.expected) {
			t.Errorf("test %d aggregated values: %v, expected values: %v", i, actual, tc.expected)
		}
	}
}

// assertQuery generates a query result from the local test model and compares
// it against the query returned from the server.
func (tm *testModel) assertQuery(
	name string,
	sources []string,
	downsample, agg *tspb.TimeSeriesQueryAggregator,
	derivative *tspb.TimeSeriesQueryDerivative,
	r Resolution,
	sampleDuration, start, end int64,
	expectedDatapointCount, expectedSourceCount int,
) {
	// Query the actual server.
	q := tspb.Query{
		Name:             name,
		Downsampler:      downsample,
		SourceAggregator: agg,
		Derivative:       derivative,
		Sources:          sources,
	}
	actualDatapoints, actualSources, err := tm.DB.Query(context.TODO(), q, r, sampleDuration, start, end)
	if err != nil {
		tm.t.Fatal(err)
	}
	if a, e := len(actualDatapoints), expectedDatapointCount; a != e {
		tm.t.Logf("actual datapoints: %v", actualDatapoints)
		tm.t.Fatal(errors.Errorf("query expected %d datapoints, got %d", e, a))
	}
	if a, e := len(actualSources), expectedSourceCount; a != e {
		tm.t.Fatal(errors.Errorf("query expected %d sources, got %d", e, a))
	}

	// Construct an expected result for comparison.
	var expectedDatapoints []tspb.TimeSeriesDatapoint
	expectedSources := make([]string, 0)
	dataSpans := make(map[string]*dataSpan)

	// If no specific sources were provided, look for data from every source
	// encountered by the test model.
	var sourcesToCheck map[string]struct{}
	if len(sources) == 0 {
		sourcesToCheck = tm.seenSources
	} else {
		sourcesToCheck = make(map[string]struct{})
		for _, s := range sources {
			sourcesToCheck[s] = struct{}{}
		}
	}

	// Iterate over all possible sources which may have data for this query.
	for sourceName := range sourcesToCheck {
		// Iterate over all possible key times at which query data may be present.
		for time := start - (start % r.SlabDuration()); time < end; time += r.SlabDuration() {
			// Construct a key for this source/time and retrieve it from model.
			key := MakeDataKey(name, sourceName, r, time)
			value, ok := tm.modelData[string(key)]
			if !ok {
				continue
			}

			// Add data from the key to the correct dataSpan.
			data, err := value.GetTimeseries()
			if err != nil {
				tm.t.Fatal(err)
			}
			ds, ok := dataSpans[sourceName]
			if !ok {
				ds = &dataSpan{
					startNanos:  start - (start % r.SampleDuration()),
					sampleNanos: r.SampleDuration(),
				}
				dataSpans[sourceName] = ds
				expectedSources = append(expectedSources, sourceName)
			}
			if err := ds.addData(data); err != nil {
				tm.t.Fatal(err)
			}
		}
	}

	// Verify that expected sources match actual sources.
	sort.Strings(expectedSources)
	sort.Strings(actualSources)
	if !reflect.DeepEqual(actualSources, expectedSources) {
		tm.t.Error(errors.Errorf("actual source list: %v, expected: %v", actualSources, expectedSources))
	}

	// Iterate over data in all dataSpans and construct expected datapoints.
	extractFn, err := getExtractionFunction(q.GetDownsampler())
	if err != nil {
		tm.t.Fatal(err)
	}
	downsampleFn, err := getDownsampleFunction(q.GetDownsampler())
	if err != nil {
		tm.t.Fatal(err)
	}
	var iters aggregatingIterator
	for _, ds := range dataSpans {
		iters = append(
			iters,
			newInterpolatingIterator(
				*ds, 0, sampleDuration, extractFn, downsampleFn, q.GetDerivative(),
			),
		)
	}

	iters.init()
	if !iters.isValid() {
		if a, e := 0, len(expectedDatapoints); a != e {
			tm.t.Error(errors.Errorf("query had zero datapoints, expected: %v", expectedDatapoints))
		}
		return
	}
	currentVal := func() tspb.TimeSeriesDatapoint {
		var value float64
		switch q.GetSourceAggregator() {
		case tspb.TimeSeriesQueryAggregator_SUM:
			value = iters.sum()
		case tspb.TimeSeriesQueryAggregator_AVG:
			value = iters.avg()
		case tspb.TimeSeriesQueryAggregator_MAX:
			value = iters.max()
		case tspb.TimeSeriesQueryAggregator_MIN:
			value = iters.min()
		default:
			tm.t.Fatalf("unknown query aggregator %s", q.GetSourceAggregator())
		}
		return tspb.TimeSeriesDatapoint{
			TimestampNanos: iters.timestamp(),
			Value:          value,
		}
	}

	for iters.isValid() && iters.timestamp() <= end {
		result := currentVal()
		if q.GetDerivative() != tspb.TimeSeriesQueryDerivative_NONE {
			result.Value = result.Value / float64(sampleDuration) * float64(time.Second.Nanoseconds())
		}
		expectedDatapoints = append(expectedDatapoints, result)
		iters.advance()
	}

	if !reflect.DeepEqual(actualDatapoints, expectedDatapoints) {
		tm.t.Error(errors.Errorf("actual datapoints: %v, expected: %v", actualDatapoints, expectedDatapoints))
	}
}

// TestQuery validates that query results match the expectation of the test
// model.
func TestQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
		{
			Name: "test.metric",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(1, 100),
				datapoint(5, 200),
				datapoint(15, 300),
				datapoint(16, 400),
				datapoint(17, 500),
				datapoint(22, 600),
				datapoint(52, 900),
			},
		},
	})
	tm.assertKeyCount(4)
	tm.assertModelCorrect()
	tm.assertQuery("test.metric", nil, nil, nil, nil, resolution1ns, 1, 0, 60, 7, 1)

	// Verify across multiple sources
	tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
		{
			Name:   "test.multimetric",
			Source: "source1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(1, 100),
				datapoint(15, 300),
				datapoint(17, 500),
				datapoint(52, 900),
			},
		},
		{
			Name:   "test.multimetric",
			Source: "source2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(5, 100),
				datapoint(16, 300),
				datapoint(22, 500),
				datapoint(82, 900),
			},
		},
	})

	tm.assertKeyCount(11)
	tm.assertModelCorrect()

	// Test default query: avg downsampler, sum aggregator, no derivative.
	tm.assertQuery("test.multimetric", nil, nil, nil, nil, resolution1ns, 1, 0, 90, 8, 2)
	// Test with aggregator specified.
	tm.assertQuery("test.multimetric", nil, tspb.TimeSeriesQueryAggregator_MAX.Enum(), nil, nil,
		resolution1ns, 1, 0, 90, 8, 2)
	// Test with aggregator and downsampler.
	tm.assertQuery("test.multimetric", nil, tspb.TimeSeriesQueryAggregator_MAX.Enum(), tspb.TimeSeriesQueryAggregator_AVG.Enum(), nil,
		resolution1ns, 1, 0, 90, 8, 2)
	// Test with derivative specified.
	tm.assertQuery("test.multimetric", nil, tspb.TimeSeriesQueryAggregator_AVG.Enum(), nil,
		tspb.TimeSeriesQueryDerivative_DERIVATIVE.Enum(), resolution1ns, 1, 0, 90, 8, 2)
	// Test with everything specified.
	tm.assertQuery("test.multimetric", nil, tspb.TimeSeriesQueryAggregator_MIN.Enum(), tspb.TimeSeriesQueryAggregator_MAX.Enum(),
		tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE.Enum(), resolution1ns, 1, 0, 90, 8, 2)

	// Test queries that return no data. Check with every
	// aggregator/downsampler/derivative combination. This situation is
	// particularly prone to nil panics (parts of the query system will not have
	// data).
	aggs := []tspb.TimeSeriesQueryAggregator{
		tspb.TimeSeriesQueryAggregator_MIN, tspb.TimeSeriesQueryAggregator_MAX,
		tspb.TimeSeriesQueryAggregator_AVG, tspb.TimeSeriesQueryAggregator_SUM,
	}
	derivs := []tspb.TimeSeriesQueryDerivative{
		tspb.TimeSeriesQueryDerivative_NONE, tspb.TimeSeriesQueryDerivative_DERIVATIVE,
		tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE,
	}
	for _, downsampler := range aggs {
		for _, agg := range aggs {
			for _, deriv := range derivs {
				tm.assertQuery(
					"nodata",
					nil,
					downsampler.Enum(),
					agg.Enum(),
					deriv.Enum(),
					resolution1ns,
					1,
					0,
					90,
					0,
					0,
				)
			}
		}
	}

	// Verify querying specific sources, thus excluding other available sources
	// in the same time period.
	tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
		{
			Name:   "test.specificmetric",
			Source: "source1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(1, 9999),
				datapoint(11, 9999),
				datapoint(21, 9999),
				datapoint(31, 9999),
			},
		},
		{
			Name:   "test.specificmetric",
			Source: "source2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(2, 10),
				datapoint(12, 15),
				datapoint(22, 25),
				datapoint(32, 60),
			},
		},
		{
			Name:   "test.specificmetric",
			Source: "source3",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(3, 9999),
				datapoint(13, 9999),
				datapoint(23, 9999),
				datapoint(33, 9999),
			},
		},
		{
			Name:   "test.specificmetric",
			Source: "source4",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(4, 15),
				datapoint(14, 45),
				datapoint(24, 60),
				datapoint(32, 100),
			},
		},
		{
			Name:   "test.specificmetric",
			Source: "source5",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(5, 9999),
				datapoint(15, 9999),
				datapoint(25, 9999),
				datapoint(35, 9999),
			},
		},
	})

	tm.assertKeyCount(31)
	tm.assertModelCorrect()

	// Assert querying data from subset of sources. Includes source with no
	// data.
	tm.assertQuery("test.specificmetric", []string{"source2", "source4", "source6"}, nil, nil, nil, resolution1ns, 1, 0, 90, 7, 2)

	// Assert querying data over limited range for single source. Regression
	// test for #4987.
	tm.assertQuery("test.specificmetric", []string{"source4", "source5"}, nil, nil, nil, resolution1ns, 1, 5, 24, 4, 2)
}

// TestQueryDownsampling validates that query results match the expectation of
// the test model.
func TestQueryDownsampling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	// Query with sampleDuration that is too small, expect error.
	_, _, err := tm.DB.Query(context.TODO(), tspb.Query{}, Resolution10s, 1, 0, 10000)
	if err == nil {
		t.Fatal("expected query to fail with sampleDuration less than resolution allows.")
	}
	errorStr := "was not less"
	if !testutils.IsError(err, errorStr) {
		t.Fatalf("expected error from bad query to match %q, got error %q", errorStr, err.Error())
	}

	// Query with sampleDuration which is not an even multiple of the resolution.
	_, _, err = tm.DB.Query(
		context.TODO(), tspb.Query{}, Resolution10s, Resolution10s.SampleDuration()+1, 0, 10000,
	)
	if err == nil {
		t.Fatal("expected query to fail with sampleDuration not an even multiple of the query resolution.")
	}
	errorStr = "not a multiple"
	if !testutils.IsError(err, errorStr) {
		t.Fatalf("expected error from bad query to match %q, got error %q", errorStr, err.Error())
	}

	tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
		{
			Name:   "test.metric",
			Source: "source1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(1, 100),
				datapoint(5, 500),
				datapoint(15, 500),
				datapoint(16, 600),
				datapoint(17, 700),
				datapoint(22, 200),
				datapoint(45, 500),
				datapoint(46, 600),
				datapoint(52, 200),
			},
		},
		{
			Name:   "test.metric",
			Source: "source2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(7, 0),
				datapoint(7, 700),
				datapoint(9, 900),
				datapoint(14, 400),
				datapoint(18, 800),
				datapoint(33, 300),
				datapoint(34, 400),
				datapoint(56, 600),
				datapoint(59, 900),
			},
		},
	})
	tm.assertKeyCount(9)
	tm.assertModelCorrect()

	tm.assertQuery("test.metric", nil, nil, nil, nil, resolution1ns, 10, 0, 60, 6, 2)
	tm.assertQuery("test.metric", []string{"source1"}, nil, nil, nil, resolution1ns, 10, 0, 60, 5, 1)
	tm.assertQuery("test.metric", []string{"source2"}, nil, nil, nil, resolution1ns, 10, 0, 60, 4, 1)
}
