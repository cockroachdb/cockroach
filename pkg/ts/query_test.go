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

package ts

import (
	"context"
	"math"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// dataSample is a basic data type that represents a single time series sample:
// a timestamp value pair.
type dataSample struct {
	timestamp int64
	value     float64
}

// makeInternalData makes an InternalTimeSeriesData object from a collection of data
// samples. Input is the start timestamp, the sample duration, and the set of
// samples. Sample data must be ordered by timestamp.
func makeInternalData(
	startTimestamp, sampleDuration int64, samples []dataSample,
) roachpb.InternalTimeSeriesData {
	// Adjust startTimestamp to an exact multiple of sampleDuration.
	startTimestamp -= startTimestamp % sampleDuration
	result := roachpb.InternalTimeSeriesData{
		StartTimestampNanos: startTimestamp,
		SampleDurationNanos: sampleDuration,
		Samples:             make([]roachpb.InternalTimeSeriesSample, 0),
	}

	// Run through all samples, merging any consecutive samples which correspond
	// to the same sample interval.
	for _, sample := range samples {
		offset := int32((sample.timestamp - startTimestamp) / sampleDuration)
		value := sample.value

		// Merge into the previous sample if we have the same offset.
		if count := len(result.Samples); count > 0 && result.Samples[count-1].Offset == offset {
			// Initialize max and min if necessary.
			var min, max float64
			if result.Samples[count-1].Count > 1 {
				min, max = *result.Samples[count-1].Min, *result.Samples[count-1].Max
			} else {
				min, max = result.Samples[count-1].Sum, result.Samples[count-1].Sum
			}

			result.Samples[count-1].Count++
			result.Samples[count-1].Sum += value
			result.Samples[count-1].Min = proto.Float64(math.Min(min, value))
			result.Samples[count-1].Max = proto.Float64(math.Max(max, value))
		} else if count > 0 && result.Samples[count-1].Offset > offset {
			panic("sample data provided to generateData must be ordered by timestamp.")
		} else {
			result.Samples = append(result.Samples, roachpb.InternalTimeSeriesSample{
				Offset: offset,
				Sum:    value,
				Count:  1,
			})
		}
	}

	return result
}

func makeDataSpan(
	startNanos, sampleNanos int64, internalDatas ...roachpb.InternalTimeSeriesData,
) dataSpan {
	ds := dataSpan{
		startNanos:  startNanos,
		sampleNanos: sampleNanos,
	}
	for _, data := range internalDatas {
		if err := ds.addData(data); err != nil {
			panic(err)
		}
	}
	return ds
}

func TestDataSpanIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ds := makeDataSpan(30, 10,
		makeInternalData(0, 10, []dataSample{
			{0, 0},
			{50, 50},
			{70, 70},
		}),
		makeInternalData(80, 10, []dataSample{
			{90, 90},
			{140, 140},
		}),
	)

	var iter dataSpanIterator
	verifyIter := func(
		expectedValid bool, expectedOffset int32, expectedTimestamp int64, expectedValue float64,
	) {
		t.Helper()
		if a, e := iter.isValid(), expectedValid; a != e {
			t.Fatalf("iterator valid = %t, wanted %t", a, e)
		}
		if !expectedValid {
			return
		}
		if a, e := iter.offset(), expectedOffset; a != e {
			t.Fatalf("iterator offset = %d, wanted %d", a, e)
		}
		if a, e := iter.timestamp(), expectedTimestamp; a != e {
			t.Fatalf(
				"iterator timestamp = %d, wanted %d", a, e,
			)
		}
		if a, e := iter.value(), expectedValue; a != e {
			t.Fatalf(
				"iterator value = %f, wanted %f", a, e,
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
	ds = makeDataSpan(0, 1,
		makeInternalData(0, 1, []dataSample{
			{0, 8},
			{0, 2},
			{5, 15},
			{5, 13},
			{5, 2},
		}),
	)

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
	ds := makeDataSpan(30, 10,
		makeInternalData(0, 10, []dataSample{
			{0, 0},
			{10, 10},
			{20, 20},
			{30, 30},
			{50, 50},
			{60, 60},
		}),
		makeInternalData(70, 10, []dataSample{
			{70, 70},
			{90, 90},
			{100, 100},
			{150, 150},
			{170, 170},
		}),
	)

	var iter downsamplingIterator
	verifyIter := func(
		expectedValid bool, expectedOffset int32, expectedTimestamp int64, expectedValue float64,
	) {
		t.Helper()
		if a, e := iter.isValid(), expectedValid; a != e {
			t.Fatalf("iterator valid = %t, wanted %t", a, e)
		}
		if !expectedValid {
			return
		}
		if a, e := iter.offset(), expectedOffset; a != e {
			t.Fatalf("iterator offset = %d, wanted %d", a, e)
		}
		if a, e := iter.timestamp(), expectedTimestamp; a != e {
			t.Fatalf(
				"iterator timestamp = %d, wanted %d", a, e,
			)
		}
		if a, e := iter.value(), expectedValue; a != e {
			t.Fatalf(
				"iterator value = %f, wanted %f", a, e,
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
	ds = makeDataSpan(0, 1,
		makeInternalData(0, 1, []dataSample{
			{0, 8},
			{0, 2},
			{5, 15},
			{5, 13},
			{5, 2},
			{10, 15},
			{10, 13},
			{10, 2},
			{15, 55},
			{15, 45},
		}),
	)

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

	// Split data across two InternalTimeSeriesData slabs. The dataSpan is
	// responsible for making this cohesive.
	ds := makeDataSpan(30, 10,
		makeInternalData(0, 10, []dataSample{
			{0, 1},
			{50, 5},
			{70, 10},
		}),
		makeInternalData(80, 10, []dataSample{
			{90, 30},
			{90, 20},
			{90, 10},
			{130, 0},
			{170, 50},
			{170, 30},
		}),
	)

	testCases := []struct {
		expected    []float64
		derivative  tspb.TimeSeriesQueryDerivative
		maxDistance int32
		extractFn   extractFn
	}{
		// Extraction function tests.
		{
			[]float64{3.4, 4.2, 5, 7.5, 10, 15, 20, 15, 10, 5, 0, 10, 20, 30, 40},
			tspb.TimeSeriesQueryDerivative_NONE,
			0,
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Average()
			},
		},
		{
			[]float64{3.4, 4.2, 5, 7.5, 10, 20, 30, 22.5, 15, 7.5, 0, 12.5, 25, 37.5, 50},
			tspb.TimeSeriesQueryDerivative_NONE,
			0,
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Maximum()
			},
		},
		{
			[]float64{3.4, 4.2, 5, 7.5, 10, 35, 60, 45, 30, 15, 0, 20, 40, 60, 80},
			tspb.TimeSeriesQueryDerivative_NONE,
			0,
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Sum
			},
		},
		{
			[]float64{0.8, 0.8, 0.8, 2.5, 2.5, 5, 5, -5, -5, -5, -5, 10, 10, 10, 10},
			tspb.TimeSeriesQueryDerivative_DERIVATIVE,
			0,
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Average()
			},
		},
		{
			[]float64{0.8, 0.8, 0.8, 2.5, 2.5, 5, 5, 0, 0, 0, 0, 10, 10, 10, 10},
			tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE,
			0,
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Average()
			},
		},
		// Max distance tests.
		{
			[]float64{3.4, 4.2, 5, 7.5, 10, 15, 20, 15, 10, 5, 0, 10, 20, 30, 40},
			tspb.TimeSeriesQueryDerivative_NONE,
			10, // 10 is sufficient to bridge every gap in the data.
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Average()
			},
		},
		{
			[]float64{5, 7.5, 10, 15, 20, 0, 40},
			tspb.TimeSeriesQueryDerivative_NONE,
			2, // Distance of 2 only bridges some gaps.
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Average()
			},
		},
		{
			[]float64{5, 10, 20, 0, 40},
			tspb.TimeSeriesQueryDerivative_NONE,
			1, // Distance of 1 effectively disables interpolation.
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Average()
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			actual := make([]float64, 0, len(tc.expected))
			iter := newInterpolatingIterator(
				ds, 0, 10, tc.maxDistance, tc.extractFn, downsampleSum, tc.derivative,
			)
			for i := 0; iter.isValid(); i++ {
				iter.advanceTo(int32(i))
				if value, valid := iter.value(); valid {
					actual = append(actual, value)
				}
			}
			if !reflect.DeepEqual(actual, tc.expected) {
				t.Fatalf("interpolated values: %v, wanted: %v", actual, tc.expected)
			}
		})
	}
}

// TestAggregation verifies the behavior of an iteratorSet, which
// advances multiple interpolatingIterators together.
func TestAggregation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dataSpan1 := makeDataSpan(30, 10,
		makeInternalData(0, 10, []dataSample{
			{0, 1},
			{50, 5},
			{70, 10},
		}),
		makeInternalData(80, 10, []dataSample{
			{90, 30},
			{90, 20},
			{90, 10},
			{130, 0},
			{170, 50},
			{170, 30},
		}),
	)
	dataSpan2 := makeDataSpan(30, 10,
		makeInternalData(30, 10, []dataSample{
			{30, 1},
			{60, 10},
			{60, 20},
			{60, 10},
			{60, 9},
			{60, 1},
			{70, 25},
			{90, 30},
			{90, 20},
			{90, 10},
			{180, 100},
			{180, 12},
		}),
	)

	testCases := []struct {
		expected    []float64
		maxDistance int32
		aggFunc     func(aggregatingIterator) func() (float64, bool)
	}{
		{
			[]float64{4.4, 12, 17.5, 35, 40, 36, 92, 56},
			0,
			func(ai aggregatingIterator) func() (float64, bool) {
				return ai.sum
			},
		},
		{
			[]float64{3.4, 7, 10, 25, 20, 36, 52, 56},
			0,
			func(ai aggregatingIterator) func() (float64, bool) {
				return ai.max
			},
		},
		{
			[]float64{1, 5, 7.5, 10, 20, 0, 40, 56},
			0,
			func(ai aggregatingIterator) func() (float64, bool) {
				return ai.min
			},
		},
		{
			[]float64{2.2, 6, 8.75, 17.5, 20, 18, 46, 56},
			0,
			func(ai aggregatingIterator) func() (float64, bool) {
				return ai.avg
			},
		},
		// Interpolation max distance tests.
		{
			[]float64{2.2, 6, 8.75, 17.5, 20, 18, 46, 56},
			10, // Distance of 10 will bridge every gap in the data.
			func(ai aggregatingIterator) func() (float64, bool) {
				return ai.avg
			},
		},
		{
			[]float64{1, 5, 8.75, 17.5, 20, 0, 40, 56},
			// Distance of 2 will only bridge some gaps, meaning that some data points
			// will only have a contribution from one of the two data sets.
			2,
			func(ai aggregatingIterator) func() (float64, bool) {
				return ai.avg
			},
		},
		{
			[]float64{1, 5, 10, 17.5, 20, 0, 40, 56},
			1, // Distance of 1 disables interpolation.
			func(ai aggregatingIterator) func() (float64, bool) {
				return ai.avg
			},
		},
		// Leading edge filter tests.
		{
			[]float64{2.2, 6, 8.75, 17.5, 20, 18, 46, 56},
			10,
			func(ai aggregatingIterator) func() (float64, bool) {
				// Leading edge very far in the future, do not trim incomplete points.
				return ai.makeLeadingEdgeFilter(aggregatingIterator.avg, 2000)
			},
		},
		{
			[]float64{2.2, 6, 8.75, 17.5, 20, 18, 46},
			10,
			func(ai aggregatingIterator) func() (float64, bool) {
				// Trim leading edge.
				return ai.makeLeadingEdgeFilter(aggregatingIterator.avg, 170)
			},
		},
		{
			[]float64{2.2, 6, 8.75, 17.5, 20, 18, 46},
			10,
			func(ai aggregatingIterator) func() (float64, bool) {
				// Odd behavior, but set leading edge of zero; should filter all partial
				// points.
				return ai.makeLeadingEdgeFilter(aggregatingIterator.avg, 0)
			},
		},
		{
			[]float64{3.4, 7, 10, 25, 20, 36, 52},
			0,
			func(ai aggregatingIterator) func() (float64, bool) {
				// Different aggregation function.
				return ai.makeLeadingEdgeFilter(aggregatingIterator.max, 170)
			},
		},
	}

	extractFn := func(s roachpb.InternalTimeSeriesSample) float64 {
		return s.Average()
	}
	for i, tc := range testCases {
		t.Run("", func(t *testing.T) {
			actual := make([]float64, 0, len(tc.expected))
			iters := aggregatingIterator{
				newInterpolatingIterator(
					dataSpan1, 0, 10, tc.maxDistance, extractFn, downsampleSum, tspb.TimeSeriesQueryDerivative_NONE,
				),
				newInterpolatingIterator(
					dataSpan2, 0, 10, tc.maxDistance, extractFn, downsampleSum, tspb.TimeSeriesQueryDerivative_NONE,
				),
			}
			iters.init()
			valueFn := tc.aggFunc(iters)
			for iters.isValid() {
				if value, valid := valueFn(); valid {
					actual = append(actual, value)
				}
				iters.advance()
			}
			if !reflect.DeepEqual(actual, tc.expected) {
				t.Errorf("test %d aggregated values: %v, wanted: %v", i, actual, tc.expected)
			}
		})
	}
}

// assertQuery generates a query result from the local test model and compares
// it against the query returned from the server.
//
// TODO(mrtracy): This verification model is showing its age, and likely needs
// to be restructured in order to improve its trustworthiness. Currently, there
// is just enough overlap in the functionality between the model and the real
// system that it's difficult to be sure that you don't have a common error in
// both systems.
//
// My suggestion is to break down the model query into multiple, independently
// verificable steps. These steps will not be memory or computationally
// efficient, but will be conceptually easy to verify; then we can compare its
// results against the real data store with more confidence.
func (tm *testModel) assertQuery(
	name string,
	sources []string,
	downsample, agg *tspb.TimeSeriesQueryAggregator,
	derivative *tspb.TimeSeriesQueryDerivative,
	r Resolution,
	sampleDuration, start, end, interpolationLimit int64,
	expectedDatapointCount, expectedSourceCount int,
) {
	tm.t.Helper()
	// Query the actual server.
	q := tspb.Query{
		Name:             name,
		Downsampler:      downsample,
		SourceAggregator: agg,
		Derivative:       derivative,
		Sources:          sources,
	}

	account := tm.resultMemMonitor.MakeBoundAccount()
	defer account.Close(context.TODO())
	actualDatapoints, actualSources, err := tm.DB.QueryMemoryConstrained(
		context.TODO(),
		q,
		r,
		sampleDuration,
		start,
		end,
		interpolationLimit,
		&account,
		tm.workerMemMonitor,
		tm.queryMemoryBudget,
		int64(len(tm.seenSources)),
	)

	if err != nil {
		tm.t.Fatal(err)
	}
	if a, e := len(actualDatapoints), expectedDatapointCount; a != e {
		tm.t.Logf("actual datapoints: %v", actualDatapoints)
		tm.t.Fatal(errors.Errorf("query got %d datapoints, wanted %d", a, e))
	}
	if a, e := len(actualSources), expectedSourceCount; a != e {
		tm.t.Fatal(errors.Errorf("query got %d sources, wanted %d", a, e))
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
		dataStart := start - interpolationLimit
		dataEnd := end + interpolationLimit
		for time := dataStart - (dataStart % r.SlabDuration()); time < dataEnd; time += r.SlabDuration() {
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
		tm.t.Error(errors.Errorf("actual source list: %v, wanted: %v", actualSources, expectedSources))
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
	maxDistance := int32(interpolationLimit / r.SampleDuration())
	var iters aggregatingIterator
	for _, ds := range dataSpans {
		iters = append(
			iters,
			newInterpolatingIterator(
				*ds, 0, sampleDuration, maxDistance, extractFn, downsampleFn, q.GetDerivative(),
			),
		)
	}

	iters.init()
	if !iters.isValid() {
		if a, e := 0, len(expectedDatapoints); a != e {
			tm.t.Error(errors.Errorf("query had zero datapoints, wanted: %v", expectedDatapoints))
		}
		return
	}
	currentVal := func() (tspb.TimeSeriesDatapoint, bool) {
		var value float64
		var valid bool
		switch q.GetSourceAggregator() {
		case tspb.TimeSeriesQueryAggregator_SUM:
			value, valid = iters.sum()
		case tspb.TimeSeriesQueryAggregator_AVG:
			value, valid = iters.avg()
		case tspb.TimeSeriesQueryAggregator_MAX:
			value, valid = iters.max()
		case tspb.TimeSeriesQueryAggregator_MIN:
			value, valid = iters.min()
		default:
			tm.t.Fatalf("unknown query aggregator %s", q.GetSourceAggregator())
		}
		return tspb.TimeSeriesDatapoint{
			TimestampNanos: iters.timestamp(),
			Value:          value,
		}, valid
	}

	for iters.isValid() && iters.timestamp() <= end {
		if result, valid := currentVal(); valid {
			if q.GetDerivative() != tspb.TimeSeriesQueryDerivative_NONE {
				result.Value = result.Value / float64(sampleDuration) * float64(time.Second.Nanoseconds())
			}
			expectedDatapoints = append(expectedDatapoints, result)
		}
		iters.advance()
	}

	if !reflect.DeepEqual(actualDatapoints, expectedDatapoints) {
		tm.t.Error(errors.Errorf("actual datapoints: %v, wanted: %v", actualDatapoints, expectedDatapoints))
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
	tm.assertQuery("test.metric", nil, nil, nil, nil, resolution1ns, 1, 0, 60, 0, 7, 1)

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
	tm.assertQuery("test.multimetric", nil, nil, nil, nil, resolution1ns, 1, 0, 90, 0, 8, 2)
	// Test with aggregator specified.
	tm.assertQuery("test.multimetric", nil, tspb.TimeSeriesQueryAggregator_MAX.Enum(), nil, nil,
		resolution1ns, 1, 0, 90, 0, 8, 2)
	// Test with aggregator and downsampler.
	tm.assertQuery("test.multimetric", nil, tspb.TimeSeriesQueryAggregator_MAX.Enum(), tspb.TimeSeriesQueryAggregator_AVG.Enum(), nil,
		resolution1ns, 1, 0, 90, 0, 8, 2)
	// Test with derivative specified.
	tm.assertQuery("test.multimetric", nil, tspb.TimeSeriesQueryAggregator_AVG.Enum(), nil,
		tspb.TimeSeriesQueryDerivative_DERIVATIVE.Enum(), resolution1ns, 1, 0, 90, 0, 7, 2)
	// Test with everything specified.
	tm.assertQuery("test.multimetric", nil, tspb.TimeSeriesQueryAggregator_MIN.Enum(), tspb.TimeSeriesQueryAggregator_MAX.Enum(),
		tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE.Enum(), resolution1ns, 1, 0, 90, 0, 7, 2)

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
	tm.assertQuery("test.specificmetric", []string{"source2", "source4", "source6"}, nil, nil, nil, resolution1ns, 1, 0, 90, 0, 7, 2)

	// Assert querying data over limited range for single source. Regression
	// test for #4987.
	tm.assertQuery("test.specificmetric", []string{"source4", "source5"}, nil, nil, nil, resolution1ns, 1, 5, 24, 0, 4, 2)
}

// TestQueryDownsampling validates that query results match the expectation of
// the test model.
func TestQueryDownsampling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	account := tm.workerMemMonitor.MakeBoundAccount()
	defer account.Close(context.TODO())

	// Query with sampleDuration that is too small, expect error.
	_, _, err := tm.DB.Query(
		context.TODO(), tspb.Query{}, Resolution10s, 1, 0, 10000, 0, &account, tm.workerMemMonitor,
	)
	if err == nil {
		t.Fatal("expected query to fail with sampleDuration less than resolution allows.")
	}
	errorStr := "was not less"
	if !testutils.IsError(err, errorStr) {
		t.Fatalf("bad query got error %q, wanted to match %q", err.Error(), errorStr)
	}

	// Query with sampleDuration which is not an even multiple of the resolution.
	_, _, err = tm.DB.Query(
		context.TODO(),
		tspb.Query{},
		Resolution10s,
		Resolution10s.SampleDuration()+1,
		0,
		10000,
		0,
		&account,
		tm.workerMemMonitor,
	)
	if err == nil {
		t.Fatal("expected query to fail with sampleDuration not an even multiple of the query resolution.")
	}
	errorStr = "not a multiple"
	if !testutils.IsError(err, errorStr) {
		t.Fatalf("bad query got error %q, wanted to match %q", err.Error(), errorStr)
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

	tm.assertQuery("test.metric", nil, nil, nil, nil, resolution1ns, 10, 0, 60, 0, 6, 2)
	tm.assertQuery("test.metric", []string{"source1"}, nil, nil, nil, resolution1ns, 10, 0, 60, 0, 5, 1)
	tm.assertQuery("test.metric", []string{"source2"}, nil, nil, nil, resolution1ns, 10, 0, 60, 0, 4, 1)
}

// TestInterpolationLimit validates that query results match the expectation of
// the test model.
func TestInterpolationLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	// Metric with gaps at the edge of a queryable range.
	// The first source has missing data points at 14 and 19, which can
	// be interpolated from data points located in nearby slabs.
	// 5 - [15, 16, 17, 18] - 25
	tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
		{
			Name:   "metric.edgegaps",
			Source: "source1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(5, 500),
				datapoint(15, 1500),
				datapoint(16, 1600),
				datapoint(17, 1700),
				datapoint(18, 1800),
				datapoint(25, 2500),
			},
		},
		{
			Name:   "metric.edgegaps",
			Source: "source2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(14, 1000),
				datapoint(15, 1000),
				datapoint(16, 1000),
				datapoint(17, 1000),
				datapoint(18, 1000),
				datapoint(19, 1000),
			},
		},
	})
	tm.assertKeyCount(4)
	tm.assertModelCorrect()

	tm.assertQuery("metric.edgegaps", nil, nil, nil, nil, resolution1ns, 1, 14, 19, 10, 6, 2)
	tm.assertQuery("metric.edgegaps", nil, nil, nil, nil, resolution1ns, 1, 14, 19, 0, 6, 2)
	tm.assertQuery("metric.edgegaps", []string{"source1", "source2"}, nil, nil, nil, resolution1ns, 1, 14, 19, 10, 6, 2)

	// Metric with inner gaps which may be effected by the interpolation limit.
	tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
		{
			Name:   "metric.innergaps",
			Source: "source1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(1, 100),
				datapoint(2, 200),
				datapoint(4, 400),
				datapoint(7, 700),
				datapoint(10, 1000),
			},
		},
		{
			Name:   "metric.innergaps",
			Source: "source2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(1, 100),
				datapoint(2, 100),
				datapoint(3, 100),
				datapoint(4, 100),
				datapoint(5, 100),
				datapoint(6, 100),
				datapoint(7, 100),
				datapoint(8, 100),
				datapoint(9, 100),
			},
		},
	})
	tm.assertKeyCount(7)
	tm.assertModelCorrect()

	tm.assertQuery("metric.innergaps", nil, nil, nil, nil, resolution1ns, 1, 0, 9, 0, 9, 2)
	tm.assertQuery("metric.innergaps", nil, nil, nil, nil, resolution1ns, 1, 0, 9, 2, 9, 2)
	tm.assertQuery("metric.innergaps", nil, nil, nil, nil, resolution1ns, 1, 0, 9, 3, 9, 2)
	tm.assertQuery("metric.innergaps", nil, nil, nil, nil, resolution1ns, 1, 0, 9, 10, 9, 2)
	tm.assertQuery("metric.innergaps", []string{"source1", "source2"}, nil, nil, nil, resolution1ns, 1, 0, 9, 0, 9, 2)
	tm.assertQuery("metric.innergaps", []string{"source1", "source2"}, nil, nil, nil, resolution1ns, 1, 0, 9, 2, 9, 2)
	tm.assertQuery("metric.innergaps", []string{"source1", "source2"}, nil, nil, nil, resolution1ns, 1, 0, 9, 3, 9, 2)
	tm.assertQuery("metric.innergaps", []string{"source1", "source2"}, nil, nil, nil, resolution1ns, 1, 0, 9, 10, 9, 2)
}

func TestGetMaxTimespan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		r                   Resolution
		memoryBudget        int64
		estimatedSources    int64
		interpolationLimit  int64
		expectedTimespan    int64
		expectedErrorString string
	}{
		// Simplest case: One series, room for exactly one hour of query (need two
		// slabs of memory budget, as queried time span may stagger across two
		// slabs)
		{
			Resolution10s,
			2 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			1,
			0,
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// Not enough room for to make query.
		{
			Resolution10s,
			sizeOfTimeSeriesData + sizeOfTimeSeriesData*360,
			1,
			0,
			0,
			"insufficient",
		},
		// Not enough room because of multiple sources.
		{
			Resolution10s,
			2 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			2,
			0,
			0,
			"insufficient",
		},
		// 6 sources, room for 1 hour.
		{
			Resolution10s,
			12 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			6,
			0,
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// 6 sources, room for 2 hours.
		{
			Resolution10s,
			18 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			6,
			0,
			(2 * time.Hour).Nanoseconds(),
			"",
		},
		// Not enough room due to interpolation buffer.
		{
			Resolution10s,
			12 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			6,
			1,
			0,
			"insufficient",
		},
		// Sufficient room even with interpolation buffer.
		{
			Resolution10s,
			18 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			6,
			1,
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// Insufficient room for interpolation buffer (due to straddling)
		{
			Resolution10s,
			18 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			6,
			int64(float64(Resolution10s.SlabDuration()) * 0.75),
			0,
			"insufficient",
		},
		// Sufficient room even with interpolation buffer.
		{
			Resolution10s,
			24 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			6,
			int64(float64(Resolution10s.SlabDuration()) * 0.75),
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// 1ns test resolution.
		{
			resolution1ns,
			3 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*10),
			1,
			1,
			10,
			"",
		},
	} {
		t.Run("", func(t *testing.T) {
			actual, err := getMaxTimespan(
				tc.r, tc.memoryBudget, tc.estimatedSources, tc.interpolationLimit,
			)
			if !testutils.IsError(err, tc.expectedErrorString) {
				t.Fatalf("got error %s, wanted error matching %s", err, tc.expectedErrorString)
			}
			if tc.expectedErrorString == "" {
				return
			}
			if a, e := actual, tc.expectedTimespan; a != e {
				t.Fatalf("got max timespan %d, wanted %d", a, e)
			}
		})
	}
}

func TestQueryWorkerMemoryConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModel(t)

	// Swap model's memory monitor in order to adjust allocation size.
	unlimitedMon := tm.workerMemMonitor
	adjustedMon := mon.MakeMonitor(
		"timeseries-test-worker-adjusted",
		mon.MemoryResource,
		nil,
		nil,
		1,
		math.MaxInt64,
		cluster.MakeTestingClusterSettings(),
	)
	tm.workerMemMonitor = &adjustedMon
	tm.workerMemMonitor.Start(context.TODO(), unlimitedMon, mon.BoundAccount{})
	defer tm.workerMemMonitor.Stop(context.TODO())

	tm.Start()
	defer tm.Stop()

	generateData := func(dps int64) []tspb.TimeSeriesDatapoint {
		result := make([]tspb.TimeSeriesDatapoint, 0, dps)
		var i int64
		for i = 0; i < dps; i++ {
			result = append(result, datapoint(i, float64(100*i)))
		}
		return result
	}

	// Store data for a large metric across many keys, so we can test across
	// many different memory maximums.
	tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
		{
			Name:       "test.metric",
			Source:     "source1",
			Datapoints: generateData(120),
		},
		{
			Name:       "test.metric",
			Source:     "source2",
			Datapoints: generateData(120),
		},
		{
			Name:       "test.metric",
			Source:     "source3",
			Datapoints: generateData(120),
		},
	})
	tm.assertKeyCount(36)
	tm.assertModelCorrect()

	// Track the total maximum memory used for a query with no budget.
	tm.assertQuery("test.metric", nil, nil, nil, nil, resolution1ns, 1, 11, 109, 10, 99, 3)
	memoryUsed := tm.workerMemMonitor.MaximumBytes()

	for _, limit := range []int64{
		memoryUsed,
		memoryUsed / 2,
		memoryUsed / 3,
	} {
		// Limit memory in use by model. Reset memory monitor to get new maximum.
		tm.workerMemMonitor.Stop(context.TODO())
		tm.workerMemMonitor.Start(context.TODO(), unlimitedMon, mon.BoundAccount{})
		if tm.workerMemMonitor.MaximumBytes() != 0 {
			t.Fatalf("maximum bytes was %d, wanted zero", tm.workerMemMonitor.MaximumBytes())
		}

		tm.queryMemoryBudget = limit

		// Expected maximum usage may slightly exceed the budget due to the size of
		// dataSpan structures which are not accounted for in getMaxTimespan.
		expectedMaxUsage := limit + 3*(int64(len("source1"))+sizeOfDataSpan)
		tm.assertQuery("test.metric", nil, nil, nil, nil, resolution1ns, 1, 11, 109, 10, 99, 3)
		if a, e := tm.workerMemMonitor.MaximumBytes(), expectedMaxUsage; a > e {
			t.Fatalf("memory usage for query was %d, exceeded set maximum limit %d", a, e)
		}

		// As an additional check, ensure that maximum bytes used was within 5% of memory budget;
		// we want to use as much memory as we can to ensure the fastest possible queries.
		if a, e := float64(tm.workerMemMonitor.MaximumBytes()), float64(tm.queryMemoryBudget)*0.95; a < e {
			t.Fatalf("memory usage for query was %f, wanted at least %f", a, e)
		}
	}

	// Verify insufficient memory error bubbles up.
	queryAcc := tm.workerMemMonitor.MakeBoundAccount()
	defer queryAcc.Close(context.TODO())
	_, _, err := tm.DB.QueryMemoryConstrained(
		context.TODO(),
		tspb.Query{Name: "test.metric"},
		resolution1ns,
		1,
		0,
		10000,
		5,
		&queryAcc,
		tm.workerMemMonitor,
		1000,
		3,
	)
	if errorStr := "insufficient"; !testutils.IsError(err, errorStr) {
		t.Fatalf("bad query got error %q, wanted to match %q", err.Error(), errorStr)
	}
}

func TestQueryWorkerMemoryMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModel(t)

	memoryBudget := int64(100 * 1024)

	// Swap model's memory monitor to install a limit.
	unlimitedMon := tm.workerMemMonitor
	limitedMon := mon.MakeMonitorWithLimit(
		"timeseries-test-limited",
		mon.MemoryResource,
		memoryBudget,
		nil,
		nil,
		100,
		100,
		cluster.MakeTestingClusterSettings(),
	)
	tm.workerMemMonitor = &limitedMon
	tm.workerMemMonitor.Start(context.TODO(), unlimitedMon, mon.BoundAccount{})
	defer tm.workerMemMonitor.Stop(context.TODO())

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

	// Assert correctness with no memory pressure.
	tm.assertQuery("test.metric", nil, nil, nil, nil, resolution1ns, 1, 0, 60, 0, 7, 1)

	// Assert failure with memory pressure.
	acc := limitedMon.MakeBoundAccount()
	if err := acc.Grow(context.TODO(), memoryBudget-1); err != nil {
		t.Fatal(err)
	}

	queryAcc := limitedMon.MakeBoundAccount()
	defer queryAcc.Close(context.TODO())
	_, _, err := tm.DB.Query(
		context.TODO(), tspb.Query{Name: "test.metric"}, resolution1ns, 1, 0, 10000, 0, &queryAcc, tm.workerMemMonitor,
	)
	if errorStr := "memory budget exceeded"; !testutils.IsError(err, errorStr) {
		t.Fatalf("bad query got error %q, wanted to match %q", err.Error(), errorStr)
	}

	// Assert success again with memory pressure released.
	acc.Close(context.TODO())
	tm.assertQuery("test.metric", nil, nil, nil, nil, resolution1ns, 1, 0, 60, 0, 7, 1)

	// Start/Stop limited monitor to reset maximum allocation.
	tm.workerMemMonitor.Stop(context.TODO())
	tm.workerMemMonitor.Start(context.TODO(), unlimitedMon, mon.BoundAccount{})

	var (
		memStatsBefore runtime.MemStats
		memStatsAfter  runtime.MemStats
	)
	runtime.ReadMemStats(&memStatsBefore)

	_, _, err = tm.DB.Query(
		context.TODO(), tspb.Query{Name: "test.metric"}, resolution1ns, 1, 0, 10000, 0, &queryAcc, tm.workerMemMonitor,
	)
	if err != nil {
		t.Fatalf("expected no error from query, got %v", err)
	}

	runtime.ReadMemStats(&memStatsAfter)
	t.Logf("total allocations for query: %d\n", memStatsAfter.TotalAlloc-memStatsBefore.TotalAlloc)
	t.Logf("maximum allocations for query monitor: %d\n", limitedMon.MaximumBytes())
}

// TestQueryBadRequests confirms that the query method returns gentle errors for
// obviously bad incoming data.
func TestQueryBadRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	account := tm.workerMemMonitor.MakeBoundAccount()
	defer account.Close(context.TODO())

	// Query with a downsampler that is invalid, expect error.
	downsampler := (tspb.TimeSeriesQueryAggregator)(999)
	_, _, err := tm.DB.Query(
		context.TODO(), tspb.Query{
			Name:        "metric.test",
			Downsampler: downsampler.Enum(),
		}, resolution1ns, 10, 0, 10000, 0, &account, tm.workerMemMonitor,
	)
	errorStr := "unknown time series downsampler"
	if !testutils.IsError(err, errorStr) {
		if err == nil {
			t.Fatalf("bad query got no error, wanted to match %q", errorStr)
		} else {
			t.Fatalf("bad query got error %q, wanted to match %q", err.Error(), errorStr)
		}
	}

	// Query with a aggregator that is invalid, expect error.
	aggregator := (tspb.TimeSeriesQueryAggregator)(999)
	_, _, err = tm.DB.Query(
		context.TODO(), tspb.Query{
			Name:             "metric.test",
			SourceAggregator: aggregator.Enum(),
		}, resolution1ns, 10, 0, 10000, 0, &account, tm.workerMemMonitor,
	)
	errorStr = "unknown time series aggregator"
	if !testutils.IsError(err, errorStr) {
		if err == nil {
			t.Fatalf("bad query got no error, wanted to match %q", errorStr)
		} else {
			t.Fatalf("bad query got error %q, wanted to match %q", err.Error(), errorStr)
		}
	}

	// Query with an interpolator that is invalid, expect no error (default behavior is none).
	derivative := (tspb.TimeSeriesQueryDerivative)(999)
	_, _, err = tm.DB.Query(
		context.TODO(), tspb.Query{
			Derivative: derivative.Enum(),
		}, resolution1ns, 10, 0, 10000, 0, &account, tm.workerMemMonitor,
	)
	if !testutils.IsError(err, "") {
		t.Fatalf("bad query got error %q, wanted no error", err.Error())
	}
}
