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
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

func verifySpanIteratorPosition(t *testing.T, actual, expected timeSeriesSpanIterator) {
	t.Helper()
	if a, e := actual.total, expected.total; a != e {
		t.Errorf("iterator had total index of %d, wanted %d", a, e)
	}
	if a, e := actual.inner, expected.inner; a != e {
		t.Errorf("iterator had inner index of %d, wanted %d", a, e)
	}
	if a, e := actual.outer, expected.outer; a != e {
		t.Errorf("iterator had outer index of %d, wanted %d", a, e)
	}
	if a, e := actual.timestamp, expected.timestamp; a != e {
		t.Errorf("iterator had timestamp of %d, wanted %d", a, e)
	}
	if a, e := actual.length, expected.length; a != e {
		t.Errorf("iterator had length of %d, wanted %d", a, e)
	}
}

func TestTimeSeriesSpanIteratorMovement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	iter := makeTimeSeriesSpanIterator(timeSeriesSpan{
		makeInternalData(0, 10, []dataSample{
			{10, 1},
			{20, 2},
		}),
		makeInternalData(30, 10, []dataSample{
			{30, 3},
		}),
		makeInternalData(50, 10, []dataSample{
			{50, 5},
			{70, 7},
			{90, 9},
		}),
	})

	// initialize explicit iterator results for the entire span - this makes the
	// movement tests easier to read, as we are often asserting that the same
	// position.
	explicitPositions := []timeSeriesSpanIterator{
		{
			timestamp: 10,
			length:    6,
		},
		{
			total:     1,
			outer:     0,
			inner:     1,
			timestamp: 20,
			length:    6,
		},
		{
			total:     2,
			outer:     1,
			inner:     0,
			timestamp: 30,
			length:    6,
		},
		{
			total:     3,
			outer:     2,
			inner:     0,
			timestamp: 50,
			length:    6,
		},
		{
			total:     4,
			outer:     2,
			inner:     1,
			timestamp: 70,
			length:    6,
		},
		{
			total:     5,
			outer:     2,
			inner:     2,
			timestamp: 90,
			length:    6,
		},
		{
			total:     6,
			outer:     3,
			inner:     0,
			timestamp: 0,
			length:    6,
		},
	}

	// Initial position.
	verifySpanIteratorPosition(t, iter, explicitPositions[0])

	// Forwarding.
	iter.forward()
	verifySpanIteratorPosition(t, iter, explicitPositions[1])
	iter.forward()
	verifySpanIteratorPosition(t, iter, explicitPositions[2])

	iter.forward()
	iter.forward()
	iter.forward()
	iter.forward()
	verifySpanIteratorPosition(t, iter, explicitPositions[6])
	iter.forward()
	verifySpanIteratorPosition(t, iter, explicitPositions[6])

	// Backwards.
	iter.backward()
	verifySpanIteratorPosition(t, iter, explicitPositions[5])
	iter.backward()
	iter.backward()
	iter.backward()
	iter.backward()
	verifySpanIteratorPosition(t, iter, explicitPositions[1])
	iter.backward()
	iter.backward()
	iter.backward()
	verifySpanIteratorPosition(t, iter, explicitPositions[0])

	// Seek index.
	iter.seekIndex(2)
	verifySpanIteratorPosition(t, iter, explicitPositions[2])
	iter.seekIndex(4)
	verifySpanIteratorPosition(t, iter, explicitPositions[4])
	iter.seekIndex(0)
	verifySpanIteratorPosition(t, iter, explicitPositions[0])
	iter.seekIndex(1000)
	verifySpanIteratorPosition(t, iter, explicitPositions[6])
	iter.seekIndex(-1)
	verifySpanIteratorPosition(t, iter, explicitPositions[0])

	// Seek timestamp.
	iter.seekTimestamp(0)
	verifySpanIteratorPosition(t, iter, explicitPositions[0])
	iter.seekTimestamp(15)
	verifySpanIteratorPosition(t, iter, explicitPositions[1])
	iter.seekTimestamp(50)
	verifySpanIteratorPosition(t, iter, explicitPositions[3])
	iter.seekTimestamp(80)
	verifySpanIteratorPosition(t, iter, explicitPositions[5])
	iter.seekTimestamp(10000)
	verifySpanIteratorPosition(t, iter, explicitPositions[6])
}

func TestTimeSeriesSpanIteratorValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	iter := makeTimeSeriesSpanIterator(timeSeriesSpan{
		makeInternalData(0, 10, []dataSample{
			{10, 1},
			{20, 2},
			{20, 4},
		}),
		makeInternalData(30, 10, []dataSample{
			{30, 3},
			{30, 6},
			{30, 9},
		}),
		makeInternalData(50, 10, []dataSample{
			{50, 12},
			{70, 700},
			{90, 9},
		}),
	})

	iter.seekTimestamp(30)
	for _, tc := range []struct {
		agg           tspb.TimeSeriesQueryAggregator
		expected      float64
		expectedDeriv float64
	}{
		{
			agg:           tspb.TimeSeriesQueryAggregator_AVG,
			expected:      6,
			expectedDeriv: 3,
		},
		{
			agg:           tspb.TimeSeriesQueryAggregator_SUM,
			expected:      18,
			expectedDeriv: 12,
		},
		{
			agg:           tspb.TimeSeriesQueryAggregator_MIN,
			expected:      3,
			expectedDeriv: 1,
		},
		{
			agg:           tspb.TimeSeriesQueryAggregator_MAX,
			expected:      9,
			expectedDeriv: 5,
		},
	} {
		t.Run("value", func(t *testing.T) {
			if a, e := iter.value(tc.agg), tc.expected; a != e {
				t.Errorf("value for %s of iter got %f, wanted %f", tc.agg.String(), a, e)
			}
			deriv, valid := iter.derivative(tc.agg)
			if !valid {
				t.Errorf("expected derivative to be valid, was invalid")
			}
			if a, e := deriv, tc.expectedDeriv; a != e {
				t.Errorf("derivative for %s of iter got %f, wanted %f", tc.agg.String(), a, e)
			}
		})
	}

	// Test value interpolation.
	iter.seekTimestamp(50)
	for _, tc := range []struct {
		timestamp          int64
		interpolationLimit int64
		expectedValid      bool
		expectedValue      float64
	}{
		{50, 100, true, 12},
		{50, 1, true, 12},
		// Must interpolate in between points.
		{30, 100, false, 0},
		{60, 100, false, 0},
		// Interpolation limit is respected
		{40, 100, true, 9},
		{40, 20, true, 9},
		{40, 19, false, 0},
		// Interpolation limit 0 is still a special case.
		{40, 0, true, 9},
	} {
		interpValue, valid := iter.valueAtTimestamp(tc.timestamp, tc.interpolationLimit, tspb.TimeSeriesQueryAggregator_AVG)
		if valid != tc.expectedValid {
			t.Errorf("valueAtTimestamp valid was %t, wanted %t", valid, tc.expectedValid)
			continue
		}
		if a, e := interpValue, tc.expectedValue; a != e {
			t.Errorf("valueAtTimestamp %d got %f, wanted %f", tc.timestamp, a, e)
		}
	}

	// Special case: no derivative available at index 0.
	iter.seekIndex(0)
	if _, valid := iter.valueAtTimestamp(20, 1000, tspb.TimeSeriesQueryAggregator_AVG); valid {
		t.Errorf("expected valueAtTimestamp to be invalid at index 0, was valid")
	}
	if _, valid := iter.derivative(tspb.TimeSeriesQueryAggregator_AVG); valid {
		t.Errorf("expected deriv to be invalid at index 0, was valid")
	}
}
