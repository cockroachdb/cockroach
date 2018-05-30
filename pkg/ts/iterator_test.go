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
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"

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

// makeInternalRowData makes an InternalTimeSeriesData object from a collection of data
// samples. Input is the start timestamp, the sample duration, and the set of
// samples. Sample data must be ordered by timestamp.
func makeInternalRowData(
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

// makeInternalColumnData makes an InternalTimeSeriesData object from a collection of data
// samples. Input is the start timestamp, the sample duration, and the set of
// samples. Sample data must be ordered by timestamp.
func makeInternalColumnData(
	startTimestamp, sampleDuration int64, samples []dataSample,
) roachpb.InternalTimeSeriesData {
	// Adjust startTimestamp to an exact multiple of sampleDuration.
	startTimestamp -= startTimestamp % sampleDuration
	result := roachpb.InternalTimeSeriesData{
		StartTimestampNanos: startTimestamp,
		SampleDurationNanos: sampleDuration,
		Offset:              make([]int32, 0),
		Last:                make([]float64, 0),
		First:               make([]float64, 0),
		Count:               make([]uint32, 0),
		Sum:                 make([]float64, 0),
		Min:                 make([]float64, 0),
		Max:                 make([]float64, 0),
		Variance:            make([]float64, 0),
	}

	// Run through all samples, merging any consecutive samples which correspond
	// to the same sample interval. Assume that the data will contain relevant
	// roll-ups, but discard the roll-up data if there is only one sample per
	// sample period.
	isRollup := false
	valuesForSample := make([]float64, 0, 1)
	for _, sample := range samples {
		offset := int32((sample.timestamp - startTimestamp) / sampleDuration)
		value := sample.value

		// Merge into the previous sample if we have the same offset.
		if count := len(result.Offset); count > 0 && result.Offset[count-1] == offset {
			isRollup = true
			result.Last[count-1] = value
			result.Count[count-1]++
			result.Sum[count-1] += value
			result.Max[count-1] = math.Max(result.Max[count-1], value)
			result.Min[count-1] = math.Min(result.Min[count-1], value)
			valuesForSample = append(valuesForSample, value)
		} else if count > 0 && result.Offset[count-1] > offset {
			panic("sample data provided to generateData must be ordered by timestamp.")
		} else {
			// Compute variance for previous sample if there was more than one
			// value.
			if len(valuesForSample) > 1 {
				avg := result.Sum[count-1] / float64(len(valuesForSample))
				total := 0.0
				for i := range valuesForSample {
					total += math.Pow(valuesForSample[i]-avg, 2)
				}
				result.Variance[count-1] = total / float64(len(valuesForSample))
			}

			result.Offset = append(result.Offset, offset)
			result.Last = append(result.Last, value)
			result.First = append(result.First, value)
			result.Count = append(result.Count, 1)
			result.Sum = append(result.Sum, value)
			result.Min = append(result.Min, value)
			result.Max = append(result.Max, value)
			result.Variance = append(result.Variance, 0)

			// Reset variance calculation.
			valuesForSample = valuesForSample[:0]
			valuesForSample = append(valuesForSample, value)
		}
	}

	if !isRollup {
		result.First = nil
		result.Count = nil
		result.Sum = nil
		result.Min = nil
		result.Max = nil
		result.Variance = nil
	}

	return result
}

// makeInternalSample constructs a Sample object from five numbers. The
// intention of this function is to cut down on the vertical size of expected
// result definitions in test cases.
func makeInternalSample(offset int32, sum float64, count uint32, min, max *float64) roachpb.InternalTimeSeriesSample {
	return roachpb.InternalTimeSeriesSample{
		Offset: offset,
		Sum:    sum,
		Count:  count,
		Min:    min,
		Max:    max,
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
	verifyIterTest := func(t *testing.T, iter timeSeriesSpanIterator) {
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

	// Row data only.
	t.Run("row only", func(t *testing.T) {
		verifyIterTest(t, makeTimeSeriesSpanIterator(timeSeriesSpan{
			makeInternalRowData(0, 10, []dataSample{
				{10, 1},
				{20, 2},
			}),
			makeInternalRowData(30, 10, []dataSample{
				{30, 3},
			}),
			makeInternalRowData(50, 10, []dataSample{
				{50, 5},
				{70, 7},
				{90, 9},
			}),
		}))
	})

	t.Run("columns only", func(t *testing.T) {
		verifyIterTest(t, makeTimeSeriesSpanIterator(timeSeriesSpan{
			makeInternalColumnData(0, 10, []dataSample{
				{10, 1},
				{20, 2},
			}),
			makeInternalColumnData(30, 10, []dataSample{
				{30, 3},
			}),
			makeInternalColumnData(50, 10, []dataSample{
				{50, 5},
				{70, 7},
				{90, 9},
			}),
		}))
	})

	t.Run("mixed rows and columns", func(t *testing.T) {
		verifyIterTest(t, makeTimeSeriesSpanIterator(timeSeriesSpan{
			makeInternalRowData(0, 10, []dataSample{
				{10, 1},
				{20, 2},
			}),
			makeInternalColumnData(30, 10, []dataSample{
				{30, 3},
			}),
			makeInternalRowData(50, 10, []dataSample{
				{50, 5},
				{70, 7},
				{90, 9},
			}),
		}))
	})
}

func TestTimeSeriesSpanIteratorValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	iter := makeTimeSeriesSpanIterator(timeSeriesSpan{
		makeInternalRowData(0, 10, []dataSample{
			{10, 1},
			{20, 2},
			{20, 4},
		}),
		makeInternalRowData(30, 10, []dataSample{
			{30, 3},
			{30, 6},
			{30, 9},
		}),
		makeInternalRowData(50, 10, []dataSample{
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

// dataDesc is used to describe an internal data structure independently of it
// being formatted using rows or columns.
type dataDesc struct {
	startTimestamp int64
	sampleDuration int64
	samples        []dataSample
}

func TestDownsampleSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for tcnum, tc := range []struct {
		inputDesc    []dataDesc
		samplePeriod int64
		downsampler  tspb.TimeSeriesQueryAggregator
		expectedDesc []dataDesc
	}{
		// Original sample period, average downsampler.
		{
			inputDesc: []dataDesc{
				{0, 10, []dataSample{
					{10, 1},
					{20, 2},
					{20, 4},
					{30, 5},
				}},
				{50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}},
			},
			samplePeriod: 10,
			downsampler:  tspb.TimeSeriesQueryAggregator_AVG,
			expectedDesc: []dataDesc{
				{0, 10, []dataSample{
					{10, 1},
					{20, 3},
					{30, 5},
				}},
				{50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}},
			},
		},
		// Original sample period, max downsampler.  Should fill in max value.
		{
			inputDesc: []dataDesc{
				{0, 10, []dataSample{
					{10, 1},
					{20, 2},
					{20, 4},
					{30, 5},
				}},
				{50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}},
			},
			samplePeriod: 10,
			downsampler:  tspb.TimeSeriesQueryAggregator_MAX,
			expectedDesc: []dataDesc{
				{0, 10, []dataSample{
					{10, 1},
					{20, 4},
					{30, 5},
				}},
				{50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}},
			},
		},
		// Original sample period, min downsampler.
		{
			inputDesc: []dataDesc{
				{0, 10, []dataSample{
					{10, 1},
					{20, 2},
					{20, 4},
					{30, 5},
				}},
				{50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}},
			},
			samplePeriod: 10,
			downsampler:  tspb.TimeSeriesQueryAggregator_MIN,
			expectedDesc: []dataDesc{
				{0, 10, []dataSample{
					{10, 1},
					{20, 2},
					{30, 5},
				}},
				{50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}},
			},
		},
		// AVG downsamper. Should re-use original span data.
		{
			inputDesc: []dataDesc{
				{0, 10, []dataSample{
					{10, 1},
					{20, 2},
					{20, 4},
					{30, 5},
				}},
				{50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}},
				{70, 10, []dataSample{
					{70, 7},
					{90, 9},
					{110, 8},
				}},
			},
			samplePeriod: 50,
			downsampler:  tspb.TimeSeriesQueryAggregator_AVG,
			expectedDesc: []dataDesc{
				{0, 10, []dataSample{
					{0, 3},
					{50, 6.75},
					{100, 8},
				}},
			},
		},
		// MAX downsamper. Should re-use original span data; note that the sum and
		// count values are NOT overwritten.
		{
			inputDesc: []dataDesc{
				{0, 10, []dataSample{
					{10, 1},
					{20, 2},
					{20, 4},
					{30, 5},
				}},
				{50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}},
				{70, 10, []dataSample{
					{70, 7},
					{90, 9},
					{110, 8},
				}},
			},
			samplePeriod: 50,
			downsampler:  tspb.TimeSeriesQueryAggregator_MAX,
			expectedDesc: []dataDesc{
				{0, 10, []dataSample{
					{0, 5},
					{50, 9},
					{100, 8},
				}},
			},
		},
		// MIN downsamper. Should re-use original span data; note that the sum and
		// count values are NOT overwritten.
		{
			inputDesc: []dataDesc{
				{0, 10, []dataSample{
					{10, 1},
					{20, 2},
					{20, 4},
					{30, 5},
				}},
				{50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}},
				{70, 10, []dataSample{
					{70, 7},
					{90, 9},
					{110, 8},
				}},
			},
			samplePeriod: 50,
			downsampler:  tspb.TimeSeriesQueryAggregator_MIN,
			expectedDesc: []dataDesc{
				{0, 10, []dataSample{
					{0, 1},
					{50, 5},
					{100, 8},
				}},
			},
		},
		// AVG downsampler, downsampling while re-using multiple
		// InternalTimeSeriesData structures.
		{
			inputDesc: []dataDesc{
				{0, 10, []dataSample{
					{10, 1},
				}},
				{50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}},
				{70, 10, []dataSample{
					{70, 7},
					{90, 9},
					{110, 8},
				}},
			},
			samplePeriod: 50,
			downsampler:  tspb.TimeSeriesQueryAggregator_AVG,
			expectedDesc: []dataDesc{
				{0, 10, []dataSample{
					{0, 1},
				}},
				{50, 10, []dataSample{
					{50, 6.75},
					{100, 8},
				}},
			},
		},
		// MAX downsampler, downsampling while re-using multiple
		// InternalTimeSeriesData structures.
		{
			inputDesc: []dataDesc{
				{0, 10, []dataSample{
					{10, 1},
				}},
				{50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}},
				{70, 10, []dataSample{
					{70, 7},
					{90, 9},
					{110, 8},
				}},
			},
			samplePeriod: 50,
			downsampler:  tspb.TimeSeriesQueryAggregator_MAX,
			expectedDesc: []dataDesc{
				{0, 10, []dataSample{
					{0, 1},
				}},
				{50, 10, []dataSample{
					{50, 9},
					{100, 8},
				}},
			},
		},
	} {

		// Run case in Row format.
		t.Run(fmt.Sprintf("%d:Row", tcnum), func(t *testing.T) {
			span := make(timeSeriesSpan, len(tc.inputDesc))
			for i, desc := range tc.inputDesc {
				span[i] = makeInternalRowData(desc.startTimestamp, desc.sampleDuration, desc.samples)
			}
			expectedSpan := make(timeSeriesSpan, len(tc.expectedDesc))
			for i, desc := range tc.expectedDesc {
				expectedSpan[i] = makeInternalRowData(desc.startTimestamp, desc.sampleDuration, desc.samples)
			}
			spans := map[string]timeSeriesSpan{
				"test": span,
			}
			downsampleSpans(spans, tc.samplePeriod, tc.downsampler)
			if a, e := spans["test"], expectedSpan; !reflect.DeepEqual(a, e) {
				for _, diff := range pretty.Diff(a, e) {
					t.Error(diff)
				}
			}
		})

		// Run case in Column format.
		t.Run(fmt.Sprintf("%d:Column", tcnum), func(t *testing.T) {
			span := make(timeSeriesSpan, len(tc.inputDesc))
			for i, desc := range tc.inputDesc {
				span[i] = makeInternalColumnData(desc.startTimestamp, desc.sampleDuration, desc.samples)
			}
			expectedSpan := make(timeSeriesSpan, len(tc.expectedDesc))
			for i, desc := range tc.expectedDesc {
				expectedSpan[i] = makeInternalColumnData(desc.startTimestamp, desc.sampleDuration, desc.samples)
			}
			spans := map[string]timeSeriesSpan{
				"test": span,
			}
			downsampleSpans(spans, tc.samplePeriod, tc.downsampler)
			if a, e := spans["test"], expectedSpan; !reflect.DeepEqual(a, e) {
				for _, diff := range pretty.Diff(a, e) {
					t.Error(diff)
				}
			}
		})

		// Run case in Mixed format.
		t.Run(fmt.Sprintf("%d:Mixed", tcnum), func(t *testing.T) {
			span := make(timeSeriesSpan, len(tc.inputDesc))
			for i, desc := range tc.inputDesc {
				if i%2 == 0 {
					span[i] = makeInternalRowData(desc.startTimestamp, desc.sampleDuration, desc.samples)
				} else {
					span[i] = makeInternalColumnData(desc.startTimestamp, desc.sampleDuration, desc.samples)
				}
			}
			expectedSpan := make(timeSeriesSpan, len(tc.expectedDesc))
			for i, desc := range tc.expectedDesc {
				if i%2 == 0 {
					expectedSpan[i] = makeInternalRowData(desc.startTimestamp, desc.sampleDuration, desc.samples)
				} else {
					expectedSpan[i] = makeInternalColumnData(desc.startTimestamp, desc.sampleDuration, desc.samples)
				}
			}
			spans := map[string]timeSeriesSpan{
				"test": span,
			}
			downsampleSpans(spans, tc.samplePeriod, tc.downsampler)
			if a, e := spans["test"], expectedSpan; !reflect.DeepEqual(a, e) {
				for _, diff := range pretty.Diff(a, e) {
					t.Error(diff)
				}
			}
		})
	}
}
