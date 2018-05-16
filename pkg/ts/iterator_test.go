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

func TestDownsampleSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, tc := range []struct {
		span           timeSeriesSpan
		samplePeriod   int64
		expectedResult timeSeriesSpan
	}{
		// Same sample period as disk has no result.
		{
			span: timeSeriesSpan{
				makeInternalData(0, 10, []dataSample{
					{10, 1},
					{20, 2},
					{20, 4},
					{30, 5},
				}),
				makeInternalData(50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}),
			},
			samplePeriod: 10,
			expectedResult: timeSeriesSpan{
				{
					StartTimestampNanos: 0,
					SampleDurationNanos: 10,
					Samples: []roachpb.InternalTimeSeriesSample{
						{
							Offset: 1,
							Sum:    1,
							Count:  1,
							Max:    proto.Float64(1),
							Min:    proto.Float64(1),
						},
						{
							Offset: 2,
							Sum:    6,
							Count:  2,
							Max:    proto.Float64(4),
							Min:    proto.Float64(2),
						},
						{
							Offset: 3,
							Sum:    5,
							Count:  1,
							Max:    proto.Float64(5),
							Min:    proto.Float64(5),
						},
					},
				},
				{
					StartTimestampNanos: 50,
					// Downsample does not adjust SampleDurationNanos.
					SampleDurationNanos: 10,
					Samples: []roachpb.InternalTimeSeriesSample{
						{
							Offset: 0,
							Sum:    5,
							Count:  1,
							Max:    proto.Float64(5),
							Min:    proto.Float64(5),
						},
						{
							Offset: 1,
							Sum:    6,
							Count:  1,
							Max:    proto.Float64(6),
							Min:    proto.Float64(6),
						},
					},
				},
			},
		},
		// Basic downsampling.
		{
			span: timeSeriesSpan{
				makeInternalData(0, 10, []dataSample{
					{10, 1},
					{20, 2},
					{20, 4},
					{30, 5},
				}),
				makeInternalData(50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}),
				makeInternalData(70, 10, []dataSample{
					{70, 7},
					{90, 9},
					{110, 8},
				}),
			},
			samplePeriod: 50,
			expectedResult: timeSeriesSpan{
				{
					StartTimestampNanos: 0,
					// Downsample does not adjust SampleDurationNanos.
					SampleDurationNanos: 10,
					Samples: []roachpb.InternalTimeSeriesSample{
						{
							Offset: 0,
							Sum:    12,
							Count:  4,
							Max:    proto.Float64(5.0),
							Min:    proto.Float64(1.0),
						},
						{
							Offset: 5,
							Sum:    27,
							Count:  4,
							Max:    proto.Float64(9.0),
							Min:    proto.Float64(5.0),
						},
						{
							Offset: 10,
							Sum:    8,
							Count:  1,
							Max:    proto.Float64(8.0),
							Min:    proto.Float64(8.0),
						},
					},
				},
			},
		},
		// Downsampling while re-using multiple InternalTimeSeriesData structures.
		{
			span: timeSeriesSpan{
				makeInternalData(0, 10, []dataSample{
					{10, 1},
				}),
				makeInternalData(50, 10, []dataSample{
					{50, 5},
					{60, 6},
				}),
				makeInternalData(70, 10, []dataSample{
					{70, 7},
					{90, 9},
					{110, 8},
				}),
			},
			samplePeriod: 50,
			expectedResult: timeSeriesSpan{
				{
					StartTimestampNanos: 0,
					// Downsample does not adjust SampleDurationNanos.
					SampleDurationNanos: 10,
					Samples: []roachpb.InternalTimeSeriesSample{
						{
							Offset: 0,
							Sum:    1,
							Count:  1,
							Max:    proto.Float64(1.0),
							Min:    proto.Float64(1.0),
						},
					},
				},
				{
					// Downsample does not adjust SampleDurationNanos or
					// StartTimestampNanos.
					StartTimestampNanos: 50,
					SampleDurationNanos: 10,
					Samples: []roachpb.InternalTimeSeriesSample{
						{
							Offset: 0,
							Sum:    27,
							Count:  4,
							Max:    proto.Float64(9.0),
							Min:    proto.Float64(5.0),
						},
						{
							Offset: 5,
							Sum:    8,
							Count:  1,
							Max:    proto.Float64(8.0),
							Min:    proto.Float64(8.0),
						},
					},
				},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			spans := map[string]timeSeriesSpan{
				"test": tc.span,
			}
			downsampleSpans(spans, tc.samplePeriod)
			if a, e := spans["test"], tc.expectedResult; !reflect.DeepEqual(a, e) {
				for _, diff := range pretty.Diff(a, e) {
					t.Error(diff)
				}
			}
		})
	}
}
