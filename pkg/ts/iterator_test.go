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
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts/testmodel"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
)

// makeInternalRowData makes an InternalTimeSeriesData object from a collection
// of data samples. The result will be in the soon-deprecated row format. Input
// is the start timestamp, the sample duration, and the set of samples. As
// opposed to the ToInternal() method, there are two key differences:
//
// 1. This method always procueds a single InternalTimeSeriesData object with
// the provided startTimestamp, rather than breaking up the datapoints into
// several slabs based on a slab duration.
//
// 2. The provided data samples are downsampled according to the sampleDuration,
// mimicking the process that would be used to create a data rollup. Therefore,
// the resulting InternalTimeSeriesData will have one sample for each sample
// period.
//
// Sample data must be provided ordered by timestamp or the output will be
// unpredictable.
func makeInternalRowData(
	startTimestamp, sampleDuration int64, samples []tspb.TimeSeriesDatapoint,
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
		offset := int32((sample.TimestampNanos - startTimestamp) / sampleDuration)
		value := sample.Value

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

// makeInternalRowData makes an InternalTimeSeriesData object from a collection
// of data samples. The result will be in columnar format. Input is the start
// timestamp, the sample duration, and the set of samples. As opposed to the
// ToInternal() method, there are two key differences:
//
// 1. This method always procueds a single InternalTimeSeriesData object with
// the provided startTimestamp, rather than breaking up the datapoints into
// several slabs based on a slab duration.
//
// 2. The provided data samples are downsampled according to the sampleDuration,
// mimicking the process that would be used to create a data rollup. Therefore,
// the resulting InternalTimeSeriesData will have one entry for each offset
// period. Additionally, if there are multiple datapoints in any sample period,
// then the desired result is assumed to be a rollup and every resulting sample
// period will have values for all rollup columns.
//
// Sample data must be provided ordered by timestamp or the output will be
// unpredictable.
func makeInternalColumnData(
	startTimestamp, sampleDuration int64, samples []tspb.TimeSeriesDatapoint,
) roachpb.InternalTimeSeriesData {
	// Adjust startTimestamp to an exact multiple of sampleDuration.
	startTimestamp -= startTimestamp % sampleDuration
	result := roachpb.InternalTimeSeriesData{
		StartTimestampNanos: startTimestamp,
		SampleDurationNanos: sampleDuration,
	}

	// Run through all samples, merging any consecutive samples which correspond
	// to the same sample interval. Assume that the data will contain relevant
	// roll-ups, but discard the roll-up data if there is only one sample per
	// sample period.
	isRollup := false

	// Variance computation must consider each value against the average.
	// Retain the component values of each column and compute a variance.
	valuesForSample := make([]float64, 0, 1)
	computeVariance := func() float64 {
		variance := 0.0
		if len(valuesForSample) > 1 {
			// Compute average of values.
			sum := 0.0
			for _, value := range valuesForSample {
				sum += value
			}
			avg := sum / float64(len(valuesForSample))

			// Compute variance of values using the average.
			totalSquaredDeviation := 0.0
			for _, value := range valuesForSample {
				totalSquaredDeviation += math.Pow(value-avg, 2)
			}
			variance = totalSquaredDeviation / float64(len(valuesForSample))
		}
		// Reset value collection.
		valuesForSample = valuesForSample[:0]
		return variance
	}

	for _, sample := range samples {
		offset := result.OffsetForTimestamp(sample.TimestampNanos)
		value := sample.Value

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
				result.Variance[count-1] = computeVariance()
			} else {
				valuesForSample = valuesForSample[:0]
			}

			result.Offset = append(result.Offset, offset)
			result.Last = append(result.Last, value)
			result.First = append(result.First, value)
			result.Count = append(result.Count, 1)
			result.Sum = append(result.Sum, value)
			result.Min = append(result.Min, value)
			result.Max = append(result.Max, value)
			result.Variance = append(result.Variance, 0)
			valuesForSample = append(valuesForSample, value)
		}
	}

	// Compute variance for last sample.
	result.Variance[len(result.Variance)-1] = computeVariance()

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

func TestMakeInternalData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	data := []tspb.TimeSeriesDatapoint{
		tsdp(110, 20),
		tsdp(120, 300),
		tsdp(130, 400),
		tsdp(140, 800),
		tsdp(180, 200),
		tsdp(190, 240),
		tsdp(210, 500),
		tsdp(230, 490),
		tsdp(320, 590),
		tsdp(350, 990),
	}

	// Confirm non-rollup case.
	nonRollupRow := makeInternalRowData(50, 10, data)
	nonRollupColumn := makeInternalColumnData(50, 10, data)
	expectedNonRollupRow := roachpb.InternalTimeSeriesData{
		StartTimestampNanos: 50,
		SampleDurationNanos: 10,
	}
	expectedNonRollupColumn := expectedNonRollupRow
	for _, val := range data {
		offset := int32((val.TimestampNanos - 50) / 10)
		expectedNonRollupRow.Samples = append(expectedNonRollupRow.Samples, roachpb.InternalTimeSeriesSample{
			Offset: offset,
			Count:  1,
			Sum:    val.Value,
		})
		expectedNonRollupColumn.Offset = append(expectedNonRollupColumn.Offset, offset)
		expectedNonRollupColumn.Last = append(expectedNonRollupColumn.Last, val.Value)
	}
	if a, e := nonRollupRow, expectedNonRollupRow; !reflect.DeepEqual(a, e) {
		t.Errorf("nonRollupRow got %v, wanted %v", a, e)
	}
	if a, e := nonRollupColumn, expectedNonRollupColumn; !reflect.DeepEqual(a, e) {
		t.Errorf("nonRollupColumn got %v, wanted %v", a, e)
	}

	// Confirm rollup-generating case. Values are checked against the
	// independently-verified methods of the testmodel package.
	rollupRow := makeInternalRowData(50, 50, data)
	rollupColumn := makeInternalColumnData(50, 50, data)
	expectedRollupRow := roachpb.InternalTimeSeriesData{
		StartTimestampNanos: 50,
		SampleDurationNanos: 50,
	}
	expectedRollupColumn := expectedRollupRow

	dataSeries := testmodel.DataSeries(data)
	// Last and Offset column.
	for _, dp := range dataSeries.GroupByResolution(50, testmodel.AggregateLast) {
		offset := int32((dp.TimestampNanos - 50) / 50)
		expectedRollupRow.Samples = append(expectedRollupRow.Samples, roachpb.InternalTimeSeriesSample{
			Offset: offset,
		})
		expectedRollupColumn.Offset = append(expectedRollupColumn.Offset, offset)
		expectedRollupColumn.Last = append(expectedRollupColumn.Last, dp.Value)
	}
	// Sum column.
	for i, dp := range dataSeries.GroupByResolution(50, testmodel.AggregateSum) {
		expectedRollupRow.Samples[i].Sum = dp.Value
		expectedRollupColumn.Sum = append(expectedRollupColumn.Sum, dp.Value)
	}
	// Max column.
	for i, dp := range dataSeries.GroupByResolution(50, testmodel.AggregateMax) {
		expectedRollupRow.Samples[i].Max = proto.Float64(dp.Value)
		expectedRollupColumn.Max = append(expectedRollupColumn.Max, dp.Value)
	}
	// Min column.
	for i, dp := range dataSeries.GroupByResolution(50, testmodel.AggregateMin) {
		expectedRollupRow.Samples[i].Min = proto.Float64(dp.Value)
		expectedRollupColumn.Min = append(expectedRollupColumn.Min, dp.Value)
	}
	// Count column.
	for i, dp := range dataSeries.GroupByResolution(50, func(ds testmodel.DataSeries) float64 {
		return float64(len(ds))
	}) {
		count := uint32(int32(dp.Value))
		expectedRollupRow.Samples[i].Count = count
		// Min and max are omitted from samples with a count of 1.
		if count < 2 {
			expectedRollupRow.Samples[i].Min = nil
			expectedRollupRow.Samples[i].Max = nil
		}
		expectedRollupColumn.Count = append(expectedRollupColumn.Count, count)
	}
	// First column.
	for _, dp := range dataSeries.GroupByResolution(50, testmodel.AggregateFirst) {
		expectedRollupColumn.First = append(expectedRollupColumn.First, dp.Value)
	}
	// Variance column.
	for _, dp := range dataSeries.GroupByResolution(50, testmodel.AggregateVariance) {
		expectedRollupColumn.Variance = append(expectedRollupColumn.Variance, dp.Value)
	}

	if a, e := rollupRow, expectedRollupRow; !reflect.DeepEqual(a, e) {
		t.Errorf("rollupRow got %v, wanted %v", a, e)
		for _, diff := range pretty.Diff(a, e) {
			t.Error(diff)
		}
	}
	if a, e := rollupColumn, expectedRollupColumn; !reflect.DeepEqual(a, e) {
		t.Errorf("rollupColumn got %v, wanted %v", a, e)
		for _, diff := range pretty.Diff(a, e) {
			t.Error(diff)
		}
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
			makeInternalRowData(0, 10, []tspb.TimeSeriesDatapoint{
				tsdp(10, 1),
				tsdp(20, 2),
			}),
			makeInternalRowData(30, 10, []tspb.TimeSeriesDatapoint{
				tsdp(30, 3),
			}),
			makeInternalRowData(50, 10, []tspb.TimeSeriesDatapoint{
				tsdp(50, 5),
				tsdp(70, 7),
				tsdp(90, 9),
			}),
		}))
	})

	t.Run("columns only", func(t *testing.T) {
		verifyIterTest(t, makeTimeSeriesSpanIterator(timeSeriesSpan{
			makeInternalColumnData(0, 10, []tspb.TimeSeriesDatapoint{
				tsdp(10, 1),
				tsdp(20, 2),
			}),
			makeInternalColumnData(30, 10, []tspb.TimeSeriesDatapoint{
				tsdp(30, 3),
			}),
			makeInternalColumnData(50, 10, []tspb.TimeSeriesDatapoint{
				tsdp(50, 5),
				tsdp(70, 7),
				tsdp(90, 9),
			}),
		}))
	})

	t.Run("mixed rows and columns", func(t *testing.T) {
		verifyIterTest(t, makeTimeSeriesSpanIterator(timeSeriesSpan{
			makeInternalRowData(0, 10, []tspb.TimeSeriesDatapoint{
				tsdp(10, 1),
				tsdp(20, 2),
			}),
			makeInternalColumnData(30, 10, []tspb.TimeSeriesDatapoint{
				tsdp(30, 3),
			}),
			makeInternalRowData(50, 10, []tspb.TimeSeriesDatapoint{
				tsdp(50, 5),
				tsdp(70, 7),
				tsdp(90, 9),
			}),
		}))
	})
}

func TestTimeSeriesSpanIteratorValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	iter := makeTimeSeriesSpanIterator(timeSeriesSpan{
		makeInternalRowData(0, 10, []tspb.TimeSeriesDatapoint{
			tsdp(10, 1),
			tsdp(20, 2),
			tsdp(20, 4),
		}),
		makeInternalRowData(30, 10, []tspb.TimeSeriesDatapoint{
			tsdp(30, 3),
			tsdp(30, 6),
			tsdp(30, 9),
		}),
		makeInternalRowData(50, 10, []tspb.TimeSeriesDatapoint{
			tsdp(50, 12),
			tsdp(70, 700),
			tsdp(90, 9),
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
	samples        []tspb.TimeSeriesDatapoint
}

func TestDownsampleSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Each test case is structured as such:
	// + A description of an "input" span, which describes a list of
	// InternalTimeSeriesData structures that will be assembled into a data span.
	// Each structure has a start timestamp, a sample period (should be the same
	// for all structure), and a set of data samples.
	// + A sample period, which should be greater than or equal to the sample
	// period of the input span structures.
	// + A downsampler operation.
	// + A description of an "expected" span, which describes the list of
	// InternalTimeSeriesData structures that should result from running the input
	// span through the downsampling operation.
	//
	// Importantly, both the "input" and "expected" spans are defined using the
	// dataDesc structure, rather than explicitly creating InternalTimeSeriesData
	// structures. This is because we want to test both the row format and the
	// columnar format of InternalTimeSeriesData when downsampling - therefore,
	// using the descriptors, each test case is run using first row-structured
	// data, then column-structured data, and finally a mixed-format test which
	// combines the two. This gives us a very broad test area while still
	// maintaining a compact set of test cases.
	for tcnum, tc := range []struct {
		inputDesc    []dataDesc
		samplePeriod int64
		downsampler  tspb.TimeSeriesQueryAggregator
		expectedDesc []dataDesc
	}{
		// Original sample period, average downsampler.
		{
			inputDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(10, 1),
					tsdp(20, 2),
					tsdp(20, 4),
					tsdp(30, 5),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 5),
					tsdp(60, 6),
				}},
			},
			samplePeriod: 10,
			downsampler:  tspb.TimeSeriesQueryAggregator_AVG,
			expectedDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(10, 1),
					tsdp(20, 3),
					tsdp(30, 5),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 5),
					tsdp(60, 6),
				}},
			},
		},
		// Original sample period, max downsampler. Should fill in max value.
		{
			inputDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(10, 1),
					tsdp(20, 2),
					tsdp(20, 4),
					tsdp(30, 5),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 5),
					tsdp(60, 6),
				}},
			},
			samplePeriod: 10,
			downsampler:  tspb.TimeSeriesQueryAggregator_MAX,
			expectedDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(10, 1),
					tsdp(20, 4),
					tsdp(30, 5),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 5),
					tsdp(60, 6),
				}},
			},
		},
		// Original sample period, min downsampler.
		{
			inputDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(10, 1),
					tsdp(20, 2),
					tsdp(20, 4),
					tsdp(30, 5),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 5),
					tsdp(60, 6),
				}},
			},
			samplePeriod: 10,
			downsampler:  tspb.TimeSeriesQueryAggregator_MIN,
			expectedDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(10, 1),
					tsdp(20, 2),
					tsdp(30, 5),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 5),
					tsdp(60, 6),
				}},
			},
		},
		// AVG downsamper. Should re-use original span data.
		{
			inputDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(10, 1),
					tsdp(20, 2),
					tsdp(20, 4),
					tsdp(30, 5),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 5),
					tsdp(60, 6),
				}},
				{70, 10, []tspb.TimeSeriesDatapoint{
					tsdp(70, 7),
					tsdp(90, 9),
					tsdp(110, 8),
				}},
			},
			samplePeriod: 50,
			downsampler:  tspb.TimeSeriesQueryAggregator_AVG,
			expectedDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(0, 3),
					tsdp(50, 6.75),
					tsdp(100, 8),
				}},
			},
		},
		// MAX downsamper. Should re-use original span data; note that the sum and
		// count values are NOT overwritten.
		{
			inputDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(10, 1),
					tsdp(20, 2),
					tsdp(20, 4),
					tsdp(30, 5),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 5),
					tsdp(60, 6),
				}},
				{70, 10, []tspb.TimeSeriesDatapoint{
					tsdp(70, 7),
					tsdp(90, 9),
					tsdp(110, 8),
				}},
			},
			samplePeriod: 50,
			downsampler:  tspb.TimeSeriesQueryAggregator_MAX,
			expectedDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(0, 5),
					tsdp(50, 9),
					tsdp(100, 8),
				}},
			},
		},
		// MIN downsamper. Should re-use original span data; note that the sum and
		// count values are NOT overwritten.
		{
			inputDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(10, 1),
					tsdp(20, 2),
					tsdp(20, 4),
					tsdp(30, 5),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 5),
					tsdp(60, 6),
				}},
				{70, 10, []tspb.TimeSeriesDatapoint{
					tsdp(70, 7),
					tsdp(90, 9),
					tsdp(110, 8),
				}},
			},
			samplePeriod: 50,
			downsampler:  tspb.TimeSeriesQueryAggregator_MIN,
			expectedDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(0, 1),
					tsdp(50, 5),
					tsdp(100, 8),
				}},
			},
		},
		// AVG downsampler, downsampling while re-using multiple
		// InternalTimeSeriesData structures.
		{
			inputDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(10, 1),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 5),
					tsdp(60, 6),
				}},
				{70, 10, []tspb.TimeSeriesDatapoint{
					tsdp(70, 7),
					tsdp(90, 9),
					tsdp(110, 8),
				}},
			},
			samplePeriod: 50,
			downsampler:  tspb.TimeSeriesQueryAggregator_AVG,
			expectedDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(0, 1),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 6.75),
					tsdp(100, 8),
				}},
			},
		},
		// MAX downsampler, downsampling while re-using multiple
		// InternalTimeSeriesData structures.
		{
			inputDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(10, 1),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 5),
					tsdp(60, 6),
				}},
				{70, 10, []tspb.TimeSeriesDatapoint{
					tsdp(70, 7),
					tsdp(90, 9),
					tsdp(110, 8),
				}},
			},
			samplePeriod: 50,
			downsampler:  tspb.TimeSeriesQueryAggregator_MAX,
			expectedDesc: []dataDesc{
				{0, 10, []tspb.TimeSeriesDatapoint{
					tsdp(0, 1),
				}},
				{50, 10, []tspb.TimeSeriesDatapoint{
					tsdp(50, 9),
					tsdp(100, 8),
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
