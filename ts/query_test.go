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

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

var (
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
					Offset: 6,
					Count:  2,
					Sum:    80,
					Max:    proto.Float64(50),
					Min:    proto.Float64(30),
				},
			},
		},
	}
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
		expected     []float64
		downsampleFn downsampleFn
	}{
		{
			[]float64{3.4, 4.2, 5, 7.5, 10, 15, 20, 24, 28, 32, 36, 40, 0},
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Average()
			},
		},
		{
			[]float64{3.4, 4.2, 5, 7.5, 10, 20, 30, 34, 38, 42, 46, 50, 0},
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Maximum()
			},
		},
		{
			[]float64{3.4, 4.2, 5, 7.5, 10, 35, 60, 64, 68, 72, 76, 80, 0},
			func(s roachpb.InternalTimeSeriesSample) float64 {
				return s.Sum
			},
		},
	}

	for i, tc := range testCases {
		actual := make([]float64, 0, len(tc.expected))
		iter := ds.newIterator(0, tc.downsampleFn)
		for i := 0; i < len(tc.expected); i++ {
			iter.advanceTo(int32(i))
			actual = append(actual, iter.value())
		}
		if !reflect.DeepEqual(actual, tc.expected) {
			t.Fatalf("test %d, interpolated values: %v, expected values: %v", i, actual, tc.expected)
		}
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
		aggFunc  func(ui unionIterator) float64
	}{
		{
			[]float64{4.4, 12, 17.5, 35, 40, 80, 56},
			func(ui unionIterator) float64 {
				return ui.sum()
			},
		},
		{
			[]float64{3.4, 7, 10, 25, 20, 40, 56},
			func(ui unionIterator) float64 {
				return ui.max()
			},
		},
		{
			[]float64{1, 5, 7.5, 10, 20, 40, 0},
			func(ui unionIterator) float64 {
				return ui.min()
			},
		},
		{
			[]float64{2.2, 6, 8.75, 17.5, 20, 40, 28},
			func(ui unionIterator) float64 {
				return ui.avg()
			},
		},
	}

	downsampleFn := func(s roachpb.InternalTimeSeriesSample) float64 {
		return s.Average()
	}
	for i, tc := range testCases {
		actual := make([]float64, 0, len(tc.expected))
		iters := unionIterator{
			dataSpan1.newIterator(0, downsampleFn),
			dataSpan2.newIterator(0, downsampleFn),
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
	downsample, agg *TimeSeriesQueryAggregator,
	derivative *TimeSeriesQueryDerivative,
	r Resolution,
	start, end int64,
	expectedDatapointCount, expectedSourceCount int,
) {
	// Query the actual server.
	q := Query{
		Name:             name,
		Downsampler:      downsample,
		SourceAggregator: agg,
		Derivative:       derivative,
		Sources:          sources,
	}
	actualDatapoints, actualSources, err := tm.DB.Query(q, r, start, end)
	if err != nil {
		tm.t.Fatal(err)
	}
	if a, e := len(actualDatapoints), expectedDatapointCount; a != e {
		tm.t.Logf("actual datapoints: %v", actualDatapoints)
		tm.t.Fatal(util.ErrorfSkipFrames(1, "query expected %d datapoints, got %d", e, a))
	}
	if a, e := len(actualSources), expectedSourceCount; a != e {
		tm.t.Fatal(util.ErrorfSkipFrames(1, "query expected %d sources, got %d", e, a))
	}

	// Construct an expected result for comparison.
	var expectedDatapoints []TimeSeriesDatapoint
	expectedSources := make([]string, 0, 0)
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
		for time := start - (start % r.KeyDuration()); time < end; time += r.KeyDuration() {
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

	// Iterate over data in all dataSpans and construct expected datapoints.
	var startOffset int32
	isDerivative := q.GetDerivative() != TimeSeriesQueryDerivative_NONE
	if isDerivative {
		startOffset = -1
	}
	downsampleFn, err := getDownsampleFunction(q.GetDownsampler())
	if err != nil {
		tm.t.Fatal(err)
	}
	var iters unionIterator
	for _, ds := range dataSpans {
		iters = append(iters, ds.newIterator(startOffset, downsampleFn))
	}

	iters.init()
	currentVal := func() TimeSeriesDatapoint {
		var value float64
		switch q.GetSourceAggregator() {
		case TimeSeriesQueryAggregator_SUM:
			value = iters.sum()
		case TimeSeriesQueryAggregator_AVG:
			value = iters.avg()
		case TimeSeriesQueryAggregator_MAX:
			value = iters.max()
		case TimeSeriesQueryAggregator_MIN:
			value = iters.min()
		default:
			tm.t.Fatalf("unknown query aggregator %s", q.GetSourceAggregator())
		}
		return TimeSeriesDatapoint{
			TimestampNanos: iters.timestamp(),
			Value:          value,
		}
	}

	var last TimeSeriesDatapoint
	if isDerivative {
		last = currentVal()
		if iters.offset() < 0 {
			iters.advance()
		}
	}
	for iters.isValid() && iters.timestamp() <= end {
		current := currentVal()
		result := current
		if isDerivative {
			dTime := (current.TimestampNanos - last.TimestampNanos) / int64(time.Second)
			if dTime == 0 {
				result.Value = 0
			} else {
				result.Value = (current.Value - last.Value) / float64(dTime)
			}
			if result.Value < 0 &&
				q.GetDerivative() == TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE {
				result.Value = 0
			}
		}
		expectedDatapoints = append(expectedDatapoints, result)
		last = current
		iters.advance()
	}

	sort.Strings(expectedSources)
	sort.Strings(actualSources)
	if !reflect.DeepEqual(actualSources, expectedSources) {
		tm.t.Error(util.ErrorfSkipFrames(1, "actual source list: %v, expected: %v", actualSources, expectedSources))
	}
	if !reflect.DeepEqual(actualDatapoints, expectedDatapoints) {
		tm.t.Error(util.ErrorfSkipFrames(1, "actual datapoints: %v, expected: %v", actualDatapoints, expectedDatapoints))
	}
}

// TestQuery validates that query results match the expectation of the test
// model.
func TestQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	tm.storeTimeSeriesData(resolution1ns, []TimeSeriesData{
		{
			Name: "test.metric",
			Datapoints: []TimeSeriesDatapoint{
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
	tm.assertQuery("test.metric", nil, nil, nil, nil, resolution1ns, 0, 60, 7, 1)

	// Verify across multiple sources
	tm.storeTimeSeriesData(resolution1ns, []TimeSeriesData{
		{
			Name:   "test.multimetric",
			Source: "source1",
			Datapoints: []TimeSeriesDatapoint{
				datapoint(1, 100),
				datapoint(15, 300),
				datapoint(17, 500),
				datapoint(52, 900),
			},
		},
		{
			Name:   "test.multimetric",
			Source: "source2",
			Datapoints: []TimeSeriesDatapoint{
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
	tm.assertQuery("test.multimetric", nil, nil, nil, nil, resolution1ns, 0, 90, 8, 2)
	// Test with aggregator specified.
	tm.assertQuery("test.multimetric", nil, TimeSeriesQueryAggregator_MAX.Enum(), nil, nil,
		resolution1ns, 0, 90, 8, 2)
	// Test with aggregator and downsampler.
	tm.assertQuery("test.multimetric", nil, TimeSeriesQueryAggregator_MAX.Enum(), TimeSeriesQueryAggregator_AVG.Enum(), nil,
		resolution1ns, 0, 90, 8, 2)
	// Test with derivative specified.
	tm.assertQuery("test.multimetric", nil, TimeSeriesQueryAggregator_AVG.Enum(), nil,
		TimeSeriesQueryDerivative_DERIVATIVE.Enum(), resolution1ns, 0, 90, 8, 2)
	// Test with everything specified.
	tm.assertQuery("test.multimetric", nil, TimeSeriesQueryAggregator_MIN.Enum(), TimeSeriesQueryAggregator_MAX.Enum(),
		TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE.Enum(), resolution1ns, 0, 90, 8, 2)
	// Test query that returns no data.
	tm.assertQuery("nodata", nil, nil, nil, nil, resolution1ns, 0, 90, 0, 0)

	// Verify querying specific sources, thus excluding other available sources
	// in the same time period.
	tm.storeTimeSeriesData(resolution1ns, []TimeSeriesData{
		{
			Name:   "test.specificmetric",
			Source: "source1",
			Datapoints: []TimeSeriesDatapoint{
				datapoint(1, 9999),
				datapoint(11, 9999),
				datapoint(21, 9999),
				datapoint(31, 9999),
			},
		},
		{
			Name:   "test.specificmetric",
			Source: "source2",
			Datapoints: []TimeSeriesDatapoint{
				datapoint(2, 10),
				datapoint(12, 15),
				datapoint(22, 25),
				datapoint(32, 60),
			},
		},
		{
			Name:   "test.specificmetric",
			Source: "source3",
			Datapoints: []TimeSeriesDatapoint{
				datapoint(3, 9999),
				datapoint(13, 9999),
				datapoint(23, 9999),
				datapoint(33, 9999),
			},
		},
		{
			Name:   "test.specificmetric",
			Source: "source4",
			Datapoints: []TimeSeriesDatapoint{
				datapoint(4, 15),
				datapoint(14, 45),
				datapoint(24, 60),
				datapoint(32, 100),
			},
		},
		{
			Name:   "test.specificmetric",
			Source: "source5",
			Datapoints: []TimeSeriesDatapoint{
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
	tm.assertQuery("test.specificmetric", []string{"source2", "source4", "source6"}, nil, nil, nil, resolution1ns, 0, 90, 7, 2)

	// Assert querying data over limited range for single source. Regression
	// test for #4987.
	tm.assertQuery("test.specificmetric", []string{"source4", "source5"}, nil, nil, nil, resolution1ns, 5, 24, 4, 2)
}
