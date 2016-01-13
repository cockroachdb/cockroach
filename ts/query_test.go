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

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

var (
	testSeries1 = []*roachpb.InternalTimeSeriesData{
		{
			StartTimestampNanos: 0,
			SampleDurationNanos: 10,
			Samples: []*roachpb.InternalTimeSeriesSample{
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
			Samples: []*roachpb.InternalTimeSeriesSample{
				{
					Offset: 1,
					Count:  3,
					Sum:    60,
				},
				{
					Offset: 6,
					Count:  2,
					Sum:    80,
				},
			},
		},
	}
	testSeries2 = []*roachpb.InternalTimeSeriesData{
		{
			StartTimestampNanos: 30,
			SampleDurationNanos: 10,
			Samples: []*roachpb.InternalTimeSeriesSample{
				{
					Offset: 0,
					Count:  1,
					Sum:    1,
				},
				{
					Offset: 3,
					Count:  5,
					Sum:    50,
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
				},
				{
					Offset: 15,
					Count:  2,
					Sum:    112,
				},
			},
		},
	}
)

// TestInterpolation verifies the interpolated average values of a single interpolatingIterator.
func TestAvgInterpolation(t *testing.T) {
	defer leaktest.AfterTest(t)
	dataSpan := &dataSpan{
		startNanos:  30,
		sampleNanos: 10,
	}
	for _, data := range testSeries1 {
		if err := dataSpan.addData(data); err != nil {
			t.Fatal(err)
		}
	}

	expected := []float64{3.4, 4.2, 5, 7.5, 10, 15, 20, 24, 28, 32, 36, 40, 0}
	actual := make([]float64, 0, len(expected))
	iter := dataSpan.newIterator()
	for i := 0; i < len(expected); i++ {
		iter.advanceTo(int32(i))
		actual = append(actual, iter.avg())
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("interpolated values: %v, expected values: %v", actual, expected)
	}
}

// TestSumAvgInterpolation verifies the behavior of an iteratorSet, which
// advances multiple interpolatingIterators together.
func TestSumAvgInterpolation(t *testing.T) {
	defer leaktest.AfterTest(t)
	dataSpan1 := &dataSpan{
		startNanos:  30,
		sampleNanos: 10,
	}
	dataSpan2 := &dataSpan{
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

	expected := []float64{4.4, 12, 17.5, 35, 40, 80, 56}
	actual := make([]float64, 0, len(expected))
	offsets := make([]int32, 0, len(expected))
	iters := unionIterator{
		dataSpan1.newIterator(),
		dataSpan2.newIterator(),
	}
	iters.init()
	for iters.isValid() {
		actual = append(actual, iters.avg())
		offsets = append(offsets, iters[0].offset)
		iters.advance()
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("summed values: %v, expected values: %v", actual, expected)
	}
}

func TestSumRateInterpolation(t *testing.T) {
	defer leaktest.AfterTest(t)
	dataSpan1 := &dataSpan{
		startNanos:  30,
		sampleNanos: 10,
	}
	dataSpan2 := &dataSpan{
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

	expected := []float64{0.8, 3.8, 5.5, 17.5, 2.5, 8, 4}
	actual := make([]float64, 0, len(expected))
	offsets := make([]int32, 0, len(expected))
	iters := unionIterator{
		dataSpan1.newIterator(),
		dataSpan2.newIterator(),
	}
	iters.init()
	for iters.isValid() {
		actual = append(actual, iters.dAvg())
		offsets = append(offsets, iters[0].offset)
		iters.advance()
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("summed values: %v, expected values: %v", actual, expected)
	}
}

// assertQuery generates a query result from the local test model and compares
// it against the query returned from the server.
func (tm *testModel) assertQuery(name string, sources []string, agg *TimeSeriesQueryAggregator,
	r Resolution, start, end int64, expectedDatapointCount int, expectedSourceCount int) {
	// Query the actual server.
	q := TimeSeriesQueryRequest_Query{
		Name:       name,
		Aggregator: agg,
		Sources:    sources,
	}
	actualDatapoints, actualSources, err := tm.DB.Query(q, r, start, end)
	if err != nil {
		tm.t.Fatal(err)
	}
	if a, e := len(actualDatapoints), expectedDatapointCount; a != e {
		tm.t.Logf("actual datapoints: %v", actualDatapoints)
		tm.t.Fatalf("query expected %d datapoints, got %d", e, a)
	}
	if a, e := len(actualSources), expectedSourceCount; a != e {
		tm.t.Fatalf("query expected %d sources, got %d", e, a)
	}

	// Construct an expected result for comparison.
	var expectedDatapoints []*TimeSeriesDatapoint
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
			if err := ds.addData(&data); err != nil {
				tm.t.Fatal(err)
			}
		}
	}

	// Iterate over data in all dataSpans and construct expected datapoints.
	var iters unionIterator
	for _, ds := range dataSpans {
		iters = append(iters, ds.newIterator())
	}
	iters.init()
	for iters.isValid() {
		var value float64
		switch q.GetAggregator() {
		case TimeSeriesQueryAggregator_AVG:
			value = iters.avg()
		case TimeSeriesQueryAggregator_AVG_RATE:
			value = iters.dAvg()
		}
		expectedDatapoints = append(expectedDatapoints, &TimeSeriesDatapoint{
			TimestampNanos: iters.timestamp(),
			Value:          value,
		})
		iters.advance()
	}

	sort.Strings(expectedSources)
	sort.Strings(actualSources)
	if !reflect.DeepEqual(actualSources, expectedSources) {
		tm.t.Errorf("actual source list: %v, expected: %v", actualSources, expectedSources)
	}
	if !reflect.DeepEqual(actualDatapoints, expectedDatapoints) {
		tm.t.Errorf("actual datapoints: %v, expected: %v", actualDatapoints, expectedDatapoints)
	}
}

// TestQuery validates that query results match the expectation of the test
// model.
func TestQuery(t *testing.T) {
	defer leaktest.AfterTest(t)
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	tm.storeTimeSeriesData(resolution1ns, []TimeSeriesData{
		{
			Name: "test.metric",
			Datapoints: []*TimeSeriesDatapoint{
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
	tm.assertQuery("test.metric", nil, nil, resolution1ns, 0, 60, 7, 1)

	// Verify across multiple sources
	tm.storeTimeSeriesData(resolution1ns, []TimeSeriesData{
		{
			Name:   "test.multimetric",
			Source: "source1",
			Datapoints: []*TimeSeriesDatapoint{
				datapoint(1, 100),
				datapoint(15, 300),
				datapoint(17, 500),
				datapoint(52, 900),
			},
		},
		{
			Name:   "test.multimetric",
			Source: "source2",
			Datapoints: []*TimeSeriesDatapoint{
				datapoint(5, 100),
				datapoint(16, 300),
				datapoint(22, 500),
				datapoint(82, 900),
			},
		},
	})

	tm.assertKeyCount(11)
	tm.assertModelCorrect()
	tm.assertQuery("test.multimetric", nil, nil, resolution1ns, 0, 90, 8, 2)
	tm.assertQuery("test.multimetric", nil, TimeSeriesQueryAggregator_AVG.Enum(),
		resolution1ns, 0, 90, 8, 2)
	tm.assertQuery("test.multimetric", nil, TimeSeriesQueryAggregator_AVG_RATE.Enum(),
		resolution1ns, 0, 90, 8, 2)
	tm.assertQuery("nodata", nil, nil, resolution1ns, 0, 90, 0, 0)

	// Verify querying specific sources, thus excluding other available sources
	// in the same time period.
	tm.storeTimeSeriesData(resolution1ns, []TimeSeriesData{
		{
			Name:   "test.specificmetric",
			Source: "source1",
			Datapoints: []*TimeSeriesDatapoint{
				datapoint(1, 9999),
				datapoint(11, 9999),
				datapoint(21, 9999),
				datapoint(31, 9999),
			},
		},
		{
			Name:   "test.specificmetric",
			Source: "source2",
			Datapoints: []*TimeSeriesDatapoint{
				datapoint(2, 10),
				datapoint(12, 15),
				datapoint(22, 25),
				datapoint(32, 60),
			},
		},
		{
			Name:   "test.specificmetric",
			Source: "source3",
			Datapoints: []*TimeSeriesDatapoint{
				datapoint(3, 9999),
				datapoint(13, 9999),
				datapoint(23, 9999),
				datapoint(33, 9999),
			},
		},
		{
			Name:   "test.specificmetric",
			Source: "source4",
			Datapoints: []*TimeSeriesDatapoint{
				datapoint(4, 15),
				datapoint(14, 45),
				datapoint(24, 60),
				datapoint(32, 100),
			},
		},
		{
			Name:   "test.specificmetric",
			Source: "source5",
			Datapoints: []*TimeSeriesDatapoint{
				datapoint(5, 9999),
				datapoint(15, 9999),
				datapoint(25, 9999),
				datapoint(35, 9999),
			},
		},
	})

	tm.assertKeyCount(31)
	tm.assertModelCorrect()
	tm.assertQuery("test.specificmetric", []string{"source2", "source4", "source6"}, nil, resolution1ns, 0, 90, 7, 2)
}
