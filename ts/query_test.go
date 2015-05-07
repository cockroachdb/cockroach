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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package ts

import (
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
)

var (
	testSeries1 = []*proto.InternalTimeSeriesData{
		{
			StartTimestampNanos: 0,
			SampleDurationNanos: 10,
			Samples: []*proto.InternalTimeSeriesSample{
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
			Samples: []*proto.InternalTimeSeriesSample{
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
	testSeries2 = []*proto.InternalTimeSeriesData{
		{
			StartTimestampNanos: 30,
			SampleDurationNanos: 10,
			Samples: []*proto.InternalTimeSeriesSample{
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
		actual = append(actual, iters.sumAvg())
		offsets = append(offsets, iters[0].offset)
		iters.advance()
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("summed values: %v, expected values: %v", actual, expected)
	}
}

// assertQuery generates a query result from the local test model and compares
// it against the query returned from the server.
func (tm *testModel) assertQuery(name string, r Resolution, start, end int64,
	expectedDatapoints int, expectedSources int) {
	// Query the actual server.
	actual, err := tm.DB.Query(name, r, start, end)
	if err != nil {
		tm.t.Fatal(err)
	}
	if a, e := len(actual.Datapoints), expectedDatapoints; a != e {
		tm.t.Fatalf("query expected %d datapoints, got %d", e, a)
	}
	if a, e := len(actual.Sources), expectedSources; a != e {
		tm.t.Fatalf("query expected %d sources, got %d", e, a)
	}

	// Construct an expected result for comparison.
	expected := proto.TimeSeriesQueryResult{
		Name:    name,
		Sources: make([]string, 0, 0),
	}
	dataSpans := make(map[string]*dataSpan)

	// Iterate over all possible sources which may have data for this query.
	for sourceName := range tm.seenSources {
		// Iterate over all possible key times at which query data may be present.
		for time := start - (start % r.KeyDuration()); time < end; time += r.KeyDuration() {
			// Construct a key for this source/time and retrieve it from model.
			key := MakeDataKey(name, sourceName, r, time)
			value := tm.modelData[string(key)]
			if value == nil {
				continue
			}

			// Add data from the key to the correct dataSpan.
			data, err := proto.InternalTimeSeriesDataFromValue(value)
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
				expected.Sources = append(expected.Sources, sourceName)
			}
			if err := ds.addData(data); err != nil {
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
		expected.Datapoints = append(expected.Datapoints, &proto.TimeSeriesDatapoint{
			TimestampNanos: iters.timestamp(),
			Value:          iters.sumAvg(),
		})
		iters.advance()
	}

	sort.Strings(expected.Sources)
	sort.Strings(actual.Sources)
	if !reflect.DeepEqual(actual, &expected) {
		tm.t.Fatalf("actual time series: %v, expected: %v", actual, &expected)
	}
}

// TestQuery validates that query results match the expectation of the test
// model.
func TestQuery(t *testing.T) {
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	tm.storeTimeSeriesData(resolution1ns, []proto.TimeSeriesData{
		{
			Name: "test.metric",
			Datapoints: []*proto.TimeSeriesDatapoint{
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
	tm.assertQuery("test.metric", resolution1ns, 0, 60, 7, 1)

	// Verify across multiple sources
	tm.storeTimeSeriesData(resolution1ns, []proto.TimeSeriesData{
		{
			Name:   "test.multimetric",
			Source: "source1",
			Datapoints: []*proto.TimeSeriesDatapoint{
				datapoint(1, 100),
				datapoint(15, 300),
				datapoint(17, 500),
				datapoint(52, 900),
			},
		},
		{
			Name:   "test.multimetric",
			Source: "source2",
			Datapoints: []*proto.TimeSeriesDatapoint{
				datapoint(5, 100),
				datapoint(16, 300),
				datapoint(22, 500),
				datapoint(82, 900),
			},
		},
	})

	tm.assertKeyCount(11)
	tm.assertModelCorrect()
	tm.assertQuery("test.multimetric", resolution1ns, 0, 90, 8, 2)
	tm.assertQuery("nodata", resolution1ns, 0, 90, 0, 0)
}
