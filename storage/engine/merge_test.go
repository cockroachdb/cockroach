// Copyright 2014 The Cockroach Authors.
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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Peter Mattis (peter.mattis@gmail.com)

package engine

import (
	"math"
	"math/rand"
	"reflect"
	"testing"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
)

var testtime = int64(-446061360000000000)

type tsIntSample struct {
	offset int32
	count  uint32
	sum    int64
	max    int64
	min    int64
}

type tsFloatSample struct {
	offset int32
	count  uint32
	sum    float32
	max    float32
	min    float32
}

func gibberishString(n int) string {
	b := make([]byte, n, n)
	for i := 0; i < n; i++ {
		b[i] = byte(rand.Intn(math.MaxUint8 + 1))
	}
	return string(b)
}

func mustMarshal(m gogoproto.Message) []byte {
	b, err := gogoproto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

func counter(n int64) []byte {
	v := &proto.MVCCMetadata{
		Value: &proto.Value{
			Integer: gogoproto.Int64(n),
		},
	}
	return mustMarshal(v)
}

func appender(s string) []byte {
	v := &proto.MVCCMetadata{
		Value: &proto.Value{
			Bytes: []byte(s),
		},
	}
	return mustMarshal(v)
}

// timeSeriesInt generates a simple InternalTimeSeriesData object which starts
// at the given timestamp and has samples of the given duration. Samples have
// int values.
func timeSeriesInt(start int64, duration int64, samples ...tsIntSample) []byte {
	ts := &proto.InternalTimeSeriesData{
		StartTimestampNanos: start,
		SampleDurationNanos: duration,
	}
	for _, sample := range samples {
		newSample := &proto.InternalTimeSeriesSample{
			Offset:   sample.offset,
			IntCount: sample.count,
			IntSum:   gogoproto.Int64(sample.sum),
		}
		if sample.count > 1 {
			newSample.IntMax = gogoproto.Int64(sample.max)
			newSample.IntMin = gogoproto.Int64(sample.min)
		}
		ts.Samples = append(ts.Samples, newSample)
	}
	v, err := ts.ToValue()
	if err != nil {
		panic(err)
	}
	return mustMarshal(&proto.MVCCMetadata{Value: v})
}

// timeSeriesFloat generates a simple InternalTimeSeriesData object which starts
// at the given timestamp and has samples of the given duration. Samples have
// float values.
func timeSeriesFloat(start int64, duration int64, samples ...tsFloatSample) []byte {
	ts := &proto.InternalTimeSeriesData{
		StartTimestampNanos: start,
		SampleDurationNanos: duration,
	}
	for _, sample := range samples {
		newSample := &proto.InternalTimeSeriesSample{
			Offset:     sample.offset,
			FloatCount: sample.count,
			FloatSum:   gogoproto.Float32(sample.sum),
		}
		if sample.count > 1 {
			newSample.FloatMax = gogoproto.Float32(sample.max)
			newSample.FloatMin = gogoproto.Float32(sample.min)
		}
		ts.Samples = append(ts.Samples, newSample)
	}
	v, err := ts.ToValue()
	if err != nil {
		panic(err)
	}
	return mustMarshal(&proto.MVCCMetadata{Value: v})
}

// TestGoMerge tests the function goMerge but not the integration with
// the storage engines. For that, see the engine tests.
func TestGoMerge(t *testing.T) {
	// Let's start with stuff that should go wrong.
	badCombinations := []struct {
		existing, update []byte
	}{
		{counter(0), appender("")},
		{appender(""), counter(0)},
		{counter(0), nil},
		{appender(""), nil},
		{
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
			}...),
			nil,
		},
		{
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
			}...),
			appender("a"),
		},
		{
			appender("a"),
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeriesInt(testtime+1, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeriesInt(testtime, 100, []tsIntSample{
				{1, 1, 5, 5, 5},
			}...),
		},
	}
	for i, c := range badCombinations {
		_, err := goMerge(c.existing, c.update)
		if err == nil {
			t.Errorf("goMerge: %d: expected error", i)
		}
	}

	testCasesCounter := []struct {
		existing, update, expected int64
		wantError                  bool
	}{
		{0, 10, 10, false},
		{10, 20, 30, false},
		{595, -600, -5, false},
		// Close to overflow, but not quite there.
		{math.MinInt64 + 3, -3, math.MinInt64, false},
		{math.MaxInt64, 0, math.MaxInt64, false},
		// Overflows.
		{math.MaxInt64, 1, 0, true},
		{-1, math.MinInt64, 0, true},
	}
	for i, c := range testCasesCounter {
		result, err := goMerge(counter(c.existing), counter(c.update))
		if c.wantError {
			if err == nil {
				t.Errorf("goMerge: %d: wanted error but got success", i)
			}
			continue
		}
		if err != nil {
			t.Errorf("goMerge error: %d: %v", i, err)
			continue
		}
		var v proto.MVCCMetadata
		if err := gogoproto.Unmarshal(result, &v); err != nil {
			t.Errorf("goMerge error unmarshalling: %s", err)
			continue
		}
		if v.Value.GetInteger() != c.expected {
			t.Errorf("goMerge error: %d: want %v, got %v", i, c.expected, v.Value.GetInteger())
		}
	}

	gibber1, gibber2 := gibberishString(100), gibberishString(200)

	testCasesAppender := []struct {
		existing, update, expected []byte
	}{
		{appender(""), appender(""), appender("")},
		{nil, appender(""), appender("")},
		{nil, nil, mustMarshal(&proto.MVCCMetadata{Value: &proto.Value{}})},
		{appender("\n "), appender(" \t "), appender("\n  \t ")},
		{appender("ქართული"), appender("\nKhartuli"), appender("ქართული\nKhartuli")},
		{appender(gibber1), appender(gibber2), appender(gibber1 + gibber2)},
	}

	for i, c := range testCasesAppender {
		result, err := goMerge(c.existing, c.update)
		if err != nil {
			t.Errorf("goMerge error: %d: %v", i, err)
			continue
		}
		var resultV, expectedV proto.MVCCMetadata
		gogoproto.Unmarshal(result, &resultV)
		gogoproto.Unmarshal(c.expected, &expectedV)
		if !reflect.DeepEqual(resultV, expectedV) {
			t.Errorf("goMerge error: %d: want %+v, got %+v", i, expectedV, resultV)
		}
	}

	testCasesTimeSeries := []struct {
		existing, update, expected []byte
	}{
		{
			nil,
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{2, 1, 5, 5, 5},
			}...),
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{2, 1, 5, 5, 5},
			}...),
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 10, 10, 10},
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 100, 100, 100},
				{2, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 3, 115, 100, 5},
				{2, 2, 10, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{2, 1, 5, 5, 5},
			}...),
			timeSeriesInt(testtime, 1000, []tsIntSample{
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesFloat(testtime, 1000, []tsFloatSample{
				{1, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
			timeSeriesFloat(testtime, 1000, []tsFloatSample{
				{2, 1, 5, 5, 5},
			}...),
			timeSeriesFloat(testtime, 1000, []tsFloatSample{
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesFloat(testtime, 1000, []tsFloatSample{
				{1, 1, 10, 10, 10},
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
			timeSeriesFloat(testtime, 1000, []tsFloatSample{
				{1, 1, 100, 100, 100},
				{2, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
			timeSeriesFloat(testtime, 1000, []tsFloatSample{
				{1, 3, 115, 100, 5},
				{2, 2, 10, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
		},
	}

	for i, c := range testCasesTimeSeries {
		result, err := goMerge(c.existing, c.update)
		if err != nil {
			t.Errorf("goMerge error: %d: %v", i, err)
			continue
		}

		// Extract the time series and compare.
		var resultV, expectedV proto.MVCCMetadata
		gogoproto.Unmarshal(result, &resultV)
		gogoproto.Unmarshal(c.expected, &expectedV)
		resultTS, _ := proto.InternalTimeSeriesDataFromValue(resultV.Value)
		expectedTS, _ := proto.InternalTimeSeriesDataFromValue(expectedV.Value)
		if !reflect.DeepEqual(resultTS, expectedTS) {
			t.Errorf("goMerge error: %d: want %v, got %v", i, expectedTS, resultTS)
		}
	}
}
