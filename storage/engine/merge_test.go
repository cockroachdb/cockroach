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
// Author: Peter Mattis (peter@cockroachlabs.com)

package engine

import (
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
	gogoproto "github.com/gogo/protobuf/proto"
)

var testtime = int64(-446061360000000000)

type tsSample struct {
	offset int32
	count  uint32
	sum    float64
	max    float64
	min    float64
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

func appender(s string) []byte {
	v := &MVCCMetadata{
		Value: &proto.Value{
			Bytes: []byte(s),
		},
	}
	return mustMarshal(v)
}

// timeSeries generates a simple InternalTimeSeriesData object which starts
// at the given timestamp and has samples of the given duration.
func timeSeries(start int64, duration int64, samples ...tsSample) []byte {
	ts := &proto.InternalTimeSeriesData{
		StartTimestampNanos: start,
		SampleDurationNanos: duration,
	}
	for _, sample := range samples {
		newSample := &proto.InternalTimeSeriesSample{
			Offset: sample.offset,
			Count:  sample.count,
			Sum:    sample.sum,
		}
		if sample.count > 1 {
			newSample.Max = gogoproto.Float64(sample.max)
			newSample.Min = gogoproto.Float64(sample.min)
		}
		ts.Samples = append(ts.Samples, newSample)
	}
	v, err := ts.ToValue()
	if err != nil {
		panic(err)
	}
	return mustMarshal(&MVCCMetadata{Value: v})
}

// TestGoMerge tests the function goMerge but not the integration with
// the storage engines. For that, see the engine tests.
func TestGoMerge(t *testing.T) {
	defer leaktest.AfterTest(t)
	// Let's start with stuff that should go wrong.
	badCombinations := []struct {
		existing, update []byte
	}{
		{appender(""), nil},
		{
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			nil,
		},
		{
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			appender("a"),
		},
		{
			appender("a"),
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeries(testtime+1, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeries(testtime, 100, []tsSample{
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

	gibber1, gibber2 := gibberishString(100), gibberishString(200)

	testCasesAppender := []struct {
		existing, update, expected []byte
	}{
		{appender(""), appender(""), appender("")},
		{nil, appender(""), appender("")},
		{nil, nil, mustMarshal(&MVCCMetadata{Value: &proto.Value{}})},
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
		var resultV, expectedV MVCCMetadata
		if err := gogoproto.Unmarshal(result, &resultV); err != nil {
			t.Fatal(err)
		}
		if err := gogoproto.Unmarshal(c.expected, &expectedV); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(resultV, expectedV) {
			t.Errorf("goMerge error: %d: want %+v, got %+v", i, expectedV, resultV)
		}
	}

	testCasesTimeSeries := []struct {
		existing, update, expected []byte
	}{
		{
			nil,
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
		},
		{
			nil,
			timeSeries(testtime, 1000, []tsSample{
				{2, 1, 5, 5, 5},
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
				{2, 2, 10, 5, 5},
			}...),
		},
		{
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeries(testtime, 1000, []tsSample{
				{2, 1, 5, 5, 5},
			}...),
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
			timeSeries(testtime, 1000, []tsSample{
				{2, 1, 5, 5, 5},
			}...),
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 10, 10, 10},
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 100, 100, 100},
				{2, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
			timeSeries(testtime, 1000, []tsSample{
				{1, 3, 115, 100, 5},
				{2, 2, 10, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeries(testtime, 1000, []tsSample{
				{2, 1, 5, 5, 5},
			}...),
			timeSeries(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
		},
	}

	for i, c := range testCasesTimeSeries {
		expectedTS := unmarshalTimeSeries(t, c.expected)
		existingTS := unmarshalTimeSeries(t, c.existing)
		updateTS := unmarshalTimeSeries(t, c.update)

		// Directly test the C++ implementation of merging using goMerge.  goMerge
		// operates directly on marshalled bytes.
		result, err := goMerge(c.existing, c.update)
		if err != nil {
			t.Errorf("goMerge error on case %d: %s", i, err.Error())
			continue
		}
		resultTS := unmarshalTimeSeries(t, result)
		if a, e := resultTS, expectedTS; !reflect.DeepEqual(a, e) {
			t.Errorf("goMerge returned wrong result on case %d: expected %v, returned %v", i, e, a)
		}

		// Test the MergeInternalTimeSeriesData method separately.
		if existingTS == nil {
			resultTS, err = MergeInternalTimeSeriesData(updateTS)
		} else {
			resultTS, err = MergeInternalTimeSeriesData(existingTS, updateTS)
		}
		if err != nil {
			t.Errorf("MergeInternalTimeSeriesData error on case %d: %s", i, err.Error())
			continue
		}
		if a, e := resultTS, expectedTS; !reflect.DeepEqual(a, e) {
			t.Errorf("MergeInternalTimeSeriesData returned wrong result on case %d: expected %v, returned %v",
				i, e, a)
		}
	}
}

// unmarshalTimeSeries unmarshals the time series value stored in the given byte
// array. It is assumed that the time series value was originally marshalled as
// a MVCCMetadata with an inline value.
func unmarshalTimeSeries(t testing.TB, b []byte) *proto.InternalTimeSeriesData {
	if b == nil {
		return nil
	}
	var mvccValue MVCCMetadata
	if err := gogoproto.Unmarshal(b, &mvccValue); err != nil {
		t.Fatalf("error unmarshalling time series in text: %s", err)
	}
	valueTS, err := proto.InternalTimeSeriesDataFromValue(mvccValue.Value)
	if err != nil {
		t.Fatalf("error unmarshalling time series in text: %s", err)
	}
	return valueTS
}
