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
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Peter Mattis (peter@cockroachlabs.com)

package engine

import (
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/protoutil"
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

func mustMarshal(m proto.Message) []byte {
	b, err := protoutil.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

func appender(s string) []byte {
	val := roachpb.MakeValueFromString(s)
	v := &MVCCMetadata{RawBytes: val.RawBytes}
	return mustMarshal(v)
}

// timeSeries generates a simple InternalTimeSeriesData object which starts
// at the given timestamp and has samples of the given duration. The object is
// stored in an MVCCMetadata object and marshalled to bytes.
func timeSeries(start int64, duration int64, samples ...tsSample) []byte {
	tsv := timeSeriesAsValue(start, duration, samples...)
	return mustMarshal(&MVCCMetadata{RawBytes: tsv.RawBytes})
}

func timeSeriesAsValue(start int64, duration int64, samples ...tsSample) roachpb.Value {
	ts := &roachpb.InternalTimeSeriesData{
		StartTimestampNanos: start,
		SampleDurationNanos: duration,
	}
	for _, sample := range samples {
		newSample := roachpb.InternalTimeSeriesSample{
			Offset: sample.offset,
			Count:  sample.count,
			Sum:    sample.sum,
		}
		if sample.count > 1 {
			newSample.Max = proto.Float64(sample.max)
			newSample.Min = proto.Float64(sample.min)
		}
		ts.Samples = append(ts.Samples, newSample)
	}
	var v roachpb.Value
	if err := v.SetProto(ts); err != nil {
		panic(err)
	}
	return v
}

// TestGoMerge tests the function goMerge but not the integration with
// the storage engines. For that, see the engine tests.
func TestGoMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		{nil, nil, mustMarshal(&MVCCMetadata{RawBytes: []byte{}})},
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
		if err := proto.Unmarshal(result, &resultV); err != nil {
			t.Fatal(err)
		}
		if err := proto.Unmarshal(c.expected, &expectedV); err != nil {
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
				{2, 1, 5, 5, 5},
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
				{1, 1, 100, 100, 100},
				{2, 1, 5, 5, 5},
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
		if c.existing == nil {
			resultTS, err = MergeInternalTimeSeriesData(updateTS)
		} else {
			existingTS := unmarshalTimeSeries(t, c.existing)
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
func unmarshalTimeSeries(t testing.TB, b []byte) roachpb.InternalTimeSeriesData {
	var meta MVCCMetadata
	if err := proto.Unmarshal(b, &meta); err != nil {
		t.Fatalf("error unmarshalling time series in text: %s", err.Error())
	}
	valueTS, err := meta.Value().GetTimeseries()
	if err != nil {
		t.Fatalf("error unmarshalling time series in text: %s", err.Error())
	}
	return valueTS
}
