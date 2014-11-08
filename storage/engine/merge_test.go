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
	"bytes"
	"math"
	"math/rand"
	"testing"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
)

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
	v := &proto.Value{
		Integer: gogoproto.Int64(n),
	}
	return mustMarshal(v)
}

func appender(s string) []byte {
	v := &proto.Value{
		Bytes: []byte(s),
	}
	return mustMarshal(v)
}

// timeSeries generates a simple TimeSeriesData object which starts at the given
// timestamp and lasts for the given duration.  The generated TimeSeriesData has
// second-level precision, with one data point at each supplied offset.  Each
// data point has a constant integer value of 5, which was chosen arbitrarily.
func timeSeries(start int64, duration int64, offsets ...int32) []byte {
	ts := &proto.TimeSeriesData{
		StartTimestamp:    start,
		DurationInSeconds: duration,
		SamplePrecision:   proto.SECONDS,
	}
	for _, offset := range offsets {
		ts.Data = append(ts.Data, &proto.TimeSeriesDataPoint{
			Offset:   offset,
			ValueInt: gogoproto.Int64(5),
		})
	}
	v, err := ts.ToValue()
	if err != nil {
		panic(err)
	}
	return mustMarshal(v)
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
		{timeSeries(5000, 3600, 100), nil},
		{timeSeries(5000, 3600, 100), appender("a")},
		{appender("a"), timeSeries(5000, 3600, 100)},
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
		var v proto.Value
		if err := gogoproto.Unmarshal(result, &v); err != nil {
			t.Errorf("goMerge error unmarshalling: %s", err)
			continue
		}
		if *v.Integer != c.expected {
			t.Errorf("goMerge error: %d: want %v, get %v", i, c.expected, *v.Integer)
		}
	}

	gibber1, gibber2 := gibberishString(100), gibberishString(200)

	testCasesAppender := []struct {
		existing, update, expected []byte
	}{
		{appender(""), appender(""), appender("")},
		{nil, appender(""), appender("")},
		{nil, nil, nil},
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
		if !bytes.Equal(result, c.expected) {
			t.Errorf("goMerge error: %d: want %v, get %v", i, c.expected, result)
		}
	}

	testCasesTimeSeries := []struct {
		existing, update, expected []byte
	}{
		{
			nil,
			timeSeries(-446061360, 3600, 30, 250, 460),
			timeSeries(-446061360, 3600, 30, 250, 460),
		},
		{
			nil,
			nil,
			nil,
		},
		{
			timeSeries(-446061360, 3600, 30, 250, 460),
			timeSeries(-446061360, 3600, 1000, 1900, 3000),
			timeSeries(-446061360, 3600, 30, 250, 460, 1000, 1900, 3000),
		},
		{
			timeSeries(-446061360, 3600, 30, 250, 460),
			timeSeries(-446061360, 3600),
			timeSeries(-446061360, 3600, 30, 250, 460),
		},
	}

	for i, c := range testCasesTimeSeries {
		result, err := goMerge(c.existing, c.update)
		if err != nil {
			t.Errorf("goMerge error: %d: %v", i, err)
			continue
		}
		if !bytes.Equal(result, c.expected) {
			// Extract the time series so we can actually read the error.
			var resultV, expectedV proto.Value
			gogoproto.Unmarshal(result, &resultV)
			gogoproto.Unmarshal(c.expected, &expectedV)
			resultTS, _ := proto.TimeSeriesFromValue(&resultV)
			expectedTS, _ := proto.TimeSeriesFromValue(&expectedV)
			t.Errorf("goMerge error: %d: want %v, get %v", i, expectedTS, resultTS)
		}
	}
}
