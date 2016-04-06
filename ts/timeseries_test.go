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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func ts(name string, dps ...TimeSeriesDatapoint) TimeSeriesData {
	return TimeSeriesData{
		Name:       name,
		Datapoints: dps,
	}
}

func tsdp(ts time.Duration, val float64) TimeSeriesDatapoint {
	return TimeSeriesDatapoint{
		TimestampNanos: ts.Nanoseconds(),
		Value:          val,
	}
}

// TestToInternal verifies the conversion of TimeSeriesData to internal storage
// format is correct.
func TestToInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcases := []struct {
		keyDuration    int64
		sampleDuration int64
		expectedError  string
		input          TimeSeriesData
		expected       []roachpb.InternalTimeSeriesData
	}{
		{
			time.Minute.Nanoseconds(),
			101,
			"does not evenly divide key duration",
			ts("error.series"),
			nil,
		},
		{
			time.Minute.Nanoseconds(),
			time.Hour.Nanoseconds(),
			"does not evenly divide key duration",
			ts("error.series"),
			nil,
		},
		{
			(24 * time.Hour).Nanoseconds(),
			(20 * time.Minute).Nanoseconds(),
			"",
			ts("test.series",
				tsdp(5*time.Hour+5*time.Minute, 1.0),
				tsdp(24*time.Hour+39*time.Minute, 2.0),
				tsdp(10*time.Hour+10*time.Minute, 3.0),
				tsdp(48*time.Hour, 4.0),
				tsdp(15*time.Hour+22*time.Minute+1, 5.0),
				tsdp(52*time.Hour+15*time.Minute, 0.0),
			),
			[]roachpb.InternalTimeSeriesData{
				{
					StartTimestampNanos: 0,
					SampleDurationNanos: 20 * time.Minute.Nanoseconds(),
					Samples: []roachpb.InternalTimeSeriesSample{
						{
							Offset: 15,
							Count:  1,
							Sum:    1.0,
						},
						{
							Offset: 30,
							Count:  1,
							Sum:    3.0,
						},
						{
							Offset: 46,
							Count:  1,
							Sum:    5.0,
						},
					},
				},
				{
					StartTimestampNanos: 24 * time.Hour.Nanoseconds(),
					SampleDurationNanos: 20 * time.Minute.Nanoseconds(),
					Samples: []roachpb.InternalTimeSeriesSample{
						{
							Offset: 1,
							Count:  1,
							Sum:    2.0,
						},
					},
				},
				{
					StartTimestampNanos: 48 * time.Hour.Nanoseconds(),
					SampleDurationNanos: 20 * time.Minute.Nanoseconds(),
					Samples: []roachpb.InternalTimeSeriesSample{
						{
							Offset: 0,
							Count:  1,
							Sum:    4.0,
						},
						{
							Offset: 12,
							Count:  1,
							Sum:    0.0,
						},
					},
				},
			},
		},
	}

	for i, tc := range tcases {
		actual, err := tc.input.ToInternal(tc.keyDuration, tc.sampleDuration)
		if e := tc.expectedError; len(e) > 0 {
			if !testutils.IsError(err, e) {
				t.Errorf("unexpected error from case %d: %v", i, err)
			}
		} else {
			if err != nil {
				t.Errorf("unexpected error from case %d: %v", i, err)
			}
		}

		if !reflect.DeepEqual(actual, tc.expected) {
			t.Errorf("case %d fails: ToInternal result was %v, expected %v", i, actual, tc.expected)
		}
	}
}

// TestDiscardEarlierSamples verifies that only a single sample is kept for each
// sample period; earlier samples are discarded.
func TestDiscardEarlierSamples(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := ts("test.series",
		tsdp(5*time.Hour+5*time.Minute, -1.0),
		tsdp(5*time.Hour+5*time.Minute, -2.0),
	)
	internal, err := ts.ToInternal(Resolution10s.KeyDuration(), Resolution10s.SampleDuration())
	if err != nil {
		t.Fatal(err)
	}

	out, err := engine.MergeInternalTimeSeriesData(internal...)
	if err != nil {
		t.Fatal(err)
	}

	if maxVal := out.Samples[0].Max; maxVal != nil {
		t.Fatal("Expected maximum of sample 0 to be nil; samples are no longer merged.")
	}
	if a, e := out.Samples[0].Sum, -2.0; a != e {
		t.Fatalf("Expected sum of sample 0 to be %f after initial merge, was %f", e, a)
	}
}
