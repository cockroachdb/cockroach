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
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func ts(name string, dps ...*TimeSeriesDatapoint) *TimeSeriesData {
	return &TimeSeriesData{
		Name:       name,
		Datapoints: dps,
	}
}

func tsdp(ts time.Duration, val float64) *TimeSeriesDatapoint {
	return &TimeSeriesDatapoint{
		TimestampNanos: int64(ts),
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
		expectsError   bool
		input          *TimeSeriesData
		expected       []*roachpb.InternalTimeSeriesData
	}{
		{
			time.Minute.Nanoseconds(),
			101,
			true,
			ts("error.series"),
			nil,
		},
		{
			time.Minute.Nanoseconds(),
			time.Hour.Nanoseconds(),
			true,
			ts("error.series"),
			nil,
		},
		{
			(time.Hour * 24).Nanoseconds(),
			(time.Minute * 20).Nanoseconds(),
			false,
			ts("test.series",
				tsdp((time.Hour*5)+(time.Minute*5), 1.0),
				tsdp((time.Hour*24)+(time.Minute*39), 2.0),
				tsdp((time.Hour*10)+(time.Minute*10), 3.0),
				tsdp((time.Hour*48), 4.0),
				tsdp((time.Hour*15)+(time.Minute*22)+1, 5.0),
				tsdp((time.Hour*52)+(time.Minute*15), 0.0),
			),
			[]*roachpb.InternalTimeSeriesData{
				{
					StartTimestampNanos: 0,
					SampleDurationNanos: int64(time.Minute * 20),
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
					StartTimestampNanos: int64(time.Hour * 24),
					SampleDurationNanos: int64(time.Minute * 20),
					Samples: []roachpb.InternalTimeSeriesSample{
						{
							Offset: 1,
							Count:  1,
							Sum:    2.0,
						},
					},
				},
				{
					StartTimestampNanos: int64(time.Hour * 48),
					SampleDurationNanos: int64(time.Minute * 20),
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
		if err != nil {
			if !tc.expectsError {
				t.Errorf("unexpected error from case %d: %s", i, err.Error())
			}
			continue
		} else if tc.expectsError {
			t.Errorf("expected error from case %d, none encountered", i)
			continue
		}

		if !reflect.DeepEqual(actual, tc.expected) {
			t.Errorf("case %d fails: ToInternal result was %v, expected %v", i, actual, tc.expected)
		}
	}
}

// TestNegativeMax is a regression test for github issue #5414; the max value
// could be improperly set on disk if the real maximum was a negative number.
func TestNegativeMax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := ts("test.series",
		tsdp((time.Hour*5)+(time.Minute*5), -1.0),
		tsdp((time.Hour*5)+(time.Minute*5), -2.0),
	)
	internal, err := ts.ToInternal(Resolution10s.KeyDuration(), Resolution10s.SampleDuration())
	if err != nil {
		t.Fatal(err)
	}

	out, err := engine.MergeInternalTimeSeriesData(internal...)
	if err != nil {
		t.Fatal(err)
	}

	if maxVal := out.Samples[0].Max; maxVal == nil {
		t.Fatal("Expected maximum of sample 0 to be non-nil after initial merge.")
	} else if *maxVal != -1.0 {
		t.Fatalf("Expected maximum of sample 0 to be -1.0 after initial merge, was %f", *maxVal)
	}

	// Prevent regression of #5414: The negative number was only improperly set
	// if a sample with an explicitly set negative max value is merged (it did
	// not occur when individual samples were merged, because a sample with
	// count=1 does not have an explicitly set max). Therefore, it could only
	// happen if a sample is merged twice.
	out, err = engine.MergeInternalTimeSeriesData(out)
	if err != nil {
		t.Fatal(err)
	}

	if maxVal := out.Samples[0].Max; maxVal == nil {
		t.Fatal("Expected maximum of sample 0 to be non-nil after initial merge.")
	} else if *maxVal != -1.0 {
		t.Fatalf("Expected maximum of sample 0 to be -1.0 after initial merge, was %f", *maxVal)
	}
}
