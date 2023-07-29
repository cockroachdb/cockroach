// Copyright 2015 The Cockroach Authors.
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
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func tsd(name, source string, dps ...tspb.TimeSeriesDatapoint) tspb.TimeSeriesData {
	return tspb.TimeSeriesData{
		Name:       name,
		Source:     source,
		Datapoints: dps,
	}
}

func tsdp(ts time.Duration, val float64) tspb.TimeSeriesDatapoint {
	return tspb.TimeSeriesDatapoint{
		TimestampNanos: ts.Nanoseconds(),
		Value:          val,
	}
}

// TestToInternal verifies the conversion of tspb.TimeSeriesData to internal storage
// format is correct.
func TestToInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tcases := []struct {
		keyDuration    int64
		sampleDuration int64
		expectedError  string
		input          tspb.TimeSeriesData
		expected       []roachpb.InternalTimeSeriesData
	}{
		{
			time.Minute.Nanoseconds(),
			101,
			"does not evenly divide key duration",
			tsd("error.series", ""),
			nil,
		},
		{
			time.Minute.Nanoseconds(),
			time.Hour.Nanoseconds(),
			"does not evenly divide key duration",
			tsd("error.series", ""),
			nil,
		},
		{
			(24 * time.Hour).Nanoseconds(),
			(20 * time.Minute).Nanoseconds(),
			"",
			tsd("test.series", "",
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
					Offset:              []int32{15, 30, 46},
					Last:                []float64{1.0, 3.0, 5.0},
				},
				{
					StartTimestampNanos: 24 * time.Hour.Nanoseconds(),
					SampleDurationNanos: 20 * time.Minute.Nanoseconds(),
					Offset:              []int32{1},
					Last:                []float64{2.0},
				},
				{
					StartTimestampNanos: 48 * time.Hour.Nanoseconds(),
					SampleDurationNanos: 20 * time.Minute.Nanoseconds(),
					Offset:              []int32{0, 12},
					Last:                []float64{4.0, 0.0},
				},
			},
		},
	}

	for i, tc := range tcases {
		actual, err := tc.input.ToInternal(tc.keyDuration, tc.sampleDuration)
		if !testutils.IsError(err, tc.expectedError) {
			t.Errorf("expected error %q from case %d, got %v", tc.expectedError, i, err)
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
	defer log.Scope(t).Close(t)

	// TODO(ajkr): this should also run on Pebble. Maybe combine it into the merge_test.go tests or prove
	// it is redundant with the tests there.
	ts := tsd("test.series", "",
		tsdp(5*time.Hour+5*time.Minute, -1.0),
		tsdp(5*time.Hour+5*time.Minute, -2.0),
	)
	internal, err := ts.ToInternal(Resolution10s.SlabDuration(), Resolution10s.SampleDuration())
	if err != nil {
		t.Fatal(err)
	}

	out, err := storage.MergeInternalTimeSeriesData(internal...)
	if err != nil {
		t.Fatal(err)
	}

	if count := out.SampleCount(); count != 1 {
		t.Fatalf("expected only sample to be kept, found %d", count)
	}
}
