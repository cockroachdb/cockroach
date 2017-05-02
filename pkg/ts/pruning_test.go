// Copyright 2016 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestContainsTimeSeries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tsdb := (*DB)(nil)

	for i, tcase := range []struct {
		start    roachpb.RKey
		end      roachpb.RKey
		expected bool
	}{
		{
			roachpb.RKey("a"),
			roachpb.RKey("b"),
			false,
		},
		{
			roachpb.RKeyMin,
			roachpb.RKey(keys.SystemPrefix),
			false,
		},
		{
			roachpb.RKeyMin,
			roachpb.RKeyMax,
			true,
		},
		{
			roachpb.RKeyMin,
			roachpb.RKey(MakeDataKey("metric", "", Resolution10s, 0)),
			true,
		},
		{
			roachpb.RKey(MakeDataKey("metric", "", Resolution10s, 0)),
			roachpb.RKeyMax,
			true,
		},
		{
			roachpb.RKey(MakeDataKey("metric", "", Resolution10s, 0)),
			roachpb.RKey(MakeDataKey("metric.b", "", Resolution10s, 0)),
			true,
		},
	} {
		if actual := tsdb.ContainsTimeSeries(tcase.start, tcase.end); actual != tcase.expected {
			t.Errorf("case %d: was %t, expected %t", i, actual, tcase.expected)
		}
	}
}

func TestFindTimeSeries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	// Populate data: two metrics, two sources, two resolutions, two keys.
	metrics := []string{"metric.a", "metric.z"}
	sources := []string{"source1", "source2"}
	resolutions := []Resolution{Resolution10s, resolution1ns}
	for _, metric := range metrics {
		for _, source := range sources {
			for _, resolution := range resolutions {
				tm.storeTimeSeriesData(resolution, []tspb.TimeSeriesData{
					{
						Name:   metric,
						Source: source,
						Datapoints: []tspb.TimeSeriesDatapoint{
							{
								TimestampNanos: 400 * 1e9,
								Value:          1,
							},
							{
								TimestampNanos: 500 * 1e9,
								Value:          2,
							},
						},
					},
				})
			}
		}
	}

	e := tm.LocalTestCluster.Eng
	for i, tcase := range []struct {
		start     roachpb.RKey
		end       roachpb.RKey
		timestamp hlc.Timestamp
		expected  []timeSeriesResolutionInfo
	}{
		// Entire key range.
		{
			start:     roachpb.RKeyMin,
			end:       roachpb.RKeyMax,
			timestamp: hlc.MaxTimestamp,
			expected: []timeSeriesResolutionInfo{
				{
					Name:       metrics[0],
					Resolution: Resolution10s,
				},
				{
					Name:       metrics[0],
					Resolution: resolution1ns,
				},
				{
					Name:       metrics[1],
					Resolution: Resolution10s,
				},
				{
					Name:       metrics[1],
					Resolution: resolution1ns,
				},
			},
		},
		// Timestamp at 400s means we prune nothing.
		{
			start:     roachpb.RKeyMin,
			end:       roachpb.RKeyMax,
			timestamp: hlc.Timestamp{WallTime: 400 * 1e9},
			expected:  nil,
		},
		// Timestamp at 401s is just at the limit for 1ns time series pruning.
		{
			start:     roachpb.RKeyMin,
			end:       roachpb.RKeyMax,
			timestamp: hlc.Timestamp{WallTime: 401 * 1e9},
			expected:  nil,
		},
		// Timestamp at 401s + 1ns prunes the 400s records at 1ns resolution.
		{
			start:     roachpb.RKeyMin,
			end:       roachpb.RKeyMax,
			timestamp: hlc.Timestamp{WallTime: 401*1e9 + 1},
			expected: []timeSeriesResolutionInfo{
				{
					Name:       metrics[0],
					Resolution: resolution1ns,
				},
				{
					Name:       metrics[1],
					Resolution: resolution1ns,
				},
			},
		},
		// Timestamp at the Resolution10s threshold doesn't prune the 10s resolutions.
		{
			start:     roachpb.RKeyMin,
			end:       roachpb.RKeyMax,
			timestamp: hlc.Timestamp{WallTime: pruneThresholdByResolution[Resolution10s]},
			expected: []timeSeriesResolutionInfo{
				{
					Name:       metrics[0],
					Resolution: resolution1ns,
				},
				{
					Name:       metrics[1],
					Resolution: resolution1ns,
				},
			},
		},
		// Timestamp at the Resolution10s threshold + 1ns prunes all time series.
		{
			start:     roachpb.RKeyMin,
			end:       roachpb.RKeyMax,
			timestamp: hlc.Timestamp{WallTime: pruneThresholdByResolution[Resolution10s] + 1},
			expected: []timeSeriesResolutionInfo{
				{
					Name:       metrics[0],
					Resolution: Resolution10s,
				},
				{
					Name:       metrics[0],
					Resolution: resolution1ns,
				},
				{
					Name:       metrics[1],
					Resolution: Resolution10s,
				},
				{
					Name:       metrics[1],
					Resolution: resolution1ns,
				},
			},
		},
		// Key range entirely outside of time series range.
		{
			start:     roachpb.RKey("a"),
			end:       roachpb.RKey("b"),
			timestamp: hlc.MaxTimestamp,
			expected:  nil,
		},
		// Key range split between metrics.
		{
			start:     roachpb.RKeyMin,
			end:       roachpb.RKey(MakeDataKey("metric.b", "", Resolution10s, 0)),
			timestamp: hlc.MaxTimestamp,
			expected: []timeSeriesResolutionInfo{
				{
					Name:       metrics[0],
					Resolution: Resolution10s,
				},
				{
					Name:       metrics[0],
					Resolution: resolution1ns,
				},
			},
		},
		{
			start:     roachpb.RKey(MakeDataKey("metric.b", "", Resolution10s, 0)),
			end:       roachpb.RKeyMax,
			timestamp: hlc.MaxTimestamp,
			expected: []timeSeriesResolutionInfo{
				{
					Name:       metrics[1],
					Resolution: Resolution10s,
				},
				{
					Name:       metrics[1],
					Resolution: resolution1ns,
				},
			},
		},
		// Key range split within a metric along resolution boundary.
		{
			start:     roachpb.RKeyMin,
			end:       roachpb.RKey(MakeDataKey(metrics[0], "", resolution1ns, 0)),
			timestamp: hlc.MaxTimestamp,
			expected: []timeSeriesResolutionInfo{
				{
					Name:       metrics[0],
					Resolution: Resolution10s,
				},
			},
		},
		{
			start:     roachpb.RKey(MakeDataKey(metrics[0], "", resolution1ns, 0)),
			end:       roachpb.RKeyMax,
			timestamp: hlc.MaxTimestamp,
			expected: []timeSeriesResolutionInfo{
				{
					Name:       metrics[0],
					Resolution: resolution1ns,
				},
				{
					Name:       metrics[1],
					Resolution: Resolution10s,
				},
				{
					Name:       metrics[1],
					Resolution: resolution1ns,
				},
			},
		},
	} {
		snap := e.NewSnapshot()
		actual, err := findTimeSeries(snap, tcase.start, tcase.end, tcase.timestamp)
		snap.Close()
		if err != nil {
			t.Fatalf("case %d: unexpected error %q", i, err)
		}

		if !reflect.DeepEqual(actual, tcase.expected) {
			t.Fatalf("case %d: got %v, expected %v", i, actual, tcase.expected)
		}
	}
}

func TestPruneTimeSeries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	// Arbitrary timestamp
	var now int64 = 1475700000 * 1e9

	// Populate data: two metrics, two sources, two resolutions, two keys.
	metrics := []string{"metric.a", "metric.z"}
	sources := []string{"source1", "source2"}
	resolutions := []Resolution{Resolution10s, resolution1ns}
	for _, metric := range metrics {
		for _, source := range sources {
			for _, resolution := range resolutions {
				tm.storeTimeSeriesData(resolution, []tspb.TimeSeriesData{
					{
						Name:   metric,
						Source: source,
						Datapoints: []tspb.TimeSeriesDatapoint{
							{
								TimestampNanos: now,
								Value:          1,
							},
							{
								TimestampNanos: now - int64(365*24*time.Hour),
								Value:          2,
							},
						},
					},
				})
			}
		}
	}

	tm.assertModelCorrect()
	tm.assertKeyCount(16)

	tm.prune(
		now,
		timeSeriesResolutionInfo{
			Name:       "metric.notexists",
			Resolution: resolutions[0],
		},
	)
	tm.assertModelCorrect()
	tm.assertKeyCount(16)

	tm.prune(
		now,
		timeSeriesResolutionInfo{
			Name:       metrics[0],
			Resolution: resolutions[0],
		},
	)
	tm.assertModelCorrect()
	tm.assertKeyCount(14)

	tm.prune(
		now,
		timeSeriesResolutionInfo{
			Name:       metrics[0],
			Resolution: resolutions[1],
		},
		timeSeriesResolutionInfo{
			Name:       metrics[1],
			Resolution: resolutions[0],
		},
		timeSeriesResolutionInfo{
			Name:       metrics[1],
			Resolution: resolutions[1],
		},
	)
	tm.assertModelCorrect()
	tm.assertKeyCount(8)

	tm.prune(
		now+int64(365*24*time.Hour),
		timeSeriesResolutionInfo{
			Name:       metrics[0],
			Resolution: resolutions[0],
		},
		timeSeriesResolutionInfo{
			Name:       metrics[0],
			Resolution: resolutions[1],
		},
		timeSeriesResolutionInfo{
			Name:       metrics[1],
			Resolution: resolutions[0],
		},
		timeSeriesResolutionInfo{
			Name:       metrics[1],
			Resolution: resolutions[1],
		},
	)
	tm.assertModelCorrect()
	tm.assertKeyCount(0)
}
