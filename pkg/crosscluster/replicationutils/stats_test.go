// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationutils

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/rangescanstats/rangescanstatspb"
	"github.com/stretchr/testify/require"
)

func TestRangeStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		name        string
		stats       rangescanstatspb.RangeStats
		fraction    float32
		expectedMsg string

		inputStats map[int32]*rangescanstatspb.RangeStats
	}

	testCases := []testCase{
		{
			name: "steady state",
			stats: rangescanstatspb.RangeStats{
				RangeCount: 10,
			},
			fraction:    1,
			expectedMsg: "",
			inputStats: map[int32]*rangescanstatspb.RangeStats{
				1: {RangeCount: 5},
				2: {RangeCount: 3},
				3: {RangeCount: 2},
			},
		},
		{
			name: "initial scan",
			stats: rangescanstatspb.RangeStats{
				RangeCount:         10,
				ScanningRangeCount: 6,
				LaggingRangeCount:  2,
			},
			fraction:    0.4,
			expectedMsg: "initial scan on 6 out of 10 ranges",
			inputStats: map[int32]*rangescanstatspb.RangeStats{
				1: {RangeCount: 5, ScanningRangeCount: 4},
				2: {RangeCount: 3, ScanningRangeCount: 2},
				3: {RangeCount: 2, ScanningRangeCount: 0, LaggingRangeCount: 2},
			},
		},
		{
			name: "lagging",
			stats: rangescanstatspb.RangeStats{
				RangeCount:        10,
				LaggingRangeCount: 2,
			},
			fraction:    0.8,
			expectedMsg: "catching up on 2 out of 10 ranges",
			inputStats: map[int32]*rangescanstatspb.RangeStats{
				1: {RangeCount: 5},
				2: {RangeCount: 3},
				3: {RangeCount: 2, LaggingRangeCount: 2},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewAggregateRangeStatsCollector(3)
			for id, stats := range tc.inputStats {
				r.Add(id, stats)
			}

			total, fraction, msg := r.RollupStats()
			require.Equal(t, tc.stats, total)
			require.Equal(t, tc.fraction, fraction)
			require.Equal(t, tc.expectedMsg, msg)

			rInitializing := NewAggregateRangeStatsCollector(4)
			for id, stats := range tc.inputStats {
				r.Add(id, stats)
			}
			total, fraction, msg = rInitializing.RollupStats()
			require.Equal(t, total, rangescanstatspb.RangeStats{})
			require.Equal(t, float32(0), fraction)
			require.Contains(t, msg, "starting streams")
		})
	}
}
