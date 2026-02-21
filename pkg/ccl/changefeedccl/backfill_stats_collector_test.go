// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestBackfillStatsCollector verifies that we are properly rolling up
// our per aggregator backfill stats and that it emits the correct status message.
func TestBackfillStatsCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		name             string
		initialScan      bool
		expectedFraction float32
		expectedMsg      string
		inputStats       map[int32]*backfillStats
		processorCount   int32
	}

	testCases := []testCase{
		{
			name:             "steady state",
			initialScan:      false,
			expectedFraction: 1,
			expectedMsg:      "",
			processorCount:   3,
			inputStats: map[int32]*backfillStats{
				1: {TotalRanges: 5, PendingRanges: 0},
				2: {TotalRanges: 3, PendingRanges: 0},
				3: {TotalRanges: 2, PendingRanges: 0},
			},
		},
		{
			name:             "initial scan in progress",
			initialScan:      true,
			expectedFraction: 0.4,
			expectedMsg:      "initial scan on 6 out of 10 ranges",
			processorCount:   3,
			inputStats: map[int32]*backfillStats{
				1: {TotalRanges: 5, PendingRanges: 4},
				2: {TotalRanges: 3, PendingRanges: 2},
				3: {TotalRanges: 2, PendingRanges: 0},
			},
		},
		{
			name:             "initial scan complete",
			initialScan:      true,
			expectedFraction: 1,
			expectedMsg:      "initial scan on 0 out of 10 ranges",
			processorCount:   3,
			inputStats: map[int32]*backfillStats{
				1: {TotalRanges: 5, PendingRanges: 0},
				2: {TotalRanges: 3, PendingRanges: 0},
				3: {TotalRanges: 2, PendingRanges: 0},
			},
		},
		{
			name:             "not all aggregators have reported",
			initialScan:      true,
			expectedFraction: 0,
			expectedMsg:      "starting streams (2 out of 3)",
			processorCount:   3,
			inputStats: map[int32]*backfillStats{
				1: {TotalRanges: 5, PendingRanges: 5},
				2: {TotalRanges: 5, PendingRanges: 5},
			},
		},
		{
			name:        "schemachange backfill",
			initialScan: false,
			// We don't want to emit a fraction during schema change backfills, since we
			// prefer emitting the highwater.
			expectedFraction: 1,
			expectedMsg:      "schemachange backfill on 4 out of 10 ranges",
			processorCount:   3,
			inputStats: map[int32]*backfillStats{
				1: {TotalRanges: 5, PendingRanges: 2},
				2: {TotalRanges: 3, PendingRanges: 2},
				3: {TotalRanges: 2, PendingRanges: 0},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			statsCollector := newBackfillStatsCollector(tc.processorCount)
			for id, stats := range tc.inputStats {
				statsCollector.add(id, stats.TotalRanges, stats.PendingRanges)
			}

			fraction, msg := statsCollector.rollupStats(tc.initialScan)
			require.Equal(t, tc.expectedFraction, fraction)
			require.Equal(t, tc.expectedMsg, msg)
		})
	}
}
