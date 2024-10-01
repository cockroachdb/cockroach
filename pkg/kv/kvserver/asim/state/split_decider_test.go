// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/stretchr/testify/require"
)

var testingSequence = []Key{10, 1, 9, 2, 8, 3, 4, 7, 5, 6}

func TestSplitDecider(t *testing.T) {
	testSettings := config.DefaultSimulationSettings()
	testSettings.SplitQPSThreshold = 2500
	testSettings.SplitStatRetention = 60 * time.Second
	startTime := TestingStartTime()
	decider := NewSplitDecider(testSettings)

	// A decider should be created for a range when a load event is first
	// recorded against it.
	require.Nil(t, decider.deciders[1])
	decider.Record(startTime, 1, workload.LoadEvent{Key: 1, Reads: 1})
	require.NotNil(t, decider.deciders[1])

	// No valid split key should be found when there has been below threshold
	// load.
	splitKey, found := decider.SplitKey(startTime, 1)
	require.False(t, found)
	require.Equal(t, InvalidKey, splitKey)

	// No ranges should have been accumulated as suggestions for splitting.
	suggestions := decider.ClearSplitKeys()
	require.Empty(t, suggestions)
	sequence := testingSequence

	// Register load greater than the threshold.
	for i := 0; int64(i) < int64(testSettings.SplitStatRetention/time.Second); i++ {
		for j := 0; j < int(testSettings.SplitQPSThreshold)+100; j++ {
			decider.Record(
				OffsetTick(startTime, int64(i)),
				1,
				workload.LoadEvent{Key: int64(sequence[j%len(sequence)]), Reads: 1},
			)
		}
	}

	// There should now be 1 suggested range for splitting which corresponds to
	// the midpoint of the testing sequence.
	require.Equal(t, []RangeID{1}, decider.ClearSplitKeys())
	splitKey, found = decider.SplitKey(startTime.Add(testSettings.SplitStatRetention), 1)
	require.True(t, found)
	require.Equal(t, Key(6), splitKey)

	// After clearing the split keys, it should now return no new suggestions.
	require.Equal(t, []RangeID{}, decider.ClearSplitKeys())
}

func TestSplitDeciderWorkload(t *testing.T) {
	testingRangeID := FirstRangeID
	startTime := TestingStartTime()

	testCases := []struct {
		desc             string
		ticks            []int64
		sequence         []Key
		qps              int64
		threshold        float64
		retention        time.Duration
		expectedSplitKey Key
		expectedOk       bool
	}{
		{
			desc:             "no split, low qps",
			ticks:            []int64{20, 40, 60, 80},
			sequence:         testingSequence,
			qps:              1000,
			threshold:        2500,
			retention:        120 * time.Second,
			expectedSplitKey: InvalidKey,
			expectedOk:       false,
		},
		{
			desc:             "split, load split evenly left/right of 6",
			ticks:            []int64{20, 40, 60, 80},
			sequence:         testingSequence,
			qps:              3000,
			threshold:        2500,
			retention:        120 * time.Second,
			expectedSplitKey: 6,
			expectedOk:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			testSettings := config.DefaultSimulationSettings()
			testSettings.SplitQPSThreshold = tc.threshold
			testSettings.SplitStatRetention = tc.retention
			splitDecider := NewSplitDecider(testSettings)
			lastTick := int64(0)

			for _, tick := range tc.ticks {
				tickDelta := tick - lastTick
				lastTick = tick

				for loadEventIdx := 0; loadEventIdx < int(tickDelta)*int(tc.qps); loadEventIdx++ {
					loadEvent := workload.LoadEvent{
						Key:   int64(tc.sequence[loadEventIdx%len(tc.sequence)]),
						Reads: 1,
					}
					splitDecider.Record(OffsetTick(startTime, tick), RangeID(testingRangeID), loadEvent)
				}
			}
			splitKey, ok := splitDecider.SplitKey(OffsetTick(startTime, tc.ticks[len(tc.ticks)-1]), RangeID(testingRangeID))
			require.Equal(t, tc.expectedOk, ok)
			require.Equal(t, tc.expectedSplitKey, splitKey)
			if tc.expectedOk {
				require.GreaterOrEqual(t, len(splitDecider.ClearSplitKeys()), 1)
			}
		})
	}
}
