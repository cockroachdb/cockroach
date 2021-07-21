// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxusage

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func checkTimeHelper(t *testing.T, expected, actual time.Time, delta time.Duration) {
	diff := actual.Sub(expected)
	require.Less(t, diff, delta)
}

func checkStatsHelper(t *testing.T, expected, actual roachpb.IndexUsageStatistics) {
	require.Equal(t, expected.TotalReadCount, actual.TotalReadCount)
	require.Equal(t, expected.TotalWriteCount, actual.TotalWriteCount)

	require.Equal(t, expected.TotalRowsRead, actual.TotalRowsRead)
	require.Equal(t, expected.TotalRowsWritten, actual.TotalRowsWritten)

	checkTimeHelper(t, expected.LastRead, actual.LastRead, time.Second)
	checkTimeHelper(t, expected.LastWrite, actual.LastWrite, time.Second)
}

func TestIndexUsageStatisticsSubsystem(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()

	indices := []roachpb.IndexUsageKey{
		{
			TableID: 1,
			IndexID: 1,
		},
		{
			TableID: 2,
			IndexID: 1,
		},
		{
			TableID: 2,
			IndexID: 2,
		},
	}

	testInputs := []indexUse{
		{
			key:      indices[0],
			usageTyp: readOp,
		},
		{
			key:      indices[0],
			usageTyp: readOp,
		},
		{
			key:      indices[0],
			usageTyp: writeOp,
		},
		{
			key:      indices[1],
			usageTyp: readOp,
		},
		{
			key:      indices[1],
			usageTyp: readOp,
		},
		{
			key:      indices[2],
			usageTyp: writeOp,
		},
		{
			key:      indices[2],
			usageTyp: writeOp,
		},
	}

	expectedIndexUsage := map[roachpb.IndexUsageKey]roachpb.IndexUsageStatistics{
		indices[0]: {
			TotalReadCount:  2,
			LastRead:        timeutil.Now(),
			TotalWriteCount: 1,
			LastWrite:       timeutil.Now(),
		},
		indices[1]: {
			TotalReadCount: 2,
			LastRead:       timeutil.Now(),
		},
		indices[2]: {
			TotalWriteCount: 2,
			LastWrite:       timeutil.Now(),
		},
	}

	statsProcessedSignal := make(chan struct{})
	onStatsIngested := func(_ roachpb.IndexUsageKey) {
		statsProcessedSignal <- struct{}{}
	}
	waitForStatsIngested := func() {
		statsProcessed := 0
		var timer timeutil.Timer
		timer.Reset(time.Second)
		for statsProcessed < len(testInputs) {
			select {
			case <-statsProcessedSignal:
				statsProcessed++
			case <-timer.C:
				timer.Read = true
				t.Fatalf("expected stats ingestion to complete, but it didn't.")
			}
		}
	}

	localIndexUsage := NewLocalIndexUsageStats(&Config{
		ChannelSize: 10,
		Setting:     cluster.MakeTestingClusterSettings(),
		Knobs: &TestingKnobs{
			OnIndexUsageStatsProcessedCallback: onStatsIngested,
		},
	})

	localIndexUsage.Start(ctx, stopper)
	defer stopper.Stop(ctx)

	for _, input := range testInputs {
		localIndexUsage.record(ctx, input)
	}

	waitForStatsIngested()

	t.Run("point lookup", func(t *testing.T) {
		actualEntryCount := 0
		for _, index := range indices {
			stats := localIndexUsage.Get(index)
			require.NotNil(t, stats)

			actualEntryCount++

			checkStatsHelper(t, expectedIndexUsage[index], stats)
		}
	})

	t.Run("iterator", func(t *testing.T) {
		actualEntryCount := 0
		err := localIndexUsage.ForEach(IteratorOptions{}, func(key *roachpb.IndexUsageKey, value *roachpb.IndexUsageStatistics) error {
			actualEntryCount++

			checkStatsHelper(t, expectedIndexUsage[*key], *value)
			return nil
		})
		require.Equal(t, len(expectedIndexUsage), actualEntryCount)
		require.NoError(t, err)
	})

	t.Run("iterator with options", func(t *testing.T) {
		actualEntryCount := uint64(0)
		maxEntry := uint64(2)
		err := localIndexUsage.ForEach(IteratorOptions{
			SortedTableID: true,
			SortedIndexID: true,
			Max:           &maxEntry,
		}, func(key *roachpb.IndexUsageKey, value *roachpb.IndexUsageStatistics) error {
			actualEntryCount++

			checkStatsHelper(t, expectedIndexUsage[*key], *value)

			return nil
		})
		require.Equal(t, maxEntry, actualEntryCount)
		require.NoError(t, err)
	})

	t.Run("clear", func(t *testing.T) {
		actualEntryCount := 0
		expectedEntryCount := 0
		localIndexUsage.clear()
		err := localIndexUsage.ForEach(IteratorOptions{}, func(_ *roachpb.IndexUsageKey, _ *roachpb.IndexUsageStatistics) error {
			actualEntryCount++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, expectedEntryCount, actualEntryCount)
	})
}
