// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package indexstatslocal_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/indexusagestats"
	"github.com/cockroachdb/cockroach/pkg/sql/indexusagestats/indexstatslocal"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type testIndexUsageCounter [indexusagestats.FullScan + 1]uint64

func TestIndexUsageStatisticsSubsystem(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()

	subsystem := indexstatslocal.New(&indexstatslocal.Config{
		ChannelSize: 10,
		Setting:     cluster.MakeTestingClusterSettings(),
	})

	subsystem.Start(ctx, stopper)
	defer stopper.Stop(ctx)

	var statsWriter indexusagestats.Writer = subsystem

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

	testInputs := []indexusagestats.MetaData{
		{
			Key:       indices[0],
			UsageType: indexusagestats.LookupJoin,
		},
		{
			Key:       indices[0],
			UsageType: indexusagestats.LookupJoin,
		},
		{
			Key:       indices[0],
			UsageType: indexusagestats.FullScan,
		},
		{
			Key:       indices[1],
			UsageType: indexusagestats.NonFullScan,
		},
		{
			Key:       indices[1],
			UsageType: indexusagestats.ZigzagJoin,
		},
		{
			Key:       indices[2],
			UsageType: indexusagestats.FullScan,
		},
		{
			Key:       indices[2],
			UsageType: indexusagestats.ZigzagJoin,
		},
	}

	expectedIndexUsage := make(map[roachpb.IndexUsageKey]testIndexUsageCounter)

	expectedVals := testIndexUsageCounter{}
	expectedVals[indexusagestats.LookupJoin] = 2
	expectedVals[indexusagestats.FullScan] = 1
	expectedIndexUsage[indices[0]] = expectedVals

	expectedVals = testIndexUsageCounter{}
	expectedVals[indexusagestats.ZigzagJoin] = 1
	expectedVals[indexusagestats.NonFullScan] = 1
	expectedIndexUsage[indices[1]] = expectedVals

	expectedVals = testIndexUsageCounter{}
	expectedVals[indexusagestats.ZigzagJoin] = 1
	expectedVals[indexusagestats.FullScan] = 1
	expectedIndexUsage[indices[2]] = expectedVals

	expectedMinLastUsedTimestamp := timeutil.Now()
	for _, input := range testInputs {
		statsWriter.Record(ctx, input)
	}

	// Sleep to ensure that the data are ingested.
	time.Sleep(time.Millisecond * 100)

	t.Run("point lookup", func(t *testing.T) {
		actualEntryCount := 0
		for _, index := range indices {
			stats := subsystem.GetIndexUsageStats(roachpb.IndexUsageKey{
				TableID: index.TableID,
				IndexID: index.IndexID,
			})
			require.NotNil(t, stats)

			actualEntryCount++
			expected := expectedIndexUsage[index]

			require.Equal(t, expected[indexusagestats.LookupJoin], stats.LookupJoinCount)
			require.Equal(t, expected[indexusagestats.InvertedJoin], stats.InvertedJoinCount)
			require.Equal(t, expected[indexusagestats.ZigzagJoin], stats.ZigzagJoinCount)
			require.Equal(t, expected[indexusagestats.NonFullScan], stats.NonFullScanCount)
			require.Equal(t, expected[indexusagestats.FullScan], stats.FullScanCount)

			require.True(t, expectedMinLastUsedTimestamp.Before(stats.LastJoin))
			require.True(t, expectedMinLastUsedTimestamp.Before(stats.LastScan))
		}
		require.Equal(t, len(indices), actualEntryCount)
	})

	t.Run("iterator", func(t *testing.T) {
		actualEntryCount := 0
		err := subsystem.IterateIndexUsageStats(indexusagestats.IteratorOptions{}, func(key *roachpb.IndexUsageKey, value *roachpb.IndexUsageStatistics) error {
			actualEntryCount++

			expected := expectedIndexUsage[*key]

			require.Equal(t, expected[indexusagestats.LookupJoin], value.LookupJoinCount)
			require.Equal(t, expected[indexusagestats.InvertedJoin], value.InvertedJoinCount)
			require.Equal(t, expected[indexusagestats.ZigzagJoin], value.ZigzagJoinCount)
			require.Equal(t, expected[indexusagestats.NonFullScan], value.NonFullScanCount)
			require.Equal(t, expected[indexusagestats.FullScan], value.FullScanCount)

			require.True(t, expectedMinLastUsedTimestamp.Before(value.LastJoin))
			require.True(t, expectedMinLastUsedTimestamp.Before(value.LastScan))

			return nil
		})
		require.Equal(t, len(indices), actualEntryCount)
		require.NoError(t, err)
	})

	t.Run("iterator with options", func(t *testing.T) {
		actualEntryCount := uint64(0)
		maxEntry := uint64(1)
		err := subsystem.IterateIndexUsageStats(indexusagestats.IteratorOptions{
			SortedTableID: true,
			SortedIndexID: true,
			Max:           &maxEntry,
		}, func(key *roachpb.IndexUsageKey, value *roachpb.IndexUsageStatistics) error {
			actualEntryCount++

			require.Equal(t, indices[0], *key)
			expected := expectedIndexUsage[*key]

			require.Equal(t, expected[indexusagestats.LookupJoin], value.LookupJoinCount)
			require.Equal(t, expected[indexusagestats.InvertedJoin], value.InvertedJoinCount)
			require.Equal(t, expected[indexusagestats.ZigzagJoin], value.ZigzagJoinCount)
			require.Equal(t, expected[indexusagestats.NonFullScan], value.NonFullScanCount)
			require.Equal(t, expected[indexusagestats.FullScan], value.FullScanCount)

			require.True(t, expectedMinLastUsedTimestamp.Before(value.LastJoin))
			require.True(t, expectedMinLastUsedTimestamp.Before(value.LastScan))

			return nil
		})
		require.Equal(t, maxEntry, actualEntryCount)
		require.NoError(t, err)
	})

	t.Run("reset", func(t *testing.T) {
		minResetTimestamp := timeutil.Now()
		subsystem.Clear()
		actualEntryCount := 0
		err := subsystem.IterateIndexUsageStats(indexusagestats.IteratorOptions{}, func(key *roachpb.IndexUsageKey, value *roachpb.IndexUsageStatistics) error {
			actualEntryCount++
			return nil
		})
		require.Equal(t, 0 /* expected */, actualEntryCount)
		require.NoError(t, err)
		require.True(t, minResetTimestamp.Before(subsystem.GetLastReset()), "expected subsystem to reset at least after %v, but found %v", minResetTimestamp, subsystem.GetLastReset())
	})
}
