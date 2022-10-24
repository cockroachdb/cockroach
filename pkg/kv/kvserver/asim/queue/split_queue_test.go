// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package queue

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/stretchr/testify/require"
)

func TestSplitQueue(t *testing.T) {
	start := state.TestingStartTime()
	ctx := context.Background()
	testSettings := config.DefaultSimulationSettings()

	// NB: This test assume 5 second split queue delays for simplification.
	testSettings.SplitQueueDelay = 5 * time.Second

	// Limit the interesting range to just be [0,100).
	endKey := state.Key(100)
	// The store that will have a lease for the interesting range.
	testingStore := state.StoreID(1)

	testingState := func(
		replicaCounts map[state.StoreID]int,
		replicationFactor int,
		leaseholder state.StoreID,
	) state.State {
		s := state.NewTestStateReplCounts(replicaCounts, replicationFactor, int(endKey))
		s.TransferLease(state.RangeID(2 /* The interesting range */), leaseholder)
		return s
	}

	getStartKeySizes := func(s state.State) map[int64]int64 {
		ret := make(map[int64]int64)
		for _, rng := range s.Ranges() {
			startKey, _, _ := s.RangeSpan(rng.RangeID())
			// Ignore the first range and the rhs split we created at endKey.
			if startKey == -1 || startKey == endKey {
				continue
			}
			size := rng.Size()
			ret[int64(startKey)] = size
		}
		return ret
	}

	testCases := []struct {
		desc           string
		splitThreshold int64
		ticks          []int64
		workload       map[int64]workload.LoadEvent
		expected       []map[int64]int64
	}{
		{
			desc:           "no split below threshold",
			splitThreshold: 10,
			ticks:          []int64{0, 10},
			workload: map[int64]workload.LoadEvent{
				0: {Key: 0, Writes: 1, WriteSize: 9},
			},
			expected: []map[int64]int64{
				{0: 9},
				{0: 9},
			},
		},
		{
			desc:           "1 split [0,100) -> [0,50) [50,100)",
			splitThreshold: 10,
			ticks:          []int64{0, 5, 10},
			workload: map[int64]workload.LoadEvent{
				0: {Key: 0, Writes: 1, WriteSize: 18},
			},
			expected: []map[int64]int64{
				{0: 18},
				{0: 9, 50: 9},
				{0: 9, 50: 9},
			},
		},
		{
			desc:           "2 splits [0,100) -> [0,50)[50,100) -> [0,50)[50,75)[75,100)",
			splitThreshold: 10,
			ticks:          []int64{0, 5, 10, 15, 20},
			workload: map[int64]workload.LoadEvent{
				0:  {Key: 0, Writes: 1, WriteSize: 18},
				15: {Key: 51, Writes: 1, WriteSize: 5},
			},
			expected: []map[int64]int64{
				{0: 18},
				{0: 9, 50: 9},
				{0: 9, 50: 9},
				{0: 9, 50: 14},
				{0: 9, 50: 7, 75: 7},
			},
		},
		{
			desc:           "3 splits [0,100) -> [0,50)[50,100) -> [0,25)[25,50][50,75)[75,100)",
			splitThreshold: 10,
			ticks:          []int64{0, 5, 10, 15},
			workload: map[int64]workload.LoadEvent{
				0: {Key: 0, Writes: 1, WriteSize: 20},
			},
			expected: []map[int64]int64{
				{0: 20},
				{0: 10, 50: 10},
				{0: 5, 25: 5, 50: 10},
				{0: 5, 25: 5, 50: 5, 75: 5},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			replicaCounts := map[state.StoreID]int{1: 1, 2: 1, 3: 1}
			s := testingState(
				replicaCounts,
				3, /* replication factor */
				testingStore,
			)
			s.SplitRange(endKey)

			changer := state.NewReplicaChanger()
			store, _ := s.Store(testingStore)
			sq := NewSplitQueue(
				store.StoreID(),
				changer,
				testSettings.RangeSplitDelayFn(),
				tc.splitThreshold,
				start,
				testSettings,
			)

			results := make([]map[int64]int64, 0, 1)

			for _, tick := range tc.ticks {

				// If there is a load event for this tick, then apply it to
				// state.
				if loadEvent, ok := tc.workload[tick]; ok {
					s.ApplyLoad(workload.LoadBatch{loadEvent})
				}

				// Tick the split queue, if there are pending changes then
				// enqueue them for application.
				sq.Tick(ctx, state.OffsetTick(start, tick), s)

				// Tick state updates that are queued for completion.
				changer.Tick(state.OffsetTick(start, tick), s)

				// Check every replica on the leaseholder store for enqueuing.
				for _, repl := range s.Replicas(store.StoreID()) {
					sq.MaybeAdd(ctx, repl, s)
				}
				results = append(results, getStartKeySizes(s))
			}
			require.Equal(t, tc.expected, results)
		})
	}
}
