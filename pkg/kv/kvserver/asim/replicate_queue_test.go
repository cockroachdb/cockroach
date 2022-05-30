// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

// TestReplicateQueue
func TestReplicateQueue(t *testing.T) {
	start := state.TestingStartTime()
	ctx := context.Background()
	testingDelay := 5 * time.Second
	testingStore := state.StoreID(1)

	getReplCounts := func(s state.State) map[int]int {
		storeView := make(map[int]int)
		for _, desc := range s.StoreDescriptors() {
			storeView[int(desc.StoreID)] = int(desc.Capacity.RangeCount)
		}
		return storeView
	}

	testingState := func(replicaCounts map[state.StoreID]int, replicationFactor int32) (state.State, []state.Range) {
		s, ranges := state.NewTestStateReplCounts(replicaCounts)
		spanConfig := roachpb.SpanConfig{NumVoters: replicationFactor, NumReplicas: replicationFactor}
		for _, r := range ranges {
			s.SetSpanConfig(r.RangeID(), spanConfig)
		}
		return s, ranges
	}

	testCases := []struct {
		desc          string
		replicaCounts map[state.StoreID]int
		ticks         []int64
		expected      map[int64]map[int]int
	}{
		{
			// NB: Expect no action, range counts are balanced.
			desc: "s1:(l=10,r=10),s2:(l=0,r=10) balanced replicas, noop",
			replicaCounts: map[state.StoreID]int{
				1: 10, 2: 10,
			},
			ticks: []int64{5, 10, 15},
			expected: map[int64]map[int]int{
				5:  {1: 10, 2: 10},
				10: {1: 10, 2: 10},
				15: {1: 10, 2: 10},
			},
		},
		{
			// NB: Expect replica transfer towards s3, one per interval. Since
			// no lease transfers can occur, we expect only replica
			// rebalancing. The only option is moving replicas from s2 -> s3.
			desc: "s1:(l=10,r=10), s2:(l=0.r=10), s3:(l=0,r=0) rebalance replicas s2 -> s3",
			replicaCounts: map[state.StoreID]int{
				1: 10, 2: 10, 3: 0,
			},
			ticks: []int64{5, 10, 15},
			expected: map[int64]map[int]int{
				5:  {1: 10, 2: 10, 3: 0},
				10: {1: 10, 2: 9, 3: 1},
				15: {1: 10, 2: 8, 3: 2},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			s, _ := testingState(tc.replicaCounts, 2 /* replication factor */)
			changer := state.NewReplicaChanger()
			store, _ := s.Store(testingStore)
			rq := NewReplicateQueue(store.StoreID(), changer, testingDelay, s.MakeAllocator(store.StoreID()))
			s.TickClock(start)

			results := make(map[int64]map[int]int)
			// Initialize the store pool information.
			exchange := state.NewFixedDelayExhange(start, testingDelay, time.Second*0)
			exchange.Put(start, s.StoreDescriptors()...)

			nextRepl := 0
			repls := s.Replicas(store.StoreID())

			for _, tick := range tc.ticks {

				// Add a new repl to the replicate queue.
				rq.MaybeAdd(ctx, repls[nextRepl], s)
				nextRepl++
				// Tick the storepool clock, to update wall clock and avoid
				// considering stores as dead.
				s.TickClock(state.OffsetTick(start, tick))

				// Tick state updates that are queued for completion.
				changer.Tick(state.OffsetTick(start, tick), s)

				// Update the store's view of the cluster, we update all stores
				// but only care about s1's view.
				exchange.Put(state.OffsetTick(start, tick), s.StoreDescriptors()...)

				// Update s1's view of the cluster.
				s.UpdateStorePool(store.StoreID(), exchange.Get(state.OffsetTick(start, tick), store.Descriptor().StoreID))

				// Tick the replicate queue, popping a queued replicas and
				// considering rebalance.
				rq.Tick(ctx, state.OffsetTick(start, tick), s)

				results[tick] = getReplCounts(s)
			}
			require.Equal(t, tc.expected, results)
		})
	}
}
