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
	"container/heap"
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

// TestQueuePriorityQueue verifies priority queue implementation.
func TestQueuePriorityQueue(t *testing.T) {
	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	const count = 3
	expRanges := make([]roachpb.RangeID, count+1)
	pq := priorityQueue{}
	pq.items = make([]*replicaItem, count)
	for i := 0; i < count; {
		pq.items[i] = &replicaItem{
			rangeID:  roachpb.RangeID(i),
			priority: float64(i),
			index:    i,
		}
		expRanges[3-i] = pq.items[i].rangeID
		i++
	}
	heap.Init(&pq)

	// Insert a new item and then modify its priority.
	priorityItem := &replicaItem{
		rangeID:  -1,
		priority: 4.0,
	}
	heap.Push(&pq, priorityItem)
	expRanges[0] = priorityItem.rangeID

	// Take the items out; they should arrive in decreasing priority order.
	for i := 0; pq.Len() > 0; i++ {
		item := heap.Pop(&pq).(*replicaItem)
		require.Equal(t, expRanges[i], item.rangeID)
	}
}

func getReplCounts(s state.State) map[int]int {
	storeView := make(map[int]int)
	stores := s.Stores()
	storeIDs := []state.StoreID{}
	for _, store := range stores {
		storeIDs = append(storeIDs, store.StoreID())
	}
	for _, desc := range s.StoreDescriptors(storeIDs...) {
		storeView[int(desc.StoreID)] = int(desc.Capacity.RangeCount)
	}
	return storeView
}

func TestReplicateQueue(t *testing.T) {
	start := state.TestingStartTime()
	ctx := context.Background()
	testingStore := state.StoreID(1)
	testSettings := config.DefaultSimulationSettings()

	// NB: This test assumes 5s interval/changes for simplification purposes.
	testSettings.ReplicaChangeBaseDelay = 5 * time.Second
	testSettings.StateExchangeDelay = 0
	keyspace := 10000

	testCases := []struct {
		desc          string
		replicaCounts map[state.StoreID]int
		ticks         []int64
		expected      map[int64]map[int]int
		replFactor    int
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
			replFactor: 2,
		},
		{
			// NB: Expect replica transfer towards s3, one per interval. Since
			// no lease transfers can occur (except when removing the outgoing
			// LH), we expect only replica rebalancing. The only option is
			// moving replicas from s2 -> s3.
			desc: "s1:(l=10,r=10), s2:(l=0.r=10), s3:(l=0,r=0) rebalance replicas s1,s2 -> s3",
			replicaCounts: map[state.StoreID]int{
				1: 10, 2: 10, 3: 0,
			},
			ticks: []int64{5, 10, 15},
			expected: map[int64]map[int]int{
				5:  {1: 10, 2: 10, 3: 0},
				10: {1: 10, 2: 9, 3: 1},
				15: {1: 9, 2: 9, 3: 2},
			},
			replFactor: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			s := state.NewTestStateReplCounts(tc.replicaCounts, tc.replFactor, keyspace)
			changer := state.NewReplicaChanger()
			store, _ := s.Store(testingStore)
			// Disable lease transfers to only test replica rebalancing.
			testSettings.ReplQueueTransfersEnabled = false

			rq := NewReplicateQueue(
				store.StoreID(),
				changer,
				s.MakeAllocator(store.StoreID()),
				start,
				testSettings,
			)
			s.TickClock(start)

			results := make(map[int64]map[int]int)
			// Initialize the store pool information.
			gossip := gossip.NewStoreGossip(testSettings)
			gossip.Tick(start.Add(-testSettings.StateExchangeDelay), s)

			nextRepl := 0
			repls := s.Replicas(store.StoreID())

			for _, tick := range tc.ticks {
				// Tick the storepool clock, to update wall clock and avoid
				// considering stores as dead.
				s.TickClock(state.OffsetTick(start, tick))

				// Update the store's view of the cluster, we update all stores
				// but only care about s1's view.
				gossip.Tick(state.OffsetTick(start, tick), s)

				// Tick the replicate queue, popping a queued replicas and
				// considering rebalance.
				rq.Tick(ctx, state.OffsetTick(start, tick), s)

				// Tick state updates that are queued for completion.
				changer.Tick(state.OffsetTick(start, tick), s)

				// Add a new repl to the replicate queue.
				rq.MaybeAdd(ctx, repls[nextRepl], s)
				nextRepl++

				results[tick] = getReplCounts(s)
			}
			require.Equal(t, tc.expected, results)
		})
	}
}

func TestReplicateQueueLeases(t *testing.T) {
	start := state.TestingStartTime()
	ctx := context.Background()
	testingStore := state.StoreID(1)
	testSettings := config.DefaultSimulationSettings()
	keyspace := 10000

	getLeaseCount := func(s state.State) map[int]int {
		storeIDList := []state.StoreID{}
		for _, store := range s.Stores() {
			storeIDList = append(storeIDList, store.StoreID())
		}
		descriptors := s.StoreDescriptors(storeIDList...)

		ret := make(map[int]int)
		for _, desc := range descriptors {
			ret[int(desc.StoreID)] = int(desc.Capacity.LeaseCount)
		}
		return ret
	}

	testCases := []struct {
		desc                                    string
		replicaCounts                           map[state.StoreID]int
		ticks                                   int
		expectedLeaseCounts, expectedReplCounts map[int]int
		replFactor                              int
	}{
		{
			// NB: Replica counts are balanced but lease counts are not, expect
			// lease count rebalancing.
			desc:          "balance leases",
			replicaCounts: map[state.StoreID]int{1: 20, 2: 20},
			ticks:         200,
			// NB: The lease rebaance threshold is 5% of the mean, there is a
			// minimum of mean + 5, meaning that at 15 leases, when the average
			// is 10, no more lease transfer targets will be returned from
			// maybeTransferLease.
			expectedLeaseCounts: map[int]int{1: 15, 2: 5},
			expectedReplCounts:  map[int]int{1: 20, 2: 20},
			replFactor:          2,
		},
		{
			// NB: Replica and lease counts are imbalanced, expect range and
			// lease count rebalancing.
			desc:          "balance lease and replica counts",
			ticks:         100,
			replicaCounts: map[state.StoreID]int{1: 20, 2: 20, 3: 0},
			// NB: mean is 20/3 = 6.66, so the minimum leases a store can have
			// is ceil(6.66-5)=2.
			expectedLeaseCounts: map[int]int{1: 9, 2: 2, 3: 9},
			expectedReplCounts:  map[int]int{1: 14, 2: 14, 3: 12},
			replFactor:          2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			s := state.NewTestStateReplCounts(tc.replicaCounts, tc.replFactor, keyspace)
			changer := state.NewReplicaChanger()
			store, _ := s.Store(testingStore)

			rq := NewReplicateQueue(
				store.StoreID(),
				changer,
				s.MakeAllocator(store.StoreID()),
				start,
				testSettings,
			)
			s.TickClock(start)

			// Initialize the store pool information.
			gossip := gossip.NewStoreGossip(testSettings)
			gossip.Tick(start.Add(-testSettings.StateExchangeDelay), s)

			nextRepl := 0
			repls := s.Replicas(store.StoreID())

			for tick := int64(0); int(tick) < tc.ticks; tick++ {
				// Tick the storepool clock, to update wall clock and avoid
				// considering stores as dead.
				s.TickClock(state.OffsetTick(start, tick))

				// Tick state updates that are queued for completion.
				changer.Tick(state.OffsetTick(start, tick), s)

				// Update the store's view of the cluster, we update all stores
				// but only care about s1's view.
				gossip.Tick(state.OffsetTick(start, tick), s)

				// Tick the replicate queue, popping a queued replicas and
				// considering rebalance.
				rq.Tick(ctx, state.OffsetTick(start, tick), s)

				if nextRepl >= len(repls) {
					nextRepl = 0
					repls = s.Replicas(store.StoreID())
				}

				// Add a new repl to the replicate queue.
				rq.MaybeAdd(ctx, repls[nextRepl], s)
				nextRepl++
			}
			require.Equal(t, tc.expectedLeaseCounts, getLeaseCount(s), "lease counts differ.")
			require.Equal(t, tc.expectedReplCounts, getReplCounts(s), "repl counts differ.")
		})
	}
}
