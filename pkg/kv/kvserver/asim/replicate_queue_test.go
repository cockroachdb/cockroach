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
	"container/heap"
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
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

func TestReplicateQueue(t *testing.T) {
	start := state.TestingStartTime()
	ctx := context.Background()
	testingStore := state.StoreID(1)
	testSettings := config.DefaultSimulationSettings()

	// NB: This test assumes 5s interval/changes for simplification purposes.
	testSettings.StateExchangeInterval = 5 * time.Second
	testSettings.ReplicaChangeBaseDelay = 5 * time.Second

	getReplCounts := func(s state.State) map[int]int {
		storeView := make(map[int]int)
		for _, desc := range s.StoreDescriptors() {
			storeView[int(desc.StoreID)] = int(desc.Capacity.RangeCount)
		}
		return storeView
	}

	testingState := func(replicaCounts map[state.StoreID]int, replicationFactor int32) state.State {
		s := state.NewTestStateReplCounts(replicaCounts, 3 /* replsPerRange */)
		spanConfig := roachpb.SpanConfig{NumVoters: replicationFactor, NumReplicas: replicationFactor}
		for _, r := range s.Ranges() {
			s.SetSpanConfig(r.RangeID(), spanConfig)
		}
		return s
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
			s := testingState(tc.replicaCounts, 2 /* replication factor */)
			changer := state.NewReplicaChanger()
			store, _ := s.Store(testingStore)
			rq := NewReplicateQueue(
				store.StoreID(),
				changer,
				testSettings.ReplicaChangeDelayFn(),
				s.MakeAllocator(store.StoreID()),
				start,
			)
			s.TickClock(start)

			results := make(map[int64]map[int]int)
			// Initialize the store pool information.
			exchange := state.NewFixedDelayExhange(
				start,
				testSettings.StateExchangeInterval,
				time.Second*0, /* no state update delay */
			)
			exchange.Put(start, s.StoreDescriptors()...)

			nextRepl := 0
			repls := s.Replicas(store.StoreID())

			for _, tick := range tc.ticks {
				// Tick the storepool clock, to update wall clock and avoid
				// considering stores as dead.
				s.TickClock(state.OffsetTick(start, tick))

				// Update the store's view of the cluster, we update all stores
				// but only care about s1's view.
				exchange.Put(state.OffsetTick(start, tick), s.StoreDescriptors()...)

				// Update s1's view of the cluster.
				s.UpdateStorePool(
					store.StoreID(),
					exchange.Get(state.OffsetTick(start, tick), store.Descriptor().StoreID),
				)

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
		replicationFactor int32,
		leaseholder state.StoreID,
	) state.State {
		s := state.NewTestStateReplCounts(replicaCounts, int(replicationFactor))
		spanConfig := roachpb.SpanConfig{
			NumVoters:   replicationFactor,
			NumReplicas: replicationFactor,
		}
		for _, r := range s.Ranges() {
			s.SetSpanConfig(r.RangeID(), spanConfig)
		}
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
