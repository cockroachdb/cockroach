// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package queue

import (
	"container/heap"
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
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

func testingGetReplLeaseCounts(s state.State) (replCounts, leaseCounts map[int]int) {
	storeReplView := make(map[int]int)
	storeLeaseView := make(map[int]int)
	stores := s.Stores()
	storeIDs := make([]state.StoreID, len(stores))
	for i, store := range stores {
		storeIDs[i] = store.StoreID()
	}
	for _, desc := range s.StoreDescriptors(false /* cached */, storeIDs...) {
		storeReplView[int(desc.StoreID)] = int(desc.Capacity.RangeCount)
		storeLeaseView[int(desc.StoreID)] = int(desc.Capacity.LeaseCount)
	}
	return storeReplView, storeLeaseView
}

func makeConstraint(k, v string) roachpb.Constraint {
	return roachpb.Constraint{
		Type:  roachpb.Constraint_REQUIRED,
		Key:   k,
		Value: v,
	}
}

func singleLocality(k, v string) roachpb.Locality {
	return roachpb.Locality{Tiers: []roachpb.Tier{{Key: k, Value: v}}}
}

func TestReplicateQueue(t *testing.T) {
	start := state.TestingStartTime()
	ctx := context.Background()
	testingStore := state.StoreID(1)
	testSettings := config.DefaultSimulationSettings()

	// NB: This test assumes 5s interval/changes for simplification purposes.
	testSettings.StateExchangeInterval = 5 * time.Second
	testSettings.ReplicaChangeBaseDelay = 5 * time.Second

	conjunctionConstraint := func(k, v string, numReplicas int32) roachpb.ConstraintsConjunction {
		return roachpb.ConstraintsConjunction{
			NumReplicas: numReplicas,
			Constraints: []roachpb.Constraint{makeConstraint(k, v)},
		}
	}

	testingState := func(replicaCounts map[state.StoreID]int, spanConfig *roachpb.SpanConfig, initialRF int) state.State {
		s := state.NewStateWithReplCounts(replicaCounts, initialRF, 1000 /* keyspace */, testSettings)
		for _, r := range s.Ranges() {
			s.SetSpanConfigForRange(r.RangeID(), spanConfig)
			s.TransferLease(r.RangeID(), testingStore)
		}
		return s
	}

	testCases := []struct {
		desc               string
		replicaCounts      map[state.StoreID]int
		spanConfig         roachpb.SpanConfig
		initialRF          int
		nonLiveNodes       map[state.NodeID]livenesspb.NodeLivenessStatus
		nodeLocalities     map[state.NodeID]roachpb.Locality
		ticks              []int64
		expectedReplCounts map[int64]map[int]int
	}{
		{
			// NB: Expect no action, range counts are balanced.
			desc: "s1:(r=10),s2:(r=10) balanced replicas, noop",
			replicaCounts: map[state.StoreID]int{
				1: 10, 2: 10,
			},
			ticks: []int64{5, 10, 15},
			expectedReplCounts: map[int64]map[int]int{
				5:  {1: 10, 2: 10},
				10: {1: 10, 2: 10},
				15: {1: 10, 2: 10},
			},
		},
		{
			// NB: Expect replica transfer towards s3, one per interval. The only
			// option is moving replicas from s2 -> s3.
			desc: "s1:(r=10), s2:(r=10), s3:(r=0) rebalance replicas s2 -> s3",
			replicaCounts: map[state.StoreID]int{
				1: 10, 2: 10, 3: 0,
			},
			ticks: []int64{5, 10, 15, 20},
			expectedReplCounts: map[int64]map[int]int{
				5:  {1: 10, 2: 10, 3: 0},
				10: {1: 10, 2: 10, 3: 0},
				15: {1: 10, 2: 9, 3: 1},
				20: {1: 9, 2: 9, 3: 2},
			},
		},
		{
			desc: "up-replicate RF=3 -> RF=5",
			replicaCounts: map[state.StoreID]int{
				1: 1, 2: 1, 3: 1, 4: 0, 5: 0,
			},
			initialRF: 3,
			spanConfig: roachpb.SpanConfig{
				NumReplicas: 5,
				NumVoters:   5,
			},
			ticks: []int64{5, 10, 15, 20},
			expectedReplCounts: map[int64]map[int]int{
				5:  {1: 1, 2: 1, 3: 1, 4: 0, 5: 0},
				10: {1: 1, 2: 1, 3: 1, 4: 0, 5: 0},
				15: {1: 1, 2: 1, 3: 1, 4: 0, 5: 1},
				20: {1: 1, 2: 1, 3: 1, 4: 1, 5: 1},
			},
		},
		{
			desc: "up-replicate RF=3 -> RF=5 +2 non-voters",
			replicaCounts: map[state.StoreID]int{
				1: 1, 2: 1, 3: 1, 4: 0, 5: 0,
			},
			initialRF: 3,
			spanConfig: roachpb.SpanConfig{
				NumReplicas: 5,
				NumVoters:   3,
			},
			ticks: []int64{5, 10, 15, 20},
			expectedReplCounts: map[int64]map[int]int{
				5:  {1: 1, 2: 1, 3: 1, 4: 0, 5: 0},
				10: {1: 1, 2: 1, 3: 1, 4: 0, 5: 0},
				15: {1: 1, 2: 1, 3: 1, 4: 0, 5: 1},
				20: {1: 1, 2: 1, 3: 1, 4: 1, 5: 1},
			},
		},
		{
			desc: "down-replicate RF=5 -> RF=3",
			replicaCounts: map[state.StoreID]int{
				1: 1, 2: 1, 3: 1, 4: 1, 5: 1,
			},
			initialRF: 5,
			spanConfig: roachpb.SpanConfig{
				NumReplicas: 3,
				NumVoters:   3,
			},
			ticks: []int64{5, 10, 15, 20},
			expectedReplCounts: map[int64]map[int]int{
				5:  {1: 1, 2: 1, 3: 1, 4: 1, 5: 1},
				10: {1: 1, 2: 1, 3: 1, 4: 1, 5: 1},
				15: {1: 1, 2: 1, 3: 1, 4: 0, 5: 1},
				20: {1: 1, 2: 0, 3: 1, 4: 0, 5: 1},
			},
		},
		{
			desc: "replace dead voter",
			replicaCounts: map[state.StoreID]int{
				1: 1, 2: 1, 3: 1, 4: 0,
			},
			initialRF: 3,
			spanConfig: roachpb.SpanConfig{
				NumReplicas: 3,
				NumVoters:   3,
			},
			nonLiveNodes: map[state.NodeID]livenesspb.NodeLivenessStatus{
				3: livenesspb.NodeLivenessStatus_DEAD},
			ticks: []int64{5, 10, 15},
			expectedReplCounts: map[int64]map[int]int{
				5:  {1: 1, 2: 1, 3: 1, 4: 0},
				10: {1: 1, 2: 1, 3: 1, 4: 0},
				15: {1: 1, 2: 1, 3: 0, 4: 1},
			},
		},
		{
			desc: "replace decommissioning voters",
			replicaCounts: map[state.StoreID]int{
				1: 10, 2: 10, 3: 10, 4: 0,
			},
			initialRF: 3,
			spanConfig: roachpb.SpanConfig{
				NumReplicas: 3,
				NumVoters:   3,
			},
			nonLiveNodes: map[state.NodeID]livenesspb.NodeLivenessStatus{
				3: livenesspb.NodeLivenessStatus_DECOMMISSIONING},
			ticks: []int64{5, 10, 15, 20, 25},
			expectedReplCounts: map[int64]map[int]int{
				5:  {1: 10, 2: 10, 3: 10, 4: 0},
				10: {1: 10, 2: 10, 3: 10, 4: 0},
				15: {1: 10, 2: 10, 3: 9, 4: 1},
				20: {1: 10, 2: 10, 3: 8, 4: 2},
				25: {1: 10, 2: 10, 3: 7, 4: 3},
			},
		},
		{
			// There are 10 voters in region a and b each at the moment.The span
			// config specifies 1 replica should be in a and one replica in c. b is
			// not a valid region, replicas should move from b -> c.
			desc: "handle span config constraints",
			replicaCounts: map[state.StoreID]int{
				1: 10, 2: 10, 3: 0,
			},
			initialRF: 2,
			spanConfig: roachpb.SpanConfig{
				NumReplicas: 2,
				NumVoters:   2,
				Constraints: []roachpb.ConstraintsConjunction{
					conjunctionConstraint("region", "a", 1),
					conjunctionConstraint("region", "c", 1),
				},
			},
			nodeLocalities: map[state.NodeID]roachpb.Locality{
				1: singleLocality("region", "a"),
				2: singleLocality("region", "b"),
				3: singleLocality("region", "c"),
			},
			ticks: []int64{5, 10, 15, 20, 25},
			expectedReplCounts: map[int64]map[int]int{
				5:  {1: 10, 2: 10, 3: 0},
				10: {1: 10, 2: 10, 3: 0},
				15: {1: 10, 2: 9, 3: 1},
				20: {1: 10, 2: 8, 3: 2},
				25: {1: 10, 2: 7, 3: 3},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			spanConfig := tc.spanConfig
			initialRF := tc.initialRF
			// Default to RF=2 for simplicity when not specified. Although this is
			// fairly unrealistic.
			if spanConfig.NumReplicas == 0 {
				spanConfig = roachpb.SpanConfig{NumVoters: 2, NumReplicas: 2}
			}
			if initialRF == 0 {
				initialRF = 2
			}

			s := testingState(tc.replicaCounts, &spanConfig, initialRF)
			changer := state.NewReplicaChanger()
			store, _ := s.Store(testingStore)
			rq := NewReplicateQueue(
				store.StoreID(),
				changer,
				testSettings,
				s.MakeAllocator(store.StoreID()),
				s.StorePool(store.StoreID()),
				start,
			)
			s.TickClock(start)

			for nodeID, livenessStatus := range tc.nonLiveNodes {
				s.SetNodeLiveness(nodeID, livenessStatus)
			}

			for nodeID, locality := range tc.nodeLocalities {
				s.SetNodeLocality(nodeID, locality)
			}

			replCountResults := make(map[int64]map[int]int)
			// Initialize the store pool information.
			gossip := gossip.NewGossip(s, testSettings)
			gossip.Tick(ctx, start, s)

			nextRepl := 0
			repls := s.Replicas(store.StoreID())

			for _, tick := range tc.ticks {
				// Tick the storepool clock, to update wall clock and avoid
				// considering stores as dead.
				s.TickClock(state.OffsetTick(start, tick))

				// Tick state updates that are queued for completion.
				changer.Tick(state.OffsetTick(start, tick), s)

				// Update the store's view of the cluster, we update all stores
				// but only care about s1's view.
				gossip.Tick(ctx, state.OffsetTick(start, tick), s)

				// Tick the replicate queue, popping a queued replicas and
				// considering rebalance.
				rq.Tick(ctx, state.OffsetTick(start, tick), s)

				if nextRepl == len(repls) {
					repls = s.Replicas(store.StoreID())
					nextRepl = 0
				}
				// Add a new repl to the replicate queue.
				rq.MaybeAdd(ctx, repls[nextRepl], s)

				nextRepl++
				replCounts, _ := testingGetReplLeaseCounts(s)
				replCountResults[tick] = replCounts
			}
			require.Equal(t, tc.expectedReplCounts, replCountResults)
		})
	}
}
