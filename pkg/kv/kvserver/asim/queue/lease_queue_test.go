// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package queue

import (
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

func TestLeaseQueue(t *testing.T) {
	start := state.TestingStartTime()
	ctx := context.Background()
	testingStore := state.StoreID(1)
	testSettings := config.DefaultSimulationSettings()

	// NB: This test assumes 5s interval/changes for simplification purposes.
	testSettings.StateExchangeInterval = 5 * time.Second
	testSettings.ReplicaChangeBaseDelay = 5 * time.Second

	leasePreference := func(constraints ...roachpb.Constraint) roachpb.LeasePreference {
		preference := roachpb.LeasePreference{
			Constraints: make([]roachpb.Constraint, len(constraints)),
		}
		copy(preference.Constraints, constraints)
		return preference
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
		desc                string
		replicaCounts       map[state.StoreID]int
		spanConfig          roachpb.SpanConfig
		initialRF           int
		nonLiveNodes        map[state.NodeID]livenesspb.NodeLivenessStatus
		nodeLocalities      map[state.NodeID]roachpb.Locality
		ticks               []int64
		expectedLeaseCounts map[int64]map[int]int
	}{
		{
			// Initially n1 will have all the leases.
			desc: "imbalanced leases",
			replicaCounts: map[state.StoreID]int{
				1: 100, 2: 100,
			},
			ticks: []int64{5, 10, 15, 20, 25},
			expectedLeaseCounts: map[int64]map[int]int{
				5:  {1: 100, 2: 0},
				10: {1: 100, 2: 0},
				15: {1: 99, 2: 1},
				20: {1: 98, 2: 2},
				25: {1: 97, 2: 3},
			},
		},
		{
			desc: "handle lease preferences",
			replicaCounts: map[state.StoreID]int{
				1: 10, 2: 10,
			},
			spanConfig: roachpb.SpanConfig{
				NumReplicas: 2,
				NumVoters:   2,
				LeasePreferences: []roachpb.LeasePreference{
					leasePreference(makeConstraint("region", "b")),
				},
			},
			nodeLocalities: map[state.NodeID]roachpb.Locality{
				1: singleLocality("region", "a"),
				2: singleLocality("region", "b"),
			},
			ticks: []int64{5, 10, 15, 20, 25},
			expectedLeaseCounts: map[int64]map[int]int{
				5:  {1: 10, 2: 0},
				10: {1: 10, 2: 0},
				15: {1: 9, 2: 1},
				20: {1: 8, 2: 2},
				25: {1: 7, 2: 3},
			},
		},
		{
			desc: "don't transfer leases to draining store",
			replicaCounts: map[state.StoreID]int{
				1: 10, 2: 10,
			},
			spanConfig: roachpb.SpanConfig{
				NumReplicas: 2,
				NumVoters:   2,
				LeasePreferences: []roachpb.LeasePreference{
					leasePreference(makeConstraint("region", "b")),
				},
			},
			nodeLocalities: map[state.NodeID]roachpb.Locality{
				1: singleLocality("region", "a"),
				2: singleLocality("region", "b"),
			},
			nonLiveNodes: map[state.NodeID]livenesspb.NodeLivenessStatus{
				2: livenesspb.NodeLivenessStatus_DRAINING},
			ticks: []int64{5, 10, 15},
			expectedLeaseCounts: map[int64]map[int]int{
				5:  {1: 10, 2: 0},
				10: {1: 10, 2: 0},
				15: {1: 10, 2: 0},
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
			lq := NewLeaseQueue(
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

			leaseCountResults := make(map[int64]map[int]int)
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

				// Tick the lease queue, popping a queued replica and considering lease
				// transfers.
				lq.Tick(ctx, state.OffsetTick(start, tick), s)

				if nextRepl == len(repls) {
					repls = s.Replicas(store.StoreID())
					nextRepl = 0
				}
				// Add a new repl to the lease queue.
				lq.MaybeAdd(ctx, repls[nextRepl], s)

				nextRepl++
				_, leaseCounts := testingGetReplLeaseCounts(s)
				leaseCountResults[tick] = leaseCounts
			}
			require.Equal(t, tc.expectedLeaseCounts, leaseCountResults)
		})
	}
}
