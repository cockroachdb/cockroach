// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storerebalancer

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/op"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/stretchr/testify/require"
)

func testingGetStoreQPS(s state.State) map[state.StoreID]float64 {
	ret := map[state.StoreID]float64{}
	stores := s.Stores()
	storeIDs := make([]state.StoreID, len(stores))
	for i, store := range stores {
		storeIDs[i] = store.StoreID()
	}
	for _, desc := range s.StoreDescriptors(false /* cached */, storeIDs...) {
		ret[state.StoreID(desc.StoreID)] = desc.Capacity.QueriesPerSecond
	}
	return ret
}

func TestStoreRebalancer(t *testing.T) {
	testingStore := state.StoreID(1)
	testSettings := config.DefaultSimulationSettings()
	start := testSettings.StartTime
	testSettings.ReplicaChangeBaseDelay = 5 * time.Second
	testSettings.StateExchangeDelay = 0

	clusterInfo := state.ClusterInfoWithStoreCount(6, 1 /* storesPerNode */)

	// NB: We trigger lease rebalancing in this test, where the end result
	// should be a perfectly balanced QPS of 500 per store. We only simulate
	// store 1, it has an available replica on every store, it can transfer a
	// lease to:
	//   - initial state
	//     qps [3000,0,0,0,0,0], sum = 3000
	//     r2  [1,1,1,0,0,0], lh=1, qps=500
	//     r3  [1,0,1,1,0,0], lh=1, qps=500
	//     r4  [1,0,0,1,1,0], lh=1, qps=500
	//     r5  [1,0,0,0,1,1], lh=1, qps=500
	//     r6  [1,1,0,0,0,1], lh=1, qps=500
	//     r7  [1,1,1,0,0,0], lh=1, qps=500
	//   - expect
	//     transfer r2(1->2),  2500
	//     transfer r3(1->3),  2000
	//     transfer r4(1->4),  1500
	//     transfer r5(1->5),  1000
	//     transfer r6(1->6),  500
	leaseRangesInfo := state.RangesInfo{
		state.RangeInfoWithReplicas(100, []state.StoreID{1, 2, 3}, []state.StoreID{}, 1, nil),
		state.RangeInfoWithReplicas(200, []state.StoreID{1, 3, 4}, []state.StoreID{}, 1, nil),
		state.RangeInfoWithReplicas(300, []state.StoreID{1, 4, 5}, []state.StoreID{}, 1, nil),
		state.RangeInfoWithReplicas(400, []state.StoreID{1, 5, 6}, []state.StoreID{}, 1, nil),
		state.RangeInfoWithReplicas(500, []state.StoreID{1, 6, 2}, []state.StoreID{}, 1, nil),
		state.RangeInfoWithReplicas(600, []state.StoreID{1, 2, 3}, []state.StoreID{}, 1, nil),
	}
	leaseState := state.LoadConfig(clusterInfo, leaseRangesInfo, testSettings)
	for i := 2; i < 8; i++ {
		state.TestingSetRangeQPS(leaseState, state.RangeID(i), 500)
	}

	// NB: We trigger range rebalancing (relocate range), where the end result
	// will remain an imbalanced QPS, however with relocations having occurred.
	//   - initial state
	//     qps [3200,3000,3000,0,0,0], sum = 9200
	//     r2  [1,1,1,0,0,0], lh=1, qps=800
	//     r3  [1,1,1,0,0,0], lh=1, qps=800
	//     r4  [1,1,1,0,0,0], lh=1, qps=800
	//     r5  [1,1,1,0,0,0], lh=1, qps=800
	//     r6  [1,1,1,0,0,0], lh=2, qps=3000
	//     r7  [1,1,1,0,0,0], lh=3, qps=3000
	//   - expect
	//     relocate r2([1,2,3]->[4,5,6]), 2400
	//     relocate r3([1,2,3]->[4,5,6]), 1600
	rangeRangesinfo := state.RangesInfo{
		state.RangeInfoWithReplicas(100, []state.StoreID{1, 2, 3}, []state.StoreID{}, 1, nil),
		state.RangeInfoWithReplicas(200, []state.StoreID{1, 2, 3}, []state.StoreID{}, 1, nil),
		state.RangeInfoWithReplicas(300, []state.StoreID{1, 2, 3}, []state.StoreID{}, 1, nil),
		state.RangeInfoWithReplicas(400, []state.StoreID{1, 2, 3}, []state.StoreID{}, 1, nil),
		state.RangeInfoWithReplicas(500, []state.StoreID{1, 2, 3}, []state.StoreID{}, 2, nil),
		state.RangeInfoWithReplicas(600, []state.StoreID{1, 2, 3}, []state.StoreID{}, 3, nil),
	}
	rangeState := state.LoadConfig(clusterInfo, rangeRangesinfo, testSettings)
	for i := 2; i < 6; i++ {
		state.TestingSetRangeQPS(rangeState, state.RangeID(i), 800)
	}
	state.TestingSetRangeQPS(rangeState, state.RangeID(6), 3000)
	state.TestingSetRangeQPS(rangeState, state.RangeID(7), 3000)

	testCases := []struct {
		desc             string
		s                state.State
		ticks            []int64
		expectedPhases   []storeRebalancerPhase
		expectedStoreQPS []map[state.StoreID]float64
	}{
		{
			desc:  "lease rebalancing off hot store",
			s:     leaseState,
			ticks: []int64{0, 5, 10, 15, 20, 25, 30},
			expectedPhases: []storeRebalancerPhase{
				rebalancerSleeping,
				leaseRebalancing,
				leaseRebalancing,
				leaseRebalancing,
				leaseRebalancing,
				leaseRebalancing,
				rebalancerSleeping,
			},
			expectedStoreQPS: []map[state.StoreID]float64{
				{1: 3000, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0},
				{1: 3000, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0},
				{1: 2500, 2: 0, 3: 500, 4: 0, 5: 0, 6: 0},
				{1: 2000, 2: 0, 3: 500, 4: 500, 5: 0, 6: 0},
				{1: 1500, 2: 0, 3: 500, 4: 500, 5: 500, 6: 0},
				{1: 1000, 2: 0, 3: 500, 4: 500, 5: 500, 6: 500},
				{1: 500, 2: 500, 3: 500, 4: 500, 5: 500, 6: 500},
			},
		},
		{
			desc:  "range rebalancing off hot store",
			s:     rangeState,
			ticks: []int64{0, 5, 10, 15, 20, 25, 30},
			expectedPhases: []storeRebalancerPhase{
				rebalancerSleeping,
				rangeRebalancing,
				rangeRebalancing,
				rebalancerSleeping,
				rebalancerSleeping,
				rebalancerSleeping,
				rebalancerSleeping,
			},
			expectedStoreQPS: []map[state.StoreID]float64{
				{1: 3200, 2: 3000, 3: 3000, 4: 0, 5: 0, 6: 0},
				{1: 3200, 2: 3000, 3: 3000, 4: 0, 5: 0, 6: 0},
				{1: 2400, 2: 3000, 3: 3000, 4: 0, 5: 0, 6: 800},
				{1: 2400, 2: 3000, 3: 3000, 4: 0, 5: 0, 6: 800},
				{1: 1600, 2: 3000, 3: 3000, 4: 800, 5: 0, 6: 800},
				{1: 1600, 2: 3000, 3: 3000, 4: 800, 5: 0, 6: 800},
				{1: 1600, 2: 3000, 3: 3000, 4: 800, 5: 0, 6: 800},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			s := tc.s

			gossip := gossip.NewGossip(s, testSettings)
			gossip.Tick(ctx, start, s)

			allocator := s.MakeAllocator(testingStore)
			storePool := s.StorePool(testingStore)
			changer := state.NewReplicaChanger()
			controller := op.NewController(changer, allocator, storePool, testSettings, testingStore)
			src := newStoreRebalancerControl(start, testingStore, controller, allocator, storePool, testSettings, GetStateRaftStatusFn(s))
			s.TickClock(start)

			resultsQPS := []map[state.StoreID]float64{}
			resultsPhase := []storeRebalancerPhase{}
			for _, tick := range tc.ticks {
				s.TickClock(state.OffsetTick(start, tick))
				changer.Tick(state.OffsetTick(start, tick), s)
				controller.Tick(ctx, state.OffsetTick(start, tick), s)
				src.Tick(ctx, state.OffsetTick(start, tick), s)
				resultsPhase = append(resultsPhase, src.rebalancerState.phase)
				storeQPS := testingGetStoreQPS(s)
				resultsQPS = append(resultsQPS, storeQPS)
			}

			require.Equal(t, tc.expectedStoreQPS, resultsQPS)
			require.Equal(t, tc.expectedPhases, resultsPhase)
		})
	}

}

func TestStoreRebalancerBalances(t *testing.T) {
	testingStore := state.StoreID(1)
	testSettings := config.DefaultSimulationSettings()
	start := testSettings.StartTime
	testSettings.ReplicaAddRate = 1
	testSettings.ReplicaChangeBaseDelay = 1 * time.Second
	testSettings.StateExchangeInterval = 1 * time.Second
	testSettings.StateExchangeDelay = 0

	distributeQPS := func(s state.State, qpsCounts map[state.StoreID]float64) {
		dist := make([]float64, len(qpsCounts))
		for i := 0; i < len(qpsCounts); i++ {
			dist[i] = qpsCounts[state.StoreID(i+1)]
		}
		state.TestDistributeQPSCounts(s, dist)
	}

	testCases := []struct {
		desc      string
		qpsCounts map[state.StoreID]float64
		ticks     []int64
		expected  []map[state.StoreID]float64
	}{
		{
			desc: "balanced, no store rebalancer activity",
			qpsCounts: map[state.StoreID]float64{
				1: 1000, 2: 1000, 3: 1000,
			},
			ticks: []int64{5, 10, 15, 20},
			expected: []map[state.StoreID]float64{
				{1: 1000, 2: 1000, 3: 1000},
				{1: 1000, 2: 1000, 3: 1000},
				{1: 1000, 2: 1000, 3: 1000},
				{1: 1000, 2: 1000, 3: 1000},
			},
		},
		{
			desc: "underfull, no store rebalancer activity",
			qpsCounts: map[state.StoreID]float64{
				1: 1000, 2: 2000, 3: 2000,
			},
			ticks: []int64{5, 10, 15, 20},
			expected: []map[state.StoreID]float64{
				{1: 1000, 2: 2000, 3: 2000},
				{1: 1000, 2: 2000, 3: 2000},
				{1: 1000, 2: 2000, 3: 2000},
				{1: 1000, 2: 2000, 3: 2000},
			},
		},
		{
			desc: "overfull, transfer leases away",
			qpsCounts: map[state.StoreID]float64{
				1: 2000, 2: 1000, 3: 1000,
			},
			ticks: []int64{5, 10, 15, 20},
			expected: []map[state.StoreID]float64{
				{1: 2000, 2: 1000, 3: 1000},
				{1: 1750, 2: 1250, 3: 1000},
				{1: 1500, 2: 1250, 3: 1250},
				{1: 1500, 2: 1250, 3: 1250},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			s := state.NewStateWithDistribution([]float64{1, 0, 0}, 10, 3, 42, testSettings)
			// NB: The leaseholder distribution influences the QPS of ranges
			// when you wish to distribute QPS with a set count for each store.
			// e.g. having 5 vs 1 leaseholder results in the QPS being 100 per
			// range vs 500.  We are only interested in testing the store
			// rebalancer on s1 and wish for the QPS per range to be calculated
			// with 8 ranges on s1. Create 10 ranges, transfer the lease for a
			// range to s2 and s3. This results in the desired QPS per range on
			// s1 and the desired starting QPS counts on the other stores as
			// well.
			s.TransferLease(4, 2)
			s.TransferLease(5, 3)
			distributeQPS(s, tc.qpsCounts)

			gossip := gossip.NewGossip(s, testSettings)

			// Update the storepool for informing allocator decisions.
			gossip.Tick(ctx, start, s)

			allocator := s.MakeAllocator(testingStore)
			storePool := s.StorePool(testingStore)
			changer := state.NewReplicaChanger()
			controller := op.NewController(changer, allocator, storePool, testSettings, testingStore)
			src := newStoreRebalancerControl(start, testingStore, controller, allocator, storePool, testSettings, GetStateRaftStatusFn(s))
			s.TickClock(start)

			results := []map[state.StoreID]float64{}
			for _, tick := range tc.ticks {
				s.TickClock(state.OffsetTick(start, tick))
				changer.Tick(state.OffsetTick(start, tick), s)
				controller.Tick(ctx, state.OffsetTick(start, tick), s)
				gossip.Tick(ctx, state.OffsetTick(start, tick), s)
				src.Tick(ctx, state.OffsetTick(start, tick), s)

				results = append(results, testingGetStoreQPS(s))
			}
			require.Equal(t, tc.expected, results)
		})
	}
}
