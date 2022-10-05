// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package op

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestLeaseTransferOp(t *testing.T) {
	start := state.TestingStartTime()

	testCases := []struct {
		desc                 string
		ranges               int
		ticks                []int64
		transfers            map[int64][][]int64 /* tick -> [][rangeID, target] */
		expectedLeaseholders []map[state.RangeID]state.StoreID
	}{
		{
			desc:      "single range, single transfer (1->2)",
			ranges:    1,
			ticks:     []int64{0, 5},
			transfers: map[int64][][]int64{0: {{2, 2}}},
			expectedLeaseholders: []map[state.RangeID]state.StoreID{
				{2: 1},
				{2: 2},
			},
		},
		{
			desc:      "single range, 3 transfers (1->2->3->1)",
			ranges:    1,
			ticks:     []int64{0, 5, 10, 15},
			transfers: map[int64][][]int64{0: {{2, 2}}, 5: {{2, 3}}, 10: {{2, 1}}},
			expectedLeaseholders: []map[state.RangeID]state.StoreID{
				{2: 1},
				{2: 2},
				{2: 3},
				{2: 1},
			},
		},
		{
			desc:      "multi range, single transfer r2(1->2),r3(1->3)",
			ranges:    2,
			ticks:     []int64{0, 5},
			transfers: map[int64][][]int64{0: {{2, 2}, {3, 3}}},
			expectedLeaseholders: []map[state.RangeID]state.StoreID{
				{2: 1, 3: 1},
				{2: 2, 3: 3},
			},
		},
		{
			desc:      "multi range, multi transfer t0=[r2(1->2),r3(1->3)],t5[r2(2->3)],t10[r3(3->2)]",
			ranges:    2,
			ticks:     []int64{0, 5, 10, 15},
			transfers: map[int64][][]int64{0: {{2, 2}, {3, 3}}, 5: {{2, 3}}, 10: {{3, 2}}},
			expectedLeaseholders: []map[state.RangeID]state.StoreID{
				{2: 1, 3: 1},
				{2: 2, 3: 3},
				{2: 3, 3: 3},
				{2: 3, 3: 2},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			s := state.NewTestStateReplCounts(map[state.StoreID]int{1: tc.ranges + 1, 2: tc.ranges + 1, 3: tc.ranges + 1}, 3, 1000)
			settings := config.DefaultSimulationSettings()
			changer := state.NewReplicaChanger()
			controller := NewController(changer, allocatorimpl.Allocator{}, settings, 1 /* storeID ignored*/)

			for i := 2; i <= tc.ranges+1; i++ {
				s.TransferLease(state.RangeID(i), 1)
			}

			results := make([]map[state.RangeID]state.StoreID, len(tc.ticks))
			pending := []DispatchedTicket{}
			for i, tick := range tc.ticks {
				changer.Tick(state.OffsetTick(start, tick), s)
				controller.Tick(ctx, state.OffsetTick(start, tick), s)

				for _, transfers := range tc.transfers[tick] {
					rangeID, target := transfers[0], transfers[1]
					op := NewTransferLeaseOp(state.OffsetTick(start, tick), roachpb.RangeID(rangeID), 0, roachpb.StoreID(target), 0)
					ticket := controller.Dispatch(ctx, state.OffsetTick(start, tick), s, op)
					pending = append(pending, ticket)
				}

				tickResults := make(map[state.RangeID]state.StoreID)
				for rangeID := range tc.expectedLeaseholders[i] {
					leaseholderStore, ok := s.LeaseholderStore(rangeID)
					require.True(t, ok, "Unable to find leaseholder store for range %d", rangeID)
					tickResults[rangeID] = leaseholderStore.StoreID()
				}
				results[i] = tickResults
			}

			// Check all the pending relocations that we have sent, ensure that
			// they are completed and did not report an error.
			for _, ticket := range pending {
				op, ok := controller.Check(ticket)
				require.True(t, ok)
				done, _ := op.Done()
				require.True(t, done)
				require.NoError(t, op.Errors())

			}

			require.Equal(t, tc.expectedLeaseholders, results)
		})
	}

}

func TestRelocateRangeOp(t *testing.T) {
	start := state.TestingStartTime()

	settings := config.DefaultSimulationSettings()
	settings.ReplicaAddRate = 1
	settings.ReplicaChangeBaseDelay = 5 * time.Second
	settings.StateExchangeInterval = 1 * time.Second
	settings.StateExchangeDelay = 0

	type testRelocationArgs struct {
		voters               []state.StoreID
		transferToFirstVoter bool
	}

	type rangeState struct {
		voters      []int64
		leaseholder int64
	}

	// NB: All test start with the same configuration, 3 voters on stores
	// (1,2,3) w/ the leasholder on s1.
	testCases := []struct {
		desc          string
		ticks         []int64
		relocations   map[int64]map[state.RangeID]testRelocationArgs
		expectedState map[int64]map[state.RangeID]rangeState
	}{
		// Single range, single relocate range test cases.
		{
			desc:  "single change (1*,2,3)->(1*,2,4)",
			ticks: []int64{0, 5, 10, 15},
			relocations: map[int64]map[state.RangeID]testRelocationArgs{
				0: {2: {voters: []state.StoreID{1, 2, 4}, transferToFirstVoter: false}},
			},
			expectedState: map[int64]map[state.RangeID]rangeState{
				0:  {2: rangeState{voters: []int64{1, 2, 3}, leaseholder: 1}},
				15: {2: rangeState{voters: []int64{1, 2, 4}, leaseholder: 1}},
			},
		},
		{
			desc:  "single change, transfer (1*,2,3)->(1,2*,4)",
			ticks: []int64{0, 5, 10, 15},
			relocations: map[int64]map[state.RangeID]testRelocationArgs{
				0: {2: {voters: []state.StoreID{2, 1, 4}, transferToFirstVoter: true}},
			},
			expectedState: map[int64]map[state.RangeID]rangeState{
				0:  {2: rangeState{voters: []int64{1, 2, 3}, leaseholder: 1}},
				15: {2: rangeState{voters: []int64{1, 2, 4}, leaseholder: 2}},
			},
		},
		{
			desc:  "single change, remove lh, transfer (1*,2,3)->(2,3,4*)",
			ticks: []int64{0, 5, 10, 15},
			relocations: map[int64]map[state.RangeID]testRelocationArgs{
				0: {2: {voters: []state.StoreID{4, 3, 2}, transferToFirstVoter: true}},
			},
			expectedState: map[int64]map[state.RangeID]rangeState{
				0:  {2: rangeState{voters: []int64{1, 2, 3}, leaseholder: 1}},
				15: {2: rangeState{voters: []int64{2, 3, 4}, leaseholder: 4}},
			},
		},
		{
			desc:  "multi change, transfer (1*,2,3)->(4,5,6*)",
			ticks: []int64{0, 5, 10, 15, 20},
			relocations: map[int64]map[state.RangeID]testRelocationArgs{
				0: {2: {voters: []state.StoreID{6, 5, 4}, transferToFirstVoter: true}},
			},
			expectedState: map[int64]map[state.RangeID]rangeState{
				0:  {2: rangeState{voters: []int64{1, 2, 3}, leaseholder: 1}},
				20: {2: rangeState{voters: []int64{4, 5, 6}, leaseholder: 6}},
			},
		},
		// Single range, multi relocate range.
		{
			desc:  "2 relocates (1*,2,3)->(1,4*,6)->(2*,3,5)",
			ticks: []int64{0, 5, 10, 15, 20, 25, 30, 35, 40},
			relocations: map[int64]map[state.RangeID]testRelocationArgs{
				0:  {2: {voters: []state.StoreID{4, 1, 6}, transferToFirstVoter: true}},
				20: {2: {voters: []state.StoreID{2, 3, 5}, transferToFirstVoter: true}},
			},
			expectedState: map[int64]map[state.RangeID]rangeState{
				0:  {2: rangeState{voters: []int64{1, 2, 3}, leaseholder: 1}},
				20: {2: rangeState{voters: []int64{1, 4, 6}, leaseholder: 4}},
				40: {2: rangeState{voters: []int64{2, 3, 5}, leaseholder: 2}},
			},
		},
		// Multi range, single relocate range.
		{
			desc:  "r2(1*,2,3)->(4,5,6*), r3(1*,2,3)->(4,5,6*)",
			ticks: []int64{0, 5, 10, 15, 20, 25, 30},
			relocations: map[int64]map[state.RangeID]testRelocationArgs{
				0: {2: {voters: []state.StoreID{6, 5, 4}, transferToFirstVoter: true},
					3: {voters: []state.StoreID{6, 5, 4}, transferToFirstVoter: true}},
			},
			expectedState: map[int64]map[state.RangeID]rangeState{
				0: {
					2: rangeState{voters: []int64{1, 2, 3}, leaseholder: 1},
					3: rangeState{voters: []int64{1, 2, 3}, leaseholder: 1},
				},
				20: {
					2: rangeState{voters: []int64{4, 5, 6}, leaseholder: 6},
					3: rangeState{voters: []int64{4, 5, 6}, leaseholder: 6},
				},
			},
		},
		// Multi range, multi relocate range.
		{
			desc:  "r2(1*,2,3)->(1,4*,6)->(2*,3,5), r3(1*,2,3)->(1,4*,6)->(2*,3,5)",
			ticks: []int64{0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50},
			relocations: map[int64]map[state.RangeID]testRelocationArgs{
				0: {
					2: {voters: []state.StoreID{4, 1, 6}, transferToFirstVoter: true},
					3: {voters: []state.StoreID{4, 1, 6}, transferToFirstVoter: true},
				},
				20: {
					2: {voters: []state.StoreID{2, 3, 5}, transferToFirstVoter: true},
					3: {voters: []state.StoreID{2, 3, 5}, transferToFirstVoter: true},
				},
			},
			expectedState: map[int64]map[state.RangeID]rangeState{
				0: {
					2: rangeState{voters: []int64{1, 2, 3}, leaseholder: 1},
					3: rangeState{voters: []int64{1, 2, 3}, leaseholder: 1},
				},
				20: {
					2: rangeState{voters: []int64{1, 4, 6}, leaseholder: 4},
					3: rangeState{voters: []int64{1, 4, 6}, leaseholder: 4},
				},
				40: {
					2: rangeState{voters: []int64{2, 3, 5}, leaseholder: 2},
					3: rangeState{voters: []int64{2, 3, 5}, leaseholder: 2},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			s := state.NewTestStateReplCounts(map[state.StoreID]int{1: 3, 2: 3, 3: 3, 4: 0, 5: 0, 6: 0}, 3, 1000)
			changer := state.NewReplicaChanger()
			allocator := s.MakeAllocator(state.StoreID(1))
			controller := NewController(changer, allocator, settings, 1)

			// Transfer the lease to store 1 for all ranges.
			for i := 2; i < 4; i++ {
				store, ok := s.LeaseholderStore(state.RangeID(i))
				require.True(t, ok)
				if store.StoreID() != 1 {
					require.True(t, s.TransferLease(state.RangeID(i), 1),
						"Unable to transfer lease for range %d to store 1: state %s", i, s)
				}
			}

			gossip := gossip.NewStoreGossip(settings)
			gossip.Tick(start, s)

			results := map[int64]map[state.RangeID]rangeState{}
			pending := []DispatchedTicket{}
			for _, tick := range tc.ticks {
				// Tick pending changes and tick the range rebalancer - the
				// range rebalancer will fail if any pending changes that were
				// set to complete at tick t, still exist at tick t. So we tick
				// it first here.
				changer.Tick(state.OffsetTick(start, tick), s)
				controller.Tick(ctx, state.OffsetTick(start, tick), s)

				relocations := tc.relocations[tick]
				for rangeID, relocation := range relocations {
					voterTargets := []roachpb.ReplicationTarget{}
					for _, voter := range relocation.voters {
						voterTargets = append(voterTargets, roachpb.ReplicationTarget{StoreID: roachpb.StoreID(voter), NodeID: roachpb.NodeID(voter)})
					}
					rangeStartKey, _, ok := s.RangeSpan(rangeID)
					require.True(t, ok)

					op := NewRelocateRangeOp(
						state.OffsetTick(start, tick),
						rangeStartKey.ToRKey().AsRawKey(),
						voterTargets,
						[]roachpb.ReplicationTarget{},
						relocation.transferToFirstVoter,
					)

					ticket := controller.Dispatch(ctx, state.OffsetTick(start, tick), s, op)
					pending = append(pending, ticket)
				}

				if len(tc.expectedState[tick]) == 0 {
					continue
				}

				// If we expect some state at this tick, then record the state,
				// sorted by range id.
				tickResults := make(map[state.RangeID]rangeState)
				for rangeID := range tc.expectedState[tick] {
					rng, ok := s.Range(rangeID)
					require.True(t, ok)
					leaseholderStore, ok := s.LeaseholderStore(rangeID)
					require.True(t, ok)
					rState := rangeState{
						leaseholder: int64(leaseholderStore.StoreID()),
						voters:      []int64{},
					}
					for _, replica := range rng.Replicas() {
						rState.voters = append(rState.voters, int64(replica.StoreID()))
					}
					sort.Slice(rState.voters, func(i, j int) bool {
						return rState.voters[i] < rState.voters[j]
					})

					tickResults[rangeID] = rState
				}
				results[tick] = tickResults
			}

			// Check all the pending relocations that we have sent, ensure that
			// they are completed and did not report an error.
			for _, ticket := range pending {
				op, ok := controller.Check(ticket)
				require.True(t, ok)
				done, _ := op.Done()
				require.True(t, done)
				require.NoError(t, op.Errors())
			}

			// Assert that the state is expected.
			require.Equal(t, tc.expectedState, results)
		})
	}
}
