// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

const testingDelay = 5 * time.Second

func testRC(storeID StoreID, changeType roachpb.ReplicaChangeType) kvpb.ReplicationChange {
	return kvpb.ReplicationChange{
		ChangeType: changeType,
		Target: roachpb.ReplicationTarget{
			NodeID:  roachpb.NodeID(storeID),
			StoreID: roachpb.StoreID(storeID),
		},
	}
}

func testMakeReplicaChange(rangeKey Key, chgs ...kvpb.ReplicationChange) func(s State) Change {
	return func(s State) Change {
		rng := s.RangeFor(rangeKey)
		change := ReplicaChange{}
		change.Wait = testingDelay
		change.Changes = chgs
		change.RangeID = rng.RangeID()
		change.Author = 1
		return &change
	}
}

func testMakeRangeSplitChange(splitKey Key) func(s State) Change {
	return func(s State) Change {
		rng := s.RangeFor(splitKey)
		change := RangeSplitChange{}
		change.Wait = testingDelay
		change.RangeID = rng.RangeID()
		change.SplitKey = splitKey
		change.Author = 1
		return &change
	}
}

func testMakeLeaseTransferChange(rangeKey Key, target StoreID) func(s State) Change {
	return func(s State) Change {
		return &LeaseTransferChange{
			Wait:           testingDelay,
			TransferTarget: target,
			RangeID:        s.RangeFor(rangeKey).RangeID(),
		}
	}
}

func testMakeRangeState(stores int, voters, nonVoters []StoreID) State {
	settings := config.DefaultSimulationSettings()
	clusterInfo := ClusterInfoWithStoreCount(stores, 1 /* storesPerNode */)
	s := LoadClusterInfo(clusterInfo, settings)
	LoadRangeInfo(s, RangeInfoWithReplicas(
		MinKey, voters, nonVoters, 1 /* leaseholder */, &defaultSpanConfig))
	return s
}

func testGetLHLocations(state State) map[int64]StoreID {
	leases := make(map[int64]StoreID)
	for _, rng := range state.Ranges() {
		rangeID := rng.RangeID()
		startKey, _, _ := state.RangeSpan(rangeID)
		lh, _ := state.LeaseholderStore(rangeID)
		leases[int64(startKey)] = lh.StoreID()
	}
	return leases
}

func stores(s ...StoreID) []StoreID {
	if len(s) == 0 {
		return []StoreID{}
	}
	return s
}

func testGetAllReplLocations(state State, replicaType roachpb.ReplicaType) map[int64][]StoreID {
	view := make(map[int64][]StoreID)
	for _, rng := range state.Ranges() {
		locations := testGetReplLocations(state, rng, replicaType)
		start, _, _ := state.RangeSpan(rng.RangeID())
		view[int64(start)] = locations
	}
	return view
}

func testGetReplLocations(state State, r Range, replicaType roachpb.ReplicaType) []StoreID {
	view := []StoreID{}
	desc := r.Descriptor()

	switch replicaType {
	case roachpb.VOTER_FULL:
		for _, voter := range desc.Replicas().VoterDescriptors() {
			view = append(view, StoreID(voter.StoreID))
		}
	case roachpb.NON_VOTER:
		for _, nonVoter := range desc.Replicas().NonVoterDescriptors() {
			view = append(view, StoreID(nonVoter.StoreID))
		}
	default:
		panic("Unsupported replica type for getting replica locations.")
	}

	sort.Slice(view, func(i, j int) bool {
		return view[i] < view[j]
	})
	return view
}

// TestReplicaChange asserts that:
// (1) changes either all succeed or all fail.
// (2) removes fail if it is the leaseholder.
// (3) add and removes fail if the removed store is the leaseholder and it
//
//	cannot be transferred to the added store.
//
// (4) In (3) the lease transfers when to the newly added store when possible.
func TestReplicaChange(t *testing.T) {
	testCases := []struct {
		desc                              string
		initRepls                         map[StoreID]int
		stores                            int
		initVoters, initNonVoters         []StoreID
		change                            func(s State) Change
		expectedVoters, expectedNonVoters []StoreID
		expectedLeaseholder               StoreID
	}{
		{
			desc:       "add replica",
			stores:     2,
			initVoters: stores(1),
			change: testMakeReplicaChange(2,
				testRC(2, roachpb.ADD_VOTER)),
			expectedVoters:      stores(1, 2),
			expectedNonVoters:   stores(),
			expectedLeaseholder: 1,
		},
		{
			desc:       "remove replica",
			stores:     2,
			initVoters: stores(1, 2),
			change: testMakeReplicaChange(2,
				testRC(2, roachpb.REMOVE_VOTER)),
			expectedVoters:      stores(1),
			expectedNonVoters:   stores(),
			expectedLeaseholder: 1,
		},
		{
			desc:       "move replica s1 -> s2, moves lease",
			stores:     2,
			initVoters: stores(1),
			change: testMakeReplicaChange(2,
				testRC(1, roachpb.REMOVE_VOTER),
				testRC(2, roachpb.ADD_VOTER)),
			expectedVoters:      stores(2),
			expectedNonVoters:   stores(),
			expectedLeaseholder: 2,
		},
		{
			desc:       "fails remove leaseholder",
			stores:     1,
			initVoters: stores(1),
			change: testMakeReplicaChange(2,
				testRC(1, roachpb.REMOVE_VOTER)),
			expectedVoters:      stores(1),
			expectedNonVoters:   stores(),
			expectedLeaseholder: 1,
		},
		{
			desc:       "fails remove no such store",
			stores:     2,
			initVoters: stores(1, 2),
			change: testMakeReplicaChange(2,
				testRC(5, roachpb.REMOVE_VOTER)),
			expectedVoters:      stores(1, 2),
			expectedNonVoters:   stores(),
			expectedLeaseholder: 1,
		},
		{
			desc:       "add non-voter s2",
			stores:     2,
			initVoters: stores(1),
			change: testMakeReplicaChange(2,
				testRC(2, roachpb.ADD_NON_VOTER)),
			expectedVoters:      stores(1),
			expectedNonVoters:   stores(2),
			expectedLeaseholder: 1,
		},
		{
			desc:          "remove non-voter",
			stores:        2,
			initVoters:    stores(1),
			initNonVoters: stores(2),
			change: testMakeReplicaChange(2,
				testRC(2, roachpb.REMOVE_NON_VOTER)),
			expectedVoters:      stores(1),
			expectedNonVoters:   stores(),
			expectedLeaseholder: 1,
		},
		{
			desc:          "promote non-voter s2",
			stores:        2,
			initVoters:    stores(1),
			initNonVoters: stores(2),
			change: testMakeReplicaChange(2,
				testRC(2, roachpb.REMOVE_NON_VOTER),
				testRC(2, roachpb.ADD_VOTER)),
			expectedVoters:      stores(1, 2),
			expectedNonVoters:   stores(),
			expectedLeaseholder: 1,
		},
		{
			desc:       "swap add s2 demote s1",
			stores:     2,
			initVoters: stores(1),
			change: testMakeReplicaChange(2,
				testRC(1, roachpb.REMOVE_VOTER),
				testRC(1, roachpb.ADD_NON_VOTER),
				testRC(2, roachpb.ADD_VOTER)),
			expectedVoters:      stores(2),
			expectedNonVoters:   stores(1),
			expectedLeaseholder: 2,
		},
		{
			desc:       "demote s2",
			stores:     2,
			initVoters: stores(1, 2),
			change: testMakeReplicaChange(2,
				testRC(2, roachpb.REMOVE_VOTER),
				testRC(2, roachpb.ADD_NON_VOTER)),
			expectedVoters:      stores(1),
			expectedNonVoters:   stores(2),
			expectedLeaseholder: 1,
		},
		{
			desc:       "demote s1 fails, no lh added",
			stores:     2,
			initVoters: stores(1, 2),
			change: testMakeReplicaChange(2,
				testRC(1, roachpb.REMOVE_VOTER),
				testRC(1, roachpb.ADD_NON_VOTER)),
			expectedVoters:      stores(1, 2),
			expectedNonVoters:   stores(),
			expectedLeaseholder: 1,
		},
		{
			desc:          "swap promote s2 demote s1",
			stores:        2,
			initVoters:    stores(1),
			initNonVoters: stores(2),
			change: testMakeReplicaChange(2,
				testRC(2, roachpb.REMOVE_NON_VOTER),
				testRC(2, roachpb.ADD_VOTER),
				testRC(1, roachpb.REMOVE_VOTER),
				testRC(1, roachpb.ADD_NON_VOTER)),
			expectedVoters:      stores(2),
			expectedNonVoters:   stores(1),
			expectedLeaseholder: 2,
		},
		{
			desc:       "remove non-voter fails when actually voter",
			stores:     2,
			initVoters: stores(1, 2),
			change: testMakeReplicaChange(2,
				testRC(2, roachpb.REMOVE_NON_VOTER),
				testRC(1, roachpb.REMOVE_VOTER),
				testRC(1, roachpb.ADD_NON_VOTER)),
			expectedVoters:      stores(1, 2),
			expectedNonVoters:   stores(),
			expectedLeaseholder: 1,
		},
		{
			desc:          "changes roll back - add non voter",
			stores:        3,
			initVoters:    stores(1),
			initNonVoters: stores(2),
			change: testMakeReplicaChange(2,
				testRC(3, roachpb.REMOVE_VOTER),
				testRC(1, roachpb.REMOVE_VOTER),
				testRC(1, roachpb.ADD_NON_VOTER)),
			expectedVoters:      stores(1),
			expectedNonVoters:   stores(2),
			expectedLeaseholder: 1,
		},
		{
			desc:          "changes roll back - complex",
			stores:        4,
			initVoters:    stores(1),
			initNonVoters: stores(2),
			change: testMakeReplicaChange(2,
				// In this change, we are:
				//  - Promoting non-voter on s2
				//  - Rebalancing the voter from s1 to s2, including lease transfer.
				//  - Removing non-voter on s4, which doesn't exist.
				// The changes should succeed until removing the non-voter, which is
				// checked last in ReplicaChange.Apply(). The changes should then be
				// rolled back fully.
				testRC(1, roachpb.REMOVE_VOTER),
				testRC(2, roachpb.ADD_VOTER),
				testRC(4, roachpb.REMOVE_NON_VOTER)),
			expectedVoters:      stores(1),
			expectedNonVoters:   stores(2),
			expectedLeaseholder: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			state := testMakeRangeState(tc.stores, tc.initVoters, tc.initNonVoters)
			r, _ := state.Range(1)
			state.TransferLease(r.RangeID(), StoreID(1))
			change := tc.change(state)
			change.Apply(state)
			voters := testGetReplLocations(state, r, roachpb.VOTER_FULL)
			nonVoters := testGetReplLocations(state, r, roachpb.NON_VOTER)
			leaseholder := StoreID(-1)
			for _, repl := range r.Replicas() {
				if repl.HoldsLease() {
					leaseholder = repl.StoreID()
				}
			}
			require.Equal(t, tc.expectedVoters, voters,
				"voter locations don't match expected")
			require.Equal(t, tc.expectedNonVoters, nonVoters,
				"non-voter locations don't match expected")
			require.Equal(t, tc.expectedLeaseholder, leaseholder,
				"leaseholder location doesn't match expected")
		})
	}
}

// TestReplicaStateChanger asserts that the replica changer maintains:
// (1) At most one pending change per range.
// (2) The timestamp returned from a change push is expected.
// (3) The changes are applied at timestamp returned.
func TestReplicaStateChanger(t *testing.T) {
	start := TestingStartTime()

	testCases := []struct {
		desc                              string
		stores                            int
		initVoters, initNonVoters         []StoreID
		ticks                             []int64
		pushes                            map[int64]func(s State) Change
		expectedVoters, expectedNonVoters map[int64]map[int64][]StoreID
		expectedLeaseholder               map[int64]map[int64]StoreID
		expectedTimestamps                []int64
	}{
		{
			desc:       "move s1 -> s2",
			stores:     2,
			initVoters: stores(1),
			ticks:      []int64{5, 10},
			pushes: map[int64]func(s State) Change{
				5: testMakeReplicaChange(2,
					testRC(1, roachpb.REMOVE_VOTER),
					testRC(2, roachpb.ADD_VOTER)),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1}},
				10: {0: {2}},
			},
			expectedLeaseholder: map[int64]map[int64]StoreID{
				5:  {0: 1},
				10: {0: 2},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:       "move s1 -> s2 -> s3",
			stores:     3,
			initVoters: stores(1),
			ticks:      []int64{5, 10, 15},
			pushes: map[int64]func(s State) Change{
				5: testMakeReplicaChange(2,
					testRC(1, roachpb.REMOVE_VOTER),
					testRC(2, roachpb.ADD_VOTER)),
				10: testMakeReplicaChange(2,
					testRC(2, roachpb.REMOVE_VOTER),
					testRC(3, roachpb.ADD_VOTER)),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1}},
				10: {0: {2}},
				15: {0: {3}},
			},
			expectedLeaseholder: map[int64]map[int64]StoreID{
				5:  {0: 1},
				10: {0: 2},
				15: {0: 3},
			},
			expectedTimestamps: []int64{10, 15},
		},
		{
			desc:       "move (complex) (1,2,3) -> (4,5,6)",
			stores:     6,
			initVoters: stores(1, 2, 3),
			ticks:      []int64{5, 10, 15, 20},
			pushes: map[int64]func(s State) Change{
				5: testMakeReplicaChange(2,
					testRC(1, roachpb.REMOVE_VOTER),
					testRC(6, roachpb.ADD_VOTER)),
				10: testMakeReplicaChange(2,
					testRC(2, roachpb.REMOVE_VOTER),
					testRC(5, roachpb.ADD_VOTER)),
				15: testMakeReplicaChange(2,
					testRC(3, roachpb.REMOVE_VOTER),
					testRC(4, roachpb.ADD_VOTER)),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1, 2, 3}},
				10: {0: {2, 3, 6}},
				15: {0: {3, 5, 6}},
				20: {0: {4, 5, 6}},
			},
			expectedLeaseholder: map[int64]map[int64]StoreID{
				5:  {0: 1},
				10: {0: 6},
				15: {0: 6},
				20: {0: 6},
			},
			expectedTimestamps: []int64{10, 15, 20},
		},
		{
			desc:       "non-allowed change during pending",
			stores:     3,
			initVoters: stores(1),
			ticks:      []int64{5, 6, 15},
			pushes: map[int64]func(s State) Change{
				// NB: change at tick 6 will be ignored as there is already a
				// pending change (1->2).
				5: testMakeReplicaChange(2,
					testRC(1, roachpb.REMOVE_VOTER),
					testRC(2, roachpb.ADD_VOTER)),
				6: testMakeReplicaChange(2,
					testRC(1, roachpb.REMOVE_VOTER),
					testRC(3, roachpb.ADD_VOTER)),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1}},
				6:  {0: {1}},
				15: {0: {2}},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:       "split range  [1] -> [1,100)[100,+)",
			stores:     3,
			initVoters: stores(1, 2, 3),
			ticks:      []int64{5, 10},
			pushes: map[int64]func(s State) Change{
				5: testMakeRangeSplitChange(100),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1, 2, 3}},
				10: {0: {1, 2, 3}, 100: {1, 2, 3}},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:       "split range  [1] -> [1,100)[100,+) -> [1,50)[50,100)[100,+)",
			stores:     3,
			initVoters: stores(1, 2, 3),
			ticks:      []int64{5, 10, 15},
			pushes: map[int64]func(s State) Change{
				5:  testMakeRangeSplitChange(100),
				10: testMakeRangeSplitChange(50),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1, 2, 3}},
				10: {0: {1, 2, 3}, 100: {1, 2, 3}},
				15: {0: {1, 2, 3}, 50: {1, 2, 3}, 100: {1, 2, 3}},
			},
			expectedTimestamps: []int64{10, 15},
		},
		{
			desc:       "split range  [1] -> [1,100)[100,+), move replica [100,+):s1 -> s4",
			stores:     4,
			initVoters: stores(1, 2, 3),
			ticks:      []int64{5, 10, 15},
			pushes: map[int64]func(s State) Change{
				5: testMakeRangeSplitChange(100),
				10: testMakeReplicaChange(100,
					testRC(1, roachpb.REMOVE_VOTER),
					testRC(4, roachpb.ADD_VOTER)),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1, 2, 3}},
				10: {0: {1, 2, 3}, 100: {1, 2, 3}},
				15: {0: {1, 2, 3}, 100: {2, 3, 4}},
			},
			expectedTimestamps: []int64{10, 15},
		},
		{
			desc:       "overlapping split -> replica changes are blocked",
			stores:     2,
			initVoters: stores(1),
			ticks:      []int64{5, 6, 15},
			pushes: map[int64]func(s State) Change{
				// NB: Two changes affect the same range (move & split), only
				// one concurrent change per range may be enqueued at any time.
				5: testMakeRangeSplitChange(100),
				6: testMakeReplicaChange(100,
					testRC(1, roachpb.REMOVE_VOTER),
					testRC(2, roachpb.ADD_VOTER)),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1}},
				6:  {0: {1}},
				15: {0: {1}, 100: {1}},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:       "overlapping replica -> split changes are blocked",
			stores:     2,
			initVoters: stores(1),
			ticks:      []int64{5, 6, 15},
			pushes: map[int64]func(s State) Change{
				// NB: Two changes affect the same range (move & split), only
				// one concurrent change per range may be enqueued at any time.
				5: testMakeReplicaChange(100,
					testRC(1, roachpb.REMOVE_VOTER),
					testRC(2, roachpb.ADD_VOTER)),
				6: testMakeRangeSplitChange(100),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1}},
				6:  {0: {1}},
				15: {0: {2}},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:       "range splits don't block on the leaseholder store's other changes",
			stores:     2,
			initVoters: stores(1),
			ticks:      []int64{5, 10, 11, 20},
			pushes: map[int64]func(s State) Change{
				// NB: The change at tick 10 and 11 overlap in their "delay"
				// but on the same leaseholder store. Splits do not block
				// on replica changes, so it can go through.
				5: testMakeRangeSplitChange(100),
				10: testMakeReplicaChange(0,
					testRC(1, roachpb.REMOVE_VOTER),
					testRC(2, roachpb.ADD_VOTER)),
				11: testMakeRangeSplitChange(200),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1}},
				10: {0: {1}, 100: {1}},
				11: {0: {1}, 100: {1}},
				20: {0: {2}, 100: {1}, 200: {1}},
			},
			expectedTimestamps: []int64{10, 15, 16},
		},
		{
			desc:       "transfer lease (1,2,3) 1 -> 3",
			stores:     3,
			initVoters: stores(1, 2, 3),
			ticks:      []int64{5, 10},
			pushes: map[int64]func(s State) Change{
				5: testMakeLeaseTransferChange(100, 3),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1, 2, 3}},
				10: {0: {1, 2, 3}},
			},
			expectedLeaseholder: map[int64]map[int64]StoreID{
				5:  {0: 1},
				10: {0: 3},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:       "transfer lease (1,2,3) 1 -> 2 -> 3",
			stores:     3,
			initVoters: stores(1, 2, 3),
			ticks:      []int64{5, 10, 15},
			pushes: map[int64]func(s State) Change{
				5:  testMakeLeaseTransferChange(100, 2),
				10: testMakeLeaseTransferChange(100, 3),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1, 2, 3}},
				10: {0: {1, 2, 3}},
				15: {0: {1, 2, 3}},
			},
			expectedLeaseholder: map[int64]map[int64]StoreID{
				5:  {0: 1},
				10: {0: 2},
				15: {0: 3},
			},
			expectedTimestamps: []int64{10, 15},
		},
		{
			desc:       "",
			stores:     3,
			initVoters: stores(1, 2, 3),
			ticks:      []int64{5, 10, 15},
			pushes: map[int64]func(s State) Change{
				5:  testMakeLeaseTransferChange(100, 2),
				10: testMakeLeaseTransferChange(100, 3),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1, 2, 3}},
				10: {0: {1, 2, 3}},
				15: {0: {1, 2, 3}},
			},
			expectedLeaseholder: map[int64]map[int64]StoreID{
				5:  {0: 1},
				10: {0: 2},
				15: {0: 3},
			},
			expectedTimestamps: []int64{10, 15},
		},
		{
			desc:       "overlapping lh and split changes are blocked",
			stores:     3,
			initVoters: stores(1, 2, 3),
			ticks:      []int64{5, 6, 15},
			pushes: map[int64]func(s State) Change{
				// NB: Two changes affect the same range (move & transfer), only
				// one concurrent change per range may be enqueued at any time.
				5: testMakeLeaseTransferChange(100, 2),
				6: testMakeRangeSplitChange(100),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1, 2, 3}},
				6:  {0: {1, 2, 3}},
				15: {0: {1, 2, 3}},
			},
			expectedLeaseholder: map[int64]map[int64]StoreID{
				5:  {0: 1},
				6:  {0: 1},
				15: {0: 2},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:       "overlapping lh and split changes are blocked",
			stores:     3,
			initVoters: stores(1, 2, 3),
			ticks:      []int64{5, 6, 15},
			pushes: map[int64]func(s State) Change{
				// NB: Two changes affect the same range (move & transfer), only
				// one concurrent change per range may be enqueued at any time.
				5: testMakeLeaseTransferChange(100, 2),
				6: testMakeRangeSplitChange(100),
			},
			expectedVoters: map[int64]map[int64][]StoreID{
				5:  {0: {1, 2, 3}},
				6:  {0: {1, 2, 3}},
				15: {0: {1, 2, 3}},
			},
			expectedLeaseholder: map[int64]map[int64]StoreID{
				5:  {0: 1},
				6:  {0: 1},
				15: {0: 2},
			},
			expectedTimestamps: []int64{10},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			changer := NewReplicaChanger()
			state := testMakeRangeState(tc.stores, tc.initVoters, tc.initNonVoters)
			resultsVoters := make(map[int64]map[int64][]StoreID)
			resultsNonVoters := make(map[int64]map[int64][]StoreID)
			tsResults := make([]int64, 0, 1)
			resultLeaseholders := make(map[int64]map[int64]StoreID)

			for _, tick := range tc.ticks {
				changer.Tick(OffsetTick(start, tick), state)
				if change, ok := tc.pushes[tick]; ok {
					if ts, ok := changer.Push(OffsetTick(start, tick), change(state)); ok {
						tsResults = append(tsResults, ReverseOffsetTick(start, ts))
					}
				}
				voters := testGetAllReplLocations(state, roachpb.VOTER_FULL)
				nonVoters := testGetAllReplLocations(state, roachpb.NON_VOTER)
				resultsVoters[tick] = voters
				resultsNonVoters[tick] = nonVoters
				resultLeaseholders[tick] = testGetLHLocations(state)
			}

			require.Equal(t, tc.expectedVoters, resultsVoters)
			require.Equal(t, tc.expectedTimestamps, tsResults)
			if tc.expectedLeaseholder != nil {
				require.Equal(t, tc.expectedLeaseholder, resultLeaseholders)
			}
			if tc.expectedNonVoters != nil {
				require.Equal(t, tc.expectedNonVoters, resultsNonVoters)
			}
		})
	}
}
