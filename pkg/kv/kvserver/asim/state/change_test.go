// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package state

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testingDelay = 5 * time.Second

func testMakeReplicaChange(add, remove StoreID, rangeKey Key) func(s State) Change {
	return func(s State) Change {
		rng := s.RangeFor(rangeKey)
		change := ReplicaChange{}
		change.Wait = testingDelay
		change.Add = add
		change.Remove = remove
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
			Author:         1,
		}
	}
}

func testMakeReplicaState(replCounts map[StoreID]int) (State, Range) {
	numReplicas := 0
	for _, count := range replCounts {
		numReplicas += count
	}

	state := NewTestStateReplCounts(replCounts, numReplicas, 50 /* keyspace */)
	rng, _ := state.Range(RangeID(2))
	return state, rng
}

func testGetAllReplLocations(
	state State, excludedRanges map[RangeID]bool,
) (map[int64][]int, map[int64][]int) {
	rmapView := make(map[int64][]int)
	storeView := make(map[int64][]int)
	for _, rng := range state.Ranges() {
		if _, ok := excludedRanges[rng.RangeID()]; ok {
			continue
		}
		rmap, stores := testGetReplLocations(state, rng)
		start, _, _ := state.RangeSpan(rng.RangeID())
		rmapView[int64(start)] = rmap
		storeView[int64(start)] = stores
	}
	return rmapView, storeView
}

func testGetLHLocations(state State, excludedRanges map[RangeID]bool) map[int64]int {
	leases := make(map[int64]int)
	for _, rng := range state.Ranges() {
		if _, ok := excludedRanges[rng.RangeID()]; ok {
			continue
		}
		startKey, _, _ := state.RangeSpan(rng.RangeID())
		lh, _ := state.LeaseholderStore(rng.RangeID())
		leases[int64(startKey)] = int(lh.StoreID())
	}
	return leases
}

func testGetReplLocations(state State, r Range) ([]int, []int) {
	storeView := []int{}
	for _, store := range state.Stores() {
		if _, ok := store.Replica(r.RangeID()); ok {
			storeView = append(storeView, int(store.StoreID()))
		}
	}

	rmapView := []int{}
	for _, replica := range r.Replicas() {
		rmapView = append(rmapView, int(replica.StoreID()))
	}

	sort.Ints(rmapView)
	sort.Ints(storeView)
	return rmapView, storeView
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
		desc                string
		initRepls           map[StoreID]int
		initLease           int
		change              func(s State) Change
		expectedReplicas    []int
		expectedLeaseholder int
	}{
		{
			desc:                "add replica",
			initRepls:           map[StoreID]int{1: 1, 2: 0},
			initLease:           1,
			change:              testMakeReplicaChange(2, 0, 2),
			expectedReplicas:    []int{1, 2},
			expectedLeaseholder: 1,
		},
		{
			desc:                "remove replica",
			initRepls:           map[StoreID]int{1: 1, 2: 1},
			initLease:           1,
			change:              testMakeReplicaChange(0, 2, 2),
			expectedReplicas:    []int{1},
			expectedLeaseholder: 1,
		},
		{
			desc:                "move replica s2 -> s3",
			initRepls:           map[StoreID]int{1: 1, 2: 1, 3: 0},
			initLease:           1,
			change:              testMakeReplicaChange(3, 2, 2),
			expectedReplicas:    []int{1, 3},
			expectedLeaseholder: 1,
		},
		{
			desc:                "move replica s1 -> s2, moves lease",
			initRepls:           map[StoreID]int{1: 1, 2: 0},
			initLease:           1,
			change:              testMakeReplicaChange(2, 1, 2),
			expectedReplicas:    []int{2},
			expectedLeaseholder: 2,
		},
		{
			desc:                "fails remove leaseholder",
			initRepls:           map[StoreID]int{1: 1},
			initLease:           1,
			change:              testMakeReplicaChange(0, 1, 2),
			expectedReplicas:    []int{1},
			expectedLeaseholder: 1,
		},
		{
			desc:                "fails remove no such store",
			initRepls:           map[StoreID]int{1: 1, 2: 1},
			initLease:           1,
			change:              testMakeReplicaChange(0, 5, 2),
			expectedReplicas:    []int{1, 2},
			expectedLeaseholder: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			state, r := testMakeReplicaState(tc.initRepls)
			state.TransferLease(r.RangeID(), StoreID(tc.initLease))
			change := tc.change(state)
			change.Apply(state)
			replLocations, _ := testGetReplLocations(state, r)
			leaseholder := -1
			for _, repl := range r.Replicas() {
				if repl.HoldsLease() {
					leaseholder = int(repl.StoreID())
				}
			}
			require.Equal(t, tc.expectedReplicas, replLocations)
			require.Equal(t, tc.expectedLeaseholder, leaseholder)
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
		desc                string
		initRepls           map[StoreID]int
		ticks               []int64
		pushes              map[int64]func(s State) Change
		expected            map[int64]map[int64][]int
		expectedLeaseholder map[int64]map[int64]int
		expectedTimestamps  []int64
	}{
		{
			desc:      "move s1 -> s2",
			initRepls: map[StoreID]int{1: 1, 2: 0},
			ticks:     []int64{5, 10},
			pushes: map[int64]func(s State) Change{
				5: testMakeReplicaChange(2, 1, 2),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1}},
				10: {0: {2}},
			},
			expectedLeaseholder: map[int64]map[int64]int{
				5:  {0: 1},
				10: {0: 2},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:      "move s1 -> s2 -> s3",
			initRepls: map[StoreID]int{1: 1, 2: 0, 3: 0},
			ticks:     []int64{5, 10, 15},
			pushes: map[int64]func(s State) Change{
				5:  testMakeReplicaChange(2, 1, 2),
				10: testMakeReplicaChange(3, 2, 2),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1}},
				10: {0: {2}},
				15: {0: {3}},
			},
			expectedLeaseholder: map[int64]map[int64]int{
				5:  {0: 1},
				10: {0: 2},
				15: {0: 3},
			},
			expectedTimestamps: []int64{10, 15},
		},
		{
			desc:      "move (complex) (1,2,3) -> (4,5,6)",
			initRepls: map[StoreID]int{1: 1, 2: 1, 3: 1, 4: 0, 5: 0, 6: 0},
			ticks:     []int64{5, 10, 15, 20},
			pushes: map[int64]func(s State) Change{
				5:  testMakeReplicaChange(6, 1, 2),
				10: testMakeReplicaChange(5, 2, 2),
				15: testMakeReplicaChange(4, 3, 2),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1, 2, 3}},
				10: {0: {2, 3, 6}},
				15: {0: {3, 5, 6}},
				20: {0: {4, 5, 6}},
			},
			expectedLeaseholder: map[int64]map[int64]int{
				5:  {0: 1},
				10: {0: 6},
				15: {0: 6},
				20: {0: 6},
			},
			expectedTimestamps: []int64{10, 15, 20},
		},
		{
			desc:      "non-allowed change during pending",
			initRepls: map[StoreID]int{1: 1, 2: 0, 3: 0},
			ticks:     []int64{5, 6, 15},
			pushes: map[int64]func(s State) Change{
				// NB: change at tick 6 will be ignored as there is already a
				// pending change (1->2).
				5: testMakeReplicaChange(2, 1, 2),
				6: testMakeReplicaChange(3, 1, 2),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1}},
				6:  {0: {1}},
				15: {0: {2}},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:      "split range  [1] -> [1,100)[100,+)",
			initRepls: map[StoreID]int{1: 1, 2: 1, 3: 1},
			ticks:     []int64{5, 10},
			pushes: map[int64]func(s State) Change{
				5: testMakeRangeSplitChange(100),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1, 2, 3}},
				10: {0: {1, 2, 3}, 100: {1, 2, 3}},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:      "split range  [1] -> [1,100)[100,+) -> [1,50)[50,100)[100,+)",
			initRepls: map[StoreID]int{1: 1, 2: 1, 3: 1},
			ticks:     []int64{5, 10, 15},
			pushes: map[int64]func(s State) Change{
				5:  testMakeRangeSplitChange(100),
				10: testMakeRangeSplitChange(50),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1, 2, 3}},
				10: {0: {1, 2, 3}, 100: {1, 2, 3}},
				15: {0: {1, 2, 3}, 50: {1, 2, 3}, 100: {1, 2, 3}},
			},
			expectedTimestamps: []int64{10, 15},
		},
		{
			desc:      "split range  [1] -> [1,100)[100,+), move replica [100,+):s1 -> s4",
			initRepls: map[StoreID]int{1: 1, 2: 1, 3: 1, 4: 0},
			ticks:     []int64{5, 10, 15},
			pushes: map[int64]func(s State) Change{
				5:  testMakeRangeSplitChange(100),
				10: testMakeReplicaChange(4, 1, 100),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1, 2, 3}},
				10: {0: {1, 2, 3}, 100: {1, 2, 3}},
				15: {0: {1, 2, 3}, 100: {2, 3, 4}},
			},
			expectedTimestamps: []int64{10, 15},
		},
		{
			desc:      "overlapping split -> replica changes are blocked",
			initRepls: map[StoreID]int{1: 1, 2: 0},
			ticks:     []int64{5, 6, 15},
			pushes: map[int64]func(s State) Change{
				// NB: Two changes affect the same range (move & split), only
				// one concurrent change per range may be enqueued at any time.
				5: testMakeRangeSplitChange(100),
				6: testMakeReplicaChange(2, 1, 100),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1}},
				6:  {0: {1}},
				15: {0: {1}, 100: {1}},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:      "overlapping replica -> split changes are blocked",
			initRepls: map[StoreID]int{1: 1, 2: 0},
			ticks:     []int64{5, 6, 15},
			pushes: map[int64]func(s State) Change{
				// NB: Two changes affect the same range (move & split), only
				// one concurrent change per range may be enqueued at any time.
				5: testMakeReplicaChange(2, 1, 100),
				6: testMakeRangeSplitChange(100),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1}},
				6:  {0: {1}},
				15: {0: {2}},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:      "range splits don't block on the leaseholder store's other changes",
			initRepls: map[StoreID]int{1: 1, 2: 0},
			ticks:     []int64{5, 10, 11, 20},
			pushes: map[int64]func(s State) Change{
				// NB: The change at tick 10 and 11 overlap in their "delay"
				// but on the same leaseholder store. Splits do not block
				// on replica changes, so it can go through.
				5:  testMakeRangeSplitChange(100),
				10: testMakeReplicaChange(2, 1, 0),
				11: testMakeRangeSplitChange(200),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1}},
				10: {0: {1}, 100: {1}},
				11: {0: {1}, 100: {1}},
				20: {0: {2}, 100: {1}, 200: {1}},
			},
			expectedTimestamps: []int64{10, 15, 16},
		},
		{
			desc:      "transfer lease (1,2,3) 1 -> 3",
			initRepls: map[StoreID]int{1: 1, 2: 1, 3: 1},
			ticks:     []int64{5, 10},
			pushes: map[int64]func(s State) Change{
				5: testMakeLeaseTransferChange(100, 3),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1, 2, 3}},
				10: {0: {1, 2, 3}},
			},
			expectedLeaseholder: map[int64]map[int64]int{
				5:  {0: 1},
				10: {0: 3},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:      "transfer lease (1,2,3) 1 -> 2 -> 3",
			initRepls: map[StoreID]int{1: 1, 2: 1, 3: 1},
			ticks:     []int64{5, 10, 15},
			pushes: map[int64]func(s State) Change{
				5:  testMakeLeaseTransferChange(100, 2),
				10: testMakeLeaseTransferChange(100, 3),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1, 2, 3}},
				10: {0: {1, 2, 3}},
				15: {0: {1, 2, 3}},
			},
			expectedLeaseholder: map[int64]map[int64]int{
				5:  {0: 1},
				10: {0: 2},
				15: {0: 3},
			},
			expectedTimestamps: []int64{10, 15},
		},
		{
			desc:      "",
			initRepls: map[StoreID]int{1: 1, 2: 1, 3: 1},
			ticks:     []int64{5, 10, 15},
			pushes: map[int64]func(s State) Change{
				5:  testMakeLeaseTransferChange(100, 2),
				10: testMakeLeaseTransferChange(100, 3),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1, 2, 3}},
				10: {0: {1, 2, 3}},
				15: {0: {1, 2, 3}},
			},
			expectedLeaseholder: map[int64]map[int64]int{
				5:  {0: 1},
				10: {0: 2},
				15: {0: 3},
			},
			expectedTimestamps: []int64{10, 15},
		},
		{
			desc:      "overlapping lh and split changes are blocked",
			initRepls: map[StoreID]int{1: 1, 2: 1, 3: 1},
			ticks:     []int64{5, 6, 15},
			pushes: map[int64]func(s State) Change{
				// NB: Two changes affect the same range (move & transfer), only
				// one concurrent change per range may be enqueued at any time.
				5: testMakeLeaseTransferChange(100, 2),
				6: testMakeRangeSplitChange(100),
			},
			expected: map[int64]map[int64][]int{
				5:  {0: {1, 2, 3}},
				6:  {0: {1, 2, 3}},
				15: {0: {1, 2, 3}},
			},
			expectedLeaseholder: map[int64]map[int64]int{
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
			state, _ := testMakeReplicaState(tc.initRepls)
			results := make(map[int64]map[int64][]int)
			tsResults := make([]int64, 0, 1)
			resultLeaseholders := make(map[int64]map[int64]int)

			for _, tick := range tc.ticks {
				changer.Tick(OffsetTick(start, tick), state)
				if change, ok := tc.pushes[tick]; ok {
					if ts, ok := changer.Push(OffsetTick(start, tick), change(state)); ok {
						tsResults = append(tsResults, ReverseOffsetTick(start, ts))
					}
				}
				rmapView, storeView := testGetAllReplLocations(state, map[RangeID]bool{1: true})
				require.Equal(t, rmapView, storeView, "RangeMap state and the Store state have different values")
				results[tick] = rmapView
				resultLeaseholders[tick] = testGetLHLocations(state, map[RangeID]bool{1: true})
			}

			require.Equal(t, tc.expected, results)
			require.Equal(t, tc.expectedTimestamps, tsResults)
			if tc.expectedLeaseholder != nil {
				require.Equal(t, tc.expectedLeaseholder, resultLeaseholders)
			}
		})
	}
}
