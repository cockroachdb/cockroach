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

func testMakeReplicaChange(add, remove StoreID) ReplicaChange {
	change := ReplicaChange{}
	change.Wait = testingDelay
	change.Add = add
	change.Remove = remove
	return change
}

func testMakeReplicaState(replCounts map[StoreID]int) (State, Range) {
	state := NewTestStateReplCounts(replCounts, 3 /* replsPerRange */)
	// The first range has ID 1, this is the initial range in the keyspace.
	// We split that and use the rhs, range 2.
	rng, _ := state.Range(RangeID(2))
	return state, rng
}

func testGetReplLocations(state State, r Range) ([]int, []int) {
	storeView := []int{}
	for _, store := range state.Stores() {
		if _, ok := store.Replicas()[r.RangeID()]; ok {
			storeView = append(storeView, int(store.StoreID()))
		}
	}

	rmapView := []int{}
	for storeID := range r.Replicas() {
		rmapView = append(rmapView, int(storeID))
	}

	sort.Ints(rmapView)
	sort.Ints(storeView)
	return rmapView, storeView
}

// TestReplicaChange asserts that:
// (1) changes either all succeed or all fail.
// (2) removes fail if it is the leaseholder.
// (3) add and removes fail if the removed store is the leaseholder and it
//     cannot be transferred to the added store.
// (4) In (3) the lease transfers when to the newly added store when possible.
func TestReplicaChange(t *testing.T) {
	testCases := []struct {
		desc                string
		initRepls           map[StoreID]int
		initLease           int
		change              ReplicaChange
		expectedReplicas    []int
		expectedLeaseholder int
	}{
		{
			desc:                "add replica",
			initRepls:           map[StoreID]int{1: 1, 2: 0},
			initLease:           1,
			change:              testMakeReplicaChange(2, 0),
			expectedReplicas:    []int{1, 2},
			expectedLeaseholder: 1,
		},
		{
			desc:                "remove replica",
			initRepls:           map[StoreID]int{1: 1, 2: 1},
			initLease:           1,
			change:              testMakeReplicaChange(0, 2),
			expectedReplicas:    []int{1},
			expectedLeaseholder: 1,
		},
		{
			desc:                "move replica s2 -> s3",
			initRepls:           map[StoreID]int{1: 1, 2: 1, 3: 0},
			initLease:           1,
			change:              testMakeReplicaChange(3, 2),
			expectedReplicas:    []int{1, 3},
			expectedLeaseholder: 1,
		},
		{
			desc:                "move replica s1 -> s2, moves lease",
			initRepls:           map[StoreID]int{1: 1, 2: 0},
			initLease:           1,
			change:              testMakeReplicaChange(2, 1),
			expectedReplicas:    []int{2},
			expectedLeaseholder: 2,
		},
		{
			desc:                "fails remove leaseholder",
			initRepls:           map[StoreID]int{1: 1},
			initLease:           1,
			change:              testMakeReplicaChange(0, 1),
			expectedReplicas:    []int{1},
			expectedLeaseholder: 1,
		},
		{
			desc:                "fails remove no such store",
			initRepls:           map[StoreID]int{1: 1, 2: 1},
			initLease:           1,
			change:              testMakeReplicaChange(0, 5),
			expectedReplicas:    []int{1, 2},
			expectedLeaseholder: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			state, r := testMakeReplicaState(tc.initRepls)
			state.TransferLease(r.RangeID(), StoreID(tc.initLease))
			tc.change.RangeID = r.RangeID()
			tc.change.Apply(state)
			replLocations, _ := testGetReplLocations(state, r)
			leaseholder := -1
			for storeID, repl := range r.Replicas() {
				if repl.HoldsLease() {
					leaseholder = int(storeID)
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
		desc               string
		initRepls          map[StoreID]int
		ticks              []int64
		pushes             map[int64]ReplicaChange
		expected           map[int64][]int
		expectedTimestamps []int64
	}{
		{
			desc:      "move s1 -> s2",
			initRepls: map[StoreID]int{1: 1, 2: 0},
			ticks:     []int64{5, 10},
			pushes: map[int64]ReplicaChange{
				5: testMakeReplicaChange(2, 1),
			},
			expected: map[int64][]int{
				5:  {1},
				10: {2},
			},
			expectedTimestamps: []int64{10},
		},
		{
			desc:      "move s1 -> s2 -> s3",
			initRepls: map[StoreID]int{1: 1, 2: 0, 3: 0},
			ticks:     []int64{5, 10, 15},
			pushes: map[int64]ReplicaChange{
				5:  testMakeReplicaChange(2, 1),
				10: testMakeReplicaChange(3, 2),
			},
			expected: map[int64][]int{
				5:  {1},
				10: {2},
				15: {3},
			},
			expectedTimestamps: []int64{10, 15},
		},
		{
			desc:      "move (complex) (1,2,3) -> (4,5,6)",
			initRepls: map[StoreID]int{1: 1, 2: 1, 3: 1, 4: 0, 5: 0, 6: 0},
			ticks:     []int64{5, 10, 15, 20},
			pushes: map[int64]ReplicaChange{
				5:  testMakeReplicaChange(6, 1),
				10: testMakeReplicaChange(5, 2),
				15: testMakeReplicaChange(4, 3),
			},
			expected: map[int64][]int{
				5:  {1, 2, 3},
				10: {2, 3, 6},
				15: {3, 5, 6},
				20: {4, 5, 6},
			},
			expectedTimestamps: []int64{10, 15, 20},
		},
		{
			desc:      "non-allowed change during pending",
			initRepls: map[StoreID]int{1: 1, 2: 0, 3: 0},
			ticks:     []int64{5, 6, 15},
			pushes: map[int64]ReplicaChange{
				// NB: change at tick 6 will be ignored as there is already a
				// pending change (1->2).
				5: testMakeReplicaChange(2, 1),
				6: testMakeReplicaChange(3, 1),
			},
			expected: map[int64][]int{
				5:  {1},
				6:  {1},
				15: {2},
			},
			expectedTimestamps: []int64{10},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			changer := NewReplicaChanger()
			state, r := testMakeReplicaState(tc.initRepls)
			results := make(map[int64][]int)
			tsResults := make([]int64, 0, 1)

			for _, tick := range tc.ticks {
				changer.Tick(OffsetTick(start, tick), state)
				if change, ok := tc.pushes[tick]; ok {
					change.RangeID = r.RangeID()
					if ts, ok := changer.Push(OffsetTick(start, tick), &change); ok {
						tsResults = append(tsResults, ReverseOffsetTick(start, ts))
					}
				}
				rmapView, storeView := testGetReplLocations(state, r)
				require.Equal(t, rmapView, storeView, "RangeMap state and the Store state have different values")
				results[tick] = rmapView
			}
			require.Equal(t, tc.expected, results)
			require.Equal(t, tc.expectedTimestamps, tsResults)
		})
	}
}
