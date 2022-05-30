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

// TestReplicaStateChanger asserts that the replica changer maintains:
// (1) At most one pending change per range.
// (2) The timestamp returned from a change push is expected.
// (3) The changes are applied at timestamp returned.
func TestReplicaStateChanger(t *testing.T) {
	testingDelay := 5 * time.Second

	createTestState := func(replCounts map[StoreID]int) (State, Range) {
		state, ranges := NewTestStateReplCounts(replCounts)
		return state, ranges[0]
	}

	cng := func(addRemove ...StoreID) ReplicaChange {
		change := ReplicaChange{}
		change.Wait = testingDelay

		if len(addRemove) > 1 {
			change.Remove = addRemove[1]
		}
		if len(addRemove) > 0 {
			change.Add = addRemove[0]
		}
		return change
	}

	getReplLocations := func(state State, r Range) ([]int, []int) {
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
				5: cng(2, 1),
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
				5:  cng(2, 1),
				10: cng(3, 2),
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
				5:  cng(6, 1),
				10: cng(5, 2),
				15: cng(4, 3),
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
				5: cng(2, 1),
				6: cng(3, 1),
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
			state, r := createTestState(tc.initRepls)
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
				rmapView, storeView := getReplLocations(state, r)
				require.Equal(t, rmapView, storeView, "RangeMap state and the Store state have different values")
				results[tick] = rmapView
			}
			require.Equal(t, tc.expected, results)
			require.Equal(t, tc.expectedTimestamps, tsResults)
		})
	}
}
