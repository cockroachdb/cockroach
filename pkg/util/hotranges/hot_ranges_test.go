// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package hotranges

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// constructHotReplicas generates a slice of random HotReplicas
// based on the inputted size. It should be used for performance testing.
func constructHotReplicas(amount int) []HotReplica {
	hotReplicas := make([]HotReplica, 0)
	key := 0
	for len(hotReplicas) < amount {
		qps := rand.Float64() * 100
		if qps > 50 {
			key++
		}
		hotReplicas = append(hotReplicas, HotReplica{
			qps:      qps,
			startKey: strconv.Itoa(key),
			endKey:   strconv.Itoa(key + 1),
		})
		key++
	}
	return hotReplicas
}

func TestValidateGroupRangesCandidate(t *testing.T) {
	candidate := []HotReplica{
		{
			qps:      20,
			startKey: "1",
			endKey:   "2",
		},
		{
			qps:      10,
			startKey: "2",
			endKey:   "3",
		},
		{
			qps:      30,
			startKey: "3",
			endKey:   "4",
		},
	}
	// Verify odd calculation.
	require.Equal(t, false, validateGroupRangesCandidate(candidate, 21))
	require.Equal(t, true, validateGroupRangesCandidate(candidate, 19))

	candidate = append(candidate, HotReplica{qps: 15, startKey: "4", endKey: "5"})

	// Verify even calculation.
	require.Equal(t, false, validateGroupRangesCandidate(candidate, 18))
	require.Equal(t, false, validateGroupRangesCandidate(candidate, 19))
	// Ensure the original order of slice is maintained
	require.Equal(t, HotReplica{qps: 20, startKey: "1", endKey: "2"}, candidate[0])
}

func TestFindGroupedRangesCandidate(t *testing.T) {
	groupedRanges := map[int][]HotReplica{
		1: {
			{
				qps:      20,
				startKey: "1",
				endKey:   "2",
			},
			{
				qps:      10,
				startKey: "2",
				endKey:   "3",
			},
			{
				qps:      30,
				startKey: "3",
				endKey:   "4",
			},
		},
		2: {
			{
				qps:      40,
				startKey: "5",
				endKey:   "6",
			},
			{
				qps:      50,
				startKey: "6",
				endKey:   "7",
			},
		},
		3: {
			{
				qps:      70,
				startKey: "8",
				endKey:   "9",
			},
		},
	}

	// Verify typical case.
	resKey, resQPS := findGroupedRangesCandidate(groupedRanges)
	require.Equal(t, 1, resKey)
	require.Equal(t, float64(20), resQPS)

	groupedRanges = map[int][]HotReplica{
		1: {
			{
				qps:      5,
				startKey: "1",
				endKey:   "2",
			},
		},
		2: {
			{
				qps:      10,
				startKey: "3",
				endKey:   "4",
			},
		},
	}
	// Verify edge case.
	resKey, resQPS = findGroupedRangesCandidate(groupedRanges)
	require.Equal(t, 0, resKey)
	require.Equal(t, math.MaxFloat64, resQPS)
}

func TestMergeRanges(t *testing.T) {
	ranges := []HotReplica{
		{
			qps:      35,
			startKey: "1",
			endKey:   "2",
		},
		{
			qps:      30,
			startKey: "2",
			endKey:   "3",
		},
		{
			qps:      20,
			startKey: "3",
			endKey:   "4",
		},
		{
			qps:      50,
			startKey: "5",
			endKey:   "6",
		},
		{
			qps:      60,
			startKey: "6",
			endKey:   "7",
		},
		{
			qps:      10,
			startKey: "7",
			endKey:   "8",
		},
		{
			qps:      65,
			startKey: "9",
			endKey:   "10",
		},
		{
			qps:      90,
			startKey: "10",
			endKey:   "11",
		},
		{
			qps:      75,
			startKey: "12",
			endKey:   "13",
		},
	}
	group := map[int][]HotReplica{
		1: {ranges[0], ranges[1], ranges[2]},
		2: {ranges[3], ranges[4], ranges[5]},
		3: {ranges[6], ranges[7]},
		4: {ranges[8]},
	}

	resRanges := mergeRanges(ranges, group, 5)
	// Verify the ideal case where ranges can be aggregated to at or under the budget.
	require.Equal(t, 5, len(resRanges))
	require.Equal(t, []HotReplica{
		{
			qps:      float64(85) / float64(3),
			startKey: "1",
			endKey:   "4",
		},
		{
			qps:      float64(40),
			startKey: "5",
			endKey:   "8",
		},
		ranges[6],
		ranges[7],
		ranges[8],
	}, resRanges)

	group = map[int][]HotReplica{
		1: {ranges[0], ranges[1], ranges[2]},
		2: {ranges[3], ranges[4], ranges[5]},
		3: {ranges[6], ranges[7]},
		4: {ranges[8]},
	}
	// Verify the non-ideal case where partially aggregated but ran out of valid canididates to merge.
	resRanges = mergeRanges(ranges, group, 4)
	require.Equal(t, 5, len(resRanges))
	require.Equal(t, map[int][]HotReplica{4: {ranges[8]}}, group)

	ranges[1].qps = 25
	ranges[3].qps = 35
	group = map[int][]HotReplica{
		1: {ranges[0], ranges[1], ranges[2]},
		2: {ranges[3], ranges[4], ranges[5]},
		3: {ranges[6], ranges[7]},
		4: {ranges[8]},
	}
	// Verify worst case where no aggregation occurs due to all invalid candidates.
	resRanges = mergeRanges(ranges, group, 5)
	require.Equal(t, 9, len(resRanges))
	require.Equal(t, map[int][]HotReplica{4: {ranges[8]}}, group)
}

func TestHottestRangesAggregated(t *testing.T) {
	ranges := []HotReplica{
		{
			qps:      35,
			startKey: "1",
			endKey:   "2",
		},
		{
			qps:      30,
			startKey: "2",
			endKey:   "3",
		},
		{
			qps:      20,
			startKey: "3",
			endKey:   "4",
		},
		{
			qps:      50,
			startKey: "5",
			endKey:   "6",
		},
		{
			qps:      60,
			startKey: "6",
			endKey:   "7",
		},
		{
			qps:      10,
			startKey: "7",
			endKey:   "8",
		},
		{
			qps:      65,
			startKey: "9",
			endKey:   "10",
		},
		{
			qps:      90,
			startKey: "10",
			endKey:   "11",
		},
		{
			qps:      75,
			startKey: "12",
			endKey:   "13",
		},
	}
	// Verify case where ranges are within budget and do not need to be aggregated.
	resRanges := HottestRangesAggregated(ranges, 10)
	require.Equal(t, ranges, resRanges)

	// Verify case where returned ranges are still over budget and we return by highest
	// qps up to the budget.
	resRanges = HottestRangesAggregated(ranges, 4)
	require.Equal(t, []HotReplica{
		ranges[7],
		ranges[8],
		ranges[6],
		{
			qps:      float64(40),
			startKey: "5",
			endKey:   "8",
		},
	}, resRanges)

	//Verify performance.
	// TODO @santamaura: Set to proper value for testing after optimization.
	ranges = constructHotReplicas(10000)
	tBegin := timeutil.Now()
	HottestRangesAggregated(ranges, 1000)
	latency := timeutil.Since(tBegin).Seconds()
	fmt.Println(latency)
	require.True(t, latency < 60)
}
