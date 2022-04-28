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
	"math/rand"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// constructHotKeySpans generates a slice of random SampleKeySpans
// based on the inputted size. It should be used for performance testing.
func constructHotKeySpans(amount int) []SampleKeySpan {
	hotKeySpans := make([]SampleKeySpan, 0)
	key := 0
	for len(hotKeySpans) < amount {
		qps := rand.Float64() * 100
		if qps > 50 {
			key++
		}
		hotKeySpans = append(hotKeySpans, SampleKeySpan{
			qps:      qps,
			startKey: strconv.Itoa(key),
			endKey:   strconv.Itoa(key + 1),
		})
		key++
	}
	return hotKeySpans
}

func TestShouldMergeAdjacentKeySpans(t *testing.T) {
	candidate := []SampleKeySpan{
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
	require.Equal(t, false, shouldMergeAdjacentKeySpans(candidate, 21))
	require.Equal(t, true, shouldMergeAdjacentKeySpans(candidate, 19))

	candidate = append(candidate, SampleKeySpan{qps: 15, startKey: "4", endKey: "5"})

	// Verify even calculation.
	require.Equal(t, false, shouldMergeAdjacentKeySpans(candidate, 18))
	require.Equal(t, false, shouldMergeAdjacentKeySpans(candidate, 19))
	// Ensure the original order of slice is maintained
	require.Equal(t, SampleKeySpan{qps: 20, startKey: "1", endKey: "2"}, candidate[0])
}

func TestFindAggregatedKeySpans(t *testing.T) {
	keySpans := []SampleKeySpan{
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
	group := [][]SampleKeySpan{
		{
			keySpans[0],
			keySpans[1],
			keySpans[2],
		},
		{
			keySpans[3],
			keySpans[4],
			keySpans[5],
		},
		{
			keySpans[8],
		},
		{
			keySpans[6],
			keySpans[7],
		},
	}
	resKeySpans := findAggregatedKeySpans(group, 5, len(keySpans))
	// Verify the ideal case where key spans can be aggregated to at or under the budget.
	require.Equal(t, 5, len(resKeySpans))
	require.Equal(t, []SampleKeySpan{
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
		keySpans[8],
		keySpans[6],
		keySpans[7],
	}, resKeySpans)
	// Verify the non-ideal case where partially aggregated but ran out of valid canididates to merge.
	resKeySpans = findAggregatedKeySpans(group, 4, len(keySpans))
	require.Equal(t, 5, len(resKeySpans))

	group[0][1].qps = 25
	group[1][0].qps = 35

	// Verify worst case where no aggregation occurs due to all invalid candidates.
	resKeySpans = findAggregatedKeySpans(group, 5, len(keySpans))
	require.Equal(t, 9, len(resKeySpans))
}

func TestHottestKeySpansAggregated(t *testing.T) {
	keySpans := []SampleKeySpan{
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
	// Verify case where key spans are within budget and do not need to be aggregated.
	resKeySpans := HottestKeySpansAggregated(keySpans, 10)
	require.Equal(t, keySpans, resKeySpans)

	// Verify case where returned key spans are still over budget and we return by highest
	// qps up to the budget.
	resKeySpans = HottestKeySpansAggregated(keySpans, 4)
	require.Equal(t, []SampleKeySpan{
		keySpans[7],
		keySpans[8],
		keySpans[6],
		{
			qps:      float64(40),
			startKey: "5",
			endKey:   "8",
		},
	}, resKeySpans)

	//Verify performance.
	// TODO @santamaura: Set to proper value for testing after optimization.
	keySpans = constructHotKeySpans(100000)
	tBegin := timeutil.Now()
	HottestKeySpansAggregated(keySpans, 1000)
	latency := timeutil.Since(tBegin).Seconds()
	require.True(t, latency < 60)
}
