// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitFinder(t *testing.T) {
	mx := math.MaxInt64
	mn := math.MinInt64

	testCases := []struct {
		desc        string
		config      *kv
		expected    []int
		expectPanic bool
	}{
		{
			desc:   "hash [min,max], 5 splits",
			config: &kv{splits: 5, cycleLength: 1 /* irrelevant for hash */},
			// NB: We perform integer division to determine the split points,
			//     -2 to account for the error on this case.
			expected: []int{2*(mn/3) - 2, (mn / 3) - 2, -2, (mx / 3) - 2, 2*(mx/3) - 2},
		},
		{
			desc:     "hash [min,max], 1 splits",
			config:   &kv{splits: 1},
			expected: []int{-1},
		},
		{
			desc:     "sequential [0, 600), 5 splits",
			config:   &kv{splits: 5, sequential: true, cycleLength: 600},
			expected: []int{100, 200, 300, 400, 500},
		},
		{
			desc:     "sequential [0, 5), 4 splits",
			config:   &kv{splits: 4, sequential: true, cycleLength: 5},
			expected: []int{1, 2, 3, 4},
		},
		{
			desc:     "zipfian [0, max), 5 splits",
			config:   &kv{splits: 5, zipfian: true, cycleLength: 1 /* irrelevant for zipf */},
			expected: []int{mx / 6, 2 * (mx / 6), 3 * (mx / 6), 4 * (mx / 6), 5 * (mx / 6)},
		},
		{
			desc:        "invalid: splits >= cycle-length when sequential",
			config:      &kv{splits: 5, cycleLength: 5, sequential: true},
			expectPanic: true,
		},
		{
			desc:        "invalid: splits < 0",
			config:      &kv{splits: -1},
			expectPanic: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			splits := tc.config.splits

			if tc.expectPanic {
				require.Panics(t, func() { tc.config.splitFinder(0) })
				return
			}

			results := make([]int, splits)
			for i := range results {
				results[i] = tc.config.splitFinder(i)
			}
			require.Equal(t, tc.expected, results)
		})
	}
}
