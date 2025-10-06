// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitFinder(t *testing.T) {
	mx := math.MaxInt64
	mn := math.MinInt64
	mxu := uint64(math.MaxUint64)

	testCases := []struct {
		desc        string
		config      *kv
		expected    []interface{}
		expectPanic bool
	}{
		{
			desc:   "hash [min,max], 5 splits",
			config: &kv{splits: 5, cycleLength: 1 /* irrelevant for hash */},
			// NB: We perform integer division to determine the split points,
			//     -2 to account for the error on this case.
			expected: []interface{}{
				int64(2*(mn/3) - 2),
				int64((mn / 3) - 2),
				int64(-2),
				int64((mx / 3) - 2),
				int64(2*(mx/3) - 2),
			},
		},
		{
			desc:     "hash [min,max], 1 splits",
			config:   &kv{splits: 1},
			expected: []interface{}{int64(-1)},
		},
		{
			desc:     "sequential [0, 600), 5 splits",
			config:   &kv{splits: 5, sequential: true, cycleLength: 600},
			expected: []interface{}{int64(100), int64(200), int64(300), int64(400), int64(500)},
		},
		{
			desc:     "sequential [0, 5), 4 splits",
			config:   &kv{splits: 4, sequential: true, cycleLength: 5},
			expected: []interface{}{int64(1), int64(2), int64(3), int64(4)},
		},
		{
			desc:   "zipfian [0, max), 5 splits",
			config: &kv{splits: 5, zipfian: true, cycleLength: 1 /* irrelevant for zipf */},
			expected: []interface{}{
				int64(mx / 6),
				int64(2 * (mx / 6)),
				int64(3 * (mx / 6)),
				int64(4 * (mx / 6)),
				int64(5 * (mx / 6)),
			},
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
		{
			desc:   "sequential [0, 600), 5 splits, strings",
			config: &kv{splits: 5, sequential: true, cycleLength: 600, keySize: 20},
			expected: []interface{}{
				"00000000000000000100",
				"00000000000000000200",
				"00000000000000000300",
				"00000000000000000400",
				"00000000000000000500",
			},
		},
		{
			desc:   "hash [0,max], 5 splits, strings",
			config: &kv{splits: 5, cycleLength: 1 /* irrelevant for hash */, keySize: 20},
			expected: []interface{}{
				fmt.Sprintf("%020d", mxu/6),
				fmt.Sprintf("%020d", 2*(mxu/6)),
				fmt.Sprintf("%020d", 3*(mxu/6)),
				fmt.Sprintf("%020d", 4*(mxu/6)),
				fmt.Sprintf("%020d", 5*(mxu/6)),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			splits := tc.config.splits
			_, _, tr, r := tc.config.createKeyGenerator()

			if tc.expectPanic {
				require.Panics(t, func() { splitFinder(0, 0, r, tr) })
				return
			}

			results := make([]interface{}, splits)
			for i := range results {
				results[i] = splitFinder(i, splits, r, tr)
			}
			require.Equal(t, tc.expected, results)
		})
	}
}

func TestInitialSeqValidation(t *testing.T) {
	testCases := []struct {
		desc     string
		config   *kv
		expected int64
		err      string
	}{
		{
			desc:     "--sequential",
			config:   &kv{sequential: true, writeSeq: "S13"},
			expected: 13,
		},
		{
			desc:     "random",
			config:   &kv{writeSeq: "R17"},
			expected: 17,
		},
		{
			desc:     "--zipfian",
			config:   &kv{zipfian: true, writeSeq: "Z19"},
			expected: 19,
		},
		{
			desc:   "wrong",
			config: &kv{writeSeq: "G10"},
			err:    "--write-seq has to be of the form",
		},
		{
			desc:   "--zipfian with S",
			config: &kv{zipfian: true, writeSeq: "S10"},
			err:    "sequential --write-seq is incompatible",
		},
		{
			desc:   "--zipfian with R",
			config: &kv{zipfian: true, writeSeq: "R10"},
			err:    "random --write-seq incompatible",
		},
		{
			desc:   "--sequential with Z",
			config: &kv{sequential: true, writeSeq: "Z10"},
			err:    "zipfian --write-seq is incompatible",
		},
		{
			desc:   "--sequential with R",
			config: &kv{sequential: true, writeSeq: "R10"},
			err:    "random --write-seq incompatible",
		},
		{
			desc:   "random with Z",
			config: &kv{writeSeq: "Z10"},
			err:    "zipfian --write-seq is incompatible",
		},
		{
			desc:   "random with S",
			config: &kv{writeSeq: "S10"},
			err:    "sequential --write-seq is incompatible",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			// Fill in defaults for params that are typically filled from flags.
			tc.config.cycleLength = math.MaxInt64
			tc.config.targetCompressionRatio = 1

			err := tc.config.validateConfig()
			if len(tc.err) > 0 {
				require.ErrorContains(t, err, tc.err, "incorrect validation error")
			} else {
				require.NoError(t, err, "valid config rejected")
			}
		})
	}
}
