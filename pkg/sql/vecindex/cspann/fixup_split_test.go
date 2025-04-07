// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func TestSplitPartitionData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var workspace workspace.T
	vectors := vector.MakeSetFromRawData([]float32{
		0, 0,
		1, 1,
		2, 3,
		3, 3,
		4, 4,
		5, 5,
		6, 6,
	}, 2)

	childKeys := []ChildKey{
		{KeyBytes: KeyBytes("vec1")},
		{KeyBytes: KeyBytes("vec2")},
		{KeyBytes: KeyBytes("vec3")},
		{KeyBytes: KeyBytes("vec4")},
		{KeyBytes: KeyBytes("vec5")},
		{KeyBytes: KeyBytes("vec6")},
		{KeyBytes: KeyBytes("vec7")},
	}
	valueBytes := []ValueBytes{
		{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7},
	}

	testCases := []struct {
		desc          string
		leftOffsets   []uint64
		rightOffsets  []uint64
		expectedLeft  []uint64
		expectedRight []uint64
	}{
		{
			desc:         "no reordering",
			leftOffsets:  []uint64{0, 1, 2, 3},
			rightOffsets: []uint64{4, 5, 6},
		},
		{
			desc:         "only one on left",
			leftOffsets:  []uint64{1},
			rightOffsets: []uint64{0, 2, 3, 4, 5, 6},
		},
		{
			desc:         "only one on right",
			leftOffsets:  []uint64{0, 1, 2, 4, 5, 6},
			rightOffsets: []uint64{3},
		},
		{
			desc:         "interleaved",
			leftOffsets:  []uint64{0, 2, 4, 6},
			rightOffsets: []uint64{1, 3, 5},
		},
		{
			desc:         "another interleaved",
			leftOffsets:  []uint64{1, 4, 5},
			rightOffsets: []uint64{0, 2, 3, 6},
		},
		{
			desc:         "reversed",
			leftOffsets:  []uint64{4, 5, 6},
			rightOffsets: []uint64{0, 1, 2, 3},
		},
		{
			desc:         "out of order",
			leftOffsets:  []uint64{5, 4, 6},
			rightOffsets: []uint64{3, 0, 1, 2},
		},
	}

	findKey := func(allKeys []ChildKey, toFind ChildKey) int {
		for i, key := range allKeys {
			if key.Equal(toFind) {
				return i
			}
		}
		return -1
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tempVectors := vectors.Clone()
			tempChildKeys := slices.Clone(childKeys)
			tempValueBytes := slices.Clone(valueBytes)
			splitPartitionData(&workspace,
				tempVectors, tempChildKeys, tempValueBytes, tc.leftOffsets, tc.rightOffsets)

			// Ensure that partition data is on the correct side.
			for originalOffset := range childKeys {
				newOffset := findKey(tempChildKeys, childKeys[originalOffset])

				if newOffset < len(tc.leftOffsets) {
					require.Contains(t, tc.leftOffsets, uint64(originalOffset))
				} else {
					require.Contains(t, tc.rightOffsets, uint64(originalOffset))
				}

				require.Equal(t, tempVectors.At(newOffset), vectors.At(originalOffset))
				require.Equal(t, tempValueBytes[newOffset], valueBytes[originalOffset])
			}
		})
	}
}
