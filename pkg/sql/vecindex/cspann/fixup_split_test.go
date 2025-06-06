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
		desc        string
		assignments []uint64
		expected    []int
	}{
		{
			desc:        "no reordering",
			assignments: []uint64{0, 0, 0, 0, 1, 1, 1},
			expected:    []int{0, 1, 2, 3, 4, 5, 6},
		},
		{
			desc:        "only one on left",
			assignments: []uint64{1, 0, 1, 1, 1, 1, 1},
			expected:    []int{1, 0, 2, 3, 4, 5, 6},
		},
		{
			desc:        "only one on right",
			assignments: []uint64{0, 0, 0, 1, 0, 0, 0},
			expected:    []int{0, 1, 2, 6, 4, 5, 3},
		},
		{
			desc:        "interleaved",
			assignments: []uint64{0, 1, 0, 1, 0, 1, 0},
			expected:    []int{0, 6, 2, 4, 3, 5, 1},
		},
		{
			desc:        "another interleaved",
			assignments: []uint64{1, 0, 1, 1, 0, 0, 1},
			expected:    []int{5, 1, 4, 3, 2, 0, 6},
		},
		{
			desc:        "reversed",
			assignments: []uint64{1, 1, 1, 1, 0, 0, 0},
			expected:    []int{6, 5, 4, 3, 2, 1, 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tempVectors := vectors.Clone()
			tempChildKeys := slices.Clone(childKeys)
			tempValueBytes := slices.Clone(valueBytes)
			splitPartitionData(&workspace, tempVectors, tempChildKeys, tempValueBytes, tc.assignments)

			// Ensure that partition data is on the correct side.
			for i := range tc.expected {
				require.Equal(t, tempVectors.At(tc.expected[i]), vectors.At(i))
				require.Equal(t, tempChildKeys[tc.expected[i]], childKeys[i])
				require.Equal(t, tempValueBytes[tc.expected[i]], valueBytes[i])
			}
		})
	}
}
