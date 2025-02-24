// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func TestSplitPartitionData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var workspace workspace.T
	quantizer := quantize.NewRaBitQuantizer(2, 42)
	vectors := vector.MakeSetFromRawData([]float32{
		0, 0,
		1, 1,
		2, 3,
		3, 3,
		4, 4,
		5, 5,
		6, 6,
	}, 2)
	quantizedSet := quantizer.Quantize(&workspace, vectors)

	splitPartition := NewPartition(
		quantizer,
		quantizedSet,
		[]ChildKey{
			{KeyBytes: KeyBytes("vec1")},
			{KeyBytes: KeyBytes("vec2")},
			{KeyBytes: KeyBytes("vec3")},
			{KeyBytes: KeyBytes("vec4")},
			{KeyBytes: KeyBytes("vec5")},
			{KeyBytes: KeyBytes("vec6")},
			{KeyBytes: KeyBytes("vec7")},
		},
		[]ValueBytes{
			{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}, {11, 12}, {13, 14},
		},
		1)

	validate := func(split *splitData, offsets []uint64) {
		require.Equal(t, splitPartition.Level(), split.Partition.Level())
		require.Equal(t, len(offsets), split.Partition.Count())

		// Validate centroid.
		centroid := vector.T{0, 0}
		split.Vectors.Centroid(centroid)
		require.Equal(t, centroid, split.Partition.Centroid())

		oldCentroidDistances := splitPartition.QuantizedSet().GetCentroidDistances()
		centroidDistances := split.Partition.QuantizedSet().GetCentroidDistances()
		for i, offset := range offsets {
			cmp, err := vectors.At(int(offset)).Compare(split.Vectors.At(i))
			require.NoError(t, err)
			require.Equal(t, 0, cmp)
			require.Equal(t, oldCentroidDistances[offset], split.OldCentroidDistances[i])
			require.Equal(t, splitPartition.ChildKeys()[offset], split.Partition.ChildKeys()[i])
			require.Equal(t, splitPartition.ValueBytes()[offset], split.Partition.ValueBytes()[i])

			// Validate centroid distances.
			expectedDistance := num32.L2Distance(centroid, split.Vectors.At(i))
			require.Equal(t, expectedDistance, centroidDistances[i])
		}
	}

	testCases := []struct {
		desc          string
		leftOffsets   []uint64
		rightOffsets  []uint64
		expectedLeft  []uint64
		expectedRight []uint64
	}{
		{
			desc:          "no reordering",
			leftOffsets:   []uint64{0, 1, 2, 3},
			rightOffsets:  []uint64{4, 5, 6},
			expectedLeft:  []uint64{0, 1, 2, 3},
			expectedRight: []uint64{4, 5, 6},
		},
		{
			desc:          "only one on left",
			leftOffsets:   []uint64{1},
			rightOffsets:  []uint64{0, 2, 3, 4, 5, 6},
			expectedLeft:  []uint64{1},
			expectedRight: []uint64{0, 2, 3, 4, 5, 6},
		},
		{
			desc:          "only one on right",
			leftOffsets:   []uint64{0, 1, 2, 4, 5, 6},
			rightOffsets:  []uint64{3},
			expectedLeft:  []uint64{0, 1, 2, 6, 4, 5},
			expectedRight: []uint64{3},
		},
		{
			desc:          "interleaved",
			leftOffsets:   []uint64{0, 2, 4, 6},
			rightOffsets:  []uint64{1, 3, 5},
			expectedLeft:  []uint64{0, 6, 2, 4},
			expectedRight: []uint64{3, 5, 1},
		},
		{
			desc:          "another interleaved",
			leftOffsets:   []uint64{1, 4, 5},
			rightOffsets:  []uint64{0, 2, 3, 6},
			expectedLeft:  []uint64{5, 1, 4},
			expectedRight: []uint64{3, 2, 0, 6},
		},
		{
			desc:          "reversed",
			leftOffsets:   []uint64{4, 5, 6},
			rightOffsets:  []uint64{0, 1, 2, 3},
			expectedLeft:  []uint64{6, 5, 4},
			expectedRight: []uint64{3, 2, 1, 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tempVectors := vector.MakeSet(2)
			tempVectors.AddSet(vectors)
			leftSplit, rightSplit := splitPartitionData(
				&workspace, quantizer, splitPartition, tempVectors, tc.leftOffsets, tc.rightOffsets)

			validate(&leftSplit, tc.expectedLeft)
			validate(&rightSplit, tc.expectedRight)
		})
	}
}
