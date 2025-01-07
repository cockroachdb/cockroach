// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func TestSplitPartitionData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := internal.WithWorkspace(context.Background(), &internal.Workspace{})
	quantizer := quantize.NewRaBitQuantizer(2, 42)
	store := vecstore.NewInMemoryStore(2, 42)
	options := VectorIndexOptions{Seed: 42}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	index, err := NewVectorIndex(ctx, store, quantizer, &options, stopper)
	require.NoError(t, err)
	worker := NewFixupWorker(&index.fixups)

	vectors := vector.MakeSetFromRawData([]float32{
		0, 0,
		1, 1,
		2, 3,
		3, 3,
		4, 4,
		5, 5,
		6, 6,
	}, 2)
	quantizedSet := quantizer.Quantize(ctx, &vectors)

	splitPartition := vecstore.NewPartition(
		quantizer,
		quantizedSet,
		[]vecstore.ChildKey{
			{PrimaryKey: vecstore.PrimaryKey("vec1")},
			{PrimaryKey: vecstore.PrimaryKey("vec2")},
			{PrimaryKey: vecstore.PrimaryKey("vec3")},
			{PrimaryKey: vecstore.PrimaryKey("vec4")},
			{PrimaryKey: vecstore.PrimaryKey("vec5")},
			{PrimaryKey: vecstore.PrimaryKey("vec6")},
			{PrimaryKey: vecstore.PrimaryKey("vec7")},
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
			tempVectors.AddSet(&vectors)
			leftSplit, rightSplit := worker.splitPartitionData(
				ctx, splitPartition, tempVectors, tc.leftOffsets, tc.rightOffsets)

			validate(&leftSplit, tc.expectedLeft)
			validate(&rightSplit, tc.expectedRight)
		})
	}
}
