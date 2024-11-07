// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRaBitCodeSet(t *testing.T) {
	cs := MakeRaBitQCodeSet(65)
	require.Equal(t, 0, cs.Count)
	require.Equal(t, 2, cs.Width)

	// Add code.
	cs.Add(RaBitQCode{1, 2})
	require.Equal(t, 1, cs.Count)

	// Add additional codes.
	cs.AddUndefined(2)
	copy(cs.At(1), []uint64{3, 4})
	copy(cs.At(2), []uint64{5, 6})
	require.Equal(t, 3, cs.Count)
	require.Equal(t, RaBitQCode{5, 6}, cs.At(2))

	// Remove codes.
	cs.ReplaceWithLast(1)
	require.Equal(t, 2, cs.Count)
	require.Equal(t, RaBitQCode{5, 6}, cs.At(1))

	cs.ReplaceWithLast(0)
	require.Equal(t, 1, cs.Count)
	require.Equal(t, RaBitQCode{5, 6}, cs.At(0))

	cs.ReplaceWithLast(0)
	require.Equal(t, 0, cs.Count)

	data := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	cs = MakeRaBitQCodeSetFromRawData(data, 3)
	require.Equal(t, 4, cs.Count)
	require.Equal(t, 3, cs.Width)
	require.Equal(t, data, cs.Data)
}

func TestRaBitQuantizedVectorSet(t *testing.T) {
	var quantizedSet RaBitQuantizedVectorSet
	quantizedSet.Centroid = []float32{1, 2, 3}
	quantizedSet.Codes.Width = 3

	quantizedSet.AddUndefined(5)
	copy(quantizedSet.Codes.At(4), []uint64{1, 2, 3})
	quantizedSet.CodeCounts[4] = 15
	quantizedSet.CentroidDistances[4] = 1.23
	quantizedSet.DotProducts[4] = 4.56
	require.Equal(t, 5, quantizedSet.Codes.Count)
	require.Len(t, quantizedSet.CodeCounts, 5)
	require.Len(t, quantizedSet.CentroidDistances, 5)
	require.Len(t, quantizedSet.DotProducts, 5)

	quantizedSet.ReplaceWithLast(2)
	require.Equal(t, 4, quantizedSet.Codes.Count)
	require.Equal(t, RaBitQCode{1, 2, 3}, quantizedSet.Codes.At(2))
	require.Len(t, quantizedSet.CodeCounts, 4)
	require.Equal(t, uint32(15), quantizedSet.CodeCounts[2])
	require.Len(t, quantizedSet.CentroidDistances, 4)
	require.Equal(t, float32(1.23), quantizedSet.CentroidDistances[2])
	require.Len(t, quantizedSet.DotProducts, 4)
	require.Equal(t, float32(4.56), quantizedSet.DotProducts[2])
}
