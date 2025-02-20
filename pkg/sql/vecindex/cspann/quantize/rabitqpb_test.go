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

	// Clone.
	data = []uint64{1, 2, 3, 4, 5, 6}
	cs = MakeRaBitQCodeSetFromRawData(data, 2)
	cs2 := cs.Clone()
	cs.ReplaceWithLast(0)
	cs2.Add(RaBitQCode{10, 20})
	require.Equal(t, 2, cs.Count)
	require.Equal(t, 2, cs.Width)
	require.Equal(t, []uint64{5, 6, 3, 4}, cs.Data)
	require.Equal(t, 4, cs2.Count)
	require.Equal(t, 2, cs2.Width)
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 10, 20}, cs2.Data)
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

	// Ensure that cloning does not disturb anything.
	cloned := quantizedSet.Clone().(*RaBitQuantizedVectorSet)
	copy(cloned.Codes.At(0), []uint64{10, 20, 30})
	cloned.Centroid[0] = 10
	cloned.CodeCounts[0] = 10
	cloned.CentroidDistances[0] = 10
	cloned.DotProducts[0] = 10
	cloned.ReplaceWithLast(1)
	cloned.ReplaceWithLast(1)
	cloned.ReplaceWithLast(1)
	cloned.ReplaceWithLast(1)

	quantizedSet.ReplaceWithLast(2)
	require.Equal(t, 4, quantizedSet.Codes.Count)
	require.Equal(t, RaBitQCode{1, 2, 3}, quantizedSet.Codes.At(2))
	require.Len(t, quantizedSet.CodeCounts, 4)
	require.Equal(t, uint32(15), quantizedSet.CodeCounts[2])
	require.Len(t, quantizedSet.CentroidDistances, 4)
	require.Equal(t, float32(1.23), quantizedSet.CentroidDistances[2])
	require.Len(t, quantizedSet.DotProducts, 4)
	require.Equal(t, float32(4.56), quantizedSet.DotProducts[2])

	// Check that clone is unaffected.
	require.Equal(t, []float32{10, 2, 3}, cloned.Centroid)
	require.Equal(t, RaBitQCodeSet{Count: 1, Width: 3, Data: []uint64{10, 20, 30}}, cloned.Codes)
	require.Equal(t, []uint32{10}, cloned.CodeCounts)
	require.Equal(t, []float32{10}, cloned.CentroidDistances)
	require.Equal(t, []float32{10}, cloned.DotProducts)
}
