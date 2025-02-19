// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func TestUnQuantizedVectorSet(t *testing.T) {
	quantizedSet := UnQuantizedVectorSet{
		Vectors: vector.MakeSet(2),
	}
	quantizedSet.Centroid = []float32{4, 2}

	// Add vectors.
	vectors := vector.MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
	quantizedSet.AddSet(vectors)
	require.Equal(t, 3, quantizedSet.GetCount())
	require.Equal(t, vector.T{4, 2}, quantizedSet.GetCentroid())

	vectors = vector.MakeSetFromRawData([]float32{7, 8, 9, 10}, 2)
	quantizedSet.AddSet(vectors)
	require.Equal(t, 5, quantizedSet.GetCount())
	distances := testutils.RoundFloats(quantizedSet.GetCentroidDistances(), 4)
	require.Equal(t, []float32{3, 2.2361, 4.1231, 6.7082, 9.434}, distances)

	// Ensure that cloning does not disturb anything.
	cloned := quantizedSet.Clone().(*UnQuantizedVectorSet)
	cloned.Centroid[0] = 10
	cloned.CentroidDistances[0] = 10
	copy(cloned.Vectors.At(0), vector.T{0, 0})
	cloned.ReplaceWithLast(1)

	// Remove vector.
	quantizedSet.ReplaceWithLast(2)
	require.Equal(t, 4, quantizedSet.GetCount())
	distances = testutils.RoundFloats(quantizedSet.GetCentroidDistances(), 4)
	require.Equal(t, []float32{3, 2.2361, 9.434, 6.7082}, distances)

	// ComputeSquaredDistances on vectors.
	quantizedSet.ComputeSquaredDistances(vector.T{-1, 1}, distances)
	require.Equal(t, []float32{5, 25, 181, 113}, testutils.RoundFloats(distances, 4))

	// Check that clone is unaffected.
	require.Equal(t, []float32{10, 2}, cloned.Centroid)
	require.Equal(t, []float32{10, 9.43, 4.12, 6.71}, testutils.RoundFloats(cloned.CentroidDistances, 2))
	require.Equal(t, vector.Set{Dims: 2, Count: 4, Data: []float32{0, 0, 9, 10, 5, 6, 7, 8}}, cloned.Vectors)
}
