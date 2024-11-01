// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"testing"

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
	quantizedSet.AddSet(&vectors)
	require.Equal(t, 3, quantizedSet.GetCount())
	require.Equal(t, vector.T{4, 2}, quantizedSet.GetCentroid())

	vectors = vector.MakeSetFromRawData([]float32{7, 8, 9, 10}, 2)
	quantizedSet.AddSet(&vectors)
	require.Equal(t, 5, quantizedSet.GetCount())
	distances := roundFloats(quantizedSet.GetCentroidDistances(), 4)
	require.Equal(t, []float32{3, 2.2361, 4.1231, 6.7082, 9.434}, distances)

	// Remove vector.
	quantizedSet.ReplaceWithLast(2)
	require.Equal(t, 4, quantizedSet.GetCount())
	distances = roundFloats(quantizedSet.GetCentroidDistances(), 4)
	require.Equal(t, []float32{3, 2.2361, 9.434, 6.7082}, distances)

	// ComputeSquaredDistances on vectors.
	quantizedSet.ComputeSquaredDistances(vector.T{-1, 1}, distances)
	require.Equal(t, []float32{5, 25, 181, 113}, roundFloats(distances, 4))
}
