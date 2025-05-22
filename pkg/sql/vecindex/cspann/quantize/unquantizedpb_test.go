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

	// Add vectors.
	vectors := vector.MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
	quantizedSet.AddSet(vectors)
	require.Equal(t, 3, quantizedSet.GetCount())

	vectors = vector.MakeSetFromRawData([]float32{7, 8, 9, 10}, 2)
	quantizedSet.AddSet(vectors)
	require.Equal(t, 5, quantizedSet.GetCount())

	// Ensure that cloning does not disturb anything.
	cloned := quantizedSet.Clone().(*UnQuantizedVectorSet)
	copy(cloned.Vectors.At(0), vector.T{0, 0})
	cloned.ReplaceWithLast(1)

	// Remove vector.
	quantizedSet.ReplaceWithLast(2)
	require.Equal(t, 4, quantizedSet.GetCount())

	// Check that clone is unaffected.
	require.Equal(t, vector.Set{Dims: 2, Count: 4, Data: []float32{0, 0, 9, 10, 5, 6, 7, 8}}, cloned.Vectors)
}
