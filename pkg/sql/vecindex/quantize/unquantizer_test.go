// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

// Basic tests.
func TestUnQuantizerSimple(t *testing.T) {
	ctx := context.Background()
	quantizer := NewUnQuantizer(2)
	require.Equal(t, 2, quantizer.GetOriginalDims())
	require.Equal(t, 2, quantizer.GetRandomDims())

	// Quantize empty set.
	vectors := vector.MakeSet(2)
	quantizedSet := quantizer.Quantize(ctx, &vectors)
	require.Equal(t, vector.T{0, 0}, quantizedSet.GetCentroid())
	require.Equal(t, []float32{}, roundFloats(quantizedSet.GetCentroidDistances(), 2))

	// Add 3 vectors and verify centroid and centroid distances.
	vectors = vector.MakeSetFromRawData([]float32{5, 2, 1, 2, 6, 5}, 2)
	quantizedSet = quantizer.Quantize(ctx, &vectors)
	require.Equal(t, vector.T{4, 3}, quantizedSet.GetCentroid())
	require.Equal(t, []float32{1.41, 3.16, 2.83}, roundFloats(quantizedSet.GetCentroidDistances(), 2))

	// Add 2 more vectors to existing set.
	vectors = vector.MakeSetFromRawData([]float32{4, 3, 6, 5}, 2)
	quantizer.QuantizeInSet(ctx, quantizedSet, &vectors)
	require.Equal(t, 5, quantizedSet.GetCount())
	require.Equal(t, []float32{1.41, 3.16, 2.83, 0, 2.83}, roundFloats(quantizedSet.GetCentroidDistances(), 2))

	// Ensure distances and error bounds are correct.
	distances := make([]float32, quantizedSet.GetCount())
	errorBounds := make([]float32, quantizedSet.GetCount())
	quantizer.EstimateSquaredDistances(ctx, quantizedSet, vector.T{1, 1}, distances, errorBounds)
	require.Equal(t, []float32{17, 1, 41, 13, 41}, roundFloats(distances, 2))
	require.Equal(t, []float32{0, 0, 0, 0, 0}, roundFloats(errorBounds, 2))
	require.Equal(t, vector.T{4, 3}, quantizedSet.GetCentroid())

	// Query vector is centroid.
	quantizer.EstimateSquaredDistances(ctx, quantizedSet, vector.T{0, 0}, distances, errorBounds)
	require.Equal(t, []float32{29, 5, 61, 25, 61}, roundFloats(distances, 2))
	require.Equal(t, []float32{0, 0, 0, 0, 0}, roundFloats(errorBounds, 2))

	// Call RandomizeVector.
	output := vector.T{3, 4}
	quantizer.RandomizeVector(ctx, vector.T{1, 2}, output, false /* invert */)
	require.Equal(t, vector.T{1, 2}, output)
	quantizer.RandomizeVector(ctx, vector.T{5, 6}, output, true /* invert */)
	require.Equal(t, vector.T{5, 6}, output)

	// Remove quantized vectors.
	quantizedSet.ReplaceWithLast(1)
	quantizedSet.ReplaceWithLast(3)
	quantizedSet.ReplaceWithLast(1)
	require.Equal(t, 2, quantizedSet.GetCount())
	require.Equal(t, []float32{1.41, 2.83}, roundFloats(quantizedSet.GetCentroidDistances(), 2))
	distances = distances[:2]
	errorBounds = errorBounds[:2]
	quantizer.EstimateSquaredDistances(ctx, quantizedSet, vector.T{1, 1}, distances, errorBounds)
	require.Equal(t, []float32{17, 41}, roundFloats(distances, 2))
	require.Equal(t, []float32{0, 0}, roundFloats(errorBounds, 2))

	// Remove remaining quantized vectors.
	quantizedSet.ReplaceWithLast(0)
	quantizedSet.ReplaceWithLast(0)
	require.Equal(t, 0, quantizedSet.GetCount())
	require.Equal(t, vector.T{4, 3}, quantizedSet.GetCentroid())
	require.Equal(t, []float32{}, roundFloats(quantizedSet.GetCentroidDistances(), 2))
	distances = distances[:0]
	errorBounds = errorBounds[:0]
	quantizer.EstimateSquaredDistances(ctx, quantizedSet, vector.T{1, 1}, distances, errorBounds)

	// Empty quantized set.
	vectors = vector.MakeSet(2)
	quantizedSet = quantizer.Quantize(ctx, &vectors)
	require.Equal(t, vector.T{0, 0}, quantizedSet.GetCentroid())
	require.Equal(t, []float32(nil), quantizedSet.GetCentroidDistances())

	// Add single vector to quantized set.
	vectors = vector.T{4, 4}.AsSet()
	quantizer.QuantizeInSet(ctx, quantizedSet, &vectors)
	require.Equal(t, 1, quantizedSet.GetCount())
	require.Equal(t, []float32{5.66}, roundFloats(quantizedSet.GetCentroidDistances(), 2))
	distances = distances[:1]
	errorBounds = errorBounds[:1]
	quantizer.EstimateSquaredDistances(ctx, quantizedSet, vector.T{1, 1}, distances, errorBounds)
	require.Equal(t, []float32{18}, roundFloats(distances, 2))
	require.Equal(t, []float32{0}, roundFloats(errorBounds, 2))
}

func roundFloats(s []float32, prec int) []float32 {
	t := make([]float32, len(s))
	copy(t, s)
	num32.Round(t, prec)
	return t
}
