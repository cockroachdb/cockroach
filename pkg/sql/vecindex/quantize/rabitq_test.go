// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"context"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

// Basic tests.
func TestRaBitQuantizerSimple(t *testing.T) {
	workspace := &internal.Workspace{}
	ctx := internal.WithWorkspace(context.Background(), workspace)
	defer require.True(t, workspace.IsClear())

	t.Run("add and remove vectors", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42)
		require.Equal(t, 2, quantizer.GetOriginalDims())
		require.Equal(t, 2, quantizer.GetRandomDims())

		// Add 3 vectors and verify centroid and centroid distances.
		vectors := vector.MakeSetFromRawData([]float32{5, 2, 1, 2, 6, 5}, 2)
		quantizedSet := quantizer.Quantize(ctx, &vectors)
		require.Equal(t, vector.T{4, 3}, quantizedSet.GetCentroid())
		require.Equal(t, []float32{1.41, 3.16, 2.83}, roundFloats(quantizedSet.GetCentroidDistances(), 2))

		// Add 2 more vectors to existing set.
		vectors = vector.MakeSetFromRawData([]float32{4, 3, 6, 5}, 2)
		quantizer.QuantizeInSet(ctx, quantizedSet, &vectors)
		require.Equal(t, 5, quantizedSet.GetCount())

		// Ensure distances and error bounds are correct.
		distances := make([]float32, quantizedSet.GetCount())
		errorBounds := make([]float32, quantizedSet.GetCount())
		quantizer.EstimateSquaredDistances(ctx, quantizedSet, vector.T{1, 1}, distances, errorBounds)
		require.Equal(t, []float32{17, 0, 41, 13, 41}, roundFloats(distances, 2))
		require.Equal(t, []float32{7.21, 16.12, 14.42, 0, 14.42}, roundFloats(errorBounds, 2))
		require.Equal(t, vector.T{4, 3}, quantizedSet.GetCentroid())

		// Remove quantized vectors from the set.
		quantizedSet.ReplaceWithLast(1)
		quantizedSet.ReplaceWithLast(3)
		quantizedSet.ReplaceWithLast(1)
		require.Equal(t, 2, quantizedSet.GetCount())
		distances = distances[:2]
		errorBounds = errorBounds[:2]
		quantizer.EstimateSquaredDistances(ctx, quantizedSet, vector.T{1, 1}, distances, errorBounds)
		require.Equal(t, []float32{17, 41}, roundFloats(distances, 2))
		require.Equal(t, []float32{7.21, 14.42}, roundFloats(errorBounds, 2))

		// Remove remaining quantized vectors.
		quantizedSet.ReplaceWithLast(0)
		quantizedSet.ReplaceWithLast(0)
		require.Equal(t, 0, quantizedSet.GetCount())
		require.Equal(t, vector.T{4, 3}, quantizedSet.GetCentroid())
		distances = distances[:0]
		errorBounds = errorBounds[:0]
		quantizer.EstimateSquaredDistances(ctx, quantizedSet, vector.T{1, 1}, distances, errorBounds)
	})

	t.Run("empty quantized set", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42)
		vectors := vector.MakeSet(2)
		quantizedSet := quantizer.Quantize(ctx, &vectors)
		require.Equal(t, vector.T{0, 0}, quantizedSet.GetCentroid())
		require.Nil(t, quantizedSet.GetCentroidDistances())
	})
}

// Edge cases.
func TestRaBitQuantizerEdge(t *testing.T) {
	workspace := &internal.Workspace{}
	ctx := internal.WithWorkspace(context.Background(), workspace)
	defer require.True(t, workspace.IsClear())

	// Search for query vector with two equal dimensions, which makes Δ = 0.
	t.Run("two dimensions equal", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42)
		vectors := vector.MakeSetFromRawData([]float32{4, 4, -3, -3}, 2)
		quantizedSet := quantizer.Quantize(ctx, &vectors).(*RaBitQuantizedVectorSet)
		require.Equal(t, 2, quantizedSet.GetCount())
		require.Equal(t, []uint64{0xc000000000000000, 0x0}, quantizedSet.Codes.Data)
		require.Equal(t, []uint32{2, 0}, quantizedSet.CodeCounts)

		distances := make([]float32, 2)
		errorBounds := make([]float32, 2)
		quantizer.EstimateSquaredDistances(ctx, quantizedSet, vector.T{1, 1}, distances, errorBounds)
		require.Equal(t, []float32{18, 32}, roundFloats(distances, 2))
		require.Equal(t, []float32{4.95, 4.95}, roundFloats(errorBounds, 2))
	})

	t.Run("many dimensions, not multiple of 64", func(t *testing.T) {
		// Number dimensions is > 64 and not a multiple of 64.
		quantizer := NewRaBitQuantizer(141, 42)

		vectors := vector.MakeSet(141)
		vectors.AddUndefined(2)
		zeros := vectors.At(0)
		ones := vectors.At(1)
		for i := 0; i < len(ones); i++ {
			zeros[i] = 0
			ones[i] = 1
		}
		quantizedSet := quantizer.Quantize(ctx, &vectors).(*RaBitQuantizedVectorSet)
		require.Equal(t, []float32{5.94, 5.94}, roundFloats(quantizedSet.CentroidDistances, 2))
		code := quantizedSet.Codes.At(0)
		require.Equal(t, RaBitQCode{0, 0, 0}, code)
		require.Equal(t, uint32(0), quantizedSet.CodeCounts[0])
		code = quantizedSet.Codes.At(1)
		require.Equal(t, RaBitQCode{0xffffffffffffffff, 0xffffffffffffffff, 0xfff8000000000000}, code)
		require.Equal(t, uint32(141), quantizedSet.CodeCounts[1])

		distances := make([]float32, quantizedSet.GetCount())
		errorBounds := make([]float32, quantizedSet.GetCount())
		quantizer.EstimateSquaredDistances(ctx, quantizedSet, ones, distances, errorBounds)
		require.Equal(t, []float32{141, 0}, roundFloats(distances, 2))
		require.Equal(t, []float32{5.94, 5.94}, roundFloats(errorBounds, 2))
	})

	t.Run("add centroid to set", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42)
		vectors := vector.MakeSetFromRawData([]float32{1, 5, 5, 13}, 2)
		quantizedSet := quantizer.Quantize(ctx, &vectors).(*RaBitQuantizedVectorSet)
		require.Equal(t, []float32{3, 9}, quantizedSet.Centroid)

		// Add centroid to the set along with another vector.
		vectors = vector.MakeSetFromRawData([]float32{1, 5, 3, 9}, 2)
		quantizer.QuantizeInSet(ctx, quantizedSet, &vectors)
		distances := make([]float32, 4)
		errorBounds := make([]float32, 4)
		quantizer.EstimateSquaredDistances(ctx, quantizedSet, vector.T{3, 2}, distances, errorBounds)
		require.Equal(t, []float32{22.33, 115.67, 22.33, 49}, roundFloats(distances, 2))
		require.Equal(t, []float32{44.27, 44.27, 44.27, 0}, roundFloats(errorBounds, 2))
	})

	t.Run("query vector is centroid", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42)
		vectors := vector.MakeSetFromRawData([]float32{1, 5, -3, -9}, 2)
		quantizedSet := quantizer.Quantize(ctx, &vectors).(*RaBitQuantizedVectorSet)
		require.Equal(t, []float32{-1, -2}, quantizedSet.Centroid)
		distances := make([]float32, 2)
		errorBounds := make([]float32, 2)
		quantizer.EstimateSquaredDistances(ctx, quantizedSet, vector.T{-1, -2}, distances, errorBounds)
		require.Equal(t, []float32{53, 53}, roundFloats(distances, 2))
		require.Equal(t, []float32{0, 0}, roundFloats(errorBounds, 2))
	})
}

func TestRaBitRandomizeVector(t *testing.T) {
	workspace := &internal.Workspace{}
	ctx := internal.WithWorkspace(context.Background(), workspace)
	defer require.True(t, workspace.IsClear())

	const dims = 97
	const count = 5
	quantizer := NewRaBitQuantizer(dims, 46)

	// Generate random vectors with exponentially increasing norms, in order
	// make distances more distinct.
	rng := rand.New(rand.NewSource(42))
	data := make([]float32, dims*count)
	for i := range data {
		vecIdx := float64(i / dims)
		data[i] = float32(rng.NormFloat64() * math.Pow(1.5, vecIdx))
	}

	original := vector.MakeSetFromRawData(data, dims)
	randomized := vector.MakeSet(dims)
	randomized.AddUndefined(count)
	for i := range original.Count {
		quantizer.RandomizeVector(ctx, original.At(i), randomized.At(i), false /* invert */)

		// Ensure that inverting RandomizeVector recovers original vector.
		randomizedInv := make([]float32, dims)
		quantizer.RandomizeVector(ctx, randomized.At(i), randomizedInv, true /* invert */)
		for j, val := range original.At(i) {
			require.InDelta(t, val, randomizedInv[j], 0.00001)
		}
	}

	// Ensure that distances are similar, whether using the original vectors or
	// the randomized vectors.
	originalSet := quantizer.Quantize(ctx, &original).(*RaBitQuantizedVectorSet)
	randomizedSet := quantizer.Quantize(ctx, &randomized).(*RaBitQuantizedVectorSet)

	distances := make([]float32, count)
	errorBounds := make([]float32, count)
	quantizer.EstimateSquaredDistances(ctx, originalSet, original.At(0), distances, errorBounds)
	require.Equal(t, []float32{3.25, 272.75, 555.63, 945.07, 2393.15}, roundFloats(distances, 2))
	require.Equal(t, []float32{37.58, 46.08, 57.55, 69.46, 110.57}, roundFloats(errorBounds, 2))

	quantizer.EstimateSquaredDistances(ctx, randomizedSet, randomized.At(0), distances, errorBounds)
	require.Equal(t, []float32{0, 259.83, 504.97, 907.4, 2314.59}, roundFloats(distances, 2))
	require.Equal(t, []float32{37.58, 46.08, 57.55, 69.46, 110.57}, roundFloats(errorBounds, 2))
}

// Load some real OpenAI embeddings and spot check calculations.
func TestRaBitQuantizeEmbeddings(t *testing.T) {
	workspace := &internal.Workspace{}
	ctx := internal.WithWorkspace(context.Background(), workspace)
	defer require.True(t, workspace.IsClear())

	features := testutils.LoadFeatures(t, 100)
	quantizer := NewRaBitQuantizer(features.Dims, 42)
	require.Equal(t, 512, quantizer.GetOriginalDims())

	quantizedSet := quantizer.Quantize(ctx, &features)
	require.Equal(t, 100, quantizedSet.GetCount())

	centroid := quantizedSet.GetCentroid()
	require.Len(t, centroid, 512)
	require.InDelta(t, -0.00452728, centroid[0], 0.0000001)
	require.InDelta(t, -0.00299389, centroid[511], 0.0000001)

	centroidDistances := quantizedSet.GetCentroidDistances()
	require.Len(t, centroidDistances, 100)
	require.InDelta(t, 0.7345806, centroidDistances[0], 0.0000001)
	require.InDelta(t, 0.7328457, centroidDistances[99], 0.0000001)

	queryVector := features.At(0)
	squaredDistances := make([]float32, quantizedSet.GetCount())
	errorBounds := make([]float32, quantizedSet.GetCount())
	quantizer.EstimateSquaredDistances(ctx, quantizedSet, queryVector, squaredDistances, errorBounds)
	num32.Round(squaredDistances, 4)
	num32.Round(errorBounds, 4)
	require.Equal(t, float32(0.0182), squaredDistances[0])
	require.Equal(t, float32(1.0965), squaredDistances[99])
	require.Equal(t, float32(0.0477), errorBounds[0])
	require.Equal(t, float32(0.0476), errorBounds[99])
}

// Benchmark quantization of 100 vectors.
func BenchmarkQuantize(b *testing.B) {
	ctx := internal.WithWorkspace(context.Background(), &internal.Workspace{})
	features := testutils.LoadFeatures(b, 100)
	quantizer := NewRaBitQuantizer(features.Dims, 42)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		quantizer.Quantize(ctx, &features)
	}
}

// Benchmark distance estimation of 100 vectors.
func BenchmarkEstimateSquaredDistances(b *testing.B) {
	ctx := internal.WithWorkspace(context.Background(), &internal.Workspace{})
	features := testutils.LoadFeatures(b, 100)
	quantizer := NewRaBitQuantizer(features.Dims, 42)
	quantizedSet := quantizer.Quantize(ctx, &features)

	queryVector := features.At(0)
	squaredDistances := make([]float32, quantizedSet.GetCount())
	errorBounds := make([]float32, quantizedSet.GetCount())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		quantizer.EstimateSquaredDistances(ctx, quantizedSet, queryVector, squaredDistances, errorBounds)
	}
}
