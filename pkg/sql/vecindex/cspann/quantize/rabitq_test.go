// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

// Basic tests.
func TestRaBitQuantizerSimple(t *testing.T) {
	var workspace workspace.T
	defer require.True(t, workspace.IsClear())

	t.Run("add and remove vectors", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42)
		require.Equal(t, 2, quantizer.GetDims())

		// Add 3 vectors and verify centroid and centroid distances.
		vectors := vector.MakeSetFromRawData([]float32{5, 2, 1, 2, 6, 5}, 2)
		quantizedSet := quantizer.Quantize(&workspace, vectors)
		require.Equal(t, vector.T{4, 3}, quantizedSet.GetCentroid())
		require.Equal(t, []float32{1.41, 3.16, 2.83},
			testutils.RoundFloats(quantizedSet.GetCentroidDistances(), 2))

		// Add 2 more vectors to existing set.
		vectors = vector.MakeSetFromRawData([]float32{4, 3, 6, 5}, 2)
		quantizer.QuantizeInSet(&workspace, quantizedSet, vectors)
		require.Equal(t, 5, quantizedSet.GetCount())

		// Ensure distances and error bounds are correct.
		distances := make([]float32, quantizedSet.GetCount())
		errorBounds := make([]float32, quantizedSet.GetCount())
		quantizer.EstimateSquaredDistances(
			&workspace, quantizedSet, vector.T{1, 1}, distances, errorBounds)
		require.Equal(t, []float32{17, 0, 41, 13, 41}, testutils.RoundFloats(distances, 2))
		require.Equal(t, []float32{7.21, 16.12, 14.42, 0, 14.42},
			testutils.RoundFloats(errorBounds, 2))
		require.Equal(t, vector.T{4, 3}, quantizedSet.GetCentroid())

		// Remove quantized vectors from the set.
		quantizedSet.ReplaceWithLast(1)
		quantizedSet.ReplaceWithLast(3)
		quantizedSet.ReplaceWithLast(1)
		require.Equal(t, 2, quantizedSet.GetCount())
		distances = distances[:2]
		errorBounds = errorBounds[:2]
		quantizer.EstimateSquaredDistances(
			&workspace, quantizedSet, vector.T{1, 1}, distances, errorBounds)
		require.Equal(t, []float32{17, 41}, testutils.RoundFloats(distances, 2))
		require.Equal(t, []float32{7.21, 14.42}, testutils.RoundFloats(errorBounds, 2))

		// Remove remaining quantized vectors.
		quantizedSet.ReplaceWithLast(0)
		quantizedSet.ReplaceWithLast(0)
		require.Equal(t, 0, quantizedSet.GetCount())
		require.Equal(t, vector.T{4, 3}, quantizedSet.GetCentroid())
		distances = distances[:0]
		errorBounds = errorBounds[:0]
		quantizer.EstimateSquaredDistances(
			&workspace, quantizedSet, vector.T{1, 1}, distances, errorBounds)
	})

	t.Run("empty quantized set", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42)
		vectors := vector.MakeSet(2)
		quantizedSet := quantizer.Quantize(&workspace, vectors)
		require.Equal(t, vector.T{0, 0}, quantizedSet.GetCentroid())
		require.Equal(t, []float32{}, quantizedSet.GetCentroidDistances())
	})

	t.Run("empty quantized set with capacity", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(65, 42)
		centroid := make([]float32, 65)
		for i := range centroid {
			centroid[i] = float32(i)
		}
		quantizedSet := quantizer.NewQuantizedVectorSet(
			5, centroid).(*RaBitQuantizedVectorSet)
		require.Equal(t, centroid, quantizedSet.Centroid)
		require.Equal(t, 0, quantizedSet.Codes.Count)
		require.Equal(t, 2, quantizedSet.Codes.Width)
		require.Equal(t, 10, cap(quantizedSet.Codes.Data))
		require.Equal(t, 5, cap(quantizedSet.CodeCounts))
		require.Equal(t, 5, cap(quantizedSet.CentroidDistances))
		require.Equal(t, 5, cap(quantizedSet.DotProducts))
	})
}

// Edge cases.
func TestRaBitQuantizerEdge(t *testing.T) {
	var workspace workspace.T
	defer require.True(t, workspace.IsClear())

	// Search for query vector with two equal dimensions, which makes Δ = 0.
	t.Run("two dimensions equal", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42)
		vectors := vector.MakeSetFromRawData([]float32{4, 4, -3, -3}, 2)
		quantizedSet := quantizer.Quantize(&workspace, vectors).(*RaBitQuantizedVectorSet)
		require.Equal(t, 2, quantizedSet.GetCount())
		require.Equal(t, []uint64{0xc000000000000000, 0x0}, quantizedSet.Codes.Data)
		require.Equal(t, []uint32{2, 0}, quantizedSet.CodeCounts)

		distances := make([]float32, 2)
		errorBounds := make([]float32, 2)
		quantizer.EstimateSquaredDistances(
			&workspace, quantizedSet, vector.T{1, 1}, distances, errorBounds)
		require.Equal(t, []float32{18, 32}, testutils.RoundFloats(distances, 2))
		require.Equal(t, []float32{4.95, 4.95}, testutils.RoundFloats(errorBounds, 2))
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
		quantizedSet := quantizer.Quantize(&workspace, vectors).(*RaBitQuantizedVectorSet)
		require.Equal(t, []float32{5.94, 5.94},
			testutils.RoundFloats(quantizedSet.CentroidDistances, 2))
		code := quantizedSet.Codes.At(0)
		require.Equal(t, RaBitQCode{0, 0, 0}, code)
		require.Equal(t, uint32(0), quantizedSet.CodeCounts[0])
		code = quantizedSet.Codes.At(1)
		require.Equal(t, RaBitQCode{0xffffffffffffffff, 0xffffffffffffffff, 0xfff8000000000000}, code)
		require.Equal(t, uint32(141), quantizedSet.CodeCounts[1])

		distances := make([]float32, quantizedSet.GetCount())
		errorBounds := make([]float32, quantizedSet.GetCount())
		quantizer.EstimateSquaredDistances(
			&workspace, quantizedSet, ones, distances, errorBounds)
		require.Equal(t, []float32{141, 0}, testutils.RoundFloats(distances, 2))
		require.Equal(t, []float32{5.94, 5.94}, testutils.RoundFloats(errorBounds, 2))
	})

	t.Run("add centroid to set", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42)
		vectors := vector.MakeSetFromRawData([]float32{1, 5, 5, 13}, 2)
		quantizedSet := quantizer.Quantize(&workspace, vectors).(*RaBitQuantizedVectorSet)
		require.Equal(t, []float32{3, 9}, quantizedSet.Centroid)

		// Add centroid to the set along with another vector.
		vectors = vector.MakeSetFromRawData([]float32{1, 5, 3, 9}, 2)
		quantizer.QuantizeInSet(&workspace, quantizedSet, vectors)
		distances := make([]float32, 4)
		errorBounds := make([]float32, 4)
		quantizer.EstimateSquaredDistances(
			&workspace, quantizedSet, vector.T{3, 2}, distances, errorBounds)
		require.Equal(t, []float32{22.33, 115.67, 22.33, 49}, testutils.RoundFloats(distances, 2))
		require.Equal(t, []float32{44.27, 44.27, 44.27, 0}, testutils.RoundFloats(errorBounds, 2))
	})

	t.Run("query vector is centroid", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42)
		vectors := vector.MakeSetFromRawData([]float32{1, 5, -3, -9}, 2)
		quantizedSet := quantizer.Quantize(&workspace, vectors).(*RaBitQuantizedVectorSet)
		require.Equal(t, []float32{-1, -2}, quantizedSet.Centroid)
		distances := make([]float32, 2)
		errorBounds := make([]float32, 2)
		quantizer.EstimateSquaredDistances(
			&workspace, quantizedSet, vector.T{-1, -2}, distances, errorBounds)
		require.Equal(t, []float32{53, 53}, testutils.RoundFloats(distances, 2))
		require.Equal(t, []float32{0, 0}, testutils.RoundFloats(errorBounds, 2))
	})
}

// Load some real OpenAI embeddings and spot check calculations.
func TestRaBitQuantizeEmbeddings(t *testing.T) {
	var workspace workspace.T
	defer require.True(t, workspace.IsClear())

	features := testutils.LoadFeatures(t, 100)
	quantizer := NewRaBitQuantizer(features.Dims, 42)
	require.Equal(t, 512, quantizer.GetDims())

	quantizedSet := quantizer.Quantize(&workspace, features)
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
	quantizer.EstimateSquaredDistances(
		&workspace, quantizedSet, queryVector, squaredDistances, errorBounds)
	num32.Round(squaredDistances, 4)
	num32.Round(errorBounds, 4)
	require.Equal(t, float32(0), squaredDistances[0])
	require.Equal(t, float32(1.1069), squaredDistances[99])
	require.Equal(t, float32(0.0477), errorBounds[0])
	require.Equal(t, float32(0.0476), errorBounds[99])
}

// Benchmark quantization of 100 vectors.
func BenchmarkQuantize(b *testing.B) {
	var workspace workspace.T
	features := testutils.LoadFeatures(b, 100)
	quantizer := NewRaBitQuantizer(features.Dims, 42)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		quantizer.Quantize(&workspace, features)
	}
}

// Benchmark distance estimation of 100 vectors.
func BenchmarkEstimateSquaredDistances(b *testing.B) {
	var workspace workspace.T
	features := testutils.LoadFeatures(b, 100)
	quantizer := NewRaBitQuantizer(features.Dims, 42)
	quantizedSet := quantizer.Quantize(&workspace, features)

	queryVector := features.At(0)
	squaredDistances := make([]float32, quantizedSet.GetCount())
	errorBounds := make([]float32, quantizedSet.GetCount())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		quantizer.EstimateSquaredDistances(
			&workspace, quantizedSet, queryVector, squaredDistances, errorBounds)
	}
}
