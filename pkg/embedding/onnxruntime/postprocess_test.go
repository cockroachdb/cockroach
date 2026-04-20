// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package onnxruntime

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMeanPool(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		// batch=1, seq=3, dims=2
		// Token embeddings:
		//   token 0: [1.0, 2.0]
		//   token 1: [3.0, 4.0]
		//   token 2: [5.0, 6.0]  (masked out)
		// Attention mask: [1, 1, 0]
		// Expected: mean of tokens 0,1 = [(1+3)/2, (2+4)/2] = [2.0, 3.0]
		tokenEmbeddings := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}
		attentionMask := []int64{1, 1, 0}
		result := MeanPool(tokenEmbeddings, attentionMask, 1, 3, 2)

		require.Len(t, result, 2)
		require.InDelta(t, 2.0, result[0], 1e-6)
		require.InDelta(t, 3.0, result[1], 1e-6)
	})

	t.Run("batch", func(t *testing.T) {
		// batch=2, seq=2, dims=2
		// Batch 0: tokens [[1,2],[3,4]], mask [1,1] → mean [2, 3]
		// Batch 1: tokens [[5,6],[7,8]], mask [1,0] → mean [5, 6]
		tokenEmbeddings := []float32{
			1.0, 2.0, 3.0, 4.0, // batch 0
			5.0, 6.0, 7.0, 8.0, // batch 1
		}
		attentionMask := []int64{1, 1, 1, 0}
		result := MeanPool(tokenEmbeddings, attentionMask, 2, 2, 2)

		require.Len(t, result, 4)
		// Batch 0
		require.InDelta(t, 2.0, result[0], 1e-6)
		require.InDelta(t, 3.0, result[1], 1e-6)
		// Batch 1
		require.InDelta(t, 5.0, result[2], 1e-6)
		require.InDelta(t, 6.0, result[3], 1e-6)
	})

	t.Run("all_masked", func(t *testing.T) {
		// All tokens masked out — should not panic, returns near-zero.
		tokenEmbeddings := []float32{1.0, 2.0, 3.0, 4.0}
		attentionMask := []int64{0, 0}
		result := MeanPool(tokenEmbeddings, attentionMask, 1, 2, 2)

		require.Len(t, result, 2)
		// With maskSum clamped to 1e-9, values should be very close to zero.
		require.InDelta(t, 0.0, result[0], 1e-3)
		require.InDelta(t, 0.0, result[1], 1e-3)
	})
}

func TestL2Normalize(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		// [3, 4] → norm=5, normalized=[0.6, 0.8]
		embeddings := []float32{3.0, 4.0}
		result := L2Normalize(embeddings, 1, 2)

		require.Len(t, result, 2)
		require.InDelta(t, 0.6, result[0], 1e-6)
		require.InDelta(t, 0.8, result[1], 1e-6)

		// Verify unit norm.
		norm := math.Sqrt(float64(result[0])*float64(result[0]) +
			float64(result[1])*float64(result[1]))
		require.InDelta(t, 1.0, norm, 1e-6)
	})

	t.Run("batch", func(t *testing.T) {
		// Batch of 2 vectors.
		embeddings := []float32{3.0, 4.0, 0.0, 5.0}
		result := L2Normalize(embeddings, 2, 2)

		require.Len(t, result, 4)
		// Vector 0: [3,4] → [0.6, 0.8]
		require.InDelta(t, 0.6, result[0], 1e-6)
		require.InDelta(t, 0.8, result[1], 1e-6)
		// Vector 1: [0,5] → [0, 1]
		require.InDelta(t, 0.0, result[2], 1e-6)
		require.InDelta(t, 1.0, result[3], 1e-6)
	})

	t.Run("zero_vector", func(t *testing.T) {
		// Zero vector should stay zero (no division by zero).
		embeddings := []float32{0.0, 0.0, 0.0}
		result := L2Normalize(embeddings, 1, 3)

		require.Len(t, result, 3)
		require.Equal(t, float32(0.0), result[0])
		require.Equal(t, float32(0.0), result[1])
		require.Equal(t, float32(0.0), result[2])
	})
}

func TestPostProcess(t *testing.T) {
	// batch=1, seq=2, dims=2
	// Token embeddings: [[3, 0], [0, 4]], mask [1, 1]
	// Mean pool: [(3+0)/2, (0+4)/2] = [1.5, 2.0]
	// L2 norm: sqrt(1.5^2 + 2.0^2) = sqrt(2.25+4.0) = sqrt(6.25) = 2.5
	// Normalized: [1.5/2.5, 2.0/2.5] = [0.6, 0.8]
	tokenEmbeddings := []float32{3.0, 0.0, 0.0, 4.0}
	attentionMask := []int64{1, 1}
	result := PostProcess(tokenEmbeddings, attentionMask, 1, 2, 2)

	require.Len(t, result, 2)
	require.InDelta(t, 0.6, result[0], 1e-6)
	require.InDelta(t, 0.8, result[1], 1e-6)

	// Verify unit norm.
	norm := math.Sqrt(float64(result[0])*float64(result[0]) +
		float64(result[1])*float64(result[1]))
	require.InDelta(t, 1.0, norm, 1e-6)
}
