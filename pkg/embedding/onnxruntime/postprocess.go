// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package onnxruntime

import "math"

// MeanPool computes the mean-pooled sentence embedding from per-token
// embeddings, weighted by the attention mask. This is the standard pooling
// strategy used by sentence-transformer models.
//
// Parameters:
//   - tokenEmbeddings: float32 array of shape [batchSize, seqLen, dims]
//   - attentionMask:   int64 array of shape [batchSize, seqLen]
//   - batchSize, seqLen, dims: tensor dimensions
//
// Returns a float32 array of shape [batchSize, dims].
func MeanPool(
	tokenEmbeddings []float32, attentionMask []int64, batchSize, seqLen, dims int,
) []float32 {
	result := make([]float32, batchSize*dims)

	for b := 0; b < batchSize; b++ {
		// Sum the attention mask for this sequence.
		var maskSum float64
		for s := 0; s < seqLen; s++ {
			maskSum += float64(attentionMask[b*seqLen+s])
		}
		if maskSum < 1e-9 {
			maskSum = 1e-9
		}

		// Weighted sum of token embeddings.
		for d := 0; d < dims; d++ {
			var sum float64
			for s := 0; s < seqLen; s++ {
				mask := float64(attentionMask[b*seqLen+s])
				embedding := float64(tokenEmbeddings[b*seqLen*dims+s*dims+d])
				sum += embedding * mask
			}
			result[b*dims+d] = float32(sum / maskSum)
		}
	}

	return result
}

// L2Normalize normalizes each vector in the batch to unit length (L2 norm).
//
// Parameters:
//   - embeddings: float32 array of shape [batchSize, dims]
//   - batchSize, dims: tensor dimensions
//
// Returns a new float32 array of shape [batchSize, dims] with each vector
// normalized to unit length. Zero vectors are left as zero.
func L2Normalize(embeddings []float32, batchSize, dims int) []float32 {
	result := make([]float32, batchSize*dims)

	for b := 0; b < batchSize; b++ {
		// Compute L2 norm.
		var normSq float64
		for d := 0; d < dims; d++ {
			v := float64(embeddings[b*dims+d])
			normSq += v * v
		}
		norm := math.Sqrt(normSq)
		if norm < 1e-12 {
			// Zero vector — leave as zero.
			continue
		}

		// Normalize.
		for d := 0; d < dims; d++ {
			result[b*dims+d] = float32(float64(embeddings[b*dims+d]) / norm)
		}
	}

	return result
}

// PostProcess applies mean pooling followed by L2 normalization to convert
// per-token embeddings into normalized sentence embeddings.
//
// This is the standard post-processing pipeline for sentence-transformer
// models like all-MiniLM-L6-v2 when the ONNX model outputs token-level
// embeddings (last_hidden_state) rather than pre-pooled sentence embeddings.
//
// Parameters:
//   - tokenEmbeddings: float32 array of shape [batchSize, seqLen, dims]
//   - attentionMask:   int64 array of shape [batchSize, seqLen]
//   - batchSize, seqLen, dims: tensor dimensions
//
// Returns a float32 array of shape [batchSize, dims].
func PostProcess(
	tokenEmbeddings []float32, attentionMask []int64, batchSize, seqLen, dims int,
) []float32 {
	pooled := MeanPool(tokenEmbeddings, attentionMask, batchSize, seqLen, dims)
	return L2Normalize(pooled, batchSize, dims)
}
