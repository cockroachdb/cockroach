// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package embedding

import "context"

// Embedder is the interface for all embedding providers (local ONNX
// and remote API-based). Implementations must be safe for concurrent
// use after construction.
type Embedder interface {
	// Embed produces a normalized embedding vector for a single text
	// input. The returned slice has length Dims().
	Embed(ctx context.Context, text string) ([]float32, error)

	// EmbedBatch produces normalized embedding vectors for multiple
	// texts. Each inner slice has length Dims().
	EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)

	// Dims returns the embedding dimension of the loaded model.
	Dims() int
}

// ImageEmbedder produces embedding vectors from raw image data.
// Multimodal models (e.g., Vertex AI multimodalembedding) implement
// both Embedder and ImageEmbedder on the same struct, allowing text
// and images to be embedded into the same vector space for
// cross-modal search.
type ImageEmbedder interface {
	// EmbedImage produces a normalized embedding vector for a single
	// image. The returned slice has length Dims().
	EmbedImage(ctx context.Context, image []byte) ([]float32, error)

	// EmbedImageBatch produces normalized embedding vectors for
	// multiple images. Each inner slice has length Dims().
	EmbedImageBatch(ctx context.Context, images [][]byte) ([][]float32, error)

	// Dims returns the embedding dimension of the model.
	Dims() int
}
