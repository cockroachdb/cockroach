// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package embedding provides a high-level API for generating vector
// embeddings from text using transformer ONNX models. It combines a
// WordPiece tokenizer with ONNX Runtime inference and post-processing
// (mean pooling + L2 normalization) into a single text-to-embedding
// pipeline.
package embedding

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/embedding/onnxruntime"
	"github.com/cockroachdb/cockroach/pkg/embedding/tokenizer"
	"github.com/cockroachdb/errors"
)

// Engine combines a WordPiece tokenizer with an ONNX model to produce
// vector embeddings from raw text. It is safe for concurrent use.
type Engine struct {
	tokenizer *tokenizer.Tokenizer
	model     *onnxruntime.Model
	dims      int
}

type engineOptions struct {
	numThreads int
	maxSeqLen  int
}

// EngineOption configures the Engine.
type EngineOption func(*engineOptions)

// WithNumThreads sets the number of intra-op threads for ONNX Runtime
// inference. 0 (the default) lets ONNX Runtime choose.
func WithNumThreads(n int) EngineOption {
	return func(o *engineOptions) { o.numThreads = n }
}

// WithMaxSeqLen sets the maximum sequence length for tokenization.
// Inputs longer than this are truncated. The default is 256.
func WithMaxSeqLen(n int) EngineOption {
	return func(o *engineOptions) { o.maxSeqLen = n }
}

// NewEngine creates an Engine by loading an ONNX model and a
// vocabulary file. The model and vocab must be compatible (the vocab
// size must match the model's embedding layer).
func NewEngine(modelPath, vocabPath string, opts ...EngineOption) (*Engine, error) {
	o := engineOptions{
		numThreads: 0,
		maxSeqLen:  256,
	}
	for _, apply := range opts {
		apply(&o)
	}

	f, err := os.Open(vocabPath)
	if err != nil {
		return nil, errors.Wrapf(err, "opening vocab %s", vocabPath)
	}
	defer f.Close()

	vocab, err := tokenizer.LoadVocab(f)
	if err != nil {
		return nil, errors.Wrapf(err, "loading vocab from %s", vocabPath)
	}

	tok := tokenizer.NewTokenizer(
		vocab,
		tokenizer.WithMaxSeqLen(o.maxSeqLen),
	)

	model, err := onnxruntime.LoadModel(modelPath, o.numThreads)
	if err != nil {
		return nil, errors.Wrapf(err, "loading model from %s", modelPath)
	}

	return &Engine{
		tokenizer: tok,
		model:     model,
		dims:      model.Dims(),
	}, nil
}

// Close releases all resources held by the Engine. After Close, the
// Engine must not be used.
func (e *Engine) Close() {
	if e.model != nil {
		e.model.Close()
		e.model = nil
	}
}

// Dims returns the embedding dimension of the loaded model.
func (e *Engine) Dims() int {
	return e.dims
}

// Embed produces a normalized embedding vector for a single text
// input. The returned slice has length Dims().
func (e *Engine) Embed(text string) ([]float32, error) {
	enc := e.tokenizer.Encode(text)
	seqLen := len(enc.InputIDs)

	raw, err := e.model.RunInference(
		enc.InputIDs, enc.AttentionMask, 1, seqLen,
	)
	if err != nil {
		return nil, errors.Wrap(err, "running inference")
	}

	result := onnxruntime.PostProcess(
		raw, enc.AttentionMask, 1, seqLen, e.dims,
	)
	return result, nil
}

// Tokenizer returns the engine's tokenizer. This is useful for
// components like the chunker that need token counting.
func (e *Engine) Tokenizer() *tokenizer.Tokenizer {
	return e.tokenizer
}

// EmbedBatch produces normalized embedding vectors for multiple texts.
// Each inner slice has length Dims().
func (e *Engine) EmbedBatch(texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	inputIDs, attentionMask, batchSize, seqLen :=
		e.tokenizer.EncodeBatch(texts)

	raw, err := e.model.RunInference(
		inputIDs, attentionMask, batchSize, seqLen,
	)
	if err != nil {
		return nil, errors.Wrap(err, "running batch inference")
	}

	flat := onnxruntime.PostProcess(
		raw, attentionMask, batchSize, seqLen, e.dims,
	)

	// Split the flat [batchSize * dims] slice into individual vectors.
	result := make([][]float32, batchSize)
	for i := range result {
		result[i] = flat[i*e.dims : (i+1)*e.dims]
	}
	return result, nil
}
