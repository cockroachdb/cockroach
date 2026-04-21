// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package embedding

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/embedding/onnxruntime"
	"github.com/stretchr/testify/require"
)

func skipIfNoRuntime(t *testing.T) {
	t.Helper()
	if os.Getenv("ONNX_RUNTIME_LIB_DIR") == "" {
		t.Skip("ONNX_RUNTIME_LIB_DIR not set; skipping integration test")
	}
}

func initRuntime(t *testing.T) {
	t.Helper()
	libDir := os.Getenv("ONNX_RUNTIME_LIB_DIR")
	_, err := onnxruntime.EnsureInit(
		onnxruntime.EnsureInitErrorDisplayPrivate, libDir,
	)
	require.NoError(t, err)
}

func testModelPath(t *testing.T) string {
	t.Helper()
	candidates := []string{
		filepath.Join("onnxruntime", "testdata", "test_model.onnx"),
		filepath.Join("pkg", "embedding", "onnxruntime", "testdata", "test_model.onnx"),
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	t.Skip("no test model available")
	return ""
}

func testVocabPath(t *testing.T) string {
	t.Helper()
	candidates := []string{
		filepath.Join("testdata", "tiny_vocab_32.txt"),
		filepath.Join("pkg", "embedding", "testdata", "tiny_vocab_32.txt"),
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	t.Skip("no test vocab available")
	return ""
}

func TestEngine(t *testing.T) {
	skipIfNoRuntime(t)
	initRuntime(t)

	modelPath := testModelPath(t)
	vocabPath := testVocabPath(t)

	t.Run("embed_single", func(t *testing.T) {
		eng, err := NewEngine(modelPath, vocabPath, WithMaxSeqLen(8))
		require.NoError(t, err)
		defer eng.Close()

		require.Greater(t, eng.Dims(), 0)

		vec, err := eng.Embed("hello world")
		require.NoError(t, err)
		require.Len(t, vec, eng.Dims())

		// Verify the vector is unit-normalized (L2 norm ≈ 1.0).
		var norm float64
		for _, v := range vec {
			norm += float64(v) * float64(v)
		}
		norm = math.Sqrt(norm)
		require.InDelta(t, 1.0, norm, 1e-3)
	})

	t.Run("embed_batch", func(t *testing.T) {
		eng, err := NewEngine(modelPath, vocabPath, WithMaxSeqLen(8))
		require.NoError(t, err)
		defer eng.Close()

		vecs, err := eng.EmbedBatch([]string{"hello", "world"})
		require.NoError(t, err)
		require.Len(t, vecs, 2)
		for i, vec := range vecs {
			require.Lenf(t, vec, eng.Dims(), "vector %d", i)
		}
	})

	t.Run("embed_empty_string", func(t *testing.T) {
		eng, err := NewEngine(modelPath, vocabPath, WithMaxSeqLen(8))
		require.NoError(t, err)
		defer eng.Close()

		vec, err := eng.Embed("")
		require.NoError(t, err)
		require.Len(t, vec, eng.Dims())
	})

	t.Run("embed_batch_empty", func(t *testing.T) {
		eng, err := NewEngine(modelPath, vocabPath, WithMaxSeqLen(8))
		require.NoError(t, err)
		defer eng.Close()

		vecs, err := eng.EmbedBatch(nil)
		require.NoError(t, err)
		require.Nil(t, vecs)
	})
}
