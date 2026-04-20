// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package onnxruntime

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// skipIfNoRuntime skips the test if the ONNX Runtime library is not available.
func skipIfNoRuntime(t *testing.T) {
	t.Helper()
	libDir := os.Getenv("ONNX_RUNTIME_LIB_DIR")
	if libDir == "" {
		t.Skip("ONNX_RUNTIME_LIB_DIR not set; skipping ONNX Runtime integration test")
	}
}

// initForTest initializes the ONNX Runtime from the ONNX_RUNTIME_LIB_DIR
// environment variable, resetting any prior state.
func initForTest(t *testing.T) {
	t.Helper()
	resetForTesting()
	t.Cleanup(resetForTesting)
	libDir := os.Getenv("ONNX_RUNTIME_LIB_DIR")
	_, err := EnsureInit(EnsureInitErrorDisplayPrivate, libDir)
	require.NoError(t, err)
}

// testModelPath returns the path to a test ONNX model. If
// ONNX_TEST_MODEL_PATH is set, it is used. Otherwise, we look in testdata/.
func testModelPath(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("ONNX_TEST_MODEL_PATH"); p != "" {
		return p
	}
	candidates := []string{
		filepath.Join("testdata", "test_model.onnx"),
		filepath.Join("pkg", "embedding", "onnxruntime", "testdata", "test_model.onnx"),
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	t.Skip("no test model available (set ONNX_TEST_MODEL_PATH)")
	return ""
}

func TestEnsureInit(t *testing.T) {
	t.Run("missing_library_public_error", func(t *testing.T) {
		resetForTesting()
		defer resetForTesting()

		_, err := ensureInit(
			EnsureInitErrorDisplayPublic, "/nonexistent/path", "",
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "this operation is not available")
	})

	t.Run("missing_library_private_error", func(t *testing.T) {
		resetForTesting()
		defer resetForTesting()

		_, err := ensureInit(
			EnsureInitErrorDisplayPrivate, "/nonexistent/path", "",
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "onnxruntime")
	})

	t.Run("successful_init", func(t *testing.T) {
		skipIfNoRuntime(t)
		resetForTesting()
		defer resetForTesting()

		libDir := os.Getenv("ONNX_RUNTIME_LIB_DIR")
		loc, err := EnsureInit(EnsureInitErrorDisplayPrivate, libDir)
		require.NoError(t, err)
		require.NotEmpty(t, loc)
	})
}

func TestLoadModel(t *testing.T) {
	skipIfNoRuntime(t)
	initForTest(t)

	t.Run("load_and_introspect", func(t *testing.T) {
		modelPath := testModelPath(t)
		model, err := LoadModel(modelPath, 1)
		require.NoError(t, err)
		defer model.Close()

		require.Greater(t, model.NumInputs(), 0)
		require.Greater(t, model.NumOutputs(), 0)
		require.Greater(t, model.Dims(), 0)
	})

	t.Run("nonexistent_model", func(t *testing.T) {
		_, err := LoadModel("/nonexistent/model.onnx", 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "onnxruntime")
	})
}

func TestRunInference(t *testing.T) {
	skipIfNoRuntime(t)
	initForTest(t)

	modelPath := testModelPath(t)
	model, err := LoadModel(modelPath, 1)
	require.NoError(t, err)
	defer model.Close()

	t.Run("single_sequence", func(t *testing.T) {
		batchSize := 1
		seqLen := 4
		inputIDs := []int64{1, 2, 3, 0}
		attentionMask := []int64{1, 1, 1, 0}

		output, err := model.RunInference(inputIDs, attentionMask, batchSize, seqLen)
		require.NoError(t, err)
		require.NotEmpty(t, output)

		for i, v := range output {
			require.Falsef(t, v != v, "NaN at index %d", i)
		}
	})
}

func TestBatchInference(t *testing.T) {
	skipIfNoRuntime(t)
	initForTest(t)

	modelPath := testModelPath(t)
	model, err := LoadModel(modelPath, 1)
	require.NoError(t, err)
	defer model.Close()

	batchSize := 2
	seqLen := 4
	inputIDs := []int64{
		1, 2, 3, 0,
		4, 5, 0, 0,
	}
	attentionMask := []int64{
		1, 1, 1, 0,
		1, 1, 0, 0,
	}

	output, err := model.RunInference(inputIDs, attentionMask, batchSize, seqLen)
	require.NoError(t, err)
	require.NotEmpty(t, output)

	dims := model.Dims()
	require.True(t, len(output) == batchSize*seqLen*dims || len(output) == batchSize*dims,
		"unexpected output length %d (expected %d or %d)",
		len(output), batchSize*seqLen*dims, batchSize*dims)
}

func TestConcurrentInference(t *testing.T) {
	skipIfNoRuntime(t)
	initForTest(t)

	modelPath := testModelPath(t)
	model, err := LoadModel(modelPath, 1)
	require.NoError(t, err)
	defer model.Close()

	const numGoroutines = 4
	var wg sync.WaitGroup
	errs := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			inputIDs := []int64{1, 2, 3, 0}
			attentionMask := []int64{1, 1, 1, 0}
			_, errs[idx] = model.RunInference(inputIDs, attentionMask, 1, 4)
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		require.NoErrorf(t, e, "goroutine %d failed", i)
	}
}
