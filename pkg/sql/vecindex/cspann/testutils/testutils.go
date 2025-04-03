// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
)

// LoadFeatures loads up to 10K 512 dimension float32 unit vectors that are laid
// out contiguously in row-wise order. These vectors are OpenAI embeddings of
// images. If count < 10K, then a subset of the features are returned.
func LoadFeatures(t testing.TB, count int) vector.Set {
	var filePath string
	if bazel.BuiltWithBazel() {
		runfile, err := bazel.Runfile("pkg/sql/vecindex/cspann/testdata/features_10000.gob")
		require.NoError(t, err)
		filePath = runfile
	} else {
		// Get the absolute path of this test file.
		_, testFile, _, ok := runtime.Caller(0)
		require.True(t, ok)

		// Point to the features file.
		parentDir := filepath.Dir(testFile)
		filePath = filepath.Join(parentDir, "..", "testdata", "features_10000.gob")
	}

	f, err := os.Open(filePath)
	require.NoError(t, err)
	defer f.Close()

	decoder := gob.NewDecoder(f)
	var data []float32
	err = decoder.Decode(&data)
	require.NoError(t, err)

	return vector.MakeSetFromRawData(data[:count*512], 512)
}

// RoundFloat rounds the given float32 value using the given precision.
func RoundFloat(s float32, prec int) float32 {
	return float32(scalar.Round(float64(s), prec))
}

// RoundFloats rounds all float32 values in the slice using the given precision.
func RoundFloats(s []float32, prec int) []float32 {
	if len(s) == 0 {
		return s
	}
	t := make([]float32, len(s))
	copy(t, s)
	num32.Round(t, prec)
	return t
}

// NormalizeSlice returns nil rather than the empty slice.
func NormalizeSlice[T any](s []T) []T {
	if len(s) == 0 {
		return []T(nil)
	}
	return s
}
