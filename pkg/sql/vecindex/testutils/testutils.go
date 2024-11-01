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
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

// LoadFeatures loads up to 10K 512 dimension float32 unit vectors that are laid
// out contiguously in row-wise order. These vectors are OpenAI embeddings of
// images. If count < 10K, then a subset of the features are returned.
func LoadFeatures(t testing.TB, count int) vector.Set {
	var filePath string
	if bazel.BuiltWithBazel() {
		runfile, err := bazel.Runfile("pkg/sql/vecindex/testdata/features_10000.gob")
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
