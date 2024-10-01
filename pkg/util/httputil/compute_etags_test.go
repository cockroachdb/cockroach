// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package httputil

import (
	"archive/tar"
	"bytes"
	"testing"
	"testing/fstest"

	"github.com/cockroachdb/cockroach/pkg/util/assetbundle"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestComputeEtags_WithFiles(t *testing.T) {
	mapfs := fstest.MapFS{
		"dist/foo.js": &fstest.MapFile{
			Data: []byte("console.log('hello world');"),
		},
		"bar.txt": {
			Data: []byte("bar.txt contents"),
		},
		"lorem/ipsum/dolor.png": {
			Data: []byte("pretend this is a png"),
		},
	}
	fsys, err := mapfs.Sub(".")
	require.NoError(t, err)

	expected := map[string]string{
		"/dist/foo.js":           "ad43b0d7fb055db16583c156c5507ed58c157e9d",
		"/bar.txt":               "9b66cb7326bd7d5ded65d24c151438edfcaa5045",
		"/lorem/ipsum/dolor.png": "7ee5592b671378807bd078624358d5140c6d8512",
	}

	hashes := make(map[string]string)
	result := ComputeEtags(fsys, hashes)

	require.NoError(t, result)
	require.EqualValues(t, expected, hashes)
	require.NotEmpty(t, hashes)
}

func TestComputeEtags_EmptyFS(t *testing.T) {
	mapfs := fstest.MapFS{}
	fsys, err := mapfs.Sub(".")
	require.NoError(t, err)

	expected := map[string]string{}

	hashes := make(map[string]string)
	result := ComputeEtags(fsys, hashes)

	require.NoError(t, result)
	require.EqualValues(t, expected, hashes)
	require.Empty(t, hashes)
}

func TestComputeEtags_NilMap(t *testing.T) {
	mapfs := fstest.MapFS{
		"dist/foo.js": &fstest.MapFile{
			Data: []byte("console.log('hello world');"),
		},
		"bar.txt": {
			Data: []byte("bar.txt contents"),
		},
		"lorem/ipsum/dolor.png": {
			Data: []byte("pretend this is a png"),
		},
	}
	fsys, err := mapfs.Sub(".")
	require.NoError(t, err)

	result := ComputeEtags(fsys, nil)
	require.Errorf(t, result, "Unable to hash files without a hash destination")
}

func TestComputeEtags_TarFS(t *testing.T) {
	// Build a tar ball
	var tarContents bytes.Buffer
	tarWriter := tar.NewWriter(&tarContents)

	addFileToTar := func(path string, contents string) {
		err := tarWriter.WriteHeader(&tar.Header{Name: path, Size: int64(len(contents))})
		require.NoError(t, err)
		_, err = tarWriter.Write([]byte(contents))
		require.NoError(t, err)
	}
	addFileToTar("dist/foo.js", "console.log('hello world');")
	addFileToTar("bar.txt", "bar.txt contents")
	addFileToTar("lorem/ipsum/dolor.png", "pretend this is a png")

	require.NoError(t, tarWriter.Close())

	// Compress the tar ball
	var compressedTarContents bytes.Buffer
	writer, _ := zstd.NewWriter(&compressedTarContents)
	_, err := writer.Write(tarContents.Bytes())
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	// Create an io/fs.FS from the .tar.zst file
	fs, err := assetbundle.AsFS(bytes.NewBuffer(compressedTarContents.Bytes()))
	require.NoError(t, err)

	// Hash the files in that .tar.zst file system
	hashes := make(map[string]string)
	result := ComputeEtags(fs, hashes)
	require.NoError(t, result)

	expected := map[string]string{
		"/dist/foo.js":           "ad43b0d7fb055db16583c156c5507ed58c157e9d",
		"/bar.txt":               "9b66cb7326bd7d5ded65d24c151438edfcaa5045",
		"/lorem/ipsum/dolor.png": "7ee5592b671378807bd078624358d5140c6d8512",
	}
	require.NoError(t, result)
	require.EqualValues(t, expected, hashes)
	require.NotEmpty(t, hashes)
}
