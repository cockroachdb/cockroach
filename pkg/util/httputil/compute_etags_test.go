// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package httputil

import (
	"testing"
	"testing/fstest"

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
