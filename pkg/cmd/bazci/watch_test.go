// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package main

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func assertFileCopiedVerbatim(t *testing.T, relPath string) {
	testdata := testutils.TestDataPath(t)
	actual, err := ioutil.ReadFile(path.Join(artifactsDir, relPath))
	assert.Nil(t, err)
	expected, err := ioutil.ReadFile(path.Join(testdata, relPath))
	assert.Nil(t, err)
	assert.Equal(t, actual, expected)
}

func assertFilesIdentical(t *testing.T, actualPath, expectedPath string) {
	actual, err := ioutil.ReadFile(actualPath)
	assert.Nil(t, err)
	expected, err := ioutil.ReadFile(expectedPath)
	assert.Nil(t, err)
	assert.Equal(t, actual, expected)
}

func TestWatch(t *testing.T) {
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()
	artifactsDir = dir
	testdata := testutils.TestDataPath(t)
	info := buildInfo{
		binDir:      path.Join(testdata, "bazel-bin"),
		testlogsDir: path.Join(testdata, "bazel-testlogs"),
		goBinaries:  []string{"//pkg/cmd/fake_bin:fake_bin"},
		tests:       []string{"//pkg/rpc:rpc_test", "//pkg/server:server_test"},
	}
	completion := make(chan error, 1)
	completion <- nil

	err := makeWatcher(completion, info).Watch()

	assert.Nil(t, err)
	assertFileCopiedVerbatim(t, "bazel-testlogs/pkg/rpc/rpc_test/test.log")
	assertFileCopiedVerbatim(t, "bazel-testlogs/pkg/server/server_test/shard_1_of_16/test.log")
	assertFileCopiedVerbatim(t, "bazel-testlogs/pkg/server/server_test/shard_2_of_16/test.log")
	assertFileCopiedVerbatim(t, "bazel-bin/pkg/cmd/fake_bin/fake_bin_/fake_bin")
	// check the xml file was munged correctly.
	assertFilesIdentical(t, path.Join(artifactsDir, "bazel-testlogs/pkg/rpc/rpc_test/test.xml"),
		path.Join(testdata, "expected/rpc_test.xml"))
	assertFilesIdentical(t, path.Join(artifactsDir, "bazel-testlogs/pkg/server/server_test/shard_1_of_16/test.xml"),
		path.Join(testdata, "expected/server_1_test.xml"))
	assertFilesIdentical(t, path.Join(artifactsDir, "bazel-testlogs/pkg/server/server_test/shard_2_of_16/test.xml"),
		path.Join(testdata, "expected/server_2_test.xml"))
}
