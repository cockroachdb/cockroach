// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
)

// TempDir creates a directory and a function to clean it up at the end of the
// test.
func TempDir(t testing.TB) (string, func()) {
	tmpDir := ""
	if bazel.BuiltWithBazel() {
		// Bazel sets up private temp directories for each test.
		// Normally, this private temp directory will be cleaned up automatically.
		// However, we do use external tools (such as stress) which re-execute the
		// same test multiple times.  Bazel, on the other hand, does not know about
		// this, and only creates this temporary directory once.  So, ensure we create
		// a unique temporary directory underneath bazel TEST_TMPDIR.
		tmpDir = bazel.TestTmpDir()
	}

	dir, err := ioutil.TempDir(tmpDir, fileutil.EscapeFilename(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}
	return dir, cleanup
}
