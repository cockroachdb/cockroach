// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
)

// TempDir creates a directory and a function to clean it up at the end of the
// test.
func TempDir(t TestNamedFatalerLogger) (string, func()) {
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

	dir, err := os.MkdirTemp(tmpDir, fileutil.EscapeFilename(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			// Temp dir cleanup may race with process shutdown, where
			// some process is writing to dir, just as we try to shut down.
			// This should not fail the test.
			t.Logf("encountered error %s while removing temp dir %s", err, dir)
		}
	}
	return dir, cleanup
}
