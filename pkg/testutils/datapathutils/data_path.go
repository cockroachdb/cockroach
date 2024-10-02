// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package datapathutils

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/stretchr/testify/require"
)

// DebuggableTempDir returns a temporary directory that is appropriate for
// retaining leftover artifacts for debugging in case the test fails. That makes
// it an appropriate replacement for os.TempDir() in the case where the test is
// executing remotely. If the test is not executing remotely, or the test is not
// built with Bazel, then os.TempDir() is returned (the person executing the
// test should set up their $TMPDIR appropriately so they know where to find
// these leftover artifacts). If the test is executing remotely, then we instead
// use TEST_UNDECLARED_OUTPUTS_DIR, and if any files are left-over in this
// directory after the test exits, then Bazel will package them up into
// outputs.zip. If you are always going to clean up the files you create either
// way, testutils.TempDir() may be more convenient.
//
// This also means that os.MkdirTemp(datapathutils.DebuggableTempDir(), ...)
// is appropriate as a drop-in replacement for os.MkdirTemp("", ...)
func DebuggableTempDir() string {
	if bazel.BuiltWithBazel() {
		isRemote := os.Getenv("REMOTE_EXEC")
		if len(isRemote) > 0 {
			outputs := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
			if len(outputs) > 0 {
				return outputs
			}
		}
	}
	return os.TempDir()
}

// TestDataPath returns a path to an asset in the testdata directory. It knows
// to access the right path when executing under bazel.
//
// For example, if there is a file testdata/a.txt, you can get a path to that
// file using TestDataPath(t, "a.txt").
func TestDataPath(t testing.TB, relative ...string) string {
	relative = append([]string{"testdata"}, relative...)
	// dev notifies the library that the test is running in a subdirectory of the
	// workspace with the environment variable below.
	if bazel.BuiltWithBazel() {
		relative = append([]string{bazel.RelativeTestTargetPath()}, relative...)
		return RewritableDataPath(t, relative...)
	}

	// Otherwise we're in the package directory and can just return a relative path.
	ret := path.Join(relative...)
	ret, err := filepath.Abs(ret)
	require.NoError(t, err)
	return ret
}

// RewritableDataPath returns a path to an asset relative to the top of the
// workspace. Generally you should use TestDataPath if you're trying to access
// a file in your test's `testdata` directory, or bazel.Runfile if a read-only
// link to the file is OK. This function is only necessary if you need the path
// to a file that you can --rewrite.
func RewritableDataPath(t testing.TB, relative ...string) string {
	if bazel.BuiltWithBazel() {
		cockroachWorkspace, set := envutil.EnvString("COCKROACH_WORKSPACE", 0)
		if set {
			return path.Join(cockroachWorkspace, path.Join(relative...))
		}
		runfiles, err := bazel.RunfilesPath()
		require.NoError(t, err)
		relative = append([]string{runfiles}, relative...)
		return filepath.Join(relative...)
	}

	// Get the path to this file from the runtime.
	_, thisFilePath, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to get caller information")
	}
	cockroachWorkspace := thisFilePath
	for {
		dir := filepath.Base(cockroachWorkspace)
		cockroachWorkspace = filepath.Dir(cockroachWorkspace)
		const target = "pkg"
		if dir == target {
			break
		}
		if len(cockroachWorkspace) < 2 {
			panic(fmt.Errorf("did not find %q subdirectory in %q", target, thisFilePath))
		}
	}
	relative = append([]string{cockroachWorkspace}, relative...)
	return filepath.Join(relative...)
}
