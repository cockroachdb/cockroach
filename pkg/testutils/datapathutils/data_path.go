// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package datapathutils

import (
	"fmt"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/stretchr/testify/require"
)

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
