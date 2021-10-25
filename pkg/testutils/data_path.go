// Copyright 2020 The Cockroach Authors.
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
	"path"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/stretchr/testify/require"
)

// TestDataPath returns a path to an asset in the testdata directory. It knows
// to access accesses the right path when executing under bazel.
//
// For example, if there is a file testdata/a.txt, you can get a path to that
// file using TestDataPath(t, "a.txt").
func TestDataPath(t testing.TB, relative ...string) string {
	relative = append([]string{"testdata"}, relative...)
	// dev notifies the library that the test is running in a subdirectory of the
	// workspace with the environment variable below.
	if bazel.BuiltWithBazel() {
		//lint:ignore SA4006 apparently a linter bug.
		cockroachWorkspace, set := envutil.EnvString("COCKROACH_WORKSPACE", 0)
		if set {
			return path.Join(cockroachWorkspace, bazel.RelativeTestTargetPath(), path.Join(relative...))
		}
		runfiles, err := bazel.RunfilesPath()
		require.NoError(t, err)
		return path.Join(runfiles, bazel.RelativeTestTargetPath(), path.Join(relative...))
	}

	// Otherwise we're in the package directory and can just return a relative path.
	ret := path.Join(relative...)
	ret, err := filepath.Abs(ret)
	require.NoError(t, err)
	return ret
}
