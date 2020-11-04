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
	"os"
	"path"
	"strings"

	"github.com/cockroachdb/errors"
)

//
// Utilities in this file are intended to provide helpers for tests
// to access system resources in a correct way, when executing under bazel.
//
// See  https://docs.bazel.build/versions/master/test-encyclopedia.html for more details
// on the directory layout.
//
// When test is compiled with bazel, bazel will create a directory, called RUNFILES directory,
// containing all of the resources required to execute such test (libraries, final binary,
// data resources, etc).
//
// Bazel also sets up various environmental variables that point to the location(s) of
// those resources.
//

// Name of the environment variable pointing to the absolute path to the base of the RUNFILES tree.
const testSrcDirEnv = "TEST_SRCDIR"

// Name of the environment variable containing the name of the "workspace".
const testWorkspaceEnv = "TEST_WORKSPACE"

// Name of the environment variable pointing to the absolute path of the
// temporary directory created for the execution of the test.
const testTmpDirEnv = "TEST_TMPDIR"

// Name of the environment variable containing the bazel target path (//pkg/cmd/foo:bar).
const testTargetEnv = "TEST_TARGET"

// runningUnderBazel returns true if the test is executed by bazel.
func runningUnderBazel() bool {
	return os.Getenv(testSrcDirEnv) != ""
}

func requireEnv(env string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	panic(errors.AssertionFailedf("expected value for env: %s", env))
}

// TestSrcDir returns the path to the "source" tree.
//
// If running under bazel, this will point to a private, *readonly*
// directory containing symlinks (or copies) of the test data dependencies.
// This directory must be treated readonly.  It's an error to try to modify
// anything under this directory: though the operation may succeed, the test
// would not be hermetic, and may fail under other environments.
func TestSrcDir() string {
	// If testSrcDirEnv is not set, it means we are not running under bazel,
	// and so we can use "" as our directory which should point to the
	// src root.
	if srcDir := os.Getenv(testSrcDirEnv); srcDir != "" {
		return srcDir
	}
	return ""
}

// bazeRelativeTargetPath returns relative path to the package
// of the current test.
func bazelRelativeTargetPath() string {
	target := os.Getenv(testTargetEnv)
	if target == "" {
		return ""
	}

	// Drop target name.
	if last := strings.LastIndex(target, ":"); last > 0 {
		target = target[:last]
	}
	return strings.TrimPrefix(target, "//")
}

// TestDataPath returns a path to the directory containing test data files.
//
// Test files are usually checked into the repository under "testdata" directory.
// If we are not using bazel, then the test executes in the directory of
// the actual test, so the files can be referenced via "testdata/subdir/file" relative path.
//
// However, if we are running under bazel, the data files are specified
// via go_test "data" attribute.  These files, in turn, are available under RUNFILES directory.
// This helper attempts to construct appropriate path to the RUNFILES directory
// containing test data files, given the relative (to the test) path components.
//
func TestDataPath(relative ...string) string {
	if runningUnderBazel() {
		return path.Join(TestSrcDir(), requireEnv(testWorkspaceEnv), bazelRelativeTargetPath(),
			path.Join(relative...))
	}
	return path.Join(relative...)
}
