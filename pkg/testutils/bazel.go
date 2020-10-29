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
	"testing"
)

const testSrcDirEnv = "TEST_SRCDIR"
const testTmpDirEnv = "TEST_TMPDIR"
const testWorkspaceEnv = "TEST_WORKSPACE"
const testTargetEnv = "TEST_TARGET"

// RunningUnderBazel returns true if the test is executed by bazel.
func RunningUnderBazel() bool {
	return os.Getenv(testSrcDirEnv) != ""
}

func requireEnv(env string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	panic("expected value for env " + env)
}

// TestSrcDir returns the path to the "source" tree.
//
// If running under bazel, this will point to a private, *readonly*
// directory containing symlinks (or copies) of the test data depencies.
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

// TestTempDir returns a temporary directory and a cleanup function for a test.
func TestTempDir(t testing.TB) (string, func()) {
	if RunningUnderBazel() {
		// Bazel sets up private temp directories for each test.
		return requireEnv(testTmpDirEnv), func() {}
	}
	return TempDir(t)
}

// bazeRelativeTargetPath returns relative path to the package
// of the current test.
func bazelRelativeTargetPath() []string {
	target := os.Getenv(testTargetEnv)
	if target == "" {
		return nil
	}

	// Strip out leading package "//"
	target = strings.TrimPrefix(target, "//")

	// Strip out ":XXX" from the last component
	pieces := strings.Split(target, "/")

	last := len(pieces) - 1
	if last >= 0 {
		if idx := strings.LastIndex(pieces[last], ":"); idx > 0 {
			pieces[last] = pieces[last][:idx]
		}
	}
	return pieces
}

// TestDataPath returns a path to the directory containing test data files.
//
// Test files are usually checked into the repository under "testdata" directory.
// If we are not using bazel, then the test executes with current working directory of
// the actual test, so the files can be referenced via "testdata/subdir/file" relative path.
//
// However, if we are running under bazel, the data files are specified
// via go_test "data" attribute.  These files, in turn, are available under run files directory.
// This helper attempts to construct appropriate path to the run files directory
// containing test data files, given the relative (to the test) path components.
func TestDataPath(relative ...string) string {
	if RunningUnderBazel() {
		pieces := append(bazelRelativeTargetPath(), relative...)
		return path.Join(TestSrcDir(), os.Getenv(testWorkspaceEnv), path.Join(pieces...))
	}
	return path.Join(relative...)
}
