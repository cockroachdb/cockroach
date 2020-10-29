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
	"testing"
)

const testSrcDirEnv = "TEST_SRCDIR"
const testTmpDirEnv = "TEST_TMPDIR"

// RunningUnderBazel returns true if the test is executed by bazel.
func RunningUnderBazel() bool {
	return os.Getenv(testSrcDirEnv) != ""
}

func requireEnv(env string) string {
	v := os.Getenv(env)
	if v == "" {
		panic("Expected value for " + env)
	}
	return v
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
	return os.Getenv(testSrcDirEnv)
}

// TestTempDir returns a temporary directory and a cleanup function for a test.
func TestTempDir(t testing.TB) (string, func()) {
	if RunningUnderBazel() {
		// Bazel sets up private temp directories for each test.
		return requireEnv(testTmpDirEnv), func() {}
	}
	return TempDir(t)
}
