// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !bazel

package bazel

// This file contains stub implementations for non-bazel builds.
// See bazel.go for full documentation on the contracts of these functions.

// BuiltWithBazel returns true iff this library was built with Bazel.
func BuiltWithBazel() bool {
	return false
}

// Runfile is not implemented.
func Runfile(string) (string, error) {
	panic("not built with Bazel")
}

// RunfilesPath is not implemented.
func RunfilesPath() (string, error) {
	panic("not built with Bazel")
}

// TestTmpDir is not implemented.
func TestTmpDir() string {
	panic("not built with Bazel")
}

// RelativeTestTargetPath is not implemented.
func RelativeTestTargetPath() string {
	panic("not built with Bazel")
}

// SetGoEnv is not implemented.
func SetGoEnv() {
	panic("not built with Bazel")
}
