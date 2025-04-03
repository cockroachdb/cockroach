// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !bazel

package bazel

// This file contains stub implementations for non-bazel builds.
// See bazel.go for full documentation on the contracts of these functions.

// BuiltWithBazel returns true iff this library was built with Bazel.
func BuiltWithBazel() bool {
	return false
}

// InBazelTest returns true iff called from a test run by Bazel.
func InBazelTest() bool {
	return false
}

// InTestWrapper returns true iff called from Bazel's generated test wrapper.
func InTestWrapper() bool {
	return false
}

// FindBinary is not implemented.
func FindBinary(pkg, name string) (string, bool) {
	panic("not build with Bazel")
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

// NewTmpDir is not implemented.
func NewTmpDir(prefix string) (string, error) {
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
