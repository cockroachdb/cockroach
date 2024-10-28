// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !bazel_code_cover
// +build !bazel_code_cover

// Allows instrumented binaries to output code coverage
// data.
package buildutil 

const CrdbCoverageBuild = false

// MaybeInitCodeCoverage does nothing unless we are building in a special
// coverage collection mode. See the same function in the corresponding
// code_cover_on.go file
func MaybeInitCodeCoverage() {}
