// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// See grunning.Supported for an explanation behind this build tag.
//
//go:build bazel

package grunning

import "runtime"

const supported = true

// grunningnanos returns the running time observed by the current goroutine.
func grunningnanos() int64 {
	return runtime.Grunningnanos()
}
