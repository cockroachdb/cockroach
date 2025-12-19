// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Yielding relies on our fork of Go. See yield_nofork.go.
//go:build bazel

package admission

import "runtime"

//gcassert:inline
func runtimeYield() {
	runtime.Yield()
}
