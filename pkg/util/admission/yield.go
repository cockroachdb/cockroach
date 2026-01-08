// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Yielding relies on our fork of Go. See yield_nofork.go.
//go:build bazel

package admission

import (
	"runtime"
	"time"
)

// runtimeYield calls runtime.Yield and returns the duration the goroutine was
// delayed.
//
//gcassert:inline
func runtimeYield() time.Duration {
	return time.Duration(runtime.Yield())
}
