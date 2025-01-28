// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package benchmark

import "flag"

var warmupConfig struct {
	iterations *int
}

// RegisterWarmupFlags adds flags to the test binary necessary to enable running
// warmup iterations before benchmarking. Add this to the top of the `TestMain`
// function.
func RegisterWarmupFlags() {
	warmupConfig.iterations = flag.Int("warmup-iterations", 0, "number of iterations to run before benchmarking")
}

// Warmup runs the given function the specified number of times before
// benchmarking. This is useful for warming up caches before benchmarking. This
// should be run before the benchmarking loop, and the benchmark timer should be
// reset after the warmup.
func Warmup(f func()) {
	if *warmupConfig.iterations <= 0 {
		return
	}
	for i := 0; i < *warmupConfig.iterations; i++ {
		f()
	}
}
