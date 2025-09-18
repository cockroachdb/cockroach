// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

// NOTE: If you change the number of shards, you must also update the
// shard_count in BUILD.bazel to match.

func TestBenchmarkExpectationShard1(t *testing.T) {
	reg.RunExpectations(t, 1, 4)
}

func TestBenchmarkExpectationShard2(t *testing.T) {
	reg.RunExpectations(t, 2, 4)
}

func TestBenchmarkExpectationShard3(t *testing.T) {
	reg.RunExpectations(t, 3, 4)
}

func TestBenchmarkExpectationShard4(t *testing.T) {
	reg.RunExpectations(t, 4, 4)
}
