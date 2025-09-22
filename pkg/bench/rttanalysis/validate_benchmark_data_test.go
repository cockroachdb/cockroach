// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

// NOTE: If you change the number of shards, you must also update the
// shard_count in BUILD.bazel to match.
const shardCount = 4

// Validate that shardCount matches TEST_TOTAL_SHARDS environment variable at init time
var _ = func() int {
	totalShardsStr, found := envutil.ExternalEnvString("TEST_TOTAL_SHARDS", 1)
	if totalShardsStr == "" || !found {
		return 0
	}
	totalShards, err := strconv.Atoi(totalShardsStr)
	if err != nil {
		return 0
	}
	if totalShards != shardCount {
		panic("shardCount mismatch: update shard_count in pkg/bench/rttanalysis/BUILD.bazel to match shardCount constant")
	}
	return 0
}()

func TestBenchmarkExpectationShard1(t *testing.T) {
	reg.RunExpectationsSharded(t, 1, shardCount)
}

func TestBenchmarkExpectationShard2(t *testing.T) {
	reg.RunExpectationsSharded(t, 2, shardCount)
}

func TestBenchmarkExpectationShard3(t *testing.T) {
	reg.RunExpectationsSharded(t, 3, shardCount)
}

func TestBenchmarkExpectationShard4(t *testing.T) {
	reg.RunExpectationsSharded(t, 4, shardCount)
}
