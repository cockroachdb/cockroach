// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/stretchr/testify/require"
)

// NOTE: If you change the number of shards, you must also update the
// shard_count in BUILD.bazel to match.
const shardCount = 5

// shardFunctions is the list of shard functions, which should match the
// shard count.
var shardFunctions = []func(t *testing.T){TestBenchmarkExpectationShard1,
	TestBenchmarkExpectationShard2,
	TestBenchmarkExpectationShard3,
	TestBenchmarkExpectationShard4,
	TestBenchmarkExpectationShard5}

const shardFunctionPrefix = "TestBenchmarkExpectationShard"

func init() {
	// Assert that the shard count matches.
	if len(shardFunctions) != shardCount {
		panic(fmt.Sprintf("shardCount mismatch: %d != %d", len(shardFunctions), shardCount))
	}
	// Validate the function names have the expected format.
	for i, shardFunction := range shardFunctions {
		// Extract the function name only.
		fullName := runtime.FuncForPC(reflect.ValueOf(shardFunction).Pointer()).Name()
		lastDot := strings.LastIndex(fullName, ".")
		functionName := fullName[lastDot+1:]
		expectedFunctionName := fmt.Sprintf("%s%d", shardFunctionPrefix, i+1)
		if functionName != expectedFunctionName {
			panic(fmt.Sprintf("shard function name mismatch: %s != %s", functionName, expectedFunctionName))
		}
	}
}

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

// extractShardIndex returns the shard index from the callers function name.
func extractShardIndex(t *testing.T) int {
	pc, _, _, _ := runtime.Caller(1)
	fullName := runtime.FuncForPC(pc).Name()
	lastDot := strings.LastIndex(fullName, ".")
	funcName := fullName[lastDot+1:]
	idx, err := strconv.Atoi(strings.TrimPrefix(funcName, shardFunctionPrefix))
	require.NoError(t, err)
	return idx
}

func TestBenchmarkExpectationShard1(t *testing.T) {
	reg.RunExpectationsSharded(t, extractShardIndex(t), shardCount)
}

func TestBenchmarkExpectationShard2(t *testing.T) {
	reg.RunExpectationsSharded(t, extractShardIndex(t), shardCount)
}

func TestBenchmarkExpectationShard3(t *testing.T) {
	reg.RunExpectationsSharded(t, extractShardIndex(t), shardCount)
}

func TestBenchmarkExpectationShard4(t *testing.T) {
	reg.RunExpectationsSharded(t, extractShardIndex(t), shardCount)
}

func TestBenchmarkExpectationShard5(t *testing.T) {
	reg.RunExpectationsSharded(t, extractShardIndex(t), shardCount)
}
