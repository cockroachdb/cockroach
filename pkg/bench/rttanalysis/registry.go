// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Registry stores RoundTripBenchmarkTestCase definitions grouped under a go
// benchmark test. Registered tests should be run from within the benchmark of
// the same name. For example, if you want a benchmark to have name
// BenchmarkCreateRole, you should register cases with the name "CreateRole".
// This structure facilitates a registry concept without needing to rename
// existing benchmarks.
//
// All registered benchmark tests can be run with the command:
//
//	./dev test pkg/bench/rttanalysis -f TestBenchmarkExpectation
//
// A specific benchmark test can be run by adding its registered name as a suffix to the filter:
//
//	./dev test pkg/bench/rttanalysis -f TestBenchmarkExpectation/CreateRole
//
// The expectation is that there should be a single registry per package and
// that tests are registered to it during initialization.
type Registry struct {
	numNodes int
	cc       ClusterConstructor
	r        map[string][]RoundTripBenchTestCase
}

// NewRegistry constructs a new benchmark registry.
func NewRegistry(numNodes int, cc ClusterConstructor) *Registry {
	return &Registry{
		numNodes: numNodes,
		cc:       cc,
		r:        make(map[string][]RoundTripBenchTestCase),
	}
}

// Run is used to run a benchmark suite which has been previously registered.
func (r *Registry) Run(b *testing.B) {
	tests, ok := r.r[bName(b)]
	require.True(b, ok)
	runCPUMemBenchmark(bShim{b}, tests, r.cc)
}

// RunExpectations runs all the benchmarks for one iteration
// and validates that the number of RPCs meets the expectation. If run
// with the --rewrite flag, it will rewrite the run benchmarks. The
// benchmarks can be filtered by passing the usual test filters underneath
// this test's name.
//
// It takes a long time and thus is skipped under duress and short.
func (r *Registry) RunExpectations(t *testing.T) {
	r.RunExpectationsSharded(t, 1, 1)
}

// RunExpectationsSharded runs all the benchmarks for one iteration
// and validates that the number of RPCs meets the expectation. If run
// with the --rewrite flag, it will rewrite the run benchmarks. The
// benchmarks can be filtered by passing the usual test filters underneath
// this test's name.
//
// It takes a long time and thus is skipped under duress and short.
//
// When shard and totalShards are provided (> 1), only a subset of benchmarks
// assigned to the specific shard will be run, enabling parallel execution.
// Test groups are distributed across shards using round-robin assignment.
func (r *Registry) RunExpectationsSharded(t *testing.T, shard, totalShards int) {
	defer jobs.TestingSetIDsToIgnore(map[jobspb.JobID]struct{}{3001: {}, 3002: {}})()
	skip.UnderDuress(t)
	skip.UnderShort(t)
	if runtime.GOARCH == "s390x" {
		skip.IgnoreLint(t, "test prone to crashing under s390x (see #154317)")
	}

	// If totalShards is 1, run all tests; otherwise shard them
	var registryToUse *Registry
	if totalShards <= 1 {
		// Run all test groups
		registryToUse = r
	} else {
		// Create a registry with only the test groups assigned to this shard
		shardRegistry := &Registry{
			numNodes: r.numNodes,
			cc:       r.cc,
			r:        make(map[string][]RoundTripBenchTestCase),
		}

		// Distribute test groups across shards using round-robin assignment
		// First, get all group names and sort them for consistent ordering
		groupNames := make([]string, 0, len(r.r))
		for groupName := range r.r {
			groupNames = append(groupNames, groupName)
		}
		// Sort for deterministic assignment across runs
		for i := 0; i < len(groupNames); i++ {
			for j := i + 1; j < len(groupNames); j++ {
				if groupNames[i] > groupNames[j] {
					groupNames[i], groupNames[j] = groupNames[j], groupNames[i]
				}
			}
		}

		// Assign groups to shards using round-robin
		for i, groupName := range groupNames {
			assignedShard := (i % totalShards) + 1
			if assignedShard == shard {
				shardRegistry.r[groupName] = r.r[groupName]
			}
		}
		registryToUse = shardRegistry
	}

	runBenchmarkExpectationTests(t, registryToUse)
}

// Register registers a set of test cases to a given benchmark name. It is
// intended to be called during initialization.
func (r *Registry) Register(name string, tests []RoundTripBenchTestCase) {
	if _, exists := r.r[name]; exists {
		panic(errors.Errorf("Benchmark%s already registered", name))
	}
	r.r[name] = tests
}

// bName trims the Benchmark prefix from b.Name().
func bName(b *testing.B) string {
	return strings.TrimPrefix(b.Name(), "Benchmark")
}
