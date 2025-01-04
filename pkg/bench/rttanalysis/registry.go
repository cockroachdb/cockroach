// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	"strings"
	"testing"

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
// It takes a long time and thus is skipped under stress, race
// and short.
func (r *Registry) RunExpectations(t *testing.T) {
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderShort(t)
	skip.UnderDeadlock(t)

	runBenchmarkExpectationTests(t, r)
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
