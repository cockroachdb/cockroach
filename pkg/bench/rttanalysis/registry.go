// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rttanalysis

import (
	"context"
	gosql "database/sql"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
	cc ClusterConstructor
	r  map[string][]RoundTripBenchTestCase
}

// NewRegistry constructs a new benchmark registry.
func NewRegistry(cc ClusterConstructor) *Registry {
	return &Registry{
		cc: cc,
		r:  make(map[string][]RoundTripBenchTestCase),
	}
}

// Run is used to run a benchmark suite which has been previously registered.
func (r *Registry) Run(b *testing.B) {
	tests, ok := r.r[bName(b)]
	require.True(b, ok)
	runRoundTripBenchmark(bShim{b}, tests, r.cc)
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

	runBenchmarkExpectationTests(t, reg)
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

var reg = NewRegistry(MakeClusterConstructor(func(
	t testing.TB, knobs base.TestingKnobs,
) (_ *gosql.DB, cleanup func()) {
	s, sql, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "bench",
		Knobs:       knobs,
	})
	return sql, func() { s.Stopper().Stop(context.Background()) }
}))
