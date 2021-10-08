// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package rttanalysisccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/bench/rttanalysis"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
)

// TestBenchmarkExpectation runs all of the benchmarks and
// one iteration and validates that the number of RPCs meets
// the expectation.
//
// It takes a long time and thus is skipped under stress, race
// and short.
func TestBenchmarkExpectation(t *testing.T) {
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderShort(t)

	rttanalysis.RunBenchmarkExpectationTests(t)
}
