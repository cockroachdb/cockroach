// Copyright 2020 The Cockroach Authors.
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
	"testing"

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

	RunBenchmarkExpectationTests(t)
}
