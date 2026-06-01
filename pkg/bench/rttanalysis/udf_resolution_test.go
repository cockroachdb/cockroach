// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

var udfResolutionCases = []RoundTripBenchTestCase{
	{
		Name:  "select from udf",
		Setup: `CREATE FUNCTION f() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;`,
		Stmt:  `SELECT f()`,
	},
}

// benchmark-ci: benchtime=20x
func BenchmarkUDFResolution(b *testing.B) {
	runCPUMemBenchmark(bShim{b}, udfResolutionCases, defaultCC)
}

func TestBenchmarkExpectation_UDFResolution(t *testing.T) {
	runExpectation(t, "UDFResolution", udfResolutionCases, defaultCC)
}
