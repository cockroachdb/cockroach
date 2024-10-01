// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

func BenchmarkUDFResolution(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("UDFResolution", []RoundTripBenchTestCase{
		{
			Name:  "select from udf",
			Setup: `CREATE FUNCTION f() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;`,
			Stmt:  `SELECT f()`,
		},
	})
}
