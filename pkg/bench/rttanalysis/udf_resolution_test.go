// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
