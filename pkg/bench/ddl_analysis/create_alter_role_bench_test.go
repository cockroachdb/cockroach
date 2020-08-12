// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bench

import "testing"

func BenchmarkCreateRole(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "create role with no options",
			stmt:  "CREATE ROLE rolea",
			reset: "DROP ROLE rolea",
		},
		{
			name:  "create role with 1 option",
			stmt:  "CREATE ROLE rolea LOGIN",
			reset: "DROP ROLE rolea",
		},
		{
			name:  "create role with 2 options",
			stmt:  "CREATE ROLE rolea LOGIN CREATEROLE",
			reset: "DROP ROLE rolea",
		},
		{
			name:  "create role with 3 options",
			stmt:  "CREATE ROLE rolea LOGIN CREATEROLE VALID UNTIL '2021-01-01'",
			reset: "DROP ROLE rolea",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkAlterRole(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "alter role with 1 option",
			setup: "CREATE ROLE rolea",
			stmt:  "ALTER ROLE rolea CREATEROLE",
			reset: "DROP ROLE rolea",
		},
		{
			name:  "alter role with 2 options",
			setup: "CREATE ROLE rolea",
			stmt:  "ALTER ROLE rolea CREATEROLE LOGIN",
			reset: "DROP ROLE rolea",
		},
		{
			name:  "alter role with 3 options",
			setup: "CREATE ROLE rolea",
			stmt:  "ALTER ROLE rolea CREATEROLE LOGIN PASSWORD '123'",
			reset: "DROP ROLE rolea",
		},
	}

	RunRoundTripBenchmark(b, tests)
}
