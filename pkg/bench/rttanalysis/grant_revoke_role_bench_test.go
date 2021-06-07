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

import "testing"

func BenchmarkGrantRole(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			Name: "grant 1 role",
			Setup: `CREATE ROLE a;
CREATE ROLE b;`,
			Stmt:  "GRANT a TO b",
			Reset: "DROP ROLE a,b",
		},
		{
			Name: "grant 2 roles",
			Setup: `CREATE ROLE a;
CREATE ROLE b;
CREATE ROLE c;`,
			Stmt:  "GRANT a,b TO c",
			Reset: "DROP ROLE a,b,c",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkRevokeRole(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			Name: "revoke 1 role",
			Setup: `CREATE ROLE a;
CREATE ROLE b;
GRANT a TO b`,
			Stmt:  "REVOKE a FROM b",
			Reset: "DROP ROLE a,b",
		},
		{
			Name: "revoke 2 roles",
			Setup: `CREATE ROLE a;
CREATE ROLE b;
CREATE ROLE c;
GRANT a,b TO c;`,
			Stmt:  "REVOKE a,b FROM c",
			Reset: "DROP ROLE a,b,c",
		},
	}

	RunRoundTripBenchmark(b, tests)
}
