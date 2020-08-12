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

func BenchmarkGrantRole(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name: "grant 1 role",
			setup: `CREATE ROLE a;
CREATE ROLE b;`,
			stmt:  "GRANT a TO b",
			reset: "DROP ROLE a,b",
		},
		{
			name: "grant 2 roles",
			setup: `CREATE ROLE a;
CREATE ROLE b;
CREATE ROLE c;`,
			stmt:  "GRANT a,b TO c",
			reset: "DROP ROLE a,b,c",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkRevokeRole(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name: "revoke 1 role",
			setup: `CREATE ROLE a;
CREATE ROLE b;
GRANT a TO b`,
			stmt:  "REVOKE a FROM b",
			reset: "DROP ROLE a,b",
		},
		{
			name: "revoke 2 roles",
			setup: `CREATE ROLE a;
CREATE ROLE b;
CREATE ROLE c;
GRANT a,b TO c;`,
			stmt:  "REVOKE a,b FROM c",
			reset: "DROP ROLE a,b,c",
		},
	}

	RunRoundTripBenchmark(b, tests)
}
