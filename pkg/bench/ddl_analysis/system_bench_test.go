// Copyright 2021 The Cockroach Authors.
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

func BenchmarkSystemDatabaseQueries(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		// This query performs 1-2 lookups: getting the descriptor ID by name, then
		// fetching the system table descriptor. The descriptor is then cached.
		{
			name: "select system.users with schema name",
			stmt: `SELECT username, "hashedPassword" FROM system.public.users WHERE username = 'root'`,
		},
		// This query performs 1 extra lookup since the executor first tries to
		// lookup the name `current_db.system.users`.
		{
			name: "select system.users without schema name",
			stmt: `SELECT username, "hashedPassword" FROM system.users WHERE username = 'root'`,
		},
		// This query performs 0 extra lookups since the name resolution logic does
		// not try to resolve `"".system.users` and instead resolves
		//`system.public.users` right away.
		{
			name:  "select system.users with empty database name",
			setup: `SET sql_safe_updates = false; USE "";`,
			stmt:  `SELECT username, "hashedPassword"  FROM system.users WHERE username = 'root'`,
		},
	}

	RunRoundTripBenchmark(b, tests)
}
