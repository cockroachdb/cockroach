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

func BenchmarkVirtualTableQueries(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		// This benchmark should perform exactly one kv operation to fetch all of
		// the descriptors.
		{
			name: "select crdb_internal.tables with 1 fk",
			setup: `
CREATE TABLE t1 (i INT PRIMARY KEY);
CREATE TABLE t2 (i INT PRIMARY KEY, j INT REFERENCES t1(i));
`,
			stmt: `SELECT * FROM "".crdb_internal.tables`,
		},
		{
			// We expect this to perform, somewhat surprisingly, 8 kv operations.
			// It performs one to fetch all of the descriptors initially. Then it
			// gets all of the databases which is 3 reads, one for each namespace
			// table and one to fetch all of the descriptors. Then it checks to see
			// if any of the databases have descriptors (system, defaultdb, postgres,
			// and test) schemas which is another 4, leaving 8.
			name: "select crdb_internal.invalid_objects with 1 fk",
			setup: `
CREATE TABLE t1 (i INT PRIMARY KEY);
CREATE TABLE t2 (i INT PRIMARY KEY, j INT REFERENCES t1(i));
`,
			stmt: `SELECT * FROM "".crdb_internal.invalid_objects`,
		},
	}

	RunRoundTripBenchmark(b, tests)
}
