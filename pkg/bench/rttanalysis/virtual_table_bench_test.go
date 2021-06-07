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

func BenchmarkVirtualTableQueries(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		// This benchmark should perform exactly one kv operation to fetch all of
		// the descriptors.
		{
			Name: "select crdb_internal.tables with 1 fk",
			Setup: `
CREATE TABLE t1 (i INT PRIMARY KEY);
CREATE TABLE t2 (i INT PRIMARY KEY, j INT REFERENCES t1(i));
`,
			Stmt: `SELECT * FROM "".crdb_internal.tables`,
		},
		{
			// We expect this to perform exactly one kv operation to fetch all of
			// the descriptors.
			Name: "select crdb_internal.invalid_objects with 1 fk",
			Setup: `
CREATE TABLE t1 (i INT PRIMARY KEY);
CREATE TABLE t2 (i INT PRIMARY KEY, j INT REFERENCES t1(i));
`,
			Stmt: `SELECT * FROM "".crdb_internal.invalid_objects`,
		},
	}

	RunRoundTripBenchmark(b, tests)
}
