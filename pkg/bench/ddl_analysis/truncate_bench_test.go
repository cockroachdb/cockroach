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

func BenchmarkTruncate(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "truncate 1 column 0 rows",
			setup: "CREATE TABLE t(x INT);",
			stmt:  "TRUNCATE t",
		},
		{
			name: "truncate 1 column 1 row",
			setup: `CREATE TABLE t(x INT); 
INSERT INTO t (x) VALUES (1);`,
			stmt: "TRUNCATE t",
		},
		{
			name: "truncate 1 column 2 rows",
			setup: `CREATE TABLE t(x INT); 
INSERT INTO t (x) VALUES (1);
INSERT INTO t (x) VALUES (2);`,
			stmt: "TRUNCATE t",
		},
		{
			name:  "truncate 2 column 0 rows",
			setup: `CREATE TABLE t(x INT, y INT);`,
			stmt:  "TRUNCATE t",
		},
		{
			name: "truncate 2 column 1 rows",
			setup: `CREATE TABLE t(x INT, y INT); 
INSERT INTO t (x, y) VALUES (1, 1);`,
			stmt: "TRUNCATE t",
		},
		{
			name: "truncate 2 column 2 rows",
			setup: `CREATE TABLE t(x INT, y INT); 
INSERT INTO t (x, y) VALUES (1, 1);
INSERT INTO t (x,y) VALUES (2, 2);`,
			stmt: "TRUNCATE t",
		},
	}

	RunRoundTripBenchmark(b, tests)
}
