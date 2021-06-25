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

func BenchmarkTruncate(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			Name:  "truncate 1 column 0 rows",
			Setup: "CREATE TABLE t(x INT);",
			Stmt:  "TRUNCATE t",
		},
		{
			Name: "truncate 1 column 1 row",
			Setup: `CREATE TABLE t(x INT); 
INSERT INTO t (x) VALUES (1);`,
			Stmt: "TRUNCATE t",
		},
		{
			Name: "truncate 1 column 2 rows",
			Setup: `CREATE TABLE t(x INT); 
INSERT INTO t (x) VALUES (1);
INSERT INTO t (x) VALUES (2);`,
			Stmt: "TRUNCATE t",
		},
		{
			Name:  "truncate 2 column 0 rows",
			Setup: `CREATE TABLE t(x INT, y INT);`,
			Stmt:  "TRUNCATE t",
		},
		{
			Name: "truncate 2 column 1 rows",
			Setup: `CREATE TABLE t(x INT, y INT); 
INSERT INTO t (x, y) VALUES (1, 1);`,
			Stmt: "TRUNCATE t",
		},
		{
			Name: "truncate 2 column 2 rows",
			Setup: `CREATE TABLE t(x INT, y INT); 
INSERT INTO t (x, y) VALUES (1, 1);
INSERT INTO t (x,y) VALUES (2, 2);`,
			Stmt: "TRUNCATE t",
		},
	}

	RunRoundTripBenchmark(b, tests)
}
