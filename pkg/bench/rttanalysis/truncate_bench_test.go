// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

func BenchmarkTruncate(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("Truncate", []RoundTripBenchTestCase{
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
	})
}
