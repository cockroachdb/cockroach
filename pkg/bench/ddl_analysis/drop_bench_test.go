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

func BenchmarkDropRole(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "drop 1 role",
			setup: "CREATE ROLE a",
			stmt:  "DROP ROLE a",
		},
		{
			name:  "drop 2 roles",
			setup: "CREATE ROLE a; CREATE ROLE b;",
			stmt:  "DROP ROLE a, b",
		},
		{
			name:  "drop 3 roles",
			setup: "CREATE ROLE a; CREATE ROLE b; CREATE ROLE c;",
			stmt:  "DROP ROLE a, b, c",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkDropTable(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "drop 1 table",
			setup: "CREATE TABLE t1()",
			stmt:  "DROP TABLE t1",
		},
		{
			name:  "drop 2 tables",
			setup: "CREATE TABLE t1(); CREATE TABLE t2();",
			stmt:  "DROP TABLE t1,t2",
		},
		{
			name:  "drop 3 tables",
			setup: "CREATE TABLE t1(); CREATE TABLE t2(); CREATE TABLE t3();",
			stmt:  "DROP TABLE t1,t2,t3",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkDropView(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "drop 1 view",
			setup: "CREATE TABLE t(x INT); CREATE VIEW vx1 AS SELECT x FROM t",
			stmt:  "DROP VIEW vx1",
		},
		{
			name: "drop 2 views",
			setup: `CREATE TABLE t(x INT); 
CREATE VIEW vx1 AS SELECT x FROM t;
CREATE VIEW vx2 AS SELECT x FROM t;`,
			stmt: "DROP VIEW vx1, vx2",
		},
		{
			name: "drop 3 views",
			setup: `CREATE TABLE t(x INT); 
CREATE VIEW vx1 AS SELECT x FROM t;
CREATE VIEW vx2 AS SELECT x FROM t;
CREATE VIEW vx3 AS SELECT x FROM t;`,
			stmt: "DROP VIEW vx1,vx2,vx3",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkDropSequence(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "drop 1 sequence",
			setup: "CREATE SEQUENCE s",
			stmt:  "DROP SEQUENCE s",
		},
		{
			name:  "drop 2 sequences",
			setup: "CREATE SEQUENCE s1; CREATE SEQUENCE s2;",
			stmt:  "DROP SEQUENCE s1,s2",
		},
		{
			name:  "drop 3 sequences",
			setup: "CREATE SEQUENCE s1; CREATE SEQUENCE s2; CREATE SEQUENCE s3;",
			stmt:  "DROP SEQUENCE s1,s2,s3",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkDropDatabase(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name: "drop database 0 tables",
			setup: `CREATE DATABASE d; 
USE d;`,
			stmt: "DROP DATABASE d",
		},
		{
			name: "drop database 1 table",
			setup: `CREATE DATABASE d; 
USE d;
CREATE TABLE t()`,
			stmt: "DROP DATABASE d",
		},
		{
			name: "drop database 2 tables",
			setup: `CREATE DATABASE d; 
USE d;
CREATE TABLE t1();
CREATE TABLE t2();`,
			stmt: "DROP DATABASE d",
		},
		{
			name: "drop database 3 tables",
			setup: `CREATE DATABASE d; 
USE d;
CREATE TABLE t1();
CREATE TABLE t2();
CREATE TABLE t3();`,
			stmt: "DROP DATABASE d",
		},
	}

	RunRoundTripBenchmark(b, tests)
}
