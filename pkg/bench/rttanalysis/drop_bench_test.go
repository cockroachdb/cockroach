// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

func BenchmarkDropRole(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("DropRole", []RoundTripBenchTestCase{
		{
			Name:  "drop 1 role",
			Setup: "CREATE ROLE a",
			Stmt:  "DROP ROLE a",
		},
		{
			Name:  "drop 2 roles",
			Setup: "CREATE ROLE a; CREATE ROLE b;",
			Stmt:  "DROP ROLE a, b",
		},
		{
			Name:  "drop 3 roles",
			Setup: "CREATE ROLE a; CREATE ROLE b; CREATE ROLE c;",
			Stmt:  "DROP ROLE a, b, c",
		},
	})
}

func BenchmarkDropTable(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("DropTable", []RoundTripBenchTestCase{
		{
			Name:  "drop 1 table",
			Setup: "CREATE TABLE t1()",
			Stmt:  "DROP TABLE t1",
		},
		{
			Name:  "drop 2 tables",
			Setup: "CREATE TABLE t1(); CREATE TABLE t2();",
			Stmt:  "DROP TABLE t1,t2",
		},
		{
			Name:  "drop 3 tables",
			Setup: "CREATE TABLE t1(); CREATE TABLE t2(); CREATE TABLE t3();",
			Stmt:  "DROP TABLE t1,t2,t3",
		},
	})
}

func BenchmarkDropView(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("DropView", []RoundTripBenchTestCase{
		{
			Name:  "drop 1 view",
			Setup: "CREATE TABLE t(x INT); CREATE VIEW vx1 AS SELECT x FROM t",
			Stmt:  "DROP VIEW vx1",
		},
		{
			Name: "drop 2 views",
			Setup: `CREATE TABLE t(x INT); 
CREATE VIEW vx1 AS SELECT x FROM t;
CREATE VIEW vx2 AS SELECT x FROM t;`,
			Stmt: "DROP VIEW vx1, vx2",
		},
		{
			Name: "drop 3 views",
			Setup: `CREATE TABLE t(x INT); 
CREATE VIEW vx1 AS SELECT x FROM t;
CREATE VIEW vx2 AS SELECT x FROM t;
CREATE VIEW vx3 AS SELECT x FROM t;`,
			Stmt: "DROP VIEW vx1,vx2,vx3",
		},
	})
}

func BenchmarkDropSequence(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("DropSequence", []RoundTripBenchTestCase{
		{
			Name:  "drop 1 sequence",
			Setup: "CREATE SEQUENCE s",
			Stmt:  "DROP SEQUENCE s",
		},
		{
			Name:  "drop 2 sequences",
			Setup: "CREATE SEQUENCE s1; CREATE SEQUENCE s2;",
			Stmt:  "DROP SEQUENCE s1,s2",
		},
		{
			Name:  "drop 3 sequences",
			Setup: "CREATE SEQUENCE s1; CREATE SEQUENCE s2; CREATE SEQUENCE s3;",
			Stmt:  "DROP SEQUENCE s1,s2,s3",
		},
	})
}

func BenchmarkDropDatabase(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("DropDatabase", []RoundTripBenchTestCase{
		{
			Name: "drop database 0 tables",
			Setup: `CREATE DATABASE d; 
USE d;`,
			Stmt: "DROP DATABASE d",
		},
		{
			Name: "drop database 1 table",
			Setup: `CREATE DATABASE d; 
USE d;
CREATE TABLE t()`,
			Stmt: "DROP DATABASE d",
		},
		{
			Name: "drop database 2 tables",
			Setup: `CREATE DATABASE d; 
USE d;
CREATE TABLE t1();
CREATE TABLE t2();`,
			Stmt: "DROP DATABASE d",
		},
		{
			Name: "drop database 3 tables",
			Setup: `CREATE DATABASE d; 
USE d;
CREATE TABLE t1();
CREATE TABLE t2();
CREATE TABLE t3();`,
			Stmt: "DROP DATABASE d",
		},
	})
}
