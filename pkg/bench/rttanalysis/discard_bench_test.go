// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

func BenchmarkDiscard(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("Discard", []RoundTripBenchTestCase{
		{
			Name: "DISCARD ALL, no tables",
			Setup: `DROP DATABASE IF EXISTS db CASCADE; 
CREATE DATABASE db;`,
			Stmt: "DISCARD ALL",
		},
		{
			Name: "DISCARD ALL, 1 tables in 1 db",
			Setup: `
SET experimental_enable_temp_tables = true;
DROP DATABASE IF EXISTS db CASCADE; 
CREATE DATABASE db; 
USE db; 
CREATE TEMPORARY TABLE foo(i int);
`,
			Stmt: "DISCARD ALL",
		},
		{
			Name: "DISCARD ALL, 2 tables in 2 dbs",
			Setup: `
SET experimental_enable_temp_tables = true;
DROP DATABASE IF EXISTS db CASCADE; 
CREATE DATABASE db; 
USE db; 
CREATE TEMPORARY TABLE foo(i int);
CREATE TEMPORARY TABLE bar(i int);
DROP DATABASE IF EXISTS db2 CASCADE; 
CREATE DATABASE db2; 
USE db2; 
CREATE TEMPORARY TABLE foo(i int);
CREATE TEMPORARY TABLE bar(i int);
`,
			Stmt: "DISCARD ALL",
		},
	})
}
