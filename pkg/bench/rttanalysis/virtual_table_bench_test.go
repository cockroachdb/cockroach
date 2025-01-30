// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

func BenchmarkVirtualTableQueries(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("VirtualTableQueries", []RoundTripBenchTestCase{
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
		// This checks that descriptors are still cached after they are written. We
		// expect the second and third selects not to go to KV because the
		// descriptors were cached after the first lookup.
		{
			Name: "virtual table cache with schema change",
			Setup: `
SET autocommit_before_ddl = false;
CREATE TABLE t1 (i INT PRIMARY KEY);
CREATE TABLE t2 (i INT PRIMARY KEY, j INT);`,
			Stmt: `
SELECT * FROM crdb_internal.tables;
ALTER TABLE t1 ADD COLUMN j INT;
SELECT * FROM crdb_internal.table_columns;
CREATE INDEX idx ON t2 (j);
SELECT * FROM crdb_internal.index_columns;`,
			Reset: "RESET autocommit_before_ddl;",
		},
		// This checks that catalog point lookups following a virtual table scan
		// access cached descriptors.
		{
			Name: "virtual table cache with point lookups",
			Setup: `
CREATE TABLE t1 (i INT PRIMARY KEY);
CREATE TABLE t2 (i INT PRIMARY KEY, j INT);`,
			Stmt: `
SELECT * FROM crdb_internal.tables;
SELECT * FROM t1;
SELECT * FROM t2;`,
		},
	})
}
