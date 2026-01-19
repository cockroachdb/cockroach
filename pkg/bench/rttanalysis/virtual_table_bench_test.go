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
SET create_table_with_schema_locked = false;
CREATE TABLE t1 (i INT PRIMARY KEY);
CREATE TABLE t2 (i INT PRIMARY KEY, j INT);`,
			Stmt: `
SELECT * FROM crdb_internal.tables;
ALTER TABLE t1 ADD COLUMN j INT;
SELECT * FROM crdb_internal.table_columns;
CREATE INDEX idx ON t2 (j);
SELECT * FROM crdb_internal.index_columns;`,
			Reset: "RESET autocommit_before_ddl;" +
				"RESET create_table_with_schema_locked;",
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
		// This test checks the performance of the crdb_internal virtual tables used by
		// SHOW CREATE ALL ROUTINES.
		{
			Name:  "show_create_all_routines",
			Setup: buildNFunctions(100),
			Stmt:  `SHOW CREATE ALL ROUTINES;`,
		},
		// This test checks the performance of the crdb_internal virtual tables used by
		// SHOW CREATE ALL TRIGGERS.
		{
			Name:  "show_create_all_triggers",
			Setup: buildNTablesWithTriggers(100),
			Stmt:  `SHOW CREATE ALL TRIGGERS;`,
		},
		// This test checks the performance of a query joining index_usage_statistics,
		// table_indexes, and pg_catalog tables.
		{
			Name:    "index_usage_statistics_join",
			SetupEx: buildNTablesWithIndexes(20, 5),
			Stmt: `
SELECT ti.descriptor_name AS table_name, ti.descriptor_id AS table_id,
       ti.index_name, ti.index_id, ti.index_type, ti.is_unique, ti.is_inverted,
       total_reads, last_read, ti.created_at, ns.nspname::STRING
FROM crdb_internal.index_usage_statistics AS us
JOIN crdb_internal.table_indexes AS ti ON (us.index_id = ti.index_id) AND (us.table_id = ti.descriptor_id)
JOIN pg_catalog.pg_class AS c ON ti.descriptor_id::OID = c.oid
JOIN pg_catalog.pg_namespace AS ns ON ns.oid = c.relnamespace
ORDER BY total_reads ASC;`,
		},
	})
}
