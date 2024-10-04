// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

func BenchmarkGenerateObjects(b *testing.B) { reg.Run(b) }
func init() {
	// Note: we have 1 db auto-generated every time because it's just too
	// expensive to DROP CASCADE these dbs containing many test objects.
	reg.Register("GenerateObjects", []RoundTripBenchTestCase{
		{
			Name: "generate 10 tables",
			Stmt: `SELECT crdb_internal.generate_test_objects('{"names":"gen.public.foo","counts":[1,0,10]}'::JSONB)`,
		},
		{
			Name: "generate 1000 tables - this test should use the same number of RTTs as for 10 tables",
			Stmt: `SELECT crdb_internal.generate_test_objects('{"names":"gen.public.foo","counts":[1,0,1000]}'::JSONB)`,
		},
		{
			Name: "generate 50000 tables",
			Stmt: `SELECT crdb_internal.generate_test_objects('{"names":"gen.public.foo","counts":[1,0,50000]}'::JSONB)`,
		},
		{
			Name:  "generate 100 tables from template",
			Setup: `CREATE TABLE IF NOT EXISTS foo(x INT)`,
			Stmt:  `SELECT crdb_internal.generate_test_objects('{"names":"gen.public.foo","counts":[1,0,100],"table_templates":["foo"]}'::JSONB)`,
		},
		{
			Name: "generate 10x10 schemas and tables in existing db",
			Stmt: `SELECT crdb_internal.generate_test_objects('{"names":"gen.foo","counts":[10,10]}'::JSONB)`,
		},
	})
}
