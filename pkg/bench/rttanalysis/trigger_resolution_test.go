// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

// BenchmarkTriggerResolution benchmarks the KV round-trips taken to
// execute statements with triggers.
//
// This benchmark can be run with the command:
//
//	./dev test pkg/bench/rttanalysis -f TestBenchmarkExpectation/TriggerResolution
func BenchmarkTriggerResolution(b *testing.B) {
	reg.Run(b)
}
func init() {
	reg.Register("TriggerResolution", []RoundTripBenchTestCase{
		{
			Name: "insert into table with before trigger",
			Setup: `
        CREATE TABLE trigger_table (a INT);
        CREATE FUNCTION trigger_fn() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$ BEGIN RETURN NEW; END $$;
        CREATE TRIGGER tr BEFORE INSERT ON trigger_table FOR EACH ROW EXECUTE FUNCTION trigger_fn();`,
			Stmt: `INSERT INTO trigger_table VALUES (100);`,
		},
		{
			Name: "insert into table with after trigger",
			Setup: `
        CREATE TABLE trigger_table (a INT);
        CREATE FUNCTION trigger_fn() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$ BEGIN RETURN NEW; END $$;
        CREATE TRIGGER tr AFTER INSERT ON trigger_table FOR EACH ROW EXECUTE FUNCTION trigger_fn();`,
			Stmt: `INSERT INTO trigger_table VALUES (100);`,
		},
	})
}
