// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

var auditCases = []RoundTripBenchTestCase{
	{
		Name: "select from an audit table",
		Setup: `CREATE TABLE audit_table(a INT) WITH (schema_locked = false);
							ALTER TABLE audit_table EXPERIMENTAL_AUDIT SET READ WRITE;`,
		Stmt: "SELECT * from audit_table",
	},
}

// benchmark-ci: benchtime=20x
func BenchmarkAudit(b *testing.B) { runCPUMemBenchmark(bShim{b}, auditCases, defaultCC) }

func TestBenchmarkExpectation_Audit(t *testing.T) {
	runExpectation(t, "Audit", auditCases, defaultCC)
}
