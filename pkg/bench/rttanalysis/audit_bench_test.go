// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

func BenchmarkAudit(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("Audit", []RoundTripBenchTestCase{
		{
			Name: "select from an audit table",
			Setup: `CREATE TABLE audit_table(a INT);
							ALTER TABLE audit_table EXPERIMENTAL_AUDIT SET READ WRITE;`,
			Stmt: "SELECT * from audit_table",
		},
	})
}
