// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

func BenchmarkGrant(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("Grant", []RoundTripBenchTestCase{
		{
			Name: "grant all on 1 table",
			Setup: `CREATE USER TEST; 
CREATE TABLE t0();`,
			Stmt:  "GRANT ALL ON * TO TEST",
			Reset: "DROP ROLE TEST",
		},
		{
			Name: "grant all on 2 tables",
			Setup: `CREATE USER TEST; 
CREATE TABLE t0();
CREATE TABLE t1();`,
			Stmt:  "GRANT ALL ON * TO TEST",
			Reset: "DROP ROLE TEST",
		},
		{
			Name: "grant all on 3 tables",
			Setup: `CREATE USER TEST; 
CREATE TABLE t0();
CREATE TABLE t1();
CREATE TABLE t2();`,
			Stmt:  "GRANT ALL ON * TO TEST",
			Reset: "DROP ROLE TEST",
		},
	})
}

func BenchmarkRevoke(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("Revoke", []RoundTripBenchTestCase{
		{
			Name: "revoke all on 1 table",
			Setup: `CREATE USER TEST; 
CREATE TABLE t0();
GRANT ALL ON * TO TEST;`,
			Stmt:  "REVOKE ALL ON * FROM TEST",
			Reset: "DROP ROLE TEST",
		},
		{
			Name: "revoke all on 2 tables",
			Setup: `CREATE USER TEST; 
CREATE TABLE t0();
CREATE TABLE t1();
GRANT ALL ON * TO TEST;`,
			Stmt:  "REVOKE ALL ON * FROM TEST",
			Reset: "DROP ROLE TEST",
		},
		{
			Name: "revoke all on 3 tables",
			Setup: `CREATE USER TEST; 
CREATE TABLE t0();
CREATE TABLE t1();
CREATE TABLE t2();
GRANT ALL ON * TO TEST;`,
			Stmt:  "REVOKE ALL ON * FROM TEST",
			Reset: "DROP ROLE TEST",
		},
	})
}
