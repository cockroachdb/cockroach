// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rttanalysis

import "testing"

func BenchmarkGrant(b *testing.B) {
	tests := []RoundTripBenchTestCase{
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
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkRevoke(b *testing.B) {
	tests := []RoundTripBenchTestCase{
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
	}

	RunRoundTripBenchmark(b, tests)
}
