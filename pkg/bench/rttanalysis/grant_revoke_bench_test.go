// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

var grantCases = []RoundTripBenchTestCase{
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

// benchmark-ci: benchtime=20x
func BenchmarkGrant(b *testing.B) { runCPUMemBenchmark(bShim{b}, grantCases, defaultCC) }

func TestBenchmarkExpectation_Grant(t *testing.T) {
	runExpectation(t, "Grant", grantCases, defaultCC)
}

var revokeCases = []RoundTripBenchTestCase{
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

// benchmark-ci: benchtime=20x
func BenchmarkRevoke(b *testing.B) { runCPUMemBenchmark(bShim{b}, revokeCases, defaultCC) }

func TestBenchmarkExpectation_Revoke(t *testing.T) {
	runExpectation(t, "Revoke", revokeCases, defaultCC)
}
