// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package rttanalysisccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/bench/rttanalysis"
)

const (
	multipleTableFixture = `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2", "us-east3";
USE test;
CREATE TABLE test11 (p int) LOCALITY REGIONAL BY TABLE IN "us-east1";
CREATE TABLE test12 (p int) LOCALITY REGIONAL BY TABLE IN "us-east1";
CREATE TABLE test21 (p int) LOCALITY REGIONAL BY ROW;
CREATE TABLE test22 (p int) LOCALITY REGIONAL BY ROW;
CREATE TABLE test23 (p int) LOCALITY REGIONAL BY ROW;
CREATE TABLE test24 (p int) LOCALITY REGIONAL BY ROW;
CREATE TABLE test31 (p int) LOCALITY GLOBAL;
CREATE TABLE test32 (p int) LOCALITY GLOBAL;
CREATE TABLE test41 (p int) LOCALITY REGIONAL BY TABLE IN "us-east2";
CREATE TABLE test42 (p int) LOCALITY REGIONAL BY TABLE IN "us-east2";
`
)

func BenchmarkAlterRegions(b *testing.B) {
	tests := []rttanalysis.RoundTripBenchTestCase{
		{
			Name:  "alter empty database add region",
			Setup: `CREATE DATABASE test PRIMARY REGION "us-east1"`,
			Stmt:  `ALTER DATABASE test ADD REGION "us-east2"`,
			Reset: "DROP DATABASE test",
		},
		{
			Name:  "alter empty database drop region",
			Setup: `CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2"`,
			Stmt:  `ALTER DATABASE test DROP REGION "us-east2"`,
			Reset: "DROP DATABASE test",
		},
		{
			Name:  "alter populated database drop region",
			Setup: multipleTableFixture,
			Stmt:  `ALTER DATABASE test DROP REGION "us-east3"`,
			Reset: "DROP DATABASE test",
		},
		{
			Name:  "alter populated database add region",
			Setup: multipleTableFixture,
			Stmt:  `ALTER DATABASE test ADD REGION "us-east4"`,
			Reset: "DROP DATABASE test",
		},
	}

	RunRoundTripBenchmarkMultiRegion(b, tests)
}

func BenchmarkAlterPrimaryRegion(b *testing.B) {
	tests := []rttanalysis.RoundTripBenchTestCase{
		{
			Name:  "alter empty database set initial primary region",
			Setup: "CREATE DATABASE test",
			Stmt:  `ALTER DATABASE test SET PRIMARY REGION "us-east1"`,
			Reset: "DROP DATABASE test",
		},
		{
			Name:  "alter empty database alter primary region",
			Setup: `CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2"`,
			Stmt:  `ALTER DATABASE test SET PRIMARY REGION "us-east2"`,
			Reset: "DROP DATABASE test",
		},
		{
			Name: "alter populated database set initial primary region",
			Setup: `
CREATE DATABASE test;
USE test;
CREATE TABLE test1 (p int);
CREATE TABLE test2 (p int);
CREATE TABLE test3 (p int);
CREATE TABLE test4 (p int);
CREATE TABLE test5 (p int);
CREATE TABLE test6 (p int);
CREATE TABLE test7 (p int);
CREATE TABLE test8 (p int);
CREATE TABLE test9 (p int);
CREATE TABLE test10 (p int);
`,
			Stmt:  `ALTER DATABASE test SET PRIMARY REGION "us-east1"`,
			Reset: "DROP DATABASE test",
		},
		{
			Name:  "alter populated database alter primary region",
			Setup: multipleTableFixture,
			Stmt:  `ALTER DATABASE test SET PRIMARY REGION "us-east2"`,
			Reset: "DROP DATABASE test",
		},
	}

	RunRoundTripBenchmarkMultiRegion(b, tests)
}

func BenchmarkAlterSurvivalGoals(b *testing.B) {
	tests := []rttanalysis.RoundTripBenchTestCase{
		{
			Name:  "alter empty database from zone to region",
			Setup: `CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2","us-east3"`,
			Stmt:  `ALTER DATABASE test SURVIVE REGION FAILURE`,
			Reset: "DROP DATABASE test",
		},
		{
			Name:  "alter empty database from region to zone",
			Setup: `CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2","us-east3" SURVIVE ZONE FAILURE`,
			Stmt:  `ALTER DATABASE test SURVIVE ZONE FAILURE`,
			Reset: "DROP DATABASE test",
		},
		{
			Name:  "alter populated database from zone to region",
			Setup: multipleTableFixture,
			Stmt:  `ALTER DATABASE test SURVIVE REGION FAILURE`,
			Reset: "DROP DATABASE test",
		},
		{
			Name: "alter populated database from region to zone",
			Setup: multipleTableFixture + `
ALTER DATABASE test SURVIVE REGION FAILURE`,
			Stmt:  `ALTER DATABASE test SURVIVE ZONE FAILURE`,
			Reset: "DROP DATABASE test",
		},
	}

	RunRoundTripBenchmarkMultiRegion(b, tests)
}

func BenchmarkAlterTableLocality(b *testing.B) {
	tests := []rttanalysis.RoundTripBenchTestCase{
		{
			Name: "alter from global to regional by table",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2", "us-east3";
USE test;
CREATE TABLE test (p int) LOCALITY GLOBAL;
`,
			Stmt:  `ALTER TABLE test SET LOCALITY REGIONAL IN PRIMARY REGION`,
			Reset: "DROP DATABASE test",
		},
		{
			Name: "alter from regional by table to global",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2", "us-east3";
USE test;
CREATE TABLE test (p int) LOCALITY REGIONAL IN PRIMARY REGION;
`,
			Stmt:  `ALTER TABLE test SET LOCALITY GLOBAL`,
			Reset: "DROP DATABASE test",
		},
		{
			Name: "alter from global to rbr",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2", "us-east3";
USE test;
CREATE TABLE test (p int) LOCALITY GLOBAL;
`,
			Stmt:  `ALTER TABLE test SET LOCALITY REGIONAL BY ROW`,
			Reset: "DROP DATABASE test",
		},
		{
			Name: "alter from regional by table to rbr",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2", "us-east3";
USE test;
CREATE TABLE test (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
`,
			Stmt:  `ALTER TABLE test SET LOCALITY REGIONAL BY ROW`,
			Reset: "DROP DATABASE test",
		},
		{
			Name: "alter from rbr to regional by table",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2", "us-east3";
USE test;
CREATE TABLE test (p int) LOCALITY REGIONAL BY ROW;
`,
			Stmt:  `ALTER TABLE test SET LOCALITY REGIONAL BY TABLE IN PRIMARY REGION`,
			Reset: "DROP DATABASE test",
		},
		{
			Name: "alter from rbr to global",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2", "us-east3";
USE test;
CREATE TABLE test (p int) LOCALITY REGIONAL BY ROW;
`,
			Stmt:  `ALTER TABLE test SET LOCALITY GLOBAL`,
			Reset: "DROP DATABASE test",
		},
	}

	RunRoundTripBenchmarkMultiRegion(b, tests)
}
