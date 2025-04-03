// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysisccl

import (
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/bench/rttanalysis"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
)

const numNodes = 4

// RunRoundTripBenchmarkMultiRegion sets up a multi-region db run the RoundTripBenchTestCase test cases
// and counts how many round trips the Stmt specified by the test case performs.
var reg = rttanalysis.NewRegistry(numNodes, rttanalysis.MakeClusterConstructor(func(
	tb testing.TB, knobs base.TestingKnobs,
) (*gosql.DB, *gosql.DB, func()) {
	cluster, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		tb, numNodes, knobs,
	)
	db := cluster.ServerConn(0)
	// Eventlog is async, and introduces jitter in the benchmark.
	if _, err := db.Exec("SET CLUSTER SETTING server.eventlog.enabled = false"); err != nil {
		tb.Fatal(err)
	}
	if _, err := db.Exec("CREATE USER testuser"); err != nil {
		tb.Fatal(err)
	}
	if _, err := db.Exec("GRANT admin TO testuser"); err != nil {
		tb.Fatal(err)
	}
	url, testuserCleanup := pgurlutils.PGUrl(
		tb, cluster.Server(0).ApplicationLayer().AdvSQLAddr(), "rttanalysisccl", url.User("testuser"),
	)
	conn, err := gosql.Open("postgres", url.String())
	if err != nil {
		tb.Fatal(err)
	}
	return conn, nil, func() {
		cleanup()
		testuserCleanup()
	}
}))

func TestBenchmarkExpectation(t *testing.T) { reg.RunExpectations(t) }

const (
	multipleTableFixture = `
BEGIN; CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2", "us-east3"; COMMIT;
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

func BenchmarkAlterRegions(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("AlterRegions", []rttanalysis.RoundTripBenchTestCase{
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
	})
}

func BenchmarkAlterPrimaryRegion(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("AlterPrimaryRegion", []rttanalysis.RoundTripBenchTestCase{
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
	})
}

func BenchmarkAlterSurvivalGoals(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("AlterSurvivalGoals", []rttanalysis.RoundTripBenchTestCase{
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
	})
}

func BenchmarkAlterTableLocality(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("AlterTableLocality", []rttanalysis.RoundTripBenchTestCase{
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
	})
}
