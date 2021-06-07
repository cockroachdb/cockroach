package ddl_analysisccl

import (
	"testing"

	bench "github.com/cockroachdb/cockroach/pkg/bench/ddl_analysis"
)

func BenchmarkAlterDatabaseMultiRegion(b *testing.B) {
	tests := []bench.RoundTripBenchTestCase{
		{
			Name:  "alter database single primary region",
			Setup: "CREATE DATABASE test",
			Stmt:  `ALTER DATABASE test SET PRIMARY REGION "us-east-1"`,
			Reset: "DROP DATABASE TEST",
		},
		{
			Name:  "alter database add non-primary region",
			Setup: `CREATE DATABASE test PRIMARY REGION "us-east-1"`,
			Stmt:  `ALTER DATABASE test ADD REGION "us-central-1"`,
			Reset: "DROP DATABASE TEST",
		},
	}

	RunRoundTripBenchmarkMultiRegion(b, tests)
}

func BenchmarkAlterPrimaryRegionMultipleTables(b *testing.B) {
	tests := []bench.RoundTripBenchTestCase{
		{
			Name: "alter database with many tables",
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
			Stmt:  `ALTER DATABASE test SET PRIMARY REGION "us-east-1"`,
			Reset: "DROP DATABASE TEST",
		},
		{
			Name: "alter database with many tables change region",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east-1" REGIONS "us-east-1", "us-central-1", "us-west-1";
USE test;
CREATE TABLE test1 (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
CREATE TABLE test2 (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
CREATE TABLE test3 (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
CREATE TABLE test4 (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
CREATE TABLE test5 (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
CREATE TABLE test6 (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
CREATE TABLE test7 (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
CREATE TABLE test8 (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
CREATE TABLE test9 (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
CREATE TABLE test10 (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
`,
			Stmt:  `ALTER DATABASE test SET PRIMARY REGION "us-central-1"`,
			Reset: "DROP DATABASE TEST",
		},
	}

	RunRoundTripBenchmarkMultiRegion(b, tests)
}

func BenchmarkAlterSurvivalGoals(b *testing.B) {
	tests := []bench.RoundTripBenchTestCase{
		{
			Name: "alter from zone to region",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east-1" REGIONS "us-east-1", "us-central-1", "us-west-1";
`,
			Stmt:  `ALTER DATABASE test SURVIVE REGION FAILURE`,
			Reset: "DROP DATABASE TEST",
		},
		{
			Name: "alter from region to zone",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east-1" REGIONS "us-east-1", "us-central-1", "us-west-1" SURVIVE REGION FAILURE;
`,
			Stmt:  `ALTER DATABASE test SURVIVE ZONE FAILURE`,
			Reset: "DROP DATABASE TEST",
		},
	}

	RunRoundTripBenchmarkMultiRegion(b, tests)
}

func BenchmarkAlterTableLocality(b *testing.B) {
	tests := []bench.RoundTripBenchTestCase{
		{
			Name: "alter from global to regional by table",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east-1" REGIONS "us-east-1", "us-central-1", "us-west-1";
USE test;
CREATE TABLE TEST (p int) LOCALITY GLOBAL;
`,
			Stmt:  `ALTER TABLE test SET LOCALITY REGIONAL IN PRIMARY REGION`,
			Reset: "DROP DATABASE TEST",
		},
		{
			Name: "alter from regional by table to global",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east-1" REGIONS "us-east-1", "us-central-1", "us-west-1";
USE test;
CREATE TABLE test (p int) LOCALITY REGIONAL IN PRIMARY REGION;
`,
			Stmt:  `ALTER TABLE test SET LOCALITY GLOBAL`,
			Reset: "DROP DATABASE TEST",
		},
		{
			Name: "alter from global to rbr",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east-1" REGIONS "us-east-1", "us-central-1", "us-west-1";
CREATE TABLE test.tb (p int) LOCALITY GLOBAL;
`,
			Stmt:  `ALTER TABLE test.tb SET LOCALITY REGIONAL BY ROW`,
			Reset: "DROP DATABASE TEST",
		},
		{
			Name: "alter from regional by table to rbr",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east-1" REGIONS "us-east-1", "us-central-1", "us-west-1";
USE test;
CREATE TABLE TEST (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
`,
			Stmt:  `ALTER TABLE test SET LOCALITY REGIONAL BY ROW`,
			Reset: "DROP DATABASE TEST",
		},
		{
			Name: "alter from rbr to regional by table",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east-1" REGIONS "us-east-1", "us-central-1", "us-west-1";
USE test;
CREATE TABLE TEST (p int) LOCALITY REGIONAL BY ROW;
`,
			Stmt:  `ALTER TABLE test SET LOCALITY REGIONAL BY TABLE IN PRIMARY REGION`,
			Reset: "DROP DATABASE TEST",
		},
		{
			Name: "alter from rbr to global",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east-1" REGIONS "us-east-1", "us-central-1", "us-west-1";
USE test;
CREATE TABLE TEST (p int) LOCALITY REGIONAL BY ROW;
`,
			Stmt:  `ALTER TABLE test SET LOCALITY GLOBAL`,
			Reset: "DROP DATABASE TEST",
		},
	}

	RunRoundTripBenchmarkMultiRegion(b, tests)
}
