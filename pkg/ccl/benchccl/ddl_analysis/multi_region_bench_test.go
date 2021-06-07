package ddl_analysis

import (
	"testing"

	bench "github.com/cockroachdb/cockroach/pkg/bench/ddl_analysis"
)

const (
	multipleTableFixture = `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2";
USE test;
CREATE TABLE test11 (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
CREATE TABLE test12 (p int) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
CREATE TABLE test21 (p int) LOCALITY REGIONAL BY ROW;
CREATE TABLE test22 (p int) LOCALITY REGIONAL BY ROW;
CREATE TABLE test23 (p int) LOCALITY REGIONAL BY ROW;
CREATE TABLE test24 (p int) LOCALITY REGIONAL BY ROW;
CREATE TABLE test31 (p int) LOCALITY GLOBAL;
CREATE TABLE test32 (p int) LOCALITY GLOBAL;
`
)

func BenchmarkAlterDatabaseMultiRegion(b *testing.B) {
	tests := []bench.RoundTripBenchTestCase{
		{
			Name:  "alter database single primary region",
			Setup: "CREATE DATABASE test",
			Stmt:  `ALTER DATABASE test SET PRIMARY REGION "us-east1"`,
			Reset: "DROP DATABASE test",
		},
		{
			Name:  "alter database add non-primary region",
			Setup: `CREATE DATABASE test PRIMARY REGION "us-east1"`,
			Stmt:  `ALTER DATABASE test ADD REGION "us-east2"`,
			Reset: "DROP DATABASE test",
		},
	}

	RunRoundTripBenchmarkMultiRegion(b, tests)
}

func BenchmarkAlterRegions(b *testing.B) {
	tests := []bench.RoundTripBenchTestCase{
		{
			Name:  "drop region from many tables",
			Setup: multipleTableFixture,
			Stmt:  `ALTER DATABASE test DROP REGION "us-east2"`,
			Reset: "DROP DATABASE test",
		},
		{
			Name:  "add region to many tables",
			Setup: multipleTableFixture,
			Stmt:  `ALTER DATABASE test ADD REGION "us-east3"`,
			Reset: "DROP DATABASE test",
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
			Stmt:  `ALTER DATABASE test SET PRIMARY REGION "us-east1"`,
			Reset: "DROP DATABASE test",
		},
		{
			Name:  "alter database with many tables change region",
			Setup: multipleTableFixture,
			Stmt:  `ALTER DATABASE test SET PRIMARY REGION "us-east2"`,
			Reset: "DROP DATABASE test",
		},
	}

	RunRoundTripBenchmarkMultiRegion(b, tests)
}

func BenchmarkAlterSurvivalGoals(b *testing.B) {
	tests := []bench.RoundTripBenchTestCase{
		{
			Name: "alter from zone to region",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2", "us-east3";
`,
			Stmt:  `ALTER DATABASE test SURVIVE REGION FAILURE`,
			Reset: "DROP DATABASE test",
		},
		{
			Name: "alter from region to zone",
			Setup: `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2", "us-east3" SURVIVE REGION FAILURE;
`,
			Stmt:  `ALTER DATABASE test SURVIVE ZONE FAILURE`,
			Reset: "DROP DATABASE test",
		},
	}

	RunRoundTripBenchmarkMultiRegion(b, tests)
}

func BenchmarkAlterTableLocality(b *testing.B) {
	tests := []bench.RoundTripBenchTestCase{
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
