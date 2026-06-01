// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

var alterTableAddColumnCases = []RoundTripBenchTestCase{
	{
		Name:  "alter table add 1 column",
		Setup: "CREATE TABLE alter_table()",
		Stmt:  "ALTER TABLE alter_table ADD COLUMN a INT",
	},
	{
		Name:  "alter table add 2 columns",
		Setup: "CREATE TABLE alter_table()",
		Stmt: "ALTER TABLE alter_table ADD COLUMN a INT, " +
			"ADD COLUMN b INT",
	},
	{
		Name:  "alter table add 3 columns",
		Setup: "CREATE TABLE alter_table()",
		Stmt: "ALTER TABLE alter_table ADD COLUMN a INT, " +
			"ADD COLUMN b INT, ADD COLUMN c INT",
	},
}

// benchmark-ci: benchtime=20x
func BenchmarkAlterTableAddColumn(b *testing.B) {
	runCPUMemBenchmark(bShim{b}, alterTableAddColumnCases, defaultCC)
}

func TestBenchmarkExpectation_AlterTableAddColumn(t *testing.T) {
	runExpectation(t, "AlterTableAddColumn", alterTableAddColumnCases, defaultCC)
}

var alterTableAddCheckConstraintCases = []RoundTripBenchTestCase{
	{
		Name:  "alter table add 1 check constraint",
		Setup: "CREATE TABLE alter_table(x INT, y INT, z INT)",
		Stmt:  "ALTER TABLE alter_table ADD CONSTRAINT ck CHECK(x > 0)",
	},
	{
		Name:  "alter table add 2 check constraints",
		Setup: "CREATE TABLE alter_table(x INT, y INT, z INT)",
		Stmt: "ALTER TABLE alter_table ADD CONSTRAINT ck1 CHECK(x > 0), " +
			"ADD CONSTRAINT ck2 CHECK(y > 0)",
	},
	{
		Name:  "alter table add 3 check constraints",
		Setup: "CREATE TABLE alter_table(x INT, y INT, z INT)",
		Stmt: "ALTER TABLE alter_table ADD CONSTRAINT ck1 CHECK(x > 0), " +
			"ADD CONSTRAINT ck2 CHECK(y > 0), ADD CONSTRAINT ck3 CHECK(z > 0)",
	},
}

// benchmark-ci: benchtime=20x
func BenchmarkAlterTableAddCheckConstraint(b *testing.B) {
	runCPUMemBenchmark(bShim{b}, alterTableAddCheckConstraintCases, defaultCC)
}

func TestBenchmarkExpectation_AlterTableAddCheckConstraint(t *testing.T) {
	runExpectation(t, "AlterTableAddCheckConstraint", alterTableAddCheckConstraintCases, defaultCC)
}

var alterTableAddForeignKeyCases = []RoundTripBenchTestCase{
	{
		Name: "alter table add 1 foreign key",
		Setup: `CREATE TABLE alter_table(x INT, y INT, z INT);
CREATE TABLE fk_table1(x INT PRIMARY KEY);`,
		Stmt: "ALTER TABLE alter_table " +
			"ADD CONSTRAINT fk FOREIGN KEY (x) REFERENCES fk_table1(x)",
	},
	{
		Name: "alter table add 2 foreign keys",
		Setup: `CREATE TABLE alter_table(x INT, y INT, z INT);
CREATE TABLE fk_table1(x INT PRIMARY KEY);
CREATE TABLE fk_table2(y INT PRIMARY KEY);`,
		Stmt: "ALTER TABLE alter_table " +
			"ADD CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES fk_table1(x)," +
			"ADD CONSTRAINT fk2 FOREIGN KEY (y) REFERENCES fk_table2(y)",
	},
	{
		Name: "alter table add 3 foreign keys",
		Setup: `CREATE TABLE alter_table(x INT, y INT, z INT);
CREATE TABLE fk_table1(x INT PRIMARY KEY);
CREATE TABLE fk_table2(y INT PRIMARY KEY);
CREATE TABLE fk_table3(z INT PRIMARY KEY);`,
		Stmt: "ALTER TABLE alter_table " +
			"ADD CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES fk_table1(x)," +
			"ADD CONSTRAINT fk2 FOREIGN KEY (y) REFERENCES fk_table2(y)," +
			"ADD CONSTRAINT fk3 FOREIGN KEY (z) REFERENCES fk_table3(z)",
	},
	{
		Name: "alter table add foreign key with 3 columns",
		Setup: `CREATE TABLE referencer(x INT, y INT, z INT, PRIMARY KEY(x,y,z));
CREATE TABLE referenced(x INT, y INT, z INT, PRIMARY KEY(x,y,z));`,
		Stmt: "ALTER TABLE referencer " +
			"ADD CONSTRAINT fk FOREIGN KEY (x,y,z) REFERENCES referenced(x,y,z)",
	},
}

// benchmark-ci: benchtime=20x
func BenchmarkAlterTableAddForeignKey(b *testing.B) {
	runCPUMemBenchmark(bShim{b}, alterTableAddForeignKeyCases, defaultCC)
}

func TestBenchmarkExpectation_AlterTableAddForeignKey(t *testing.T) {
	runExpectation(t, "AlterTableAddForeignKey", alterTableAddForeignKeyCases, defaultCC)
}

var alterTableDropColumnCases = []RoundTripBenchTestCase{
	{
		Name:  "alter table drop 1 column",
		Setup: `CREATE TABLE alter_table(a INT)`,
		Stmt:  "ALTER TABLE alter_table DROP COLUMN a",
	},
	{
		Name:  "alter table drop 2 columns",
		Setup: `CREATE TABLE alter_table(a INT, b INT)`,
		Stmt:  "ALTER TABLE alter_table DROP COLUMN a, DROP COLUMN b",
	},
	{
		Name:  "alter table drop 3 columns",
		Setup: `CREATE TABLE alter_table(a INT, b INT, c INT)`,
		Stmt: "ALTER TABLE alter_table DROP COLUMN a, DROP COLUMN b, " +
			"DROP COLUMN c",
	},
}

// benchmark-ci: benchtime=20x
func BenchmarkAlterTableDropColumn(b *testing.B) {
	runCPUMemBenchmark(bShim{b}, alterTableDropColumnCases, defaultCC)
}

func TestBenchmarkExpectation_AlterTableDropColumn(t *testing.T) {
	runExpectation(t, "AlterTableDropColumn", alterTableDropColumnCases, defaultCC)
}

var alterTableDropConstraintCases = []RoundTripBenchTestCase{
	{
		Name:  "alter table drop 1 check constraint",
		Setup: `CREATE TABLE alter_table(a INT, CONSTRAINT ck CHECK (a > 0))`,
		Stmt:  "ALTER TABLE alter_table DROP CONSTRAINT ck",
	},
	{
		Name: "alter table drop 2 check constraints",
		Setup: `CREATE TABLE alter_table(a INT, CONSTRAINT ck1 CHECK (a > 0),
b INT, CONSTRAINT ck2 CHECK(b > 0))`,
		Stmt: "ALTER TABLE alter_table DROP CONSTRAINT ck1, DROP CONSTRAINT ck2",
	},
	{
		Name: "alter table drop 3 check constraints",
		Setup: `CREATE TABLE alter_table(a INT, CONSTRAINT ck1 CHECK (a > 0),
b INT, CONSTRAINT ck2 CHECK(b > 0),
c INT, CONSTRAINT ck3 CHECK (c > 0))`,
		Stmt: "ALTER TABLE alter_table DROP CONSTRAINT ck1, DROP CONSTRAINT ck2, " +
			"DROP CONSTRAINT ck3",
	},
}

// benchmark-ci: benchtime=20x
func BenchmarkAlterTableDropConstraint(b *testing.B) {
	runCPUMemBenchmark(bShim{b}, alterTableDropConstraintCases, defaultCC)
}

func TestBenchmarkExpectation_AlterTableDropConstraint(t *testing.T) {
	runExpectation(t, "AlterTableDropConstraint", alterTableDropConstraintCases, defaultCC)
}

var alterTableSplitCases = []RoundTripBenchTestCase{
	{
		Name:  "alter table split at 1 value",
		Setup: `CREATE TABLE alter_table(a INT)`,
		Stmt:  "ALTER TABLE alter_table SPLIT AT VALUES (1)",
	},
	{
		Name:  "alter table split at 2 values",
		Setup: `CREATE TABLE alter_table(a INT, b INT)`,
		Stmt:  "ALTER TABLE alter_table SPLIT AT VALUES (1), (2)",
	},
	{
		Name:  "alter table split at 3 values",
		Setup: `CREATE TABLE alter_table(a INT, b INT, c INT)`,
		Stmt:  "ALTER TABLE alter_table SPLIT AT VALUES (1), (2), (3)",
	},
}

// benchmark-ci: benchtime=20x
func BenchmarkAlterTableSplit(b *testing.B) {
	runCPUMemBenchmark(bShim{b}, alterTableSplitCases, defaultCC)
}

func TestBenchmarkExpectation_AlterTableSplit(t *testing.T) {
	runExpectation(t, "AlterTableSplit", alterTableSplitCases, defaultCC)
}

var alterTableUnsplitCases = []RoundTripBenchTestCase{
	{
		Name: "alter table unsplit at 1 value",
		Setup: `CREATE TABLE alter_table(a INT);
ALTER TABLE alter_table SPLIT AT VALUES (1);`,
		Stmt: "ALTER TABLE alter_table UNSPLIT AT VALUES (1)",
	},
	{
		Name: "alter table unsplit at 2 values",
		Setup: `CREATE TABLE alter_table(a INT, b INT);
ALTER TABLE alter_table SPLIT AT VALUES (1), (2);`,
		Stmt: "ALTER TABLE alter_table UNSPLIT AT VALUES (1), (2)",
	},
	{
		Name: "alter table unsplit at 3 values",
		Setup: `CREATE TABLE alter_table(a INT, b INT, c INT);
ALTER TABLE alter_table SPLIT AT VALUES (1), (2), (3)`,
		Stmt: "ALTER TABLE alter_table UNSPLIT AT VALUES (1), (2), (3)",
	},
}

// benchmark-ci: benchtime=20x
func BenchmarkAlterTableUnsplit(b *testing.B) {
	runCPUMemBenchmark(bShim{b}, alterTableUnsplitCases, defaultCC)
}

func TestBenchmarkExpectation_AlterTableUnsplit(t *testing.T) {
	runExpectation(t, "AlterTableUnsplit", alterTableUnsplitCases, defaultCC)
}

var alterTableConfigureZoneCases = []RoundTripBenchTestCase{
	{
		Name:  "alter table configure zone 5 replicas",
		Setup: `CREATE TABLE alter_table(a INT);`,
		Stmt:  "ALTER TABLE alter_table CONFIGURE ZONE USING num_replicas = 5",
	},
	{
		Name:  "alter table configure zone 7 replicas ",
		Setup: `CREATE TABLE alter_table(a INT);`,
		Stmt:  "ALTER TABLE alter_table CONFIGURE ZONE USING num_replicas = 7",
	},
	{
		Name:  "alter table configure zone ranges",
		Setup: `CREATE TABLE alter_table(a INT);`,
		Stmt: "ALTER TABLE alter_table CONFIGURE ZONE USING " +
			"range_min_bytes = 0, range_max_bytes = 500000000",
	},
}

// benchmark-ci: benchtime=20x
func BenchmarkAlterTableConfigureZone(b *testing.B) {
	runCPUMemBenchmark(bShim{b}, alterTableConfigureZoneCases, defaultCC)
}

func TestBenchmarkExpectation_AlterTableConfigureZone(t *testing.T) {
	runExpectation(t, "AlterTableConfigureZone", alterTableConfigureZoneCases, defaultCC)
}
