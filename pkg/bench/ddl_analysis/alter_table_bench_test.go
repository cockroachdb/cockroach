// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bench

import "testing"

func BenchmarkAlterTableAddColumn(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "alter table add 1 column",
			setup: "CREATE TABLE alter_table()",
			stmt:  "ALTER TABLE alter_table ADD COLUMN a INT",
		},
		{
			name:  "alter table add 2 columns",
			setup: "CREATE TABLE alter_table()",
			stmt: "ALTER TABLE alter_table ADD COLUMN a INT, " +
				"ADD COLUMN b INT",
		},
		{
			name:  "alter table add 3 columns",
			setup: "CREATE TABLE alter_table()",
			stmt: "ALTER TABLE alter_table ADD COLUMN a INT, " +
				"ADD COLUMN b INT, ADD COLUMN c INT",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkAlterTableAddCheckConstraint(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "alter table add 1 check constraint",
			setup: "CREATE TABLE alter_table(x INT, y INT, z INT)",
			stmt:  "ALTER TABLE alter_table ADD CONSTRAINT ck CHECK(x > 0)",
		},
		{
			name:  "alter table add 2 check constraints",
			setup: "CREATE TABLE alter_table(x INT, y INT, z INT)",
			stmt: "ALTER TABLE alter_table ADD CONSTRAINT ck1 CHECK(x > 0), " +
				"ADD CONSTRAINT ck2 CHECK(y > 0)",
		},
		{
			name:  "alter table add 3 check constraints",
			setup: "CREATE TABLE alter_table(x INT, y INT, z INT)",
			stmt: "ALTER TABLE alter_table ADD CONSTRAINT ck1 CHECK(x > 0), " +
				"ADD CONSTRAINT ck2 CHECK(y > 0), ADD CONSTRAINT ck3 CHECK(z > 0)",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkAlterTableAddForeignKey(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name: "alter table add 1 foreign key",
			setup: `CREATE TABLE alter_table(x INT, y INT, z INT); 
CREATE TABLE fk_table1(x INT PRIMARY KEY);`,
			stmt: "ALTER TABLE alter_table " +
				"ADD CONSTRAINT fk FOREIGN KEY (x) REFERENCES fk_table1(x)",
		},
		{
			name: "alter table add 2 foreign keys",
			setup: `CREATE TABLE alter_table(x INT, y INT, z INT); 
CREATE TABLE fk_table1(x INT PRIMARY KEY); 
CREATE TABLE fk_table2(y INT PRIMARY KEY);`,
			stmt: "ALTER TABLE alter_table " +
				"ADD CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES fk_table1(x)," +
				"ADD CONSTRAINT fk2 FOREIGN KEY (y) REFERENCES fk_table2(y)",
		},
		{
			name: "alter table add 3 foreign keys",
			setup: `CREATE TABLE alter_table(x INT, y INT, z INT); 
CREATE TABLE fk_table1(x INT PRIMARY KEY); 
CREATE TABLE fk_table2(y INT PRIMARY KEY);
CREATE TABLE fk_table3(z INT PRIMARY KEY);`,
			stmt: "ALTER TABLE alter_table " +
				"ADD CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES fk_table1(x)," +
				"ADD CONSTRAINT fk2 FOREIGN KEY (y) REFERENCES fk_table2(y)," +
				"ADD CONSTRAINT fk3 FOREIGN KEY (z) REFERENCES fk_table3(z)",
		},
		{
			name: "alter table add foreign key with 3 columns",
			setup: `CREATE TABLE referencer(x INT, y INT, z INT, PRIMARY KEY(x,y,z));
CREATE TABLE referenced(x INT, y INT, z INT, PRIMARY KEY(x,y,z));`,
			stmt: "ALTER TABLE referencer " +
				"ADD CONSTRAINT fk FOREIGN KEY (x,y,z) REFERENCES referenced(x,y,z)",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkAlterTableDropColumn(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "alter table drop 1 column",
			setup: `CREATE TABLE alter_table(a INT)`,
			stmt:  "ALTER TABLE alter_table DROP COLUMN a",
		},
		{
			name:  "alter table drop 2 columns",
			setup: `CREATE TABLE alter_table(a INT, b INT)`,
			stmt:  "ALTER TABLE alter_table DROP COLUMN a, DROP COLUMN b",
		},
		{
			name:  "alter table drop 3 columns",
			setup: `CREATE TABLE alter_table(a INT, b INT, c INT)`,
			stmt: "ALTER TABLE alter_table DROP COLUMN a, DROP COLUMN b, " +
				"DROP COLUMN c",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkAlterTableDropConstraint(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "alter table drop 1 check constraint",
			setup: `CREATE TABLE alter_table(a INT, CONSTRAINT ck CHECK (a > 0))`,
			stmt:  "ALTER TABLE alter_table DROP CONSTRAINT ck",
		},
		{
			name: "alter table drop 2 check constraints",
			setup: `CREATE TABLE alter_table(a INT, CONSTRAINT ck1 CHECK (a > 0),
b INT, CONSTRAINT ck2 CHECK(b > 0))`,
			stmt: "ALTER TABLE alter_table DROP CONSTRAINT ck1, DROP CONSTRAINT ck2",
		},
		{
			name: "alter table drop 3 check constraints",
			setup: `CREATE TABLE alter_table(a INT, CONSTRAINT ck1 CHECK (a > 0),
b INT, CONSTRAINT ck2 CHECK(b > 0),
c INT, CONSTRAINT ck3 CHECK (c > 0))`,
			stmt: "ALTER TABLE alter_table DROP CONSTRAINT ck1, DROP CONSTRAINT ck2, " +
				"DROP CONSTRAINT ck3",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkAlterTableSplit(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "alter table split at 1 value",
			setup: `CREATE TABLE alter_table(a INT)`,
			stmt:  "ALTER TABLE alter_table SPLIT AT VALUES (1)",
		},
		{
			name:  "alter table split at 2 values",
			setup: `CREATE TABLE alter_table(a INT, b INT)`,
			stmt:  "ALTER TABLE alter_table SPLIT AT VALUES (1), (2)",
		},
		{
			name:  "alter table split at 3 values",
			setup: `CREATE TABLE alter_table(a INT, b INT, c INT)`,
			stmt:  "ALTER TABLE alter_table SPLIT AT VALUES (1), (2), (3)",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkAlterTableUnsplit(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name: "alter table unsplit at 1 value",
			setup: `CREATE TABLE alter_table(a INT);
ALTER TABLE alter_table SPLIT AT VALUES (1);`,
			stmt: "ALTER TABLE alter_table UNSPLIT AT VALUES (1)",
		},
		{
			name: "alter table unsplit at 2 values",
			setup: `CREATE TABLE alter_table(a INT, b INT);
ALTER TABLE alter_table SPLIT AT VALUES (1), (2);`,
			stmt: "ALTER TABLE alter_table UNSPLIT AT VALUES (1), (2)",
		},
		{
			name: "alter table unsplit at 3 values",
			setup: `CREATE TABLE alter_table(a INT, b INT, c INT);
ALTER TABLE alter_table SPLIT AT VALUES (1), (2), (3)`,
			stmt: "ALTER TABLE alter_table UNSPLIT AT VALUES (1), (2), (3)",
		},
	}

	RunRoundTripBenchmark(b, tests)
}

func BenchmarkAlterTableConfigureZone(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "alter table configure zone 5 replicas",
			setup: `CREATE TABLE alter_table(a INT);`,
			stmt:  "ALTER TABLE alter_table CONFIGURE ZONE USING num_replicas = 5",
		},
		{
			name:  "alter table configure zone 7 replicas ",
			setup: `CREATE TABLE alter_table(a INT);`,
			stmt:  "ALTER TABLE alter_table CONFIGURE ZONE USING num_replicas = 7",
		},
		{
			name:  "alter table configure zone ranges",
			setup: `CREATE TABLE alter_table(a INT);`,
			stmt: "ALTER TABLE alter_table CONFIGURE ZONE USING " +
				"range_min_bytes = 0, range_max_bytes = 90000",
		},
	}

	RunRoundTripBenchmark(b, tests)
}
