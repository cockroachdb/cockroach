// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package sql_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestShowCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, _ := createTestServerContext()
	server, sqlDB, _ := setupWithContext(t, ctx)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
	`); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		stmt   string
		expect string // empty means identical to stmt
	}{
		{
			stmt: `CREATE TABLE t (
	i INT,
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP DEFAULT NOW()
)`,
			expect: `CREATE TABLE "T" (
	"i" INT NULL,
	"s" STRING NULL,
	"v" FLOAT NOT NULL,
	"t" TIMESTAMP NULL DEFAULT NOW()
)`,
		},
		{
			stmt: `CREATE TABLE T (
	i INT PRIMARY KEY
)`,
			expect: `CREATE TABLE "T" (
	"i" INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY ("i")
)`,
		},
		{
			stmt: `
				CREATE TABLE T (i INT, f FLOAT, s STRING, d DATE);
				CREATE INDEX idx_if on T (f, i) STORING (s, d);
				CREATE UNIQUE INDEX on T (d);
			`,
			expect: `CREATE TABLE "T" (
	"i" INT NULL,
	"f" FLOAT NULL,
	"s" STRING NULL,
	"d" DATE NULL,
	INDEX "idx_if" ("f", "i") STORING ("s", "d"),
	UNIQUE INDEX "T_d_key" ("d")
)`,
		},
		{
			stmt: `CREATE TABLE "T" (
	"te""st" INT NOT NULL,
	CONSTRAINT "pri""mary" PRIMARY KEY ("te""st")
)`,
		},
	}
	const name = "T"
	for _, test := range tests {
		if test.expect == "" {
			test.expect = test.stmt
		}
		if _, err := sqlDB.Exec(test.stmt); err != nil {
			t.Fatal(err)
		}
		row := sqlDB.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", name))
		var scanName, create string
		if err := row.Scan(&scanName, &create); err != nil {
			t.Fatal(err)
		}
		if scanName != name {
			t.Fatalf("expected table name %s, got %s", name, scanName)
		}
		if create != test.expect {
			t.Fatalf("statement: %s\ngot: %s\nexpected: %s", test.stmt, create, test.expect)
			continue
		}
		if _, err := sqlDB.Exec(fmt.Sprintf("DROP TABLE %s", name)); err != nil {
			t.Fatal(err)
		}
		// Re-insert to make sure it's round-trippable.
		if _, err := sqlDB.Exec(test.expect); err != nil {
			t.Fatalf("reinsert failure: %s: %s", test.expect, err)
		}
		row = sqlDB.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", name))
		if err := row.Scan(&scanName, &create); err != nil {
			t.Fatal(err)
		}
		if create != test.expect {
			t.Errorf("round trip statement: %s\ngot: %s", test.expect, create)
			continue
		}
		if _, err := sqlDB.Exec(fmt.Sprintf("DROP TABLE %s", name)); err != nil {
			t.Fatal(err)
		}
	}
}
