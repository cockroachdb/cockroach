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

	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestShowCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

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
			stmt: `CREATE TABLE %s (
	i INT,
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP DEFAULT NOW(),
	CHECK (i > 0)
)`,
			expect: `CREATE TABLE %s (
	i INT NULL,
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP NULL DEFAULT NOW(),
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s),
	CHECK (i > 0)
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	i INT CHECK (i > 0),
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP DEFAULT NOW()
)`,
			expect: `CREATE TABLE %s (
	i INT NULL,
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP NULL DEFAULT NOW(),
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s),
	CHECK (i > 0)
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	i INT NULL,
	s STRING NULL,
	CONSTRAINT ck CHECK (i > 0)
)`,
			expect: `CREATE TABLE %s (
	i INT NULL,
	s STRING NULL,
	FAMILY "primary" (i, rowid),
	FAMILY fam_1_s (s),
	CONSTRAINT ck CHECK (i > 0)
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	i INT PRIMARY KEY
)`,
			expect: `CREATE TABLE %s (
	i INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i),
	FAMILY "primary" (i)
)`,
		},
		{
			stmt: `
				CREATE TABLE %s (i INT, f FLOAT, s STRING, d DATE);
				CREATE INDEX idx_if on %[1]s (f, i) STORING (s, d);
				CREATE UNIQUE INDEX on %[1]s (d);
			`,
			expect: `CREATE TABLE %s (
	i INT NULL,
	f FLOAT NULL,
	s STRING NULL,
	d DATE NULL,
	INDEX idx_if (f, i) STORING (s, d),
	UNIQUE INDEX %[1]s_d_key (d),
	FAMILY "primary" (i, f, d, rowid),
	FAMILY fam_1_s (s)
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	"te""st" INT NOT NULL,
	CONSTRAINT "pri""mary" PRIMARY KEY ("te""st"),
	FAMILY "primary" ("te""st")
)`,
		},
	}
	for i, test := range tests {
		name := fmt.Sprintf("T%d", i)
		fmt.Println("NAME", name)
		if test.expect == "" {
			test.expect = test.stmt
		}
		stmt := fmt.Sprintf(test.stmt, name)
		expect := fmt.Sprintf(test.expect, name)
		if _, err := sqlDB.Exec(stmt); err != nil {
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
		if create != expect {
			t.Fatalf("statement: %s\ngot: %s\nexpected: %s", stmt, create, expect)
			continue
		}
		if _, err := sqlDB.Exec(fmt.Sprintf("DROP TABLE %s", name)); err != nil {
			t.Fatal(err)
		}
		// Re-insert to make sure it's round-trippable.
		name += "_2"
		expect = fmt.Sprintf(test.expect, name)
		if _, err := sqlDB.Exec(expect); err != nil {
			t.Fatalf("reinsert failure: %s: %s", expect, err)
		}
		row = sqlDB.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", name))
		if err := row.Scan(&scanName, &create); err != nil {
			t.Fatal(err)
		}
		if create != expect {
			t.Errorf("round trip statement: %s\ngot: %s", expect, create)
			continue
		}
		if _, err := sqlDB.Exec(fmt.Sprintf("DROP TABLE %s", name)); err != nil {
			t.Fatal(err)
		}
	}
}
