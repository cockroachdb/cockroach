// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltestutils

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/stretchr/testify/require"
)

// ShowCreateTableTestCase is a test case for ShowCreateTableTest.
type ShowCreateTableTestCase struct {
	// CreateStatement is the statement used to create the table.
	// A %s may be used to signify the table name.
	CreateStatement string
	// Expect is the statement that is expected from SHOW CREATE TABLE.
	// A %s may be used to signify the table name.
	Expect string
	// Database is the database to execute on.
	// Execute on "d" by default.
	Database string
}

// ShowCreateTableTest tests the output for SHOW CREATE TABLE matches
// the expect values. Furthermore, it round trips SHOW CREATE TABLE
// statements to ensure they produces an identical SHOW CREATE TABLE.
func ShowCreateTableTest(
	t *testing.T, extraQuerySetup string, testCases []ShowCreateTableTestCase,
) {
	params, _ := tests.CreateTestServerParams()
	params.Locality.Tiers = []roachpb.Tier{
		{Key: "region", Value: "us-west1"},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
    SET CLUSTER SETTING sql.cross_db_fks.enabled = TRUE;
		CREATE DATABASE d;
		USE d;
		-- Create a table we can point FKs to.
		CREATE TABLE items (
			a int8,
			b int8,
			c int8 unique,
			primary key (a, b)
		);
		-- Create a database we can cross reference.
		CREATE DATABASE o;
		CREATE TABLE o.foo(x int primary key);
	`); err != nil {
		t.Fatal(err)
	}
	if extraQuerySetup != "" {
		if _, err := sqlDB.Exec(extraQuerySetup); err != nil {
			t.Fatal(err)
		}
	}
	for i, test := range testCases {
		name := fmt.Sprintf("t%d", i)
		t.Run(name, func(t *testing.T) {
			if test.Expect == "" {
				test.Expect = test.CreateStatement
			}
			db := test.Database
			if db == "" {
				db = "d"
			}
			_, err := sqlDB.Exec("USE $1", db)
			require.NoError(t, err)
			stmt := fmt.Sprintf(test.CreateStatement, name)
			expect := fmt.Sprintf(test.Expect, name)
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
			}
			if _, err := sqlDB.Exec(fmt.Sprintf("DROP TABLE %s", name)); err != nil {
				t.Fatal(err)
			}
			// Re-insert to make sure it's round-trippable.
			name += "_roundtrip"
			expect = fmt.Sprintf(test.Expect, name)
			if _, err := sqlDB.Exec(expect); err != nil {
				t.Fatalf("reinsert failure: %s: %s", expect, err)
			}
			row = sqlDB.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", name))
			if err := row.Scan(&scanName, &create); err != nil {
				t.Fatal(err)
			}
			if create != expect {
				t.Fatalf("round trip statement: %s\ngot: %s", expect, create)
			}
			if _, err := sqlDB.Exec(fmt.Sprintf("DROP TABLE %s", name)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
