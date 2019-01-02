// Copyright 2018 The Cockroach Authors.
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

package sql_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCommentOnColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (c1 INT, c2 INT, c3 INT);
	`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		exec   string
		query  string
		expect gosql.NullString
	}{
		{
			`COMMENT ON COLUMN t.c1 IS 'foo'`,
			`SELECT col_description(attrelid, attnum) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c1'`,
			gosql.NullString{String: `foo`, Valid: true},
		},
		{
			`TRUNCATE t`,
			`SELECT col_description(attrelid, attnum) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c1'`,
			gosql.NullString{String: `foo`, Valid: true},
		},
		{
			`ALTER TABLE t RENAME COLUMN c1 TO c1_1`,
			`SELECT col_description(attrelid, attnum) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c1_1'`,
			gosql.NullString{String: `foo`, Valid: true},
		},
		{
			`COMMENT ON COLUMN t.c1_1 IS NULL`,
			`SELECT col_description(attrelid, attnum) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c1_1'`,
			gosql.NullString{Valid: false},
		},
		{
			`COMMENT ON COLUMN t.c3 IS 'foo'`,
			`SELECT col_description(attrelid, attnum) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c3'`,
			gosql.NullString{String: `foo`, Valid: true},
		},
		{
			`ALTER TABLE t DROP COLUMN c2`,
			`SELECT col_description(attrelid, attnum) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c3'`,
			gosql.NullString{String: `foo`, Valid: true},
		},
	}

	for _, tc := range testCases {
		if _, err := db.Exec(tc.exec); err != nil {
			t.Fatal(err)
		}

		row := db.QueryRow(tc.query)
		var comment gosql.NullString
		if err := row.Scan(&comment); err != nil {
			t.Fatal(err)
		}
		if tc.expect != comment {
			t.Fatalf("expected comment %v, got %v", tc.expect, comment)
		}
	}
}

func TestCommentOnColumnTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (c INT);
		BEGIN;
		ALTER TABLE t ADD COLUMN x INT;
		COMMENT ON COLUMN t.x IS 'foo';
		COMMIT;
	`); err != nil {
		t.Fatal(err)
	}
}

func TestCommentOnColumnWhenDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (c INT);
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`COMMENT ON COLUMN t.c IS 'foo'`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`DROP TABLE t`); err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow(`SELECT comment FROM system.comments LIMIT 1`)
	var comment string
	err := row.Scan(&comment)
	if err != gosql.ErrNoRows {
		if err != nil {
			t.Fatal(err)
		}

		t.Fatal("comment remain")
	}
}

func TestCommentOnColumnWhenDropColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (c INT);
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`COMMENT ON COLUMN t.c IS 'foo'`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`ALTER TABLE t DROP COLUMN c`); err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow(`SELECT comment FROM system.comments LIMIT 1`)
	var comment string
	err := row.Scan(&comment)
	if err != gosql.ErrNoRows {
		if err != nil {
			t.Fatal(err)
		}

		t.Fatal("comment remain")
	}
}
