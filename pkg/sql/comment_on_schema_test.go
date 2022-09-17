// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func TestCommentOnSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
		CREATE SCHEMA d;
	`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		exec   string
		query  string
		expect gosql.NullString
	}{
		{
			`COMMENT ON SCHEMA d IS 'foo'`,
			`SELECT obj_description(oid, 'pg_namespace') FROM pg_namespace WHERE nspname = 'd'`,
			gosql.NullString{String: `foo`, Valid: true},
		},
		{
			`ALTER SCHEMA d RENAME TO d2`,
			`SELECT obj_description(oid, 'pg_namespace') FROM pg_namespace WHERE nspname = 'd2'`,
			gosql.NullString{String: `foo`, Valid: true},
		},
		{
			`COMMENT ON SCHEMA d2 IS NULL`,
			`SELECT obj_description(oid, 'pg_namespace') FROM pg_namespace WHERE nspname = 'd2'`,
			gosql.NullString{Valid: false},
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

func TestCommentOnSchemaWhenDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
		CREATE SCHEMA d;
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`COMMENT ON SCHEMA d IS 'foo'`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`DROP SCHEMA d`); err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow(`SELECT comment FROM system.comments LIMIT 1`)
	var comment string
	err := row.Scan(&comment)
	if !errors.Is(err, gosql.ErrNoRows) {
		if err != nil {
			t.Fatal(err)
		}

		t.Fatal("comment remaining in system.comments despite drop")
	}
}
