// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

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

func TestCommentOnSequence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE SEQUENCE s;
	`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		exec   string
		query  string
		expect gosql.NullString
	}{
		{
			`COMMENT ON SEQUENCE s IS 'foo'`,
			`SELECT obj_description('s'::regclass)`,
			gosql.NullString{String: `foo`, Valid: true},
		},
		{
			`COMMENT ON SEQUENCE s IS NULL`,
			`SELECT obj_description('s'::regclass)`,
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

func TestCommentOnSequenceWhenDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE SEQUENCE s;
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`COMMENT ON SEQUENCE s IS 'foo'`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`DROP SEQUENCE s`); err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow(`SELECT comment FROM system.comments LIMIT 1`)
	var comment string
	err := row.Scan(&comment)
	if !errors.Is(err, gosql.ErrNoRows) {
		if err != nil {
			t.Fatal(err)
		}

		t.Fatal("dropped comment remain comment")
	}
}
