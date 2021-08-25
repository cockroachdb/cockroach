// Copyright 2019 The Cockroach Authors.
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
)

func TestCommentOnConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
		CREATE DATABASE dbase;
		SET DATABASE = dbase;
		CREATE TABLE test_table1 (val INT, CONSTRAINT test_constraint1 CHECK (val > 0));
	`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		exec   string
		query  string
		expect gosql.NullString
	}{
		{
			`COMMENT ON CONSTRAINT test_constraint1 IS 'test_comment1';`,
			`SELECT conname FROM pg_constraint WHERE conname='test_constraint1';`,
			gosql.NullString{String: `test_constraint1`, Valid: true},
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
