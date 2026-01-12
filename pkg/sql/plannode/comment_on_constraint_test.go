// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestCommentOnConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	runCommentOnTests(t, func(db *gosql.DB) {
		if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t ( a int UNIQUE, b numeric CONSTRAINT positive_price CHECK (b > 0), c int CHECK (b > c), CONSTRAINT pkey PRIMARY KEY (a,c));
		CREATE TABLE t2 (a UUID PRIMARY KEY, b int NOT NULL REFERENCES  t (a));
		CREATE SCHEMA s;
		CREATE TABLE s.t ( a int UNIQUE, b numeric CONSTRAINT positive_price CHECK (b > 0), c int CHECK (b > c), CONSTRAINT pkey PRIMARY KEY (a,c));
`); err != nil {
			t.Fatal(err)
		}

		testCases := []struct {
			exec   string
			query  string
			expect gosql.NullString
		}{
			{
				`COMMENT ON CONSTRAINT t_a_key ON t IS 'unique_comment'`,
				`SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname='t_a_key'`,
				gosql.NullString{String: `unique_comment`, Valid: true},
			},
			{
				`COMMENT ON CONSTRAINT t_a_key ON s.t IS 'unique_comment'`,
				`SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname='t_a_key'`,
				gosql.NullString{String: `unique_comment`, Valid: true},
			},
			{
				`COMMENT ON CONSTRAINT positive_price ON t IS 'check_comment'`,
				`SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname='positive_price'`,
				gosql.NullString{String: `check_comment`, Valid: true},
			},
			{
				`COMMENT ON CONSTRAINT check_b_c ON t IS 'check_defaultname_comment'`,
				`SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname='check_b_c'`,
				gosql.NullString{String: `check_defaultname_comment`, Valid: true},
			},
			{
				`COMMENT ON CONSTRAINT pkey ON t IS 'primary_userdef_comment'`,
				`SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname='pkey'`,
				gosql.NullString{String: `primary_userdef_comment`, Valid: true},
			},
			{
				`COMMENT ON CONSTRAINT "t2_pkey" ON t2 IS 'primary_comment'`,
				`SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname='t2_pkey'`,
				gosql.NullString{String: `primary_comment`, Valid: true},
			},
			{
				`COMMENT ON CONSTRAINT t2_b_fkey ON t2 IS 'fk_comment'`,
				`SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname='t2_b_fkey'`,
				gosql.NullString{String: `fk_comment`, Valid: true},
			},
			{
				`COMMENT ON CONSTRAINT t2_b_fkey ON t2 IS 'fk_comment'; COMMENT ON CONSTRAINT t2_b_fkey ON t2 IS NULL`,
				`SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname='t2_b_fkey'`,
				gosql.NullString{Valid: false},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.exec, func(t *testing.T) {
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
			})
		}
	})
}
