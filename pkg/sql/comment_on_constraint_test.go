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
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t ( a int UNIQUE, b numeric CONSTRAINT positive_price CHECK (b > 0), c int CHECK (b > c), CONSTRAINT pkey PRIMARY KEY (a,c));
		CREATE TABLE t2 (a UUID PRIMARY KEY, b int NOT NULL REFERENCES  t (a))
`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		exec   string
		query  string
		expect gosql.NullString
	}{
		//SEPT 1 EOD:MAKING FOREIGN KEY CONSTRAINT AND UNIQUE CONSTRAINT TEST!
		{
			`COMMENT ON CONSTRAINT t_a_key ON t IS 'unique_comment'`,
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
			`COMMENT ON CONSTRAINT "primary" ON t2 IS 'primary_comment'`,
			`SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname='primary'`,
			gosql.NullString{String: `primary_comment`, Valid: true},
		},
		{
			`COMMENT ON CONSTRAINT fk_b_ref_t ON t2 IS 'fk_comment'`,
			`SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname='fk_b_ref_t'`,
			gosql.NullString{String: `fk_comment`, Valid: true},
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
