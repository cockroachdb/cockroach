// Copyright 2016 The Cockroach Authors.
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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestSplitAt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	r := sqlutils.MakeSQLRunner(db)

	r.Exec(t, "CREATE DATABASE d")
	r.Exec(t, `CREATE TABLE d.t (
		i INT,
		s STRING,
		PRIMARY KEY (i, s),
		INDEX s_idx (s)
	)`)
	r.Exec(t, `CREATE TABLE d.i (k INT PRIMARY KEY)`)

	tests := []struct {
		in    string
		error string
		args  []interface{}
	}{
		{
			in: "ALTER TABLE d.t SPLIT AT VALUES (2, 'b')",
		},
		{
			// Splitting at an existing split is a silent no-op.
			in: "ALTER TABLE d.t SPLIT AT VALUES (2, 'b')",
		},
		{
			in: "ALTER TABLE d.t SPLIT AT VALUES (3, 'c'), (4, 'd')",
		},
		{
			in: "ALTER TABLE d.t SPLIT AT SELECT 5, 'd'",
		},
		{
			in: "ALTER TABLE d.t SPLIT AT SELECT * FROM (VALUES (6, 'e'), (7, 'f')) AS a",
		},
		{
			in: "ALTER TABLE d.t SPLIT AT VALUES (10)",
		},
		{
			in:    "ALTER TABLE d.t SPLIT AT VALUES ('c', 3)",
			error: "could not parse \"c\" as type int",
		},
		{
			in:    "ALTER TABLE d.t SPLIT AT VALUES (i, s)",
			error: `column "i" does not exist`,
		},
		{
			in: "ALTER INDEX d.t@s_idx SPLIT AT VALUES ('f')",
		},
		{
			in:    "ALTER INDEX d.t@not_present SPLIT AT VALUES ('g')",
			error: `index "not_present" does not exist`,
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES (avg(1::float))",
			error: "aggregate functions are not allowed in VALUES",
		},
		{
			in:   "ALTER TABLE d.i SPLIT AT VALUES ($1)",
			args: []interface{}{8},
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES ($1)",
			error: "no value provided for placeholder: $1",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES ($1)",
			args:  []interface{}{"blah"},
			error: "error in argument for $1: strconv.ParseInt",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES ($1::string)",
			args:  []interface{}{"1"},
			error: "SPLIT AT data column 1 (k) must be of type int, not type string",
		},
		{
			in: "ALTER TABLE d.i SPLIT AT VALUES ((SELECT 1))",
		},
		{
			in: "ALTER TABLE d.i SPLIT AT VALUES (10) WITH EXPIRATION '1 day'",
		},
		{
			in: "ALTER TABLE d.i SPLIT AT VALUES (11) WITH EXPIRATION '1 day'::interval",
		},
		{
			in: "ALTER TABLE d.i SPLIT AT VALUES (12) WITH EXPIRATION '7258118400000000000.0'",
		},
		{
			in: "ALTER TABLE d.i SPLIT AT VALUES (13) WITH EXPIRATION '2200-01-01 00:00:00.0'",
		},
		{
			in: "ALTER TABLE d.i SPLIT AT VALUES (14) WITH EXPIRATION TIMESTAMP '2200-01-01 00:00:00.0'",
		},
		{
			in: "ALTER TABLE d.i SPLIT AT VALUES (15) WITH EXPIRATION '2200-01-01 00:00:00.0':::timestamp",
		},
		{
			in: "ALTER TABLE d.i SPLIT AT VALUES (16) WITH EXPIRATION TIMESTAMPTZ '2200-01-01 00:00:00.0'",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES (17) WITH EXPIRATION 'a'",
			error: "SPLIT AT: value is neither timestamp, decimal, nor interval",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES (17) WITH EXPIRATION true",
			error: "SPLIT AT: expected timestamp, decimal, or interval, got bool",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES (17) WITH EXPIRATION '1969-01-01 00:00:00.0'",
			error: "SPLIT AT: timestamp before 1970-01-01T00:00:00Z is invalid",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES (17) WITH EXPIRATION '1970-01-01 00:00:00.0'",
			error: "SPLIT AT: zero timestamp is invalid",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES (17) WITH EXPIRATION '-1 day'::interval",
			error: "SPLIT AT: expiration time should be greater than or equal to current time",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES (17) WITH EXPIRATION '0.1us'",
			error: "SPLIT AT: interval value '0.1us' too small, absolute value must be >= 1µs",
		},
	}

	for _, tt := range tests {
		var key roachpb.Key
		var pretty string
		var expirationTimestamp gosql.NullString
		err := db.QueryRow(tt.in, tt.args...).Scan(&key, &pretty, &expirationTimestamp)
		if err != nil && tt.error == "" {
			t.Fatalf("%s: unexpected error: %s", tt.in, err)
		} else if tt.error != "" && err == nil {
			t.Fatalf("%s: expected error: %s", tt.in, tt.error)
		} else if err != nil && tt.error != "" {
			if !strings.Contains(err.Error(), tt.error) {
				t.Fatalf("%s: unexpected error: %s", tt.in, err)
			}
		} else {
			// Successful split, verify it happened.
			rng, err := s.(*server.TestServer).LookupRange(key)
			if err != nil {
				t.Fatal(err)
			}
			expect := roachpb.Key(rng.StartKey)
			if !expect.Equal(key) {
				t.Fatalf("%s: expected range start %s, got %s", tt.in, expect, pretty)
			}
		}
	}
}
