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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestUnsplitAt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	r := sqlutils.MakeSQLRunner(db)

	r.Exec(t, "CREATE DATABASE d")
	r.Exec(t, `CREATE TABLE d.t (
		i INT,
		s STRING,
		PRIMARY KEY (i, s),
		INDEX s_idx (s)
	)`)
	r.Exec(t, `CREATE TABLE d.i (k INT PRIMARY KEY)`)

	// Create initial splits
	splitStmts := []string{
		"ALTER TABLE d.t SPLIT AT VALUES (2, 'b'), (3, 'c'), (4, 'd'), (5, 'd'), (6, 'e'), (7, 'f')",
		"ALTER TABLE d.t SPLIT AT VALUES (10)",
		"ALTER TABLE d.i SPLIT AT VALUES (1), (8)",
		"ALTER INDEX d.t@s_idx SPLIT AT VALUES ('f')",
	}

	for _, splitStmt := range splitStmts {
		var key roachpb.Key
		var pretty string
		var expirationTimestamp gosql.NullString
		if err := db.QueryRow(splitStmt).Scan(&key, &pretty, &expirationTimestamp); err != nil {
			t.Fatalf("unexpected error setting up test: %s", err)
		}
	}

	tests := []struct {
		in    string
		error string
		args  []interface{}
	}{
		{
			in: "ALTER TABLE d.t UNSPLIT AT VALUES (2, 'b')",
		},
		{
			in: "ALTER TABLE d.t UNSPLIT AT VALUES (3, 'c'), (4, 'd')",
		},
		{
			in: "ALTER TABLE d.t UNSPLIT AT SELECT 5, 'd'",
		},
		{
			in: "ALTER TABLE d.t UNSPLIT AT SELECT * FROM (VALUES (6, 'e'), (7, 'f')) AS a",
		},
		{
			in: "ALTER TABLE d.t UNSPLIT AT VALUES (10)",
		},
		{
			in:   "ALTER TABLE d.i UNSPLIT AT VALUES ($1)",
			args: []interface{}{8},
		},
		{
			in: "ALTER TABLE d.i UNSPLIT AT VALUES ((SELECT 1))",
		},
		{
			in: "ALTER INDEX d.t@s_idx UNSPLIT AT VALUES ('f')",
		},
		{
			in:    "ALTER TABLE d.t UNSPLIT AT VALUES (1, 'non-existent')",
			error: "could not UNSPLIT AT (1, 'non-existent')",
		},
		{
			in:    "ALTER TABLE d.t UNSPLIT AT VALUES ('c', 3)",
			error: "could not parse \"c\" as type int",
		},
		{
			in:    "ALTER TABLE d.t UNSPLIT AT VALUES (i, s)",
			error: `column "i" does not exist`,
		},
		{
			in:    "ALTER INDEX d.t@not_present UNSPLIT AT VALUES ('g')",
			error: `index "not_present" does not exist`,
		},
		{
			in:    "ALTER TABLE d.i UNSPLIT AT VALUES (avg(1::float))",
			error: "aggregate functions are not allowed in VALUES",
		},
		{
			in:    "ALTER TABLE d.i UNSPLIT AT VALUES ($1)",
			error: "no value provided for placeholder: $1",
		},
		{
			in:    "ALTER TABLE d.i UNSPLIT AT VALUES ($1)",
			args:  []interface{}{"blah"},
			error: "error in argument for $1: strconv.ParseInt",
		},
		{
			in:    "ALTER TABLE d.i UNSPLIT AT VALUES ($1::string)",
			args:  []interface{}{"1"},
			error: "UNSPLIT AT data column 1 (k) must be of type int, not type string",
		},
	}

	for _, tt := range tests {
		var key roachpb.Key
		var pretty string
		err := db.QueryRow(tt.in, tt.args...).Scan(&key, &pretty)
		if err != nil && tt.error == "" {
			t.Fatalf("%s: unexpected error: %s", tt.in, err)
		} else if tt.error != "" && err == nil {
			t.Fatalf("%s: expected error: %s", tt.in, tt.error)
		} else if err != nil && tt.error != "" {
			if !strings.Contains(err.Error(), tt.error) {
				t.Fatalf("%s: unexpected error: %s", tt.in, err)
			}
		} else {
			// Successful unsplit, verify it happened.
			rng, err := s.(*server.TestServer).LookupRange(key)
			if err != nil {
				t.Fatal(err)
			}
			if (rng.GetStickyBit() != hlc.Timestamp{}) {
				t.Fatalf("%s: expected range sticky bit to be hlc.MinTimestamp, got %s", tt.in, pretty)
			}
		}
	}
}
