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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestUnsplitAt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	// TODO(jeffreyxiao): Disable the merge queue due to a race condition. The
	// merge queue might issue an AdminMerge and before the actual merge happens,
	// the LHS of the merge is manually split and is later merged even though a
	// sticky bit has been set on the new RHS. This race condition happens
	// because there is two independent fetches of the RHS during a merge
	// operation (one in the merge queue and another in the actual merge). The
	// merge queue should pass the expected descriptor of the RHS into the
	// AdminMerge request.
	params.Knobs = base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			DisableMergeQueue: true,
		},
	}
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
	r.Exec(t, `CREATE TABLE i (k INT PRIMARY KEY)`)

	tests := []struct {
		splitStmt   string
		unsplitStmt string
		// Number of unsplits expected.
		count int
		error string
		args  []interface{}
	}{
		{
			splitStmt:   "ALTER TABLE d.t SPLIT AT VALUES (2, 'b')",
			unsplitStmt: "ALTER TABLE d.t UNSPLIT AT VALUES (2, 'b')",
			count:       1,
		},
		{
			splitStmt:   "ALTER TABLE d.t SPLIT AT VALUES (3, 'c'), (4, 'd')",
			unsplitStmt: "ALTER TABLE d.t UNSPLIT AT VALUES (3, 'c'), (4, 'd')",
			count:       2,
		},
		{
			splitStmt:   "ALTER TABLE d.t SPLIT AT VALUES (5, 'd')",
			unsplitStmt: "ALTER TABLE d.t UNSPLIT AT SELECT 5, 'd'",
			count:       1,
		},
		{
			splitStmt:   "ALTER TABLE d.t SPLIT AT VALUES (6, 'e'), (7, 'f')",
			unsplitStmt: "ALTER TABLE d.t UNSPLIT AT SELECT * FROM (VALUES (6, 'e'), (7, 'f')) AS a",
			count:       2,
		},
		{
			splitStmt:   "ALTER TABLE d.t SPLIT AT VALUES (10)",
			unsplitStmt: "ALTER TABLE d.t UNSPLIT AT VALUES (10)",
			count:       1,
		},
		{
			splitStmt:   "ALTER TABLE d.i SPLIT AT VALUES (1)",
			unsplitStmt: "ALTER TABLE d.i UNSPLIT AT VALUES ((SELECT 1))",
			count:       1,
		},
		{
			splitStmt:   "ALTER TABLE d.i SPLIT AT VALUES (8)",
			unsplitStmt: "ALTER TABLE d.i UNSPLIT AT VALUES ($1)",
			args:        []interface{}{8},
			count:       1,
		},
		{
			splitStmt:   "ALTER INDEX d.t@s_idx SPLIT AT VALUES ('f')",
			unsplitStmt: "ALTER INDEX d.t@s_idx UNSPLIT AT VALUES ('f')",
			count:       1,
		},
		{
			splitStmt:   "ALTER TABLE d.t SPLIT AT VALUES (8, 'g'), (9, 'h'), (10, 'i')",
			unsplitStmt: "ALTER TABLE d.t UNSPLIT ALL",
			count:       3,
		},
		{
			splitStmt:   "ALTER INDEX d.t@s_idx SPLIT AT VALUES ('g'), ('h'), ('i')",
			unsplitStmt: "ALTER INDEX d.t@s_idx UNSPLIT ALL",
			count:       3,
		},
		{
			splitStmt:   "ALTER TABLE d.i SPLIT AT VALUES (10), (11), (12)",
			unsplitStmt: "ALTER TABLE d.i UNSPLIT ALL",
			count:       3,
		},
		{
			splitStmt:   "ALTER TABLE i SPLIT AT VALUES (10), (11), (12)",
			unsplitStmt: "ALTER TABLE i UNSPLIT ALL",
			count:       3,
		},
		{
			unsplitStmt: "ALTER TABLE d.t UNSPLIT AT VALUES (1, 'non-existent')",
			error:       "could not UNSPLIT AT (1, 'non-existent')",
		},
		{
			unsplitStmt: "ALTER TABLE d.t UNSPLIT AT VALUES ('c', 3)",
			error:       "could not parse \"c\" as type int",
		},
		{
			unsplitStmt: "ALTER TABLE d.t UNSPLIT AT VALUES (i, s)",
			error:       `column "i" does not exist`,
		},
		{
			unsplitStmt: "ALTER INDEX d.t@not_present UNSPLIT AT VALUES ('g')",
			error:       `index "not_present" does not exist`,
		},
		{
			unsplitStmt: "ALTER TABLE d.i UNSPLIT AT VALUES (avg(1::float))",
			error:       "aggregate functions are not allowed in VALUES",
		},
		{
			unsplitStmt: "ALTER TABLE d.i UNSPLIT AT VALUES ($1)",
			error:       "no value provided for placeholder: $1",
		},
		{
			unsplitStmt: "ALTER TABLE d.i UNSPLIT AT VALUES ($1)",
			args:        []interface{}{"blah"},
			error:       "error in argument for $1: strconv.ParseInt",
		},
		{
			unsplitStmt: "ALTER TABLE d.i UNSPLIT AT VALUES ($1::string)",
			args:        []interface{}{"1"},
			error:       "UNSPLIT AT data column 1 (k) must be of type int, not type string",
		},
	}

	for _, tt := range tests {
		var key roachpb.Key
		var pretty string
		var expirationTimestamp gosql.NullString

		if tt.splitStmt != "" {
			rows, err := db.Query(tt.splitStmt)
			if err != nil {
				t.Fatalf("%s: unexpected error setting up test: %s", tt.splitStmt, err)
			}
			for rows.Next() {
				if err := rows.Scan(&key, &pretty, &expirationTimestamp); err != nil {
					t.Fatalf("%s: unexpected error setting up test: %s", tt.splitStmt, err)
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("%s: unexpected error setting up test: %s", tt.splitStmt, err)
			}
		}

		rows, err := db.Query(tt.unsplitStmt, tt.args...)
		if err != nil && tt.error == "" {
			t.Fatalf("%s: unexpected error: %s", tt.unsplitStmt, err)
		} else if tt.error != "" && err == nil {
			t.Fatalf("%s: expected error: %s", tt.unsplitStmt, tt.error)
		} else if err != nil && tt.error != "" {
			if !strings.Contains(err.Error(), tt.error) {
				t.Fatalf("%s: unexpected error: %s", tt.unsplitStmt, err)
			}
		} else {
			actualCount := 0
			for rows.Next() {
				actualCount++
				err := rows.Scan(&key, &pretty)
				if err != nil {
					t.Fatalf("%s: unexpected error: %s", tt.unsplitStmt, err)
				}
				// Successful unsplit, verify it happened.
				rng, err := s.(*server.TestServer).LookupRange(key)
				if err != nil {
					t.Fatal(err)
				}
				if !rng.GetStickyBit().IsEmpty() {
					t.Fatalf("%s: expected range sticky bit to be hlc.MinTimestamp, got %s", tt.unsplitStmt, rng.GetStickyBit())
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("%s: unexpected error: %s", tt.unsplitStmt, err)
			}

			if tt.count != actualCount {
				t.Fatalf("%s: expected %d unsplits, got %d", tt.unsplitStmt, tt.count, actualCount)
			}
		}
	}
}
