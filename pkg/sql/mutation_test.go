// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Regression tests for #22304.
// Checks that a mutation with RETURNING checks low-level constraints
// before returning anything -- or that at least no constraint-violating
// values are visible to the client.
func TestConstraintValidationBeforeBuffering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
CREATE DATABASE d;
CREATE TABLE d.a(a INT PRIMARY KEY);
INSERT INTO d.a(a) VALUES (1);
	`); err != nil {
		t.Fatal(err)
	}

	step1 := func() (*gosql.Rows, error) {
		return db.Query("INSERT INTO d.a(a) TABLE generate_series(1,3000) RETURNING a")
	}
	step2 := func() (*gosql.Rows, error) {
		if _, err := db.Exec(`INSERT INTO d.a(a) TABLE generate_series(2, 3000)`); err != nil {
			return nil, err
		}
		return db.Query("UPDATE d.a SET a = a - 1 WHERE a > 1 RETURNING a")
	}
	for i, step := range []func() (*gosql.Rows, error){step1, step2} {
		rows, err := step()
		if err != nil {
			if !testutils.IsError(err, `duplicate key value`) {
				t.Errorf("%d: %v", i, err)
			}
		} else {
			defer rows.Close()

			hasNext := rows.Next()
			if !hasNext {
				t.Errorf("%d: returning claims to return no error, yet returns no rows either", i)
			} else {
				var val int
				err := rows.Scan(&val)

				if err != nil {
					if !testutils.IsError(err, `duplicate key value`) {
						t.Errorf("%d: %v", i, err)
					}
				} else {
					// No error. Maybe it'll come later.
					if val == 1 {
						t.Errorf("%d: returning returns rows, including an invalid duplicate", i)
					}

					for rows.Next() {
						err := rows.Scan(&val)
						if err != nil {
							if !testutils.IsError(err, `duplicate key value`) {
								t.Errorf("%d: %v", i, err)
							}
						}
						if val == 1 {
							t.Errorf("%d returning returns rows, including an invalid duplicate", i)
						}
					}
				}
			}
		}
	}
}
