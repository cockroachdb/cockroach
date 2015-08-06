// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package driver_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

var isError = testutils.IsError

func setup(t *testing.T) (*server.TestServer, *sql.DB) {
	s := server.StartTestServer(nil)
	db, err := sql.Open("cockroach", "https://root@"+s.ServingAddr()+"?certs=test_certs")
	if err != nil {
		t.Fatal(err)
	}
	return s, db
}

func cleanup(s *server.TestServer, db *sql.DB) {
	_ = db.Close()
	s.Stop()
}

type resultSlice [][]*string

func (r resultSlice) String() string {
	results := make([][]string, len(r))

	for i, subSlice := range r {
		results[i] = make([]string, len(subSlice))
		for j, str := range subSlice {
			if str == nil {
				results[i][j] = "<NULL>"
			} else {
				results[i][j] = *str
			}
		}
	}

	return fmt.Sprintf("%s", results)
}

func asResultSlice(src [][]string) resultSlice {
	result := make(resultSlice, len(src))
	for i, subSlice := range src {
		result[i] = make([]*string, len(subSlice))
		for j := range subSlice {
			result[i][j] = &subSlice[j]
		}
	}
	return result
}

func readAll(t *testing.T, rows *sql.Rows) resultSlice {
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}

	colStrs := make([]*string, len(cols))
	for i := range cols {
		colStrs[i] = &cols[i]
	}

	results := resultSlice{colStrs}

	for rows.Next() {
		strs := make([]*string, len(cols))
		vals := make([]interface{}, len(cols))
		for i := range vals {
			vals[i] = &strs[i]
		}
		if err := rows.Scan(vals...); err != nil {
			t.Fatal(err)
		}
		results = append(results, strs)
	}

	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	return results
}

func verifyResults(expectedResults resultSlice, actualResults resultSlice) error {
	errMismatch := fmt.Errorf("expected: %s\nactual: %s\n", expectedResults, actualResults)

	if len(expectedResults) != len(actualResults) {
		return errMismatch
	}

	for i := 0; i < len(expectedResults); i++ {
		if len(expectedResults[i]) != len(actualResults[i]) {
			return errMismatch
		}

		for j := 0; j < len(expectedResults[i]); j++ {
			if expectedResults[i][j] == nil && actualResults[i][j] == nil {
				continue
			}
			if !(expectedResults[i][j] != nil && actualResults[i][j] != nil) {
				return errMismatch
			}
			if *expectedResults[i][j] != *actualResults[i][j] {
				return errMismatch
			}
		}
	}

	return nil
}

func TestPlaceholders(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup(t)
	defer cleanup(s, db)

	if _, err := db.Exec(`CREATE DATABASE t`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t.kv VALUES ($1, $2), ($3, $4)`, "a", "b", "c", nil); err != nil {
		t.Fatal(err)
	}

	if rows, err := db.Query("SELECT * FROM t.kv"); err != nil {
		t.Fatal(err)
	} else {
		results := readAll(t, rows)
		expectedResults := asResultSlice([][]string{
			{"k", "v"},
			{"a", "b"},
			{"c", ""},
		})
		expectedResults[2][1] = nil
		if err := verifyResults(expectedResults, results); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := db.Exec(`DELETE FROM t.kv WHERE k IN ($1)`, "c"); err != nil {
		t.Fatal(err)
	}

	if rows, err := db.Query("SELECT * FROM t.kv"); err != nil {
		t.Fatal(err)
	} else {
		results := readAll(t, rows)
		expectedResults := asResultSlice([][]string{
			{"k", "v"},
			{"a", "b"},
		})
		if err := verifyResults(expectedResults, results); err != nil {
			t.Fatal(err)
		}
	}
}

func TestInsecure(t *testing.T) {
	defer leaktest.AfterTest(t)
	// Start test server in insecure mode.
	s := &server.TestServer{}
	s.Ctx = server.NewTestContext()
	s.Ctx.Insecure = true
	if err := s.Start(); err != nil {
		t.Fatalf("Could not start server: %v", err)
	}
	t.Logf("Test server listening on %s: %s", s.Ctx.RequestScheme(), s.ServingAddr())
	defer s.Stop()

	// We can't attempt a connection through HTTPS since the client just retries forever.
	// DB connection using plain HTTP.
	db, err := sql.Open("cockroach", "http://root@"+s.ServingAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()
	if _, err := db.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}
}
