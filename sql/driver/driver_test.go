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
	"log"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

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
			if subSlice[j] == "<NULL>" {
				result[i][j] = nil
			} else {
				result[i][j] = &subSlice[j]
			}
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

	timeVal := time.Date(2015, time.August, 30, 3, 34, 45, 345670000, time.UTC)
	intervalVal, err := time.ParseDuration("34h2s")
	if err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(`CREATE DATABASE t`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE t.alltypes (a BIGINT PRIMARY KEY, b FLOAT, c TEXT, d BOOLEAN, e TIMESTAMP, f DATE, g INTERVAL)`); err != nil {
		t.Fatal(err)
	}
	// Insert values for all the different types.
	if _, err := db.Exec(`INSERT INTO t.alltypes (a, b, c, d, e, f, g) VALUES ($1, $2, $3, $4, $5, $5::DATE, $6::INTERVAL)`, 123, 3.4, "blah", true, timeVal, intervalVal); err != nil {
		t.Fatal(err)
	}
	// Insert a row with NULL values
	if _, err := db.Exec(`INSERT INTO t.alltypes (a, b, c, d, e, f, g) VALUES ($1, $2, $3, $4, $5, $6, $7)`, 456, nil, nil, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SELECT a, b FROM t.alltypes WHERE a IN ($1)", 123); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SELECT a, b FROM t.alltypes WHERE b IN ($1)", 3.4); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SELECT a, b FROM t.alltypes WHERE c IN ($1)", "blah"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SELECT a, b FROM t.alltypes WHERE d IN ($1)", true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SELECT a, b FROM t.alltypes WHERE e IN ($1)", timeVal); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SELECT a, b FROM t.alltypes WHERE f IN ($1::DATE)", timeVal); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SELECT a, b FROM t.alltypes WHERE g IN ($1::INTERVAL)", intervalVal); err != nil {
		t.Fatal(err)
	}
	if rows, err := db.Query("SELECT * FROM t.alltypes"); err != nil {
		t.Fatal(err)
	} else {
		results := readAll(t, rows)
		expectedResults := asResultSlice([][]string{
			{"a", "b", "c", "d", "e", "f", "g"},
			{"123", "3.4", "blah", "true", "2015-08-30 03:34:45.34567+00:00", "2015-08-30", "34h0m2s"},
			{"456", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
		})
		if err := verifyResults(expectedResults, results); err != nil {
			t.Fatal(err)
		}
	}
	// Delete a row using a placeholder param.
	if _, err := db.Exec(`DELETE FROM t.alltypes WHERE a IN ($1)`, 123); err != nil {
		t.Fatal(err)
	}
	if rows, err := db.Query("SELECT * FROM t.alltypes"); err != nil {
		t.Fatal(err)
	} else {
		results := readAll(t, rows)
		expectedResults := asResultSlice([][]string{
			{"a", "b", "c", "d", "e", "f", "g"},
			{"456", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
		})
		if err := verifyResults(expectedResults, results); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConnectionSettings(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(nil)
	url := "https://root@" + s.ServingAddr() + "?certs=test_certs"
	// Create a new Database called t.
	db, err := sql.Open("cockroach", url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE DATABASE t`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Open a new db client against the server with
	// a fresh set of connections with the new settings.
	// We create a database 't' above because you cannot
	// use sql: "SET DATABASE = t" without the database
	// existing.
	url += "&database=t"
	if db, err = sql.Open("cockroach", url); err != nil {
		t.Fatal(err)
	}
	defer cleanup(s, db)
	if _, err := db.Exec(`CREATE TABLE kv (k CHAR PRIMARY KEY, v CHAR)`); err != nil {
		t.Fatal(err)
	}
	numTxs := 5
	txs := make([]*sql.Tx, 0, numTxs)
	for i := 0; i < numTxs; i++ {
		// Each transaction gets its own connection.
		if tx, err := db.Begin(); err != nil {
			t.Fatal(err)
		} else {
			txs = append(txs, tx)
		}
	}
	for _, tx := range txs {
		// Settings work!
		if _, err := tx.Exec(`SELECT * from kv`); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
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
	if _, err := db.Exec(`SELECT 1`); err != nil {
		t.Fatal(err)
	}
}
