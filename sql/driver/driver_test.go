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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func setup(t *testing.T, loc *time.Location) (*server.TestServer, *sql.DB) {
	s := server.StartTestServer(nil)
	db, err := sql.Open("cockroach",
		fmt.Sprintf(
			"https://%s@%s?certs=%s&time_zone=%s",
			security.RootUser,
			s.ServingAddr(),
			security.EmbeddedCertsDir,
			loc,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	return s, db
}

func cleanup(s *server.TestServer, db *sql.DB) {
	_ = db.Close()
	s.Stop()
}

func TestDates(t *testing.T) {
	defer leaktest.AfterTest(t)

	// From https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
	locationNames := []string{
		"Canada/Newfoundland", // half-hour zone
		"Europe/London",       // same as UTC, except for DST
		"Pacific/Kiritimati",  // maximum positive offset
		"Pacific/Midway",      // maximum negative offset
		"US/Pacific",          // negative offset
		"UTC",
	}

	for _, locationName := range locationNames {
		loc, err := time.LoadLocation(locationName)
		if err != nil {
			t.Error(err)
			continue
		}

		s, db := setup(t, loc)
		defer cleanup(s, db)

		var date *driver.Date
		for _, year := range []int{
			1200, // distant past
			2020, // present day, for DST rules
			4000, // distant future
		} {
			for _, month := range []time.Month{
				time.December, // winter
				time.August,   // summer
			} {
				for hour := 0; hour < 24; hour++ {
					timestamp := time.Date(year, month, 20, hour, 34, 45, 123, loc)

					if err := db.QueryRow("SELECT $1::DATE", timestamp).Scan(&date); err != nil {
						t.Fatal(err)
					}

					if expected := driver.MakeDate(timestamp); *date != expected {
						t.Fatalf("expected date to be truncated to:\n%s\nbut got:\n%s", expected, date)
					}
				}
			}
		}
	}
}

func TestPlaceholders(t *testing.T) {
	defer leaktest.AfterTest(t)

	// loc is selected so that timeVal below maps to a different date in
	// loc and UTC.
	loc, err := time.LoadLocation("Pacific/Midway")
	if err != nil {
		t.Fatal(err)
	}

	s, db := setup(t, loc)
	defer cleanup(s, db)

	year, month, day := 3015, time.August, 30
	timeVal := time.Date(year, month, day, 3, 34, 45, 345670000, loc)
	dateVal := driver.MakeDate(time.Date(year, month, day, 0, 0, 0, 0, time.UTC))
	intervalVal, err := time.ParseDuration("34h2s")
	if err != nil {
		t.Fatal(err)
	}

	if result, err := db.Exec(`CREATE DATABASE t`); err != nil {
		t.Fatal(err)
	} else if _, err := result.LastInsertId(); !testutils.IsError(err, "no LastInsertId available after DDL statement") {
		t.Error(err)
	} else if _, err := result.RowsAffected(); !testutils.IsError(err, "no RowsAffected available after DDL statement") {
		t.Error(err)
	}

	schema := `
CREATE TABLE t.alltypes (
  a BIGINT PRIMARY KEY,
  b FLOAT,
  c TEXT,
  d BYTES,
  e BOOLEAN,
  f TIMESTAMP,
  g DATE,
  h INTERVAL
)
`
	if result, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	} else if _, err := result.LastInsertId(); !testutils.IsError(err, "no LastInsertId available after DDL statement") {
		t.Error(err)
	} else if _, err := result.RowsAffected(); !testutils.IsError(err, "no RowsAffected available after DDL statement") {
		t.Error(err)
	}

	var (
		a int64
		b sql.NullFloat64
		c sql.NullString
		d sql.NullString
		e sql.NullBool
		f *time.Time
		g *driver.Date
		h *time.Duration
	)

	if rows, err := db.Query("SELECT * FROM t.alltypes"); err != nil {
		t.Fatal(err)
	} else {
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}

		if expected := []string{"a", "b", "c", "d", "e", "f", "g", "h"}; !reflect.DeepEqual(cols, expected) {
			t.Errorf("got unexpected columns:\n%s\nexpected:\n%s", cols, expected)
		}
	}

	// Insert values for all the different types.
	if result, err := db.Exec(`INSERT INTO t.alltypes VALUES ($1, $2, $3, $4, $5, $6, $6::DATE, $7::INTERVAL)`,
		123, 3.4, "blah", []byte("foo"), true, timeVal, intervalVal); err != nil {
		t.Fatal(err)
	} else if got, err := result.RowsAffected(); err != nil {
		t.Fatal(err)
	} else if e := int64(1); got != e {
		t.Fatalf("expected %d rows affected, got %d", e, a)
	}
	// Insert a row with NULL values
	if result, err := db.Exec(`INSERT INTO t.alltypes VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		456, nil, nil, nil, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	} else if got, err := result.RowsAffected(); err != nil {
		t.Fatal(err)
	} else if e := int64(1); got != e {
		t.Fatalf("expected %d rows affected, got %d", e, a)
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
	if _, err := db.Query("SELECT a, b FROM t.alltypes WHERE d IN ($1)", []byte("foo")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SELECT a, b FROM t.alltypes WHERE e IN ($1)", true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SELECT a, b FROM t.alltypes WHERE f IN ($1)", timeVal); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SELECT a, b FROM t.alltypes WHERE g IN ($1::DATE)", timeVal); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SELECT a, b FROM t.alltypes WHERE h IN ($1::INTERVAL)", intervalVal); err != nil {
		t.Fatal(err)
	}
	if rows, err := db.Query("SELECT * FROM t.alltypes"); err != nil {
		t.Fatal(err)
	} else {
		defer rows.Close()

		rows.Next()
		if err := rows.Scan(&a, &b, &c, &d, &e, &f, &g, &h); err != nil {
			t.Fatal(err)
		}

		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}

		if !(a == 123 && b.Float64 == 3.4 && c.String == "blah" && d.String == "foo" &&
			e.Bool && f.Equal(timeVal) && *g == dateVal && *h == intervalVal) {
			t.Errorf(
				"expected:\n%+v\ngot:\n%+v",
				[]interface{}{123, 3.4, "blah", "foo", true, timeVal, dateVal, intervalVal},
				[]interface{}{a, b, c, d, e, f, g, h},
			)
		}

		rows.Next()
		if err := rows.Scan(&a, &b, &c, &d, &e, &f, &g, &h); err != nil {
			t.Fatal(err)
		}

		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}

		if !(a == 456 && !b.Valid && !c.Valid && !d.Valid && !e.Valid && f == nil && g == nil && h == nil) {
			t.Errorf(
				"expected:\n%+v\ngot:\n%+v",
				[]interface{}{123, "<NOT NULL>", "<NOT NULL>", "<NOT NULL>", "<NOT NULL>", "<NULL>", "<NULL>", "<NULL>"},
				[]interface{}{a, b, c, d, e, f, g, h},
			)
		}

		if rows.Next() {
			t.Error("expected rows to be complete")
		}
	}
	// Delete a row using a placeholder param.
	if result, err := db.Exec(`DELETE FROM t.alltypes WHERE a IN ($1)`, 123); err != nil {
		t.Fatal(err)
	} else if got, err := result.RowsAffected(); err != nil {
		t.Fatal(err)
	} else if e := int64(1); got != e {
		t.Fatalf("expected %d rows affected, got %d", e, a)
	}
	if rows, err := db.Query("SELECT * FROM t.alltypes"); err != nil {
		t.Fatal(err)
	} else {
		defer rows.Close()

		rows.Next()
		if err := rows.Scan(&a, &b, &c, &d, &e, &f, &g, &h); err != nil {
			t.Fatal(err)
		}

		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}

		if !(a == 456 && !b.Valid && !c.Valid && !d.Valid && !e.Valid && f == nil && g == nil && h == nil) {
			t.Errorf(
				"expected:\n%+v\ngot:\n%+v",
				[]interface{}{123, "<NOT NULL>", "<NOT NULL>", "<NOT NULL>", "<NOT NULL>", "<NULL>", "<NULL>", "<NULL>"},
				[]interface{}{a, b, c, d, e, f, g, h},
			)
		}

		if rows.Next() {
			t.Error("expected rows to be complete")
		}
	}
}

func TestConnectionSettings(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(nil)
	url := fmt.Sprintf(
		"https://%s@%s?certs=%s&time_zone=%s",
		security.RootUser,
		s.ServingAddr(),
		security.EmbeddedCertsDir,
		time.Local,
	)

	db, err := sql.Open("cockroach", url)
	if err != nil {
		t.Fatal(err)
	}

	if rows, err := db.Query(`SHOW TIME ZONE`); err != nil {
		t.Fatal(err)
	} else {
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}

		if expected := []string{"TIME ZONE"}; !reflect.DeepEqual(cols, expected) {
			t.Errorf("got unexpected columns:\n%s\nexpected:\n%s", cols, expected)
		}

		var timeZone sql.NullString
		rows.Next()
		if err := rows.Scan(&timeZone); err != nil {
			t.Fatal(err)
		}
		if !(timeZone.Valid && timeZone.String == time.Local.String()) {
			t.Errorf("got unexpected time zone:\n%s\nexpected:\n%s", timeZone.String, time.Local)
		}

		if rows.Next() {
			t.Error("expected rows to be complete")
		}
	}

	// Create a new Database called t.
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
		if _, err := tx.Query(`SELECT * from kv`); err != nil {
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

// concurrentIncrements starts two Goroutines in parallel, both of which
// read the integer stored at the other's key, increment and update their own.
// It checks that the outcome is serializable, i.e. exactly one of the
// two Goroutines (the later write) sees the previous write by the other.
func concurrentIncrements(db *sql.DB, t *testing.T) {
	// Start with a clean slate.
	if _, err := db.Exec(`TRUNCATE TABLE t.kv`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t.kv (k, v) VALUES (0,0),(1,0)`); err != nil {
		t.Fatal(err)
	}
	var wgStartWrite, wgEnd sync.WaitGroup
	wgEnd.Add(2)
	wgStartWrite.Add(2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wgEnd.Done()
			doneWaiting := false
			// Loop until success.
			for {
				txn, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				// Although the SELECT and the UPDATE below can be combined into a
				// single statement, we prefer them being separate to ensure that
				// this transaction is truly a multi-statement transaction, providing
				// plenty of opportunity to mess up serializability.
				var value int64
				if err := txn.QueryRow(`SELECT v FROM t.kv WHERE k = $1`, (i+1)%2).Scan(&value); err != nil {
					if err := txn.Rollback(); err != nil {
						t.Fatal(err)
					}
					continue
				}
				value++
				if !doneWaiting {
					wgStartWrite.Done()
					wgStartWrite.Wait()
					doneWaiting = true
				}
				if _, err := txn.Exec(`UPDATE t.kv SET v = $2 WHERE k = $1`, i, value); err != nil {
					if err := txn.Rollback(); err != nil {
						t.Fatal(err)
					}
					continue
				}
				if err := txn.Commit(); err != nil {
					continue
				}
				// Success.
				break
			}
		}(i)
	}
	// Wait for the goroutines to finish.
	wgEnd.Wait()
	var min, max, sum int64
	if err := db.QueryRow(`SELECT MIN(v), MAX(v), SUM(v) FROM t.kv`).Scan(&min, &max, &sum); err != nil {
		t.Fatal(err)
	}
	if min != 1 {
		t.Errorf("unexpected min: %d", min)
	}
	if max != 2 {
		t.Errorf("unexpected max: %d", max)
	}
	if sum != 3 {
		t.Errorf("unexpected sum: %d", sum)
	}

}

// TestConcurrentIncrements is a simple explicit test for serializability
// for the concrete situation described in:
// https://groups.google.com/forum/#!topic/cockroach-db/LdrC5_T0VNw
// This test is a copy of the test in client/... which runs the same
// test for the KV layer. This Belt and Suspenders test is mostly
// documentation and adds another layer of confidence that transactions
// are serializable and performant even at the SQL layer.
func TestConcurrentIncrements(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup(t, time.Local)
	defer cleanup(s, db)

	if _, err := db.Exec(`CREATE DATABASE t`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE t.kv (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}
	// Convenience loop: Crank up this number for testing this
	// more often. It'll increase test duration though.
	for k := 0; k < 5; k++ {
		concurrentIncrements(db, t)
	}
}
