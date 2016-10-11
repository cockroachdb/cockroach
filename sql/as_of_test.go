// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Matt Jibson (mjibson@gmail.com)

package sql_test

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	csql "github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/storagebase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/pkg/errors"
)

func TestAsOfTime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	params.Knobs.SQLSchemaChanger = &csql.SchemaChangerTestingKnobs{
		AsyncExecNotification: asyncSchemaChangerDisabled,
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	const val1 = 1
	const val2 = 2
	const query = "SELECT a FROM d.t AS OF SYSTEM TIME '%s' WHERE a > $1"

	var i, j int
	var tm, now time.Time

	// Expect an error if table doesn't exist at specified time. This ensures
	// that the code that fetches schemas at the time returns an error instead
	// of panics.
	if err := db.QueryRow("SELECT now()").Scan(&tm); err != nil {
		t.Fatal(err)
	}
	tsEmpty := tm.Format(time.RFC3339Nano)
	if _, err := db.Query(fmt.Sprintf(query, tsEmpty), 0); !testutils.IsError(err, `pq: database "d" does not exist`) {
		t.Fatal(err)
	}

	var logicalTS int64
	if err := db.QueryRow("CREATE DATABASE d; SELECT cluster_logical_timestamp()::int").Scan(&logicalTS); err != nil {
		t.Fatal(err)
	}
	// Make sure we see the DB create by using a TS after its txn time.
	tsDBExists := time.Unix(0, logicalTS+1).Format(time.RFC3339Nano)
	// Wait until now() is after the above cluster_logical_timestamp() so the max check doesn't get hit.
	util.SucceedsSoon(t, func() error {
		return waitForNow(t, db, tsDBExists)
	})

	if _, err := db.Query(fmt.Sprintf(query, tsDBExists), 0); !testutils.IsError(err, `pq: table "d.t" does not exist`) {
		t.Fatal(err)
	}

	if _, err := db.Exec(`
		CREATE TABLE d.t (a INT, b INT);
		CREATE TABLE d.j (c INT);
	`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT now()").Scan(&tm); err != nil {
		t.Fatal(err)
	}
	tsTableExists := tm.Format(time.RFC3339Nano)
	if err := db.QueryRow(fmt.Sprintf(query, tsTableExists), 0).Scan(&i); !testutils.IsError(err, "sql: no rows in result set") {
		t.Fatal(err)
	}

	if _, err := db.Exec("INSERT INTO d.t (a) VALUES ($1)", val1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO d.j (c) VALUES ($1)", val2); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT a, now() FROM d.t").Scan(&i, &tm); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %v, got %v", val1, i)
	}
	tsVal1 := tm.Format(time.RFC3339Nano)
	if _, err := db.Exec("UPDATE d.t SET a = $1", val2); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE d.j SET c = $1", val1); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT a FROM d.t").Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val2 {
		t.Fatalf("expected %v, got %v", val2, i)
	}
	if err := db.QueryRow(fmt.Sprintf("SELECT a, c, now() FROM d.t, d.j AS OF SYSTEM TIME '%s'", tsVal1)).Scan(&i, &j, &now); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %v, got %v", val1, i)
	} else if j != val2 {
		t.Fatalf("expected %v, got %v", val2, j)
	} else if !now.After(tm) {
		t.Fatalf("expected now > ts1")
	}

	// Verify that non-strings fail.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1"); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, `pq: syntax error at or near "1"`) {
		t.Fatal(err)
	}

	// Future queries shouldn't work.
	if err := db.QueryRow("SELECT a FROM d.t AS OF SYSTEM TIME '2200-01-01'").Scan(&i); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "pq: cannot specify timestamp in the future") {
		t.Fatal(err)
	}

	// Old queries shouldn't work.
	if err := db.QueryRow("SELECT a FROM d.t AS OF SYSTEM TIME '1969-12-31'").Scan(&i); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "pq: batch timestamp -86400.000000000,0 must be after replica GC threshold 0.000000000,0") {
		t.Fatal(err)
	}

	// Subqueries shouldn't work.
	if _, err := db.Query(fmt.Sprintf("SELECT (SELECT a FROM d.t AS OF SYSTEM TIME '%s')", tsVal1)); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "pq: unexpected AS OF SYSTEM TIME") {
		t.Fatal(err)
	}

	// Verify that we can read columns in the past that are dropped in the future.
	if _, err := db.Exec("ALTER TABLE d.t DROP COLUMN a"); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(fmt.Sprintf(query, tsVal1), 0).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %v, got %v", val1, i)
	}

	// Can't use in a transaction.
	if _, err := db.Query(fmt.Sprintf("BEGIN; SELECT a FROM d.t AS OF SYSTEM TIME '%s'; COMMIT;", tsVal1)); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "pq: unexpected AS OF SYSTEM TIME") {
		t.Fatal(err)
	}
}

// Test that a TransactionRetryError will retry the read until it succeeds. The
// test is designed so that if the proto timestamps are bumped during retry
// a failure will occur.
func TestAsOfRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, cmdFilters := createTestServerParams()
	// Disable one phase commits because they cannot be restarted.
	params.Knobs.Store.(*storage.StoreTestingKnobs).DisableOnePhaseCommits = true
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	const val1 = 1
	const val2 = 2
	const name = "boulanger"
	var walltime int64

	if _, err := sqlDB.Exec(`
			CREATE DATABASE d;
			CREATE TABLE d.t (s STRING PRIMARY KEY, a INT);
		`); err != nil {
		t.Fatal(err)
	}
	if err := sqlDB.QueryRow(`
			INSERT INTO d.t (s, a) VALUES ($1, $2)
			RETURNING cluster_logical_timestamp()::int;
		`, name, val1).Scan(&walltime); err != nil {
		t.Fatal(err)
	}
	tsStart := time.Unix(0, walltime+1).Format(time.RFC3339Nano)

	// Wait until now() is after tsStart so that we can subtract 1 from tsVal1
	// later on. This still isn't totally safe, but it prevents test flakiness.
	util.SucceedsSoon(t, func() error {
		return waitForNow(t, sqlDB, tsStart)
	})

	if err := sqlDB.QueryRow("UPDATE d.t SET a = $1 RETURNING cluster_logical_timestamp()::int", val2).Scan(&walltime); err != nil {
		t.Fatal(err)
	}
	tsVal2 := time.Unix(0, walltime).Format(time.RFC3339Nano)
	tsVal1 := time.Unix(0, walltime-1).Format(time.RFC3339Nano)

	// Set up error injection that causes retries.
	magicVals := createFilterVals(nil, nil)
	magicVals.restartCounts = map[string]int{
		name: 5,
	}
	cleanupFilter := cmdFilters.AppendFilter(
		func(args storagebase.FilterArgs) *roachpb.Error {
			magicVals.Lock()
			defer magicVals.Unlock()

			switch req := args.Req.(type) {
			case *roachpb.ScanRequest:
				for key, count := range magicVals.restartCounts {
					checkCorrectTxn(string(req.Key), magicVals, args.Hdr.Txn)
					if count > 0 && bytes.Contains(req.Key, []byte(key)) {
						magicVals.restartCounts[key]--
						err := roachpb.NewTransactionRetryError()
						magicVals.failedValues[string(req.Key)] =
							failureRecord{err, args.Hdr.Txn}
						txn := args.Hdr.Txn.Clone()
						txn.Timestamp = txn.Timestamp.Add(0, 1)
						return roachpb.NewErrorWithTxn(err, &txn)
					}
				}
			}
			return nil
		}, false)

	var i int
	// Query with tsVal1 which should return the first value. Since tsVal1 is just
	// one nanosecond before tsVal2, any proto timestamp bumping will return val2
	// and error.
	// Must specify the WHERE here to trigger the injection errors.
	if err := sqlDB.QueryRow(fmt.Sprintf("SELECT a FROM d.t AS OF SYSTEM TIME '%s' WHERE s = '%s'", tsVal1, name)).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("unexpected val: %v", i)
	}

	cleanupFilter()
	// Verify that the retry errors were injected.
	checkRestarts(t, magicVals)

	// Query with tsVal2 to ensure val2 is indeed present.
	if err := sqlDB.QueryRow(fmt.Sprintf("SELECT a FROM d.t AS OF SYSTEM TIME '%s'", tsVal2)).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val2 {
		t.Fatalf("unexpected val: %v", i)
	}
}

func waitForNow(t *testing.T, db *gosql.DB, ts string) error {
	var afterNow bool
	if err := db.QueryRow("SELECT now() >= $1", ts).Scan(&afterNow); err != nil {
		return err
	}
	if !afterNow {
		return errors.Errorf("timestamp not after now(): %s", ts)
	}
	return nil
}
