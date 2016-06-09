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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	csql "github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/storagebase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestAsOfTime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, _ := createTestServerContext()
	ctx.TestingKnobs.SQLSchemaChangeManager = &csql.SchemaChangeManagerTestingKnobs{
		AsyncSchemaChangerExecNotification: schemaChangeManagerDisabled,
	}
	server, db, _ := setupWithContext(t, &ctx)
	defer cleanup(server, db)

	const val1 = 1
	const val2 = 2

	var i int
	var tm time.Time

	// Expect an error if table doesn't exist at specified time. This ensures
	// that the code that fetches schemas at the time returns an error instead
	// of panics.
	if err := db.QueryRow("SELECT now()").Scan(&tm); err != nil {
		t.Fatal(err)
	}
	tsEmpty := tm.Format(time.RFC3339Nano)
	if _, err := db.Query(fmt.Sprintf("SELECT a FROM d.t AS OF SYSTEM TIME '%s'", tsEmpty)); !testutils.IsError(err, `pq: database "d" does not exist`) {
		t.Fatal("unexpected error:", err)
	}

	if _, err := db.Exec("CREATE DATABASE d"); err != nil {
		t.Fatal("unexpected error:", err)
	}
	if err := db.QueryRow("SELECT now()").Scan(&tm); err != nil {
		t.Fatal(err)
	}
	tsDBExists := tm.Format(time.RFC3339Nano)
	if _, err := db.Query(fmt.Sprintf("SELECT a FROM d.t AS OF SYSTEM TIME '%s'", tsDBExists)); !testutils.IsError(err, `pq: table "d.t" does not exist`) {
		t.Fatal("unexpected error:", err)
	}

	if _, err := db.Exec("CREATE TABLE d.t (a INT, b INT)"); err != nil {
		t.Fatal("unexpected error:", err)
	}
	if err := db.QueryRow("SELECT now()").Scan(&tm); err != nil {
		t.Fatal(err)
	}
	tsTableExists := tm.Format(time.RFC3339Nano)
	if err := db.QueryRow(fmt.Sprintf("SELECT a FROM d.t AS OF SYSTEM TIME '%s'", tsTableExists)).Scan(&i); !testutils.IsError(err, "sql: no rows in result set") {
		t.Fatal("unexpected error:", err)
	}

	if _, err := db.Exec("INSERT INTO d.t (a) VALUES ($1)", val1); err != nil {
		t.Fatal("unexpected error:", err)
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
	if err := db.QueryRow("SELECT a FROM d.t").Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val2 {
		t.Fatalf("expected %v, got %v", val2, i)
	}
	if err := db.QueryRow(fmt.Sprintf("SELECT a FROM d.t AS OF SYSTEM TIME '%s'", tsVal1)).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %v, got %v", val1, i)
	}
	if err := db.QueryRow("SELECT a FROM d.t").Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val2 {
		t.Fatalf("expected %v, got %v", val2, i)
	}

	// Verify that non-strings fail.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1"); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, `pq: syntax error at or near "1"`) {
		t.Fatal("unexpected error:", err)
	}

	// Future queries shouldn't work.
	if err := db.QueryRow("SELECT a FROM d.t AS OF SYSTEM TIME '2200-01-01'").Scan(&i); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "pq: cannot specify timestamp in the future") {
		t.Fatal("unexpected error:", err)
	}

	// Subqueries shouldn't work.
	if _, err := db.Query(fmt.Sprintf("SELECT (SELECT a FROM d.t AS OF SYSTEM TIME '%s')", tsVal1)); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "pq: unexpected AS OF SYSTEM TIME") {
		t.Fatal("unexpected error:", err)
	}

	// Verify that we can read columns in the past that are dropped in the future.
	if _, err := db.Exec("ALTER TABLE d.t DROP COLUMN a"); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(fmt.Sprintf("SELECT a FROM d.t AS OF SYSTEM TIME '%s'", tsVal1)).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %v, got %v", val1, i)
	}
	if err := db.QueryRow("SELECT a FROM d.t").Scan(&i); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, `pq: qualified name "a" not found`) {
		t.Fatal("unexpected error:", err)
	}

	// Can't use in a transaction.
	if _, err := db.Query(fmt.Sprintf("BEGIN; SELECT a FROM d.t AS OF SYSTEM TIME '%s'; COMMIT;", tsVal1)); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "pq: unexpected AS OF SYSTEM TIME") {
		t.Fatal("unexpected error:", err)
	}
}

// Test that a TransactionRetryError will retry the read until it succeeds. The
// test is designed so that if the proto timestamps are bumped during retry
// a failure will occur.
func TestAsOfRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cmdFilters := createTestServerContext()
	// Disable one phase commits because they cannot be restarted.
	ctx.TestingKnobs.Store.(*storage.StoreTestingKnobs).DisableOnePhaseCommits = true
	server, sqlDB, _ := setupWithContext(t, &ctx)
	defer cleanup(server, sqlDB)

	const val1 = 1
	const val2 = 2
	const name = "boulanger"

	if _, err := sqlDB.Exec(fmt.Sprintf(`
			CREATE DATABASE d;
			CREATE TABLE d.t (s STRING PRIMARY KEY, a INT);
			INSERT INTO d.t (s, a) VALUES ('%v', %v);
		`, name, val1)); err != nil {
		t.Fatal(err)
	}

	var logical int64
	if err := sqlDB.QueryRow("UPDATE d.t SET a = $1 RETURNING cluster_logical_timestamp()::int", val2).Scan(&logical); err != nil {
		t.Fatal(err)
	}
	tsVal2 := time.Unix(0, logical).Format(time.RFC3339Nano)
	tsVal1 := time.Unix(0, logical-1).Format(time.RFC3339Nano)

	// Set up error injection that causes retries.
	magicVals := createFilterVals(nil, nil)
	magicVals.restartCounts = map[string]int{
		name: 5,
	}
	injectErrs := func(
		req roachpb.Request,
		hdr roachpb.Header,
		magicVals *filterVals,
	) error {
		magicVals.Lock()
		defer magicVals.Unlock()

		switch req := req.(type) {
		case *roachpb.ScanRequest:
			for key, count := range magicVals.restartCounts {
				checkCorrectTxn(string(req.Key), magicVals, hdr.Txn)
				if count > 0 && bytes.Contains(req.Key, []byte(key)) {
					magicVals.restartCounts[key]--
					err := roachpb.NewTransactionRetryError()
					magicVals.failedValues[string(req.Key)] =
						failureRecord{err, hdr.Txn}
					return err
				}
			}
		}
		return nil
	}
	cleanupFilter := cmdFilters.AppendFilter(
		func(args storagebase.FilterArgs) *roachpb.Error {
			if err := injectErrs(args.Req, args.Hdr, magicVals); err != nil {
				return roachpb.NewErrorWithTxn(err, args.Hdr.Txn)
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
