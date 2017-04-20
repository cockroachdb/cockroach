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

	"golang.org/x/net/context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestAsOfTime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	params.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		AsyncExecNotification: asyncSchemaChangerDisabled,
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	const val1 = 1
	const val2 = 2
	const query = "SELECT a FROM d.t AS OF SYSTEM TIME %s WHERE a > $1"

	var i, j int

	// Expect an error if table doesn't exist at specified time. This ensures
	// that the code that fetches schemas at the time returns an error instead
	// of panics.
	var tsEmpty string
	if err := db.QueryRow("SELECT cluster_logical_timestamp()").Scan(&tsEmpty); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(fmt.Sprintf(query, tsEmpty), 0); !testutils.IsError(err, `pq: database "d" does not exist`) {
		t.Fatal(err)
	}

	var tsDBExists string
	if err := db.QueryRow("CREATE DATABASE d; SELECT cluster_logical_timestamp()").Scan(&tsDBExists); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(fmt.Sprintf(query, tsDBExists), 0); !testutils.IsError(err, `pq: table "d.t" does not exist`) {
		t.Fatal(err)
	}

	if _, err := db.Exec(`
		CREATE TABLE d.t (a INT, b INT);
		CREATE TABLE d.j (c INT);
	`); err != nil {
		t.Fatal(err)
	}
	var tsTableExists string
	if err := db.QueryRow("SELECT cluster_logical_timestamp()").Scan(&tsTableExists); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(fmt.Sprintf(query, tsTableExists), 0).Scan(&i); !testutils.IsError(err, "sql: no rows in result set") {
		t.Fatal(err)
	}

	if _, err := db.Exec("INSERT INTO d.t (a) VALUES ($1)", val1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO d.j (c) VALUES ($1)", val2); err != nil {
		t.Fatal(err)
	}
	var tsVal1 string
	if err := db.QueryRow("SELECT a, cluster_logical_timestamp() FROM d.t").Scan(&i, &tsVal1); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %v, got %v", val1, i)
	}
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
	if err := db.QueryRow(fmt.Sprintf("SELECT a, c FROM d.t, d.j AS OF SYSTEM TIME %s", tsVal1)).Scan(&i, &j); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %v, got %v", val1, i)
	} else if j != val2 {
		t.Fatalf("expected %v, got %v", val2, j)
	}

	// Future queries shouldn't work.
	if err := db.QueryRow("SELECT a FROM d.t AS OF SYSTEM TIME '2200-01-01'").Scan(&i); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "pq: cannot specify timestamp in the future") {
		t.Fatal(err)
	}

	// Verify queries with positive scale work properly.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1e1"); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, `pq: database "d" does not exist`) {
		t.Fatal(err)
	}

	// Verify queries with large exponents error properly.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1e40"); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "value out of range") {
		t.Fatal(err)
	}

	// Verify logical parts parse with < 10 digits.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1.123456789"); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, `pq: database "d" does not exist`) {
		t.Fatal(err)
	}

	// Verify logical parts parse with == 10 digits.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1.1234567890"); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, `pq: database "d" does not exist`) {
		t.Fatal(err)
	}

	// Too much logical precision is an error.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1.12345678901"); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "logical part has too many digits") {
		t.Fatal(err)
	}

	// Old queries shouldn't work.
	if err := db.QueryRow("SELECT a FROM d.t AS OF SYSTEM TIME '1969-12-31'").Scan(&i); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "pq: batch timestamp -86400.000000000,0 must be after GC threshold 0.000000000,0") {
		t.Fatal(err)
	}

	// Subqueries shouldn't work.
	if _, err := db.Query(fmt.Sprintf("SELECT (SELECT a FROM d.t AS OF SYSTEM TIME %s)", tsVal1)); err == nil {
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
	if _, err := db.Query(fmt.Sprintf("BEGIN; SELECT a FROM d.t AS OF SYSTEM TIME %s; COMMIT;", tsVal1)); err == nil {
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
	defer s.Stopper().Stop(context.TODO())

	const val1 = 1
	const val2 = 2
	const name = "boulanger"

	if _, err := sqlDB.Exec(`
			CREATE DATABASE d;
			CREATE TABLE d.t (s STRING PRIMARY KEY, a INT);
		`); err != nil {
		t.Fatal(err)
	}
	var tsStart string
	if err := sqlDB.QueryRow(`
			INSERT INTO d.t (s, a) VALUES ($1, $2)
			RETURNING cluster_logical_timestamp();
		`, name, val1).Scan(&tsStart); err != nil {
		t.Fatal(err)
	}

	var tsVal2 string
	if err := sqlDB.QueryRow("UPDATE d.t SET a = $1 RETURNING cluster_logical_timestamp()", val2).Scan(&tsVal2); err != nil {
		t.Fatal(err)
	}
	walltime := new(apd.Decimal)
	if _, _, err := walltime.SetString(tsVal2); err != nil {
		t.Fatalf("couldn't set decimal: %s", tsVal2)
	}
	oneTick := apd.New(1, 0)
	// Set tsVal1 to 1ns before tsVal2.
	if _, err := parser.ExactCtx.Sub(walltime, walltime, oneTick); err != nil {
		t.Fatal(err)
	}
	tsVal1 := walltime.ToStandard()

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
					if err := checkCorrectTxn(string(req.Key), magicVals, args.Hdr.Txn); err != nil {
						return roachpb.NewError(err)
					}
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
	if err := sqlDB.QueryRow(fmt.Sprintf("SELECT a FROM d.t AS OF SYSTEM TIME %s WHERE s = '%s'", tsVal1, name)).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("unexpected val: %v", i)
	}

	cleanupFilter()
	// Verify that the retry errors were injected.
	checkRestarts(t, magicVals)

	// Query with tsVal2 to ensure val2 is indeed present.
	if err := sqlDB.QueryRow(fmt.Sprintf("SELECT a FROM d.t AS OF SYSTEM TIME %s", tsVal2)).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val2 {
		t.Fatalf("unexpected val: %v", i)
	}
}
