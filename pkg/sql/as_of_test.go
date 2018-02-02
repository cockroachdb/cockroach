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

package sql_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestAsOfTime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		AsyncExecNotification: asyncSchemaChangerDisabled,
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	const val1 = 1
	const val2 = 2
	const query = "SELECT a FROM d.public.t AS OF SYSTEM TIME %s WHERE a > $1"

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
	if _, err := db.Query(fmt.Sprintf(query, tsDBExists), 0); !testutils.IsError(err, `pq: relation "d.public.t" does not exist`) {
		t.Fatal(err)
	}

	if _, err := db.Exec(`
		CREATE TABLE d.public.t (a INT, b INT);
		CREATE TABLE d.public.j (c INT);
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

	if _, err := db.Exec("INSERT INTO d.public.t (a) VALUES ($1)", val1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO d.public.j (c) VALUES ($1)", val2); err != nil {
		t.Fatal(err)
	}
	var tsVal1 string
	if err := db.QueryRow("SELECT a, cluster_logical_timestamp() FROM d.public.t").Scan(&i, &tsVal1); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %v, got %v", val1, i)
	}
	if _, err := db.Exec("UPDATE d.public.t SET a = $1", val2); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE d.public.j SET c = $1", val1); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT a FROM d.public.t").Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val2 {
		t.Fatalf("expected %v, got %v", val2, i)
	}

	// Test a simple query, and do it with and without wrapping parens to check
	// that parens don't matter.
	testutils.RunTrueAndFalse(t, "parens", func(t *testing.T, useParens bool) {
		openParens := ""
		closeParens := ""
		if useParens {
			openParens = "(("
			closeParens = "))"
		}
		query := fmt.Sprintf("%sSELECT a, c FROM d.public.t, d.public.j AS OF SYSTEM TIME %s%s", openParens, tsVal1, closeParens)
		if err := db.QueryRow(query).Scan(&i, &j); err != nil {
			t.Fatal(err)
		} else if i != val1 {
			t.Fatalf("expected %v, got %v", val1, i)
		} else if j != val2 {
			t.Fatalf("expected %v, got %v", val2, j)
		}
	})

	// Future queries shouldn't work.
	if err := db.QueryRow("SELECT a FROM d.public.t AS OF SYSTEM TIME '2200-01-01'").Scan(&i); !testutils.IsError(err, "pq: AS OF SYSTEM TIME: cannot specify timestamp in the future") {
		t.Fatal(err)
	}

	// Verify queries with positive scale work properly.
	if _, err := db.Query("SELECT a FROM d.public.t AS OF SYSTEM TIME 1e1"); !testutils.IsError(err, `pq: database "d" does not exist`) {
		t.Fatal(err)
	}

	// Verify queries with large exponents error properly.
	if _, err := db.Query("SELECT a FROM d.public.t AS OF SYSTEM TIME 1e40"); !testutils.IsError(err, "value out of range") {
		t.Fatal(err)
	}

	// Verify logical parts parse with < 10 digits.
	if _, err := db.Query("SELECT a FROM d.public.t AS OF SYSTEM TIME 1.123456789"); !testutils.IsError(err, `pq: database "d" does not exist`) {
		t.Fatal(err)
	}

	// Verify logical parts parse with == 10 digits.
	if _, err := db.Query("SELECT a FROM d.public.t AS OF SYSTEM TIME 1.1234567890"); !testutils.IsError(err, `pq: database "d" does not exist`) {
		t.Fatal(err)
	}

	// Too much logical precision is an error.
	if _, err := db.Query("SELECT a FROM d.public.t AS OF SYSTEM TIME 1.12345678901"); !testutils.IsError(err, "logical part has too many digits") {
		t.Fatal(err)
	}

	// Ditto, as string.
	if _, err := db.Query("SELECT a FROM d.public.t AS OF SYSTEM TIME '1.12345678901'"); !testutils.IsError(err, "logical part has too many digits") {
		t.Fatal(err)
	}

	// String values that are neither timestamps nor decimals are an error.
	if _, err := db.Query("SELECT a FROM d.public.t AS OF SYSTEM TIME 'xxx'"); !testutils.IsError(err, "value is neither timestamp nor decimal") {
		t.Fatal(err)
	}

	// Zero is not a valid value.
	for _, zero := range []string{"0", "'0'", "0.0000000000", "'0.0000000000'"} {
		if _, err := db.Query("SELECT a FROM d.public.t AS OF SYSTEM TIME " + zero); !testutils.IsError(err, "zero timestamp is invalid") {
			t.Fatal(err)
		}
	}

	// Old queries shouldn't work.
	if err := db.QueryRow("SELECT a FROM d.public.t AS OF SYSTEM TIME '1969-12-31'").Scan(&i); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "pq: batch timestamp -86400.000000000,0 must be after GC threshold 0.000000000,0") {
		t.Fatal(err)
	}

	// Subqueries shouldn't work.
	_, err := db.Query(
		fmt.Sprintf("SELECT (SELECT a FROM d.public.t AS OF SYSTEM TIME %s)", tsVal1))
	if !testutils.IsError(err, "pq: AS OF SYSTEM TIME must be provided on a top-level statement") {
		t.Fatalf("expected not supported, got: %v", err)
	}

	// Subqueries do work of the timestamps are consistent.
	_, err = db.Query(
		fmt.Sprintf("SELECT (SELECT a FROM d.public.t AS OF SYSTEM TIME %s) FROM (SELECT 1) AS OF SYSTEM TIME '1980-01-01'", tsVal1))
	if !testutils.IsError(err, "pq: cannot specify AS OF SYSTEM TIME with different timestamps") {
		t.Fatalf("expected inconsistent statements, got: %v", err)
	}
	if err := db.QueryRow(
		fmt.Sprintf("SELECT (SELECT 1 FROM d.public.t AS OF SYSTEM TIME %s) FROM (SELECT 1) AS OF SYSTEM TIME %s", tsVal1, tsVal1)).Scan(&i); err != nil {
		t.Fatal(err)
	}

	// Verify that we can read columns in the past that are dropped in the future.
	if _, err := db.Exec("ALTER TABLE d.public.t DROP COLUMN a"); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(fmt.Sprintf(query, tsVal1), 0).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %v, got %v", val1, i)
	}

	// Can't use in a transaction.
	_, err = db.Query(
		fmt.Sprintf("BEGIN; SELECT a FROM d.public.t AS OF SYSTEM TIME %s; COMMIT;", tsVal1))
	if !testutils.IsError(err, "pq: AS OF SYSTEM TIME must be provided on a top-level statement") {
		t.Fatalf("expected not supported, got: %v", err)
	}
}

// Test that a TransactionRetryError will retry the read until it succeeds. The
// test is designed so that if the proto timestamps are bumped during retry
// a failure will occur.
func TestAsOfRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, cmdFilters := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	const val1 = 1
	const val2 = 2
	const name = "boulanger"

	if _, err := sqlDB.Exec(`
			CREATE DATABASE d;
			CREATE TABLE d.public.t (s STRING PRIMARY KEY, a INT);
		`); err != nil {
		t.Fatal(err)
	}
	var tsStart string
	if err := sqlDB.QueryRow(`
			INSERT INTO d.public.t (s, a) VALUES ($1, $2)
			RETURNING cluster_logical_timestamp();
		`, name, val1).Scan(&tsStart); err != nil {
		t.Fatal(err)
	}

	var tsVal2 string
	if err := sqlDB.QueryRow("UPDATE d.public.t SET a = $1 RETURNING cluster_logical_timestamp()", val2).Scan(&tsVal2); err != nil {
		t.Fatal(err)
	}
	walltime := new(apd.Decimal)
	if _, _, err := walltime.SetString(tsVal2); err != nil {
		t.Fatalf("couldn't set decimal: %s", tsVal2)
	}
	oneTick := apd.New(1, 0)
	// Set tsVal1 to 1ns before tsVal2.
	if _, err := tree.ExactCtx.Sub(walltime, walltime, oneTick); err != nil {
		t.Fatal(err)
	}
	tsVal1 := walltime.Text('f')

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
				if client.TestingIsRangeLookupRequest(req) {
					return nil
				}
				for key, count := range magicVals.restartCounts {
					if err := checkCorrectTxn(string(req.Key), magicVals, args.Hdr.Txn); err != nil {
						return roachpb.NewError(err)
					}
					if count > 0 && bytes.Contains(req.Key, []byte(key)) {
						magicVals.restartCounts[key]--
						err := roachpb.NewTransactionRetryError(roachpb.RETRY_REASON_UNKNOWN)
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
	if err := sqlDB.QueryRow(fmt.Sprintf("SELECT a FROM d.public.t AS OF SYSTEM TIME %s WHERE s = '%s'", tsVal1, name)).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("unexpected val: %v", i)
	}

	cleanupFilter()
	// Verify that the retry errors were injected.
	checkRestarts(t, magicVals)

	// Query with tsVal2 to ensure val2 is indeed present.
	if err := sqlDB.QueryRow(fmt.Sprintf("SELECT a FROM d.public.t AS OF SYSTEM TIME %s", tsVal2)).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val2 {
		t.Fatalf("unexpected val: %v", i)
	}
}

// Test that SHOW TRACE FOR SELECT ... AS OF SYSTEM TIME works.
// AS OF SYSTEM TIME is generally only accepted at the topmost level of a query,
// but SHOW TRACE FOR is a special case.
func TestShowTraceAsOfTime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	const val1 = 456
	const val2 = 789

	if _, err := db.Exec(`
		CREATE DATABASE test;
		CREATE TABLE test.public.t (x INT);
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("INSERT INTO test.public.t (x) VALUES ($1)", val1); err != nil {
		t.Fatal(err)
	}
	var tsVal1 string
	var i int
	err := db.QueryRow("SELECT x, cluster_logical_timestamp() FROM test.public.t").Scan(
		&i, &tsVal1)
	if err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %d, got %v", val1, i)
	}
	if _, err := db.Exec("UPDATE test.public.t SET x = $1", val2); err != nil {
		t.Fatal(err)
	}

	// We now run a traced historical query and expect to see val1 instead of the
	// more recent val2. We play some tricks for testing this; we run a SHOW KV
	// TRACE so that rows like "output row: [<foo>]" are part of the results. And
	// then we look for a particular such row. Unfortunately we can't easily do
	// this on the original query because SELECT ... FROM [SHOW TRACE FOR ... AS
	// OF SYSTEM TIME ... ) WHERE ...  is not supported because of AS OF SYSTEM
	// TIME limitations. So, we cheat and we use a subsequent SHOW TRACE FOR
	// SESSION, which will present the results recorded by the first query.
	query := fmt.Sprintf("SHOW KV TRACE FOR SELECT x FROM test.public.t AS OF SYSTEM TIME %s", tsVal1)
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}

	query = fmt.Sprintf("select count(1) from [show kv trace for session] "+
		"where message = 'output row: [%d]'", val1)
	if err := db.QueryRow(query).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != 1 {
		t.Fatalf("expected to find one matching row, got %v", i)
	}
}
