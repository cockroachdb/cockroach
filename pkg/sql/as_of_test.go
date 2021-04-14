// Copyright 2016 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestAsOfTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	params.Knobs.GCJob = &sql.GCJobTestingKnobs{RunBeforeResume: func(_ jobspb.JobID) error { select {} }}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

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
	if _, err := db.Query(fmt.Sprintf(query, tsEmpty), 0); !testutils.IsError(err, `pq: relation "d.t" does not exist`) {
		t.Fatal(err)
	}

	var tsDBExists string
	if err := db.QueryRow("CREATE DATABASE d; SELECT cluster_logical_timestamp()").Scan(&tsDBExists); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(fmt.Sprintf(query, tsDBExists), 0); !testutils.IsError(err, `pq: relation "d.t" does not exist`) {
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

	// Test a simple query, and do it with and without wrapping parens to check
	// that parens don't matter.
	testutils.RunTrueAndFalse(t, "parens", func(t *testing.T, useParens bool) {
		openParens := ""
		closeParens := ""
		if useParens {
			openParens = "(("
			closeParens = "))"
		}
		query := fmt.Sprintf("%sSELECT a, c FROM d.t, d.j AS OF SYSTEM TIME %s%s", openParens, tsVal1, closeParens)
		if err := db.QueryRow(query).Scan(&i, &j); err != nil {
			t.Fatal(err)
		} else if i != val1 {
			t.Fatalf("expected %v, got %v", val1, i)
		} else if j != val2 {
			t.Fatalf("expected %v, got %v", val2, j)
		}
	})

	// Future queries shouldn't work if not marked as synthetic.
	if err := db.QueryRow("SELECT a FROM d.t AS OF SYSTEM TIME '2200-01-01'").Scan(&i); !testutils.IsError(err, "pq: AS OF SYSTEM TIME: cannot specify timestamp in the future") {
		t.Fatal(err)
	}

	// Future queries shouldn't work if too far in the future.
	if err := db.QueryRow("SELECT a FROM d.t AS OF SYSTEM TIME '+10h?'").Scan(&i); !testutils.IsError(err, "pq: request timestamp .* too far in future") {
		t.Fatal(err)
	}

	// Future queries work if marked as synthetic and only slightly in future.
	if err := db.QueryRow("SELECT a FROM d.t AS OF SYSTEM TIME '+10ms?'").Scan(&i); err != nil {
		t.Fatal(err)
	}

	// Verify queries with positive scale work properly.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1e1"); !testutils.IsError(err, `pq: relation "d.t" does not exist`) {
		t.Fatal(err)
	}

	// Verify queries with large exponents error properly.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1e40"); !testutils.IsError(err, "value out of range") {
		t.Fatal(err)
	}
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1.4"); !testutils.IsError(err,
		`parsing argument: strconv.ParseInt: parsing "4000000000": value out of range`) {
		t.Fatal(err)
	}

	// Verify logical parts parse with < 10 digits.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1.123456789"); !testutils.IsError(err, `pq: relation "d.t" does not exist`) {
		t.Fatal(err)
	}

	// Verify logical parts parse with == 10 digits.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1.1234567890"); !testutils.IsError(err, `pq: relation "d.t" does not exist`) {
		t.Fatal(err)
	}

	// Too much logical precision is an error.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 1.12345678901"); !testutils.IsError(err, "logical part has too many digits") {
		t.Fatal(err)
	}

	// Ditto, as string.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME '1.12345678901'"); !testutils.IsError(err, "logical part has too many digits") {
		t.Fatal(err)
	}

	// String values that are neither timestamps nor decimals are an error.
	if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME 'xxx'"); !testutils.IsError(err, "value is neither timestamp, decimal, nor interval") {
		t.Fatal(err)
	}

	// Zero is not a valid value.
	for _, zero := range []string{"0", "'0'", "0.0000000000", "'0.0000000000'"} {
		if _, err := db.Query("SELECT a FROM d.t AS OF SYSTEM TIME " + zero); !testutils.IsError(err, "zero timestamp is invalid") {
			t.Fatal(err)
		}
	}

	// Queries before the Unix epoch definitely shouldn't work.
	if err := db.QueryRow("SELECT a FROM d.t AS OF SYSTEM TIME '1969-12-30'").Scan(&i); err == nil {
		t.Fatal("expected error")
	} else if !testutils.IsError(err, "AS OF SYSTEM TIME: timestamp before 1970-01-01T00:00:00Z is invalid") {
		t.Fatal(err)
	}

	// Subqueries shouldn't work.
	_, err := db.Query(
		fmt.Sprintf("SELECT (SELECT a FROM d.t AS OF SYSTEM TIME %s)", tsVal1))
	if !testutils.IsError(err, "pq: AS OF SYSTEM TIME must be provided on a top-level statement") {
		t.Fatalf("expected not supported, got: %v", err)
	}

	// Subqueries do work of the timestamps are consistent.
	_, err = db.Query(
		fmt.Sprintf("SELECT (SELECT a FROM d.t AS OF SYSTEM TIME %s) FROM (SELECT 1) AS OF SYSTEM TIME '1980-01-01'", tsVal1))
	if !testutils.IsError(err, "cannot specify AS OF SYSTEM TIME with different timestamps") {
		t.Fatalf("expected inconsistent statements, got: %v", err)
	}
	if err := db.QueryRow(
		fmt.Sprintf("SELECT (SELECT 1 FROM d.t AS OF SYSTEM TIME %s) FROM (SELECT 1) AS OF SYSTEM TIME %s", tsVal1, tsVal1)).Scan(&i); err != nil {
		t.Fatal(err)
	}

	// Lightly test AS OF SYSTEM TIME with SET TRANSACTION, more complete testing
	// for this functionality lives in the logic_tests.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(fmt.Sprintf("SET TRANSACTION AS OF SYSTEM TIME %s", tsVal1)); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow("SELECT a FROM d.t WHERE a > $1", 0).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %v, got %v", val1, i)
	}
	if err := tx.Commit(); err != nil {
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

	// Can use in a transaction by using the SET TRANSACTION syntax
	if err := db.QueryRow(fmt.Sprintf(`
			BEGIN;
			SET TRANSACTION AS OF SYSTEM TIME %s;
			SELECT a FROM d.t;
			COMMIT;
	`, tsVal1)).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %v, got %v", val1, i)
	}

	// Can't use in a transaction without SET TRANSACTION AS OF SYSTEM TIME syntax
	_, err = db.Query(
		fmt.Sprintf("BEGIN; SELECT a FROM d.t AS OF SYSTEM TIME %s; COMMIT;", tsVal1))
	if !testutils.IsError(err, "try SET TRANSACTION AS OF SYSTEM TIME") {
		t.Fatalf("expected try SET TRANSACTION AS OF SYSTEM TIME, got: %v", err)
	}
}

// Test that a TransactionRetryError will retry the read until it succeeds. The
// test is designed so that if the proto timestamps are bumped during retry
// a failure will occur.
func TestAsOfRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, cmdFilters := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

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
		func(args kvserverbase.FilterArgs) *roachpb.Error {
			magicVals.Lock()
			defer magicVals.Unlock()

			switch req := args.Req.(type) {
			case *roachpb.GetRequest:
				if kv.TestingIsRangeLookupRequest(req) {
					return nil
				}
				for key, count := range magicVals.restartCounts {
					if err := checkCorrectTxn(string(req.Key), magicVals, args.Hdr.Txn); err != nil {
						return roachpb.NewError(err)
					}
					if count > 0 && bytes.Contains(req.Key, []byte(key)) {
						magicVals.restartCounts[key]--
						err := roachpb.NewTransactionRetryError(
							roachpb.RETRY_REASON_UNKNOWN, "filter err")
						magicVals.failedValues[string(req.Key)] =
							failureRecord{err, args.Hdr.Txn}
						txn := args.Hdr.Txn.Clone()
						txn.WriteTimestamp = txn.WriteTimestamp.Add(0, 1)
						return roachpb.NewErrorWithTxn(err, txn)
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

// Test that tracing works with SELECT ... AS OF SYSTEM TIME.
func TestShowTraceAsOfTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	const val1 = 456
	const val2 = 789

	if _, err := db.Exec(`
		CREATE DATABASE test;
		CREATE TABLE test.t (x INT);
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("INSERT INTO test.t (x) VALUES ($1)", val1); err != nil {
		t.Fatal(err)
	}
	var tsVal1 string
	var i int
	err := db.QueryRow("SELECT x, cluster_logical_timestamp() FROM test.t").Scan(
		&i, &tsVal1)
	if err != nil {
		t.Fatal(err)
	} else if i != val1 {
		t.Fatalf("expected %d, got %v", val1, i)
	}
	if _, err := db.Exec("UPDATE test.t SET x = $1", val2); err != nil {
		t.Fatal(err)
	}

	// We now run a traced historical query and expect to see val1 instead of the
	// more recent val2. We play some tricks for testing this; we run SET tracing = results
	// so that rows like "output row: [<foo>]" are part of the results. And
	// then we look for a particular such row.
	query := fmt.Sprintf("SET tracing = on,results; SELECT x FROM test.t AS OF SYSTEM TIME %s; SET tracing = off", tsVal1)
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}

	query = fmt.Sprintf("SELECT count(1) FROM [SHOW KV TRACE FOR SESSION] "+
		"WHERE message = 'output row: [%d]'", val1)
	if err := db.QueryRow(query).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != 1 {
		t.Fatalf("expected to find one matching row, got %v", i)
	}
}
