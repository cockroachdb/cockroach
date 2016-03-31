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
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/storageutils"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/uuid"
	_ "github.com/cockroachdb/pq"
)

type failureRecord struct {
	err error
	txn *roachpb.Transaction
}

type filterVals struct {
	sync.Mutex
	// key -> number of times an retriable error will be injected when that key
	// is written.
	restartCounts map[string]int
	// key -> number of times a TransactionAborted error will be injected when
	// that key is written. Note that injecting this is pretty funky: it can only
	// be done on the first write of a txn, otherwise the previously written
	// intents will linger on.
	abortCounts map[string]int

	// Map from a values to the number of times we'll inject a
	// TransactionRetryError in the transaction writing that value.
	endTxnRestartCounts map[string]int

	// Keys for which we injected an error.
	failedValues map[string]failureRecord

	// Map in which we're accumulated the ids of the txns that we need to inject
	// errors into because of endTxnRestartCounts.
	txnsToFail map[uuid.UUID]bool
}

func createFilterVals(
	restartCounts map[string]int,
	abortCounts map[string]int) *filterVals {
	return &filterVals{
		restartCounts: restartCounts,
		abortCounts:   abortCounts,
		failedValues:  map[string]failureRecord{},
		txnsToFail:    map[uuid.UUID]bool{},
	}
}

// checkCorrectTxn checks that the current txn is the correct one, according to
// the way the previous txn that tried to write value failed.
func checkCorrectTxn(value string, magicVals *filterVals, txn *roachpb.Transaction) {
	failureRec, found := magicVals.failedValues[value]
	if !found {
		return
	}
	switch failureRec.err.(type) {
	case *roachpb.TransactionAbortedError:
		// The previous txn should have been aborted, so check that we're running
		// in a new one.
		if failureRec.txn.Equal(txn) {
			panic(fmt.Sprintf("new transaction for value \"%s\" is the same "+
				"as the old one", value))
		}
	default:
		// The previous txn should have been restarted, so we should be running in
		// the same one.
		if !failureRec.txn.Equal(txn) {
			// panic and not t.Fatal because this runs on a different goroutine
			panic(fmt.Sprintf("new transaction for value \"%s\" (%s) "+
				"is not the same as the old one (%s)", value,
				txn, failureRec.txn))
		}
	}
	// Don't check this value in subsequent transactions.
	delete(magicVals.failedValues, value)
}

func injectErrors(
	req roachpb.Request,
	hdr roachpb.Header,
	magicVals *filterVals,
) error {
	magicVals.Lock()
	defer magicVals.Unlock()

	switch req := req.(type) {
	case *roachpb.ConditionalPutRequest:
		txnID := *hdr.Txn.TxnMeta.ID
		for key, count := range magicVals.restartCounts {
			checkCorrectTxn(string(req.Value.RawBytes), magicVals, hdr.Txn)
			if count > 0 && bytes.Contains(req.Value.RawBytes, []byte(key)) {
				magicVals.restartCounts[key]--
				err := roachpb.NewReadWithinUncertaintyIntervalError(
					roachpb.ZeroTimestamp, roachpb.ZeroTimestamp)
				magicVals.failedValues[string(req.Value.RawBytes)] =
					failureRecord{err, hdr.Txn}
				return err
			}
		}
		for key, count := range magicVals.abortCounts {
			checkCorrectTxn(string(req.Value.RawBytes), magicVals, hdr.Txn)
			if count > 0 && bytes.Contains(req.Value.RawBytes, []byte(key)) {
				magicVals.abortCounts[key]--
				err := roachpb.NewTransactionAbortedError()
				magicVals.failedValues[string(req.Value.RawBytes)] =
					failureRecord{err, hdr.Txn}
				return err
			}
		}
		// If we're writing a value that's marked for an EndTransaction failure,
		// keep track of the txn id so we can fail it later on.
		for key, count := range magicVals.endTxnRestartCounts {
			if count > 0 && bytes.Contains(req.Value.RawBytes, []byte(key)) {
				if _, found := magicVals.txnsToFail[txnID]; found {
					continue
				}
				magicVals.endTxnRestartCounts[key]--
				magicVals.txnsToFail[txnID] = true
			}
		}
		return nil
	case *roachpb.EndTransactionRequest:
		txnID := *hdr.Txn.TxnMeta.ID
		if !magicVals.txnsToFail[txnID] {
			return nil
		}
		delete(magicVals.txnsToFail, txnID)
		// Note that we can't return TransactionAborted errors, although those are
		// more representative for the errors that EndTransaction might encounter,
		// because returning those would result in the txn's intents being left
		// around.
		return roachpb.NewTransactionRetryError()
	default:
		return nil
	}
}

// checkRestart checks that there are no errors left to inject.
func checkRestarts(t *testing.T, magicVals *filterVals) {
	magicVals.Lock()
	defer magicVals.Unlock()
	for key, count := range magicVals.restartCounts {
		if count != 0 {
			file, line, _ := caller.Lookup(1)
			t.Errorf("%s:%d: INSERT for \"%s\" still has to be retried %d times",
				file, line, key, count)
		}
	}
	for key, count := range magicVals.abortCounts {
		if count != 0 {
			file, line, _ := caller.Lookup(1)
			t.Errorf("%s:%d: INSERT for \"%s\" still has to be aborted %d times",
				file, line, key, count)
		}
	}
	for key, count := range magicVals.endTxnRestartCounts {
		if count != 0 {
			file, line, _ := caller.Lookup(1)
			t.Errorf("%s:%d: txn writing \"%s\" still has to be aborted %d times",
				file, line, key, count)
		}
	}
	if len(magicVals.txnsToFail) > 0 {
		file, line, _ := caller.Lookup(1)
		t.Errorf("%s:%d: txns still to be failed: %v", file, line, magicVals.txnsToFail)
	}
	if t.Failed() {
		t.Fatalf("checking error injection failed")
	}
}

// TestTxnRestart tests the logic in the sql executor for automatically retrying
// txns in case of retriable errors.
func TestTxnRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cmdFilters := createTestServerContext()
	server, sqlDB, _ := setupWithContext(t, ctx)
	defer cleanup(server, sqlDB)

	// Make sure all the commands we send in this test are sent over the same connection.
	// This is a bit of a hack; in Go you're not supposed to have connection state
	// outside of using a db.Tx. But we can't use a db.Tx here, because we want
	// to control the batching of BEGIN/COMMIT statements.
	// This SetMaxOpenConns is pretty shady, it doesn't guarantee that you'll be using
	// the *same* one connection across calls. A proper solution would be to use a
	// lib/pq connection directly. As of Feb 2016, there's code in cli/sql_util.go to
	// do that.
	sqlDB.SetMaxOpenConns(1)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k TEXT PRIMARY KEY, v TEXT, t DECIMAL);
`); err != nil {
		t.Fatal(err)
	}

	// Set up error injection that causes retries.
	magicVals := createFilterVals(nil, nil)
	magicVals.restartCounts = map[string]int{
		"boulanger": 2,
		"dromedary": 2,
		"fajita":    2,
		"hooly":     2,
		"josephine": 2,
		"laureal":   2,
	}
	magicVals.abortCounts = map[string]int{
		"boulanger": 2,
	}
	magicVals.endTxnRestartCounts = map[string]int{
		"boulanger": 2,
		"dromedary": 2,
		"fajita":    2,
		"hooly":     2,
	}
	cleanupFilter := cmdFilters.AppendFilter(
		func(args storageutils.FilterArgs) *roachpb.Error {
			if err := injectErrors(args.Req, args.Hdr, magicVals); err != nil {
				return roachpb.NewErrorWithTxn(err, args.Hdr.Txn)
			}
			return nil
		}, false)

	// Test that implicit txns - txns for which we see all the statements and prefixes
	// of txns (statements batched together with the BEGIN stmt) - are retried.
	// We also exercise the SQL cluster logical timestamp in here, because
	// this must be properly propagated across retries.
	if _, err := sqlDB.Exec(`
INSERT INTO t.test (k, v, t) VALUES ('a', 'boulanger', cluster_logical_timestamp());
BEGIN;
INSERT INTO t.test (k, v, t) VALUES ('c', 'dromedary', cluster_logical_timestamp());
INSERT INTO t.test (k, v, t) VALUES ('e', 'fajita', cluster_logical_timestamp());
END;
INSERT INTO t.test (k, v, t) VALUES ('g', 'hooly', cluster_logical_timestamp());
BEGIN;
INSERT INTO t.test (k, v, t) VALUES ('i', 'josephine', cluster_logical_timestamp());
INSERT INTO t.test (k, v, t) VALUES ('k', 'laureal', cluster_logical_timestamp());
`); err != nil {
		t.Fatal(err)
	}
	cleanupFilter()

	checkRestarts(t, magicVals)

	if _, err := sqlDB.Exec("END;"); err != nil {
		t.Fatal(err)
	}
	// Check that the txns succeeded by reading the rows.
	var count int
	if err := sqlDB.QueryRow("SELECT COUNT(*) FROM t.test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 6 {
		t.Fatalf("Expected 6 rows, got %d", count)
	}

	// Now test that we don't retry what we shouldn't: insert an error into a txn
	// we can't automatically retry (because it spans requests).

	magicVals = createFilterVals(nil, nil)
	magicVals.restartCounts = map[string]int{
		"hooly": 2,
	}
	cleanupFilter = cmdFilters.AppendFilter(
		func(args storageutils.FilterArgs) *roachpb.Error {
			if err := injectErrors(args.Req, args.Hdr, magicVals); err != nil {
				return roachpb.NewErrorWithTxn(err, args.Hdr.Txn)
			}
			return nil
		}, false)
	defer cleanupFilter()

	// Start a txn.
	if _, err := sqlDB.Exec(`
DELETE FROM t.test WHERE true;
BEGIN;
`); err != nil {
		t.Fatal(err)
	}

	// Continue the txn in a new request, which is not retriable.
	_, err := sqlDB.Exec("INSERT INTO t.test (k, v, t) VALUES ('g', 'hooly', cluster_logical_timestamp())")
	if !testutils.IsError(
		err, "encountered previous write with future timestamp") {
		t.Errorf("didn't get expected injected error. Got: %s", err)
	}
}

// exec takes a closure and executes it repeatedly as long as it says it needs
// to be retried.
// TODO(andrei): change this to return an error and make runTestTxn inspect the
// error to see if it's retriable once we get a retriable error code.
func exec(t *testing.T, sqlDB *sql.DB, fn func(*sql.Tx) bool) {
	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	// TODO(andrei): make the priority deterministic here once Radu introduces that syntax.
	if _, err := tx.Exec(
		"SAVEPOINT cockroach_restart; SET TRANSACTION PRIORITY LOW;"); err != nil {
		t.Fatal(err)
	}

	for fn(tx) {
		if _, err := tx.Exec("ROLLBACK TO SAVEPOINT cockroach_restart"); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// Returns true on retriable errors.
func runTestTxn(t *testing.T, magicVals *filterVals, expectedErr string,
	injectReleaseError *bool, sqlDB *sql.DB, tx *sql.Tx) bool {
	retriesNeeded :=
		(magicVals.restartCounts["boulanger"] + magicVals.abortCounts["boulanger"]) > 0
	var err error
	if retriesNeeded {
		_, err = tx.Exec("INSERT INTO t.test (k, v) VALUES (1, 'boulanger')")
		if !testutils.IsError(err, expectedErr) {
			t.Fatalf("expected to fail here. err: %s", err)
		}
		return true
	}
	// Now the INSERT should succeed.
	_, err = tx.Exec("DELETE FROM t.test WHERE true; INSERT INTO t.test (k, v) VALUES (0, 'sentinel');")
	if err != nil {
		t.Fatal(err)
	}

	retriesNeeded = *injectReleaseError
	if retriesNeeded {
		*injectReleaseError = false
		abortTxn(t, sqlDB, 0)
	}
	_, err = tx.Exec("RELEASE SAVEPOINT cockroach_restart")
	if retriesNeeded {
		if err == nil {
			t.Fatal("expected RELEASE to fail")
		}
		return true
	} else {
		if err != nil {
			t.Fatal(err)
		}
		return false
	}
}

// abortTxn writes to a key (with retries) and as a side effect aborts a txn
// that had an intent on that key.
// This cannot be done as an injected error, since we want the pusher to clean
// up the intents of the pushee.
func abortTxn(t *testing.T, sqlDB *sql.DB, key int) {
	// Execute all statements in one batch so that the server retries it
	// automatically in case our txn (the pusher) fails to push the victim. Even
	// though it starts with a HIGH priority, that's no guarantee it'll always
	// succeed from the first try.
	// TODO(andrei): make the priority deterministic here once Radu introduces
	// that syntax and then we won't need all the statements in one batch.
	deleteCmd := fmt.Sprintf("DELETE FROM t.test WHERE k = %d", key)
	if _, err := sqlDB.Exec(
		fmt.Sprintf("BEGIN; SET TRANSACTION PRIORITY HIGH; %s; COMMIT;", deleteCmd)); err != nil {
		t.Fatal(err)
	}
}

// TestUserTxnRestart tests user-directed txn restarts.
// The test will inject and otherwise create retriable errors of various kinds
// and checks that we still manage to run a txn despite them.
func TestTxnUserRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cmdFilters := createTestServerContext()
	server, sqlDB, _ := setupWithContext(t, ctx)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	// Set up error injection that causes retries.
	testCases := []struct {
		magicVals   *filterVals
		expectedErr string
	}{
		{
			magicVals: createFilterVals(
				map[string]int{"boulanger": 2}, // restartCounts
				nil),
			expectedErr: ".*encountered previous write with future timestamp.*",
		},
		{
			magicVals: createFilterVals(
				nil,
				map[string]int{"boulanger": 2}), // abortCounts
			expectedErr: ".*txn aborted.*",
		},
	}

	for _, tc := range testCases {
		cleanupFilter := cmdFilters.AppendFilter(
			func(args storageutils.FilterArgs) *roachpb.Error {
				if err := injectErrors(args.Req, args.Hdr, tc.magicVals); err != nil {
					return roachpb.NewErrorWithTxn(err, args.Hdr.Txn)
				}
				return nil
			}, false)

		// Also inject an error at RELEASE time, besides the error injected by magicVals.
		injectReleaseError := true

		commitCount := server.TestServer.MustGetSQLCounter("txn.commit.count")
		// This is the magic. Run the txn closure until all the retries are exhausted.
		exec(t, sqlDB, func(tx *sql.Tx) bool {
			return runTestTxn(t, tc.magicVals, tc.expectedErr, &injectReleaseError, sqlDB, tx)
		})
		checkRestarts(t, tc.magicVals)

		// Check that we only wrote the sentinel row.
		rows, err := sqlDB.Query("SELECT * FROM t.test")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var k int
			var v string
			err = rows.Scan(&k, &v)
			if err != nil {
				t.Fatal(err)
			}
			if k != 0 || v != "sentinel" {
				t.Fatalf("didn't find expected row: %d %s", k, v)
			}
		}
		// Check that the commit counter was incremented. It could have been
		// incremented by more than 1 because of the transactions we use to force
		// aborts, plus who knows what else the server is doing in the background.
		checkCounterGE(t, server, "txn.commit.count", commitCount+1)
		// Clean up the table for the next test iteration.
		_, err = sqlDB.Exec("DELETE FROM t.test WHERE true")
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()
		cleanupFilter()
	}
}

// Test that rando commands while in COMMIT_WAIT return a particular error.
func TestCommitWaitState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	server, sqlDB, _ := setup(t)
	defer cleanup(server, sqlDB)
	if _, err := sqlDB.Exec(`
CREATE DATABASE t; CREATE TABLE t.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		"SAVEPOINT cockroach_restart; RELEASE cockroach_restart;"); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec("INSERT INTO t.test (k, v) VALUES (0, 'sentinel');")
	if !testutils.IsError(err, "current transaction is committed") {
		t.Fatal(err)
	}
	// Rollback should respond with a COMMIT command tag.
	err = tx.Rollback()
	if !testutils.IsError(err, "unexpected command tag COMMIT") {
		t.Fatal(err)
	}
}

// Test that if there's an error on COMMIT that needs to be reported to the user
// the txn will be rolled back. As opposed to an error on a COMMIT in an auto-retry
// txn, where we retry the txn (not tested here).
func TestErrorOnCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	server, sqlDB, _ := setup(t)
	defer cleanup(server, sqlDB)
	if _, err := sqlDB.Exec(`
CREATE DATABASE t; CREATE TABLE t.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	// TODO(andrei): make the priority deterministic here once Radu introduces that syntax.
	if _, err := tx.Exec("SAVEPOINT cockroach_restart; SET TRANSACTION PRIORITY LOW;"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("INSERT INTO t.test (k, v) VALUES (0, 'sentinel');"); err != nil {
		t.Fatal(err)
	}

	abortTxn(t, sqlDB, 0)

	if err = tx.Commit(); err == nil {
		t.Fatal("expected commit to fail")
	}

	// Check that there's no error reading and we don't see any rows.
	var rows *sql.Rows
	if rows, err = sqlDB.Query("SELECT * FROM t.test"); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		var k int
		var v string
		_ = rows.Scan(&k, &v)
		t.Fatalf("found unexpected row: %d %s", k, v)
	}
	rows.Close()
}

// TestRollbackToSavepointStatement tests that issuing a RESTART outside of a
// txn produces the proper error.
func TestRollbackToSavepointStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	server, sqlDB, _ := setup(t)
	defer cleanup(server, sqlDB)

	// ROLLBACK TO SAVEPOINT without a transaction
	_, err := sqlDB.Exec("ROLLBACK TO SAVEPOINT cockroach_restart")
	if !testutils.IsError(err, "the transaction is not in a retriable state") {
		t.Fatal("expected to fail here. err: ", err)
	}
	// ROLLBACK TO SAVEPOINT with a wrong name
	_, err = sqlDB.Exec("ROLLBACK TO SAVEPOINT foo")
	if !testutils.IsError(err, "SAVEPOINT not supported except for COCKROACH_RESTART") {
		t.Fatal("expected to fail here. err: ", err)
	}

	// ROLLBACK TO SAVEPOINT in a non-retriable transaction
	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("SAVEPOINT cockroach_restart"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("BOGUS SQL STATEMENT"); err == nil {
		t.Fatalf("expected to fail here. err: %s", err)
	}
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT cockroach_restart")
	if !testutils.IsError(err,
		"SAVEPOINT COCKROACH_RESTART has not been used or a non-retriable error was encountered") {
		t.Fatal("expected to fail here. err: ", err)
	}
}

// TestNonRetriableError checks that a non-retriable error (e.g. duplicate key)
// doesn't leave the txn in a restartable state.
func TestNonRetriableError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	server, sqlDB, _ := setup(t)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	var tx *sql.Tx
	var err error
	if tx, err = sqlDB.Begin(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("SAVEPOINT cockroach_restart"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("INSERT INTO t.test (k, v) VALUES (0, 'test')"); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec("INSERT INTO t.test (k, v) VALUES (0, 'test');")
	if !testutils.IsError(err, "duplicate key value") {
		t.Errorf("expected duplicate key error. Got: %s", err)
	}
	if _, err := tx.Exec("ROLLBACK TO SAVEPOINT cockroach_restart"); !testutils.IsError(
		err, "current transaction is aborted, commands ignored until end of "+
			"transaction block; SAVEPOINT COCKROACH_RESTART has not been used or a "+
			"non-retriable error was encountered.") {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// TestRollbackInRestartWait ensures that a ROLLBACK while the txn is in the
// RetryWait state works.
func TestRollbackInRestartWait(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cmdFilters := createTestServerContext()
	server, sqlDB, _ := setupWithContext(t, ctx)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k TEXT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	// Set up error injection that causes retries.
	magicVals := createFilterVals(nil, nil)
	magicVals.endTxnRestartCounts = map[string]int{
		"boulanger": 1,
	}
	cmdFilters.AppendFilter(
		func(args storageutils.FilterArgs) *roachpb.Error {
			if err := injectErrors(args.Req, args.Hdr, magicVals); err != nil {
				return roachpb.NewErrorWithTxn(err, args.Hdr.Txn)
			}
			return nil
		}, false)

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("SAVEPOINT cockroach_restart"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		"INSERT INTO t.test (k, v) VALUES ('g', 'boulanger')"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("RELEASE SAVEPOINT cockroach_restart"); err == nil {
		t.Fatal("expected RELEASE to fail")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
