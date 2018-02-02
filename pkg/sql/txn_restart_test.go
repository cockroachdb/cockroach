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
	gosql "database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type failureRecord struct {
	err error
	txn *roachpb.Transaction
}

type filterVals struct {
	syncutil.Mutex
	// key -> number of times an retriable error will be injected when that key
	// is written.
	restartCounts map[string]int
	// key -> number of times a TransactionAborted error will be injected when
	// that key is written. Note that injecting this is pretty funky: it can only
	// be done on the first write of a txn, otherwise the previously written
	// intents will linger on.
	abortCounts map[string]int

	// Keys for which we injected an error.
	failedValues map[string]failureRecord
}

func createFilterVals(restartCounts map[string]int, abortCounts map[string]int) *filterVals {
	return &filterVals{
		restartCounts: restartCounts,
		abortCounts:   abortCounts,
		failedValues:  map[string]failureRecord{},
	}
}

// checkCorrectTxn checks that the current txn is the correct one, according to
// the way the previous txn that tried to write value failed.
func checkCorrectTxn(value string, magicVals *filterVals, txn *roachpb.Transaction) error {
	failureRec, found := magicVals.failedValues[value]
	if !found {
		return nil
	}
	switch failureRec.err.(type) {
	case *roachpb.TransactionAbortedError:
		// The previous txn should have been aborted, so check that we're running
		// in a new one.
		if failureRec.txn.ID == txn.ID {
			return errors.Errorf(`new transaction for value "%s" is the same as the old one`, value)
		}
	default:
		// The previous txn should have been restarted, so we should be running in
		// the same one.
		if failureRec.txn.ID != txn.ID {
			return errors.Errorf(`new transaction for value "%s" (%s) is not the same as the old one (%s)`, value, txn, failureRec.txn)
		}
	}
	// Don't check this value in subsequent transactions.
	delete(magicVals.failedValues, value)

	return nil
}

type injectionApproach struct {
	counts map[string]int
	errFn  func() error
}

type injectionApproaches []injectionApproach

func (ia injectionApproaches) Len() int      { return len(ia) }
func (ia injectionApproaches) Swap(i, j int) { ia[i], ia[j] = ia[j], ia[i] }

func injectErrors(
	req roachpb.Request, hdr roachpb.Header, magicVals *filterVals, verifyTxn bool,
) error {
	magicVals.Lock()
	defer magicVals.Unlock()

	switch req := req.(type) {
	case *roachpb.ConditionalPutRequest:
		// Create a list of each injection approach and shuffle the order of
		// injection for some additional randomness.
		injections := injectionApproaches{
			{counts: magicVals.restartCounts, errFn: func() error {
				// Note we use a retry error that cannot be automatically retried
				// by the transaction coord sender.
				return roachpb.NewTransactionRetryError(roachpb.RETRY_POSSIBLE_REPLAY)
			}},
			{counts: magicVals.abortCounts, errFn: func() error {
				return roachpb.NewTransactionAbortedError()
			}},
		}
		shuffle.Shuffle(injections)

		for _, injection := range injections {
			for key, count := range injection.counts {
				if verifyTxn {
					if err := checkCorrectTxn(string(req.Value.RawBytes), magicVals, hdr.Txn); err != nil {
						return err
					}
				}
				if count > 0 && bytes.Contains(req.Value.RawBytes, []byte(key)) {
					injection.counts[key]--
					err := injection.errFn()
					magicVals.failedValues[string(req.Value.RawBytes)] = failureRecord{err, hdr.Txn}
					return err
				}
			}
		}
		return nil
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
	if t.Failed() {
		t.Fatalf("checking error injection failed")
	}
}

// TxnAborter can be used to listen for transactions running particular
// SQL statements; the trapped transactions will be aborted.
// The TxnAborter needs to be hooked up to a Server's
// Knobs.StatementFilter, so that the Aborter sees what statements are being
// executed. This is done by calling HookupToExecutor(), which returns a
// stuitable ExecutorTestingKnobs.
// A statement can be registered for abortion (meaning, the statement's
// transaction will be TransactionAborted) with QueueStmtForAbortion(). When the
// Aborter sees that statement, it will run a higher priority transaction that
// tramples the data, so the original transaction will get a TransactionAborted
// error when it tries to commit.
//
// Note that transaction cannot be aborted using an injected error, since we
// want the pusher to clean up the intents of the pushee.
//
// The aborter only works with INSERT statements operating on the table t.test
// defined as:
//	`CREATE DATABASE t; CREATE TABLE t.public.test (k INT PRIMARY KEY, v TEXT)`
// The TxnAborter runs transactions deleting the row for the `k` that the
// trapped transactions were writing to.
//
// Be sure to set DisableAutoCommit on the ExecutorTestingKnobs, otherwise
// implicit transactions won't have a chance to be aborted.
//
// Example usage:
//
//	func TestTxnAutoRetry(t *testing.T) {
//		defer leaktest.AfterTest(t)()
//		aborter := NewTxnAborter()
//		defer aborter.Close(t)
//		params, cmdFilters := tests.CreateTestServerParams()
//		params.Knobs.SQLExecutor = aborter.executorKnobs()
//		s, sqlDB, _ := serverutils.StartServer(t, params)
//		defer s.Stopper().Stop(context.TODO())
//		{
//			pgURL, cleanup := sqlutils.PGUrl(t, s.ServingAddr(), "TestTxnAutoRetry", url.User(security.RootUser)
//			defer cleanup()
//			if err := aborter.Init(pgURL); err != nil {
//				t.Fatal(err)
//			}
//		}
//
//		sqlDB.Exec(`CREATE DATABASE t; CREATE TABLE t.public.test (k INT PRIMARY KEY, v TEXT)`)
//		const sentinelInsert = "INSERT INTO t.public.test(k, v) VALUES (0, 'sentinel')"
//		if err := aborter.QueueStmtForAbortion(
//			sentinelInsert, 1 /* abortCount */, true /* willBeRetriedIbid */,
//		); err != nil {
//			t.Fatal(err)
//		}
//		sqlDB.Exec(sentinelInsert)
//	...
type TxnAborter struct {
	mu struct {
		syncutil.Mutex
		stmtsToAbort map[string]*restartInfo
	}
	// A second connection pool, to be used by aborts.
	// This is needed because the main conn pool is going to be restricted to one
	// connection.
	// TODO(andrei): remove this if we ever move to using libpq conns directly.
	// See TODOs around on SetMaxOpenConns.
	abortDB *gosql.DB
}

type restartInfo struct {
	// The numberic value being inserted in col 'k'.
	key int
	// The remaining number of times to abort the txn.
	abortCount     int
	satisfied      bool
	checkSatisfied bool
	// The number of times the statement as been executed.
	execCount int
}

func NewTxnAborter() *TxnAborter {
	ta := new(TxnAborter)
	ta.mu.stmtsToAbort = make(map[string]*restartInfo)
	return ta
}

func (ta *TxnAborter) Init(pgURL url.URL) error {
	abortDB, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		return err
	}
	ta.abortDB = abortDB
	return nil
}

var valuesRE = regexp.MustCompile(`VALUES.*\((\d),`)

// QueueStmtForAbortion registers a statement whose transaction will be aborted.
//
// stmt needs to be the statement, literally as the parser will convert it back
// to a string.
// abortCount specifies how many times a txn running this statement will be
// aborted.
// willBeRetriedIbid should be set if the statement will be retried by the test
// (as an identical statement). This allows the TxnAborter to assert, on
// Close(), that the statement has been retried the intended number of times by
// the end of the test (besides asserting that an error was injected the right
// number of times. So, the Aborter can be used to check that the retry
// machinery has done its job. The Aborter will consider the statement to have
// been retried correctly if the statement has been executed at least once after
// the Aborter is done injecting errors because of it. So normally we'd expect
// this statement to executed RestartCount + 1 times, but we allow it to be
// retried more times because the statement's txn might also retried because of
// other statements.
//
// Calling QueueStmtForAbortion repeatedly with the same stmt is allowed, and
// each call checks that the previous one was satisfied.
func (ta *TxnAborter) QueueStmtForAbortion(
	stmt string, abortCount int, willBeRetriedIbid bool,
) error {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	if ri, ok := ta.mu.stmtsToAbort[stmt]; ok {
		// If we're overwriting a statement that was already queued, verify it
		// first.
		if err := ri.Verify(); err != nil {
			return errors.Wrapf(err, `statement "%s" error`, stmt)
		}
	}
	// Extract the "key" - the value of the first col, which will be trampled on.
	switch matches := valuesRE.FindStringSubmatch(stmt); len(matches) {
	case 0, 1:
		return errors.Errorf(`bad statement "%s": key col not found`, stmt)
	default:
		key, err := strconv.Atoi(matches[1])
		if err != nil {
			return errors.Wrapf(err, `bad statement "%s"`, stmt)
		}
		ta.mu.stmtsToAbort[stmt] = &restartInfo{
			key:            key,
			abortCount:     abortCount,
			satisfied:      false,
			checkSatisfied: willBeRetriedIbid,
		}
		return nil
	}
}

// GetExecCount returns the number of times a statement has been seen.
// You probably don't want to call this while the TxnAborter might be in
// the process of aborting the txn containing stmt, as the result will not be
// deterministic.
func (ta *TxnAborter) GetExecCount(stmt string) (int, bool) {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	if ri, ok := ta.mu.stmtsToAbort[stmt]; ok {
		return ri.execCount, true
	}
	return 0, false
}

func (ta *TxnAborter) statementFilter(
	ctx context.Context, stmt string, r sql.ResultsWriter, err error,
) error {
	ta.mu.Lock()
	ri, ok := ta.mu.stmtsToAbort[stmt]
	shouldAbort := false
	if ok {
		ri.execCount++
		if ri.abortCount == 0 {
			log.VEventf(ctx, 1, "TxnAborter sees satisfied statement %q", stmt)
			ri.satisfied = true
		}
		if ri.abortCount > 0 && err == nil {
			log.Infof(ctx, "TxnAborter aborting txn for statement %q", stmt)
			ri.abortCount--
			shouldAbort = true
		}
	}
	ta.mu.Unlock()
	if shouldAbort {
		if err := ta.abortTxn(ri.key); err != nil {
			return errors.Wrap(err, "TxnAborter failed to abort")
		}
	}
	return nil
}

// executorKnobs are the bridge between the TxnAborter and the sql.Executor.
func (ta *TxnAborter) executorKnobs() base.ModuleTestingKnobs {
	return &sql.ExecutorTestingKnobs{
		// We're going to abort txns using a TxnAborter, and that's incompatible
		// with AutoCommit.
		DisableAutoCommit: true,
		StatementFilter:   ta.statementFilter,
	}
}

// abortTxn writes to a key and as a side effect aborts a txn that had an intent
// on that key.
func (ta *TxnAborter) abortTxn(key int) error {
	tx, err := ta.abortDB.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec("SET TRANSACTION PRIORITY HIGH"); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM t.public.test WHERE k = $1", key); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

type TxnAborterVerifierError struct {
	errs []error
}

func (e *TxnAborterVerifierError) Error() string {
	strs := make([]string, 0)
	for _, err := range e.errs {
		strs = append(strs, err.Error())
	}
	return strings.Join(strs, "\n")
}

func (ta *TxnAborter) VerifyAndClear() error {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	allErr := TxnAborterVerifierError{}
	for stmt, ri := range ta.mu.stmtsToAbort {
		if err := ri.Verify(); err != nil {
			allErr.errs = append(allErr.errs, errors.Wrapf(err, `statement "%s" error`, stmt))
		}
	}
	ta.mu.stmtsToAbort = make(map[string]*restartInfo)
	if len(allErr.errs) != 0 {
		return &allErr
	}
	return nil
}

func (ta *TxnAborter) Close(t testing.TB) {
	ta.abortDB.Close()
	if err := ta.VerifyAndClear(); err != nil {
		t.Error(err)
	}
}

func (ri *restartInfo) Verify() error {
	if ri.abortCount != 0 {
		return errors.Errorf("%d additional aborts expected", ri.abortCount)
	}
	if ri.checkSatisfied && !ri.satisfied {
		return errors.New("previous abort did not result in a retry")
	}
	return nil
}

// Test the logic in the sql executor for automatically retrying txns in case of
// retriable errors.
func TestTxnAutoRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	aborter := NewTxnAborter()
	defer aborter.Close(t)
	params, cmdFilters := tests.CreateTestServerParams()
	params.Knobs.SQLExecutor = aborter.executorKnobs()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	{
		pgURL, cleanup := sqlutils.PGUrl(t, s.ServingAddr(), "TestTxnAutoRetry", url.User(security.RootUser))
		defer cleanup()
		if err := aborter.Init(pgURL); err != nil {
			t.Fatal(err)
		}
	}

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
CREATE TABLE t.public.test (k INT PRIMARY KEY, v TEXT, t DECIMAL);
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
	cleanupFilter := cmdFilters.AppendFilter(
		func(args storagebase.FilterArgs) *roachpb.Error {
			if err := injectErrors(args.Req, args.Hdr, magicVals, true /* verifyTxn */); err != nil {
				return roachpb.NewErrorWithTxn(err, args.Hdr.Txn)
			}
			return nil
		}, false)

	if err := aborter.QueueStmtForAbortion(
		"INSERT INTO t.public.test(k, v, t) VALUES (1, 'boulanger', cluster_logical_timestamp())", 2 /* abortCount */, true, /* willBeRetriedIbid */
	); err != nil {
		t.Fatal(err)
	}
	if err := aborter.QueueStmtForAbortion(
		"INSERT INTO t.public.test(k, v, t) VALUES (2, 'dromedary', cluster_logical_timestamp())", 2 /* abortCount */, true, /* willBeRetriedIbid */
	); err != nil {
		t.Fatal(err)
	}
	if err := aborter.QueueStmtForAbortion(
		"INSERT INTO t.public.test(k, v, t) VALUES (3, 'fajita', cluster_logical_timestamp())", 2 /* abortCount */, true, /* willBeRetriedIbid */
	); err != nil {
		t.Fatal(err)
	}
	if err := aborter.QueueStmtForAbortion(
		"INSERT INTO t.public.test(k, v, t) VALUES (4, 'hooly', cluster_logical_timestamp())", 2 /* abortCount */, true, /* willBeRetriedIbid */
	); err != nil {
		t.Fatal(err)
	}

	// Test that implicit txns - txns for which we see all the statements and prefixes
	// of txns (statements batched together with the BEGIN stmt) - are retried.
	// We also exercise the SQL cluster logical timestamp in here, because
	// this must be properly propagated across retries.
	//
	// The SELECT within the transaction also checks that discarded
	// intermediate result sets are properly released: the result set it
	// produces is accounted for by the session monitor, and if it is
	// not properly released upon a retry the monitor will cause the
	// server to panic (and thus the test to fail) when the connection
	// is closed.
	//
	// TODO(knz): This test can be made more robust by exposing the
	// current allocation count in monitor and checking that it has the
	// same value at the beginning of each retry.
	if _, err := sqlDB.Exec(`
INSERT INTO t.public.test(k, v, t) VALUES (1, 'boulanger', cluster_logical_timestamp());
BEGIN;
SELECT * FROM t.public.test;
INSERT INTO t.public.test(k, v, t) VALUES (2, 'dromedary', cluster_logical_timestamp());
INSERT INTO t.public.test(k, v, t) VALUES (3, 'fajita', cluster_logical_timestamp());
END;
INSERT INTO t.public.test(k, v, t) VALUES (4, 'hooly', cluster_logical_timestamp());
BEGIN;
INSERT INTO t.public.test(k, v, t) VALUES (5, 'josephine', cluster_logical_timestamp());
INSERT INTO t.public.test(k, v, t) VALUES (6, 'laureal', cluster_logical_timestamp());
`); err != nil {
		t.Fatal(err)
	}
	cleanupFilter()

	checkRestarts(t, magicVals)

	if _, err := sqlDB.Exec("END"); err != nil {
		t.Fatal(err)
	}

	// Check that the txns succeeded by reading the rows.
	var count int
	if err := sqlDB.QueryRow("SELECT count(*) FROM t.public.test").Scan(&count); err != nil {
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
		func(args storagebase.FilterArgs) *roachpb.Error {
			if err := injectErrors(args.Req, args.Hdr, magicVals, true /* verifyTxn */); err != nil {
				return roachpb.NewErrorWithTxn(err, args.Hdr.Txn)
			}
			return nil
		}, false)
	defer cleanupFilter()

	// Start a txn.
	if _, err := sqlDB.Exec(`
DELETE FROM t.public.test WHERE true;
BEGIN;
`); err != nil {
		t.Fatal(err)
	}

	// Run a batch of statements to move the txn out of the AutoRetry state,
	// otherwise the INSERT below would be automatically retried.
	if _, err := sqlDB.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}

	// Continue the txn in a new request, which is not retriable.
	_, err := sqlDB.Exec("INSERT INTO t.public.test(k, v, t) VALUES (4, 'hooly', cluster_logical_timestamp())")
	if !testutils.IsError(
		err, "RETRY_POSSIBLE_REPLAY") {
		t.Errorf("didn't get expected injected error. Got: %v", err)
	}
}

// Test the logic in the sql executor for retrying txns in case of retriable
// errors with parallel statements. We test that when a statement running in
// parallel with others gets a retryable error, the other statements synchronize
// and retry without leaving around any unintended committable data. This was
// the case before #17197 was addressed.
func TestTxnAutoRetryParallelStmts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, cmdFilters := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	sqlDB.SetMaxOpenConns(1)

	// Create three tables because spanBasedDependencyAnalyzer currently
	// considers all writes to the same table to be dependent.
	//
	// TODO(nvanbenschoten): once #14953 is addressed, we can get rid of the
	// extra tables.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test  (k INT PRIMARY KEY, v TEXT, t DECIMAL);
CREATE TABLE t.public.test2 (k INT PRIMARY KEY, v TEXT, t DECIMAL);
CREATE TABLE t.public.test3 (k INT PRIMARY KEY, v TEXT, t DECIMAL);
`); err != nil {
		t.Fatal(err)
	}

	// Run a subtest for each combination of three different statements facing
	// retryable errors or not.
	for _, firstRetry := range []bool{false, true} {
		for _, secondRetry := range []bool{false, true} {
			for _, thirdRetry := range []bool{false, true} {
				name := fmt.Sprintf("retryStmt1=%t,retryStmt2=%t,retryStmt3=%t",
					firstRetry, secondRetry, thirdRetry)
				t.Run(name, func(t *testing.T) {
					if _, err := sqlDB.Exec(`
DELETE FROM t.public.test;
DELETE FROM t.public.test2;
DELETE FROM t.public.test3;
`); err != nil {
						t.Fatal(err)
					}

					magicVals := createFilterVals(nil, nil)
					for key, retry := range map[string]bool{
						"boulanger": firstRetry,
						"dromedary": secondRetry,
						"josephine": thirdRetry,
					} {
						if retry {
							magicVals.restartCounts = map[string]int{key: 2}
							magicVals.abortCounts = map[string]int{key: 2}
						}
					}
					var wg sync.WaitGroup
					defer wg.Wait()
					cleanupFilter := cmdFilters.AppendFilter(
						func(args storagebase.FilterArgs) *roachpb.Error {
							if err := injectErrors(args.Req, args.Hdr, magicVals, false /* verifyTxn */); err != nil {
								// When we inject a TransactionAbortedError, we need to make sure
								// that the transaction's meta record is actually aborted, otherwise
								// it's possible that concurrent requests for the same transaction
								// could continue to heartbeat the pending transaction record, which
								// could lead to a deadlock where a retry is blocked trying to push
								// the previous incarnation of the meta record.
								//
								// In real scenerios (not injected test errors), a TransactionAbortedError
								// will never be seen unless the txn record is actually aborted, so
								// this isn't a concern. We are simply trying to mirror that behavior
								// here.
								if _, ok := err.(*roachpb.TransactionAbortedError); ok {
									// We use a WaitGroup to make sure this async abort cleans up
									// before the subtest.
									wg.Add(1)
									go func() {
										defer wg.Done()
										assureTxnAborted(t, s, args.Hdr.Txn)
									}()
								}
								return roachpb.NewErrorWithTxn(err, args.Hdr.Txn)
							}
							return nil
						}, false)

					// In the case of `retryStmt1=true,retryStmt2=false,retryStmt3=false`,
					// the first insert will get a retry error while the second and third
					// inserts are concurrently executing. The entire transaction should
					// be auto-retried a few times without the successful inserts ever
					// getting duplicate key errors.
					if _, err := sqlDB.Exec(`
BEGIN;
INSERT INTO t.public.test(k, v, t)  VALUES (1, 'boulanger', cluster_logical_timestamp()) RETURNING NOTHING;
INSERT INTO t.public.test2(k, v, t) VALUES (2, 'dromedary', cluster_logical_timestamp()) RETURNING NOTHING;
INSERT INTO t.public.test3(k, v, t) VALUES (3, 'josephine', cluster_logical_timestamp()) RETURNING NOTHING;
END;
`); err != nil {
						t.Fatal(err)
					}

					cleanupFilter()
					checkRestarts(t, magicVals)

					// Verify that the txns succeeded by reading the rows from each table.
					// Each should have written exactly one row.
					for _, table := range []string{"test", "test2", "test3"} {
						var count int
						if err := sqlDB.QueryRow(fmt.Sprintf("SELECT count(*) FROM t.public.%s", table)).Scan(&count); err != nil {
							t.Fatal(err)
						}
						if count != 1 {
							t.Fatalf("Expected 1 rows, got %d", count)
						}
					}
				})
			}
		}
	}
}

// assureTxnAborted makes sure that the meta record for the provided transaction
// is aborted. It is important that this accompanies an injected
// TransactionAbortedError in situations with concurrent Txn requests. If not,
// this can lead to incorrect assumptions by the client.
func assureTxnAborted(t *testing.T, s serverutils.TestServerInterface, txn *roachpb.Transaction) {
	abortBa := roachpb.BatchRequest{}
	push := &roachpb.PushTxnRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
		PusheeTxn: txn.TxnMeta,
		PushType:  roachpb.PUSH_ABORT,
		Force:     true,
	}
	push.PusherTxn.Priority = roachpb.MaxTxnPriority
	abortBa.Add(push)
	if _, pErr := s.DistSender().Send(context.Background(), abortBa); pErr != nil {
		t.Fatalf("failed to abort transaction: %v", pErr)
	}
}

// Test that aborted txn are only retried once.
// Prevents regressions of #8456.
func TestAbortedTxnOnlyRetriedOnce(t *testing.T) {
	defer leaktest.AfterTest(t)()

	aborter := NewTxnAborter()
	defer aborter.Close(t)
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLExecutor = aborter.executorKnobs()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	{
		pgURL, cleanup := sqlutils.PGUrl(t, s.ServingAddr(), "TestAbortedTxnOnlyRetriedOnce", url.User(security.RootUser))
		defer cleanup()
		if err := aborter.Init(pgURL); err != nil {
			t.Fatal(err)
		}
	}

	const insertStmt = "INSERT INTO t.public.test(k, v) VALUES (1, 'boulanger')"
	if err := aborter.QueueStmtForAbortion(
		insertStmt, 1 /* abortCount */, true, /* willBeRetriedIbid */
	); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(insertStmt); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	execCount, ok := aborter.GetExecCount(insertStmt)
	if !ok {
		t.Fatalf("aborter has no state on %q", insertStmt)
	}
	if execCount != 2 {
		t.Fatalf("expected %q to be executed 2 times, but got %d", insertStmt, execCount)
	}
}

// rollbackStrategy is the type of statement which a client can use to
// rollback aborted txns from retryable errors. We accept two statements
// for rolling back to the cockroach_restart savepoint. See
// *Executor.execStmtInAbortedTxn for more about transaction retries.
type rollbackStrategy int

const (
	rollbackToSavepoint rollbackStrategy = iota
	declareSavepoint
)

func (rs rollbackStrategy) SQLCommand() string {
	switch rs {
	case rollbackToSavepoint:
		return "ROLLBACK TO SAVEPOINT cockroach_restart"
	case declareSavepoint:
		return "SAVEPOINT cockroach_restart"
	}
	panic("unreachable")
}

// exec takes a closure and executes it repeatedly as long as it says it needs
// to be retried. The function also takes a rollback strategy, which specifies
// the statement which the client will use to rollback aborted txns from retryable
// errors.
func retryExec(t *testing.T, sqlDB *gosql.DB, rs rollbackStrategy, fn func(*gosql.Tx) bool) {
	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		"SAVEPOINT cockroach_restart; SET TRANSACTION PRIORITY LOW"); err != nil {
		t.Fatal(err)
	}

	for fn(tx) {
		if _, err := tx.Exec(rs.SQLCommand()); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// isRetryableErr returns whether the given error is a PG retryable error.
func isRetryableErr(err error) bool {
	pqErr, ok := err.(*pq.Error)
	return ok && pqErr.Code == "40001"
}

// Returns true on retriable errors.
func runTestTxn(
	t *testing.T,
	magicVals *filterVals,
	expectedErr string,
	sqlDB *gosql.DB,
	tx *gosql.Tx,
	sentinelInsert string,
) bool {
	// Run a bogus statement to disable the automatic server retries of subsequent
	// statements.
	if _, err := tx.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}

	retriesNeeded :=
		(magicVals.restartCounts["boulanger"] + magicVals.abortCounts["boulanger"]) > 0
	if retriesNeeded {
		_, err := tx.Exec("INSERT INTO t.public.test(k, v) VALUES (1, 'boulanger')")
		if !testutils.IsError(err, expectedErr) {
			t.Fatalf("unexpected error: %v", err)
		}
		return isRetryableErr(err)
	}
	// Now the INSERT should succeed.
	if _, err := tx.Exec(
		"DELETE FROM t.public.test WHERE true;" + sentinelInsert,
	); err != nil {
		t.Fatal(err)
	}

	_, err := tx.Exec("RELEASE SAVEPOINT cockroach_restart")
	return isRetryableErr(err)
}

// TestUserTxnRestart tests user-directed txn restarts.
// The test will inject and otherwise create retriable errors of various kinds
// and checks that we still manage to run a txn despite them.
func TestTxnUserRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up error injection that causes retries.
	testCases := []struct {
		magicVals   *filterVals
		expectedErr string
	}{
		{
			magicVals: createFilterVals(
				map[string]int{"boulanger": 2}, // restartCounts
				nil),
			expectedErr: ".*RETRY_POSSIBLE_REPLAY.*",
		},
		{
			magicVals: createFilterVals(
				nil,
				map[string]int{"boulanger": 2}), // abortCounts
			expectedErr: ".*txn aborted.*",
		},
	}

	for _, tc := range testCases {
		for _, rs := range []rollbackStrategy{rollbackToSavepoint, declareSavepoint} {
			for _, parallel := range []bool{false, true} {
				t.Run(fmt.Sprintf("err=%s,stgy=%d,parallel=%t", tc.expectedErr, rs, parallel), func(t *testing.T) {
					aborter := NewTxnAborter()
					defer aborter.Close(t)
					params, cmdFilters := tests.CreateTestServerParams()
					params.Knobs.SQLExecutor = aborter.executorKnobs()
					s, sqlDB, _ := serverutils.StartServer(t, params)
					defer s.Stopper().Stop(context.TODO())
					{
						pgURL, cleanup := sqlutils.PGUrl(t, s.ServingAddr(), "TestTxnUserRestart", url.User(security.RootUser))
						defer cleanup()
						if err := aborter.Init(pgURL); err != nil {
							t.Fatal(err)
						}
					}

					if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
						t.Fatal(err)
					}
					cleanupFilter := cmdFilters.AppendFilter(
						func(args storagebase.FilterArgs) *roachpb.Error {
							if err := injectErrors(args.Req, args.Hdr, tc.magicVals, true /* verifyTxn */); err != nil {
								return roachpb.NewErrorWithTxn(err, args.Hdr.Txn)
							}
							return nil
						}, false)

					// Also inject an error at RELEASE time, besides the error injected by magicVals.
					sentinelInsert := "INSERT INTO t.public.test(k, v) VALUES (0, 'sentinel')"
					if parallel {
						sentinelInsert += " RETURNING NOTHING"
					}
					if err := aborter.QueueStmtForAbortion(
						sentinelInsert, 1 /* abortCount */, true, /* willBeRetriedIbid */
					); err != nil {
						t.Fatal(err)
					}

					commitCount := s.MustGetSQLCounter(sql.MetaTxnCommit.Name)
					// This is the magic. Run the txn closure until all the retries are exhausted.
					retryExec(t, sqlDB, rs, func(tx *gosql.Tx) bool {
						return runTestTxn(t, tc.magicVals, tc.expectedErr, sqlDB, tx, sentinelInsert)
					})
					checkRestarts(t, tc.magicVals)

					// Check that we only wrote the sentinel row.
					rows, err := sqlDB.Query("SELECT * FROM t.public.test")
					if err != nil {
						t.Fatal(err)
					}
					defer rows.Close()
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
					if err := checkCounterGE(s, sql.MetaTxnCommit, commitCount+1); err != nil {
						t.Error(err)
					}
					// Clean up the table for the next test iteration.
					_, err = sqlDB.Exec("DELETE FROM t.public.test WHERE true")
					if err != nil {
						t.Fatal(err)
					}
					cleanupFilter()
				})
			}
		}
	}
}

// Test that rando commands while in COMMIT_WAIT return a particular error.
func TestCommitWaitState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	if _, err := sqlDB.Exec(`
CREATE DATABASE t; CREATE TABLE t.public.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		"SAVEPOINT cockroach_restart; RELEASE cockroach_restart"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t.public.test(k, v) VALUES (0, 'sentinel')"); !testutils.IsError(err, "current transaction is committed") {
		t.Fatalf("unexpected error: %v", err)
	}
	// Rollback should respond with a COMMIT command tag.
	if err := tx.Rollback(); !testutils.IsError(err, "unexpected command tag COMMIT") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Test that a COMMIT getting an error, retriable or not, leaves the txn
// finalized and not in Aborted/RestartWait (i.e. COMMIT, like ROLLBACK, is
// always final). As opposed to an error on a COMMIT in an auto-retry
// txn, where we retry the txn (not tested here).
func TestErrorOnCommitFinalizesTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	aborter := NewTxnAborter()
	defer aborter.Close(t)
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLExecutor = aborter.executorKnobs()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	{
		pgURL, cleanup := sqlutils.PGUrl(t, s.ServingAddr(), "TestErrorOnCommitFinalizesTxn", url.User(security.RootUser))
		defer cleanup()
		if err := aborter.Init(pgURL); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE t; CREATE TABLE t.public.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}
	// We need to do everything on one connection as we'll want to observe the
	// connection state after a COMMIT.
	sqlDB.SetMaxOpenConns(1)

	// We're going to test both errors that would leave the transaction in the
	// RestartWait state and errors that would leave the transaction in Aborted,
	// if they were to happen on any other statement than COMMIT.
	// We do that by always injecting a retryable error at COMMIT, but once in a
	// txn that had a "retry intent" (SAVEPOINT cockroach_restart), and once in a
	// txn without it.
	testCases := []struct {
		retryIntent bool
	}{
		{false},
		{true},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("retryIntent=%t", tc.retryIntent), func(t *testing.T) {
			const insertStmt = "INSERT INTO t.public.test(k, v) VALUES (0, 'boulanger')"
			if err := aborter.QueueStmtForAbortion(
				insertStmt, 1 /* abortCount */, false, /* willBeRetriedIbid */
			); err != nil {
				t.Fatal(err)
			}
			if _, err := sqlDB.Exec("BEGIN"); err != nil {
				t.Fatal(err)
			}
			if tc.retryIntent {
				if _, err := sqlDB.Exec("SAVEPOINT cockroach_restart"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := sqlDB.Exec(insertStmt); err != nil {
				t.Fatal(err)
			}
			if _, err := sqlDB.Exec("COMMIT"); !testutils.IsError(err, "pq: restart transaction") {
				t.Fatalf("unexpected error: %v", err)
			}

			// Check that we can start another txn on the (one and only) connection.
			if _, err := sqlDB.Exec("BEGIN"); err != nil {
				t.Fatal(err)
			}
			// Check that we don't see any rows, so the previous txn was rolled back.
			rows, err := sqlDB.Query("SELECT * FROM t.public.test")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if rows.Next() {
				var k int
				var v string
				err := rows.Scan(&k, &v)
				t.Fatalf("found unexpected row: %d %s, %v", k, v, err)
			}
			if _, err := sqlDB.Exec("END"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestRollbackInRestartWait ensures that a ROLLBACK while the txn is in the
// RetryWait state works.
func TestRollbackInRestartWait(t *testing.T) {
	defer leaktest.AfterTest(t)()

	aborter := NewTxnAborter()
	defer aborter.Close(t)
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLExecutor = aborter.executorKnobs()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	{
		pgURL, cleanup := sqlutils.PGUrl(t, s.ServingAddr(), "TestRollbackInRestartWait", url.User(security.RootUser))
		defer cleanup()
		if err := aborter.Init(pgURL); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	// Set up error injection that causes retries.
	const insertStmt = "INSERT INTO t.public.test(k, v) VALUES (0, 'boulanger')"
	if err := aborter.QueueStmtForAbortion(
		insertStmt, 1 /* abortCount */, false, /* willBeRetriedIbid */
	); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("SAVEPOINT cockroach_restart"); err != nil {
		t.Fatal(err)
	}
	// Run a batch of statements to move the txn out of the AutoRetry state,
	// otherwise the INSERT below would be automatically retried.
	if _, err := tx.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(insertStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("RELEASE SAVEPOINT cockroach_restart"); !testutils.IsError(
		err, "pq: restart transaction") {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// TestUnexpectedStatementInRestartWait ensures that a statement other than
// ROLLBACK [TO SAVEPOINT] while the txn is in the RetryWait state terminates
// the transaction. More importantly than the state in which the transaction
// transitions when this happens is that this test prevents a regression of
// #15412, whereby the server would crash in this situation.
func TestUnexpectedStatementInRestartWait(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec("SAVEPOINT cockroach_restart"); err != nil {
		t.Fatal(err)
	}
	// Run a batch of statements to move the txn out of the AutoRetry state,
	// otherwise the SELECT below would be automatically retried.
	if _, err := tx.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(
		"SELECT CRDB_INTERNAL.FORCE_RETRY('1s':::INTERVAL)"); !testutils.IsError(
		err, `forced by crdb_internal\.force_retry\(\)`) {
		t.Fatal(err)
	}
	var state string
	if err := tx.QueryRow("SHOW TRANSACTION STATUS").Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "RestartWait" {
		t.Fatalf("expected state %s, got: %s", "RestartWait", state)
	}

	if _, err := tx.Exec("SELECT 1"); !testutils.IsError(err,
		`pq: Expected "ROLLBACK TO SAVEPOINT COCKROACH_RESTART": `+
			"current transaction is aborted, commands ignored until end of transaction block") {
		t.Fatal(err)
	}
	if err := tx.QueryRow("SHOW TRANSACTION STATUS").Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "Aborted" {
		t.Fatalf("expected state %s, got: %s", "Aborted", state)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// TestNonRetryableError verifies that a non-retryable error is propagated to the client.
func TestNonRetryableError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, cmdFilters := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	testKey := []byte("test_key")
	hitError := false
	cleanupFilter := cmdFilters.AppendFilter(
		func(args storagebase.FilterArgs) *roachpb.Error {
			if req, ok := args.Req.(*roachpb.ScanRequest); ok {
				if bytes.Contains(req.Key, testKey) && !client.TestingIsRangeLookupRequest(req) {
					hitError = true
					return roachpb.NewErrorWithTxn(fmt.Errorf("testError"), args.Hdr.Txn)
				}
			}
			return nil
		}, false)
	defer cleanupFilter()

	// We need to do everything on one connection as we'll want to observe the
	// connection state after a COMMIT.
	sqlDB.SetMaxOpenConns(1)
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k TEXT PRIMARY KEY, v TEXT);
INSERT INTO t.public.test (k, v) VALUES ('test_key', 'test_val');
SELECT * from t.public.test WHERE k = 'test_key';
`); !testutils.IsError(err, "pq: testError") {
		t.Errorf("unexpected error %v", err)
	}
	if !hitError {
		t.Errorf("expected to hit error, but it didn't happen")
	}
}

// Verifies that an expired lease is released and a new lease is acquired on transaction
// restart.
//
// This test triggers the above scenario by making ReadWithinUncertaintyIntervalError advance
// the clock, so that the transaction timestamp exceeds the deadline of the EndTransactionRequest.
func TestReacquireLeaseOnRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	advancement := 2 * base.DefaultTableDescriptorLeaseDuration

	var cmdFilters tests.CommandFilters
	cmdFilters.AppendFilter(tests.CheckEndTransactionTrigger, true)

	var clockUpdate int32
	testKey := []byte("test_key")
	testingKnobs := &storage.StoreTestingKnobs{
		EvalKnobs: batcheval.TestingKnobs{
			TestingEvalFilter: cmdFilters.RunFilters,
		},
		DisableMaxOffsetCheck: true,
		ClockBeforeSend: func(c *hlc.Clock, ba roachpb.BatchRequest) {
			if atomic.LoadInt32(&clockUpdate) > 0 {
				return
			}

			// Hack to advance the transaction timestamp on a transaction restart.
			for _, union := range ba.Requests {
				if req, ok := union.GetInner().(*roachpb.ScanRequest); ok {
					if bytes.Contains(req.Key, testKey) && !client.TestingIsRangeLookupRequest(req) {
						atomic.AddInt32(&clockUpdate, 1)
						now := c.Now()
						now.WallTime += advancement.Nanoseconds()
						c.Update(now)
						break
					}
				}
			}
		},
	}

	params, _ := tests.CreateTestServerParams()
	params.Knobs.Store = testingKnobs
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	var restartDone int32
	cleanupFilter := cmdFilters.AppendFilter(
		func(args storagebase.FilterArgs) *roachpb.Error {
			// Allow two restarts so that the auto retry on the first uncertainty
			// interval error also fails.
			if atomic.LoadInt32(&restartDone) > 1 {
				return nil
			}

			if req, ok := args.Req.(*roachpb.ScanRequest); ok {
				if bytes.Contains(req.Key, testKey) && !client.TestingIsRangeLookupRequest(req) {
					atomic.AddInt32(&restartDone, 1)
					// Return ReadWithinUncertaintyIntervalError to update the transaction timestamp on retry.
					txn := args.Hdr.Txn
					txn.ResetObservedTimestamps()
					now := s.Clock().Now()
					txn.UpdateObservedTimestamp(s.(*server.TestServer).Gossip().NodeID.Get(), now)
					return roachpb.NewErrorWithTxn(roachpb.NewReadWithinUncertaintyIntervalError(now, now, txn), txn)
				}
			}
			return nil
		}, false)
	defer cleanupFilter()

	sqlDB.SetMaxOpenConns(1)
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k TEXT PRIMARY KEY, v TEXT);
INSERT INTO t.public.test (k, v) VALUES ('test_key', 'test_val');
`); err != nil {
		t.Fatal(err)
	}
	// Acquire the lease and enable the auto-retry. The first read attempt will trigger ReadWithinUncertaintyIntervalError
	// and advance the transaction timestamp. The transaction timestamp will exceed the lease expiration
	// time, and the second read attempt will re-acquire the lease.
	if _, err := sqlDB.Exec(`
SELECT * from t.public.test WHERE k = 'test_key';
`); err != nil {
		t.Fatal(err)
	}

	if u := atomic.LoadInt32(&clockUpdate); u != 1 {
		t.Errorf("expected exacltly one clock update, but got %d", u)
	}
	if u := atomic.LoadInt32(&restartDone); u != 2 {
		t.Errorf("expected exactly two restarts, but got %d", u)
	}
}

// Verifies that the uncommitted descriptor cache is flushed on a txn restart.
//
// This test triggers the above scenario by triggering a restart by returning
// ReadWithinUncertaintyIntervalError on the first transaction attempt.
func TestFlushUncommitedDescriptorCacheOnRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var cmdFilters tests.CommandFilters
	cmdFilters.AppendFilter(tests.CheckEndTransactionTrigger, true)
	testKey := []byte("test_key")
	testingKnobs := &storage.StoreTestingKnobs{
		EvalKnobs: batcheval.TestingKnobs{
			TestingEvalFilter: cmdFilters.RunFilters,
		},
	}

	params, _ := tests.CreateTestServerParams()
	params.Knobs.Store = testingKnobs
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	var restartDone int32
	cleanupFilter := cmdFilters.AppendFilter(
		func(args storagebase.FilterArgs) *roachpb.Error {
			if atomic.LoadInt32(&restartDone) > 0 {
				return nil
			}

			if req, ok := args.Req.(*roachpb.ScanRequest); ok {
				if bytes.Contains(req.Key, testKey) && !client.TestingIsRangeLookupRequest(req) {
					atomic.AddInt32(&restartDone, 1)
					// Return ReadWithinUncertaintyIntervalError.
					txn := args.Hdr.Txn
					txn.ResetObservedTimestamps()
					now := s.Clock().Now()
					txn.UpdateObservedTimestamp(s.(*server.TestServer).Gossip().NodeID.Get(), now)
					return roachpb.NewErrorWithTxn(roachpb.NewReadWithinUncertaintyIntervalError(now, now, txn), txn)
				}
			}
			return nil
		}, false)
	defer cleanupFilter()

	sqlDB.SetMaxOpenConns(1)
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k TEXT PRIMARY KEY, v TEXT);
INSERT INTO t.public.test (k, v) VALUES ('test_key', 'test_val');
`); err != nil {
		t.Fatal(err)
	}
	// Read from a table, rename it, and then read from the table to trigger
	// the retry. On the second attempt the first read from the table should
	// not see the uncommitted renamed table.
	if _, err := sqlDB.Exec(`
BEGIN;
SELECT * from t.public.test WHERE k = 'foobar';
ALTER TABLE t.public.test RENAME TO t.public.foo;
SELECT * from t.public.foo WHERE k = 'test_key';
COMMIT;
`); err != nil {
		t.Fatal(err)
	}

	if u := atomic.LoadInt32(&restartDone); u != 1 {
		t.Errorf("expected exactly one restart, but got %d", u)
	}
}

// Test that if as part of a transaction A we receive a retryable error intended
// for a different transaction B, we don't retry transaction A.
func TestRetryableErrorForWrongTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	bogusTxnID := "deadb33f-baaa-aaaa-aaaa-aaaaaaaaaaad"

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k TEXT PRIMARY KEY, v TEXT);
INSERT INTO t.public.test (k, v) VALUES ('test_key', 'test_val');
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// We're going to use FORCE_RETRY() to generate an error for a different
	// transaction than the one we initiate.
	_, err = tx.Exec(`SELECT CRDB_INTERNAL.FORCE_RETRY('500ms':::INTERVAL, $1)`, bogusTxnID)
	if isRetryableErr(err) {
		t.Fatalf("expected non-retryable error, got: %s", err)
	}
	if !testutils.IsError(err, "pq: retryable error from another txn.* forced by crdb_internal.force_retry()") {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// Test that retryable errors are handled properly through DistSQL.
func TestDistSQLRetryableError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// One of the rows in the table.
	targetKey := roachpb.Key("\273\211\212")

	restarted := true

	tc := serverutils.StartTestCluster(t, 3, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
				Knobs: base.TestingKnobs{
					Store: &storage.StoreTestingKnobs{
						EvalKnobs: batcheval.TestingKnobs{
							TestingEvalFilter: func(fArgs storagebase.FilterArgs) *roachpb.Error {
								_, ok := fArgs.Req.(*roachpb.ScanRequest)
								if ok && fArgs.Req.Header().Key.Equal(targetKey) && fArgs.Hdr.Txn.Epoch == 0 {
									restarted = true
									err := roachpb.NewReadWithinUncertaintyIntervalError(
										fArgs.Hdr.Timestamp, /* readTS */
										hlc.Timestamp{},
										nil)
									errTxn := fArgs.Hdr.Txn.Clone()
									errTxn.UpdateObservedTimestamp(roachpb.NodeID(2), hlc.Timestamp{})
									pErr := roachpb.NewErrorWithTxn(err, &errTxn)
									pErr.OriginNode = 2
									return pErr
								}

								return nil
							},
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.TODO())

	db := tc.ServerConn(0)
	sqlutils.CreateTable(t, db, "t",
		"num INT PRIMARY KEY",
		3, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn))

	// We're going to split one of the tables, but node 4 is unaware of this.
	_, err := db.Exec(fmt.Sprintf(`
	ALTER TABLE "t" SPLIT AT VALUES (1), (2), (3);
	ALTER TABLE "t" TESTING_RELOCATE VALUES (ARRAY[%d], 1), (ARRAY[%d], 2), (ARRAY[%d], 3);
	`,
		tc.Server(1).GetFirstStoreID(),
		tc.Server(0).GetFirstStoreID(),
		tc.Server(2).GetFirstStoreID()))
	if err != nil {
		t.Fatal(err)
	}

	db.SetMaxOpenConns(1)

	if _, err := db.Exec("SET DISTSQL = ON"); err != nil {
		t.Fatal(err)
	}

	// Test that a stand-alone statement is retried by the Executor.
	if _, err := db.Exec("SELECT COUNT(1) FROM t"); err != nil {
		t.Fatal(err)
	}
	if !restarted {
		t.Fatalf("expected the EvalFilter to restart the txn, but it didn't")
	}

	// Test that a command that can't be retried automatically generates an error
	// with the correct code.
	restarted = false

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	// Run a batch of statements to move the txn out of the "AutoRetry" state.
	if _, err := txn.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}

	// Let's make sure that DISTSQL will actually be used.
	row := txn.QueryRow(`SELECT "Automatic" FROM [EXPLAIN (DISTSQL) SELECT COUNT(1) FROM t]`)
	var automatic bool
	if err := row.Scan(&automatic); err != nil {
		t.Fatal(err)
	}
	if !automatic {
		t.Fatal("DISTSQL not used for test's query")
	}

	_, err = txn.Exec("SELECT COUNT(1) FROM t")
	if !restarted {
		t.Fatalf("expected the EvalFilter to restart the txn, but it didn't")
	}
	if err == nil {
		t.Fatal("expected retryable error")
	}
	if !isRetryableErr(err) {
		t.Fatalf("expected retryable error, got: %s", err)
	}

	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// TestRollbackToSavepointFromUnusualStates tests that issuing a ROLLBACK TO
// SAVEPOINT from a non-retryable state works, and that the transaction that it
// opens has the same attributes as the existing one.
func TestRollbackToSavepointFromUnusualStates(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	checkState := func(tx *gosql.Tx, ts time.Time) {
		r := tx.QueryRow("SHOW TRANSACTION ISOLATION LEVEL")
		var iso string
		var pri string
		if err := r.Scan(&iso); err != nil {
			t.Fatal(err)
		} else {
			if iso != "snapshot" {
				t.Errorf("Expected snapshot, got: %s", iso)
			}
		}
		r = tx.QueryRow("SHOW TRANSACTION PRIORITY")
		if err := r.Scan(&pri); err != nil {
			t.Fatal(err)
		} else {
			if pri != "high" {
				t.Errorf("Expected high, got: %s", pri)
			}
		}
	}

	// ROLLBACK TO SAVEPOINT with a wrong name
	_, err := sqlDB.Exec("ROLLBACK TO SAVEPOINT foo")
	if !testutils.IsError(err, "SAVEPOINT not supported except for COCKROACH_RESTART") {
		t.Fatalf("unexpected error: %v", err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("SET TRANSACTION ISOLATION LEVEL SNAPSHOT"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("SET TRANSACTION PRIORITY HIGH"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("SAVEPOINT cockroach_restart"); err != nil {
		t.Fatal(err)
	}

	var ts time.Time
	r := tx.QueryRow("SELECT NOW()")
	if err := r.Scan(&ts); err != nil {
		t.Fatal(err)
	}

	checkState(tx, ts)

	// ROLLBACK TO SAVEPOINT from an Open txn should work.
	if _, err := tx.Exec("ROLLBACK TO SAVEPOINT cockroach_restart"); err != nil {
		t.Fatal(err)
	}
	checkState(tx, ts)

	// ROLLBACK TO SAVEPOINT from an Aborted txn should work.
	if _, err := tx.Exec("BOGUS SQL STATEMENT"); !testutils.IsError(err, `syntax error at or near "bogus"`) {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := tx.Exec("ROLLBACK TO SAVEPOINT cockroach_restart"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	checkState(tx, ts)

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// Test that, if we'd otherwise perform an auto-retry but results for the
// current txn have already been streamed to the client, we don't do the
// auto-restart.
func TestTxnAutoRetriesDisabledAfterResultsHaveBeenSentToClient(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	tests := []struct {
		name                              string
		autoCommit                        bool
		clientDirectedRetry               bool
		expectedTxnStateAfterRetriableErr sql.TxnStateEnum
	}{
		{
			name:                              "client_directed_retries",
			clientDirectedRetry:               true,
			expectedTxnStateAfterRetriableErr: sql.RestartWait,
		},
		{
			name:                              "no_client_directed_retries",
			clientDirectedRetry:               false,
			expectedTxnStateAfterRetriableErr: sql.Aborted,
		},
		{
			name:                              "autocommit",
			autoCommit:                        true,
			expectedTxnStateAfterRetriableErr: sql.NoTxn,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Cleanup the connection state after each test so the next one can run
			// statements.
			// TODO(andrei): Once we're on go 1.9, this test should use the new
			// db.Conn() method to tie each test to a connection; then this cleanup
			// wouldn't be necessary. Also, the test is currently technically
			// incorrect, as there's no guarantee that the state check at the end will
			// happen on the right connection.
			defer func() {
				if tc.autoCommit {
					// No cleanup necessary.
					return
				}
				if _, err := sqlDB.Exec("ROLLBACK"); err != nil {
					t.Fatal(err)
				}
			}()

			var savepoint string
			if tc.clientDirectedRetry {
				savepoint = "SAVEPOINT cockroach_restart;"
			}

			var prefix, suffix string
			if !tc.autoCommit {
				prefix = "BEGIN; " + savepoint
				suffix = "COMMIT;"
			}

			// We'll run a statement that produces enough results to overflow the
			// buffers and start streaming results to the client before the retriable
			// error is injected. We do this through a single statement (a UNION)
			// instead of two separate statements in order to support the autoCommit
			// test which needs a single statement.
			sql := fmt.Sprintf(`
				%s
				SELECT generate_series(1, 10000)
				UNION ALL
				SELECT crdb_internal.force_retry('1s');
				%s`,
				prefix, suffix)
			_, err := sqlDB.Exec(sql)
			if !isRetryableErr(err) {
				t.Fatalf("expected retriable error, got: %v", err)
			}
			var state string
			if err := sqlDB.QueryRow("SHOW TRANSACTION STATUS").Scan(&state); err != nil {
				t.Fatal(err)
			}
			if expStateStr := tc.expectedTxnStateAfterRetriableErr.String(); state != expStateStr {
				t.Fatalf("expected state %s, got: %s", expStateStr, state)
			}
		})
	}
}
