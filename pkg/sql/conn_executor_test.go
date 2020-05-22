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
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx"
	"github.com/lib/pq"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
)

func TestAnonymizeStatementsForReporting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const stmt = `
INSERT INTO sensitive(super, sensible) VALUES('that', 'nobody', 'must', 'see');

select * from crdb_internal.node_runtime_info;
`

	t.Run("unsafe", func(t *testing.T) {
		rUnsafe := "i'm not safe"
		safeErr := sql.AnonymizeStatementsForReporting("testing", stmt, rUnsafe)

		const expMessage = "panic: i'm not safe"
		actMessage := safeErr.Error()
		if actMessage != expMessage {
			t.Errorf("wanted: %s\ngot: %s", expMessage, actMessage)
		}

		const expSafeRedactedMessage = `...conn_executor_test.go:NN: <*errors.errorString>
wrapper: <*safedetails.withSafeDetails>
(more details:)
panic: %v
-- arg 1: <string>
wrapper: <*withstack.withStack>
(more details:)
github.com/cockroachdb/cockroach/pkg/sql_test.TestAnonymizeStatementsForReporting.func1
	...conn_executor_test.go:NN
testing.tRunner
	...testing.go:NN
runtime.goexit
	...asm_amd64.s:NN
wrapper: <*safedetails.withSafeDetails>
(more details:)
panic while %s %d statements: %s
-- arg 1: testing
-- arg 2: 2
-- arg 3: INSERT INTO _(_, _) VALUES (_, _, __more2__); SELECT * FROM _._`
		actSafeRedactedMessage := fileref.ReplaceAllString(errors.Redact(safeErr), "...$2:NN")
		if actSafeRedactedMessage != expSafeRedactedMessage {
			diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
				A:        difflib.SplitLines(expSafeRedactedMessage),
				B:        difflib.SplitLines(actSafeRedactedMessage),
				FromFile: "Expected",
				FromDate: "",
				ToFile:   "Actual",
				ToDate:   "",
				Context:  1,
			})
			t.Errorf("Diff:\n%s", diff)
		}

	})

	t.Run("safe", func(t *testing.T) {
		rSafe := log.Safe("something safe")
		safeErr := sql.AnonymizeStatementsForReporting("testing", stmt, rSafe)

		const expMessage = "panic: something safe"
		actMessage := safeErr.Error()
		if actMessage != expMessage {
			t.Errorf("wanted: %s\ngot: %s", expMessage, actMessage)
		}

		const expSafeSafeMessage = `...conn_executor_test.go:NN: <*errors.errorString>
wrapper: <*safedetails.withSafeDetails>
(more details:)
panic: %v
-- arg 1: something safe
wrapper: <*withstack.withStack>
(more details:)
github.com/cockroachdb/cockroach/pkg/sql_test.TestAnonymizeStatementsForReporting.func2
	...conn_executor_test.go:NN
testing.tRunner
	...testing.go:NN
runtime.goexit
	...asm_amd64.s:NN
wrapper: <*safedetails.withSafeDetails>
(more details:)
panic while %s %d statements: %s
-- arg 1: testing
-- arg 2: 2
-- arg 3: INSERT INTO _(_, _) VALUES (_, _, __more2__); SELECT * FROM _._`
		actSafeSafeMessage := fileref.ReplaceAllString(errors.Redact(safeErr), "...$2:NN")
		if actSafeSafeMessage != expSafeSafeMessage {
			diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
				A:        difflib.SplitLines(expSafeSafeMessage),
				B:        difflib.SplitLines(actSafeSafeMessage),
				FromFile: "Expected",
				FromDate: "",
				ToFile:   "Actual",
				ToDate:   "",
				Context:  1,
			})
			t.Errorf("Diff:\n%s", diff)
		}
	})
}

var fileref = regexp.MustCompile(`((?:[a-zA-Z0-9\._@-]*/)*)([a-zA-Z0-9._@-]*\.(?:go|s)):\d+`)

// Test that a connection closed abruptly while a SQL txn is in progress results
// in that txn being rolled back.
//
// TODO(andrei): This test terminates a client connection by calling Close() on
// a driver.Conn(), which sends a MsgTerminate. We should also have a test that
// closes the connection more abruptly than that.
func TestSessionFinishRollsBackTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	aborter := NewTxnAborter()
	defer aborter.Close(t)
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLExecutor = aborter.executorKnobs()
	s, mainDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	{
		pgURL, cleanup := sqlutils.PGUrl(
			t, s.ServingSQLAddr(), "TestSessionFinishRollsBackTxn", url.User(security.RootUser))
		defer cleanup()
		if err := aborter.Init(pgURL); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := mainDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	// We're going to test the rollback of transactions left in various states
	// when the connection closes abruptly.
	// For the state CommitWait, there's no actual rollback we can test for (since
	// the kv-level transaction has already been committed). But we still
	// exercise this state to check that the server doesn't crash (which used to
	// happen - #9879).
	tests := []string{"Open", "Aborted", "CommitWait"}
	for _, state := range tests {
		t.Run(state, func(t *testing.T) {
			// Create a low-level lib/pq connection so we can close it at will.
			pgURL, cleanupDB := sqlutils.PGUrl(
				t, s.ServingSQLAddr(), state, url.User(security.RootUser))
			defer cleanupDB()
			c, err := pq.Open(pgURL.String())
			if err != nil {
				t.Fatal(err)
			}
			connClosed := false
			defer func() {
				if connClosed {
					return
				}
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
			}()

			ctx := context.Background()
			conn := c.(driver.ConnBeginTx)
			txn, err := conn.BeginTx(ctx, driver.TxOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tx := txn.(driver.ExecerContext)
			if _, err := tx.ExecContext(ctx, "SET TRANSACTION PRIORITY NORMAL", nil); err != nil {
				t.Fatal(err)
			}

			if state == "CommitWait" {
				if _, err := tx.ExecContext(ctx, "SAVEPOINT cockroach_restart", nil); err != nil {
					t.Fatal(err)
				}
			}

			insertStmt := "INSERT INTO t.public.test(k, v) VALUES (1, 'a')"
			if state == "RestartWait" {
				// To get a txn in RestartWait, we'll use an aborter.
				if err := aborter.QueueStmtForAbortion(
					insertStmt, 1 /* restartCount */, false /* willBeRetriedIbid */); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := tx.ExecContext(ctx, insertStmt, nil); err != nil {
				t.Fatal(err)
			}

			if err := aborter.VerifyAndClear(); err != nil {
				t.Fatal(err)
			}

			if state == "CommitWait" {
				_, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT cockroach_restart", nil)
				if state == "CommitWait" {
					if err != nil {
						t.Fatal(err)
					}
				} else if !testutils.IsError(err, "pq: restart transaction:.*") {
					t.Fatal(err)
				}
			}

			// Abruptly close the connection.
			connClosed = true
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}

			// Check that the txn we had above was rolled back. We do this by reading
			// after the preceding txn and checking that we don't get an error and
			// that we haven't been blocked by intents (we can't exactly test that we
			// haven't been blocked but we assert that the query didn't take too
			// long).
			// We do the read in an explicit txn so that automatic retries don't hide
			// any errors.
			// TODO(andrei): Figure out a better way to test for non-blocking.
			// Use a trace when the client-side tracing story gets good enough.
			// There's a bit of difficulty because the cleanup is async.
			txCheck, err := mainDB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			// Run check at low priority so we don't push the previous transaction and
			// fool ourselves into thinking it had been rolled back.
			if _, err := txCheck.Exec("SET TRANSACTION PRIORITY LOW"); err != nil {
				t.Fatal(err)
			}
			ts := timeutil.Now()
			var count int
			if err := txCheck.QueryRow("SELECT count(1) FROM t.test").Scan(&count); err != nil {
				t.Fatal(err)
			}
			// CommitWait actually committed, so we'll need to clean up.
			if state != "CommitWait" {
				if count != 0 {
					t.Fatalf("expected no rows, got: %d", count)
				}
			} else {
				if _, err := txCheck.Exec("DELETE FROM t.test"); err != nil {
					t.Fatal(err)
				}
			}
			if err := txCheck.Commit(); err != nil {
				t.Fatal(err)
			}
			if d := timeutil.Since(ts); d > time.Second {
				t.Fatalf("Looks like the checking tx was unexpectedly blocked. "+
					"It took %s to commit.", d)
			}

		})
	}
}

// Test two things about non-retriable errors happening when the Executor does
// an "autoCommit" (i.e. commits the KV txn after running an implicit
// transaction):
// 1) The error is reported to the client.
// 2) The error doesn't leave the session in the Aborted state. After running
// implicit transactions, the state should always be NoTxn, regardless of any
// errors.
func TestNonRetriableErrorOnAutoCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	query := "SELECT 42"

	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				BeforeAutoCommit: func(ctx context.Context, stmt string) error {
					if strings.Contains(stmt, query) {
						return fmt.Errorf("injected autocommit error")
					}
					return nil
				},
			},
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	sqlDB.SetMaxOpenConns(1)

	if _, err := sqlDB.Exec(query); !testutils.IsError(err, "injected") {
		t.Fatalf("expected injected error, got: %v", err)
	}

	var state string
	if err := sqlDB.QueryRow("SHOW TRANSACTION STATUS").Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "NoTxn" {
		t.Fatalf("expected state %s, got: %s", "NoTxn", state)
	}
}

// Test that, if a ROLLBACK statement encounters an error, the error is not
// returned to the client and the session state is transitioned to NoTxn.
func TestErrorOnRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const targetKeyString string = "/Table/53/1/1/0"
	var injectedErr int64

	// We're going to inject an error into our EndTxn.
	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingProposalFilter: func(fArgs kvserverbase.ProposalFilterArgs) *roachpb.Error {
					if !fArgs.Req.IsSingleRequest() {
						return nil
					}
					req := fArgs.Req.Requests[0]
					etReq, ok := req.GetInner().(*roachpb.EndTxnRequest)
					// We only inject the error once. Turns out that during the
					// life of the test there's two EndTxns being sent - one is
					// the direct result of the test's call to tx.Rollback(),
					// the second is sent by the TxnCoordSender - indirectly
					// triggered by the fact that, on the server side, the
					// transaction's context gets canceled at the SQL layer.
					if ok &&
						etReq.Header().Key.String() == targetKeyString &&
						atomic.LoadInt64(&injectedErr) == 0 {

						atomic.StoreInt64(&injectedErr, 1)
						return roachpb.NewErrorf("test injected error")
					}
					return nil
				},
			},
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Perform a write so that the EndTxn we're going to send doesn't get elided.
	if _, err := tx.ExecContext(ctx, "INSERT INTO t.test(k, v) VALUES (1, 'abc')"); err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var state string
	if err := sqlDB.QueryRow("SHOW TRANSACTION STATUS").Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "NoTxn" {
		t.Fatalf("expected state %s, got: %s", "NoTxn", state)
	}

	if atomic.LoadInt64(&injectedErr) == 0 {
		t.Fatal("test didn't inject the error; it must have failed to find " +
			"the EndTxn with the expected key")
	}
}

func TestHalloweenProblemAvoidance(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Populate a sufficiently large number of rows. We want at least as
	// many rows as an insert can batch in its output buffer (to force a
	// buffer flush), plus as many rows as a fetcher can fetch at once
	// (to force a read buffer update), plus some more.
	//
	// Instead of customizing the working set size of the test up to the
	// default settings for the SQL package, we scale down the config
	// of the SQL package to the test. The reason for this is that
	// race-enable builds are very slow and the default batch sizes
	// would cause the test duration to exceed the timeout.
	//
	// We are also careful to override these defaults before starting
	// the server, so as to not risk updating them concurrently with
	// some background SQL activity.
	const smallerKvBatchSize = 10
	defer row.TestingSetKVBatchSize(smallerKvBatchSize)()
	const smallerInsertBatchSize = 5
	defer sql.TestingSetInsertBatchSize(smallerInsertBatchSize)()
	numRows := smallerKvBatchSize + smallerInsertBatchSize + 10

	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x FLOAT);
`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		`INSERT INTO t.test(x) SELECT generate_series(1, $1)::FLOAT`,
		numRows); err != nil {
		t.Fatal(err)
	}

	// Now slightly modify the values in duplicate rows.
	// We choose a float +0.1 to ensure that none of the derived
	// values become duplicate of already-present values.
	if _, err := db.Exec(`
INSERT INTO t.test(x)
    -- the if ensures that no row is processed two times.
SELECT IF(x::INT::FLOAT = x,
          x,
          crdb_internal.force_error(
             'NOOPE', 'insert saw its own writes: ' || x::STRING || ' (it is halloween today)')::FLOAT)
       + 0.1
  FROM t.test
`); err != nil {
		t.Fatal(err)
	}

	// Finally verify that no rows has been operated on more than once.
	row := db.QueryRow(`SELECT count(DISTINCT x) FROM t.test`)
	var cnt int
	if err := row.Scan(&cnt); err != nil {
		t.Fatal(err)
	}

	if cnt != 2*numRows {
		t.Fatalf("expected %d rows in final table, got %d", 2*numRows, cnt)
	}
}

func TestAppNameStatisticsInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	// Prepare a session with a custom application name.
	pgURL := url.URL{
		Scheme:   "postgres",
		User:     url.User(security.RootUser),
		Host:     s.ServingSQLAddr(),
		RawQuery: "sslmode=disable&application_name=mytest",
	}
	rawSQL, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rawSQL.Close()
	sqlDB := sqlutils.MakeSQLRunner(rawSQL)

	// Issue a query to be registered in stats.
	sqlDB.Exec(t, "SELECT version()")

	// Verify the query shows up in stats.
	rows := sqlDB.Query(t, "SELECT application_name, key FROM crdb_internal.node_statement_statistics")
	defer rows.Close()

	counts := map[string]int{}
	for rows.Next() {
		var appName, key string
		if err := rows.Scan(&appName, &key); err != nil {
			t.Fatal(err)
		}
		counts[appName+":"+key]++
	}
	if counts["mytest:SELECT version()"] == 0 {
		t.Fatalf("query was not counted properly: %+v", counts)
	}
}

func TestQueryProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rows, kvBatchSize = 1000, 50

	defer rowexec.TestingSetScannedRowProgressFrequency(rows / 60)()
	defer row.TestingSetKVBatchSize(kvBatchSize)()

	const expectedScans = (rows / 2) /* WHERE restricts scan to 1/2 */ / kvBatchSize
	const stallAfterScans = expectedScans/2 + 1

	var queryRunningAtomic, scannedBatchesAtomic int64
	stalled, unblock := make(chan struct{}), make(chan struct{})

	tableKey := keys.SystemSQLCodec.TablePrefix(keys.MinNonPredefinedUserDescID + 1)
	tableSpan := roachpb.Span{Key: tableKey, EndKey: tableKey.PrefixEnd()}

	// Install a store filter which, if queryRunningAtomic is 1, will count scan
	// requests issued to the test table and then, on the `stallAfterScans` one,
	// will stall the scan and in turn the query, so the test has a chance to
	// inspect the query progress. The filter signals the test that it has reached
	// the stall-point by closing the `stalled` ch and then waits for the test to
	// run its check(s) by receiving on the `unblock` channel (which the test can
	// then close once it has checked the progress).
	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(_ context.Context, req roachpb.BatchRequest) *roachpb.Error {
					if req.IsSingleRequest() {
						scan, ok := req.Requests[0].GetInner().(*roachpb.ScanRequest)
						if ok && tableSpan.ContainsKey(scan.Key) && atomic.LoadInt64(&queryRunningAtomic) == 1 {
							i := atomic.AddInt64(&scannedBatchesAtomic, 1)
							if i == stallAfterScans {
								close(stalled)
								t.Logf("stalling on scan %d at %s and waiting for test to unblock...", i, scan.Key)
								<-unblock
							}
						}
					}
					return nil
				},
			},
		},
	}
	s, rawDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(rawDB)

	db.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	db.Exec(t, `CREATE DATABASE t; CREATE TABLE t.test (x INT PRIMARY KEY);`)
	db.Exec(t, `INSERT INTO t.test SELECT generate_series(1, $1)::INT`, rows)
	db.Exec(t, `CREATE STATISTICS __auto__ FROM t.test`)
	const query = `SELECT count(*) FROM t.test WHERE x > $1 and x % 2 = 0`

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		// Ensure that after query execution, we've actually hit and closed the
		// stalled ch as expected.
		defer func() {
			select {
			case <-stalled: //stalled was closed as expected.
			default:
				panic("expected stalled to have been closed during execution")
			}
		}()
		atomic.StoreInt64(&queryRunningAtomic, 1)
		_, err := rawDB.ExecContext(ctx, query, rows/2)
		return err
	})

	t.Log("waiting for query to make progress...")
	<-stalled
	t.Log("query is now stalled. checking progress...")

	var progress string
	err := rawDB.QueryRow(`SELECT phase FROM [SHOW QUERIES] WHERE query LIKE 'SELECT count(*) FROM t.test%'`).Scan(&progress)

	// Unblock the KV requests first, regardless of what we found in the progress.
	close(unblock)
	require.NoError(t, g.Wait())

	if err != nil {
		t.Fatal(err)
	}
	// Although we know we've scanned ~50% of what we'll scan, exactly when the
	// meta makes its way back to the receiver vs when the progress is checked is
	// non-deterministic so we could see 47% done or 53% done, etc. To avoid being
	// flaky, we just make sure we see one of 4x% or 5x%
	require.Regexp(t, `executing \([45]\d\.`, progress)
}

// This test ensures that when in an explicit transaction, statement preparation
// uses the user's transaction and thus properly interacts with deadlock
// detection.
func TestPrepareInExplicitTransactionDoesNotDeadlock(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	testDB := sqlutils.MakeSQLRunner(sqlDB)
	testDB.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	testDB.Exec(t, "CREATE TABLE bar (i INT PRIMARY KEY)")

	tx1, err := sqlDB.Begin()
	require.NoError(t, err)

	tx2, err := sqlDB.Begin()
	require.NoError(t, err)

	// So now I really want to try to have a deadlock.

	_, err = tx1.Exec("ALTER TABLE foo ADD COLUMN j INT NOT NULL")
	require.NoError(t, err)

	_, err = tx2.Exec("ALTER TABLE bar ADD COLUMN j INT NOT NULL")
	require.NoError(t, err)

	// Now we want tx2 to get blocked on tx1 and stay blocked, then we want to
	// push tx1 above tx2 and have it get blocked in planning.
	errCh := make(chan error)
	go func() {
		_, err := tx2.Exec("ALTER TABLE foo ADD COLUMN k INT NOT NULL")
		errCh <- err
	}()
	select {
	case <-time.After(time.Millisecond):
	case err := <-errCh:
		t.Fatalf("expected the transaction to block, got %v", err)
	default:
	}

	// Read from foo so that we can push tx1 above tx2.
	testDB.Exec(t, "SELECT count(*) FROM foo")

	// Write into foo to push tx1
	_, err = tx1.Exec("INSERT INTO foo VALUES (1)")
	require.NoError(t, err)

	// Plan a query which will resolve bar during planning time, this would block
	// and deadlock if it were run on a new transaction.
	_, err = tx1.Prepare("SELECT NULL FROM [SHOW COLUMNS FROM bar] LIMIT 1")
	require.NoError(t, err)

	// Try to commit tx1. Either it should get a RETRY_SERIALIZABLE error or
	// tx2 should. Ensure that either one or both of them does.
	if tx1Err := tx1.Commit(); tx1Err == nil {
		// tx1 committed successfully, ensure tx2 failed.
		tx2ExecErr := <-errCh
		require.Regexp(t, "RETRY_SERIALIZABLE", tx2ExecErr)
		_ = tx2.Rollback()
	} else {
		require.Regexp(t, "RETRY_SERIALIZABLE", tx1Err)
		tx2ExecErr := <-errCh
		require.NoError(t, tx2ExecErr)
		if tx2CommitErr := tx2.Commit(); tx2CommitErr != nil {
			require.Regexp(t, "RETRY_SERIALIZABLE", tx2CommitErr)
		}
	}
}

// TestRetriableErrorDuringPrepare ensures that when preparing and using a new
// transaction, retriable errors are handled properly and do not propagate to
// the user's transaction.
func TestRetriableErrorDuringPrepare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const uniqueString = "'a very unique string'"
	var failed int64
	const numToFail = 2 // only fail on the first two attempts
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				BeforePrepare: func(ctx context.Context, stmt string, txn *kv.Txn) error {
					if strings.Contains(stmt, uniqueString) && atomic.AddInt64(&failed, 1) <= numToFail {
						return roachpb.NewTransactionRetryWithProtoRefreshError("boom",
							txn.ID(), *txn.TestingCloneTxn())
					}
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	testDB := sqlutils.MakeSQLRunner(sqlDB)
	testDB.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")

	stmt, err := sqlDB.Prepare("SELECT " + uniqueString)
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()
}

// This test ensures that when in an explicit transaction and statement
// preparation uses the user's transaction, errors during those planning queries
// are handled correctly.
func TestErrorDuringPrepareInExplicitTransactionPropagates(t *testing.T) {
	defer leaktest.AfterTest(t)()

	filter := newDynamicRequestFilter()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: filter.filter,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	testDB := sqlutils.MakeSQLRunner(sqlDB)
	testDB.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	testDB.Exec(t, "CREATE TABLE bar (i INT PRIMARY KEY)")

	// This test will create an explicit transaction that encounters an error on
	// a latter statement during planning of SHOW COLUMNS. The planning for this
	// SHOW COLUMNS will be run in the user's transaction. The test will inject
	// errors into the execution of that planning query and ensure that the user's
	// transaction state evolves appropriately.

	// Use pgx so that we can introspect error codes returned from cockroach.
	pgURL, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), "", url.User("root"))
	defer cleanup()
	conf, err := pgx.ParseConnectionString(pgURL.String())
	require.NoError(t, err)
	conn, err := pgx.Connect(conf)
	require.NoError(t, err)

	tx, err := conn.Begin()
	require.NoError(t, err)

	_, err = tx.Exec("SAVEPOINT cockroach_restart")
	require.NoError(t, err)

	// Do something with the user's transaction so that we'll use the user
	// transaction in the planning of the below `SHOW COLUMNS`.
	_, err = tx.Exec("INSERT INTO foo VALUES (1)")
	require.NoError(t, err)

	// Inject an error that will happen during planning.
	filter.setFilter(func(ctx context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		if ba.Txn == nil {
			return nil
		}
		if req, ok := ba.GetArg(roachpb.Get); ok {
			get := req.(*roachpb.GetRequest)
			_, tableID, err := keys.SystemSQLCodec.DecodeTablePrefix(get.Key)
			if err != nil || tableID != keys.NamespaceTableID {
				err = nil
				return nil
			}
			return roachpb.NewErrorWithTxn(
				roachpb.NewTransactionRetryError(roachpb.RETRY_REASON_UNKNOWN, "boom"), ba.Txn)
		}
		return nil
	})

	// Plan a query will get a restart error during planning.
	_, err = tx.Prepare("show_columns", "SELECT NULL FROM [SHOW COLUMNS FROM bar] LIMIT 1")
	require.Regexp(t,
		`restart transaction: TransactionRetryWithProtoRefreshError: TransactionRetryError: retry txn \(RETRY_REASON_UNKNOWN - boom\)`,
		err)
	var pgErr pgx.PgError
	require.True(t, errors.As(err, &pgErr))
	require.Equal(t, pgcode.SerializationFailure, pgErr.Code)

	// Clear the error producing filter, restart the transaction, and run it to
	// completion.
	filter.setFilter(nil)

	_, err = tx.Exec("ROLLBACK TO SAVEPOINT cockroach_restart")
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO foo VALUES (1)")
	require.NoError(t, err)
	_, err = tx.Prepare("show_columns", "SELECT NULL FROM [SHOW COLUMNS FROM bar] LIMIT 1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

// dynamicRequestFilter exposes a filter method which is a
// kvserverbase.ReplicaRequestFilter but can be set dynamically.
type dynamicRequestFilter struct {
	v atomic.Value
}

func newDynamicRequestFilter() *dynamicRequestFilter {
	f := &dynamicRequestFilter{}
	f.v.Store(kvserverbase.ReplicaRequestFilter(noopRequestFilter))
	return f
}

func (f *dynamicRequestFilter) setFilter(filter kvserverbase.ReplicaRequestFilter) {
	if filter == nil {
		f.v.Store(kvserverbase.ReplicaRequestFilter(noopRequestFilter))
	} else {
		f.v.Store(filter)
	}
}

// noopRequestFilter is a kvserverbase.ReplicaRequestFilter.
func (f *dynamicRequestFilter) filter(
	ctx context.Context, request roachpb.BatchRequest,
) *roachpb.Error {
	return f.v.Load().(kvserverbase.ReplicaRequestFilter)(ctx, request)
}

// noopRequestFilter is a kvserverbase.ReplicaRequestFilter that does nothing.
func noopRequestFilter(ctx context.Context, request roachpb.BatchRequest) *roachpb.Error {
	return nil
}
