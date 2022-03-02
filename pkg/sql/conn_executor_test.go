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

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/mutations"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgtest"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
)

func TestAnonymizeStatementsForReporting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := cluster.MakeTestingClusterSettings()
	vt, err := sql.NewVirtualSchemaHolder(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}

	const stmt1s = `
INSERT INTO sensitive(super, sensible) VALUES('that', 'nobody', 'must', 'see')
`
	stmt1, err := parser.ParseOne(stmt1s)
	if err != nil {
		t.Fatal(err)
	}

	rUnsafe := errors.New("some error")
	safeErr := sql.WithAnonymizedStatement(rUnsafe, stmt1.AST, vt)

	const expMessage = "some error"
	actMessage := safeErr.Error()
	if actMessage != expMessage {
		t.Errorf("wanted: %s\ngot: %s", expMessage, actMessage)
	}

	const expSafeRedactedMessage = `some error
(1) while executing: INSERT INTO _(_, _) VALUES ('_', '_', __more2__)
Wraps: (2) attached stack trace
  -- stack trace:
  | github.com/cockroachdb/cockroach/pkg/sql_test.TestAnonymizeStatementsForReporting
  | 	...conn_executor_test.go:NN
  | testing.tRunner
  | 	...testing.go:NN
  | runtime.goexit
  | 	...asm_scrubbed.s:NN
Wraps: (3) some error
Error types: (1) *safedetails.withSafeDetails (2) *withstack.withStack (3) *errutil.leafError`

	// Edit non-determinstic stack trace filenames from the message.
	actSafeRedactedMessage := strings.ReplaceAll(strings.ReplaceAll(fileref.ReplaceAllString(
		redact.Sprintf("%+v", safeErr).Redact().StripMarkers(), "...$2:NN"),
		"asm_arm64", "asm_scrubbed"),
		"asm_amd64", "asm_scrubbed")

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
	defer log.Scope(t).Close(t)
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
	defer log.Scope(t).Close(t)

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
	defer log.Scope(t).Close(t)

	// We can't get the tableID programmatically here.
	// The table id can be retrieved by doing.
	// CREATE DATABASE test;
	// CREATE TABLE test.t();
	// SELECT id FROM system.namespace WHERE name = 't' AND "parentID" != 1
	const targetKeyStringFmt string = "/Table/%d/1/1/0"
	var targetKeyString atomic.Value
	targetKeyString.Store("")
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
						etReq.Header().Key.String() == targetKeyString.Load().(string) &&
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

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v TEXT);
`)
	tableID := sqlutils.QueryTableID(t, sqlDB, "t", "public", "test")
	targetKeyString.Store(fmt.Sprintf(targetKeyStringFmt, tableID))

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
	defer log.Scope(t).Close(t)

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
	const smallerKvBatchSize = 5 // This is an approximation based on the TableReaderBatchBytesLimit below.
	const smallerInsertBatchSize = 5
	mutations.SetMaxBatchSizeForTests(smallerInsertBatchSize)
	defer mutations.ResetMaxBatchSizeForTests()
	numRows := smallerKvBatchSize + smallerInsertBatchSize + 10

	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	params.Knobs.DistSQL = &execinfra.TestingKnobs{
		TableReaderBatchBytesLimit: 10,
	}

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
	defer log.Scope(t).Close(t)

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
	defer log.Scope(t).Close(t)

	const rows = 1000
	defer rowexec.TestingSetScannedRowProgressFrequency(rows / 60)()

	// We'll do more than 6 scans because we set a low TableReaderBatchBytesLimit
	// below.
	const stallAfterScans = 6

	var queryRunningAtomic, scannedBatchesAtomic int64
	stalled, unblock := make(chan struct{}), make(chan struct{})

	// We'll populate the key for the knob after we create the table.
	var tableKey atomic.Value
	tableKey.Store(roachpb.Key(""))
	getTableKey := func() roachpb.Key { return tableKey.Load().(roachpb.Key) }
	spanFromKey := func(k roachpb.Key) roachpb.Span {
		return roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
	}
	getTableSpan := func() roachpb.Span { return spanFromKey(getTableKey()) }

	// Install a store filter which, if queryRunningAtomic is 1, will count scan
	// requests issued to the test table and then, on the `stallAfterScans` one,
	// will stall the scan and in turn the query, so the test has a chance to
	// inspect the query progress. The filter signals the test that it has reached
	// the stall-point by closing the `stalled` ch and then waits for the test to
	// run its check(s) by receiving on the `unblock` channel (which the test can
	// then close once it has checked the progress).
	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				// A low limit, to force many small scans such that we get progress
				// reports.
				TableReaderBatchBytesLimit: 1500,
			},
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(_ context.Context, req roachpb.BatchRequest) *roachpb.Error {
					if req.IsSingleRequest() {
						scan, ok := req.Requests[0].GetInner().(*roachpb.ScanRequest)
						if ok && getTableSpan().ContainsKey(scan.Key) && atomic.LoadInt64(&queryRunningAtomic) == 1 {
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

	// TODO(yuzefovich): the vectorized cfetcher doesn't emit metadata about
	// the progress nor do we have an infrastructure to emit such metadata at
	// the runtime (we can only propagate the metadata during the draining of
	// the flow which defeats the purpose of the progress meta), so we use the
	// old row-by-row engine in this test. We should fix that (#55758).
	db.Exec(t, `SET vectorize=off`)
	db.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	db.Exec(t, `CREATE DATABASE t; CREATE TABLE t.test (x INT PRIMARY KEY);`)
	db.Exec(t, `INSERT INTO t.test SELECT generate_series(1, $1)::INT`, rows)
	db.Exec(t, `CREATE STATISTICS __auto__ FROM t.test`)
	const query = `SELECT count(*) FROM t.test WHERE x > $1 and x % 2 = 0`

	// Invalidate the stats cache so that we can be sure to get the latest stats.
	var tableID descpb.ID
	ctx := context.Background()
	require.NoError(t, rawDB.QueryRow(`SELECT id FROM system.namespace WHERE name = 'test'`).Scan(&tableID))
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	execCfg.TableStatsCache.InvalidateTableStats(ctx, tableID)
	tableKey.Store(execCfg.Codec.TablePrefix(uint32(tableID)))

	ctx, cancel := context.WithCancel(ctx)
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
	require.Regexp(t, `executing \(..\...%\)`, progress)
}

// This test ensures that when in an explicit transaction, statement preparation
// uses the user's transaction and thus properly interacts with deadlock
// detection.
func TestPrepareInExplicitTransactionDoesNotDeadlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	defer log.Scope(t).Close(t)
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	filter := newDynamicRequestFilter()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: filter.filter,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

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
	conf, err := pgx.ParseConfig(pgURL.String())
	require.NoError(t, err)
	conn, err := pgx.ConnectConfig(ctx, conf)
	require.NoError(t, err)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "SAVEPOINT cockroach_restart")
	require.NoError(t, err)

	// Do something with the user's transaction so that we'll use the user
	// transaction in the planning of the below `SHOW COLUMNS`.
	_, err = tx.Exec(ctx, "INSERT INTO foo VALUES (1)")
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
	_, err = tx.Prepare(ctx, "show_columns", "SELECT NULL FROM [SHOW COLUMNS FROM bar] LIMIT 1")
	require.Regexp(t,
		`restart transaction: TransactionRetryWithProtoRefreshError: TransactionRetryError: retry txn \(RETRY_REASON_UNKNOWN - boom\)`,
		err)
	var pgErr = new(pgconn.PgError)
	require.True(t, errors.As(err, &pgErr))
	require.Equal(t, pgcode.SerializationFailure, pgcode.MakeCode(pgErr.Code))

	// Clear the error producing filter, restart the transaction, and run it to
	// completion.
	filter.setFilter(nil)

	_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT cockroach_restart")
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "INSERT INTO foo VALUES (1)")
	require.NoError(t, err)
	_, err = tx.Prepare(ctx, "show_columns", "SELECT NULL FROM [SHOW COLUMNS FROM bar] LIMIT 1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
}

// TestTrimFlushedStatements verifies that the conn executor trims the
// statements buffer once the corresponding results are returned to the user.
func TestTrimFlushedStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		countStmt = "SELECT count(*) FROM test"
		// stmtBufMaxLen is the maximum length the statement buffer should be during
		// execution of COUNT(*). This includes a SELECT COUNT(*) command as well
		// as a Sync command.
		stmtBufMaxLen = 2
	)
	// stmtBufLen is set to the length of the statement buffer after each SELECT
	// COUNT(*) execution.
	stmtBufLen := 0
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				AfterExecCmd: func(_ context.Context, cmd sql.Command, buf *sql.StmtBuf) {
					if strings.Contains(cmd.String(), countStmt) {
						// Only compare statement buffer length on SELECT COUNT(*) queries.
						stmtBufLen = buf.Len()
					}
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec("CREATE TABLE test (i int)")
	require.NoError(t, err)

	tx, err := sqlDB.Begin()
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := tx.Exec(countStmt)
		require.NoError(t, err)
		if stmtBufLen > stmtBufMaxLen {
			t.Fatalf("statement buffer grew to %d (> %d) after %dth execution", stmtBufLen, stmtBufMaxLen, i)
		}
	}
	require.NoError(t, tx.Commit())
}

// TestUnqualifiedIntSizeRace makes sure there is no data race using the
// default_int_size session variable during statement parsing.
// Regression test for https://github.com/cockroachdb/cockroach/issues/69451.
func TestUnqualifiedIntSizeRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
	})
	defer s.Stopper().Stop(ctx)

	// Connect to the cluster via the PGWire client.
	p, err := pgtest.NewPGTest(ctx, s.SQLAddr(), security.RootUser)
	require.NoError(t, err)

	require.NoError(t, p.SendOneLine(`Query {"String": "SET default_int_size = 8"}`))
	require.NoError(t, p.SendOneLine(`Query {"String": "SET default_int_size = 4"}`))
	require.NoError(t, p.SendOneLine(`Parse {"Query": "SELECT generate_series(1, 10)"}`))

	// wait for ready
	for i := 0; i < 2; i++ {
		until := pgtest.ParseMessages("ReadyForQuery")
		_, err = p.Until(false /* keepErrMsg */, until...)
		require.NoError(t, err)
	}
}

func TestTrimSuspendedPortals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		// select generates 10 rows of results which the test retrieves using ExecPortal
		selectStmt = "SELECT generate_series(1, 10)"

		// stmtBufMaxLen is the maximum length the statement buffer should be during
		// execution
		stmtBufMaxLen = 2

		// The name of the portal, used to get a handle on the statement buffer
		portalName = "C_1"
	)

	ctx := context.Background()

	var stmtBuff *sql.StmtBuf
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				// get a handle to the statement buffer during the Bind phase
				AfterExecCmd: func(_ context.Context, cmd sql.Command, buf *sql.StmtBuf) {
					switch tcmd := cmd.(type) {
					case sql.BindStmt:
						if tcmd.PortalName == portalName {
							stmtBuff = buf
						}
					default:
					}
				},
			},
		},
		Insecure: true,
	})
	defer s.Stopper().Stop(ctx)

	// Connect to the cluster via the PGWire client.
	p, err := pgtest.NewPGTest(ctx, s.SQLAddr(), security.RootUser)
	require.NoError(t, err)

	// Setup the portal.
	// Note: This also makes sure that modifying the sessionDataStack with
	// PushTopClone does not have a race condition.
	require.NoError(t, p.SendOneLine(`Query {"String": "BEGIN"}`))

	require.NoError(t, p.SendOneLine(fmt.Sprintf(`Parse {"Query": "%s"}`, selectStmt)))
	require.NoError(t, p.SendOneLine(fmt.Sprintf(`Bind {"DestinationPortal": "%s"}`, portalName)))

	// wait for ready
	until := pgtest.ParseMessages("ReadyForQuery")
	_, err = p.Until(false /* keepErrMsg */, until...)
	require.NoError(t, err)

	// Execute the portal 10 times
	for i := 1; i <= 10; i++ {

		// Exec the portal
		require.NoError(t, p.SendOneLine(fmt.Sprintf(`Execute {"Portal": "%s", "MaxRows": 1}`, portalName)))
		require.NoError(t, p.SendOneLine(`Sync`))

		// wait for ready
		msg, _ := p.Until(false /* keepErrMsg */, until...)

		// received messages should include a data row with the correct value
		received := pgtest.MsgsToJSONWithIgnore(msg, &datadriven.TestData{})
		require.Equal(t, 1, strings.Count(received, fmt.Sprintf(`"Type":"DataRow","Values":[{"text":"%d"}]`, i)))

		// assert that the stmtBuff never exceeds the expected size
		stmtBufLen := stmtBuff.Len()
		if stmtBufLen > stmtBufMaxLen {
			t.Fatalf("statement buffer grew to %d (> %d) after %dth execution", stmtBufLen, stmtBufMaxLen, i)
		}
	}

	// send commit
	require.NoError(t, p.SendOneLine(`Query {"String": "COMMIT"}`))

	// wait for ready
	msg, _ := p.Until(false /* keepErrMsg */, until...)
	received := pgtest.MsgsToJSONWithIgnore(msg, &datadriven.TestData{})
	require.Equal(t, 1, strings.Count(received, `"Type":"CommandComplete","CommandTag":"COMMIT"`))
}

func TestShowLastQueryStatisticsUnknown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params := base.TestServerArgs{}
	s, sqlConn, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := sqlConn.Exec("SELECT 1")
	require.NoError(t, err)

	rows, err := sqlConn.Query("SHOW LAST QUERY STATISTICS RETURNING x, y")
	require.NoError(t, err, "show last query statistics failed")
	defer rows.Close()

	resultColumns, err := rows.Columns()
	require.NoError(t, err)

	const expectedNumColumns = 2
	if len(resultColumns) != expectedNumColumns {
		t.Fatalf(
			"unexpected number of columns in result; expected %d, found %d",
			expectedNumColumns,
			len(resultColumns),
		)
	}

	var x, y gosql.NullString

	rows.Next()
	err = rows.Scan(&x, &y)
	require.NoError(t, err, "unexpected error while reading last query statistics")

	require.False(t, x.Valid)
	require.False(t, y.Valid)
}

// TestTransactionDeadline tests that the transaction deadline is set correctly:
// - In a single-tenant environment, the transaction deadline should use the leased
//   descriptor expiration.
// - In a multi-tenant environment, the transaction deadline should be set to
//   min(sqlliveness.Session expiry, lease descriptor expiration).
func TestTransactionDeadline(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var mu struct {
		syncutil.Mutex
		txnDeadline hlc.Timestamp
		txnID       string
	}
	// Create a closure that can execute some functionality wrapped within mu's lock.
	// This will be used in the tests for accessing mu.
	locked := func(f func()) { mu.Lock(); defer mu.Unlock(); f() }
	// Set up a kvserverbase.ReplicaRequestFilter which will extract the deadline for the test transaction.
	checkTransactionDeadlineFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		if ba.Txn == nil {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		currentTxnID := ba.Txn.TxnMeta.ID.String()
		if currentTxnID != mu.txnID {
			return nil
		}

		if args, ok := ba.GetArg(roachpb.EndTxn); ok {
			et := args.(*roachpb.EndTxnRequest)
			if et.Deadline == nil || et.Deadline.IsEmpty() {
				return nil
			}
			mu.txnDeadline = *et.Deadline
		}
		return nil
	}

	// Set up a sqlliveness.Session override hook so that fake sessions can be
	// injected during the execution of client transactions.
	var sessionOverride atomic.Value
	type sessionOverrideFunc = func(ctx context.Context) (sqlliveness.Session, error)
	noopSessionOverrideFunc := func(ctx context.Context) (sqlliveness.Session, error) {
		return nil, nil
	}
	sessionOverrideKnob := func(ctx context.Context) (sqlliveness.Session, error) {
		return sessionOverride.Load().(sessionOverrideFunc)(ctx)
	}
	sessionOverride.Store(noopSessionOverrideFunc)
	// setClientSessionOverride takes a session to return in the case that
	// the context is due to a client connection. It returns a closure to
	// reset the session override hook.
	setClientSessionOverride := func(fs sqlliveness.Session) (reset func()) {
		sessionOverride.Store(func(ctx context.Context) (sqlliveness.Session, error) {
			// This somewhat hacky approach allows the hook to itentify contexts
			// due to client connections. Client connections carry logtags which
			// look like: [sql,client=127.0.0.1:63305,hostssl,user=root].
			if tags := logtags.FromContext(ctx); tags == nil ||
				!strings.Contains(tags.String(), "client=") {
				return nil, nil
			}
			return fs, nil
		})
		return func() { sessionOverride.Store(noopSessionOverrideFunc) }
	}
	knobs := base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			TestingRequestFilter: checkTransactionDeadlineFilter,
		},
		SQLLivenessKnobs: &sqlliveness.TestingKnobs{
			SessionOverride: sessionOverrideKnob,
		},
	}
	testClusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: knobs,
		},
	}
	tc := serverutils.StartNewTestCluster(t, 1, testClusterArgs)
	defer tc.Stopper().Stop(ctx)
	s := tc.Server(0)

	// Setup a dynamic session override which will be used in subsequent tests to ensure the transaction
	// deadline is set accurately. Use clusterSQLLiveness for bootstrapping the tenant.
	_, sqlConn := serverutils.StartTenant(t, s,
		base.TestTenantArgs{
			TenantID:     serverutils.TestTenantID(),
			TestingKnobs: knobs,
		})
	tdb := sqlutils.MakeSQLRunner(sqlConn)
	// Set up a dummy database and table in the tenant to write to.
	tdb.Exec(t, `
CREATE DATABASE t1;
CREATE TABLE t1.test (k INT PRIMARY KEY, v TEXT);
`)

	t.Run("session_expiry_overrides_lease_deadline", func(t *testing.T) {
		// Deliberately set the sessionDuration to be less than the lease duration
		// to confirm that the sessionDuration overrides the lease duration while
		// setting the transaction deadline.
		sessionDuration := base.DefaultDescriptorLeaseDuration - time.Minute
		fs := fakeSession{exp: s.Clock().Now().Add(sessionDuration.Nanoseconds(), 0)}
		defer setClientSessionOverride(&fs)()

		txn, err := sqlConn.Begin()
		if err != nil {
			t.Fatal(err)
		}
		txnID := getTxnID(t, txn)
		locked(func() { mu.txnID = txnID })
		_, err = txn.ExecContext(ctx, "INSERT INTO t1.test(k, v) VALUES (1, 'abc')")
		if err != nil {
			t.Fatal(err)
		}
		err = txn.Commit()
		require.NoError(t, err)
		locked(func() { require.True(t, fs.Expiration().EqOrdering(mu.txnDeadline)) })
	})

	t.Run("lease_deadline_overrides_session_expiry", func(t *testing.T) {
		// Deliberately set the session duration to be more than the lease duration
		// to confirm that the lease duration overrides the session duration while
		// setting the transaction deadline
		sessionDuration := base.DefaultDescriptorLeaseDuration + time.Minute
		fs := fakeSession{exp: s.Clock().Now().Add(sessionDuration.Nanoseconds(), 0)}
		defer setClientSessionOverride(&fs)()

		txn, err := sqlConn.Begin()
		if err != nil {
			t.Fatal(err)
		}
		txnID := getTxnID(t, txn)
		locked(func() { mu.txnID = txnID })
		_, err = txn.ExecContext(ctx, "UPSERT INTO t1.test(k, v) VALUES (1, 'abc')")
		if err != nil {
			t.Fatal(err)
		}
		err = txn.Commit()
		require.NoError(t, err)

		locked(func() { require.True(t, mu.txnDeadline.Less(fs.Expiration())) })
	})

	t.Run("expired session leads to clear error", func(t *testing.T) {
		// In this test we use an intentionally expired session in the tenant
		// and observe that we get a clear error indicating that the session
		// was expired.
		sessionDuration := -time.Nanosecond
		fs := fakeSession{exp: s.Clock().Now().Add(sessionDuration.Nanoseconds(), 0)}
		defer setClientSessionOverride(&fs)()
		txn, err := sqlConn.Begin()
		if err != nil {
			t.Fatal(err)
		}
		_, err = txn.ExecContext(ctx, "UPSERT INTO t1.test(k, v) VALUES (1, 'abc')")
		require.Regexp(t, `liveness session expired (\S+) before transaction`, err)
		require.NoError(t, txn.Rollback())
	})

	t.Run("single_tenant_ignore_session_expiry", func(t *testing.T) {
		// In this test, we check that the session expiry is ignored in a single-tenant
		// environment. To verify this, we deliberately set the session duration to be
		// less than the lease duration while overriding the cluster sqlliveness.Session.
		// On multi-tenant environments, the session expiry will override the lease duration
		// while setting a transaction deadline. However, in a single tenant environment,
		// the session expiry should be ignored.
		// Open a DB connection on the server and not the tenant to test that the session
		// expiry is ignored outside of the multi-tenant environment.
		dbConn := serverutils.OpenDBConn(t, s.ServingSQLAddr(), "" /* useDatabase */, false /* insecure */, s.Stopper())
		defer dbConn.Close()
		// Set up a dummy database and table to write into for the test.
		if _, err := dbConn.Exec(`CREATE DATABASE t1;
	CREATE TABLE t1.test (k INT PRIMARY KEY, v TEXT);
	`); err != nil {
			t.Fatal(err)
		}

		// Inject an already expired session to observe that it has no effect.
		fs := &fakeSession{exp: s.Clock().Now().Add(-time.Minute.Nanoseconds(), 0)}
		defer setClientSessionOverride(fs)()
		txn, err := dbConn.Begin()
		if err != nil {
			t.Fatal(err)
		}
		txnID := getTxnID(t, txn)
		locked(func() { mu.txnID = txnID })
		_, err = txn.ExecContext(ctx, "INSERT INTO t1.test(k, v) VALUES (1, 'abc')")
		if err != nil {
			t.Fatal(err)
		}
		err = txn.Commit()
		require.NoError(t, err)

		// Confirm that the txnDeadline is not equal to the session expiration.
		locked(func() { require.True(t, fs.Expiration().Less(mu.txnDeadline)) })
	})
}

func TestShowLastQueryStatistics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params := base.TestServerArgs{}
	s, sqlConn, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	testCases := []struct {
		stmt                             string
		usesExecEngine                   bool
		expectNonTrivialSchemaChangeTime bool
	}{
		{
			stmt:                             "CREATE TABLE t(a INT, b INT)",
			usesExecEngine:                   true,
			expectNonTrivialSchemaChangeTime: false,
		},
		{
			stmt:                             "SHOW SYNTAX 'SELECT * FROM t'",
			usesExecEngine:                   false,
			expectNonTrivialSchemaChangeTime: false,
		},
		{
			stmt:                             "PREPARE stmt(INT) AS INSERT INTO t VALUES(1, $1)",
			usesExecEngine:                   false,
			expectNonTrivialSchemaChangeTime: false,
		},
		{
			stmt: `CREATE TABLE t1(a INT); 
INSERT INTO t1 SELECT i FROM generate_series(1, 10000) AS g(i);
ALTER TABLE t1 ADD COLUMN b INT DEFAULT 1`,
			usesExecEngine:                   true,
			expectNonTrivialSchemaChangeTime: true,
		},
	}

	for _, tc := range testCases {
		if _, err := sqlConn.Exec(tc.stmt); err != nil {
			require.NoError(t, err, "executing %s  ", tc.stmt)
		}

		rows, err := sqlConn.Query("SHOW LAST QUERY STATISTICS RETURNING parse_latency, plan_latency, exec_latency, service_latency, post_commit_jobs_latency")
		require.NoError(t, err, "show last query statistics failed")
		defer rows.Close()

		resultColumns, err := rows.Columns()
		require.NoError(t, err)

		const expectedNumColumns = 5
		if len(resultColumns) != expectedNumColumns {
			t.Fatalf(
				"unexpected number of columns in result; expected %d, found %d",
				expectedNumColumns,
				len(resultColumns),
			)
		}

		var parseLatency string
		var planLatency string
		var execLatency string
		var serviceLatency string
		var postCommitJobsLatency string

		rows.Next()
		err = rows.Scan(
			&parseLatency, &planLatency, &execLatency, &serviceLatency, &postCommitJobsLatency,
		)
		require.NoError(t, err, "unexpected error while reading last query statistics")

		parseInterval, err := tree.ParseDInterval(duration.IntervalStyle_POSTGRES, parseLatency)
		require.NoError(t, err)

		planInterval, err := tree.ParseDInterval(duration.IntervalStyle_POSTGRES, planLatency)
		require.NoError(t, err)

		execInterval, err := tree.ParseDInterval(duration.IntervalStyle_POSTGRES, execLatency)
		require.NoError(t, err)

		serviceInterval, err := tree.ParseDInterval(duration.IntervalStyle_POSTGRES, serviceLatency)
		require.NoError(t, err)

		postCommitJobsInterval, err := tree.ParseDInterval(duration.IntervalStyle_POSTGRES, postCommitJobsLatency)
		require.NoError(t, err)

		if parseInterval.AsFloat64() <= 0 || parseInterval.AsFloat64() > 1 {
			t.Fatalf("unexpected parse latency: %v", parseInterval.AsFloat64())
		}

		if tc.usesExecEngine && (planInterval.AsFloat64() <= 0 || planInterval.AsFloat64() > 1) {
			t.Fatalf("unexpected plan latency: %v", planInterval.AsFloat64())
		}

		// Service latencies with tests that do schema changes are hard to constrain
		// a window for, so don't bother.
		if !tc.expectNonTrivialSchemaChangeTime &&
			(serviceInterval.AsFloat64() <= 0 || serviceInterval.AsFloat64() > 1) {
			t.Fatalf("unexpected service latency: %v", serviceInterval.AsFloat64())
		}

		if tc.usesExecEngine && (execInterval.AsFloat64() <= 0 || execInterval.AsFloat64() > 1) {
			t.Fatalf("unexpected execution latency: %v", execInterval.AsFloat64())
		}

		if !tc.expectNonTrivialSchemaChangeTime &&
			(postCommitJobsInterval.AsFloat64() < 0 || postCommitJobsInterval.AsFloat64() > 1) {
			t.Fatalf("unexpected post commit jobs latency: %v", postCommitJobsInterval.AsFloat64())
		}

		if tc.expectNonTrivialSchemaChangeTime && postCommitJobsInterval.AsFloat64() < 0.1 {
			t.Fatalf(
				"expected schema changes to take longer than 0.1 seconds, took: %v",
				postCommitJobsInterval.AsFloat64(),
			)
		}

		if rows.Next() {
			t.Fatalf("unexpected number of rows returned by last query statistics: %v", err)
		}
	}
}

func TestInjectRetryErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params := base.TestServerArgs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	defer db.Close()

	_, err := db.Exec("SET inject_retry_errors_enabled = 'true'")
	require.NoError(t, err)

	t.Run("with_savepoints", func(t *testing.T) {
		// The crdb.ExecuteTx wrapper uses SAVEPOINTs to retry the transaction,
		// so it will automatically succeed.
		var txRes, attemptCount int
		err = crdb.ExecuteTx(ctx, db, nil, func(tx *gosql.Tx) error {
			attemptCount++
			if txErr := tx.QueryRow("SELECT $1::int8", attemptCount).Scan(&txRes); txErr != nil {
				return txErr
			}
			return nil
		})
		require.NoError(t, err)
		// numTxnRetryErrors is set to 3 in conn_executor_exec, so the 4th attempt
		// should be the one that succeeds.
		require.Equal(t, 4, txRes)
	})

	t.Run("without_savepoints", func(t *testing.T) {
		// Without SAVEPOINTs, the caller must keep track of the retry count
		// and disable the error injection.
		var txRes int
		for attemptCount := 1; attemptCount <= 10; attemptCount++ {
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)

			if attemptCount == 5 {
				_, err = tx.Exec("SET LOCAL inject_retry_errors_enabled = 'false'")
				require.NoError(t, err)
			}

			err = tx.QueryRow("SELECT $1::int8", attemptCount).Scan(&txRes)
			if err == nil {
				require.NoError(t, tx.Commit())
				break
			}
			pqErr := (*pq.Error)(nil)
			require.ErrorAs(t, err, &pqErr)
			require.Equal(t, "40001", string(pqErr.Code), "expected a transaction retry error code. got %v", pqErr)
			require.NoError(t, tx.Rollback())
		}
		require.Equal(t, 5, txRes)
	})
}

func TestTrackOnlyUserOpenTransactionsAndActiveStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params := base.TestServerArgs{}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	dbConn := serverutils.OpenDBConn(t, s.ServingSQLAddr(), "", false /* insecure */, s.Stopper())
	defer dbConn.Close()

	waitChannel := make(chan struct{})

	selectQuery := "SELECT * FROM t.foo"
	selectInternalQueryActive := `SELECT count(*) FROM crdb_internal.cluster_queries WHERE query = '` + selectQuery + `'`
	selectUserQueryActive := `SELECT count(*) FROM [SHOW STATEMENTS] WHERE query = '` + selectQuery + `'`

	sqlServer := s.SQLServer().(*sql.Server)
	testDB := sqlutils.MakeSQLRunner(sqlDB)
	testDB.Exec(t, "CREATE DATABASE t")
	testDB.Exec(t, "CREATE TABLE t.foo (i INT PRIMARY KEY)")
	testDB.Exec(t, "INSERT INTO t.foo VALUES (1)")

	// Begin a user-initiated transaction.
	testDB.Exec(t, "BEGIN")

	// Check that the number of open transactions has incremented.
	require.Equal(t, int64(1), sqlServer.Metrics.EngineMetrics.SQLTxnsOpen.Value())

	// Create a state of contention.
	testDB.Exec(t, "SELECT * FROM t.foo WHERE i = 1 FOR UPDATE")

	// Execute internal statement (this case is identical to opening an internal
	// transaction).
	go func() {
		_, err := s.InternalExecutor().(*sql.InternalExecutor).ExecEx(ctx,
			"test-internal-active-stmt-wait",
			nil,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			selectQuery)
		require.NoError(t, err, "expected internal SELECT query to be successful, but encountered an error")
		waitChannel <- struct{}{}
	}()

	// Check that the internal statement is active.
	testutils.SucceedsWithin(t, func() error {
		row := testDB.QueryStr(t, selectInternalQueryActive)
		if row[0][0] == "0" {
			return errors.New("internal select query is not active yet")
		}
		return nil
	}, 5*time.Second)

	testutils.SucceedsWithin(t, func() error {
		// Check that the number of open transactions has not incremented. We only
		// want to track user's open transactions. Open transaction count already
		// at one from initial user-initiated transaction.
		if sqlServer.Metrics.EngineMetrics.SQLTxnsOpen.Value() != 1 {
			return errors.Newf("Wrong SQLTxnsOpen value. Expected: %d. Actual: %d", 1, sqlServer.Metrics.EngineMetrics.SQLTxnsOpen.Value())
		}
		// Check that the number of active statements has not incremented. We only
		// want to track user's active statements.
		if sqlServer.Metrics.EngineMetrics.SQLActiveStatements.Value() != 0 {
			return errors.Newf("Wrong SQLActiveStatements value. Expected: %d. Actual: %d", 0, sqlServer.Metrics.EngineMetrics.SQLActiveStatements.Value())
		}
		return nil
	}, 5*time.Second)

	require.Equal(t, int64(1), sqlServer.Metrics.EngineMetrics.SQLTxnsOpen.Value())
	require.Equal(t, int64(0), sqlServer.Metrics.EngineMetrics.SQLActiveStatements.Value())

	// Create active user-initiated statement.
	go func() {
		_, err := dbConn.Exec(selectQuery)
		require.NoError(t, err, "expected user SELECT query to be successful, but encountered an error")
		waitChannel <- struct{}{}
	}()

	// Check that the user statement is active.
	testutils.SucceedsWithin(t, func() error {
		row := testDB.QueryStr(t, selectUserQueryActive)
		if row[0][0] == "0" {
			return errors.New("user select query is not active yet")
		}
		return nil
	}, 5*time.Second)

	testutils.SucceedsWithin(t, func() error {
		// Check that the number of open transactions has incremented. Second db
		// connection creates an implicit user-initiated transaction.
		if sqlServer.Metrics.EngineMetrics.SQLTxnsOpen.Value() != 2 {
			return errors.Newf("Wrong SQLTxnsOpen value. Expected: %d. Actual: %d", 2, sqlServer.Metrics.EngineMetrics.SQLTxnsOpen.Value())
		}
		// Check that the number of active statements has incremented.
		if sqlServer.Metrics.EngineMetrics.SQLActiveStatements.Value() != 1 {
			return errors.Newf("Wrong SQLActiveStatements value. Expected: %d. Actual: %d", 1, sqlServer.Metrics.EngineMetrics.SQLActiveStatements.Value())
		}
		return nil
	}, 5*time.Second)

	require.Equal(t, int64(2), sqlServer.Metrics.EngineMetrics.SQLTxnsOpen.Value())
	require.Equal(t, int64(1), sqlServer.Metrics.EngineMetrics.SQLActiveStatements.Value())

	// Commit the initial user-initiated transaction. The internal and user
	// select queries are no longer in contention.
	testDB.Exec(t, "COMMIT")

	// Check that both the internal & user statements are no longer active.
	testutils.SucceedsWithin(t, func() error {
		userRow := testDB.QueryStr(t, selectUserQueryActive)
		internalRow := testDB.QueryStr(t, selectInternalQueryActive)

		if userRow[0][0] != "0" {
			return errors.New("user select query is still active")
		} else if internalRow[0][0] != "0" {
			return errors.New("internal select query is still active")
		}
		return nil
	}, 5*time.Second)

	testutils.SucceedsWithin(t, func() error {
		// Check that the number of open transactions has decremented by 2 (should
		// decrement for initial user transaction and user statement executed
		// on second db connection).
		if sqlServer.Metrics.EngineMetrics.SQLTxnsOpen.Value() != 0 {
			return errors.Newf("Wrong SQLTxnsOpen value. Expected: %d. Actual: %d", 0, sqlServer.Metrics.EngineMetrics.SQLTxnsOpen.Value())
		}
		// Check that the number of active statements has decremented by 1 (should
		// not decrement for the internal active statement).
		if sqlServer.Metrics.EngineMetrics.SQLActiveStatements.Value() != 0 {
			return errors.Newf("Wrong SQLActiveStatements value. Expected: %d. Actual: %d", 0, sqlServer.Metrics.EngineMetrics.SQLActiveStatements.Value())
		}
		return nil
	}, 5*time.Second)

	require.Equal(t, int64(0), sqlServer.Metrics.EngineMetrics.SQLTxnsOpen.Value())
	require.Equal(t, int64(0), sqlServer.Metrics.EngineMetrics.SQLActiveStatements.Value())

	// Wait for both goroutine queries to finish before calling defer.
	<-waitChannel
	<-waitChannel
}

// TestEmptyTxnIsBeingCorrectlyCounted tests that SQL Active Transaction
// metric correctly handles empty transactions.
func TestEmptyTxnIsBeingCorrectlyCounted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params := base.TestServerArgs{}
	s, conn, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	sqlConn := sqlutils.MakeSQLRunner(conn)
	openSQLTxnCountPreEmptyTxns := s.SQLServer().(*sql.Server).Metrics.EngineMetrics.SQLTxnsOpen.Value()
	numOfEmptyTxns := int64(100)

	// Since we constantly have background transactions running, it introduces
	// some uncertainties and makes it difficult to compare the exact value of
	// sql.txns.open metrics. To account for the uncertainties, we execute a
	// large number of empty transactions. Then we compare the sql.txns.open
	// metrics before and after executing the batch empty transactions. We assert
	// that the delta between two observations is less than the size of the batch.
	for i := int64(0); i < numOfEmptyTxns; i++ {
		sqlConn.Exec(t, "BEGIN;COMMIT;")
	}

	openSQLTxnCountPostEmptyTxns := s.SQLServer().(*sql.Server).Metrics.EngineMetrics.SQLTxnsOpen.Value()
	require.Less(t, openSQLTxnCountPostEmptyTxns-openSQLTxnCountPreEmptyTxns, numOfEmptyTxns,
		"expected the sql.txns.open counter to be properly decremented "+
			"after executing empty transactions, but it was not")
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

type fakeSession struct{ exp hlc.Timestamp }

func (f fakeSession) ID() sqlliveness.SessionID { return "foo" }
func (f fakeSession) Expiration() hlc.Timestamp { return f.exp }
func (f fakeSession) RegisterCallbackForSessionExpiry(func(ctx context.Context)) {
	panic("unimplemented")
}

var _ sqlliveness.Session = (*fakeSession)(nil)

func getTxnID(t *testing.T, tx *gosql.Tx) (id string) {
	t.Helper()
	sqlutils.MakeSQLRunner(tx).QueryRow(t, `
SELECT id
  FROM crdb_internal.node_transactions
 WHERE session_id = (SELECT * FROM [SHOW session_id])`,
	).Scan(&id)
	return id
}
