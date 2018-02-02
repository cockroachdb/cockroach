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
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestAnonymizeStatementsForReporting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const stmt = `
INSERT INTO sensitive(super, sensible) VALUES('that', 'nobody', 'must', 'see');

select * from crdb_internal.node_runtime_info;
`

	rUnsafe := "i'm not safe"
	rSafe := log.Safe("something safe")

	safeErr := sql.AnonymizeStatementsForReporting("testing", stmt, rUnsafe)

	const (
		expMessage = "panic while testing 2 statements: INSERT INTO _(_, _) VALUES " +
			"(_, _, _, _); SELECT * FROM _._; caused by i'm not safe"
		expSafeRedactedMessage = "?:0: panic while testing 2 statements: INSERT INTO _(_, _) VALUES " +
			"(_, _, _, _); SELECT * FROM _._: caused by <redacted>"
		expSafeSafeMessage = "?:0: panic while testing 2 statements: INSERT INTO _(_, _) VALUES " +
			"(_, _, _, _); SELECT * FROM _._: caused by something safe"
	)

	actMessage := safeErr.Error()
	if actMessage != expMessage {
		t.Fatalf("wanted: %s\ngot: %s", expMessage, actMessage)
	}

	actSafeRedactedMessage := log.ReportablesToSafeError(0, "", []interface{}{safeErr}).Error()
	if actSafeRedactedMessage != expSafeRedactedMessage {
		t.Fatalf("wanted: %s\ngot: %s", expSafeRedactedMessage, actSafeRedactedMessage)
	}

	safeErr = sql.AnonymizeStatementsForReporting("testing", stmt, rSafe)

	actSafeSafeMessage := log.ReportablesToSafeError(0, "", []interface{}{safeErr}).Error()
	if actSafeSafeMessage != expSafeSafeMessage {
		t.Fatalf("wanted: %s\ngot: %s", expSafeSafeMessage, actSafeSafeMessage)
	}
}

// Test that a connection closed abruptly while a SQL txn is in progress results
// in that txn being rolled back.
func TestSessionFinishRollsBackTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	aborter := NewTxnAborter()
	defer aborter.Close(t)
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLExecutor = aborter.executorKnobs()
	s, mainDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	{
		pgURL, cleanup := sqlutils.PGUrl(
			t, s.ServingAddr(), "TestSessionFinishRollsBackTxn", url.User(security.RootUser))
		defer cleanup()
		if err := aborter.Init(pgURL); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := mainDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	// We're going to test the rollback of transactions left in various states
	// when the connection closes abruptly.
	// For the state CommitWait, there's no actual rollback we can test for (since
	// the kv-level transaction has already been committed). But we still
	// exercise this state to check that the server doesn't crash (which used to
	// happen - #9879).
	tests := []sql.TxnStateEnum{sql.Open, sql.RestartWait, sql.CommitWait}
	for _, state := range tests {
		t.Run(state.String(), func(t *testing.T) {
			// Create a low-level lib/pq connection so we can close it at will.
			pgURL, cleanupDB := sqlutils.PGUrl(
				t, s.ServingAddr(), state.String(), url.User(security.RootUser))
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

			ctx := context.TODO()
			conn := c.(driver.ConnBeginTx)
			txn, err := conn.BeginTx(ctx, driver.TxOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tx := txn.(driver.ExecerContext)
			if _, err := tx.ExecContext(ctx, "SET TRANSACTION PRIORITY NORMAL", nil); err != nil {
				t.Fatal(err)
			}

			if state == sql.RestartWait || state == sql.CommitWait {
				if _, err := tx.ExecContext(ctx, "SAVEPOINT cockroach_restart", nil); err != nil {
					t.Fatal(err)
				}
			}

			insertStmt := "INSERT INTO t.public.test(k, v) VALUES (1, 'a')"
			if state == sql.RestartWait {
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

			if state == sql.RestartWait || state == sql.CommitWait {
				_, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT cockroach_restart", nil)
				if state == sql.CommitWait {
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
			if err := txCheck.QueryRow("SELECT count(1) FROM t.public.test").Scan(&count); err != nil {
				t.Fatal(err)
			}
			// CommitWait actually committed, so we'll need to clean up.
			if state != sql.CommitWait {
				if count != 0 {
					t.Fatalf("expected no rows, got: %d", count)
				}
			} else {
				if _, err := txCheck.Exec("DELETE FROM t.public.test"); err != nil {
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
	defer s.Stopper().Stop(context.TODO())

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

	const targetKeyString string = "/Table/51/1/1/0"
	var injectedErr int64

	// We're going to inject an error into our EndTransaction.
	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &storage.StoreTestingKnobs{
				TestingProposalFilter: func(fArgs storagebase.ProposalFilterArgs) *roachpb.Error {
					if !fArgs.Req.IsSingleRequest() {
						return nil
					}
					req := fArgs.Req.Requests[0]
					etReq, ok := req.GetInner().(*roachpb.EndTransactionRequest)
					// We only inject the error once. Turns out that during the life of
					// the test there's two EndTransactions being sent - one is the direct
					// result of the test's call to tx.Rollback(), the second is sent by
					// the TxnCoordSender - indirectly triggered by the fact that, on the
					// server side, the transaction's context gets canceled at the SQL
					// layer.
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
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k INT PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Perform a write so that the EndTransaction we're going to send doesn't get
	// elided.
	if _, err := tx.ExecContext(ctx, "INSERT INTO t.public.test(k, v) VALUES (1, 'abc')"); err != nil {
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
			"the EndTransaction with the expected key")
	}
}
