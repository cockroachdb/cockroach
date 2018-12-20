// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestInternalExecutor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	ie := s.InternalExecutor().(*sql.InternalExecutor)
	row, err := ie.QueryRow(ctx, "test", nil /* txn */, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(row) != 1 {
		t.Fatalf("expected 1 col, got: %d", len(row))
	}
	r, ok := row[0].(*tree.DInt)
	if !ok || *r != 1 {
		t.Fatalf("expected a DInt == 1, got: %T:%s", r, r)
	}

	// Test that auto-retries work.
	if _, err := db.Exec("create database test; create sequence test.seq start with 1"); err != nil {
		t.Fatal(err)
	}
	// The following statement will succeed on the 2nd try.
	row, err = ie.QueryRow(
		ctx, "test", nil, /* txn */
		"select case nextval('test.seq') when 1 then crdb_internal.force_retry('1h') else 99 end",
	)
	if err != nil {
		t.Fatal(err)
	}
	r, ok = row[0].(*tree.DInt)
	if !ok || *r != 99 {
		t.Fatalf("expected a DInt == 99, got: %T:%s", r, r)
	}

	// Reset the sequence to a clear value. Next nextval() will return 2.
	if _, err := db.Exec("SELECT setval('test.seq', 1)"); err != nil {
		t.Fatal(err)
	}

	// Test the auto-retries work inside an external transaction too. In this
	// case, the executor cannot retry internally.
	cnt := 0
	err = s.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		cnt++
		row, err = ie.QueryRow(
			ctx, "test", txn,
			"select case nextval('test.seq') when 2 then crdb_internal.force_retry('1h') else 99 end",
		)
		if err != nil {
			return err
		}
		r, ok = row[0].(*tree.DInt)
		if !ok || *r != 99 {
			t.Fatalf("expected a DInt == 99, got: %T:%s", r, r)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 2 {
		t.Fatalf("expected 2 iterations, got: %d", cnt)
	}
}

func TestSessionBoundInternalExecutor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec("create database foo"); err != nil {
		t.Fatal(err)
	}

	expDB := "foo"
	ie := sql.MakeSessionBoundInternalExecutor(
		ctx,
		&sessiondata.SessionData{
			Database:      expDB,
			SequenceState: &sessiondata.SequenceState{},
		},
		s.(*server.TestServer).Server.PGServer().SQLServer,
		sql.MemoryMetrics{},
		s.ExecutorConfig().(sql.ExecutorConfig).Settings,
	)

	row, err := ie.QueryRow(ctx, "test", nil /* txn */, "show database")
	if err != nil {
		t.Fatal(err)
	}
	if len(row) != 1 {
		t.Fatalf("expected 1 col, got: %d", len(row))
	}
	r, ok := row[0].(*tree.DString)
	if !ok || string(*r) != expDB {
		t.Fatalf("expected a DString == %s, got: %T: %s", expDB, r, r)
	}
}

// TestInternalExecAppNameInitialization validates that the application name
// is properly initialized for both kinds of internal executors: the "standalone"
// internal executor and those that hang off client sessions ("session-bound").
// In both cases it does so by checking the result of SHOW application_name,
// the cancellability of the query, and the listing in the application statistics.
func TestInternalExecAppNameInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("#33268")

	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	t.Run("root internal exec", func(t *testing.T) {
		testInternalExecutorAppNameInitialization(t,
			sql.InternalAppNamePrefix+"internal-test-query",
			s.InternalExecutor().(*sql.InternalExecutor))
	})

	ie := sql.MakeSessionBoundInternalExecutor(
		context.TODO(),
		&sessiondata.SessionData{
			User:            security.RootUser,
			Database:        "defaultdb",
			ApplicationName: "appname_findme",
			SequenceState:   &sessiondata.SequenceState{},
		},
		s.(*server.TestServer).Server.PGServer().SQLServer,
		sql.MemoryMetrics{},
		s.ExecutorConfig().(sql.ExecutorConfig).Settings,
	)
	t.Run("session bound exec", func(t *testing.T) {
		testInternalExecutorAppNameInitialization(t,
			"appname_findme",
			&ie)
	})
}

type testInternalExecutor interface {
	Query(
		ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
	) ([]tree.Datums, sqlbase.ResultColumns, error)
	Exec(
		ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
	) (int, error)
}

func testInternalExecutorAppNameInitialization(
	t *testing.T, expectedAppName string, ie testInternalExecutor,
) {
	// Check that the application_name is set properly in the executor.
	if rows, _, err := ie.Query(context.TODO(), "test-query", nil,
		"SHOW application_name"); err != nil {
		t.Fatal(err)
	} else if len(rows) != 1 {
		t.Fatalf("expected 1 row, got: %+v", rows)
	} else if appName := string(*rows[0][0].(*tree.DString)); appName != expectedAppName {
		t.Fatalf("unexpected app name: expected %q, got %q", expectedAppName, appName)
	}

	// Start a background query using the internal executor. We want to
	// have this keep running until we cancel it below.
	sem := make(chan struct{})
	errChan := make(chan error)
	go func() {
		sem <- struct{}{}
		_, _, err := ie.Query(context.TODO(),
			"test-query",
			nil, /* txn */
			"SELECT pg_sleep(1337666)")
		if err != nil {
			errChan <- err
			return
		}
	}()

	<-sem

	// We'll wait until the query appears in SHOW QUERIES.
	// When it does, we capture the query ID.
	var queryID string
	testutils.SucceedsSoon(t, func() error {
		rows, _, err := ie.Query(context.TODO(),
			"find-query",
			nil, /* txn */
			// We need to assemble the magic string so that this SELECT
			// does not find itself.
			"SELECT query_id, application_name FROM [SHOW QUERIES] WHERE query LIKE '%337' || '666%'")
		if err != nil {
			return err
		}
		switch len(rows) {
		case 0:
			// The SucceedsSoon test may find this a couple of times before
			// this succeeds.
			return fmt.Errorf("query not started yet")
		case 1:
			appName := string(*rows[0][1].(*tree.DString))
			if appName != expectedAppName {
				return fmt.Errorf("unexpected app name: expected %q, got %q", expectedAppName, appName)
			}

			// Good app name, retrieve query ID for later cancellation.
			queryID = string(*rows[0][0].(*tree.DString))
			return nil
		default:
			return fmt.Errorf("unexpected results: %+v", rows)
		}
	})

	// Check that the query shows up in the internal tables without error.
	if rows, _, err := ie.Query(context.TODO(), "find-query", nil,
		"SELECT application_name FROM crdb_internal.node_queries WHERE query LIKE '%337' || '666%'"); err != nil {
		t.Fatal(err)
	} else if len(rows) != 1 {
		t.Fatalf("expected 1 query, got: %+v", rows)
	} else if appName := string(*rows[0][0].(*tree.DString)); appName != expectedAppName {
		t.Fatalf("unexpected app name: expected %q, got %q", expectedAppName, appName)
	}

	// We'll want to look at statistics below, and finish the test with
	// no goroutine leakage. To achieve this, cancel the query. and
	// drain the goroutine.
	if _, err := ie.Exec(context.TODO(), "cancel-query", nil, "CANCEL QUERY $1", queryID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errChan:
		if !isClientsideQueryCanceledErr(err) {
			t.Fatal(err)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("no error received from query supposed to be canceled")
	}

	// TODO(knz): remove this skip when we log internal queries in stats.
	t.Skip("#32215")

	// Now check that it was properly registered in statistics.
	if rows, _, err := ie.Query(context.TODO(), "find-query", nil,
		"SELECT application_name FROM crdb_internal.node_statement_statistics WHERE key LIKE 'SELECT' || ' pg_sleep('"); err != nil {
		t.Fatal(err)
	} else if len(rows) != 1 {
		t.Fatalf("expected 1 query, got: %+v", rows)
	} else if appName := string(*rows[0][0].(*tree.DString)); appName != expectedAppName {
		t.Fatalf("unexpected app name: expected %q, got %q", expectedAppName, appName)
	}
}

// TestInternalExecutorTxnAbortNotSwallowed reproduces a rare bug where the
// internal executor could swallow transaction aborted errors. Specifically, an
// optimizer code path was not propagating errors, violating the contract of our
// transaction API, and causing partial split transactions that resulted in
// replica corruption errors (#32784).
//
// Note that a fix to our transaction API to eliminate this class of errors is
// proposed in #22615.
func TestInternalExecutorTxnAbortNotSwallowed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Notify a channel whenever a HeartbeatTxn request notices that a txn has
	// been aborted.
	heartbeatSawAbortedTxn := make(chan uuid.UUID)
	params, _ := tests.CreateTestServerParams()
	params.Knobs.Store = &storage.StoreTestingKnobs{
		TestingResponseFilter: func(ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
			for i, r := range ba.Requests {
				if r.GetHeartbeatTxn() != nil && br.Responses[i].GetHeartbeatTxn().Txn.Status == roachpb.ABORTED {
					go func() {
						heartbeatSawAbortedTxn <- ba.Txn.ID
					}()
				}
			}
			return nil
		},
	}

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	ie := s.InternalExecutor().(*sql.InternalExecutor)

	if _, err := sqlDB.Exec("CREATE TABLE t (a INT)"); err != nil {
		t.Fatal(err)
	}

	// Create a new txn, and perform a write inside of it so that its txn record
	// is written and the heartbeat loop is started. The particular key doesn't
	// matter.
	txn := client.NewTxn(ctx, kvDB, s.NodeID(), client.RootTxn)
	origTxnID := txn.ID()
	if err := txn.Put(ctx, "key-foo", []byte("bar")); err != nil {
		t.Fatal(err)
	}

	// Abort the txn directly with a PushTxnRequest. This happens in practice
	// when, e.g., deadlock between two txns is detected.
	txnProto := txn.GetTxnCoordMeta(ctx).Txn
	if _, pErr := client.SendWrapped(ctx, kvDB.NonTransactionalSender(), &roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{Key: txnProto.Key},
		PusheeTxn:     txnProto.TxnMeta,
		PusherTxn:     roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Priority: roachpb.MaxTxnPriority}},
		PushType:      roachpb.PUSH_ABORT,
	}); pErr != nil {
		t.Fatal(pErr)
	}

	// Wait for one of the txn's heartbeats to notice that the heartbeat has
	// failed.
	for txnID := range heartbeatSawAbortedTxn {
		if txnID == origTxnID {
			break
		}
	}

	// Execute a SQL statement in the txn using an internal executor. Importantly,
	// we're accessing a system table, which bypasses the descriptor cache and
	// forces the optimizer to perform raw KV lookups during name resolution.
	_, err := ie.Exec(ctx, t.Name(), txn, "INSERT INTO system.zones VALUES ($1, $2)", 50, "")

	// Double-check that the client.Txn has "helpfully" given us a brand new
	// KV txn to replace our aborted txn. (#22615)
	if origTxnID == txn.ID() {
		t.Fatal("test bug: txn ID did not change after executing SQL statement on aborted txn")
	}

	// We now have proof that the client.Txn saw the aborted error; the internal
	// executor had better have bubbled this error up so that we know to retry our
	// txn from the beginning.
	if !testutils.IsError(err, "TransactionAbortedError") {
		t.Fatalf("expected query execution on aborted txn to fail, but got %+v", err)
	}
}

// TODO(andrei): Test that descriptor leases are released by the
// InternalExecutor, with and without a higher-level txn. When there is no
// higher-level txn, the leases are released normally by the txn finishing. When
// there is, they are released by the resetExtraTxnState() call in the
// InternalExecutor. Unfortunately at the moment we don't have a great way to
// test lease releases.
