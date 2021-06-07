// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestInternalExecutor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	ie := s.InternalExecutor().(*sql.InternalExecutor)
	row, err := ie.QueryRowEx(ctx, "test", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"SELECT 1")
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
	row, err = ie.QueryRowEx(
		ctx, "test", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"select case nextval('test.seq') when 1 then crdb_internal.force_retry('1h') else 99 end",
	)
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("empty result")
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
	err = s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		cnt++
		row, err = ie.QueryRowEx(
			ctx, "test", txn,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			"select case nextval('test.seq') when 2 then crdb_internal.force_retry('1h') else 99 end",
		)
		if cnt == 1 {
			require.Regexp(t, "crdb_internal.force_retry", err)
		}
		if err != nil {
			return err
		}
		if row == nil {
			return errors.New("empty result")
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

func TestInternalFullTableScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := db.Exec("CREATE DATABASE db; SET DATABASE = db;")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE t(a INT)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t VALUES (1), (2), (3)")
	require.NoError(t, err)

	_, err = db.Exec("SET disallow_full_table_scans = true")
	require.NoError(t, err)

	_, err = db.Exec("SELECT * FROM t")
	require.Error(t, err)
	require.Equal(t,
		"pq: query `SELECT * FROM t` contains a full table/index scan which is explicitly disallowed",
		err.Error())

	ie := sql.MakeInternalExecutor(
		ctx,
		s.(*server.TestServer).Server.PGServer().SQLServer,
		sql.MemoryMetrics{},
		s.ExecutorConfig().(sql.ExecutorConfig).Settings,
	)
	ie.SetSessionData(
		&sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{
				Database:  "db",
				UserProto: security.RootUserName().EncodeProto(),
			},
			LocalOnlySessionData: sessiondata.LocalOnlySessionData{
				DisallowFullTableScans: true,
			},
			SequenceState: &sessiondata.SequenceState{},
		})

	// Internal queries that perform full table scans shouldn't fail because of
	// the setting above.
	_, err = ie.Exec(ctx, "full-table-scan-select", nil, "SELECT * FROM db.t")
	require.NoError(t, err)
}

// Test for regression https://github.com/cockroachdb/cockroach/issues/65523
func TestInternalStmtFingerprintLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := db.Exec("SET CLUSTER SETTING sql.metrics.max_mem_txn_fingerprints = 0;")
	require.NoError(t, err)

	_, err = db.Exec("SET CLUSTER SETTING sql.metrics.max_mem_stmt_fingerprints = 0;")
	require.NoError(t, err)

	ie := sql.MakeInternalExecutor(
		ctx,
		s.(*server.TestServer).Server.PGServer().SQLServer,
		sql.MemoryMetrics{},
		s.ExecutorConfig().(sql.ExecutorConfig).Settings,
	)

	_, err = ie.Exec(ctx, "stmt-exceeds-fingerprint-limit", nil, "SELECT 1")
	require.NoError(t, err)
}

func TestQueryIsAdminWithNoTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec("create user testuser"); err != nil {
		t.Fatal(err)
	}

	ie := s.InternalExecutor().(*sql.InternalExecutor)

	testData := []struct {
		user     security.SQLUsername
		expAdmin bool
	}{
		{security.NodeUserName(), true},
		{security.RootUserName(), true},
		{security.TestUserName(), false},
	}

	for _, tc := range testData {
		t.Run(tc.user.Normalized(), func(t *testing.T) {
			row, cols, err := ie.QueryRowExWithCols(ctx, "test", nil, /* txn */
				sessiondata.InternalExecutorOverride{User: tc.user},
				"SELECT crdb_internal.is_admin()")
			if err != nil {
				t.Fatal(err)
			}
			if row == nil || len(cols) != 1 {
				numRows := 0
				if row != nil {
					numRows = 1
				}
				t.Fatalf("unexpected result shape %d, %d", numRows, len(cols))
			}
			isAdmin := bool(*row[0].(*tree.DBool))
			if isAdmin != tc.expAdmin {
				t.Fatalf("expected %q admin %v, got %v", tc.user, tc.expAdmin, isAdmin)
			}
		})
	}
}

func TestQueryHasRoleOptionWithNoTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	stmts := `
CREATE USER testuser VIEWACTIVITY;
CREATE USER testadmin;
GRANT admin TO testadmin`
	if _, err := db.Exec(stmts); err != nil {
		t.Fatal(err)
	}
	ie := s.InternalExecutor().(*sql.InternalExecutor)

	for _, tc := range []struct {
		user        string
		option      string
		expected    bool
		expectedErr string
	}{
		{"testuser", roleoption.VIEWACTIVITY.String(), true, ""},
		{"testuser", roleoption.CREATEROLE.String(), false, ""},
		{"testuser", "nonexistent", false, "unrecognized role option"},
		{"testadmin", roleoption.VIEWACTIVITY.String(), true, ""},
		{"testadmin", roleoption.CREATEROLE.String(), true, ""},
		{"testadmin", "nonexistent", false, "unrecognized role option"},
	} {
		username := security.MakeSQLUsernameFromPreNormalizedString(tc.user)
		row, cols, err := ie.QueryRowExWithCols(ctx, "test", nil, /* txn */
			sessiondata.InternalExecutorOverride{User: username},
			"SELECT crdb_internal.has_role_option($1)", tc.option)
		if tc.expectedErr != "" {
			if !testutils.IsError(err, tc.expectedErr) {
				t.Fatalf("expected error %q, got %q", tc.expectedErr, err)
			}
			continue
		}
		if row == nil || len(cols) != 1 {
			numRows := 0
			if row != nil {
				numRows = 1
			}
			t.Fatalf("unexpected result shape %d, %d", numRows, len(cols))
		}
		hasRoleOption := bool(*row[0].(*tree.DBool))
		if hasRoleOption != tc.expected {
			t.Fatalf(
				"expected %q has_role_option('%s') %v, got %v", tc.user, tc.option, tc.expected,
				hasRoleOption)
		}
	}
}

func TestSessionBoundInternalExecutor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec("create database foo"); err != nil {
		t.Fatal(err)
	}

	expDB := "foo"
	ie := sql.MakeInternalExecutor(
		ctx,
		s.(*server.TestServer).Server.PGServer().SQLServer,
		sql.MemoryMetrics{},
		s.ExecutorConfig().(sql.ExecutorConfig).Settings,
	)
	ie.SetSessionData(
		&sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{
				Database:  expDB,
				UserProto: security.RootUserName().EncodeProto(),
			},
			SequenceState: &sessiondata.SequenceState{},
		})

	row, err := ie.QueryRowEx(ctx, "test", nil, /* txn */
		sessiondata.InternalExecutorOverride{},
		"show database")
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
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	params.Insecure = true

	// sem will be fired every time pg_sleep(1337666) is called.
	sem := make(chan struct{})
	params.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
		BeforeExecute: func(ctx context.Context, stmt string) {
			if strings.Contains(stmt, "(1.337666") {
				sem <- struct{}{}
			}
		},
	}

	t.Run("root internal exec", func(t *testing.T) {
		s, _, _ := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(context.Background())

		testInternalExecutorAppNameInitialization(t, sem,
			catconstants.InternalAppNamePrefix+"-test-query", // app name in SHOW
			catconstants.InternalAppNamePrefix+"-test-query", // app name in stats
			s.InternalExecutor().(*sql.InternalExecutor))
	})

	// We are running the second test with a new server so
	// as to reset the statement statistics properly.
	t.Run("session bound exec", func(t *testing.T) {
		s, _, _ := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(context.Background())

		ie := sql.MakeInternalExecutor(
			context.Background(),
			s.(*server.TestServer).Server.PGServer().SQLServer,
			sql.MemoryMetrics{},
			s.ExecutorConfig().(sql.ExecutorConfig).Settings,
		)
		ie.SetSessionData(
			&sessiondata.SessionData{
				SessionData: sessiondatapb.SessionData{
					UserProto:       security.RootUserName().EncodeProto(),
					Database:        "defaultdb",
					ApplicationName: "appname_findme",
				},
				SequenceState: &sessiondata.SequenceState{},
			})
		testInternalExecutorAppNameInitialization(
			t, sem,
			"appname_findme", // app name in SHOW
			catconstants.DelegatedAppNamePrefix+"appname_findme", // app name in stats
			&ie,
		)
	})
}

type testInternalExecutor interface {
	QueryRow(
		ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
	) (tree.Datums, error)
	Exec(
		ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
	) (int, error)
}

func testInternalExecutorAppNameInitialization(
	t *testing.T,
	sem chan struct{},
	expectedAppName, expectedAppNameInStats string,
	ie testInternalExecutor,
) {
	// Check that the application_name is set properly in the executor.
	if row, err := ie.QueryRow(context.Background(), "test-query", nil,
		"SHOW application_name"); err != nil {
		t.Fatal(err)
	} else if row == nil {
		t.Fatalf("expected 1 row, got 0")
	} else if appName := string(*row[0].(*tree.DString)); appName != expectedAppName {
		t.Fatalf("unexpected app name: expected %q, got %q", expectedAppName, appName)
	}

	// Start a background query using the internal executor. We want to
	// have this keep running until we cancel it below.
	errChan := make(chan error)
	go func() {
		_, err := ie.Exec(context.Background(),
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
		row, err := ie.QueryRow(context.Background(),
			"find-query",
			nil, /* txn */
			// We need to assemble the magic string so that this SELECT
			// does not find itself.
			"SELECT query_id, application_name FROM [SHOW ALL QUERIES] WHERE query LIKE '%337' || '666%'")
		if err != nil {
			return err
		}
		if row == nil {
			// The SucceedsSoon test may find this a couple of times before
			// this succeeds.
			return fmt.Errorf("query not started yet")
		} else {
			appName := string(*row[1].(*tree.DString))
			if appName != expectedAppName {
				return fmt.Errorf("unexpected app name: expected %q, got %q", expectedAppName, appName)
			}

			// Good app name, retrieve query ID for later cancellation.
			queryID = string(*row[0].(*tree.DString))
			return nil
		}
	})

	// Check that the query shows up in the internal tables without error.
	if row, err := ie.QueryRow(context.Background(), "find-query", nil,
		"SELECT application_name FROM crdb_internal.node_queries WHERE query LIKE '%337' || '666%'"); err != nil {
		t.Fatal(err)
	} else if row == nil {
		t.Fatalf("expected 1 query, got 0")
	} else if appName := string(*row[0].(*tree.DString)); appName != expectedAppName {
		t.Fatalf("unexpected app name: expected %q, got %q", expectedAppName, appName)
	}

	// We'll want to look at statistics below, and finish the test with
	// no goroutine leakage. To achieve this, cancel the query and
	// drain the goroutine.
	if _, err := ie.Exec(context.Background(), "cancel-query", nil, "CANCEL QUERY $1", queryID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errChan:
		if !sqltestutils.IsClientSideQueryCanceledErr(err) {
			t.Fatal(err)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("no error received from query supposed to be canceled")
	}

	// Now check that it was properly registered in statistics.
	if row, err := ie.QueryRow(context.Background(), "find-query", nil,
		"SELECT application_name FROM crdb_internal.node_statement_statistics WHERE key LIKE 'SELECT' || ' pg_sleep(%'"); err != nil {
		t.Fatal(err)
	} else if row == nil {
		t.Fatalf("expected 1 query, got 0")
	} else if appName := string(*row[0].(*tree.DString)); appName != expectedAppNameInStats {
		t.Fatalf("unexpected app name: expected %q, got %q", expectedAppNameInStats, appName)
	}
}

// Test that, when executing inside a higher-level txn, the internal executor
// does not attempt to auto-retry statements when it detects the transaction to
// be pushed. The executor cannot auto-retry by itself, so let's make sure that
// it also doesn't eagerly generate retriable errors when it detects pushed
// transactions.
func TestInternalExecutorPushDetectionInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, _, db := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Setup a pushed txn.
	txn := db.NewTxn(ctx, "test")
	keyA := roachpb.Key("a")
	_, err := db.Get(ctx, keyA)
	require.NoError(t, err)
	require.NoError(t, txn.Put(ctx, keyA, "x"))
	require.NotEqual(t, txn.ReadTimestamp(), txn.ProvisionalCommitTimestamp(), "expect txn wts to be pushed")

	// Fix the txn's timestamp, such that
	// txn.IsSerializablePushAndRefreshNotPossible() and the connExecutor is
	// tempted to generate a retriable error eagerly.
	txn.CommitTimestamp()
	require.True(t, txn.IsSerializablePushAndRefreshNotPossible())

	tr := s.Tracer().(*tracing.Tracer)
	execCtx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
	defer cancel()
	ie := s.InternalExecutor().(*sql.InternalExecutor)
	_, err = ie.Exec(execCtx, "test", txn, "select 42")
	require.NoError(t, err)
	require.NoError(t, testutils.MatchInOrder(collect().String(),
		"push detected for non-refreshable txn but auto-retry not possible"))
	require.NotEqual(t, txn.ReadTimestamp(), txn.ProvisionalCommitTimestamp(), "expect txn wts to be pushed")

	require.NoError(t, txn.Rollback(ctx))
}

func TestInternalExecutorInLeafTxnDoesNotPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	rootTxn := kvDB.NewTxn(ctx, "root-txn")

	ltis := rootTxn.GetLeafTxnInputState(ctx)
	leafTxn := kv.NewLeafTxn(ctx, kvDB, roachpb.NodeID(1), &ltis)

	ie := s.InternalExecutor().(*sql.InternalExecutor)
	_, err := ie.ExecEx(
		ctx, "leaf-query", leafTxn, sessiondata.InternalExecutorOverride{User: security.RootUserName()}, "SELECT 1",
	)
	require.NoError(t, err)
}

// TODO(andrei): Test that descriptor leases are released by the
// InternalExecutor, with and without a higher-level txn. When there is no
// higher-level txn, the leases are released normally by the txn finishing. When
// there is, they are released by the resetExtraTxnState() call in the
// InternalExecutor. Unfortunately at the moment we don't have a great way to
// test lease releases.
