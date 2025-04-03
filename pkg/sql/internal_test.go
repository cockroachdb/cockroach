// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInternalExecutor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := createTestServerParamsAllowTenants()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	ie := s.InternalExecutor().(*sql.InternalExecutor)
	row, err := ie.QueryRowEx(ctx, "test", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
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
		sessiondata.NodeUserSessionDataOverride,
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
	if _, err := db.Exec("SELECT setval('test.seq', 1, true)"); err != nil {
		t.Fatal(err)
	}

	// Test the auto-retries work inside an external transaction too. In this
	// case, the executor cannot retry internally.
	cnt := 0
	err = s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		cnt++
		row, err = ie.QueryRowEx(
			ctx, "test", txn,
			sessiondata.NodeUserSessionDataOverride,
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
	params, _ := createTestServerParamsAllowTenants()
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

	mon := sql.MakeInternalExecutorMemMonitor(sql.MemoryMetrics{}, s.ClusterSettings())
	mon.StartNoReserved(ctx, s.SQLServer().(*sql.Server).GetBytesMonitor())
	ie := sql.MakeInternalExecutor(
		s.SQLServer().(*sql.Server), sql.MemoryMetrics{}, mon,
	)
	ie.SetSessionData(
		&sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{
				Database:  "db",
				UserProto: username.RootUserName().EncodeProto(),
			},
			LocalOnlySessionData: sessiondatapb.LocalOnlySessionData{
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
	params, _ := createTestServerParamsAllowTenants()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := db.Exec("SET CLUSTER SETTING sql.metrics.max_mem_txn_fingerprints = 0;")
	require.NoError(t, err)

	_, err = db.Exec("SET CLUSTER SETTING sql.metrics.max_mem_stmt_fingerprints = 0;")
	require.NoError(t, err)

	mon := sql.MakeInternalExecutorMemMonitor(sql.MemoryMetrics{}, s.ClusterSettings())
	mon.StartNoReserved(ctx, s.SQLServer().(*sql.Server).GetBytesMonitor())
	ie := sql.MakeInternalExecutor(
		s.SQLServer().(*sql.Server), sql.MemoryMetrics{}, mon,
	)
	_, err = ie.Exec(ctx, "stmt-exceeds-fingerprint-limit", nil, "SELECT 1")
	require.NoError(t, err)
}

func TestSessionBoundInternalExecutor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := createTestServerParamsAllowTenants()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec("create database foo"); err != nil {
		t.Fatal(err)
	}

	expDB := "foo"
	mon := sql.MakeInternalExecutorMemMonitor(sql.MemoryMetrics{}, s.ClusterSettings())
	mon.StartNoReserved(ctx, s.SQLServer().(*sql.Server).GetBytesMonitor())
	ie := sql.MakeInternalExecutor(
		s.SQLServer().(*sql.Server), sql.MemoryMetrics{}, mon,
	)
	ie.SetSessionData(
		&sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{
				Database:  expDB,
				UserProto: username.RootUserName().EncodeProto(),
			},
			SequenceState: &sessiondata.SequenceState{},
		})

	row, err := ie.QueryRowEx(ctx, "test", nil, /* txn */
		sessiondata.NoSessionDataOverride,
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

	params, _ := createTestServerParamsAllowTenants()
	params.Insecure = true

	// sem will be fired every time pg_sleep(1337666) is called.
	sem := make(chan struct{})
	params.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
		BeforeExecute: func(ctx context.Context, stmt string, descriptors *descs.Collection) {
			if strings.Contains(stmt, "(1.337666") {
				sem <- struct{}{}
			}
		},
	}

	t.Run("root internal exec", func(t *testing.T) {
		s := serverutils.StartServerOnly(t, params)
		defer s.Stopper().Stop(context.Background())

		testInternalExecutorAppNameInitialization(
			t, sem, catconstants.InternalAppNamePrefix+"-test-query",
			s.InternalExecutor().(*sql.InternalExecutor),
		)
	})

	// We are running the second test with a new server so
	// as to reset the statement statistics properly.
	t.Run("session bound exec", func(t *testing.T) {
		s := serverutils.StartServerOnly(t, params)
		defer s.Stopper().Stop(context.Background())

		mon := sql.MakeInternalExecutorMemMonitor(sql.MemoryMetrics{}, s.ClusterSettings())
		mon.StartNoReserved(context.Background(), s.SQLServer().(*sql.Server).GetBytesMonitor())
		ie := sql.MakeInternalExecutor(
			s.SQLServer().(*sql.Server), sql.MemoryMetrics{}, mon,
		)
		ie.SetSessionData(
			&sessiondata.SessionData{
				SessionData: sessiondatapb.SessionData{
					UserProto:       username.RootUserName().EncodeProto(),
					Database:        "defaultdb",
					ApplicationName: "appname_findme",
				},
				SequenceState: &sessiondata.SequenceState{},
			})
		testInternalExecutorAppNameInitialization(
			t, sem, catconstants.DelegatedAppNamePrefix+"appname_findme", &ie,
		)
	})
}

type testInternalExecutor interface {
	QueryRow(
		ctx context.Context, opName redact.RedactableString, txn *kv.Txn, stmt string, qargs ...interface{},
	) (tree.Datums, error)
	Exec(
		ctx context.Context, opName redact.RedactableString, txn *kv.Txn, stmt string, qargs ...interface{},
	) (int, error)
}

func testInternalExecutorAppNameInitialization(
	t *testing.T, sem chan struct{}, expectedAppName string, ie testInternalExecutor,
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
	} else if appName := string(*row[0].(*tree.DString)); appName != expectedAppName {
		t.Fatalf("unexpected app name: expected %q, got %q", expectedAppName, appName)
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

	for _, tt := range []struct {
		serializable bool
		pushed       bool
		refreshable  bool
		exp          bool
	}{
		{serializable: false, pushed: false, exp: false},
		{serializable: false, pushed: true, exp: false},
		{serializable: true, pushed: false, refreshable: false, exp: false},
		{serializable: true, pushed: false, refreshable: true, exp: false},
		{serializable: true, pushed: true, refreshable: false, exp: true},
		{serializable: true, pushed: true, refreshable: true, exp: false},
		{serializable: true, pushed: true, refreshable: true, exp: false},
	} {
		name := fmt.Sprintf("serializable=%t,pushed=%t,refreshable=%t",
			tt.serializable, tt.pushed, tt.refreshable)
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			params, _ := createTestServerParamsAllowTenants()
			s, _, db := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(ctx)

			// Setup a txn.
			txn := db.NewTxn(ctx, "test")
			// Build a key in the tenant's keyspace
			keyA := append(s.Codec().TenantPrefix(), roachpb.Key("a")...)
			if !tt.serializable {
				require.NoError(t, txn.SetIsoLevel(isolation.Snapshot))
			}
			if tt.pushed {
				// Read outside the txn.
				_, err := db.Get(ctx, keyA)
				require.NoError(t, err)
				// Write to the same key inside the txn to push its write timestamp.
				require.NoError(t, txn.Put(ctx, keyA, "x"))
				require.NotEqual(t, txn.ReadTimestamp(), txn.ProvisionalCommitTimestamp(), "expect txn wts to be pushed")
			}
			if tt.serializable && !tt.refreshable {
				// Fix the txn's timestamp to prevent refreshes.
				_, err := txn.CommitTimestamp()
				require.NoError(t, err)
			}

			// Are txn.IsSerializablePushAndRefreshNotPossible() and the connExecutor
			// tempted to generate a retriable error eagerly?
			require.Equal(t, tt.exp, txn.IsSerializablePushAndRefreshNotPossible())
			if !tt.exp {
				// Test case no longer interesting.
				return
			}

			tr := s.Tracer()
			execCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
			defer getRecAndFinish()
			ie := s.InternalExecutor().(*sql.InternalExecutor)
			_, err := ie.Exec(execCtx, "test", txn, "select 42")
			require.NoError(t, err)
			require.NoError(t, testutils.MatchInOrder(getRecAndFinish().String(),
				"push detected for non-refreshable txn but auto-retry not possible"))
			require.NotEqual(t, txn.ReadTimestamp(), txn.ProvisionalCommitTimestamp(), "expect txn wts to be pushed")

			require.NoError(t, txn.Rollback(ctx))
		})
	}
}

func TestInternalExecutorInLeafTxnDoesNotPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	rootTxn := kvDB.NewTxn(ctx, "root-txn")

	ltis, err := rootTxn.GetLeafTxnInputState(ctx)
	require.NoError(t, err)
	leafTxn := kv.NewLeafTxn(ctx, kvDB, roachpb.NodeID(1), ltis)

	ie := s.InternalExecutor().(*sql.InternalExecutor)
	_, err = ie.ExecEx(
		ctx, "leaf-query", leafTxn, sessiondata.NodeUserSessionDataOverride, "SELECT 1",
	)
	require.NoError(t, err)
}

func TestInternalExecutorWithDefinedQoSOverrideDoesNotPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	ie := s.InternalExecutor().(*sql.InternalExecutor)
	qosLevel := sessiondatapb.BulkLow
	_, err := ie.ExecEx(
		ctx, "defined_quality_of_service_level_does_not_panic", nil,
		sessiondata.InternalExecutorOverride{User: username.NodeUserName(), QualityOfService: &qosLevel},
		"SELECT 1",
	)
	require.NoError(t, err)
}

func TestInternalExecutorWithUndefinedQoSOverridePanics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	ie := s.InternalExecutor().(*sql.InternalExecutor)
	qosLevel := sessiondatapb.QoSLevel(122)
	// Only defined QoSLevels are currently allowed.
	require.Panics(t, func() {
		_, err := ie.ExecEx(
			ctx,
			"undefined_quality_of_service_level_panics",
			nil, /* txn */
			sessiondata.InternalExecutorOverride{User: username.NodeUserName(), QualityOfService: &qosLevel},
			"SELECT 1",
		)
		require.Error(t, err)
	})
}

func TestInternalDBWithOverrides(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	idb1 := s.InternalDB().(*sql.InternalDB)

	_ = idb1.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		assert.Equal(t, 8, int(txn.SessionData().DefaultIntSize))
		assert.Equal(t, sessiondatapb.DistSQLAuto, txn.SessionData().DistSQLMode)
		assert.Equal(t, "node", string(txn.SessionData().UserProto))

		row, err := txn.QueryRow(ctx, "test", txn.KV(), "show default_int_size")
		require.NoError(t, err)
		assert.Equal(t, "'8'", row[0].String())

		return nil
	})

	drow, err := idb1.Executor().QueryRow(ctx, "test", nil, "show default_int_size")
	require.NoError(t, err)
	assert.Equal(t, "'8'", drow[0].String())

	idb2 := sql.NewInternalDBWithSessionDataOverrides(idb1,
		func(sd *sessiondata.SessionData) {
			sd.UserProto = "wowowo"
			sd.DefaultIntSize = 2
			sd.DistSQLMode = sessiondatapb.DistSQLOff
		})

	_ = idb2.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Verify the initial session data was overridden.
		assert.Equal(t, 2, int(txn.SessionData().DefaultIntSize))
		assert.Equal(t, sessiondatapb.DistSQLOff, txn.SessionData().DistSQLMode)
		assert.Equal(t, "wowowo", string(txn.SessionData().UserProto))

		// Verify that the override was propagated.
		row, err := txn.QueryRow(ctx, "test", txn.KV(), "show default_int_size")
		require.NoError(t, err)
		assert.Equal(t, "'2'", row[0].String())

		row, err = txn.QueryRow(ctx, "test", txn.KV(), "show session_authorization")
		require.NoError(t, err)
		assert.Equal(t, "'wowowo'", row[0].String())

		row, err = txn.QueryRow(ctx, "test", txn.KV(), "show distsql")
		require.NoError(t, err)
		assert.Equal(t, "'off'", row[0].String())

		return nil
	})

	// Also verify the override works for the non-txn-bound executor.
	drow, err = idb2.Executor().QueryRow(ctx, "test", nil, "show default_int_size")
	require.NoError(t, err)
	assert.Equal(t, "'2'", drow[0].String())

	drow, err = idb2.Executor().QueryRow(ctx, "test", nil, "show session_authorization")
	require.NoError(t, err)
	assert.Equal(t, "'wowowo'", drow[0].String())

	drow, err = idb2.Executor().QueryRow(ctx, "test", nil, "show distsql")
	require.NoError(t, err)
	assert.Equal(t, "'off'", drow[0].String())
}

// TestInternalExecutorEncountersRetry verifies that if the internal executor
// encounters a retry error after some data (rows or metadata) have been
// communicated to the client, the query either results in a retry error (when
// rows have been sent) or correctly transparently retries (#98558).
func TestInternalExecutorEncountersRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := createTestServerParamsAllowTenants()
	srv, db, kvDB := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	if _, err := db.Exec("CREATE DATABASE test; CREATE TABLE test.t (c) AS SELECT 1"); err != nil {
		t.Fatal(err)
	}

	ie := s.InternalExecutor().(*sql.InternalExecutor)
	ieo := sessiondata.InternalExecutorOverride{
		User:                     username.NodeUserName(),
		InjectRetryErrorsEnabled: true,
	}

	// This test case verifies that if we execute the stmt of the RowsAffected
	// type, it is transparently retried and the correct number of "rows
	// affected" is reported.
	t.Run("RowsAffected stmt", func(t *testing.T) {
		// We will use PAUSE SCHEDULES statement which is of RowsAffected type.
		//
		// Notably, internally this statement will run some other queries via
		// the "nested" internal executor, but those "nested" queries don't hit
		// the injected retry error since this knob only applies to the "top"
		// IE.
		const stmt = `PAUSE SCHEDULES SELECT id FROM [SHOW SCHEDULES FOR SQL STATISTICS];`
		paused, err := ie.ExecEx(ctx, "pause schedule", nil /* txn */, ieo, stmt)
		if err != nil {
			t.Fatal(err)
		}
		if paused != 1 {
			t.Fatalf("expected 1 schedule to be paused, got %d", paused)
		}
	})

	const rowsStmt = `SELECT * FROM test.t`

	// This test case verifies that if the retry error occurs after some rows
	// have been communicated to the client, then the stmt results in the retry
	// error too - the IE cannot transparently retry it.
	t.Run("Rows stmt", func(t *testing.T) {
		_, err := ie.QueryBufferedEx(ctx, "read rows", nil /* txn */, ieo, rowsStmt)
		if !testutils.IsError(err, "inject_retry_errors_enabled") {
			t.Fatalf("expected to see injected retry error, got %v", err)
		}
	})

	// This test case verifies that ExecEx of a stmt of Rows type correctly and
	// transparently to us retries the stmt.
	t.Run("ExecEx retries in implicit txn", func(t *testing.T) {
		numRows, err := ie.ExecEx(ctx, "read rows", nil /* txn */, ieo, rowsStmt)
		if err != nil {
			t.Fatal(err)
		}
		if numRows != 1 {
			t.Fatalf("expected 1 rowsAffected, got %d", numRows)
		}
	})

	// This test case verifies that ExecEx doesn't retry when it's provided with
	// an explicit txn.
	t.Run("ExecEx doesn't retry in explicit txn", func(t *testing.T) {
		txn := kvDB.NewTxn(ctx, "explicit")
		_, err := ie.ExecEx(ctx, "read rows", txn, ieo, rowsStmt)
		if !testutils.IsError(err, "inject_retry_errors_enabled") {
			t.Fatalf("expected to see injected retry error, got %v", err)
		}
	})

	// This test case verifies that ExecEx stops retrying once the limit on the
	// number of retries is reached.
	t.Run("ExecEx retry limit reached in implicit txn", func(t *testing.T) {
		// This number must be less than the number of errors injected (which is
		// determined by sql.numTxnRetryErrors = 3).
		if _, err := db.Exec("SET CLUSTER SETTING sql.internal_executor.rows_affected_retry_limit = 1;"); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := db.Exec("RESET CLUSTER SETTING sql.internal_executor.rows_affected_retry_limit;"); err != nil {
				t.Fatal(err)
			}
		}()
		_, err := ie.ExecEx(ctx, "read rows", nil /* txn */, ieo, rowsStmt)
		if err == nil {
			t.Fatal("expected to get an injected retriable error")
		}
	})

	// TODO(yuzefovich): add a test for when a schema change is done in-between
	// the retries.
}

// TestInternalExecutorSyntheticDesc injects a synthetic descriptor
// into a new transaction and confirms that existing descriptors are
// replaced for both new and old transactions
// (using isql.WithSyntheticDescriptors).
func TestInternalExecutorSyntheticDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := createTestServerParamsAllowTenants()
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec("CREATE DATABASE test; CREATE TABLE test.t (c) AS SELECT 1"); err != nil {
		t.Fatal(err)
	}

	idb := s.InternalDB().(*sql.InternalDB)
	// Modify the existing descriptor for test, so that the column c is now known
	// as blah.
	syntheticDesc := desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, s.Codec(), "test", "t")
	syntheticDesc.Columns[0].Name = "blah"
	syntheticDesc.PrimaryIndex.StoreColumnNames[0] = "blah"
	syntheticDesc.Families[0].ColumnNames[0] = "blah"
	// Specify a nil txn, so that the internal executor layer creates
	// a new transaction.
	t.Run("inject synthetic descriptor in new txn",
		func(t *testing.T) {
			exec := idb.Executor()
			require.NoError(t, exec.WithSyntheticDescriptors(catalog.Descriptors{syntheticDesc}, func() error {
				row, err := exec.QueryRow(ctx, "query-column-name", nil, "SELECT create_statement FROM [SHOW CREATE TABLE test.t]")
				require.NoError(t, err)
				createStatement := row[0].(*tree.DString)
				require.Equal(t,
					`CREATE TABLE public.t (
	blah INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT t_pkey PRIMARY KEY (rowid ASC)
)`,
					string(*createStatement))
				return nil
			}))
		})

	// Start a new txn and pass that into the internal executor, and
	// confirm the synthetic descriptor is picked up.
	t.Run("inject synthetic descriptor in existing txn",
		func(t *testing.T) {
			require.NoError(t,
				idb.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					exec := idb.Executor()
					return exec.WithSyntheticDescriptors(catalog.Descriptors{syntheticDesc}, func() error {
						row, err := exec.QueryRow(ctx, "query-column-name", txn, "SELECT create_statement FROM [SHOW CREATE TABLE test.t]")
						require.NoError(t, err)
						createStatement := row[0].(*tree.DString)
						require.Equal(t,
							`CREATE TABLE public.t (
	blah INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT t_pkey PRIMARY KEY (rowid ASC)
)`,
							string(*createStatement))
						return nil
					})
				}),
			)
		})
}

// TODO(andrei): Test that descriptor leases are released by the
// Executor, with and without a higher-level txn. When there is no
// higher-level txn, the leases are released normally by the txn finishing. When
// there is, they are released by the resetExtraTxnState() call in the
// Executor. Unfortunately at the moment we don't have a great way to
// test lease releases.
