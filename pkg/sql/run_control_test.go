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
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestCancelDistSQLQuery runs a distsql query and cancels it randomly at
// various points of execution.
func TestCancelDistSQLQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const queryToCancel = "SELECT * FROM nums ORDER BY num"
	cancelQuery := fmt.Sprintf("CANCEL QUERIES SELECT query_id FROM [SHOW CLUSTER STATEMENTS] WHERE query = '%s'", queryToCancel)

	// conn1 is used for the query above. conn2 is solely for the CANCEL statement.
	var conn1 *gosql.DB
	var conn2 *gosql.DB

	var queryLatency *time.Duration
	sem := make(chan struct{}, 1)
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	tc := serverutils.StartNewTestCluster(t, 2, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						BeforeExecute: func(_ context.Context, stmt string) {
							if strings.HasPrefix(stmt, queryToCancel) {
								// Wait for the race to start.
								<-sem
							} else if strings.HasPrefix(stmt, cancelQuery) {
								// Signal to start the race.
								sleepTime := time.Duration(rng.Int63n(int64(*queryLatency)))
								sem <- struct{}{}
								time.Sleep(sleepTime)
							}
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())

	conn1 = tc.ServerConn(0)
	conn2 = tc.ServerConn(1)

	sqlutils.CreateTable(t, conn1, "nums", "num INT", 0, nil)
	if _, err := conn1.Exec("INSERT INTO nums SELECT generate_series(1,100)"); err != nil {
		t.Fatal(err)
	}

	if _, err := conn1.Exec("ALTER TABLE nums SPLIT AT VALUES (50)"); err != nil {
		t.Fatal(err)
	}

	// Make the second node the leaseholder for the first range to distribute the
	// query. This may have to retry if the second store's descriptor has not yet
	// propagated to the first store's StorePool.
	testutils.SucceedsSoon(t, func() error {
		_, err := conn1.Exec(fmt.Sprintf(
			"ALTER TABLE nums EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 1)",
			tc.Server(1).GetFirstStoreID()))
		return err
	})

	// Run queryToCancel to be able to get an estimate of how long it should
	// take. The goroutine in charge of cancellation will sleep a random
	// amount of time within this bound. Signal sem so that it can run
	// unhindered.
	sem <- struct{}{}
	start := timeutil.Now()
	if _, err := conn1.Exec(queryToCancel); err != nil {
		t.Fatal(err)
	}
	execTime := timeutil.Since(start)
	queryLatency = &execTime

	errChan := make(chan error)
	go func() {
		_, err := conn1.Exec(queryToCancel)
		errChan <- err
	}()
	_, err := conn2.Exec(cancelQuery)
	if err != nil && !testutils.IsError(err, "query ID") {
		t.Fatal(err)
	}

	err = <-errChan
	if err == nil {
		// A successful cancellation does not imply that the query was canceled.
		return
	}
	if !sqltestutils.IsClientSideQueryCanceledErr(err) {
		t.Fatalf("expected error with specific error code, got: %s", err)
	}
}

func TestCancelSessionPermissions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// getSessionIDs retrieves the IDs of any currently running queries for the
	// specified user.
	getSessionIDs := func(t *testing.T, ctx context.Context, conn *gosql.Conn, user string) []string {
		rows, err := conn.QueryContext(
			ctx, "SELECT session_id FROM [SHOW SESSIONS] WHERE user_name = $1", user,
		)
		if err != nil {
			t.Fatal(err)
		}
		var sessionIDs []string
		for rows.Next() {
			var sessionID string
			if err := rows.Scan(&sessionID); err != nil {
				t.Fatal(err)
			}
			sessionIDs = append(sessionIDs, sessionID)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		return sessionIDs
	}

	ctx := context.Background()
	numNodes := 2
	testCluster := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Insecure: true,
			},
		})
	defer testCluster.Stopper().Stop(ctx)

	// Create users with various permissions.
	conn, err := testCluster.ServerConn(0).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.ExecContext(ctx, `
CREATE USER has_admin;
CREATE USER has_admin2;
CREATE USER has_cancelquery CANCELQUERY;
CREATE USER no_perms;
GRANT admin TO has_admin;
GRANT admin TO has_admin2;
`)
	if err != nil {
		t.Fatal(err)
	}

	type testCase struct {
		name          string
		user          string
		targetUser    string
		shouldSucceed bool
		expectedErrRE string
	}
	testCases := []testCase{
		{"admins can cancel other admins", "has_admin", "has_admin2", true, ""},
		{"non-admins with CANCELQUERY can cancel non-admins", "has_cancelquery", "no_perms", true,
			""},
		{"non-admins cannot cancel admins", "has_cancelquery", "has_admin", false,
			"permission denied to cancel admin session"},
		{"unpermissioned users cannot cancel other users", "no_perms", "has_cancelquery", false,
			"this operation requires CANCELQUERY privilege"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Open a session for the target user.
			targetDB := getUserConn(t, tc.targetUser, testCluster.Server(0))
			defer targetDB.Close()
			sqlutils.MakeSQLRunner(targetDB).Exec(t, `SELECT version()`)

			// Retrieve the session ID.
			var sessionID string
			testutils.SucceedsSoon(t, func() error {
				sessionIDs := getSessionIDs(t, ctx, conn, tc.targetUser)
				if len(sessionIDs) == 0 {
					return errors.New("could not find session")
				}
				if len(sessionIDs) > 1 {
					return errors.New("found multiple sessions")
				}
				sessionID = sessionIDs[0]
				return nil
			})

			// Attempt to cancel the session. We connect to the other node to make sure
			// non-local sessions can be canceled.
			db := getUserConn(t, tc.user, testCluster.Server(1))
			defer db.Close()
			runner := sqlutils.MakeSQLRunner(db)
			if tc.shouldSucceed {
				runner.Exec(t, `CANCEL SESSION $1`, sessionID)
				testutils.SucceedsSoon(t, func() error {
					sessionIDs := getSessionIDs(t, ctx, conn, tc.targetUser)
					if len(sessionIDs) > 0 {
						return errors.Errorf("expected no sessions for %q, found %d", tc.targetUser, len(sessionIDs))
					}
					return nil
				})
			} else {
				runner.ExpectErr(t, tc.expectedErrRE, `CANCEL SESSION $1`, sessionID)
			}
		})
	}
}

func TestCancelQueryPermissions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// getQueryIDs retrieves the IDs of any currently running queries for the
	// specified user.
	getQueryIDs := func(t *testing.T, ctx context.Context, conn *gosql.Conn, user string) []string {
		rows, err := conn.QueryContext(
			ctx, "SELECT query_id FROM [SHOW QUERIES] WHERE user_name = $1", user,
		)
		if err != nil {
			t.Fatal(err)
		}
		var queryIDs []string
		for rows.Next() {
			var queryID string
			if err := rows.Scan(&queryID); err != nil {
				t.Fatal(err)
			}
			queryIDs = append(queryIDs, queryID)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		return queryIDs
	}

	ctx := context.Background()
	numNodes := 2
	testCluster := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Insecure: true,
			},
		})
	defer testCluster.Stopper().Stop(ctx)

	// Create users with various permissions.
	conn, err := testCluster.ServerConn(0).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.ExecContext(ctx, `
CREATE USER has_admin;
CREATE USER has_admin2;
CREATE USER has_cancelquery CANCELQUERY;
CREATE USER no_perms;
GRANT admin TO has_admin;
GRANT admin TO has_admin2;
`)
	if err != nil {
		t.Fatal(err)
	}

	type testCase struct {
		name          string
		user          string
		targetUser    string
		shouldSucceed bool
		expectedErrRE string
	}
	testCases := []testCase{
		{"admins can cancel other admins", "has_admin", "has_admin2", true, ""},
		{"non-admins with CANCELQUERY can cancel non-admins", "has_cancelquery", "no_perms", true,
			""},
		{"non-admins cannot cancel admins", "has_cancelquery", "has_admin", false,
			"permission denied to cancel admin session"},
		{"unpermissioned users cannot cancel other users", "no_perms", "has_cancelquery", false,
			"this operation requires CANCELQUERY privilege"},
	}
	// Avoid using subtests with t.Run since we may need to access `t` after the
	// subtest is done. Use a WaitGroup to make sure the error from the pg_sleep
	// goroutine is checked.
	wg := sync.WaitGroup{}
	for _, tc := range testCases {
		func() {
			wg.Add(1)
			// Start a query with the target user.
			targetDB := getUserConn(t, tc.targetUser, testCluster.Server(0))
			defer targetDB.Close()
			go func() {
				var errRE string
				if tc.shouldSucceed {
					errRE = "query execution canceled"
				} else {
					// The query should survive until the connection gets torn down at the
					// end of the test.
					errRE = "sql: database is closed"
				}
				_, err := targetDB.ExecContext(context.Background(), "SELECT pg_sleep(100)")
				if !testutils.IsError(err, errRE) {
					t.Errorf("expected error '%s', got: %v", errRE, err)
				}
				wg.Done()
			}()

			// Retrieve the query ID.
			var queryID string
			testutils.SucceedsSoon(t, func() error {
				queryIDs := getQueryIDs(t, ctx, conn, tc.targetUser)
				if len(queryIDs) == 0 {
					return errors.New("could not find query")
				}
				if len(queryIDs) > 1 {
					return errors.New("found multiple queries")
				}
				queryID = queryIDs[0]
				return nil
			})

			// Attempt to cancel the query. We connect to the other node to make sure
			// non-local queries can be canceled.
			db := getUserConn(t, tc.user, testCluster.Server(1))
			defer db.Close()
			runner := sqlutils.MakeSQLRunner(db)
			if tc.shouldSucceed {
				runner.Exec(t, `CANCEL QUERY $1`, queryID)
			} else {
				runner.ExpectErr(t, tc.expectedErrRE, `CANCEL QUERY $1`, queryID)
			}
		}()
	}
	// Give the cancel queries a chance to propagate before stopping the cluster.
	time.Sleep(time.Second)
	testCluster.Stopper().Stop(ctx)
	wg.Wait()
}

func TestCancelIfExists(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := serverutils.StartNewTestCluster(t, 1, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())

	conn := tc.ServerConn(0)

	// Try to cancel a query that doesn't exist.
	_, err := conn.Exec("CANCEL QUERY IF EXISTS '00000000000000000000000000000001'")
	if err != nil {
		t.Fatal(err)
	}

	// Try to cancel a session that doesn't exist.
	_, err = conn.Exec("CANCEL SESSION IF EXISTS '00000000000000000000000000000001'")
	if err != nil {
		t.Fatal(err)
	}
}

func TestIdleInSessionTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	numNodes := 1
	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	clusterSettingConn := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	clusterSettingConn.Exec(t, `SET CLUSTER SETTING sql.defaults.idle_in_session_timeout = '100s'`)
	defer tc.ServerConn(0).Close()

	conn, err := tc.ServerConn(0).Conn(ctx)
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		var idleInSessionTimeoutSetting string
		err = conn.QueryRowContext(ctx, `SHOW idle_in_session_timeout`).Scan(&idleInSessionTimeoutSetting)
		require.NoError(t, err)
		if idleInSessionTimeoutSetting == "100000" {
			return nil
		}
		conn, err = tc.ServerConn(0).Conn(ctx)
		return errors.Errorf("expected idle_in_session_timeout %s, got %s", "100000", idleInSessionTimeoutSetting)
	})

	_, err = conn.ExecContext(ctx, `SET idle_in_session_timeout = '2s'`)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	// Make sure executing a statement resets the idle timer.
	_, err = conn.ExecContext(ctx, `SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	// The connection should still be alive.
	err = conn.PingContext(ctx)
	if err != nil {
		t.Fatalf("expected the connection to be alive but the connection"+
			"is dead, %v", err)
	}

	// Make sure executing BEGIN resets the idle timer.
	// BEGIN is the only statement that is not run by execStmtInOpenState.
	_, err = conn.ExecContext(ctx, `BEGIN`)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	// The connection should still be alive.
	err = conn.PingContext(ctx)
	if err != nil {
		t.Fatalf("expected the connection to be alive but the connection"+
			"is dead, %v", err)
	}

	time.Sleep(3 * time.Second)
	err = conn.PingContext(ctx)

	if err == nil {
		t.Fatal("expected the connection to be killed " +
			"but the connection is still alive")
	}
}

func TestIdleInTransactionSessionTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	numNodes := 1
	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	clusterSettingConn := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	clusterSettingConn.Exec(t, `SET CLUSTER SETTING sql.defaults.idle_in_transaction_session_timeout = '123s'`)
	defer tc.ServerConn(0).Close()

	conn, err := tc.ServerConn(0).Conn(ctx)
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		var idleInTransactionSessionTimeoutSetting string
		err = conn.QueryRowContext(ctx, `SHOW idle_in_transaction_session_timeout`).Scan(&idleInTransactionSessionTimeoutSetting)
		require.NoError(t, err)
		if idleInTransactionSessionTimeoutSetting == "123000" {
			return nil
		}
		conn, err = tc.ServerConn(0).Conn(ctx)
		require.NoError(t, err)
		return errors.Errorf("expected idle_in_transaction_session_timeout %s, got %s", "123000", idleInTransactionSessionTimeoutSetting)
	})

	_, err = conn.ExecContext(ctx, `SET idle_in_transaction_session_timeout = '2s'`)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)
	// idle_in_transaction_session_timeout should only timeout if a transaction
	// is active
	err = conn.PingContext(ctx)
	if err != nil {
		t.Fatalf("expected the connection to be alive but the connection"+
			"is dead, %v", err)
	}

	_, err = conn.ExecContext(ctx, `BEGIN`)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	// Make sure executing a statement resets the idle timer.
	_, err = conn.ExecContext(ctx, `SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	// The connection should still be alive.
	err = conn.PingContext(ctx)
	if err != nil {
		t.Fatalf("expected the connection to be alive but the connection"+
			"is dead, %v", err)
	}

	time.Sleep(3 * time.Second)
	err = conn.PingContext(ctx)
	if err == nil {
		t.Fatal("expected the connection to be killed " +
			"but the connection is still alive")
	}
}

func TestIdleInTransactionSessionTimeoutAbortedState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	numNodes := 1
	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	conn := tc.ServerConn(0)
	_, err := conn.ExecContext(ctx, `SET idle_in_transaction_session_timeout = '2s'`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecContext(ctx, `BEGIN`)
	if err != nil {
		t.Fatal(err)
	}

	// Go into state aborted.
	_, err = conn.ExecContext(ctx, `SELECT crdb_internal.force_error('', 'error')`)
	if err == nil {
		t.Fatal("unexpected success")
	}
	if err.Error() != "pq: error" {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	// Make sure executing a statement resets the idle timer.
	_, err = conn.ExecContext(ctx, `SELECT 1`)
	// Statement should execute in aborted state.
	if err == nil {
		t.Fatal("unexpected success")
	}
	if err.Error() != "pq: current transaction is aborted, commands ignored until end of transaction block" {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	// The connection should still be alive.
	err = conn.PingContext(ctx)
	if err != nil {
		t.Fatalf("expected the connection to be alive but the connection"+
			"is dead, %v", err)
	}

	time.Sleep(3 * time.Second)
	err = conn.PingContext(ctx)
	if err == nil {
		t.Fatal("expected the connection to be killed " +
			"but the connection is still alive")
	}
}

func TestIdleInTransactionSessionTimeoutCommitWaitState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	numNodes := 1
	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	conn := tc.ServerConn(0)
	_, err := conn.ExecContext(ctx, `SET idle_in_transaction_session_timeout = '2s'`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecContext(ctx, `BEGIN`)
	if err != nil {
		t.Fatal(err)
	}

	// Go into commit wait state.
	_, err = conn.ExecContext(ctx, `SAVEPOINT cockroach_restart`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecContext(ctx, `RELEASE SAVEPOINT cockroach_restart`)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	// Make sure executing a statement resets the idle timer.
	// This statement errors but should still reset the timer.
	_, err = conn.ExecContext(ctx, `SELECT 1`)
	// Statement should execute in aborted state.
	if err == nil {
		t.Fatal("unexpected success")
	}
	if err.Error() != "pq: current transaction is committed, commands ignored until end of transaction block" {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	// The connection should still be alive.
	err = conn.PingContext(ctx)
	if err != nil {
		t.Fatalf("expected the connection to be alive but the connection"+
			"is dead, %v", err)
	}

	time.Sleep(3 * time.Second)
	err = conn.PingContext(ctx)
	if err == nil {
		t.Fatal("expected the connection to be killed " +
			"but the connection is still alive")
	}
}

func TestStatementTimeoutRetryableErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	numNodes := 1
	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	clusterSettingConn := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	clusterSettingConn.Exec(t, `SET CLUSTER SETTING sql.defaults.statement_timeout = '123s'`)
	defer tc.ServerConn(0).Close()

	conn, err := tc.ServerConn(0).Conn(ctx)
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		var statementTimeoutSetting string
		err = conn.QueryRowContext(ctx, `SHOW statement_timeout`).Scan(&statementTimeoutSetting)
		require.NoError(t, err)
		if statementTimeoutSetting == "123000" {
			return nil
		}
		conn, err = tc.ServerConn(0).Conn(ctx)
		require.NoError(t, err)
		return errors.Errorf("expected statement_timeout %s, got %s", "123000", statementTimeoutSetting)
	})

	_, err = conn.QueryContext(ctx, `SET statement_timeout = '0.1s'`)
	require.NoError(t, err)

	testutils.RunTrueAndFalse(t, "test statement timeout with explicit txn",
		func(t *testing.T, explicitTxn bool) {
			query := `SELECT crdb_internal.force_retry('2s');`
			if explicitTxn {
				query = `BEGIN; ` + query + ` COMMIT;`
			}
			startTime := timeutil.Now()
			_, err = conn.QueryContext(ctx, query)
			require.Regexp(t, "pq: query execution canceled due to statement timeout", err)

			// The query timeout should be triggered and therefore the force retry
			// should not last for 2 seconds as specified.
			if timeutil.Since(startTime) >= 2*time.Second {
				t.Fatal("expected the query to error out due to the statement_timeout.")
			}
		})
}

func getUserConn(t *testing.T, username string, server serverutils.TestServerInterface) *gosql.DB {
	pgURL := url.URL{
		Scheme:   "postgres",
		User:     url.User(username),
		Host:     server.ServingSQLAddr(),
		RawQuery: "sslmode=disable",
	}
	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// TestTenantStatementTimeoutAdmissionQueueCancelation tests that a KV request
// that is canceled via a statement timeout is properly removed from the
// admission control queue. A testing filter is used to "park" a small number of
// requests thereby consuming those CPU "slots" and testing knobs are used to
// tightly control the number of entries in the queue so that we guarantee our
// main statement with a timeout is blocked.
func TestTenantStatementTimeoutAdmissionQueueCancelation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "times out under stress")

	require.True(t, buildutil.CrdbTestBuild)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tenantID := serverutils.TestTenantID()

	numBlockers := 4

	// We can't get the tableID programmatically here, checked below with assert.
	const tableID = 104
	sqlEnc := keys.MakeSQLCodec(tenantID)
	tableKey := sqlEnc.TablePrefix(tableID)
	tableSpan := roachpb.Span{Key: tableKey, EndKey: tableKey.PrefixEnd()}

	unblockClientCh := make(chan struct{})
	qBlockersCh := make(chan struct{})

	var wg sync.WaitGroup
	// +1 because we want to wait until all the queue blockers and the main
	// client goroutine finish.
	wg.Add(numBlockers + 1)

	matchBatch := func(ctx context.Context, req *roachpb.BatchRequest) bool {
		tid, ok := roachpb.TenantFromContext(ctx)
		if ok && tid == tenantID && len(req.Requests) > 0 {
			scan, ok := req.Requests[0].GetInner().(*roachpb.ScanRequest)
			if ok && tableSpan.ContainsKey(scan.Key) {
				return true
			}
		}
		return false
	}

	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			AdmissionControl: &admission.Options{
				MaxCPUSlots: numBlockers,
				// During testing if CPU isn't responsive and skipEnforcement
				// turns off admission control queuing behavior, for this test
				// to be reliable we need that to not happen.
				TestingDisableSkipEnforcement: true,
			},
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, req roachpb.BatchRequest) *roachpb.Error {
					if matchBatch(ctx, &req) {
						// Notify we're blocking.
						unblockClientCh <- struct{}{}
						<-qBlockersCh
					}
					return nil
				},
				TestingResponseErrorEvent: func(ctx context.Context, req *roachpb.BatchRequest, err error) {
					if matchBatch(ctx, req) {
						scan, ok := req.Requests[0].GetInner().(*roachpb.ScanRequest)
						if ok && tableSpan.ContainsKey(scan.Key) {
							cancel()
							wg.Done()
						}
					}
				},
			},
		},
	}

	kvserver, _, _ := serverutils.StartServer(t, params)
	defer kvserver.Stopper().Stop(context.Background())

	tenant, db := serverutils.StartTenant(t, kvserver, base.TestTenantArgs{TenantID: tenantID})
	defer db.Close()

	r1 := sqlutils.MakeSQLRunner(db)
	r1.Exec(t, `CREATE TABLE foo (t int)`)

	row := r1.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'foo'`)
	var id int64
	row.Scan(&id)
	require.Equal(t, tableID, int(id))

	makeTenantConn := func() *sqlutils.SQLRunner {
		return sqlutils.MakeSQLRunner(serverutils.OpenDBConn(t, tenant.SQLAddr(), "" /* useDatabase */, false /* insecure */, kvserver.Stopper()))
	}

	blockers := make([]*sqlutils.SQLRunner, numBlockers)
	for i := 0; i < numBlockers; i++ {
		blockers[i] = makeTenantConn()
	}
	client := makeTenantConn()
	client.Exec(t, "SET statement_timeout = 500")
	for _, r := range blockers {
		go func(r *sqlutils.SQLRunner) {
			defer wg.Done()
			r.Exec(t, `SELECT * FROM foo`)
		}(r)
	}
	// Wait till all blockers are parked.
	for i := 0; i < numBlockers; i++ {
		<-unblockClientCh
	}
	client.ExpectErr(t, "timeout", `SELECT * FROM foo`)
	// Unblock the blockers.
	for i := 0; i < numBlockers; i++ {
		qBlockersCh <- struct{}{}
	}
	wg.Wait()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}
