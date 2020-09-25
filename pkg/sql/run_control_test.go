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
	gosqldriver "database/sql/driver"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

type runControlTestCase struct {
	name  string
	conn1 func(context.Context) (*gosql.Conn, error)
	conn2 func(context.Context) (*gosql.Conn, error)
}

// makeRunControlTestCases starts a two-node test cluster and tenant and returns
// two test cases: one with two connection constructors to each node in the
// cluster, and another with two connections to the tenant's SQL pod.
func makeRunControlTestCases(t *testing.T) ([]runControlTestCase, func()) {
	t.Helper()
	testCases := make([]runControlTestCase, 2)
	tc := serverutils.StartNewTestCluster(
		t, 2 /* numNodes */, base.TestClusterArgs{ReplicationMode: base.ReplicationManual},
	)
	testCases[0].name = "SystemTenant"
	testCases[0].conn1 = tc.ServerConn(0).Conn
	testCases[0].conn2 = tc.ServerConn(1).Conn

	tenantDB := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10)})
	testCases[1].name = "Tenant"
	testCases[1].conn1 = tenantDB.Conn
	testCases[1].conn2 = tenantDB.Conn

	return testCases, func() {
		_ = tenantDB.Close()
		tc.Stopper().Stop(context.Background())
	}
}

// Dummy import to pull in kvtenantccl. This allows us to start tenants.
var _ = kvtenantccl.Connector{}

func TestCancelSelectQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const queryToCancel = "SELECT * FROM generate_series(1,20000000)"

	testCases, cleanup := makeRunControlTestCases(t)
	defer cleanup()

	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conn1, err := tc.conn1(ctx)
			require.NoError(t, err)
			conn2, err := tc.conn2(ctx)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, conn1.Close())
			}()
			defer func() {
				require.NoError(t, conn2.Close())
			}()

			sem := make(chan struct{})
			errChan := make(chan error)

			go func() {
				sem <- struct{}{}
				rows, err := conn2.QueryContext(ctx, queryToCancel)
				if err != nil {
					errChan <- err
					return
				}
				for rows.Next() {
				}
				if err = rows.Err(); err != nil {
					errChan <- err
				}
			}()

			<-sem
			time.Sleep(time.Second * 2)

			const cancelQuery = "CANCEL QUERIES SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE query LIKE '%generate_series%' AND query NOT LIKE '%CANCEL QUERIES%'"

			if _, err := conn1.ExecContext(ctx, cancelQuery); err != nil {
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
		})
	}
}

// TestCancelDistSQLQuery runs a distsql query and cancels it randomly at
// various points of execution.
func TestCancelDistSQLQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const queryToCancel = "SELECT * FROM nums ORDER BY num"
	cancelQuery := fmt.Sprintf("CANCEL QUERIES SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE query = '%s'", queryToCancel)

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
	if !isClientsideQueryCanceledErr(err) {
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
	skip.WithIssue(t, 53791, "data race")
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
			"permission denied to cancel admin query"},
		{"unpermissioned users cannot cancel other users", "no_perms", "has_cancelquery", false,
			"this operation requires CANCELQUERY privilege"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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
				sqlutils.MakeSQLRunner(targetDB).ExpectErr(t, errRE, "SELECT pg_sleep(1000000)")
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
		})
	}
}

func testCancelSession(t *testing.T, hasActiveSession bool) {
	ctx := context.Background()

	testCases, cleanup := makeRunControlTestCases(t)
	defer cleanup()

	const numNodes = 2

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Since we're testing session cancellation, use single connections instead of
			// connection pools.
			var err error
			conn1, err := tc.conn1(ctx)
			if err != nil {
				t.Fatal(err)
			}
			conn2, err := tc.conn2(ctx)
			if err != nil {
				t.Fatal(err)
			}

			// Set an explicit application name for conn2 so that we can avoid
			// canceling this session when on the same node (in tenant test cases).
			_, err = conn2.ExecContext(ctx, "SET application_name = 'protected'")
			require.NoError(t, err)

			// Wait for conn2 to know about both sessions.
			if err := retry.ForDuration(10*time.Second, func() error {
				rows, err := conn2.QueryContext(ctx, "SELECT * FROM [SHOW CLUSTER SESSIONS] WHERE application_name NOT LIKE '$%'")
				if err != nil {
					return err
				}

				m, err := sqlutils.RowsToStrMatrix(rows)
				if err != nil {
					return err
				}

				if numRows := len(m); numRows != numNodes {
					return fmt.Errorf("expected %d sessions but found %d\n%s",
						numNodes, numRows, sqlutils.MatrixToStr(m))
				}

				return nil
			}); err != nil {
				t.Fatal(err)
			}

			// Get conn1's session ID now, so that we don't need to serialize the session
			// later and race with the active query's type-checking and name resolution.
			rows, err := conn1.QueryContext(
				ctx, "SELECT session_id FROM [SHOW LOCAL SESSIONS] WHERE application_name != 'protected'",
			)
			if err != nil {
				t.Fatal(err)
			}

			var id string
			if !rows.Next() {
				t.Fatal("no sessions on node 1")
			}
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}

			// Now that we've obtained the session ID, query planning won't race with
			// session serialization, so we can kick it off now.
			errChan := make(chan error, 1)
			if hasActiveSession {
				go func() {
					var err error
					_, err = conn1.ExecContext(ctx, "SELECT pg_sleep(1000000)")
					errChan <- err
				}()
			}

			// Cancel the session on node 1.
			if _, err = conn2.ExecContext(ctx, fmt.Sprintf("CANCEL SESSION '%s'", id)); err != nil {
				t.Fatal(err)
			}

			if hasActiveSession {
				// Verify that the query was canceled because the session closed.
				err = <-errChan
			} else {
				// Verify that the connection is closed.
				_, err = conn1.ExecContext(ctx, "SELECT 1")
			}

			if !errors.Is(err, gosqldriver.ErrBadConn) {
				t.Fatalf("session not canceled; actual error: %s", err)
			}
		})
	}
}

func TestCancelMultipleSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testCases, cleanup := makeRunControlTestCases(t)
	defer cleanup()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Open two connections on node 1.
			var conns [2]*gosql.Conn
			var err error
			if conns[0], err = tc.conn1(ctx); err != nil {
				t.Fatal(err)
			}
			if conns[1], err = tc.conn1(ctx); err != nil {
				t.Fatal(err)
			}

			for i := 0; i < 2; i++ {
				if _, err := conns[i].ExecContext(ctx, "SET application_name = 'killme'"); err != nil {
					t.Fatal(err)
				}
			}

			// Open a control connection on node 2.
			ctlconn, err := tc.conn2(ctx)
			if err != nil {
				t.Fatal(err)
			}

			// Cancel the sessions on node 1.
			if _, err = ctlconn.ExecContext(ctx,
				`CANCEL SESSIONS SELECT session_id FROM [SHOW CLUSTER SESSIONS] WHERE application_name = 'killme'`,
			); err != nil {
				t.Fatal(err)
			}

			// Verify that the connections on node 1 are closed.
			for i := 0; i < 2; i++ {
				_, err := conns[i].ExecContext(ctx, "SELECT 1")
				if !errors.Is(err, gosqldriver.ErrBadConn) {
					t.Fatalf("session %d not canceled; actual error: %s", i, err)
				}
			}
		})
	}
}

func TestIdleCancelSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCancelSession(t, false /* hasActiveSession */)
}

func TestActiveCancelSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCancelSession(t, true /* hasActiveSession */)
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

	var err error

	// Try to cancel a query that doesn't exist.
	_, err = conn.Exec("CANCEL QUERY IF EXISTS '00000000000000000000000000000001'")
	if err != nil {
		t.Fatal(err)
	}

	// Try to cancel a session that doesn't exist.
	_, err = conn.Exec("CANCEL SESSION IF EXISTS '00000000000000000000000000000001'")
	if err != nil {
		t.Fatal(err)
	}
}

func isClientsideQueryCanceledErr(err error) bool {
	if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
		return pgcode.MakeCode(string(pqErr.Code)) == pgcode.QueryCanceled
	}
	return pgerror.GetPGCode(err) == pgcode.QueryCanceled
}

func TestIdleInSessionTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	numNodes := 1
	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	conn, err := tc.ServerConn(0).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

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
	ctx := context.Background()

	numNodes := 1
	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	var err error
	conn, err := tc.ServerConn(0).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecContext(ctx,
		`SET idle_in_transaction_session_timeout = '2s'`)
	if err != nil {
		t.Fatal(err)
	}

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
	ctx := context.Background()

	numNodes := 1
	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	var err error
	conn, err := tc.ServerConn(0).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecContext(ctx,
		`SET idle_in_transaction_session_timeout = '2s'`)
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
	ctx := context.Background()

	numNodes := 1
	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	var err error
	conn, err := tc.ServerConn(0).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecContext(ctx,
		`SET idle_in_transaction_session_timeout = '2s'`)
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
	ctx := context.Background()

	numNodes := 1
	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	conn, err := tc.ServerConn(0).Conn(ctx)
	require.NoError(t, err)

	_, err = conn.QueryContext(ctx,
		`SET statement_timeout = '0.1s'`)
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
