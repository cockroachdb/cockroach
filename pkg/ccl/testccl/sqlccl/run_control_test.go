// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl_test

import (
	"context"
	gosql "database/sql"
	gosqldriver "database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
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

	_, tenantDB := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{TenantID: serverutils.TestTenantID()})
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
				if !sqltestutils.IsClientSideQueryCanceledErr(err) {
					t.Fatal(err)
				}
			case <-time.After(time.Second * 5):
				t.Fatal("no error received from query supposed to be canceled")
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
				if err := rows.Err(); err != nil {
					t.Fatalf("unexpected error querying sessions: %s", err.Error())
				} else {
					t.Fatal("no sessions on node 1")
				}
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
