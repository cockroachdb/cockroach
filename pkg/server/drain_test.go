// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

// TestDrain tests the Drain RPC.
func TestDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	doTestDrain(t)
}

// doTestDrain runs the drain test.
func doTestDrain(tt *testing.T) {
	var drainSleepCallCount = 0
	t := newTestDrainContext(tt, &drainSleepCallCount)
	defer t.Close()

	// Issue a probe. We're not draining yet, so the probe should
	// reflect that.
	resp := t.sendProbe()
	t.assertDraining(resp, false)
	t.assertRemaining(resp, false)
	t.assertEqual(0, drainSleepCallCount)

	// Issue a drain without shutdown, so we can probe more afterwards.
	resp = t.sendDrainNoShutdown()
	t.assertDraining(resp, true)
	t.assertRemaining(resp, true)
	t.assertEqual(1, drainSleepCallCount)

	// Issue another probe. This checks that the server is still running
	// (i.e. Shutdown: false was effective), the draining status is
	// still properly reported, and that the server only slept once (which only
	// should occur on the first drain).
	resp = t.sendProbe()
	t.assertDraining(resp, true)
	// probe-only has no remaining.
	t.assertRemaining(resp, false)
	t.assertEqual(1, drainSleepCallCount)

	// Repeat drain commands until we verify that there are zero remaining leases
	// (i.e. complete). Also validate that the server did not sleep again.
	testutils.SucceedsSoon(t, func() error {
		resp = t.sendDrainNoShutdown()
		if !resp.IsDraining {
			return errors.Newf("expected draining")
		}
		if resp.DrainRemainingIndicator > 0 {
			return errors.Newf("still %d remaining, desc: %s", resp.DrainRemainingIndicator,
				resp.DrainRemainingDescription)
		}
		return nil
	})
	t.assertEqual(1, drainSleepCallCount)

	// Now issue a drain request without drain but with shutdown.
	// We're expecting the node to be shut down after that.
	resp = t.sendShutdown()
	if resp != nil {
		t.assertDraining(resp, true)
		t.assertRemaining(resp, false)
		t.assertEqual(1, drainSleepCallCount)
	}

	// Now expect the server to be shut down.
	testutils.SucceedsSoon(t, func() error {
		_, err := t.c.Drain(context.Background(), &serverpb.DrainRequest{Shutdown: false})
		if grpcutil.IsClosedConnection(err) {
			return nil
		}
		// It is incorrect to use errors.Wrap since that will result in a nil
		// return value if err is nil, which is not desired.
		return errors.Newf("server not yet refusing RPC, got %v", err) // nolint:errwrap
	})
}

func TestEnsureSQLStatsAreFlushedDuringDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var drainSleepCallCount = 0
	drainCtx := newTestDrainContext(t, &drainSleepCallCount)
	defer drainCtx.Close()

	var (
		ts    = drainCtx.tc.Server(0).SQLServer().(*sql.Server)
		sqlDB = sqlutils.MakeSQLRunner(drainCtx.tc.ServerConn(0))
	)

	// Issue queries to be registered in stats.
	sqlDB.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (x INT PRIMARY KEY);
INSERT INTO t.test VALUES (1);
INSERT INTO t.test VALUES (2);
INSERT INTO t.test VALUES (3);
`)

	// Find the in-memory stats for the queries.
	stats, err := ts.GetScrubbedStmtStats(ctx)
	require.NoError(t, err)
	require.Truef(t,
		func(stats []appstatspb.CollectedStatementStatistics) bool {
			for _, stat := range stats {
				if stat.Key.Query == "INSERT INTO _ VALUES (_)" {
					return true
				}
			}
			return false
		}(stats),
		"expected to find in-memory stats",
	)

	// Sanity check: verify that the statement statistics system table is empty.
	sqlDB.CheckQueryResults(t,
		`SELECT count(*) FROM system.statement_statistics WHERE node_id = 1`,
		[][]string{{"0"}},
	)

	// Issue a drain.
	drainCtx.sendDrainNoShutdown()

	// Open a new SQL connection.
	sqlDB = sqlutils.MakeSQLRunner(drainCtx.tc.ServerConn(1))

	if sqlstats.GatewayNodeEnabled.Get(&drainCtx.tc.Servers[0].ClusterSettings().SV) {
		// Check that the stats were flushed into the statement stats system table.
		// Verify that the number of statistics for node 1 are non-zero.
		sqlDB.CheckQueryResults(t,
			`SELECT count(*) > 0 FROM system.statement_statistics WHERE node_id = 1`,
			[][]string{{"true"}},
		)
	} else {
		// Check that the stats were flushed into the statement stats system table.
		// Verify that the number of statistics for node 1 are non-zero.
		sqlDB.CheckQueryResults(t,
			`SELECT count(*) > 0 FROM system.statement_statistics WHERE node_id = 0 AND statistics -> 'statistics' ->> 'nodes' = '[1]'`,
			[][]string{{"true"}},
		)
	}

}

type testDrainContext struct {
	*testing.T
	tc         *testcluster.TestCluster
	c          serverpb.AdminClient
	connCloser func()
}

func newTestDrainContext(t *testing.T, drainSleepCallCount *int) *testDrainContext {
	tc := &testDrainContext{
		T: t,
		tc: testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			// We need to start the cluster insecure in order to not
			// care about TLS settings for the RPC client connection.
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						DrainSleepFn: func(time.Duration) {
							*drainSleepCallCount++
						},
					},
				},
				Insecure: true,
			},
		}),
	}

	// We'll have the RPC talk to the first node.
	tc.c = tc.tc.Server(0).GetAdminClient(t)
	tc.connCloser = func() {}

	return tc
}

func (t *testDrainContext) Close() {
	if t.connCloser != nil {
		t.connCloser()
	}
	t.tc.Stopper().Stop(context.Background())
}

func (t *testDrainContext) sendProbe() *serverpb.DrainResponse {
	return t.drainRequest(false /* drain */, false /* shutdown */)
}

func (t *testDrainContext) sendDrainNoShutdown() *serverpb.DrainResponse {
	return t.drainRequest(true /* drain */, false /* shutdown */)
}

func (t *testDrainContext) drainRequest(drain, shutdown bool) *serverpb.DrainResponse {
	// Issue a simple drain probe.
	req := &serverpb.DrainRequest{Shutdown: shutdown}

	if drain {
		req.DoDrain = true
	}

	drainStream, err := t.c.Drain(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := t.getDrainResponse(drainStream)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func (t *testDrainContext) sendShutdown() *serverpb.DrainResponse {
	req := &serverpb.DrainRequest{Shutdown: true}
	drainStream, err := t.c.Drain(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := t.getDrainResponse(drainStream)
	if err != nil {
		// It's possible we're getting "connection reset by peer" or some
		// gRPC initialization failure because the server is shutting
		// down. Tolerate that.
		log.Infof(context.Background(), "RPC error: %v", err)
	}
	return resp
}

func (t *testDrainContext) assertDraining(resp *serverpb.DrainResponse, drain bool) {
	t.Helper()
	if resp.IsDraining != drain {
		t.Fatalf("expected draining %v, got %v", drain, resp.IsDraining)
	}
}

func (t *testDrainContext) assertRemaining(resp *serverpb.DrainResponse, remaining bool) {
	t.Helper()
	if actualRemaining := (resp.DrainRemainingIndicator > 0); remaining != actualRemaining {
		t.Fatalf("expected remaining %v, got %v", remaining, actualRemaining)
	}
}

func (t *testDrainContext) assertEqual(expected int, actual int) {
	t.Helper()
	if expected == actual {
		return
	}
	t.Fatalf("expected sleep call count to be %v, got %v", expected, actual)
}

func (t *testDrainContext) getDrainResponse(
	stream serverpb.Admin_DrainClient,
) (*serverpb.DrainResponse, error) {
	resp, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	unexpected, err := stream.Recv()
	if err != io.EOF {
		if unexpected != nil {
			t.Fatalf("unexpected additional response: %# v // %v", pretty.Formatter(unexpected), err)
		}
		if err == nil {
			err = errors.New("unexpected response")
		}
		return nil, err
	}
	return resp, nil
}

func TestServerShutdownReleasesSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	tenantArgs := base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
	}

	tenant, tenantSQLRaw := serverutils.StartTenant(t, s, tenantArgs)
	defer tenant.AppStopper().Stop(ctx)
	tenantSQL := sqlutils.MakeSQLRunner(tenantSQLRaw)

	queryOwner := func(id base.SQLInstanceID) (owner *string) {
		tenantSQL.QueryRow(t, "SELECT session_id FROM system.sql_instances WHERE id = $1", id).Scan(&owner)
		return owner
	}

	sessionExists := func(session string) bool {
		rows := tenantSQL.QueryStr(t, "SELECT session_id FROM system.sqlliveness WHERE session_id = $1", session)
		return 0 < len(rows)
	}

	tmpTenant, err := s.TenantController().StartTenant(ctx, tenantArgs)
	require.NoError(t, err)

	tmpSQLInstance := tmpTenant.SQLInstanceID()
	session := queryOwner(tmpSQLInstance)
	require.NotNil(t, session)
	require.True(t, sessionExists(*session))

	require.NoError(t, tmpTenant.DrainClients(context.Background()))
	tmpTenant.AppStopper().Stop(ctx)

	require.False(t, sessionExists(*session), "expected session %s to be deleted from the sqlliveness table, but it still exists", *session)
	require.Nil(t, queryOwner(tmpSQLInstance), "expected sql_instance %d to have no owning session_id", tmpSQLInstance)
}

// Verify that drain works correctly even if we don't start the sql instance.
func TestNoSQLServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
				DisableSQLServer:  true,
			},
		})

	defer tc.Stopper().Stop(ctx)
	req := serverpb.DrainRequest{Shutdown: false, DoDrain: true, NodeId: "2"}
	drainStream, err := tc.Server(0).ApplicationLayer().GetAdminClient(t).Drain(ctx, &req)
	require.NoError(t, err)
	// When we get this next response the drain has started - check the error.
	drainResp, err := drainStream.Recv()
	require.NoError(t, err)
	require.True(t, drainResp.IsDraining)
}
