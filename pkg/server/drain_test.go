// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestDrain tests the Drain RPC.
func TestDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRaceWithIssue(t, 86974, "flaky test")
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
	// still properly reported, and the server slept once.
	resp = t.sendProbe()
	t.assertDraining(resp, true)
	// probe-only has no remaining.
	t.assertRemaining(resp, false)
	t.assertEqual(1, drainSleepCallCount)

	// Issue another drain. Verify that the remaining is zero (i.e. complete)
	// and that the server did not sleep again.
	resp = t.sendDrainNoShutdown()
	t.assertDraining(resp, true)
	t.assertRemaining(resp, false)
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
		func(stats []roachpb.CollectedStatementStatistics) bool {
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

	// Check that the stats were flushed into the statement stats system table.
	// Verify that the number of statistics for node 1 are non-zero.
	sqlDB.CheckQueryResults(t,
		`SELECT count(*) > 0 FROM system.statement_statistics WHERE node_id = 1`,
		[][]string{{"true"}},
	)
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
	var err error
	tc.c, tc.connCloser, err = getAdminClientForServer(tc.tc.Server(0))
	if err != nil {
		tc.Close()
		t.Fatal(err)
	}

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

func getAdminClientForServer(
	s serverutils.TestServerInterface,
) (c serverpb.AdminClient, closer func(), err error) {
	//lint:ignore SA1019 grpc.WithInsecure is deprecated
	conn, err := grpc.Dial(s.ServingRPCAddr(), grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	client := serverpb.NewAdminClient(conn)
	return client, func() {
		_ = conn.Close() // nolint:grpcconnclose
	}, nil
}

// TestDrainIgnoresExpiredLeases checks that the drain process
// does not linger forever in the presence of expired leases.
func TestDrainIgnoresExpiredLeases(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()
	const numServers int = 4
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "node", Value: "n" + strconv.FormatInt(int64(i+1), 10)}}},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: t.Name() + strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyEngineRegistry: stickyEngineRegistry,
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, 4, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationAuto,
		ServerArgsPerNode: stickyServerArgs,
	})
	defer tc.Stopper().Stop(ctx)

	t.Logf("setting up data...")
	s0 := tc.Server(0).(*server.TestServer)
	if err := s0.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
		for _, stmt := range []string{
			// Adjust the zone configs so that data spreads over the storage
			// nodes, excluding the observer node.
			`ALTER RANGE default CONFIGURE ZONE USING constraints = '[-node=n4]'`,
			// Create a table and make its lease live on the draining node.
			`CREATE TABLE defaultdb.public.t(x INT PRIMARY KEY)`,
			`ALTER TABLE defaultdb.public.t CONFIGURE ZONE USING num_replicas = 3, constraints = '[-node=n4]', lease_preferences = '[[+node=n1]]'`,
			`INSERT INTO defaultdb.public.t(x) SELECT generate_series(1,10000)`,
		} {
			if _, err := ie.Exec(ctx, "set-zone", nil, stmt); err != nil {
				return errors.Wrap(err, stmt)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	t.Logf("waiting for up-replication")
	// Wait for the newly created table to spread over all nodes.
	testutils.SucceedsSoon(t, func() error {
		return s0.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
			_, err := ie.Exec(ctx, "wait-replication", nil, `
SELECT -- wait for up-replication.
       IF(array_length(replicas, 1) != 3,
          crdb_internal.force_error('UU000', 'not ready: ' || array_length(replicas, 1)::string || ' replicas'),

          -- once up-replication is reached, ensure that we got the replicas where we wanted.
          IF(replicas != '{1,2,3}'::INT[] OR lease_holder != 1,
             crdb_internal.force_Error('UU000', 'zone config not applied properly: ' || replicas::string || ' / lease at n' || lease_holder::int),
             0))
  FROM [SHOW RANGES FROM TABLE defaultdb.public.t]`)
			if err != nil && !testutils.IsError(err, "not ready") {
				t.Fatal(err)
			}
			return err
		})
	})

	t.Logf("stopping the draining node")
	tc.StopServer(0)

	t.Logf("unlocking range")
	// Now remove the lease preference from the first node, so the lease freely can move to one of the follower nodes
	sSpare := tc.Server(3).(*server.TestServer)
	if err := sSpare.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
		_, err := ie.Exec(ctx, "release-range", nil,
			`ALTER TABLE defaultdb.public.t CONFIGURE ZONE USING constraints = '[]', lease_preferences = '[]'`)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	t.Logf("waiting for range to get a lease on a remaining node")
	// As a sanity check, assert that the lease has landed on the target node.
	// In particular we don't want it on the observer node, because it
	// would give us a false negative on the test result.
	testutils.SucceedsSoon(t, func() error {
		return sSpare.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
			if _, err := ie.Exec(ctx, "bring-lease", nil, `TABLE defaultdb.public.t`); err != nil {
				t.Fatal(err)
			}

			_, err := ie.Exec(ctx, "wait-replication", nil, `
SELECT IF(lease_holder = 1,
          crdb_internal.force_error('UU000', 'not ready'),
          0)
  FROM [SHOW RANGES FROM TABLE defaultdb.public.t]`)
			if err != nil && !testutils.IsError(err, "not ready") {
				t.Fatal(err)
			}
			return err
		})
	})

	t.Logf("restarting all the nodes so we get invalid leases everywhere")
	for i := 1; i < 3; i++ {
		t.Logf("restarting node %d", i+1)
		tc.StopServer(i)
		tc.RestartServer(i)
	}
	t.Logf("restarting first node")
	tc.RestartServer(0)

	t.Logf("now drain the server -- this is expected to succeed quickly even though some leases are now invalid")
	s0 = tc.Server(0).(*server.TestServer)
	testutils.SucceedsSoon(t, func() error {
		remaining, _, err := s0.Drain(ctx)
		if err != nil {
			return err
		}
		if remaining == 0 {
			// No more work to do.
			return nil
		}
		return errors.Newf("work left to do: %d", remaining)
	})
	t.Logf("drain complete")
}

// TestDrainIgnoresExpiredQuietLeases is like
// TestDrainIgnoresExpiredLeases except that it lets the leases on all
// participating nodes become invalid during the drain.
func TestDrainIgnoresExpiredQuietLeases(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()
	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "node", Value: "n" + strconv.FormatInt(int64(i+1), 10)}}},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: t.Name() + strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyEngineRegistry: stickyEngineRegistry,
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, 4, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationAuto,
		ServerArgsPerNode: stickyServerArgs,
	})
	defer tc.Stopper().Stop(ctx)

	t.Logf("setting up data...")
	s0 := tc.Server(0).(*server.TestServer)
	if err := s0.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
		for _, stmt := range []string{
			// Adjust the zone configs so that data spreads over the storage
			// nodes, excluding the observer node.
			// Create a table and make its lease live on the draining node.
			`CREATE TABLE defaultdb.public.t(x INT PRIMARY KEY)`,
			`ALTER TABLE defaultdb.public.t CONFIGURE ZONE USING num_replicas = 3, constraints = '[]', lease_preferences = '[[+node=n1]]'`,
			`INSERT INTO defaultdb.public.t(x) SELECT generate_series(1,10000)`,
		} {
			if _, err := ie.Exec(ctx, "set-zone", nil, stmt); err != nil {
				return errors.Wrap(err, stmt)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	t.Logf("waiting for up-replication")
	// Wait for the newly created table to spread over all nodes.
	testutils.SucceedsSoon(t, func() error {
		return s0.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
			_, err := ie.Exec(ctx, "wait-replication", nil, `
SELECT -- wait for up-replication.
       IF(array_length(replicas, 1) != 3,
          crdb_internal.force_error('UU000', 'not ready: ' || array_length(replicas, 1)::string || ' replicas'),

          -- once up-replication is reached, ensure that we got the replicas where we wanted.
          IF(replicas != '{1,2,3}'::INT[] OR lease_holder != 1,
             crdb_internal.force_Error('UU000', 'zone config not applied properly: ' || replicas::string || ' / lease at n' || lease_holder::int),
             0))
  FROM [SHOW RANGES FROM TABLE defaultdb.public.t]`)
			if err != nil && !testutils.IsError(err, "not ready") {
				t.Fatal(err)
			}
			return err
		})
	})

	t.Logf("restarting all the nodes so we get invalid leases everywhere")
	for i := 0; i < 3; i++ {
		t.Logf("restarting node %d", i+1)
		tc.StopServer(i)
		tc.RestartServer(i)
	}

	t.Logf("now drain the server -- this is expected to succeed quickly even though some leases are now invalid")
	s0 = tc.Server(0).(*server.TestServer)
	testutils.SucceedsSoon(t, func() error {
		remaining, _, err := s0.Drain(ctx)
		if err != nil {
			return err
		}
		if remaining == 0 {
			// No more work to do.
			return nil
		}
		return errors.Newf("work left to do: %d", remaining)
	})
	t.Logf("drain complete")
}
