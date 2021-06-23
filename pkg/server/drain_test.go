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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"google.golang.org/grpc"
)

// TestDrain tests the Drain RPC.
func TestDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	doTestDrain(t)
}

// doTestDrain runs the drain test.
func doTestDrain(tt *testing.T) {
	t := newTestDrainContext(tt)
	defer t.Close()

	// Issue a probe. We're not draining yet, so the probe should
	// reflect that.
	resp := t.sendProbe()
	t.assertDraining(resp, false)
	t.assertRemaining(resp, false)

	// Issue a drain without shutdown, so we can probe more afterwards.
	resp = t.sendDrainNoShutdown()
	t.assertDraining(resp, true)
	t.assertRemaining(resp, true)

	// Issue another probe. This checks that the server is still running
	// (i.e. Shutdown: false was effective) and also that the draining
	// status is still properly reported.
	resp = t.sendProbe()
	t.assertDraining(resp, true)
	// probe-only has no remaining.
	t.assertRemaining(resp, false)

	// Issue another drain. Verify that the remaining is zero (i.e. complete).
	resp = t.sendDrainNoShutdown()
	t.assertDraining(resp, true)
	t.assertRemaining(resp, false)

	// Now issue a drain request without drain but with shutdown.
	// We're expecting the node to be shut down after that.
	resp = t.sendShutdown()
	if resp != nil {
		t.assertDraining(resp, true)
		t.assertRemaining(resp, false)
	}

	// Now expect the server to be shut down.
	testutils.SucceedsSoon(t, func() error {
		_, err := t.c.Drain(context.Background(), &serverpb.DrainRequest{Shutdown: false})
		if grpcutil.IsClosedConnection(err) {
			return nil
		}
		return errors.Newf("server not yet refusing RPC, got %v", err)
	})
}

type testDrainContext struct {
	*testing.T
	tc         *testcluster.TestCluster
	c          serverpb.AdminClient
	connCloser func()
}

func newTestDrainContext(t *testing.T) *testDrainContext {
	tc := &testDrainContext{
		T: t,
		tc: testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			// We need to start the cluster insecure in order to not
			// care about TLS settings for the RPC client connection.
			ServerArgs: base.TestServerArgs{
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
	if resp.IsDraining != drain {
		t.Fatalf("expected draining %v, got %v", drain, resp.IsDraining)
	}
}

func (t *testDrainContext) assertRemaining(resp *serverpb.DrainResponse, remaining bool) {
	if actualRemaining := (resp.DrainRemainingIndicator > 0); remaining != actualRemaining {
		t.Fatalf("expected remaining %v, got %v", remaining, actualRemaining)
	}
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
	conn, err := grpc.Dial(s.ServingRPCAddr(), grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	client := serverpb.NewAdminClient(conn)
	return client, func() {
		_ = conn.Close() // nolint:grpcconnclose
	}, nil
}

// TestRangeCacheUpdateWithNLEAfterDrain checks that in a 3-node cluster, an observer node that
// does not witness the lease transfers during a drain, properly
// experiences a "fast" NLE at the end of the drain.
func TestRangeCacheUpdateWithNLEAfterDrain(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 4, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0 /* n1 */ : {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "storage", Value: "true"}, {Key: "node", Value: "draining"}}}},
			1 /* n2 */ : {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "storage", Value: "true"}, {Key: "node", Value: "target"}}}},
			2 /* n3 */ : {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "storage", Value: "true"}, {Key: "node", Value: "spare"}}}},
			3 /* n4 */ : {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "storage", Value: "false"}, {Key: "node", Value: "observer"}}}},
		},
	})
	defer tc.Stopper().Stop(ctx)

	t.Logf("setting up data...")
	s0 := tc.Server(0).(*server.TestServer)
	if err := s0.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
		for _, stmt := range []string{
			// Adjust the zone configs so that data spreads over the storage
			// nodes, excluding the observer node.
			`ALTER RANGE meta CONFIGURE ZONE USING constraints = '[+storage=true]'`,
			`ALTER RANGE system CONFIGURE ZONE USING constraints = '[+storage=true]'`,
			`ALTER DATABASE system CONFIGURE ZONE USING constraints = '[+storage=true]'`,
			`ALTER RANGE liveness CONFIGURE ZONE USING constraints = '[+storage=true]'`,
			`ALTER RANGE default CONFIGURE ZONE USING constraints = '[+storage=true]'`,
			// Create a table and make its lease live on the draining node.
			`CREATE TABLE defaultdb.public.t(x INT PRIMARY KEY)`,
			`ALTER TABLE defaultdb.public.t CONFIGURE ZONE USING num_replicas = 3, constraints = '[+storage=true]', lease_preferences = '[[+node=draining]]'`,
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

	t.Logf("populating cache on observer node")
	// Now query the newly created table from the observer node. This
	// populates the range cache on that node.
	sObserver := tc.Server(3).(*server.TestServer)
	if err := sObserver.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
		_, err := ie.Exec(ctx, "populate-cache", nil, `TABLE defaultdb.public.t`)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	t.Logf("unlocking range")
	// Now remove the lease preference from the first node, so the lease freely can move to the target node.
	if err := s0.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
		_, err := ie.Exec(ctx, "move-range", nil,
			`ALTER TABLE defaultdb.public.t CONFIGURE ZONE USING constraints = '[+storage=true]', lease_preferences = '[]'`)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Now drain the first node asynchronously.
	var nodeStopped syncutil.AtomicBool
	tc.Stopper().RunAsyncTask(ctx, "drain-first-node", func(ctx context.Context) {
		t.Logf("starting async drain on first node")
		for {
			remaining, _, err := s0.Drain(ctx)
			if err != nil {
				t.Logf("graceful drain failed: %v", err)
				break
			}
			if remaining == 0 {
				break
			}
		}
		t.Logf("async drain complete; node stopping")
		tc.StopServer(0)
		nodeStopped.Set(true)
	})

	t.Logf("waiting for transfer to complete")
	// Wait for the lease transfer phase during the drain to complete,
	// but before the final wait on the node.
	testutils.SucceedsSoon(t, func() error {
		if !s0.TestingLeaseTransferDoneAfterDrain.Get() {
			return errors.New("leases not transferred yet")
		}
		return nil
	})

	t.Logf("asserting that the lease has arrived in the right place")
	// As a sanity check, assert that the lease has landed on the target node.
	// In particular we don't want it on the observer node, because it
	// would give us a false negative on the test result.
	sTarget := tc.Server(1).(*server.TestServer)
	if err := sTarget.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
		_, err := ie.Exec(ctx, "wait-lease", nil, `
SELECT IF(lease_holder = 1,
          crdb_internal.force_error('UU000', 'not moved to target node'),
          0)
  FROM [SHOW RANGES FROM TABLE defaultdb.public.t]`)
		if err != nil && !testutils.IsError(err, "not moved to target node") {
			t.Fatal(err)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// If the node has already stopped at this point, it's too late for
	// the test. The query below is bound to get bogus results.
	if nodeStopped.Get() {
		t.Error("TEST DESIGN ERROR: node already stopped; too late for checking")
	}

	t.Logf("checking query speed on observer node")
	qStart := timeutil.Now()
	if err := sObserver.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
		_, err := ie.Exec(ctx, "reread-table", nil, `TABLE defaultdb.public.t`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	qEnd := timeutil.Now()
	if qDur := qEnd.Sub(qStart); qDur > 20*time.Millisecond {
		t.Error("query took too long, maybe NLE redirect did not refresh the cache")
	}
}
