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
	if resp.IsDraining != drain {
		t.Fatalf("expected draining %v, got %v", drain, resp.IsDraining)
	}
}

func (t *testDrainContext) assertRemaining(resp *serverpb.DrainResponse, remaining bool) {
	if actualRemaining := (resp.DrainRemainingIndicator > 0); remaining != actualRemaining {
		t.Fatalf("expected remaining %v, got %v", remaining, actualRemaining)
	}
}

func (t *testDrainContext) assertEqual(expected int, actual int) {
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
