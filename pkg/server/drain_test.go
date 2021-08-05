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
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
		return errors.Newf("server not yet refusing RPC, got %v", err)
	})
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
	conn, err := grpc.Dial(s.ServingRPCAddr(), grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	client := serverpb.NewAdminClient(conn)
	return client, func() {
		_ = conn.Close() // nolint:grpcconnclose
	}, nil
}

// TestVerboseLoggingForSlowDrains checks that a slow drain triggers
// verbose logging.
//
// It achieves this by starting a 2-node cluster, constraining the
// ranges to remain on the first node, and then attempting to shut
// down the first node.
//
// FIXME(knz): different shortcomings:
// 1. the current logging for slow drains uses unstructured events.
//    When an engineer runs the test with TESTFLAGS='-vmodule=store=1'
//    the test will see the logging always, even if the code that is being tested
//    does not work (because of the log.V condition).
// 2. this test code is creating a constraint on the text of unstructured
//    logging events. We usually don't do that - the DEV channel is meant
//    as a "free for all" for engineers without guarantees.
//
// Both problems can be solved by using a structured event instead.
func TestVerboseLoggingForSlowDrains(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test takes up to 20 seconds. Don't run it in short mode.
	skip.UnderShort(t)

	// Avoid log configs that disable logging output to files.
	defer log.ScopeWithoutShowLogs(t).Close(t)

	var (
		ctx                  = context.Background()
		verboseStoreLogRe    = regexp.MustCompile("failed to transfer lease")
		leaseTransferBlocked syncutil.AtomicBool
	)

	preEvalFilter := func(args kvserverbase.FilterArgs) *roachpb.Error {
		if args.Req.Method() != roachpb.TransferLease {
			// Not a lease transfer: process as usual.
			return nil
		}

		// Lease transfer blocked?
		if leaseTransferBlocked.Get() {
			// Yes: fake a failure.
			return roachpb.NewErrorf("hello!")
		}

		// No: let proceed as usual.
		return nil
	}

	t.Logf("starting cluster...")
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
							TestingEvalFilter: preEvalFilter,
						},
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	t.Logf("waiting for initial up-replication...")
	if err := tc.WaitForFullReplication(); err != nil {
		log.Fatalf(ctx, "while waiting for full replication: %v", err)
	}

	firstServer := tc.Server(0).(*server.TestServer)

	leaseTransferBlocked.Set(true)

	// First drain call: always available work.
	_, _, err := firstServer.Drain(ctx, false /* verbose */)
	if err != nil {
		t.Fatal(err)
	}
	// Second drain call: no queries any more. All of the remaining the work is transferring leases.
	remaining, _, err := firstServer.Drain(ctx, false /* verbose */)
	if err != nil {
		t.Fatal(err)
	}

	// Check that there is not additional logging yet.
	t.Logf("checking for absence of event in log files...")
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 10000, verboseStoreLogRe,
		log.WithMarkedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatal("log entries found to match regexp, when no log entry was expected")
	}

	// Third drain call: since the transfers are blocked,
	// we should see the same value as previously.
	remaining2, _, err := firstServer.Drain(ctx, true /* verbose */)
	if err != nil {
		t.Fatal(err)
	}
	if remaining2 < remaining {
		t.Fatalf("expected drain to stall; instead found progress %d -> %d", remaining, remaining2)
	}

	// Check that we find verbose log entries for the stalling drain.
	t.Logf("checking for event in log files...")
	entries, err = log.FetchEntriesFromFiles(0, math.MaxInt64, 10000, verboseStoreLogRe,
		log.WithMarkedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("no log entries matching the regexp")
	}
}
