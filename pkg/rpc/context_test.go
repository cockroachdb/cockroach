// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/logtags"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// AddTestingDialOpts adds extra dialing options to the rpc Context. This should
// be done before GRPCDial is called.
func (ctx *Context) AddTestingDialOpts(opts ...grpc.DialOption) {
	ctx.testingDialOpts = append(ctx.testingDialOpts, opts...)
}

func newTestServer(t testing.TB, ctx *Context, extraOpts ...grpc.ServerOption) *grpc.Server {
	tlsConfig, err := ctx.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	opts := []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.StatsHandler(&ctx.stats),
	}
	opts = append(opts, extraOpts...)
	return grpc.NewServer(opts...)
}

func newTestContext(clusterID uuid.UUID, clock *hlc.Clock, stopper *stop.Stopper) *Context {
	rctx := NewContext(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		testutils.NewNodeTestBaseContext(),
		clock,
		stopper,
		&cluster.MakeTestingClusterSettings().Version,
	)
	// Ensure that tests using this test context and restart/shut down
	// their servers do not inadvertently start talking to servers from
	// unrelated concurrent tests.
	rctx.ClusterID.Set(context.TODO(), clusterID)

	return rctx
}

func TestHeartbeatCB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "compression", func(t *testing.T, compression bool) {
		stopper := stop.NewStopper()
		defer stopper.Stop(context.TODO())

		// Shared cluster ID by all RPC peers (this ensures that the peers
		// don't talk to servers from unrelated tests by accident).
		clusterID := uuid.MakeV4()

		clock := hlc.NewClock(timeutil.Unix(0, 20).UnixNano, time.Nanosecond)
		serverCtx := newTestContext(clusterID, clock, stopper)
		serverCtx.rpcCompression = compression
		const serverNodeID = 1
		serverCtx.NodeID.Set(context.TODO(), serverNodeID)
		s := newTestServer(t, serverCtx)
		RegisterHeartbeatServer(s, &HeartbeatService{
			clock:              clock,
			remoteClockMonitor: serverCtx.RemoteClocks,
			clusterID:          &serverCtx.ClusterID,
			nodeID:             &serverCtx.NodeID,
			version:            serverCtx.version,
		})

		ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
		if err != nil {
			t.Fatal(err)
		}
		remoteAddr := ln.Addr().String()

		// Clocks don't matter in this test.
		clientCtx := newTestContext(clusterID, clock, stopper)
		clientCtx.rpcCompression = compression

		var once sync.Once
		ch := make(chan struct{})

		clientCtx.HeartbeatCB = func() {
			once.Do(func() {
				close(ch)
			})
		}

		if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID).Connect(context.Background()); err != nil {
			t.Fatal(err)
		}

		<-ch
	})
}

type internalServer struct{}

func (*internalServer) Batch(
	context.Context, *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	return nil, nil
}

func (*internalServer) RangeFeed(
	_ *roachpb.RangeFeedRequest, _ roachpb.Internal_RangeFeedServer,
) error {
	panic("unimplemented")
}

// TestInternalServerAddress verifies that RPCContext uses AdvertiseAddr, not Addr, to
// determine whether to apply the local server optimization.
//
// Prevents regression of https://github.com/cockroachdb/cockroach/issues/19991.
func TestInternalServerAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(timeutil.Unix(0, 1).UnixNano, time.Nanosecond)

	serverCtx := newTestContext(uuid.MakeV4(), clock, stopper)
	serverCtx.Config.Addr = "127.0.0.1:9999"
	serverCtx.Config.AdvertiseAddr = "127.0.0.1:8888"
	serverCtx.NodeID.Set(context.TODO(), 1)

	internal := &internalServer{}
	serverCtx.SetLocalInternalServer(internal)

	exp := internalClientAdapter{internal}
	if ic := serverCtx.GetLocalInternalClientForAddr(serverCtx.Config.AdvertiseAddr, 1); ic != exp {
		t.Fatalf("expected %+v, got %+v", exp, ic)
	}
}

// TestHeartbeatHealth verifies that the health status changes after
// heartbeats succeed or fail.
func TestHeartbeatHealth(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(timeutil.Unix(0, 1).UnixNano, time.Nanosecond)

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	const serverNodeID = 1
	const clientNodeID = 2

	serverCtx := newTestContext(clusterID, clock, stop.NewStopper())
	serverCtx.NodeID.Set(context.TODO(), serverNodeID)
	s := newTestServer(t, serverCtx)

	heartbeat := &ManualHeartbeatService{
		ready:              make(chan error),
		stopper:            stopper,
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		version:            serverCtx.version,
		nodeID:             &serverCtx.NodeID,
	}
	RegisterHeartbeatServer(s, heartbeat)

	errFailedHeartbeat := errors.New("failed heartbeat")

	var hbSuccess atomic.Value
	hbSuccess.Store(true)

	go func() {
		for {
			var err error
			if !hbSuccess.Load().(bool) {
				err = errFailedHeartbeat
			}

			select {
			case <-stopper.ShouldStop():
				return
			case heartbeat.ready <- err:
			}
		}
	}()

	lisNotLocalServer, err := net.Listen("tcp", "127.0.0.1:0")
	defer func() {
		netutil.FatalIfUnexpected(lisNotLocalServer.Close())
	}()
	if err != nil {
		t.Fatal(err)
	}
	lisLocalServer, err := net.Listen("tcp", "127.0.0.1:0")
	defer func() {
		netutil.FatalIfUnexpected(lisLocalServer.Close())
	}()
	if err != nil {
		t.Fatal(err)
	}

	clientCtx := newTestContext(clusterID, clock, stopper)
	clientCtx.NodeID.Set(context.TODO(), clientNodeID)
	clientCtx.Addr = lisNotLocalServer.Addr().String()
	clientCtx.AdvertiseAddr = lisLocalServer.Addr().String()
	// Make the interval shorter to speed up the test.
	clientCtx.heartbeatInterval = 1 * time.Millisecond

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()
	if _, err := clientCtx.GRPCDialNode(
		remoteAddr, serverNodeID).Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Wait for the connection.
	testutils.SucceedsSoon(t, func() error {
		err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
		if err != nil && err != ErrNotHeartbeated {
			t.Fatal(err)
		}
		return err
	})
	assertGauges(t, clientCtx.Metrics(),
		0 /* initializing */, 1 /* nominal */, 0 /* failed */)

	// Should be unhealthy in the presence of failing heartbeats.
	hbSuccess.Store(false)
	testutils.SucceedsSoon(t, func() error {
		if err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID); !testutils.IsError(err, errFailedHeartbeat.Error()) {
			return errors.Errorf("unexpected error: %v", err)
		}
		return nil
	})
	assertGauges(t, clientCtx.Metrics(),
		0 /* initializing */, 0 /* nominal */, 1 /* failed */)

	// Should become healthy in the presence of successful heartbeats.
	hbSuccess.Store(true)
	testutils.SucceedsSoon(t, func() error {
		return clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
	})
	assertGauges(t, clientCtx.Metrics(),
		0 /* initializing */, 1 /* nominal */, 0 /* failed */)

	// Should become unhealthy again in the presence of failing heartbeats.
	hbSuccess.Store(false)
	testutils.SucceedsSoon(t, func() error {
		if err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID); !testutils.IsError(err, errFailedHeartbeat.Error()) {
			return errors.Errorf("unexpected error: %v", err)
		}
		return nil
	})
	assertGauges(t, clientCtx.Metrics(),
		0 /* initializing */, 0 /* nominal */, 1 /* failed */)

	// Should become healthy in the presence of successful heartbeats.
	hbSuccess.Store(true)
	testutils.SucceedsSoon(t, func() error {
		return clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
	})
	assertGauges(t, clientCtx.Metrics(),
		0 /* initializing */, 1 /* nominal */, 0 /* failed */)

	// Ensure that non-existing connections return ErrNotHeartbeated.

	lisNonExistentConnection, err := net.Listen("tcp", "127.0.0.1:0")
	defer func() {
		netutil.FatalIfUnexpected(lisNonExistentConnection.Close())
	}()
	if err != nil {
		t.Fatal(err)
	}
	if err := clientCtx.TestingConnHealth(lisNonExistentConnection.Addr().String(), 3); err != ErrNotHeartbeated {
		t.Errorf("wanted ErrNotHeartbeated, not %v", err)
	}
	// The connection to Node 3 on the lisNonExistentConnection should be
	// initializing and the server connection should be nominal.
	testutils.SucceedsSoon(t, func() error {
		return checkGauges(clientCtx.Metrics(),
			1 /* initializing */, 1 /* nominal */, 0 /* failed */)
	})

	if err := clientCtx.TestingConnHealth(clientCtx.Addr, clientNodeID); err != ErrNotHeartbeated {
		t.Errorf("wanted ErrNotHeartbeated, not %v", err)
	}

	// Ensure that the local Addr returns ErrNotHeartbeated without having dialed
	// a connection but the local AdvertiseAddr successfully returns no error when
	// an internal server has been registered.
	clientCtx.SetLocalInternalServer(&internalServer{})

	if err := clientCtx.TestingConnHealth(clientCtx.Addr, clientNodeID); err != ErrNotHeartbeated {
		t.Errorf("wanted ErrNotHeartbeated, not %v", err)
	}
	if err := clientCtx.TestingConnHealth(clientCtx.AdvertiseAddr, clientNodeID); err != nil {
		t.Error(err)
	}

	// Ensure that when the server closes its connection the context attempts to
	// reconnect. Both the server connection on Node 1 and the non-existent
	// connection should be initializing.
	serverCtx.Stopper.Stop(context.Background())
	testutils.SucceedsSoon(t, func() error {
		return checkGauges(clientCtx.Metrics(),
			2 /* initializing */, 0 /* nominal */, 0 /* failed */)
	})
	const expNumStarted = 3 // 2 for the server and 1 for the non-existent conn
	numStarted := clientCtx.Metrics().HeartbeatLoopsStarted.Count()
	if numStarted != expNumStarted {
		t.Fatalf("expected %d heartbeat loops to have been started, got %d",
			expNumStarted, numStarted)
	}
	const expNumExited = 1 // 1 for the server upon shutdown
	numExited := clientCtx.Metrics().HeartbeatLoopsExited.Count()
	if numExited != expNumExited {
		t.Fatalf("expected %d heartbeat loops to have exited, got %d",
			expNumExited, numExited)
	}
}

func checkGauges(m *Metrics, initializing, nominal, failed int64) error {
	if got := m.HeartbeatsInitializing.Value(); got != initializing {
		return errors.Errorf("expected %d initializing heartbeats, got %d", initializing, got)
	}
	if got := m.HeartbeatsNominal.Value(); got != nominal {
		return errors.Errorf("expected %d nominal heartbeats, got %d", nominal, got)
	}
	if got := m.HeartbeatsFailed.Value(); got != failed {
		return errors.Errorf("expected %d failed heartbeats, got %d", failed, got)
	}
	return nil
}

func assertGauges(t *testing.T, m *Metrics, initializing, nominal, failed int64) {
	t.Helper()
	if err := checkGauges(m, initializing, nominal, failed); err != nil {
		t.Error(err)
	}
}

// TestConnectionRemoveNodeIDZero verifies that when a connection initiated via
// GRPCDialNode fails, we also clean up the connection returned by
// GRPCUnvalidatedDial.
//
// See #37200.
func TestConnectionRemoveNodeIDZero(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	clientCtx := newTestContext(uuid.MakeV4(), clock, stopper)
	// Provoke an error.
	_, err := clientCtx.GRPCDialNode("127.0.0.1:notaport", 1).Connect(context.Background())
	if err == nil {
		t.Fatal("expected some kind of error, got nil")
	}

	// NB: this takes a moment because GRPCDialRaw only gives up on the initial
	// connection after 1s (more precisely, the redialChan gets closed only after
	// 1s), which seems difficult to configure ad-hoc.
	testutils.SucceedsSoon(t, func() error {
		var keys []connKey
		clientCtx.conns.Range(func(k, v interface{}) bool {
			keys = append(keys, k.(connKey))
			return true
		})
		if len(keys) > 0 {
			return errors.Errorf("still have connections %v", keys)
		}
		return nil
	})
}

type interceptingListener struct {
	net.Listener
	connCB func(net.Conn)
}

func (ln *interceptingListener) Accept() (net.Conn, error) {
	conn, err := ln.Listener.Accept()
	if err == nil {
		ln.connCB(conn)
	}
	return conn, err
}

// TestHeartbeatHealth verifies that the health status changes after
// heartbeats succeed or fail due to transport failures.
func TestHeartbeatHealthTransport(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	ctx := context.Background()

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(timeutil.Unix(0, 1).UnixNano, time.Nanosecond)

	serverCtx := newTestContext(clusterID, clock, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(context.TODO(), serverNodeID)
	// newTestServer with a custom listener.
	tlsConfig, err := serverCtx.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		clusterID:          &serverCtx.ClusterID,
		nodeID:             &serverCtx.NodeID,
		version:            serverCtx.version,
	})

	mu := struct {
		syncutil.Mutex
		conns     []net.Conn
		autoClose bool
	}{}
	ln := func() *interceptingListener {
		ln, err := net.Listen("tcp", util.TestAddr.String())
		if err != nil {
			t.Fatal(err)
		}
		return &interceptingListener{
			Listener: ln,
			connCB: func(conn net.Conn) {
				mu.Lock()
				if mu.autoClose {
					_ = conn.Close()
				} else {
					mu.conns = append(mu.conns, conn)
				}
				mu.Unlock()
			}}
	}()

	stopper.RunWorker(ctx, func(context.Context) {
		<-stopper.ShouldQuiesce()
		netutil.FatalIfUnexpected(ln.Close())
		<-stopper.ShouldStop()
		s.Stop()
	})

	stopper.RunWorker(ctx, func(context.Context) {
		netutil.FatalIfUnexpected(s.Serve(ln))
	})

	remoteAddr := ln.Addr().String()

	clientCtx := newTestContext(clusterID, clock, stopper)
	// Make the interval shorter to speed up the test.
	clientCtx.heartbeatInterval = 1 * time.Millisecond
	if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID).Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Everything is normal; should become healthy.
	testutils.SucceedsSoon(t, func() error {
		return clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
	})

	closeConns := func() (numClosed int, _ error) {
		mu.Lock()
		defer mu.Unlock()
		n := len(mu.conns)
		for i := n - 1; i >= 0; i-- {
			if err := mu.conns[i].Close(); err != nil {
				return 0, err
			}
			mu.conns = mu.conns[:i]
		}
		return n, nil
	}

	isUnhealthy := func(err error) bool {
		// Most of the time, an unhealthy connection will get
		// ErrNotHeartbeated, but there are brief periods during which we
		// could get one of the grpc errors below (while the old
		// connection is in the middle of closing).
		if err == ErrNotHeartbeated {
			return true
		}
		// The expected code here is Unavailable, but at least on OSX you can also get
		//
		// rpc error: code = Internal desc = connection error: desc = "transport: authentication
		// handshake failed: write tcp 127.0.0.1:53936->127.0.0.1:53934: write: broken pipe".
		code := status.Code(err)
		return code == codes.Unavailable || code == codes.Internal
	}

	// Close all the connections until we see a failure on the main goroutine.
	done := make(chan struct{})
	if err := stopper.RunAsyncTask(ctx, "busyloop-closer", func(ctx context.Context) {
		for {
			if _, err := closeConns(); err != nil {
				log.Warning(ctx, err)
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	// We don't use SucceedsSoon because that internally uses doubling backoffs, and
	// it doesn't need too much bad luck to run into the time limit.
	for then := timeutil.Now(); ; {
		err := func() error {
			if err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID); !isUnhealthy(err) {
				return errors.Errorf("unexpected error: %v", err)
			}
			return nil
		}()
		if err == nil {
			break
		}
		if timeutil.Since(then) > 45*time.Second {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(done)

	// We can reconnect and the connection becomes healthy again.
	testutils.SucceedsSoon(t, func() error {
		if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID).Connect(context.Background()); err != nil {
			return err
		}
		return clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
	})

	// Close the listener and all the connections. Note that if we
	// only closed the listener, recently-accepted-but-not-yet-handled
	// connections could sneak in and randomly make the target healthy
	// again. To avoid this, we flip the boolean below which is used in
	// our handler callback to eagerly close any stragglers.
	mu.Lock()
	mu.autoClose = true
	mu.Unlock()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	// Also terminate any existing connections.
	if _, err := closeConns(); err != nil {
		t.Fatal(err)
	}

	// Should become unhealthy again now that the connection was closed.
	testutils.SucceedsSoon(t, func() error {
		err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID)

		if !isUnhealthy(err) {
			return errors.Errorf("unexpected error: %v", err)
		}
		return nil
	})

	// Should stay unhealthy despite reconnection attempts.
	for then := timeutil.Now(); timeutil.Since(then) < 50*clientCtx.heartbeatInterval; {
		err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
		if !isUnhealthy(err) {
			t.Fatal(err)
		}
	}
}

func TestOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	serverTime := timeutil.Unix(0, 20)
	serverClock := hlc.NewClock(serverTime.UnixNano, time.Nanosecond)
	serverCtx := newTestContext(clusterID, serverClock, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(context.TODO(), serverNodeID)
	s := newTestServer(t, serverCtx)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		clusterID:          &serverCtx.ClusterID,
		nodeID:             &serverCtx.NodeID,
		version:            serverCtx.version,
	})

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	// Create a client clock that is behind the server clock.
	clientAdvancing := AdvancingClock{time: timeutil.Unix(0, 10)}
	clientClock := hlc.NewClock(clientAdvancing.UnixNano, time.Nanosecond)
	clientCtx := newTestContext(clusterID, clientClock, stopper)
	// Make the interval shorter to speed up the test.
	clientCtx.heartbeatInterval = 1 * time.Millisecond
	clientCtx.RemoteClocks.offsetTTL = 5 * clientAdvancing.getAdvancementInterval()
	if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID).Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	expectedOffset := RemoteOffset{Offset: 10, Uncertainty: 0, MeasuredAt: 10}
	testutils.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()

		if o, ok := clientCtx.RemoteClocks.mu.offsets[remoteAddr]; !ok {
			return errors.Errorf("expected offset of %s to be initialized, but it was not", remoteAddr)
		} else if o != expectedOffset {
			return errors.Errorf("expected:\n%v\nactual:\n%v", expectedOffset, o)
		}
		return nil
	})

	// Change the client such that it receives a heartbeat right after the
	// maximum clock reading delay.
	clientAdvancing.setAdvancementInterval(
		maximumPingDurationMult*clientClock.MaxOffset() + 1*time.Nanosecond)

	testutils.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()

		if o, ok := clientCtx.RemoteClocks.mu.offsets[remoteAddr]; ok {
			return errors.Errorf("expected offset to have been cleared, but found %s", o)
		}
		return nil
	})
}

func TestFailedOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(timeutil.Unix(0, 1).UnixNano, time.Nanosecond)

	serverCtx := newTestContext(clusterID, clock, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(context.TODO(), serverNodeID)
	s := newTestServer(t, serverCtx)
	heartbeat := &ManualHeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		ready:              make(chan error),
		stopper:            stopper,
		version:            serverCtx.version,
		nodeID:             &serverCtx.NodeID,
	}
	RegisterHeartbeatServer(s, heartbeat)

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	// Create a client that never receives a heartbeat after the first.
	clientCtx := newTestContext(clusterID, clock, stopper)
	// Remove the timeout so that failure arises from exceeding the maximum
	// clock reading delay, not the timeout.
	clientCtx.heartbeatTimeout = 0
	go func() { heartbeat.ready <- nil }() // Allow one heartbeat for initialization.
	if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID).Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()

		if _, ok := clientCtx.RemoteClocks.mu.offsets[remoteAddr]; !ok {
			return errors.Errorf("expected offset of %s to be initialized, but it was not", remoteAddr)
		}
		return nil
	})

	testutils.SucceedsSoon(t, func() error {
		serverCtx.RemoteClocks.mu.Lock()
		defer serverCtx.RemoteClocks.mu.Unlock()

		if o, ok := serverCtx.RemoteClocks.mu.offsets[remoteAddr]; ok {
			return errors.Errorf("expected offset of %s to not be initialized, but it was: %v", remoteAddr, o)
		}
		return nil
	})
}

type AdvancingClock struct {
	syncutil.Mutex
	time                time.Time
	advancementInterval atomic.Value // time.Duration
}

func (ac *AdvancingClock) setAdvancementInterval(d time.Duration) {
	ac.advancementInterval.Store(d)
}

func (ac *AdvancingClock) getAdvancementInterval() time.Duration {
	v := ac.advancementInterval.Load()
	if v == nil {
		return 0
	}
	return v.(time.Duration)
}

func (ac *AdvancingClock) UnixNano() int64 {
	ac.Lock()
	time := ac.time
	ac.time = time.Add(ac.getAdvancementInterval())
	ac.Unlock()
	return time.UnixNano()
}

func TestRemoteOffsetUnhealthy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	const maxOffset = 100 * time.Millisecond

	type nodeContext struct {
		offset  time.Duration
		ctx     *Context
		errChan chan error
	}

	start := time.Date(2012, 12, 07, 0, 0, 0, 0, time.UTC)

	nodeCtxs := []nodeContext{
		{offset: 0},
		{offset: 0},
		{offset: 0},
		// The minimum offset that actually triggers node death.
		{offset: maxOffset + 1},
	}

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	for i := range nodeCtxs {
		clock := hlc.NewClock(start.Add(nodeCtxs[i].offset).UnixNano, maxOffset)
		nodeCtxs[i].errChan = make(chan error, 1)
		nodeCtxs[i].ctx = newTestContext(clusterID, clock, stopper)
		nodeCtxs[i].ctx.heartbeatInterval = maxOffset
		nodeCtxs[i].ctx.NodeID.Set(context.TODO(), roachpb.NodeID(i+1))

		s := newTestServer(t, nodeCtxs[i].ctx)
		RegisterHeartbeatServer(s, &HeartbeatService{
			clock:              clock,
			remoteClockMonitor: nodeCtxs[i].ctx.RemoteClocks,
			clusterID:          &nodeCtxs[i].ctx.ClusterID,
			nodeID:             &nodeCtxs[i].ctx.NodeID,
			version:            nodeCtxs[i].ctx.version,
		})
		ln, err := netutil.ListenAndServeGRPC(nodeCtxs[i].ctx.Stopper, s, util.TestAddr)
		if err != nil {
			t.Fatal(err)
		}
		nodeCtxs[i].ctx.Addr = ln.Addr().String()
	}

	// Fully connect the nodes.
	for i, clientNodeContext := range nodeCtxs {
		for j, serverNodeContext := range nodeCtxs {
			if i == j {
				continue
			}
			if _, err := clientNodeContext.ctx.GRPCDialNode(
				serverNodeContext.ctx.Addr,
				serverNodeContext.ctx.NodeID.Get(),
			).Connect(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Wait until all nodes are connected to all other nodes.
	for _, nodeCtx := range nodeCtxs {
		testutils.SucceedsSoon(t, func() error {
			nodeCtx.ctx.RemoteClocks.mu.Lock()
			defer nodeCtx.ctx.RemoteClocks.mu.Unlock()

			if a, e := len(nodeCtx.ctx.RemoteClocks.mu.offsets), len(nodeCtxs)-1; a != e {
				return errors.Errorf("not yet fully connected: have %d of %d connections: %v", a, e, nodeCtx.ctx.RemoteClocks.mu.offsets)
			}
			return nil
		})
	}

	for i, nodeCtx := range nodeCtxs {
		if nodeOffset := nodeCtx.offset; nodeOffset > maxOffset {
			if err := nodeCtx.ctx.RemoteClocks.VerifyClockOffset(nodeCtx.ctx.masterCtx); testutils.IsError(err, errOffsetGreaterThanMaxOffset) {
				t.Logf("max offset: %s - node %d with excessive clock offset of %s returned expected error: %s", maxOffset, i, nodeOffset, err)
			} else {
				t.Errorf("max offset: %s - node %d with excessive clock offset of %s returned unexpected error: %v", maxOffset, i, nodeOffset, err)
			}
		} else {
			if err := nodeCtx.ctx.RemoteClocks.VerifyClockOffset(nodeCtx.ctx.masterCtx); err != nil {
				t.Errorf("max offset: %s - node %d with acceptable clock offset of %s returned unexpected error: %s", maxOffset, i, nodeOffset, err)
			} else {
				t.Logf("max offset: %s - node %d with acceptable clock offset of %s did not return an error, as expected", maxOffset, i, nodeOffset)
			}
		}
	}
}

// This is a smoketest for gRPC Keepalives: rpc.Context asks gRPC to perform
// periodic pings on the transport to check that it's still alive. If the ping
// doesn't get a pong within a timeout, the transport is supposed to be closed -
// that's what we're testing here. Likewise, serverside keepalive ensures that
// if a ping is not seen within a timeout, the transport will also be closed.
//
// In this test we use a TestingHeartbeatStreamService as oppposed to a standard
// HeartbeatService. This is important to test scenarios where the
// client->server connection is partitioned but the server->client connection is
// healthy, because a TestingHeartbeatStreamService will continue to respond on
// its response stream even if it doesn't get any new requests.
func TestGRPCKeepaliveFailureFailsInflightRPCs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("Takes too long given https://github.com/grpc/grpc-go/pull/2642")

	sc := log.Scope(t)
	defer sc.Close(t)

	testCases := []grpcKeepaliveTestCase{
		// Keepalive doesn't matter if the network is fine.
		{cKeepalive: false, sKeepalive: false, partitionC2S: false, partitionS2C: false, expClose: false},

		// No keepalive. Never detects network issues.
		{cKeepalive: false, sKeepalive: false, partitionC2S: true, partitionS2C: false, expClose: false},
		{cKeepalive: false, sKeepalive: false, partitionC2S: false, partitionS2C: true, expClose: false},
		{cKeepalive: false, sKeepalive: false, partitionC2S: true, partitionS2C: true, expClose: false},

		// Client-only keepalive. Doesn't detect client->server partition.
		{cKeepalive: true, sKeepalive: false, partitionC2S: true, partitionS2C: false, expClose: false},
		{cKeepalive: true, sKeepalive: false, partitionC2S: false, partitionS2C: true, expClose: true},
		{cKeepalive: true, sKeepalive: false, partitionC2S: true, partitionS2C: true, expClose: true},

		// Server-only keepalive. Only detects server->client partition. The
		// bi-directional partition case (third case) may be is surprising.
		// The reason the client doesn't close the connection is because it
		// does not receive the connection closed message sent by the server.
		// This demonstrates why client keepalive is so important.
		{cKeepalive: false, sKeepalive: true, partitionC2S: true, partitionS2C: false, expClose: true},
		{cKeepalive: false, sKeepalive: true, partitionC2S: false, partitionS2C: true, expClose: false},
		{cKeepalive: false, sKeepalive: true, partitionC2S: true, partitionS2C: true, expClose: false},

		// Client and Server keepalive. Detects all partitions!
		{cKeepalive: true, sKeepalive: true, partitionC2S: true, partitionS2C: false, expClose: true},
		{cKeepalive: true, sKeepalive: true, partitionC2S: false, partitionS2C: true, expClose: true},
		{cKeepalive: true, sKeepalive: true, partitionC2S: true, partitionS2C: true, expClose: true},
	}

	// For consistent spacing in test names.
	fmtBool := func(b bool) string {
		s := strconv.FormatBool(b)
		if b {
			s += " "
		}
		return s
	}
	connIcon := func(partition bool) string {
		if partition {
			return "-X->"
		}
		return "--->"
	}

	// Run all the tests.
	var wg sync.WaitGroup
	wg.Add(len(testCases))
	errCh := make(chan error, len(testCases))
	for testNum, c := range testCases {
		kaName := fmt.Sprintf("clientKeepalive=%s,serverKeepalive=%s", fmtBool(c.cKeepalive), fmtBool(c.sKeepalive))
		pName := fmt.Sprintf("client%sserver,server%sclient", connIcon(c.partitionC2S), connIcon(c.partitionS2C))
		testName := fmt.Sprintf("%d/%s/%s", testNum, kaName, pName)
		ctx := logtags.AddTag(context.Background(), testName, nil)

		log.Infof(ctx, "starting sub-test")
		go func(c grpcKeepaliveTestCase) {
			errCh <- errors.Wrapf(grpcRunKeepaliveTestCase(ctx, c), "%+v", c)
			wg.Done()
		}(c)
	}
	log.Infof(context.Background(), "waiting for sub-tests to complete")
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Errorf("%+v", err)
		}
	}
}

type grpcKeepaliveTestCase struct {
	cKeepalive, sKeepalive     bool
	partitionC2S, partitionS2C bool
	expClose                   bool
}

func grpcRunKeepaliveTestCase(testCtx context.Context, c grpcKeepaliveTestCase) error {
	var cKeepalive keepalive.ClientParameters
	if c.cKeepalive {
		cKeepalive = clientTestingKeepalive
	}
	var sKeepalive keepalive.ServerParameters
	if c.sKeepalive {
		sKeepalive = serverTestingKeepalive
	}

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	ctx, cancel := stopper.WithCancelOnQuiesce(testCtx)
	defer cancel()

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	// Construct server with server-side keepalive.
	log.Infof(ctx, "constructing server")
	clock := hlc.NewClock(timeutil.Unix(0, 20).UnixNano, time.Nanosecond)
	serverCtx := newTestContext(clusterID, clock, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(context.TODO(), serverNodeID)
	tlsConfig, err := serverCtx.GetServerTLSConfig()
	if err != nil {
		return err
	}
	s := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.StatsHandler(&serverCtx.stats),
		grpc.KeepaliveParams(sKeepalive),
	)

	// Create heartbeat service. This service will continuously
	// read on its input stream and send on its output stream.
	log.Infof(ctx, "creating heartbeat service")
	const msgInterval = 10 * time.Millisecond
	hss := &HeartbeatStreamService{
		HeartbeatService: HeartbeatService{
			clock:              clock,
			remoteClockMonitor: serverCtx.RemoteClocks,
			clusterID:          &serverCtx.ClusterID,
			nodeID:             &serverCtx.NodeID,
			version:            serverCtx.version,
		},
		interval: msgInterval,
	}
	RegisterHeartbeatServer(s, hss)
	RegisterTestingHeartbeatStreamServer(s, hss)

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		return err
	}
	remoteAddr := ln.Addr().String()

	log.Infof(ctx, "setting up client")
	clientCtx := newTestContext(clusterID, clock, stopper)
	// Disable automatic heartbeats. We'll send them by hand.
	clientCtx.heartbeatInterval = math.MaxInt64

	var firstConn int32 = 1

	// We're going to open RPC transport connections using a dialer that returns
	// PartitionableConns. We'll partition the first opened connection.
	dialerCh := make(chan *testutils.PartitionableConn, 1)
	clientCtx.AddTestingDialOpts(
		grpc.WithContextDialer(
			func(_ context.Context, addr string) (net.Conn, error) {
				if !atomic.CompareAndSwapInt32(&firstConn, 1, 0) {
					// If we allow gRPC to open a 2nd transport connection, then our RPCs
					// might succeed if they're sent on that one. In the spirit of a
					// partition, we'll return errors for the attempt to open a new
					// connection (albeit for a TCP connection the error would come after
					// a socket connect timeout).
					return nil, errors.Errorf("No more connections for you. We're partitioned.")
				}

				conn, err := net.Dial("tcp", addr)
				if err != nil {
					return nil, err
				}
				transportConn := testutils.NewPartitionableConn(conn)
				dialerCh <- transportConn
				return transportConn, nil
			}),
		grpc.WithKeepaliveParams(cKeepalive),
	)
	log.Infof(ctx, "dialing server")
	conn, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID).Connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	// Create the heartbeat client.
	log.Infof(ctx, "starting heartbeat client")
	unlockedHeartbeatClient, err := NewTestingHeartbeatStreamClient(conn).PingStream(ctx)
	if err != nil {
		return err
	}
	heartbeatClient := &lockedPingStreamClient{
		TestingHeartbeatStream_PingStreamClient: unlockedHeartbeatClient,
	}

	// Perform an initial request-response round trip.
	log.Infof(ctx, "first ping")
	request := PingRequest{ServerVersion: clientCtx.version.ServerVersion}
	if err := heartbeatClient.Send(&request); err != nil {
		return err
	}
	if _, err := heartbeatClient.Recv(); err != nil {
		return err
	}

	// Launch a goroutine to read from the channel continuously and
	// a goroutine to write to the channel continuously. Both will
	// exit when the channel breaks (either because of a partition
	// or because the stopper stops).
	go func() {
		t := time.NewTicker(msgInterval)
		defer t.Stop()
		for {
			<-t.C
			log.Infof(ctx, "client send")
			if err := heartbeatClient.Send(&request); err != nil {
				return
			}
		}
	}()
	go func() {
		for {
			log.Infof(ctx, "client recv")
			if _, err := heartbeatClient.Recv(); err != nil {
				return
			}
		}
	}()

	// Now partition either client->server, server->client, or both, and attempt
	// to perform an RPC. We expect it to fail once the grpc keepalive fails to
	// get a response from the server.
	transportConn := <-dialerCh
	defer transportConn.Finish()
	if c.partitionC2S {
		log.Infof(ctx, "partition C2S")
		transportConn.PartitionC2S()
	}
	if c.partitionS2C {
		log.Infof(ctx, "partition S2C")
		transportConn.PartitionS2C()
	}

	// We want to start a goroutine that keeps trying to send requests and reports
	// the error from the send call. In cases where there are no keep-alives this
	// request may get blocked if flow control blocks it.
	errChan := make(chan error)
	sendCtx, cancel := context.WithCancel(ctx)
	r := retry.StartWithCtx(sendCtx, retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
	})
	defer cancel()
	go func() {
		for r.Next() {
			err := heartbeatClient.Send(&request)
			isClosed := err != nil && grpcutil.IsClosedConnection(err)
			log.Infof(ctx, "heartbeat Send got error %+v (closed=%v)", err, isClosed)
			select {
			case errChan <- err:
			case <-sendCtx.Done():
				return
			}
			if isClosed {
				return
			}
		}
	}()
	// Check whether the connection eventually closes. We may need to
	// adjust this duration if the test gets flaky.
	// This unfortunately massive amount of time is required due to gRPC's
	// minimum timeout of 10s and the below issue whereby keepalives are sent
	// at half the expected rate.
	// https://github.com/grpc/grpc-go/issues/2638
	const timeoutDur = 21 * time.Second
	timeout := time.After(timeoutDur)
	// sendErr will hold the last error we saw from an attempt to send a
	// heartbeat. Initialize it with a dummy error which will fail the test if
	// it is not overwritten.
	sendErr := fmt.Errorf("not a real error")
	for done := false; !done; {
		select {
		case <-timeout:
			cancel()
			done = true
		case sendErr = <-errChan:
		}
	}
	if c.expClose {
		if sendErr == nil || !grpcutil.IsClosedConnection(sendErr) {
			newErr := fmt.Errorf("expected closed connection, found %v", sendErr)
			log.Infof(ctx, "%+v", newErr)
			return newErr
		}
	} else {
		if sendErr != nil {
			newErr := fmt.Errorf("expected unclosed connection, found %v", sendErr)
			log.Infof(ctx, "%+v", newErr)
			return newErr
		}
	}

	// If the DialOptions we passed to gRPC didn't prevent it from opening new
	// connections, then next RPCs would succeed since gRPC reconnects the
	// transport (and that would succeed here since we've only partitioned one
	// connection). We could further test that the status reported by
	// Context.ConnHealth() for the remote node moves to UNAVAILABLE because of
	// the (application-level) heartbeats performed by rpc.Context, but the
	// behavior of our heartbeats in the face of transport failures is
	// sufficiently tested in TestHeartbeatHealthTransport.
	log.Infof(ctx, "test done")
	return nil
}

func TestClusterIDMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	clock := hlc.NewClock(timeutil.Unix(0, 20).UnixNano, time.Nanosecond)
	serverCtx := newTestContext(uuid.MakeV4(), clock, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(context.TODO(), serverNodeID)
	s := newTestServer(t, serverCtx)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		clusterID:          &serverCtx.ClusterID,
		nodeID:             &serverCtx.NodeID,
		version:            serverCtx.version,
	})

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	// Ensure the client ctx gets a new fresh cluster ID so it becomes
	// different from the server's.
	clientCtx := newTestContext(uuid.MakeV4(), clock, stopper)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			_, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID).Connect(context.Background())
			expected := "initial connection heartbeat failed.*doesn't match server cluster ID"
			if !testutils.IsError(err, expected) {
				t.Errorf("expected %s error, got %v", expected, err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestNodeIDMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	clock := hlc.NewClock(timeutil.Unix(0, 20).UnixNano, time.Nanosecond)
	serverCtx := newTestContext(clusterID, clock, stopper)
	serverCtx.NodeID.Set(context.TODO(), 1)
	s := newTestServer(t, serverCtx)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		clusterID:          &serverCtx.ClusterID,
		nodeID:             &serverCtx.NodeID,
		version:            serverCtx.version,
	})

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	clientCtx := newTestContext(clusterID, clock, stopper)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			_, err := clientCtx.GRPCDialNode(remoteAddr, 2).Connect(context.Background())
			expected := "initial connection heartbeat failed.*doesn't match server node ID"
			if !testutils.IsError(err, expected) {
				t.Errorf("expected %s error, got %v", expected, err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func setVersion(c *Context, v roachpb.Version) error {
	settings := cluster.MakeClusterSettings(v, v)
	cv := cluster.ClusterVersion{Version: v}
	if err := settings.InitializeVersion(cv); err != nil {
		return err
	}
	c.version = &settings.Version
	return nil
}

// Test that GRPCDial fails if there is a version incompatibility in either
// direction (client -> server or server -> client).
func TestVersionCheckBidirectional(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v1 := roachpb.Version{Major: 1}
	v2 := cluster.BinaryServerVersion

	testData := []struct {
		name          string
		serverVersion roachpb.Version
		clientVersion roachpb.Version
		expectError   bool
	}{
		{"serverVersion == clientVersion", v1, v1, false},
		{"serverVersion < clientVersion", v1, v2, true},
		{"serverVersion > clientVersion", v2, v1, true},
	}

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())

			clock := hlc.NewClock(timeutil.Unix(0, 20).UnixNano, time.Nanosecond)
			serverCtx := newTestContext(clusterID, clock, stopper)
			const serverNodeID = 1
			serverCtx.NodeID.Set(context.TODO(), serverNodeID)
			if err := setVersion(serverCtx, td.serverVersion); err != nil {
				t.Fatal(err)
			}
			s := newTestServer(t, serverCtx)
			RegisterHeartbeatServer(s, &HeartbeatService{
				clock:              clock,
				remoteClockMonitor: serverCtx.RemoteClocks,
				clusterID:          &serverCtx.ClusterID,
				nodeID:             &serverCtx.NodeID,
				version:            serverCtx.version,
			})

			ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
			if err != nil {
				t.Fatal(err)
			}
			remoteAddr := ln.Addr().String()

			clientCtx := newTestContext(clusterID, clock, stopper)
			if err := setVersion(clientCtx, td.clientVersion); err != nil {
				t.Fatal(err)
			}

			_, err = clientCtx.GRPCDialNode(remoteAddr, serverNodeID).Connect(context.Background())

			if td.expectError {
				expected := "initial connection heartbeat failed.*cluster requires at least version"
				if !testutils.IsError(err, expected) {
					t.Errorf("expected %s error, got %v", expected, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %s", err)
			}
		})
	}
}

func BenchmarkGRPCDial(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	clock := hlc.NewClock(hlc.UnixNano, 250*time.Millisecond)
	ctx := newTestContext(uuid.MakeV4(), clock, stopper)
	const serverNodeID = 1
	ctx.NodeID.Set(context.TODO(), serverNodeID)

	s := newTestServer(b, ctx)
	ln, err := netutil.ListenAndServeGRPC(ctx.Stopper, s, util.TestAddr)
	if err != nil {
		b.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := ctx.GRPCDialNode(remoteAddr, serverNodeID).Connect(context.Background())
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
