// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nodedialer

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const staticNodeID = 1

func TestNodedialerPositive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, _, _, _, nd := setUpNodedialerTest(t, staticNodeID)
	defer stopper.Stop(context.Background())
	// Ensure that dialing works.
	breaker := nd.GetCircuitBreaker(1, rpc.DefaultClass)
	assert.True(t, breaker.Ready())
	ctx := context.Background()
	_, err := nd.Dial(ctx, staticNodeID, rpc.DefaultClass)
	assert.Nil(t, err, "failed to dial")
	assert.True(t, breaker.Ready())
	assert.Equal(t, breaker.Failures(), int64(0))
}

func TestDialNoBreaker(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Don't use setUpNodedialerTest because we want access to the underlying clock and rpcContext.
	stopper := stop.NewStopper()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcCtx := newTestContext(clock, stopper)
	rpcCtx.NodeID.Set(ctx, staticNodeID)
	_, ln, _ := newTestServer(t, clock, stopper, true /* useHeartbeat */)
	defer stopper.Stop(ctx)

	nd := New(rpcCtx, newSingleNodeResolver(staticNodeID, ln.Addr()))
	_, err := nd.Dial(ctx, staticNodeID, rpc.DefaultClass)
	require.NoError(t, err)
	testutils.SucceedsSoon(t, func() error {
		return nd.ConnHealth(staticNodeID, rpc.DefaultClass)
	})
	breaker := nd.GetCircuitBreaker(staticNodeID, rpc.DefaultClass)
	assert.True(t, breaker.Ready())

	// Test that DialNoBreaker is successful normally.
	_, err = nd.DialNoBreaker(ctx, staticNodeID, rpc.DefaultClass)
	assert.Nil(t, err, "failed to dial")
	assert.True(t, breaker.Ready())
	assert.Equal(t, breaker.Failures(), int64(0))

	// Now trip the breaker and check that DialNoBreaker will go ahead
	// and dial anyway, and on top of that open the breaker again (since
	// the dial will succeed).
	breaker.Trip()
	require.True(t, breaker.Tripped())
	_, err = nd.DialNoBreaker(ctx, staticNodeID, rpc.DefaultClass)
	require.NoError(t, err)
	require.False(t, breaker.Tripped())

	// Test that resolver errors also trip the breaker, just like
	// they would for regular Dial.
	boom := fmt.Errorf("boom")
	nd = New(rpcCtx, func(roachpb.NodeID) (net.Addr, error) {
		return nil, boom
	})
	breaker = nd.GetCircuitBreaker(staticNodeID, rpc.DefaultClass)
	_, err = nd.DialNoBreaker(ctx, staticNodeID, rpc.DefaultClass)
	assert.Equal(t, errors.Cause(err), boom)
	assert.Equal(t, breaker.Failures(), int64(1))

	// Test that connection errors are reported to the breaker even
	// with DialNoBreaker.
	// To do this, we have to trick grpc into never successfully dialing
	// the server, because if it succeeds once then it doesn't try again
	// to perform a connection. To trick grpc in this way, we have to
	// set up a server without the heartbeat service running. Without
	// getting a heartbeat, the nodedialer will throw an error thinking
	// that it wasn't able to successfully make a connection.
	_, ln, _ = newTestServer(t, clock, stopper, false /* useHeartbeat */)
	nd = New(rpcCtx, newSingleNodeResolver(staticNodeID, ln.Addr()))
	breaker = nd.GetCircuitBreaker(staticNodeID, rpc.DefaultClass)
	assert.True(t, breaker.Ready())
	assert.Equal(t, breaker.Failures(), int64(0))
	_, err = nd.DialNoBreaker(ctx, staticNodeID, rpc.DefaultClass)
	assert.NotNil(t, err, "expected dial error")
	assert.Equal(t, breaker.Failures(), int64(1))
}

func TestConnHealth(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcCtx := newTestContext(clock, stopper)
	rpcCtx.NodeID.Set(ctx, staticNodeID)
	_, ln, hb := newTestServer(t, clock, stopper, true /* useHeartbeat */)
	defer stopper.Stop(ctx)
	nd := New(rpcCtx, newSingleNodeResolver(staticNodeID, ln.Addr()))

	// When no connection exists, we expect ConnHealth to return ErrNoConnection.
	require.Equal(t, rpc.ErrNoConnection, nd.ConnHealth(staticNodeID, rpc.DefaultClass))

	// After dialing the node, ConnHealth should return nil.
	_, err := nd.Dial(ctx, staticNodeID, rpc.DefaultClass)
	require.NoError(t, err)
	require.NoError(t, nd.ConnHealth(staticNodeID, rpc.DefaultClass))

	// ConnHealth should still error for other node ID and class.
	require.Error(t, nd.ConnHealth(9, rpc.DefaultClass))
	require.Equal(t, rpc.ErrNoConnection, nd.ConnHealth(staticNodeID, rpc.SystemClass))

	// When the heartbeat errors, ConnHealth should eventually error too.
	hb.setErr(errors.New("boom"))
	require.Eventually(t, func() bool {
		return nd.ConnHealth(staticNodeID, rpc.DefaultClass) != nil
	}, time.Second, 10*time.Millisecond)

	// When the heartbeat recovers, ConnHealth should too.
	hb.setErr(nil)
	require.Eventually(t, func() bool {
		return nd.ConnHealth(staticNodeID, rpc.DefaultClass) == nil
	}, time.Second, 10*time.Millisecond)

	// Tripping the breaker should return ErrBreakerOpen.
	br := nd.getBreaker(staticNodeID, rpc.DefaultClass)
	br.Trip()
	require.Equal(t, circuit.ErrBreakerOpen, nd.ConnHealth(staticNodeID, rpc.DefaultClass))

	// Resetting the breaker should recover ConnHealth.
	br.Reset()
	require.NoError(t, nd.ConnHealth(staticNodeID, rpc.DefaultClass))

	// Closing the remote connection should fail ConnHealth.
	require.NoError(t, ln.popConn().Close())
	require.Eventually(t, func() bool {
		return nd.ConnHealth(staticNodeID, rpc.DefaultClass) != nil
	}, time.Second, 10*time.Millisecond)
}

func TestConnHealthTryDial(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcCtx := newTestContext(clock, stopper)
	rpcCtx.NodeID.Set(ctx, staticNodeID)
	_, ln, hb := newTestServer(t, clock, stopper, true /* useHeartbeat */)
	defer stopper.Stop(ctx)
	nd := New(rpcCtx, newSingleNodeResolver(staticNodeID, ln.Addr()))

	// Make sure no connection exists yet, via ConnHealth().
	require.Equal(t, rpc.ErrNoConnection, nd.ConnHealth(staticNodeID, rpc.DefaultClass))

	// When no connection exists, we expect ConnHealthTryDial to dial the node,
	// which will return ErrNoHeartbeat at first but eventually succeed.
	require.Eventually(t, func() bool {
		return nd.ConnHealthTryDial(staticNodeID, rpc.DefaultClass) == nil
	}, time.Second, 10*time.Millisecond)

	// But it should error for other node ID.
	require.Error(t, nd.ConnHealthTryDial(9, rpc.DefaultClass))

	// When the heartbeat errors, ConnHealthTryDial should eventually error too.
	hb.setErr(errors.New("boom"))
	require.Eventually(t, func() bool {
		return nd.ConnHealthTryDial(staticNodeID, rpc.DefaultClass) != nil
	}, time.Second, 10*time.Millisecond)

	// When the heartbeat recovers, ConnHealthTryDial should too.
	hb.setErr(nil)
	require.Eventually(t, func() bool {
		return nd.ConnHealthTryDial(staticNodeID, rpc.DefaultClass) == nil
	}, time.Second, 10*time.Millisecond)

	// Tripping the breaker should return ErrBreakerOpen.
	br := nd.getBreaker(staticNodeID, rpc.DefaultClass)
	br.Trip()
	require.Equal(t, circuit.ErrBreakerOpen, nd.ConnHealthTryDial(staticNodeID, rpc.DefaultClass))

	// But it should eventually recover, when the breaker allows it.
	require.Eventually(t, func() bool {
		return nd.ConnHealthTryDial(staticNodeID, rpc.DefaultClass) == nil
	}, 5*time.Second, 10*time.Millisecond)

	// Closing the remote connection should eventually recover.
	require.NoError(t, ln.popConn().Close())
	require.Eventually(t, func() bool {
		return nd.ConnHealthTryDial(staticNodeID, rpc.DefaultClass) == nil
	}, time.Second, 10*time.Millisecond)
}

func TestConnHealthInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()
	localAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 26657}

	// Set up an internal server and relevant configuration. The RPC connection
	// will then be considered internal, and we don't have to dial it.
	rpcCtx := newTestContext(clock, stopper)
	rpcCtx.SetLocalInternalServer(&internalServer{})
	rpcCtx.NodeID.Set(ctx, staticNodeID)
	rpcCtx.Config.AdvertiseAddr = localAddr.String()

	nd := New(rpcCtx, newSingleNodeResolver(staticNodeID, localAddr))
	defer stopper.Stop(ctx)

	// Even though we haven't dialed the node yet, the internal connection is
	// always healthy.
	require.NoError(t, nd.ConnHealth(staticNodeID, rpc.DefaultClass))
	require.NoError(t, nd.ConnHealth(staticNodeID, rpc.SystemClass))

	// However, it does respect the breaker.
	br := nd.getBreaker(staticNodeID, rpc.DefaultClass)
	br.Trip()
	require.Equal(t, circuit.ErrBreakerOpen, nd.ConnHealth(staticNodeID, rpc.DefaultClass))

	br.Reset()
	require.NoError(t, nd.ConnHealth(staticNodeID, rpc.DefaultClass))

	// Other nodes still fail though.
	require.Error(t, nd.ConnHealth(7, rpc.DefaultClass))
}

func TestConcurrentCancellationAndTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, _, _, _, nd := setUpNodedialerTest(t, staticNodeID)
	defer stopper.Stop(context.Background())
	ctx := context.Background()
	breaker := nd.GetCircuitBreaker(staticNodeID, rpc.DefaultClass)
	// Test that when a context is canceled during dialing we always return that
	// error but we never trip the breaker.
	const N = 1000
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(2)
		// Jiggle when we cancel relative to when we dial to try to hit cases where
		// cancellation happens during the call to GRPCDial.
		iCtx, cancel := context.WithTimeout(ctx, randDuration(time.Millisecond))
		go func() {
			time.Sleep(randDuration(time.Millisecond))
			cancel()
			wg.Done()
		}()
		go func() {
			time.Sleep(randDuration(time.Millisecond))
			_, err := nd.Dial(iCtx, 1, rpc.DefaultClass)
			if err != nil &&
				!errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
				t.Errorf("got an unexpected error from Dial: %v", err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	assert.Equal(t, breaker.Failures(), int64(0))
}

func TestResolverErrorsTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, rpcCtx, _, _, _ := setUpNodedialerTest(t, staticNodeID)
	defer stopper.Stop(context.Background())
	boom := fmt.Errorf("boom")
	nd := New(rpcCtx, func(id roachpb.NodeID) (net.Addr, error) {
		return nil, boom
	})
	_, err := nd.Dial(context.Background(), staticNodeID, rpc.DefaultClass)
	assert.Equal(t, errors.Cause(err), boom)
	breaker := nd.GetCircuitBreaker(staticNodeID, rpc.DefaultClass)
	assert.False(t, breaker.Ready())
}

func TestDisconnectsTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, _, ln, hb, nd := setUpNodedialerTest(t, staticNodeID)
	defer stopper.Stop(context.Background())
	ctx := context.Background()
	breaker := nd.GetCircuitBreaker(staticNodeID, rpc.DefaultClass)

	// Now close the underlying connection from the server side and set the
	// heartbeat service to return errors. This will eventually lead to the client
	// connection being removed and Dial attempts to return an error.
	// While this is going on there will be many clients attempting to
	// connect. These connecting clients will send interesting errors they observe
	// on the errChan. Once an error from Dial is observed the test re-enables the
	// heartbeat service. The test will confirm that the only errors they record
	// in to the breaker are interesting ones as determined by shouldTrip.
	hb.setErr(fmt.Errorf("boom"))
	underlyingNetConn := ln.popConn()
	require.NoError(t, underlyingNetConn.Close())
	const N = 1000
	breakerEventChan := make(chan circuit.ListenerEvent, N)
	breaker.AddListener(breakerEventChan)
	errChan := make(chan error, N)
	shouldTrip := func(err error) bool {
		return err != nil &&
			!errors.IsAny(err, context.DeadlineExceeded, context.Canceled, circuit.ErrBreakerOpen)
	}
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(2)
		iCtx, cancel := context.WithTimeout(ctx, randDuration(time.Millisecond))
		go func() {
			time.Sleep(randDuration(time.Millisecond))
			cancel()
			wg.Done()
		}()
		go func() {
			time.Sleep(randDuration(time.Millisecond))
			_, err := nd.Dial(iCtx, 1, rpc.DefaultClass)
			if shouldTrip(err) {
				errChan <- err
			}
			wg.Done()
		}()
	}
	go func() { wg.Wait(); close(errChan) }()
	var errorsSeen int
	for range errChan {
		if errorsSeen == 0 {
			hb.setErr(nil)
		}
		errorsSeen++
	}
	breaker.RemoveListener(breakerEventChan)
	close(breakerEventChan)
	var failsSeen int
	for ev := range breakerEventChan {
		if ev.Event == circuit.BreakerFail {
			failsSeen++
		}
	}
	// Ensure that all of the interesting errors were seen by the breaker.
	require.Equal(t, errorsSeen, failsSeen)

	// Ensure that the connection eventually becomes healthy if we fix the
	// heartbeat and keep dialing.
	hb.setErr(nil)
	testutils.SucceedsSoon(t, func() error {
		if _, err := nd.Dial(ctx, staticNodeID, rpc.DefaultClass); err != nil {
			return err
		}
		return nd.ConnHealth(staticNodeID, rpc.DefaultClass)
	})
}

func setUpNodedialerTest(
	t *testing.T, nodeID roachpb.NodeID,
) (
	stopper *stop.Stopper,
	rpcCtx *rpc.Context,
	ln *interceptingListener,
	hb *heartbeatService,
	nd *Dialer,
) {
	stopper = stop.NewStopper()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	// Create an rpc Context and then
	rpcCtx = newTestContext(clock, stopper)
	rpcCtx.NodeID.Set(context.Background(), nodeID)
	_, ln, hb = newTestServer(t, clock, stopper, true /* useHeartbeat */)
	nd = New(rpcCtx, newSingleNodeResolver(nodeID, ln.Addr()))
	_, err := nd.Dial(context.Background(), nodeID, rpc.DefaultClass)
	require.NoError(t, err)
	testutils.SucceedsSoon(t, func() error {
		return nd.ConnHealth(nodeID, rpc.DefaultClass)
	})
	return stopper, rpcCtx, ln, hb, nd
}

// randDuration returns a uniform random duration between 0 and max.
func randDuration(max time.Duration) time.Duration {
	return time.Duration(rand.Intn(int(max)))
}

func newTestServer(
	t testing.TB, clock *hlc.Clock, stopper *stop.Stopper, useHeartbeat bool,
) (*grpc.Server, *interceptingListener, *heartbeatService) {
	ctx := context.Background()
	localAddr := "127.0.0.1:0"
	ln, err := net.Listen("tcp", localAddr)
	if err != nil {
		t.Fatalf("failed to listed on %v: %v", localAddr, err)
	}
	il := &interceptingListener{Listener: ln}
	s := grpc.NewServer()
	var hb *heartbeatService
	if useHeartbeat {
		hb = &heartbeatService{
			clock:         clock,
			serverVersion: clusterversion.TestingBinaryVersion,
		}
		rpc.RegisterHeartbeatServer(s, hb)
	}
	if err := stopper.RunAsyncTask(ctx, "localServer", func(ctx context.Context) {
		if err := s.Serve(il); err != nil {
			log.Infof(ctx, "server stopped: %v", err)
		}
	}); err != nil {
		t.Fatalf("failed to run test server: %v", err)
	}
	go func() { <-stopper.ShouldQuiesce(); s.Stop() }()
	return s, il, hb
}

func newTestContext(clock *hlc.Clock, stopper *stop.Stopper) *rpc.Context {
	cfg := testutils.NewNodeTestBaseContext()
	cfg.Insecure = true
	cfg.RPCHeartbeatInterval = 100 * time.Millisecond
	ctx := context.Background()
	rctx := rpc.NewContext(ctx, rpc.ContextOptions{
		TenantID: roachpb.SystemTenantID,
		Config:   cfg,
		Clock:    clock,
		Stopper:  stopper,
		Settings: cluster.MakeTestingClusterSettings(),
	})
	// Ensure that tests using this test context and restart/shut down
	// their servers do not inadvertently start talking to servers from
	// unrelated concurrent tests.
	rctx.ClusterID.Set(ctx, uuid.MakeV4())

	return rctx
}

// interceptingListener wraps a net.Listener and provides access to the
// underlying net.Conn objects which that listener Accepts.
type interceptingListener struct {
	net.Listener
	mu struct {
		syncutil.Mutex
		conns []net.Conn
	}
}

// newSingleNodeResolver returns a Resolver that resolve a single node id
func newSingleNodeResolver(id roachpb.NodeID, addr net.Addr) AddressResolver {
	return func(toResolve roachpb.NodeID) (net.Addr, error) {
		if id == toResolve {
			return addr, nil
		}
		return nil, fmt.Errorf("unknown node id %d", toResolve)
	}
}

func (il *interceptingListener) Accept() (c net.Conn, err error) {
	defer func() {
		if err == nil {
			il.mu.Lock()
			il.mu.conns = append(il.mu.conns, c)
			il.mu.Unlock()
		}
	}()
	return il.Listener.Accept()
}

func (il *interceptingListener) popConn() net.Conn {
	il.mu.Lock()
	defer il.mu.Unlock()
	if len(il.mu.conns) == 0 {
		return nil
	}
	c := il.mu.conns[0]
	il.mu.conns = il.mu.conns[1:]
	return c
}

type errContainer struct {
	syncutil.RWMutex
	err error
}

func (ec *errContainer) getErr() error {
	ec.RLock()
	defer ec.RUnlock()
	return ec.err
}

func (ec *errContainer) setErr(err error) {
	ec.Lock()
	defer ec.Unlock()
	ec.err = err
}

// heartbeatService is a dummy rpc.HeartbeatService which provides a mechanism
// to inject errors.
type heartbeatService struct {
	errContainer
	clock         *hlc.Clock
	serverVersion roachpb.Version
}

func (hb *heartbeatService) Ping(
	ctx context.Context, args *rpc.PingRequest,
) (*rpc.PingResponse, error) {
	if err := hb.getErr(); err != nil {
		return nil, err
	}
	return &rpc.PingResponse{
		Pong:          args.Ping,
		ServerTime:    hb.clock.PhysicalNow(),
		ServerVersion: hb.serverVersion,
	}, nil
}

var _ roachpb.InternalServer = &internalServer{}

type internalServer struct{}

func (*internalServer) Batch(
	context.Context, *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	return nil, nil
}

func (*internalServer) RangeLookup(
	context.Context, *roachpb.RangeLookupRequest,
) (*roachpb.RangeLookupResponse, error) {
	panic("unimplemented")
}

func (*internalServer) RangeFeed(
	*roachpb.RangeFeedRequest, roachpb.Internal_RangeFeedServer,
) error {
	panic("unimplemented")
}

func (*internalServer) GossipSubscription(
	*roachpb.GossipSubscriptionRequest, roachpb.Internal_GossipSubscriptionServer,
) error {
	panic("unimplemented")
}

func (*internalServer) ResetQuorum(
	context.Context, *roachpb.ResetQuorumRequest,
) (*roachpb.ResetQuorumResponse, error) {
	panic("unimplemented")
}

func (*internalServer) Join(
	context.Context, *roachpb.JoinNodeRequest,
) (*roachpb.JoinNodeResponse, error) {
	panic("unimplemented")
}

func (*internalServer) TokenBucket(
	ctx context.Context, in *roachpb.TokenBucketRequest,
) (*roachpb.TokenBucketResponse, error) {
	panic("unimplemented")
}

func (*internalServer) GetSpanConfigs(
	context.Context, *roachpb.GetSpanConfigsRequest,
) (*roachpb.GetSpanConfigsResponse, error) {
	panic("unimplemented")
}

func (*internalServer) UpdateSpanConfigs(
	context.Context, *roachpb.UpdateSpanConfigsRequest,
) (*roachpb.UpdateSpanConfigsResponse, error) {
	panic("unimplemented")
}

func (*internalServer) TenantSettings(
	*roachpb.TenantSettingsRequest, roachpb.Internal_TenantSettingsServer,
) error {
	panic("unimplemented")
}
