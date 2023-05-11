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
	"sync/atomic"
	"testing"
	"time"

	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	circuitbreaker "github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	clock := &timeutil.DefaultTimeSource{}
	maxOffset := time.Nanosecond
	rpcCtx := newTestContext(clock, maxOffset, stopper)
	rpcCtx.NodeID.Set(ctx, staticNodeID)
	_, ln, _ := newTestServer(t, clock, stopper, true /* useHeartbeat */)
	defer stopper.Stop(ctx)

	nd := New(rpcCtx, newSingleNodeResolver(staticNodeID, ln.Addr()))
	_, err := nd.Dial(ctx, staticNodeID, rpc.DefaultClass)
	require.NoError(t, err)
	testutils.SucceedsSoon(t, func() error {
		return nd.ConnHealth(staticNodeID, rpc.DefaultClass)
	})

	require.NoError(t, rpcCtx.ConnHealth(ln.Addr().String(), staticNodeID, rpc.DefaultClass))

	// Test that DialNoBreaker is successful normally.
	conn := rpcCtx.GRPCDialNode(ln.Addr().String(), staticNodeID, rpc.DefaultClass)
	require.NoError(t, conn.Signal().Err())
	_, err = nd.DialNoBreaker(ctx, staticNodeID, rpc.DefaultClass)
	require.NoError(t, err)
	// Ditto regular dial.
	_, err = nd.Dial(ctx, staticNodeID, rpc.DefaultClass)
	require.NoError(t, err)

	injErr := errors.New("injected error")

	// Mock-trip the breaker. (This leaves the connection intact).
	{
		b, ok := rpcCtx.GetBreakerForAddr(staticNodeID, rpc.DefaultClass, ln.Addr())
		require.True(t, ok)
		undo := circuitbreaker.TestingSetTripped(b, injErr)
		defer undo()
	}
	// Regular Dial should be refused, but DialNoBreaker will
	// still work.

	_, err = nd.Dial(ctx, staticNodeID, rpc.DefaultClass)
	require.True(t, errors.Is(err, injErr), "%+v", err)

	_, err = nd.DialNoBreaker(ctx, staticNodeID, rpc.DefaultClass)
	require.NoError(t, err)
}

func TestConnHealth(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	clock := &timeutil.DefaultTimeSource{}
	maxOffset := time.Nanosecond
	rpcCtx := newTestContext(clock, maxOffset, stopper)
	rpcCtx.NodeID.Set(ctx, staticNodeID)
	_, ln, hb := newTestServer(t, clock, stopper, true /* useHeartbeat */)
	defer stopper.Stop(ctx)
	nd := New(rpcCtx, newSingleNodeResolver(staticNodeID, ln.Addr()))

	var hbDecommission atomic.Value
	hbDecommission.Store(false)
	rpcCtx.OnOutgoingPing = func(ctx context.Context, req *rpc.PingRequest) error {
		if hbDecommission.Load().(bool) {
			return kvpb.NewDecommissionedStatusErrorf(
				codes.PermissionDenied, "target node n%s is decommissioned", req.TargetNodeID,
			)
		}
		return nil
	}

	// When no connection exists, we expect ConnHealth to return ErrNotHeartbeated.
	require.Equal(t, rpc.ErrNotHeartbeated, nd.ConnHealth(staticNodeID, rpc.DefaultClass))

	// After dialing the node, ConnHealth should return nil.
	_, err := nd.Dial(ctx, staticNodeID, rpc.DefaultClass)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return nd.ConnHealth(staticNodeID, rpc.DefaultClass) == nil
	}, time.Second, 10*time.Millisecond)

	// ConnHealth should still error for other node ID and class.
	require.Error(t, nd.ConnHealth(9, rpc.DefaultClass))
	require.Equal(t, rpc.ErrNotHeartbeated, nd.ConnHealth(staticNodeID, rpc.SystemClass))

	// When the heartbeat errors, ConnHealth should eventually error too.
	hb.setErr(errors.New("boom"))
	require.Eventually(t, func() bool {
		return nd.ConnHealth(staticNodeID, rpc.DefaultClass) != nil
	}, time.Second, 10*time.Millisecond)

	// When the heartbeat recovers, ConnHealth should too, assuming someone
	// dials the node.
	hb.setErr(nil)
	{
		// In practice this dial almost always immediately succeeds. However,
		// because the previous connection was marked as unhealthy and might
		// still just be about to be removed, the DialNoBreaker might pull
		// the broken connection out of the pool and fail on it.
		//
		// See: https://github.com/cockroachdb/cockroach/issues/91798
		require.Eventually(t, func() bool {
			_, err := nd.DialNoBreaker(ctx, staticNodeID, rpc.DefaultClass)
			return err == nil
		}, 10*time.Second, time.Millisecond)
		require.Eventually(t, func() bool {
			return nd.ConnHealth(staticNodeID, rpc.DefaultClass) == nil
		}, time.Second, 10*time.Millisecond)
	}

	// Closing the remote connection should fail ConnHealth.
	require.NoError(t, ln.popConn().Close())
	hbDecommission.Store(true)
	require.Eventually(t, func() bool {
		return nd.ConnHealth(staticNodeID, rpc.DefaultClass) != nil
	}, time.Second, 10*time.Millisecond)
}

func TestConnHealthTryDial(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	clock := &timeutil.DefaultTimeSource{}
	maxOffset := time.Nanosecond
	rpcCtx := newTestContext(clock, maxOffset, stopper)
	rpcCtx.NodeID.Set(ctx, staticNodeID)
	_, ln, hb := newTestServer(t, clock, stopper, true /* useHeartbeat */)
	defer stopper.Stop(ctx)
	nd := New(rpcCtx, newSingleNodeResolver(staticNodeID, ln.Addr()))

	// Make sure no connection exists yet, via ConnHealth().
	require.Equal(t, rpc.ErrNotHeartbeated, nd.ConnHealth(staticNodeID, rpc.DefaultClass))

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

	// Closing the remote connection should eventually recover.
	require.NoError(t, ln.popConn().Close())
	require.Eventually(t, func() bool {
		return nd.ConnHealthTryDial(staticNodeID, rpc.DefaultClass) == nil
	}, time.Second, 10*time.Millisecond)
}

func TestConnHealthInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	clock := &timeutil.DefaultTimeSource{}
	maxOffset := time.Nanosecond
	stopper := stop.NewStopper()
	localAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 26657}

	// Set up an internal server and relevant configuration. The RPC connection
	// will then be considered internal, and we don't have to dial it.
	rpcCtx := newTestContext(clock, maxOffset, stopper)
	rpcCtx.SetLocalInternalServer(
		&internalServer{},
		rpc.ServerInterceptorInfo{}, rpc.ClientInterceptorInfo{})
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
	clock := &timeutil.DefaultTimeSource{}
	maxOffset := time.Nanosecond
	// Create an rpc Context and then
	rpcCtx = newTestContext(clock, maxOffset, stopper)
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
	t testing.TB, clock hlc.WallClock, stopper *stop.Stopper, useHeartbeat bool,
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

func newTestContext(
	clock hlc.WallClock, maxOffset time.Duration, stopper *stop.Stopper,
) *rpc.Context {
	cfg := testutils.NewNodeTestBaseContext()
	cfg.Insecure = true
	cfg.RPCHeartbeatInterval = 100 * time.Millisecond
	cfg.RPCHeartbeatTimeout = 500 * time.Millisecond
	ctx := context.Background()
	rctx := rpc.NewContext(ctx, rpc.ContextOptions{
		TenantID:        roachpb.SystemTenantID,
		Config:          cfg,
		Clock:           clock,
		ToleratedOffset: maxOffset,
		Stopper:         stopper,
		Settings:        cluster.MakeTestingClusterSettings(),
	})
	// Ensure that tests using this test context and restart/shut down
	// their servers do not inadvertently start talking to servers from
	// unrelated concurrent tests.
	rctx.StorageClusterID.Set(ctx, uuid.MakeV4())

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
	n := len(il.mu.conns)
	c := il.mu.conns[n-1]
	il.mu.conns = il.mu.conns[:n-1]
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
	clock         hlc.WallClock
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
		ServerTime:    hb.clock.Now().UnixNano(),
		ServerVersion: hb.serverVersion,
	}, nil
}

var _ kvpb.InternalServer = &internalServer{}

type internalServer struct{}

func (*internalServer) Batch(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
	return nil, nil
}

func (*internalServer) RangeLookup(
	context.Context, *kvpb.RangeLookupRequest,
) (*kvpb.RangeLookupResponse, error) {
	panic("unimplemented")
}

func (*internalServer) RangeFeed(*kvpb.RangeFeedRequest, kvpb.Internal_RangeFeedServer) error {
	panic("unimplemented")
}

func (s *internalServer) MuxRangeFeed(server kvpb.Internal_MuxRangeFeedServer) error {
	panic("implement me")
}

func (*internalServer) GossipSubscription(
	*kvpb.GossipSubscriptionRequest, kvpb.Internal_GossipSubscriptionServer,
) error {
	panic("unimplemented")
}

func (*internalServer) ResetQuorum(
	context.Context, *kvpb.ResetQuorumRequest,
) (*kvpb.ResetQuorumResponse, error) {
	panic("unimplemented")
}

func (*internalServer) Join(
	context.Context, *kvpb.JoinNodeRequest,
) (*kvpb.JoinNodeResponse, error) {
	panic("unimplemented")
}

func (*internalServer) TokenBucket(
	ctx context.Context, in *kvpb.TokenBucketRequest,
) (*kvpb.TokenBucketResponse, error) {
	panic("unimplemented")
}

func (*internalServer) GetSpanConfigs(
	context.Context, *roachpb.GetSpanConfigsRequest,
) (*roachpb.GetSpanConfigsResponse, error) {
	panic("unimplemented")
}

func (*internalServer) GetAllSystemSpanConfigsThatApply(
	context.Context, *roachpb.GetAllSystemSpanConfigsThatApplyRequest,
) (*roachpb.GetAllSystemSpanConfigsThatApplyResponse, error) {
	panic("unimplemented")
}

func (*internalServer) UpdateSpanConfigs(
	context.Context, *roachpb.UpdateSpanConfigsRequest,
) (*roachpb.UpdateSpanConfigsResponse, error) {
	panic("unimplemented")
}

func (s *internalServer) SpanConfigConformance(
	context.Context, *roachpb.SpanConfigConformanceRequest,
) (*roachpb.SpanConfigConformanceResponse, error) {
	panic("unimplemented")
}

func (*internalServer) TenantSettings(
	*kvpb.TenantSettingsRequest, kvpb.Internal_TenantSettingsServer,
) error {
	panic("unimplemented")
}

func (*internalServer) GetRangeDescriptors(
	*kvpb.GetRangeDescriptorsRequest, kvpb.Internal_GetRangeDescriptorsServer,
) error {
	panic("unimplemented")
}
