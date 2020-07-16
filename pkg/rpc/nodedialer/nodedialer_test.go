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
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
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

	// Test that DialNoBreaker is successful normally.
	nd := New(rpcCtx, newSingleNodeResolver(staticNodeID, ln.Addr()))
	testutils.SucceedsSoon(t, func() error {
		return nd.ConnHealth(staticNodeID, rpc.DefaultClass)
	})
	breaker := nd.GetCircuitBreaker(staticNodeID, rpc.DefaultClass)
	assert.True(t, breaker.Ready())
	_, err := nd.DialNoBreaker(ctx, staticNodeID, rpc.DefaultClass)
	assert.Nil(t, err, "failed to dial")
	assert.True(t, breaker.Ready())
	assert.Equal(t, breaker.Failures(), int64(0))

	// Test that resolver errors don't trip the breaker.
	boom := fmt.Errorf("boom")
	nd = New(rpcCtx, func(roachpb.NodeID) (net.Addr, error) {
		return nil, boom
	})
	breaker = nd.GetCircuitBreaker(staticNodeID, rpc.DefaultClass)
	_, err = nd.DialNoBreaker(ctx, staticNodeID, rpc.DefaultClass)
	assert.Equal(t, errors.Cause(err), boom)
	assert.True(t, breaker.Ready())
	assert.Equal(t, breaker.Failures(), int64(0))

	// Test that connection errors don't trip the breaker either.
	// To do this, we have to trick grpc into never successfully dialing
	// the server, because if it succeeds once then it doesn't try again
	// to perform a connection. To trick grpc in this way, we have to
	// set up a server without the heartbeat service running. Without
	// getting a heartbeat, the nodedialer will throw an error thinking
	// that it wasn't able to successfully make a connection.
	_, ln, _ = newTestServer(t, clock, stopper, false /* useHeartbeat */)
	nd = New(rpcCtx, newSingleNodeResolver(staticNodeID, ln.Addr()))
	breaker = nd.GetCircuitBreaker(staticNodeID, rpc.DefaultClass)
	_, err = nd.DialNoBreaker(ctx, staticNodeID, rpc.DefaultClass)
	assert.NotNil(t, err, "expected dial error")
	assert.True(t, breaker.Ready())
	assert.Equal(t, breaker.Failures(), int64(0))
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
	assert.Nil(t, underlyingNetConn.Close())
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
	assert.Equal(t, errorsSeen, failsSeen)

	// Ensure that the connection becomes healthy soon now that the heartbeat
	// service is not returning errors.
	hb.setErr(nil) // reset in case there were no errors
	testutils.SucceedsSoon(t, func() error {
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
	cfg.RPCHeartbeatInterval = 10 * time.Millisecond
	rctx := rpc.NewContext(rpc.ContextOptions{
		TenantID:   roachpb.SystemTenantID,
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Config:     cfg,
		Clock:      clock,
		Stopper:    stopper,
		Settings:   cluster.MakeTestingClusterSettings(),
	})
	// Ensure that tests using this test context and restart/shut down
	// their servers do not inadvertently start talking to servers from
	// unrelated concurrent tests.
	rctx.ClusterID.Set(context.Background(), uuid.MakeV4())

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
