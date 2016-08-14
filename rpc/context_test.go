// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package rpc

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/netutil"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/pkg/errors"
)

func newTestServer(t *testing.T, ctx *Context, manual bool) (*grpc.Server, net.Listener) {
	tlsConfig, err := ctx.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))

	ln, err := netutil.ListenAndServeGRPC(ctx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}

	return s, ln
}

func TestHeartbeatCB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	clock := hlc.NewClock(time.Unix(0, 20).UnixNano)
	serverCtx := newNodeTestContext(clock, stopper)
	s, ln := newTestServer(t, serverCtx, true)
	remoteAddr := ln.Addr().String()

	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
	})

	// Clocks don't matter in this test.
	clientCtx := newNodeTestContext(clock, stopper)

	var once sync.Once
	ch := make(chan struct{})

	clientCtx.HeartbeatCB = func() {
		once.Do(func() {
			close(ch)
		})
	}

	_, err := clientCtx.GRPCDial(remoteAddr)
	if err != nil {
		t.Fatal(err)
	}

	<-ch
}

// TestHeartbeatHealth verifies that the health status changes after
// heartbeats succeed or fail.
func TestHeartbeatHealth(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(time.Unix(0, 1).UnixNano)

	serverCtx := newNodeTestContext(clock, stopper)
	s, ln := newTestServer(t, serverCtx, true)
	remoteAddr := ln.Addr().String()

	heartbeat := &ManualHeartbeatService{
		ready:              make(chan struct{}),
		stopper:            stopper,
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
	}
	RegisterHeartbeatServer(s, heartbeat)

	clientCtx := newNodeTestContext(clock, stopper)
	// Make the intervals and timeouts shorter to speed up the tests.
	clientCtx.HeartbeatInterval = 1 * time.Millisecond
	clientCtx.HeartbeatTimeout = 1 * time.Millisecond
	// Expect a failure on the initial dial as no heartbeat is available.
	if _, err := clientCtx.GRPCDial(remoteAddr); !testutils.IsError(err, "context deadline exceeded") {
		t.Fatal(err)
	}

	// This code is inherently racy so when we need to verify heartbeats we
	// want them to always succeed.
	sendHeartbeats := func() func() {
		done := make(chan struct{})
		go func() {
			for {
				select {
				case <-done:
					return
				case heartbeat.ready <- struct{}{}:
				}
			}
		}()
		return func() {
			done <- struct{}{}
		}
	}

	// Should be unhealthy in the absence of heartbeats.
	if clientCtx.IsConnHealthy(remoteAddr) {
		t.Fatalf("expected %s to be unhealthy", remoteAddr)
	}

	func() {
		defer sendHeartbeats()()

		// Should become healthy in the presence of heartbeats.
		util.SucceedsSoon(t, func() error {
			if !clientCtx.IsConnHealthy(remoteAddr) {
				return errors.Errorf("expected %s to be healthy", remoteAddr)
			}
			return nil
		})
	}()

	// Should become unhealthy again in the absence of heartbeats.
	util.SucceedsSoon(t, func() error {
		if clientCtx.IsConnHealthy(remoteAddr) {
			return errors.Errorf("expected %s to be unhealthy", remoteAddr)
		}
		return nil
	})

	func() {
		defer sendHeartbeats()()

		// Should become healthy again in the presence of heartbeats.
		util.SucceedsSoon(t, func() error {
			if !clientCtx.IsConnHealthy(remoteAddr) {
				return errors.Errorf("expected %s to be healthy", remoteAddr)
			}
			return nil
		})
	}()

	if clientCtx.IsConnHealthy("non-existent connection") {
		t.Errorf("non-existent connection is reported as healthy")
	}
}

type interceptingListener struct {
	net.Listener
	connectChan chan struct{}
	connCB      func(net.Conn)
}

func (ln *interceptingListener) Accept() (net.Conn, error) {
	<-ln.connectChan
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
	defer stopper.Stop()

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(time.Unix(0, 1).UnixNano)

	serverCtx := newNodeTestContext(clock, stopper)
	// newTestServer with a custom listener.
	tlsConfig, err := serverCtx.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	ln, err := net.Listen("tcp", util.TestAddr.String())
	if err != nil {
		t.Fatal(err)
	}
	mu := struct {
		syncutil.Mutex
		conns []net.Conn
	}{}
	connectChan := make(chan struct{})
	defer close(connectChan)
	ln = &interceptingListener{Listener: ln, connectChan: connectChan, connCB: func(conn net.Conn) {
		mu.Lock()
		mu.conns = append(mu.conns, conn)
		mu.Unlock()
	}}
	stopper.RunWorker(func() {
		<-stopper.ShouldQuiesce()
		netutil.FatalIfUnexpected(ln.Close())
		<-stopper.ShouldStop()
		s.Stop()
	})

	stopper.RunWorker(func() {
		netutil.FatalIfUnexpected(s.Serve(ln))
	})

	remoteAddr := ln.Addr().String()

	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
	})

	clientCtx := newNodeTestContext(clock, stopper)
	// Make the intervals and timeouts shorter to speed up the tests.
	clientCtx.HeartbeatInterval = 1 * time.Millisecond
	clientCtx.HeartbeatTimeout = 1 * time.Millisecond
	if _, err := clientCtx.GRPCDial(remoteAddr); !testutils.IsError(err, "context deadline exceeded") {
		t.Fatal(err)
	}
	// Allow the connection to go through.
	connectChan <- struct{}{}

	// Everything is normal; should become healthy.
	util.SucceedsSoon(t, func() error {
		if !clientCtx.IsConnHealthy(remoteAddr) {
			return errors.Errorf("expected %s to be healthy", remoteAddr)
		}
		return nil
	})

	closeConns := func() {
		mu.Lock()
		for _, conn := range mu.conns {
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
		}
		mu.conns = mu.conns[:0]
		mu.Unlock()
	}

	// Close all the connections.
	closeConns()

	// Should become unhealthy now that the connection was closed.
	util.SucceedsSoon(t, func() error {
		if clientCtx.IsConnHealthy(remoteAddr) {
			return errors.Errorf("expected %s to be unhealthy", remoteAddr)
		}
		return nil
	})

	// Should become healthy again after GRPC reconnects.
	connectChan <- struct{}{}
	util.SucceedsSoon(t, func() error {
		if !clientCtx.IsConnHealthy(remoteAddr) {
			return errors.Errorf("expected %s to be healthy", remoteAddr)
		}
		return nil
	})

	// Close the listener and all the connections.
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	closeConns()

	// Should become unhealthy again now that the connection was closed.
	util.SucceedsSoon(t, func() error {
		if clientCtx.IsConnHealthy(remoteAddr) {
			return errors.Errorf("expected %s to be unhealthy", remoteAddr)
		}
		return nil
	})

	// Should stay unhealthy despite reconnection attempts.
	errUnhealthy := errors.New("connection is still unhealthy")
	if err := util.RetryForDuration(100*clientCtx.HeartbeatInterval, func() error {
		if clientCtx.IsConnHealthy(remoteAddr) {
			return errors.Errorf("expected %s to be unhealthy", remoteAddr)
		}
		return errUnhealthy
	}); err != errUnhealthy {
		t.Fatal(err)
	}
}

func TestOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	serverTime := time.Unix(0, 20)
	serverClock := hlc.NewClock(serverTime.UnixNano)
	serverCtx := newNodeTestContext(serverClock, stopper)
	s, ln := newTestServer(t, serverCtx, true)
	remoteAddr := ln.Addr().String()

	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: serverCtx.RemoteClocks,
	})

	// Create a client clock that is behind the server clock.
	clientAdvancing := AdvancingClock{time: time.Unix(0, 10)}
	clientClock := hlc.NewClock(clientAdvancing.UnixNano)
	clientClock.SetMaxOffset(time.Millisecond)
	clientCtx := newNodeTestContext(clientClock, stopper)
	clientCtx.RemoteClocks.offsetTTL = 5 * clientAdvancing.getAdvancementInterval()
	if _, err := clientCtx.GRPCDial(remoteAddr); err != nil {
		t.Fatal(err)
	}

	expectedOffset := RemoteOffset{Offset: 10, Uncertainty: 0, MeasuredAt: 10}
	util.SucceedsSoon(t, func() error {
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

	util.SucceedsSoon(t, func() error {
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
	defer stopper.Stop()

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(time.Unix(0, 1).UnixNano)

	serverCtx := newNodeTestContext(clock, stopper)
	s, ln := newTestServer(t, serverCtx, true)
	remoteAddr := ln.Addr().String()

	heartbeat := &ManualHeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		ready:              make(chan struct{}),
		stopper:            stopper,
	}
	RegisterHeartbeatServer(s, heartbeat)

	// Create a client that never receives a heartbeat after the first.
	clientCtx := newNodeTestContext(clock, stopper)
	// Increase the timeout so that failure arises from exceeding the maximum
	// clock reading delay, not the timeout.
	clientCtx.HeartbeatTimeout = 20 * clientCtx.HeartbeatInterval
	stopper.RunWorker(func() {
		if _, err := clientCtx.GRPCDial(remoteAddr); err != nil {
			t.Fatal(err)
		}
	})
	heartbeat.ready <- struct{}{} // Allow one heartbeat for initialization.

	util.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()

		if _, ok := clientCtx.RemoteClocks.mu.offsets[remoteAddr]; !ok {
			return errors.Errorf("expected offset of %s to be initialized, but it was not", remoteAddr)
		}
		return nil
	})

	util.SucceedsSoon(t, func() error {
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
	defer stopper.Stop()

	const maxOffset = 100 * time.Millisecond

	type nodeContext struct {
		offset  time.Duration
		ctx     *Context
		errChan chan error
	}

	start := time.Date(2012, 12, 07, 0, 0, 0, 0, time.UTC)
	offsetClock := func(offset time.Duration) *hlc.Clock {
		return hlc.NewClock(func() int64 {
			return start.Add(offset).UnixNano()
		})
	}

	nodeCtxs := []nodeContext{
		{offset: 0},
		{offset: 0},
		{offset: 0},
		// The minimum offset that actually triggers node death.
		{offset: maxOffset + 1},
	}

	for i := range nodeCtxs {
		clock := offsetClock(nodeCtxs[i].offset)
		nodeCtxs[i].errChan = make(chan error, 1)

		clock.SetMaxOffset(maxOffset)
		nodeCtxs[i].ctx = newNodeTestContext(clock, stopper)
		nodeCtxs[i].ctx.HeartbeatInterval = maxOffset

		s, ln := newTestServer(t, nodeCtxs[i].ctx, true)
		RegisterHeartbeatServer(s, &HeartbeatService{
			clock:              clock,
			remoteClockMonitor: nodeCtxs[i].ctx.RemoteClocks,
		})
		nodeCtxs[i].ctx.Addr = ln.Addr().String()
	}

	// Fully connect the nodes.
	for i, clientNodeContext := range nodeCtxs {
		for j, serverNodeContext := range nodeCtxs {
			if i == j {
				continue
			}
			if _, err := clientNodeContext.ctx.GRPCDial(serverNodeContext.ctx.Addr); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Wait until all nodes are connected to all other nodes.
	for _, nodeCtx := range nodeCtxs {
		util.SucceedsSoon(t, func() error {
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
			if err := nodeCtx.ctx.RemoteClocks.VerifyClockOffset(); testutils.IsError(err, errOffsetGreaterThanMaxOffset) {
				t.Logf("max offset: %s - node %d with excessive clock offset of %s returned expected error: %s", maxOffset, i, nodeOffset, err)
			} else {
				t.Errorf("max offset: %s - node %d with excessive clock offset of %s returned unexpected error: %s", maxOffset, i, nodeOffset, err)
			}
		} else {
			if err := nodeCtx.ctx.RemoteClocks.VerifyClockOffset(); err != nil {
				t.Errorf("max offset: %s - node %d with acceptable clock offset of %s returned unexpected error: %s", maxOffset, i, nodeOffset, err)
			} else {
				t.Logf("max offset: %s - node %d with acceptable clock offset of %s did not return an error, as expected", maxOffset, i, nodeOffset)
			}
		}
	}
}
