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

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// newNodeTestContext returns a rpc.Context for testing.
// It is meant to be used by nodes.
func newNodeTestContext(clock *hlc.Clock, stopper *stop.Stopper) *Context {
	cfg := ContextConfig{
		Config:            testutils.NewNodeTestBaseConfig(),
		HLCClock:          clock,
		HeartbeatInterval: 10 * time.Millisecond,
		HeartbeatTimeout:  10 * time.Millisecond,
	}
	ctx := NewContext(log.AmbientContext{}, cfg, stopper)
	return ctx
}

// newTestServer returns a grpc.Server listening on addr. cfg.Addr is updated
// with the port the server is listening on.
func newTestServer(
	t *testing.T, stopper *stop.Stopper, cfg *base.Config, addr net.Addr,
) (*grpc.Server, net.Listener) {
	tlsConfig, err := cfg.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))

	ln, err := netutil.ListenAndServeGRPC(stopper, s, addr)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Addr = ln.Addr().String()

	return s, ln
}

func TestClockCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	// Clocks don't matter in this test.
	clock := hlc.NewClock(time.Unix(0, 20).UnixNano, time.Nanosecond)
	s, ln := newTestServer(t, stopper, testutils.NewNodeTestBaseConfig(), util.TestAddr)
	remoteAddr := ln.Addr().String()

	RegisterHeartbeatServer(s, &HeartbeatService{
		clock: clock,
		remoteClockMonitor: newRemoteClockMonitor(
			clock, time.Second /* offsetTTL */, time.Second /* histogramWindowInterval */),
	})

	var once sync.Once
	ch := make(chan struct{})
	cfg := ContextConfig{
		Config:                testutils.NewNodeTestBaseConfig(),
		HLCClock:              clock,
		HeartbeatInterval:     10 * time.Millisecond,
		HeartbeatTimeout:      10 * time.Millisecond,
		EnableClockSkewChecks: true,
		TestingKnobs: ContextTestingKnobs{
			PeriodicClockCheckHook: func() error {
				once.Do(func() {
					close(ch)
				})
				return nil
			},
		},
	}
	clientCtx := NewContext(log.AmbientContext{}, cfg, stopper)

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
	clock := hlc.NewClock(time.Unix(0, 1).UnixNano, time.Nanosecond)

	s, ln := newTestServer(t, stopper, testutils.NewNodeTestBaseConfig(), util.TestAddr)
	remoteAddr := ln.Addr().String()
	serverClockMonitor := newRemoteClockMonitor(
		clock, time.Second /* offsetTTL */, time.Second /* histogramWindowInterval */)

	heartbeat := &ManualHeartbeatService{
		ready:              make(chan error),
		stopper:            stopper,
		clock:              clock,
		remoteClockMonitor: serverClockMonitor,
	}
	RegisterHeartbeatServer(s, heartbeat)

	cfg := ContextConfig{
		Config:            testutils.NewNodeTestBaseConfig(),
		HLCClock:          clock,
		HeartbeatInterval: time.Millisecond,
		// High timeout, otherwise heartbeats sometimes timeout under stress.
		HeartbeatTimeout: time.Second,
	}
	clientCtx := NewContext(log.AmbientContext{}, cfg, stopper)

	if _, err := clientCtx.GRPCDial(remoteAddr); err != nil {
		t.Fatal(err)
	}

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

	// Wait for the connection.
	testutils.SucceedsSoon(t, func() error {
		err := clientCtx.ConnHealth(remoteAddr)
		if err != nil && err != errNotHeartbeated {
			t.Fatal(err)
		}
		return err
	})

	// Should be unhealthy in the presence of failing heartbeats.
	hbSuccess.Store(false)
	testutils.SucceedsSoon(t, func() error {
		if err := clientCtx.ConnHealth(remoteAddr); !testutils.IsError(err, errFailedHeartbeat.Error()) {
			return errors.Errorf("unexpected error: %v", err)
		}
		return nil
	})

	// Should become healthy in the presence of successful heartbeats.
	hbSuccess.Store(true)
	testutils.SucceedsSoon(t, func() error {
		return clientCtx.ConnHealth(remoteAddr)
	})

	// Should become unhealthy again in the presence of failing heartbeats.
	hbSuccess.Store(false)
	testutils.SucceedsSoon(t, func() error {
		if err := clientCtx.ConnHealth(remoteAddr); !testutils.IsError(err, errFailedHeartbeat.Error()) {
			return errors.Errorf("unexpected error: %v", err)
		}
		return nil
	})

	// Should become healthy in the presence of successful heartbeats.
	hbSuccess.Store(true)
	testutils.SucceedsSoon(t, func() error {
		return clientCtx.ConnHealth(remoteAddr)
	})

	if err := clientCtx.ConnHealth("non-existent connection"); err != errNotConnected {
		t.Errorf("unexpected error: %v", err)
	}
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
	t.Skip("#13734")

	if testing.Short() {
		t.Skip("short flag")
	}

	stopper := stop.NewStopper()
	defer stopper.Stop()

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(time.Unix(0, 1).UnixNano, time.Nanosecond)

	serverCtx := newNodeTestContext(clock, stopper)
	// newTestServer with a custom listener.
	tlsConfig, err := serverCtx.cfg.GetServerTLSConfig()
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
	ln = &interceptingListener{Listener: ln, connCB: func(conn net.Conn) {
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
	if _, err := clientCtx.GRPCDial(remoteAddr); err != nil {
		t.Fatal(err)
	}
	// Everything is normal; should become healthy.
	testutils.SucceedsSoon(t, func() error {
		return clientCtx.ConnHealth(remoteAddr)
	})

	closeConns := func() error {
		mu.Lock()
		defer mu.Unlock()

		for i := len(mu.conns) - 1; i >= 0; i-- {
			if err := mu.conns[i].Close(); err != nil {
				return err
			}
			mu.conns = mu.conns[:i]
		}
		return nil
	}

	testutils.SucceedsSoon(t, func() error {
		// Close all the connections until we see a failure.
		if err := closeConns(); err != nil {
			t.Fatal(err)
		}

		if err := clientCtx.ConnHealth(remoteAddr); grpc.Code(err) != codes.Unavailable {
			return errors.Errorf("unexpected error: %v", err)
		}
		return nil
	})

	// Should become healthy again after GRPC reconnects.
	testutils.SucceedsSoon(t, func() error {
		return clientCtx.ConnHealth(remoteAddr)
	})

	// Close the listener and all the connections.
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closeConns(); err != nil {
		t.Fatal(err)
	}

	// Should become unhealthy again now that the connection was closed.
	testutils.SucceedsSoon(t, func() error {
		if err := clientCtx.ConnHealth(remoteAddr); grpc.Code(err) != codes.Unavailable {
			return errors.Errorf("unexpected error: %v", err)
		}
		return nil
	})

	// Should stay unhealthy despite reconnection attempts.
	errUnhealthy := errors.New("connection is still unhealthy")
	if err := util.RetryForDuration(100*clientCtx.cfg.HeartbeatInterval, func() error {
		if err := clientCtx.ConnHealth(remoteAddr); grpc.Code(err) != codes.Unavailable {
			return errors.Errorf("unexpected error: %v", err)
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
	serverClock := hlc.NewClock(serverTime.UnixNano, time.Nanosecond /* maxOffset */)
	s, ln := newTestServer(t, stopper, testutils.NewNodeTestBaseConfig(), util.TestAddr)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock: serverClock,
		remoteClockMonitor: newRemoteClockMonitor(
			serverClock, time.Second /* offsetTTL */, time.Second /* histogramWindowInterval */),
	})
	remoteAddr := ln.Addr().String()

	// Create a client clock that is behind the server clock.
	clientAdvancing := AdvancingClock{time: time.Unix(0, 10)}
	clientClock := hlc.NewClock(clientAdvancing.UnixNano, time.Nanosecond)
	cfg := ContextConfig{
		Config:                testutils.NewNodeTestBaseConfig(),
		HLCClock:              clientClock,
		HeartbeatInterval:     time.Millisecond,
		HeartbeatTimeout:      10 * time.Millisecond,
		EnableClockSkewChecks: true,
		TestingKnobs: ContextTestingKnobs{
			// Don't perform offset checks, as they'll fail and crash the process.
			// This test manually verifies offsets.
			PeriodicClockCheckHook: func() error {
				return nil
			},
		},
	}
	clientCtx := NewContext(log.AmbientContext{}, cfg, stopper)
	clientCtx.RemoteClocks.offsetTTL = 5 * clientAdvancing.getAdvancementInterval()
	if _, err := clientCtx.GRPCDial(remoteAddr); err != nil {
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

// Test that measured offsets are forgotten if not updated in a while.
func TestFailedOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(time.Unix(0, 1).UnixNano, time.Nanosecond)

	s, ln := newTestServer(t, stopper, testutils.NewNodeTestBaseConfig(), util.TestAddr)
	remoteAddr := ln.Addr().String()
	serverClockMonitor := newRemoteClockMonitor(
		clock, time.Second /* offsetTTL */, time.Second /* histogramWindowInterval */)
	heartbeat := &ManualHeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverClockMonitor,
		ready:              make(chan error),
		stopper:            stopper,
	}
	RegisterHeartbeatServer(s, heartbeat)

	// Create a client that never receives a heartbeat response after the first.
	cfg := ContextConfig{
		Config:            testutils.NewNodeTestBaseConfig(),
		HLCClock:          clock,
		HeartbeatInterval: time.Millisecond,
		// Large timeout so that failure arises from exceeding the maximum clock
		// reading delay, not the timeout.
		HeartbeatTimeout:      time.Hour,
		EnableClockSkewChecks: true,
	}
	clientCtx := NewContext(log.AmbientContext{}, cfg, stopper)

	if _, err := clientCtx.GRPCDial(remoteAddr); err != nil {
		t.Fatal(err)
	}
	heartbeat.ready <- nil // Allow one heartbeat for initialization.

	testutils.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()

		if _, ok := clientCtx.RemoteClocks.mu.offsets[remoteAddr]; !ok {
			return errors.Errorf("expected offset of %s to be initialized, but it was not", remoteAddr)
		}
		return nil
	})

	testutils.SucceedsSoon(t, func() error {
		serverClockMonitor.mu.Lock()
		defer serverClockMonitor.mu.Unlock()

		if o, ok := serverClockMonitor.mu.offsets[remoteAddr]; ok {
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
		addr    net.Addr
	}

	start := time.Date(2012, 12, 07, 0, 0, 0, 0, time.UTC)

	nodeCtxs := []nodeContext{
		{offset: 0},
		{offset: 0},
		{offset: 0},
		// The minimum offset that actually triggers node death.
		{offset: maxOffset + 1},
	}

	// For each node, create both a client and a server. They both use the same
	// static clock for reporting time.
	for i := range nodeCtxs {
		clock := hlc.NewClock(start.Add(nodeCtxs[i].offset).UnixNano, maxOffset)
		addr := util.TestAddr
		baseConfig := testutils.NewNodeTestBaseConfig()
		// Create the server, so that baseConfig.Addr is filled in.
		s, ln := newTestServer(t, stopper, baseConfig, addr)
		nodeCtxs[i].addr = ln.Addr()
		cfg := ContextConfig{
			Config:                baseConfig,
			HLCClock:              clock,
			HeartbeatInterval:     maxOffset,
			HeartbeatTimeout:      10 * time.Millisecond,
			EnableClockSkewChecks: true,
			TestingKnobs: ContextTestingKnobs{
				// Don't perform offset checks, as they'll fail and crash the process.
				// This test manually verifies offsets.
				PeriodicClockCheckHook: func() error {
					return nil
				},
			},
		}
		ctx := NewContext(log.AmbientContext{}, cfg, stopper)
		nodeCtxs[i].ctx = ctx
		nodeCtxs[i].errChan = make(chan error, 1)

		RegisterHeartbeatServer(s, &HeartbeatService{
			clock:              clock,
			remoteClockMonitor: nodeCtxs[i].ctx.RemoteClocks,
		})
	}

	// Fully connect the nodes.
	for i, clientNodeContext := range nodeCtxs {
		for j, serverNodeContext := range nodeCtxs {
			if i == j {
				continue
			}
			if _, err := clientNodeContext.ctx.GRPCDial(serverNodeContext.addr.String()); err != nil {
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
				return errors.Errorf("not yet fully connected. node %s has %d of %d connections: %v",
					nodeCtx.addr.String(), a, e, nodeCtx.ctx.RemoteClocks.mu.offsets)
			}
			return nil
		})
	}

	// One node is expected to see all the other nodes as having too much skew.
	// All the other nodes are expected to see that one node as having a large
	// skew, and thus pass verification (we verify that a mojority of nodes are
	// within the offset limit).
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
