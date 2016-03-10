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
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func newTestServer(t *testing.T, ctx *Context, manual bool) (*grpc.Server, net.Listener) {
	tlsConfig, err := ctx.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))

	ln, err := util.ListenAndServeGRPC(ctx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}

	return s, ln
}

func TestOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	serverManual := hlc.NewManualClock(10)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	ctx := newNodeTestContext(serverClock, stopper)
	s, ln := newTestServer(t, ctx, true)
	remoteAddr := ln.Addr().String()

	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: ctx.RemoteClocks,
	})

	// Create a client that is 10 nanoseconds behind the server.
	// Use the server context (heartbeat is node-to-node).
	advancing := AdvancingClock{time: 0, advancementInterval: 10}
	clientClock := hlc.NewClock(advancing.UnixNano)
	context := newNodeTestContext(clientClock, stopper)
	_, err := context.GRPCDial(remoteAddr)
	if err != nil {
		t.Fatal(err)
	}

	expectedOffset := RemoteOffset{Offset: 5, Uncertainty: 5, MeasuredAt: 10}
	util.SucceedsSoon(t, func() error {
		context.RemoteClocks.mu.Lock()
		defer context.RemoteClocks.mu.Unlock()

		if o := context.RemoteClocks.mu.offsets[remoteAddr]; o != expectedOffset {
			return util.Errorf("expected:\n%v\nactual:\n%v", expectedOffset, o)
		}
		return nil
	})
}

// TestDelayedOffsetMeasurement tests that the client will record a
// zero offset if the heartbeat reply exceeds the
// maximumClockReadingDelay, but not the heartbeat timeout.
func TestDelayedOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	serverManual := hlc.NewManualClock(10)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	ctx := newNodeTestContext(serverClock, stopper)
	s, ln := newTestServer(t, ctx, true)
	remoteAddr := ln.Addr().String()

	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: ctx.RemoteClocks,
	})

	// Create a client that receives a heartbeat right after the
	// maximumClockReadingDelay.
	advancing := AdvancingClock{
		time:                0,
		advancementInterval: maximumClockReadingDelay.Nanoseconds() + 1,
	}
	clientClock := hlc.NewClock(advancing.UnixNano)
	context := newNodeTestContext(clientClock, stopper)
	_, err := context.GRPCDial(remoteAddr)
	if err != nil {
		t.Fatal(err)
	}

	// Since the reply took too long, we should have a zero offset, even
	// though the client is still healthy because it received a heartbeat
	// reply.
	context.RemoteClocks.mu.Lock()
	if o, ok := context.RemoteClocks.mu.offsets[remoteAddr]; ok {
		t.Errorf("expected offset to not exist, but found %v", o)
	}
	context.RemoteClocks.mu.Unlock()
}

func TestFailedOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(hlc.NewManualClock(1).UnixNano)

	serverCtx := newNodeTestContext(clock, stopper)
	serverCtx.RemoteClocks.monitorInterval = 100 * time.Millisecond
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
	_, err := clientCtx.GRPCDial(remoteAddr)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat.ready <- struct{}{} // Allow one heartbeat for initialization.

	util.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()

		if _, ok := clientCtx.RemoteClocks.mu.offsets[remoteAddr]; !ok {
			return util.Errorf("expected offset of %s to be initialized, but it was not", remoteAddr)
		}
		return nil
	})

	util.SucceedsSoon(t, func() error {
		serverCtx.RemoteClocks.mu.Lock()
		defer serverCtx.RemoteClocks.mu.Unlock()

		if o, ok := serverCtx.RemoteClocks.mu.offsets[remoteAddr]; ok {
			return util.Errorf("expected offset of %s to not be initialized, but it was: %v", remoteAddr, o)
		}
		return nil
	})
}

type AdvancingClock struct {
	time                int64
	advancementInterval int64
}

func (ac *AdvancingClock) UnixNano() int64 {
	time := ac.time
	ac.time = time + ac.advancementInterval
	return time
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
		{offset: maxOffset * 3},
	}

	for i := range nodeCtxs {
		clock := offsetClock(nodeCtxs[i].offset)
		nodeCtxs[i].errChan = make(chan error, 1)

		clock.SetMaxOffset(maxOffset)
		nodeCtxs[i].ctx = newNodeTestContext(clock, stopper)
		nodeCtxs[i].ctx.RemoteClocks.monitorInterval = maxOffset / 2
		// Apparently heartbeats must happen more frequently than monitor events.
		// Good thing this is documented.
		nodeCtxs[i].ctx.HeartbeatInterval = nodeCtxs[i].ctx.RemoteClocks.monitorInterval / 2

		s, ln := newTestServer(t, nodeCtxs[i].ctx, true)
		RegisterHeartbeatServer(s, &HeartbeatService{
			clock:              clock,
			remoteClockMonitor: nodeCtxs[i].ctx.RemoteClocks,
		})
		nodeCtxs[i].ctx.localAddr = ln.Addr().String()
	}

	// Fully connect the nodes.
	for i, clientNodeContext := range nodeCtxs {
		for j, serverNodeContext := range nodeCtxs {
			if i == j {
				continue
			}
			if _, err := clientNodeContext.ctx.GRPCDial(serverNodeContext.ctx.localAddr); err != nil {
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
				return util.Errorf("not yet fully connected: have %d of %d connections: %v", a, e, nodeCtx.ctx.RemoteClocks.mu.offsets)
			}
			return nil
		})
	}

	// Now that all the nodes are connected, start the clock monitors.
	for _, nodeCtx := range nodeCtxs {
		// Asynchronously closing over a range variable is unsafe.
		ctx := nodeCtx.ctx
		errChan := nodeCtx.errChan
		stopper.RunWorker(func() {
			errChan <- ctx.RemoteClocks.MonitorRemoteOffsets(stopper)
		})
	}

	const errOffsetGreaterThanMaxOffset = "the true offset is greater than the max offset"
	for i, nodeCtx := range nodeCtxs {
		if nodeOffset := nodeCtx.offset; nodeOffset > maxOffset {
			select {
			case err := <-nodeCtx.errChan:
				if testutils.IsError(err, errOffsetGreaterThanMaxOffset) {
					t.Logf("max offset: %s - node %d with excessive clock offset of %s returned expected error: %s", maxOffset, i, nodeOffset, err)
				} else {
					t.Errorf("max offset: %s - node %d with excessive clock offset of %s returned unexpected error: %s", maxOffset, i, nodeOffset, err)
				}
			case <-time.After(nodeCtx.ctx.RemoteClocks.monitorInterval * 5):
				t.Errorf("max offset: %s - node %d with excessive clock offset of %s should have return an error, but did not", maxOffset, i, nodeOffset)
			}
		} else {
			select {
			case err := <-nodeCtx.errChan:
				t.Errorf("max offset: %s - node %d with acceptable clock offset of %s returned unexpected error: %s", maxOffset, i, nodeOffset, err)
			case <-time.After(nodeCtx.ctx.RemoteClocks.monitorInterval * 5):
				t.Logf("max offset: %s - node %d with acceptable clock offset of %s did not return an error, as expected", maxOffset, i, nodeOffset)
			}
		}
	}
}
