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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

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

	heartbeat := &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock),
	}
	RegisterHeartbeatServer(s, heartbeat)

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

		if o := context.RemoteClocks.offsets[remoteAddr]; o != expectedOffset {
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

	heartbeat := &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock),
	}
	RegisterHeartbeatServer(s, heartbeat)

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
	if o, ok := context.RemoteClocks.offsets[remoteAddr]; ok {
		t.Errorf("expected offset to not exist, but found %v", o)
	}
	context.RemoteClocks.mu.Unlock()
}

func TestFailedOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	serverManual := hlc.NewManualClock(0)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	ctx := newNodeTestContext(serverClock, stopper)
	s, ln := newTestServer(t, ctx, true)
	remoteAddr := ln.Addr().String()

	heartbeat := &ManualHeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock),
		ready:              make(chan struct{}),
		stopper:            stopper,
	}
	RegisterHeartbeatServer(s, heartbeat)

	// Create a client that never receives a heartbeat after the first.
	clientManual := hlc.NewManualClock(0)
	clientClock := hlc.NewClock(clientManual.UnixNano)
	context := newNodeTestContext(clientClock, stopper)
	// Increase the timeout so that failure arises from exceeding the maximum
	// clock reading delay, not the timeout.
	context.HeartbeatTimeout = 20 * context.HeartbeatInterval
	_, err := context.GRPCDial(remoteAddr)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat.ready <- struct{}{} // Allow one heartbeat for initialization.

	util.SucceedsSoon(t, func() error {
		context.RemoteClocks.mu.Lock()
		defer context.RemoteClocks.mu.Unlock()

		if _, ok := context.RemoteClocks.offsets[remoteAddr]; !ok {
			return util.Errorf("expected offset of %s to be initialized, but it was not", remoteAddr)
		}
		return nil
	})

	expectedOffset := RemoteOffset{}
	util.SucceedsSoon(t, func() error {
		context.RemoteClocks.mu.Lock()
		defer context.RemoteClocks.mu.Unlock()

		if o := context.RemoteClocks.offsets[remoteAddr]; o != expectedOffset {
			return util.Errorf("expected offset of %s to be empty, got %v", remoteAddr, o)
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
