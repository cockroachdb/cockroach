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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

func TestClientHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := newNodeTestContext(nil, stopper)

	_, ln := newTestServer(t, nodeContext, false)
	c := NewClient(ln.Addr(), nodeContext)
	<-c.Healthy()
	newC := NewClient(ln.Addr(), nodeContext)
	<-c.Healthy()
	if newC != c {
		t.Fatal("expected cached client to be returned while healthy")
	}
}

func TestClientNoCache(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	rpcContext := newNodeTestContext(nil, stopper)
	rpcContext.DisableCache = true

	_, ln := newTestServer(t, rpcContext, false)
	c1 := NewClient(ln.Addr(), rpcContext)
	<-c1.Healthy()
	c2 := NewClient(ln.Addr(), rpcContext)
	<-c2.Healthy()
	if c1 == c2 {
		t.Errorf("expected different clients with cache disabled: %+v != %+v", c1, c2)
	}
}

// TestClientHeartbeatBadServer verifies that the client is not marked
// as "ready" until a heartbeat request succeeds.
func TestClientHeartbeatBadServer(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	ctx := newNodeTestContext(nil, stopper)
	ctx.localClock = hlc.NewClock(hlc.UnixNano)

	// Create a server without registering a heartbeat service.
	s, ln := newTestServer(t, ctx, true)

	// Create a client. It should attempt a heartbeat and fail.
	c := NewClient(ln.Addr(), ctx)

	// Register a heartbeat service.
	heartbeat := &HeartbeatService{
		clock:              ctx.localClock,
		remoteClockMonitor: newRemoteClockMonitor(ctx.localClock),
	}

	select {
	case <-c.Healthy():
		t.Error("client became healthy before a successful heartbeat")
	case <-time.After(10 * time.Millisecond):
	}

	if err := heartbeat.Register(s); err != nil {
		t.Fatalf("Unable to register heartbeat service: %s", err)
	}

	// A heartbeat should succeed and the client should become ready.
	<-c.Healthy()
}

// TestClientCloseBeforeConnect verifies that the client goroutine
// does not leak if the client is closed before connecting.
func TestClientCloseBeforeConnect(t *testing.T) {
	defer leaktest.AfterTest(t)

	c := NewClient(
		util.MakeUnresolvedAddr("tcp", ":1337"),
		&Context{Stopper: stop.NewStopper()},
	)

	c.Close()
}

func TestOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	serverManual := hlc.NewManualClock(10)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	ctx := newNodeTestContext(serverClock, stopper)
	s, ln := newTestServer(t, ctx, true)

	heartbeat := &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock),
	}
	if err := heartbeat.Register(s); err != nil {
		t.Fatalf("Unable to register heartbeat service: %s", err)
	}

	// Create a client that is 10 nanoseconds behind the server.
	// Use the server context (heartbeat is node-to-node).
	advancing := AdvancingClock{time: 0, advancementInterval: 10}
	clientClock := hlc.NewClock(advancing.UnixNano)
	context := newNodeTestContext(clientClock, stopper)
	c := NewClient(ln.Addr(), context)
	<-c.Healthy()

	expectedOffset := RemoteOffset{Offset: 5, Uncertainty: 5, MeasuredAt: 10}
	if o := c.remoteOffset; !proto.Equal(&o, &expectedOffset) {
		t.Errorf("expected offset %v, actual %v", expectedOffset, o)
	}

	// Ensure the offsets map was updated properly too.
	context.RemoteClocks.mu.Lock()
	if o := context.RemoteClocks.offsets[c.RemoteAddr().String()]; !proto.Equal(&o, &expectedOffset) {
		t.Errorf("expected offset %v, actual %v", expectedOffset, o)
	}
	context.RemoteClocks.mu.Unlock()
}

// TestDelayedOffsetMeasurement tests that the client will record a
// zero offset if the heartbeat reply exceeds the
// maximumClockReadingDelay, but not the heartbeat timeout.
func TestDelayedOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	serverManual := hlc.NewManualClock(10)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	ctx := newNodeTestContext(serverClock, stopper)
	s, ln := newTestServer(t, ctx, true)

	heartbeat := &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock),
	}
	if err := heartbeat.Register(s); err != nil {
		t.Fatalf("Unable to register heartbeat service: %s", err)
	}

	// Create a client that receives a heartbeat right after the
	// maximumClockReadingDelay.
	advancing := AdvancingClock{
		time:                0,
		advancementInterval: maximumClockReadingDelay.Nanoseconds() + 1,
	}
	clientClock := hlc.NewClock(advancing.UnixNano)
	context := newNodeTestContext(clientClock, stopper)
	c := NewClient(ln.Addr(), context)
	<-c.Healthy()

	// Since the reply took too long, we should have a zero offset, even
	// though the client is still healthy because it received a heartbeat
	// reply.
	if o := c.remoteOffset; !proto.Equal(&o, &RemoteOffset{}) {
		t.Errorf("expected offset %v, actual %v", RemoteOffset{}, o)
	}

	// Ensure the general offsets map was updated properly too.
	context.RemoteClocks.mu.Lock()
	if o, ok := context.RemoteClocks.offsets[c.RemoteAddr().String()]; ok {
		t.Errorf("expected offset to not exist, but found %v", o)
	}
	context.RemoteClocks.mu.Unlock()
}

func TestFailedOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	serverManual := hlc.NewManualClock(0)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	ctx := newNodeTestContext(serverClock, stopper)
	s, ln := newTestServer(t, ctx, true)

	heartbeat := &ManualHeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock),
		ready:              make(chan struct{}),
		stopper:            stopper,
	}
	if err := heartbeat.Register(s); err != nil {
		t.Fatalf("Unable to register heartbeat service: %s", err)
	}

	// Create a client that never receives a heartbeat after the first.
	clientManual := hlc.NewManualClock(0)
	clientClock := hlc.NewClock(clientManual.UnixNano)
	context := newNodeTestContext(clientClock, stopper)
	context.HeartbeatTimeout = 20 * context.HeartbeatInterval
	c := NewClient(ln.Addr(), context)
	heartbeat.ready <- struct{}{} // Allow one heartbeat for initialization.
	<-c.Healthy()

	// Synchronously wait on missing the next heartbeat.
	if err := util.IsTrueWithin(func() bool {
		select {
		case <-c.Healthy():
			return false
		default:
			return true
		}
	}, context.HeartbeatTimeout*10); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&c.remoteOffset, &RemoteOffset{}) {
		t.Errorf("expected offset %v, actual %v",
			RemoteOffset{}, c.remoteOffset)
	}
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
