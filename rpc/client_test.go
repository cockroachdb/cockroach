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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package rpc

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func init() {
	// Setting the heartbeat interval in individual tests
	// triggers the race detector, so this is better for now.
	heartbeatInterval = 10 * time.Millisecond
}

func TestClientHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	addr := util.CreateTestAddr("tcp")

	nodeContext := NewNodeTestContext(nil, stopper)

	// Heartbeats are node-to-node requests: use server context for both server and client.
	s := NewServer(addr, nodeContext)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	c := NewClient(s.Addr(), nodeContext)
	<-c.Healthy()
	newC := NewClient(s.Addr(), nodeContext)
	<-c.Healthy()
	if newC != c {
		t.Fatal("expected cached client to be returned while healthy")
	}
}

func TestClientNoCache(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	rpcContext := NewNodeTestContext(nil, stopper)

	rpcContext.DisableCache = true
	addr := util.CreateTestAddr("tcp")

	s := NewServer(addr, rpcContext)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	c1 := NewClient(s.Addr(), rpcContext)
	<-c1.Healthy()
	c2 := NewClient(s.Addr(), rpcContext)
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

	// Create a server without registering a heartbeat service.
	serverClock := hlc.NewClock(hlc.UnixNano)
	s := createTestServer(serverClock, stopper, t)

	// Create a client. It should attempt a heartbeat and fail.
	c := NewClient(s.Addr(), s.context)

	// Register a heartbeat service.
	heartbeat := &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock),
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

func TestOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	serverManual := hlc.NewManualClock(10)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	s := createTestServer(serverClock, stopper, t)

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
	context := NewNodeTestContext(clientClock, stopper)
	c := NewClient(s.Addr(), context)
	<-c.Healthy()

	expectedOffset := proto.RemoteOffset{Offset: 5, Uncertainty: 5, MeasuredAt: 10}
	if o := c.remoteOffset; !o.Equal(expectedOffset) {
		t.Errorf("expected offset %v, actual %v", expectedOffset, o)
	}

	// Ensure the offsets map was updated properly too.
	context.RemoteClocks.mu.Lock()
	if o := context.RemoteClocks.offsets[c.RemoteAddr().String()]; !o.Equal(expectedOffset) {
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
	s := createTestServer(serverClock, stopper, t)

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
	context := NewNodeTestContext(clientClock, stopper)
	c := NewClient(s.Addr(), context)
	<-c.Healthy()

	// Since the reply took too long, we should have a zero offset, even
	// though the client is still healthy because it received a heartbeat
	// reply.
	if o := c.remoteOffset; !o.Equal(proto.RemoteOffset{}) {
		t.Errorf("expected offset %v, actual %v", proto.RemoteOffset{}, o)
	}

	// Ensure the general offsets map was updated properly too.
	context.RemoteClocks.mu.Lock()
	if o, ok := context.RemoteClocks.offsets[c.RemoteAddr().String()]; ok {
		t.Errorf("expected offset to not exist, but found %v", o)
	}
	context.RemoteClocks.mu.Unlock()
}

func TestFailedOffestMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	serverManual := hlc.NewManualClock(0)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	s := createTestServer(serverClock, stopper, t)

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
	context := NewNodeTestContext(clientClock, stopper)
	c := NewClient(s.Addr(), context)
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
	}, heartbeatInterval*10); err != nil {
		t.Fatal(err)
	}
	if !c.remoteOffset.Equal(proto.RemoteOffset{}) {
		t.Errorf("expected offset %v, actual %v",
			proto.RemoteOffset{}, c.remoteOffset)
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

// createTestServer creates and starts a new server with a test tlsConfig and
// addr. Be sure to close the server when done. Building the server manually
// like this allows for manual registration of the heartbeat service.
func createTestServer(serverClock *hlc.Clock, stopper *stop.Stopper, t *testing.T) *Server {
	// Create a test context, but override the clock.
	nodeContext := NewNodeTestContext(serverClock, stopper)

	// Create the server so that we can register a manual clock.
	addr := util.CreateTestAddr("tcp")
	s := &Server{
		context: nodeContext,
		addr:    addr,
		methods: map[string]method{},
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	return s
}
