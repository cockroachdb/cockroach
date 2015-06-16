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
	"net/rpc"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

func init() {
	// Setting the heartbeat interval in individual tests
	// triggers the race detector, so this is better for now.
	heartbeatInterval = 10 * time.Millisecond
}

func TestClientHeartbeat(t *testing.T) {
	addr := util.CreateTestAddr("tcp")

	s := NewServer(addr, serverTestBaseContext)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	c := NewClient(s.Addr(), nil, clientTestBaseContext)
	<-c.Ready
	if c != NewClient(s.Addr(), nil, clientTestBaseContext) {
		t.Fatal("expected cached client to be returned while healthy")
	}
	<-c.Ready
	s.Close()
}

func TestClientNoCache(t *testing.T) {
	rpcContext := serverTestBaseContext
	rpcContext.DisableCache = true
	addr := util.CreateTestAddr("tcp")

	s := NewServer(addr, rpcContext)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	c1 := NewClient(s.Addr(), nil, rpcContext)
	<-c1.Ready
	c2 := NewClient(s.Addr(), nil, rpcContext)
	<-c2.Ready
	if c1 == c2 {
		t.Errorf("expected different clients with cache disabled: %+v != %+v", c1, c2)
	}
	s.Close()
}

// TestClientHeartbeatBadServer verifies that the client is not marked
// as "ready" until a heartbeat request succeeds.
func TestClientHeartbeatBadServer(t *testing.T) {
	// Create a server without registering a heartbeat service.
	serverClock := hlc.NewClock(hlc.UnixNano)
	s := createTestServer(serverClock, t)
	defer s.Close()

	// Create a client. It should attempt a heartbeat and fail.
	c := NewClient(s.Addr(), nil, s.context)

	// Register a heartbeat service.
	heartbeat := &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock),
	}

	// The client should fail the heartbeat and be recycled.
	<-c.Closed

	if err := s.RegisterName("Heartbeat", heartbeat); err != nil {
		t.Fatalf("Unable to register heartbeat service: %s", err)
	}
	// Same thing, but now the heartbeat service exists.
	c = NewClient(s.Addr(), nil, s.context)

	// A heartbeat should succeed and the client should become ready.
	<-c.Ready
	s.Close()
}

func TestOffsetMeasurement(t *testing.T) {
	serverManual := hlc.NewManualClock(10)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	s := createTestServer(serverClock, t)
	defer s.Close()

	heartbeat := &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock),
	}
	if err := s.RegisterName("Heartbeat", heartbeat); err != nil {
		t.Fatalf("Unable to register heartbeat service: %s", err)
	}

	// Create a client that is 10 nanoseconds behind the server.
	advancing := AdvancingClock{time: 0, advancementInterval: 10}
	clientClock := hlc.NewClock(advancing.UnixNano)
	context := NewTestContext(clientClock)
	c := NewClient(s.Addr(), nil, context)
	<-c.Ready

	// Ensure we get a good heartbeat before continuing.
	if err := util.IsTrueWithin(c.IsHealthy, heartbeatInterval*10); err != nil {
		t.Fatal(err)
	}

	expectedOffset := proto.RemoteOffset{Offset: 5, Uncertainty: 5, MeasuredAt: 10}
	if o := c.RemoteOffset(); !o.Equal(expectedOffset) {
		t.Errorf("expected offset %v, actual %v", expectedOffset, o)
	}

	// Ensure the offsets map was updated properly too.
	context.RemoteClocks.mu.Lock()
	if o := context.RemoteClocks.offsets[c.addr.String()]; !o.Equal(expectedOffset) {
		t.Errorf("expected offset %v, actual %v", expectedOffset, o)
	}
	context.RemoteClocks.mu.Unlock()
}

// TestDelayedOffsetMeasurement tests that the client will record a
// zero offset if the heartbeat reply exceeds the
// maximumClockReadingDelay, but not the heartbeat timeout.
func TestDelayedOffsetMeasurement(t *testing.T) {
	serverManual := hlc.NewManualClock(10)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	s := createTestServer(serverClock, t)
	defer s.Close()

	heartbeat := &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock),
	}
	if err := s.RegisterName("Heartbeat", heartbeat); err != nil {
		t.Fatalf("Unable to register heartbeat service: %s", err)
	}

	// Create a client that receives a heartbeat right after the
	// maximumClockReadingDelay.
	advancing := AdvancingClock{
		time:                0,
		advancementInterval: maximumClockReadingDelay.Nanoseconds() + 1,
	}
	clientClock := hlc.NewClock(advancing.UnixNano)
	context := NewTestContext(clientClock)
	c := NewClient(s.Addr(), nil, context)
	<-c.Ready

	// Ensure we get a good heartbeat before continuing.
	if err := util.IsTrueWithin(c.IsHealthy, heartbeatInterval*10); err != nil {
		t.Fatal(err)
	}

	// Since the reply took too long, we should have a zero offset, even
	// though the client is still healthy because it received a heartbeat
	// reply.
	if o := c.RemoteOffset(); !o.Equal(proto.RemoteOffset{}) {
		t.Errorf("expected offset %v, actual %v", proto.RemoteOffset{}, o)
	}

	// Ensure the general offsets map was updated properly too.
	context.RemoteClocks.mu.Lock()
	if o, ok := context.RemoteClocks.offsets[c.addr.String()]; ok {
		t.Errorf("expected offset to not exist, but found %v", o)
	}
	context.RemoteClocks.mu.Unlock()
}

func TestFailedOffestMeasurement(t *testing.T) {
	serverManual := hlc.NewManualClock(0)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	s := createTestServer(serverClock, t)
	defer s.Close()

	heartbeat := &ManualHeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock),
		ready:              make(chan struct{}),
	}
	if err := s.RegisterName("Heartbeat", heartbeat); err != nil {
		t.Fatalf("Unable to register heartbeat service: %s", err)
	}

	// Create a client that never receives a heartbeat after the first.
	clientManual := hlc.NewManualClock(0)
	clientClock := hlc.NewClock(clientManual.UnixNano)
	context := NewTestContext(clientClock)
	c := NewClient(s.Addr(), nil, context)
	heartbeat.ready <- struct{}{} // Allow one heartbeat for initialization.
	<-c.Ready
	// Synchronously wait on missing the next heartbeat.
	err := util.IsTrueWithin(func() bool {
		return !c.IsHealthy()
	}, heartbeatInterval*10)
	if err != nil {
		t.Fatal(err)
	}
	if !c.RemoteOffset().Equal(proto.RemoteOffset{}) {
		t.Errorf("expected offset %v, actual %v",
			proto.RemoteOffset{}, c.RemoteOffset())
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
func createTestServer(serverClock *hlc.Clock, t *testing.T) *Server {
	// Create a test context, but override the clock.
	serverContext := NewServerTestContext(serverClock)

	// Create the server so that we can register a manual clock.
	addr := util.CreateTestAddr("tcp")
	s := &Server{
		Server:  rpc.NewServer(),
		context: serverContext,
		addr:    addr,
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	return s
}
