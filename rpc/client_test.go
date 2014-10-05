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
	"net"
	"net/rpc"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

func init() {
	// Setting the heartbeat interval in individual tests
	// triggers the race detector, so this is better for now.
	heartbeatInterval = 10 * time.Millisecond
}

func TestClientHeartbeat(t *testing.T) {
	tlsConfig, err := LoadTestTLSConfig("..")
	if err != nil {
		t.Fatal(err)
	}

	clock := hlc.NewClock(hlc.UnixNano)
	rpcContext := NewContext(clock, tlsConfig)
	addr := util.CreateTestAddr("tcp")
	s := NewServer(addr, rpcContext)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	c := NewClient(s.Addr(), nil, rpcContext)
	<-c.Ready
	if c != NewClient(s.Addr(), nil, rpcContext) {
		t.Fatal("expected cached client to be returned while healthy")
	}
	<-c.Ready
	s.Close()
}

// TestClientHeartbeatBadServer verifies that the client is not marked
// as "ready" until a heartbeat request succeeds.
func TestClientHeartbeatBadServer(t *testing.T) {
	tlsConfig, err := LoadTestTLSConfig("..")
	if err != nil {
		t.Fatal(err)
	}

	clock := hlc.NewClock(hlc.UnixNano)
	rpcContext := NewContext(clock, tlsConfig)
	addr := util.CreateTestAddr("tcp")
	// Create a server which doesn't support heartbeats.
	s := &Server{
		Server:         rpc.NewServer(),
		context:        rpcContext,
		addr:           addr,
		closeCallbacks: make([]func(conn net.Conn), 0, 1),
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}

	// Now, create a client. It should attempt a heartbeat and fail,
	// causing retry loop to activate.
	c := NewClient(s.Addr(), nil, rpcContext)
	select {
	case <-c.Ready:
		t.Error("unexpected client heartbeat success")
	case <-c.Closed:
	}
	s.Close()
}

func TestOffsetMeasurement(t *testing.T) {
	tlsConfig, err := LoadTestTLSConfig("..")
	if err != nil {
		t.Fatal(err)
	}

	// Create the server so that we can register a manual clock.
	addr := util.CreateTestAddr("tcp")
	serverManual := hlc.ManualClock(10)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	serverContext := NewContext(serverClock, tlsConfig)
	s := &Server{
		Server:  rpc.NewServer(),
		context: serverContext,
		addr:    addr,
	}
	heartbeat := &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock.MaxDrift()),
	}
	s.RegisterName("Heartbeat", heartbeat)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}

	// Create a client that is 10 nanoseconds behind the server.
	advancing := AdvancingClock{time: 0, advancementInterval: 10}
	clientClock := hlc.NewClock(advancing.UnixNano)
	context := NewContext(clientClock, tlsConfig)
	c := NewClient(s.Addr(), nil, context)
	<-c.Ready
	expectedOffset := RemoteOffset{Offset: 5, Error: 5, MeasuredAt: 10}
	if o := c.RemoteOffset(); o != expectedOffset {
		t.Errorf("expected offset %v, actual %v", expectedOffset, o)
	}
	// Ensure the offsets map was updated properly too.
	if o := context.remoteClocks.offsets[c.addr.String()]; o != expectedOffset {
		t.Errorf("espected offset %v, actual %v", expectedOffset, o)
	}
	s.Close()
}

func TestFailedOffestMeasurement(t *testing.T) {
	tlsConfig, err := LoadTestTLSConfig("..")
	if err != nil {
		t.Fatal(err)
	}

	// Create the server so that we can register the heartbeat manually.
	addr := util.CreateTestAddr("tcp")
	serverManual := hlc.ManualClock(10)
	serverClock := hlc.NewClock(serverManual.UnixNano)
	serverContext := NewContext(serverClock, tlsConfig)
	s := &Server{
		Server:  rpc.NewServer(),
		context: serverContext,
		addr:    addr,
	}
	heartbeat := &ManualHeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: newRemoteClockMonitor(serverClock.MaxDrift()),
		ready:              make(chan bool),
	}
	s.RegisterName("Heartbeat", heartbeat)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}

	// Create a client that never receives a heartbeat after the first.
	clientManual := hlc.ManualClock(0)
	clientClock := hlc.NewClock(clientManual.UnixNano)
	context := NewContext(clientClock, tlsConfig)
	c := NewClient(s.Addr(), nil, context)
	heartbeat.ready <- true // Allow one heartbeat for initialization.
	<-c.Ready
	// Synchronously wait on missing the next heartbeat.
	err = util.IsTrueWithin(func() bool {
		return !c.IsHealthy()
	}, heartbeatInterval*10)
	if err != nil {
		t.Fatal(err)
	}
	if c.RemoteOffset() != InfiniteOffset {
		t.Errorf("expected offset %v, actual %v",
			InfiniteOffset, c.RemoteOffset())
	}
	// Ensure the general offsets map was updated properly too.
	if o := context.remoteClocks.offsets[c.addr.String()]; o != InfiniteOffset {
		t.Errorf("espected offset %v, actual %v", InfiniteOffset, o)
	}
	s.Close()
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
