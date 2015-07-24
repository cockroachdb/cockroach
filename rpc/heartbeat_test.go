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
// Author: Kathy Spradlin (kathyspradlin@gmail.com)

package rpc

import (
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func TestHeartbeatReply(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(5)
	clock := hlc.NewClock(manual.UnixNano)
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock),
	}

	request := &proto.PingRequest{
		Ping: "testPing",
	}
	var response *proto.PingResponse
	if responseI, err := heartbeat.Ping(request); err != nil {
		t.Fatal(err)
	} else {
		response = responseI.(*proto.PingResponse)
	}

	if response.Pong != request.Ping {
		t.Errorf("expected %s to be equal to %s", response.Pong, request.Ping)
	}

	if response.ServerTime != 5 {
		t.Errorf("expected server time 5, instead %d", response.ServerTime)
	}
}

func TestManualHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(5)
	clock := hlc.NewClock(manual.UnixNano)
	manualHeartbeat := &ManualHeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock),
		ready:              make(chan struct{}, 1),
	}
	regularHeartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock),
	}

	request := &proto.PingRequest{
		Ping: "testManual",
	}
	manualHeartbeat.ready <- struct{}{}
	var manualResponse *proto.PingResponse
	var regularResponse *proto.PingResponse
	if resp, err := regularHeartbeat.Ping(request); err != nil {
		t.Fatal(err)
	} else {
		regularResponse = resp.(*proto.PingResponse)
	}
	if resp, err := manualHeartbeat.Ping(request); err != nil {
		t.Fatal(err)
	} else {
		manualResponse = resp.(*proto.PingResponse)
	}

	// Ensure that the response is the same as with a normal heartbeat.
	if manualResponse.Pong != regularResponse.Pong {
		t.Errorf("expected pong %s, instead %s",
			manualResponse.Pong, regularResponse.Pong)
	}
	if manualResponse.ServerTime != regularResponse.ServerTime {
		t.Errorf("expected ServerTime %d, instead %d",
			manualResponse.ServerTime, regularResponse.ServerTime)
	}
}

func TestUpdateOffsetOnHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := NewNodeTestContext(nil, stopper)
	serverAddr := util.CreateTestAddr("tcp")
	// Start heartbeat.
	s := NewServer(serverAddr, nodeContext)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	// Create a client and set its remote offset. On first heartbeat,
	// it will update the server's remote clocks map. We create the
	// client manually here to allow us to set the remote offset
	// before the first heartbeat.
	tlsConfig, err := nodeContext.GetClientTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{
		Closed:       make(chan struct{}),
		addr:         util.MakeUnresolvedAddr(s.Addr().Network(), s.Addr().String()),
		tlsConfig:    tlsConfig,
		clock:        nodeContext.localClock,
		remoteClocks: nodeContext.RemoteClocks,
		remoteOffset: proto.RemoteOffset{
			Offset:      10,
			Uncertainty: 5,
			MeasuredAt:  20,
		},
	}
	if err := client.connect(); err != nil {
		t.Fatal(err)
	}

	nodeContext.RemoteClocks.mu.Lock()
	remoteAddr := client.RemoteAddr().String()
	o := nodeContext.RemoteClocks.offsets[remoteAddr]
	nodeContext.RemoteClocks.mu.Unlock()
	expServerOffset := proto.RemoteOffset{Offset: -10, Uncertainty: 5, MeasuredAt: 20}
	if o.Equal(expServerOffset) {
		t.Errorf("expected updated offset %v, instead %v", expServerOffset, o)
	}
	s.Close()

	// Remove the offset from RemoteClocks and close the connection from the
	// remote end. A new offset for the server should not be added to the clock
	// monitor.
	nodeContext.RemoteClocks.mu.Lock()
	delete(nodeContext.RemoteClocks.offsets, remoteAddr)
	s.Close()
	nodeContext.RemoteClocks.mu.Unlock()

	nodeContext.RemoteClocks.mu.Lock()
	if offset, ok := nodeContext.RemoteClocks.offsets[remoteAddr]; ok {
		t.Errorf("unexpected updated offset: %v", offset)
	}
	nodeContext.RemoteClocks.mu.Unlock()
}
