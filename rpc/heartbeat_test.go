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
)

func TestHeartbeatReply(t *testing.T) {
	manual := hlc.NewManualClock(5)
	clock := hlc.NewClock(manual.UnixNano)
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock),
	}

	request := &proto.PingRequest{
		Ping: "testPing",
	}
	response := &proto.PingResponse{}
	if err := heartbeat.Ping(request, response); err != nil {
		t.Fatal(err)
	}

	if response.Pong != request.Ping {
		t.Errorf("expected %s to be equal to %s", response.Pong, request.Ping)
	}

	if response.ServerTime != 5 {
		t.Errorf("expected server time 5, instead %d", response.ServerTime)
	}
}

func TestManualHeartbeat(t *testing.T) {
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
	manualResponse := &proto.PingResponse{}
	regularResponse := &proto.PingResponse{}
	if err := regularHeartbeat.Ping(request, regularResponse); err != nil {
		t.Fatal(err)
	}
	if err := manualHeartbeat.Ping(request, manualResponse); err != nil {
		t.Fatal(err)
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
	sContext := NewTestContext(t)
	serverAddr := util.CreateTestAddr("tcp")
	// Start heartbeat.
	s := NewServer(serverAddr, sContext)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	// Create a client and set its remote offset. On first heartbeat,
	// it will update the server's remote clocks map. We create the
	// client manually here to allow us to set the remote offset
	// before the first heartbeat.
	client := &Client{
		addr:         s.Addr(),
		Ready:        make(chan struct{}),
		Closed:       make(chan struct{}),
		clock:        sContext.localClock,
		remoteClocks: sContext.RemoteClocks,
		offset: proto.RemoteOffset{
			Offset:     10,
			Error:      5,
			MeasuredAt: 20,
		},
	}
	go client.connect(nil, sContext)
	<-client.Ready

	sContext.RemoteClocks.mu.Lock()
	o := sContext.RemoteClocks.offsets[client.LocalAddr().String()]
	sContext.RemoteClocks.mu.Unlock()
	expServerOffset := proto.RemoteOffset{Offset: -10, Error: 5, MeasuredAt: 20}
	if !o.Equal(expServerOffset) {
		t.Errorf("expected updated offset %v, instead %v", expServerOffset, o)
	}
	s.Close()
}
