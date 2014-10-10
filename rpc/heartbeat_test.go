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
	"net/rpc"
	"testing"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

func TestHeartbeatReply(t *testing.T) {
	manual := hlc.ManualClock(5)
	clock := hlc.NewClock(manual.UnixNano)
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock.MaxDrift()),
	}

	request := &PingRequest{
		Ping: "testPing",
	}
	response := &PingResponse{}
	heartbeat.Ping(request, response)

	if response.Pong != request.Ping {
		t.Errorf("expected %s to be equal to %s", response.Pong, request.Ping)
	}

	if response.ServerTime != 5 {
		t.Errorf("expected server time 5, instead %d", response.ServerTime)
	}
}

func TestManualHeartbeat(t *testing.T) {
	manual := hlc.ManualClock(5)
	clock := hlc.NewClock(manual.UnixNano)
	manualHeartbeat := &ManualHeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock.MaxDrift()),
		ready:              make(chan bool, 1),
	}
	regularHeartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock.MaxDrift()),
	}

	request := &PingRequest{
		Ping: "testManual",
	}
	manualHeartbeat.ready <- true
	manualResponse := &PingResponse{}
	regularResponse := &PingResponse{}
	regularHeartbeat.Ping(request, regularResponse)
	manualHeartbeat.Ping(request, manualResponse)

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

func TestUpdateOffset(t *testing.T) {
	tlsConfig, err := LoadTestTLSConfig("..")
	if err != nil {
		t.Fatal(err)
	}
	serverAddr := util.CreateTestAddr("tcp")
	// Start heartbeat.
	sContext := NewContext(hlc.NewClock(hlc.UnixNano), tlsConfig)
	s := NewServer(serverAddr, sContext)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	conn, err := tlsDial(s.Addr().Network(), s.Addr().String(), tlsConfig)
	if err != nil {
		t.Fatal(err)
	}
	client := rpc.NewClient(conn)

	clientOffset := RemoteOffset{Offset: 10, Error: 5, MeasuredAt: 20}
	serverOffset := RemoteOffset{Offset: -10, Error: 5, MeasuredAt: 20}
	heartbeatRequest := &PingRequest{
		Offset: clientOffset,
		Addr:   conn.LocalAddr().String(),
	}
	response := &PingResponse{}
	call := client.Go("Heartbeat.Ping", heartbeatRequest, response, nil)
	<-call.Done
	if call.Error != nil {
		t.Fatal(call.Error)
	}
	o := sContext.remoteClocks.offsets[conn.LocalAddr().String()]
	if o != serverOffset {
		t.Errorf("expected updated offset %v, instead %v", serverOffset, o)
	}
	s.Close()
}
