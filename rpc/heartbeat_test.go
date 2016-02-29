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
// Author: Kathy Spradlin (kathyspradlin@gmail.com)

package rpc

import (
	"testing"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

func TestRemoteOffsetString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ro := RemoteOffset{
		Offset:      -501584461,
		Uncertainty: 351698,
		MeasuredAt:  1430348776127420269,
	}
	expStr := "off=-0.501584461s, err=0.000351698s, at=2015-04-29 23:06:16 +0000 UTC"
	if str := ro.String(); str != expStr {
		t.Errorf("expected %s; got %s", expStr, str)
	}
}

func TestHeartbeatReply(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(5)
	clock := hlc.NewClock(manual.UnixNano)
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock),
	}

	request := &PingRequest{
		Ping: "testPing",
	}
	var response *PingResponse
	if responseI, err := heartbeat.Ping(request); err != nil {
		t.Fatal(err)
	} else {
		response = responseI.(*PingResponse)
	}

	if response.Pong != request.Ping {
		t.Errorf("expected %s to be equal to %s", response.Pong, request.Ping)
	}

	if response.ServerTime != 5 {
		t.Errorf("expected server time 5, instead %d", response.ServerTime)
	}
}

func TestManualHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	request := &PingRequest{
		Ping: "testManual",
	}
	manualHeartbeat.ready <- struct{}{}
	var manualResponse *PingResponse
	var regularResponse *PingResponse
	if resp, err := regularHeartbeat.Ping(request); err != nil {
		t.Fatal(err)
	} else {
		regularResponse = resp.(*PingResponse)
	}
	if resp, err := manualHeartbeat.Ping(request); err != nil {
		t.Fatal(err)
	} else {
		manualResponse = resp.(*PingResponse)
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
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	ctx := newNodeTestContext(nil, stopper)
	_, ln := newTestServer(t, ctx, false)
	// Create a client and set its remote offset. On first heartbeat,
	// it will update the server's remote clocks map. We create the
	// client manually here to allow us to set the remote offset
	// before the first heartbeat.
	tlsConfig, err := ctx.GetClientTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	serverAddr := ln.Addr()
	client := &Client{
		Closed:       make(chan struct{}),
		addr:         util.MakeUnresolvedAddr(serverAddr.Network(), serverAddr.String()),
		tlsConfig:    tlsConfig,
		clock:        ctx.localClock,
		remoteClocks: ctx.RemoteClocks,
		remoteOffset: RemoteOffset{
			Offset:      10,
			Uncertainty: 5,
			MeasuredAt:  20,
		},
	}
	if err = client.connect(); err != nil {
		t.Fatal(err)
	}

	ctx.RemoteClocks.mu.Lock()
	remoteAddr := client.RemoteAddr().String()
	o := ctx.RemoteClocks.offsets[remoteAddr]
	ctx.RemoteClocks.mu.Unlock()
	expServerOffset := RemoteOffset{Offset: -10, Uncertainty: 5, MeasuredAt: 20}
	if proto.Equal(&o, &expServerOffset) {
		t.Errorf("expected updated offset %v, instead %v", expServerOffset, o)
	}
	ln.Close()

	// Remove the offset from RemoteClocks and close the connection from the
	// remote end. A new offset for the server should not be added to the clock
	// monitor.
	ctx.RemoteClocks.mu.Lock()
	delete(ctx.RemoteClocks.offsets, remoteAddr)
	ln.Close()
	ctx.RemoteClocks.mu.Unlock()

	ctx.RemoteClocks.mu.Lock()
	if offset, ok := ctx.RemoteClocks.offsets[remoteAddr]; ok {
		t.Errorf("unexpected updated offset: %v", offset)
	}
	ctx.RemoteClocks.mu.Unlock()
}
