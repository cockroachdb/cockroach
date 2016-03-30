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
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestRemoteOffsetString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ro := RemoteOffset{
		Offset:      -501584461,
		Uncertainty: 351698,
		MeasuredAt:  1430348776127420269,
	}
	expStr := "off=-501.584461ms, err=351.698µs, at=2015-04-29 23:06:16.127420269 +0000 UTC"
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
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour),
	}

	request := &PingRequest{
		Ping: "testPing",
	}
	response, err := heartbeat.Ping(context.Background(), request)
	if err != nil {
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
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(5)
	clock := hlc.NewClock(manual.UnixNano)
	manualHeartbeat := &ManualHeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour),
		ready:              make(chan struct{}, 1),
	}
	regularHeartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour),
	}

	request := &PingRequest{
		Ping: "testManual",
	}
	manualHeartbeat.ready <- struct{}{}
	ctx := context.Background()
	regularResponse, err := regularHeartbeat.Ping(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	manualResponse, err := manualHeartbeat.Ping(ctx, request)
	if err != nil {
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
