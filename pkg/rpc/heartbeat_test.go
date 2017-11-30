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

package rpc

import (
	"fmt"
	"regexp"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
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

// A ManualHeartbeatService allows manual control of when heartbeats occur.
type ManualHeartbeatService struct {
	clock              *hlc.Clock
	remoteClockMonitor *RemoteClockMonitor
	// Heartbeats are processed when a value is sent here.
	ready   chan error
	stopper *stop.Stopper
}

// Ping waits until the heartbeat service is ready to respond to a Heartbeat.
func (mhs *ManualHeartbeatService) Ping(
	ctx context.Context, args *PingRequest,
) (*PingResponse, error) {
	select {
	case err := <-mhs.ready:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-mhs.stopper.ShouldStop():
	}
	hs := HeartbeatService{
		clock:              mhs.clock,
		remoteClockMonitor: mhs.remoteClockMonitor,
		clusterID:          &base.ClusterIDContainer{},
	}
	return hs.Ping(ctx, args)
}

func TestManualHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(5)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	manualHeartbeat := &ManualHeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		ready:              make(chan error, 1),
	}
	regularHeartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
	}

	request := &PingRequest{
		Ping: "testManual",
	}
	manualHeartbeat.ready <- nil
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

func TestClockOffsetMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			if match, _ := regexp.MatchString("locally configured maximum clock offset", r.(string)); !match {
				t.Errorf("expected clock mismatch error")
			}
		}
	}()

	clock := hlc.NewClock(hlc.UnixNano, 250*time.Millisecond)
	hs := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
	}

	request := &PingRequest{
		Ping:           "testManual",
		Addr:           "test",
		MaxOffsetNanos: (500 * time.Millisecond).Nanoseconds(),
	}
	response, err := hs.Ping(context.Background(), request)
	t.Fatalf("should not have reached but got response=%v err=%v", response, err)
}

func TestClusterIDCompare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	uuid1, uuid2 := uuid.MakeV4(), uuid.MakeV4()
	testData := []struct {
		name            string
		serverClusterID uuid.UUID
		clientClusterID uuid.UUID
		expectError     bool
	}{
		{"cluster IDs match", uuid1, uuid1, false},
		{"their cluster ID missing", uuid1, uuid.Nil, false},
		{"our cluster ID missing", uuid.Nil, uuid1, false},
		{"both cluster IDs missing", uuid.Nil, uuid.Nil, false},
		{"cluster ID mismatch", uuid1, uuid2, true},
	}

	manual := hlc.NewManualClock(5)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
	}

	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			heartbeat.clusterID.Reset(td.serverClusterID)
			request := &PingRequest{
				Ping:      "testPing",
				ClusterID: td.clientClusterID,
			}
			_, err := heartbeat.Ping(context.Background(), request)
			if td.expectError && err == nil {
				t.Error("expected cluster ID mismatch error")
			}
			if !td.expectError && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
		})
	}
}
