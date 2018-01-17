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
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	version := &cluster.MakeTestingClusterSettings().Version
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		version:            version,
	}

	request := &PingRequest{
		Ping:          "testPing",
		ServerVersion: version.ServerVersion,
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
	version            *cluster.ExposedClusterVersion
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
		version:            mhs.version,
	}
	return hs.Ping(ctx, args)
}

func TestManualHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(5)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	version := &cluster.MakeTestingClusterSettings().Version
	manualHeartbeat := &ManualHeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		ready:              make(chan error, 1),
		version:            version,
	}
	regularHeartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		version:            version,
	}

	request := &PingRequest{
		Ping:          "testManual",
		ServerVersion: version.ServerVersion,
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
		version:            &cluster.MakeTestingClusterSettings().Version,
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
	version := &cluster.MakeTestingClusterSettings().Version
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		version:            version,
	}

	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			heartbeat.clusterID.Reset(td.serverClusterID)
			request := &PingRequest{
				Ping:          "testPing",
				ClusterID:     &td.clientClusterID,
				ServerVersion: version.ServerVersion,
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

// Test version compatibility check in Ping handler. Note that version
// compatibility is also checked on the ping request side. This is tested in
// context_test.TestVersionCheckBidirectional.
func TestVersionCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

	empty := roachpb.Version{}

	// Version before the version check was implemented
	v0 := cluster.VersionByKey(cluster.VersionRPCNetworkStats)

	// Version where the check was implemented
	v1 := cluster.VersionByKey(cluster.VersionRPCVersionCheck)

	// Next version after
	v2 := cluster.VersionByKey(cluster.VersionRPCVersionCheck)
	v2.Unstable++

	testData := []struct {
		name           string
		clusterVersion roachpb.Version
		clientVersion  roachpb.Version
		expectError    bool
	}{
		{"clientVersion == clusterVersion", v1, v1, false},
		{"clientVersion > clusterVersion", v1, v2, false},
		{"clientVersion < clusterVersion", v2, v1, true},
		{"clusterVersion missing success", v0, empty, false},
		{"clientVersion missing fail", v1, empty, true},
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
			settings := cluster.MakeClusterSettings(td.clusterVersion, td.clusterVersion)
			cv := cluster.ClusterVersion{
				MinimumVersion: td.clusterVersion,
				UseVersion:     td.clusterVersion,
			}
			if err := settings.InitializeVersion(cv); err != nil {
				t.Fatal(err)
			}
			heartbeat.version = &settings.Version

			request := &PingRequest{
				Ping:          "testPing",
				ServerVersion: td.clientVersion,
			}
			_, err := heartbeat.Ping(context.Background(), request)
			if td.expectError {
				expected := "version compatibility check failed"
				if !testutils.IsError(err, expected) {
					t.Errorf("expected %s error, got %v", expected, err)
				}
			}
			if !td.expectError && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
		})
	}
}

// HeartbeatStreamService is like HeartbeatService, but it implements the
// TestingHeartbeatStreamServer interface in addition to the HeartbeatServer
// interface. Instead of providing a request-response model, the service reads
// on its input stream and periodically sends on its output stream with its
// latest ping response. This means that even if the service stops receiving
// requests, it will continue to send responses.
type HeartbeatStreamService struct {
	HeartbeatService
	interval time.Duration
}

func (hss *HeartbeatStreamService) PingStream(
	stream TestingHeartbeatStream_PingStreamServer,
) error {
	ctx := stream.Context()

	// Launch a goroutine to read from the stream and construct responses.
	respC := make(chan *PingResponse)
	recvErrC := make(chan error, 1)
	sendErrC := make(chan struct{})
	go func() {
		for {
			ping, err := stream.Recv()
			if err != nil {
				recvErrC <- err
				return
			}
			resp, err := hss.Ping(ctx, ping)
			if err != nil {
				recvErrC <- err
				return
			}
			select {
			case respC <- resp:
				continue
			case <-sendErrC:
				return
			}
		}
	}()

	// Launch a timer to periodically send the ping responses, even if the
	// response has not been updated.
	t := time.NewTicker(hss.interval)
	defer t.Stop()

	var resp *PingResponse
	for {
		select {
		case <-t.C:
			if resp != nil {
				err := stream.Send(resp)
				if err != nil {
					close(sendErrC)
					return err
				}
			}
		case resp = <-respC:
			// Update resp.
		case err := <-recvErrC:
			return err
		}
	}
}

// lockedPingStreamClient is an implementation of
// HeartbeatStream_PingStreamClient which provides support for concurrent calls
// to Send. Note that the default implementation of grpc.Stream for server
// responses (grpc.serverStream) is not safe for concurrent calls to Send.
type lockedPingStreamClient struct {
	TestingHeartbeatStream_PingStreamClient
	sendMu syncutil.Mutex
}

func (c *lockedPingStreamClient) Send(req *PingRequest) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.TestingHeartbeatStream_PingStreamClient.Send(req)
}
