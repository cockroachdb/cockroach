// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
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
	st := cluster.MakeTestingClusterSettings()
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		settings:           st,
	}

	request := &PingRequest{
		Ping:          "testPing",
		ServerVersion: st.Version.BinaryVersion(),
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
	settings           *cluster.Settings
	nodeID             *base.NodeIDContainer
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
	case <-mhs.stopper.ShouldQuiesce():
		return nil, errors.New("quiesce")
	}
	hs := HeartbeatService{
		clock:              mhs.clock,
		remoteClockMonitor: mhs.remoteClockMonitor,
		clusterID:          &base.ClusterIDContainer{},
		settings:           mhs.settings,
		nodeID:             mhs.nodeID,
	}
	return hs.Ping(ctx, args)
}

func TestManualHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(5)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	st := cluster.MakeTestingClusterSettings()
	manualHeartbeat := &ManualHeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		ready:              make(chan error, 1),
		settings:           st,
	}
	regularHeartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		settings:           st,
	}

	request := &PingRequest{
		Ping:          "testManual",
		ServerVersion: st.Version.BinaryVersion(),
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

	ctx := context.Background()

	clock := hlc.NewClock(hlc.UnixNano, 250*time.Millisecond)
	st := cluster.MakeTestingClusterSettings()
	hs := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		settings:           st,
	}
	hs.clusterID.Set(ctx, uuid.Nil)

	request := &PingRequest{
		Ping:                 "testManual",
		OriginAddr:           "test",
		OriginMaxOffsetNanos: (500 * time.Millisecond).Nanoseconds(),
		ServerVersion:        st.Version.BinaryVersion(),
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
	st := cluster.MakeTestingClusterSettings()
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		settings:           st,
	}

	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			heartbeat.clusterID.Reset(td.serverClusterID)
			request := &PingRequest{
				Ping:          "testPing",
				ClusterID:     &td.clientClusterID,
				ServerVersion: st.Version.BinaryVersion(),
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

func TestNodeIDCompare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []struct {
		name         string
		serverNodeID roachpb.NodeID
		clientNodeID roachpb.NodeID
		expectError  bool
	}{
		{"node IDs match", 1, 1, false},
		{"their node ID missing", 1, 0, false},
		{"our node ID missing", 0, 1, true},
		{"both node IDs missing", 0, 0, false},
		{"node ID mismatch", 1, 2, true},
	}

	manual := hlc.NewManualClock(5)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	st := cluster.MakeTestingClusterSettings()
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		nodeID:             &base.NodeIDContainer{},
		settings:           st,
	}

	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			heartbeat.nodeID.Reset(td.serverNodeID)
			request := &PingRequest{
				Ping:          "testPing",
				TargetNodeID:  td.clientNodeID,
				ServerVersion: st.Version.BinaryVersion(),
			}
			_, err := heartbeat.Ping(context.Background(), request)
			if td.expectError && err == nil {
				t.Error("expected node ID mismatch error")
			}
			if !td.expectError && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
		})
	}
}

// TestTenantVersionCheck verifies that the Ping version check allows
// secondary tenant connections to use a trailing version from what is
// active where the system tenant may not.
func TestTenantVersionCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(5)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		true /* initialize */)
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		settings:           st,
	}

	request := &PingRequest{
		Ping:          "testPing",
		ServerVersion: st.Version.BinaryMinSupportedVersion(),
	}
	const failedRE = `version compatibility check failed on ping request:` +
		` cluster requires at least version .*, but peer has version .*`
	// Ensure that the ping fails with an older version and an unadorned context.
	t.Run("too old, no tenant", func(t *testing.T) {
		_, err := heartbeat.Ping(context.Background(), request)
		require.Regexp(t, failedRE, err)
	})
	// Ensure the same behavior when a tenant ID exists but is for the system tenant.
	t.Run("too old, system tenant", func(t *testing.T) {
		tenantCtx := roachpb.NewContextForTenant(context.Background(), roachpb.SystemTenantID)
		_, err := heartbeat.Ping(tenantCtx, request)
		require.Regexp(t, failedRE, err)
	})
	// Ensure that the same ping succeeds with a secondary tenant context.
	t.Run("old, secondary tenant", func(t *testing.T) {
		tenantCtx := roachpb.NewContextForTenant(context.Background(), roachpb.MakeTenantID(2))
		_, err := heartbeat.Ping(tenantCtx, request)
		require.NoError(t, err)
	})
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
