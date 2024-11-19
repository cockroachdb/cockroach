// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	clock := timeutil.NewManualTime(timeutil.Unix(0, 5))
	maxOffset := time.Nanosecond /* maxOffset */
	st := cluster.MakeTestingClusterSettings()
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, maxOffset, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		version:            st.Version,
	}

	request := &PingRequest{
		Ping:          "testPing",
		ServerVersion: st.Version.LatestVersion(),
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
	clock              hlc.WallClock
	maxOffset          time.Duration
	remoteClockMonitor *RemoteClockMonitor
	version            clusterversion.Handle
	nodeID             *base.NodeIDContainer
	// Heartbeats are processed when a value is sent here.
	// If ready is nil, readyFn must be set instead and is
	// invoked whenever a heartbeat arrives to supply the
	// error, if one is to be injected.
	ready   chan error
	readyFn func() error
	stopper *stop.Stopper
}

// Ping waits until the heartbeat service is ready to respond to a Heartbeat.
func (mhs *ManualHeartbeatService) Ping(
	ctx context.Context, args *PingRequest,
) (*PingResponse, error) {
	extraCh := make(chan error, 1)
	if mhs.ready == nil {
		extraCh <- mhs.readyFn()
	}

	var err error
	select {
	case err = <-extraCh:
	case err = <-mhs.ready:
	case <-ctx.Done():
		err = ctx.Err()
	case <-mhs.stopper.ShouldQuiesce():
		err = errors.New("quiesce")
	}
	if err != nil {
		return nil, err
	}
	hs := HeartbeatService{
		clock:              mhs.clock,
		remoteClockMonitor: mhs.remoteClockMonitor,
		clusterID:          &base.ClusterIDContainer{},
		version:            mhs.version,
		nodeID:             mhs.nodeID,
	}
	return hs.Ping(ctx, args)
}

func TestManualHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := timeutil.NewManualTime(timeutil.Unix(0, 5))
	maxOffset := time.Nanosecond
	st := cluster.MakeTestingClusterSettings()
	manualHeartbeat := &ManualHeartbeatService{
		clock:              clock,
		maxOffset:          maxOffset,
		remoteClockMonitor: newRemoteClockMonitor(clock, maxOffset, time.Hour, 0),
		ready:              make(chan error, 1),
		version:            st.Version,
	}
	regularHeartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, maxOffset, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		version:            st.Version,
	}

	request := &PingRequest{
		Ping:          "testManual",
		ServerVersion: st.Version.LatestVersion(),
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

	clock := timeutil.NewManualTime(timeutil.Unix(0, 5))
	maxOffset := time.Nanosecond
	st := cluster.MakeTestingClusterSettings()
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, maxOffset, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		version:            st.Version,
	}

	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			heartbeat.clusterID.Reset(td.serverClusterID)
			request := &PingRequest{
				Ping:          "testPing",
				ClusterID:     &td.clientClusterID,
				ServerVersion: st.Version.LatestVersion(),
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

	clock := timeutil.NewManualTime(timeutil.Unix(0, 5))
	maxOffset := time.Nanosecond
	st := cluster.MakeTestingClusterSettings()
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, maxOffset, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		nodeID:             &base.NodeIDContainer{},
		version:            st.Version,
	}

	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			heartbeat.nodeID.Reset(td.serverNodeID)
			request := &PingRequest{
				Ping:          "testPing",
				TargetNodeID:  td.clientNodeID,
				ServerVersion: st.Version.LatestVersion(),
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
	clock := timeutil.NewManualTime(timeutil.Unix(0, 5))
	maxOffset := time.Nanosecond
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.MinSupported.Version(),
		true /* initialize */)
	heartbeat := &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: newRemoteClockMonitor(clock, maxOffset, time.Hour, 0),
		clusterID:          &base.ClusterIDContainer{},
		version:            st.Version,
	}

	request := &PingRequest{
		Ping:          "testPing",
		ServerVersion: st.Version.MinSupportedVersion(),
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
		tenantCtx := roachpb.ContextWithClientTenant(context.Background(), roachpb.SystemTenantID)
		_, err := heartbeat.Ping(tenantCtx, request)
		require.Regexp(t, failedRE, err)
	})
	// Ensure that the same ping succeeds with a secondary tenant context.
	t.Run("old, secondary tenant", func(t *testing.T) {
		tenantCtx := roachpb.ContextWithClientTenant(context.Background(), roachpb.MustMakeTenantID(2))
		_, err := heartbeat.Ping(tenantCtx, request)
		require.NoError(t, err)
	})
}
