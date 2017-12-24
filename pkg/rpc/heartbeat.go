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
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var _ security.RequestWithUser = &PingRequest{}

// GetUser implements security.RequestWithUser.
// Heartbeat messages are always sent by the node user.
func (*PingRequest) GetUser() string {
	return security.NodeUser
}

func (r RemoteOffset) measuredAt() time.Time {
	return timeutil.Unix(0, r.MeasuredAt)
}

// String formats the RemoteOffset for human readability.
func (r RemoteOffset) String() string {
	return fmt.Sprintf("off=%s, err=%s, at=%s", time.Duration(r.Offset), time.Duration(r.Uncertainty), r.measuredAt())
}

// A HeartbeatService exposes a method to echo its request params. It doubles
// as a way to measure the offset of the server from other nodes. It uses the
// clock to return the server time every heartbeat. It also keeps track of
// remote clocks sent to it by storing them in the remoteClockMonitor.
type HeartbeatService struct {
	// Provides the nanosecond unix epoch timestamp of the processor.
	clock *hlc.Clock
	// A pointer to the RemoteClockMonitor configured in the RPC Context,
	// shared by rpc clients, to keep track of remote clock measurements.
	remoteClockMonitor *RemoteClockMonitor
	clusterID          *base.ClusterIDContainer
	version            *cluster.ExposedClusterVersion
}

func checkVersion(
	clusterVersion *cluster.ExposedClusterVersion, peerVersion roachpb.Version,
) error {
	if !clusterVersion.IsInitialized() {
		// Cluster version has not yet been determined.
		return nil
	}
	if !clusterVersion.IsActive(cluster.VersionRPCVersionCheck) {
		// Cluster version predates this version check.
		return nil
	}
	minVersion := clusterVersion.Version().MinimumVersion
	if peerVersion == (roachpb.Version{}) {
		return errors.Errorf(
			"cluster requires at least version %s, but peer did not provide a version", minVersion)
	}
	if peerVersion.Less(minVersion) {
		return errors.Errorf(
			"cluster requires at least version %s, but peer has version %s", minVersion, peerVersion)
	}
	return nil
}

// Ping echos the contents of the request to the response, and returns the
// server's current clock value, allowing the requester to measure its clock.
// The requester should also estimate its offset from this server along
// with the requester's address.
func (hs *HeartbeatService) Ping(ctx context.Context, args *PingRequest) (*PingResponse, error) {
	// Enforce that clock max offsets are identical between nodes.
	// Commit suicide in the event that this is ever untrue.
	// This check is ignored if either offset is set to 0 (for unittests).
	mo, amo := hs.clock.MaxOffset(), time.Duration(args.MaxOffsetNanos)
	if mo != 0 && amo != 0 &&
		mo != timeutil.ClocklessMaxOffset && amo != timeutil.ClocklessMaxOffset &&
		mo != amo {

		panic(fmt.Sprintf("locally configured maximum clock offset (%s) "+
			"does not match that of node %s (%s)", mo, args.Addr, amo))
	}

	// Check that cluster IDs match.
	clusterID := hs.clusterID.Get()
	if args.ClusterID != nil && *args.ClusterID != uuid.Nil && clusterID != uuid.Nil &&
		*args.ClusterID != clusterID {
		return nil, errors.Errorf(
			"client cluster ID %q doesn't match server cluster ID %q", args.ClusterID, clusterID)
	}

	// Check version compatibility.
	if err := checkVersion(hs.version, args.ServerVersion); err != nil {
		return nil, errors.Wrap(err, "version compatibility check failed on ping request")
	}

	serverOffset := args.Offset
	// The server offset should be the opposite of the client offset.
	serverOffset.Offset = -serverOffset.Offset
	hs.remoteClockMonitor.UpdateOffset(ctx, args.Addr, serverOffset, 0 /* roundTripLatency */)
	return &PingResponse{
		Pong:          args.Ping,
		ServerTime:    hs.clock.PhysicalNow(),
		ServerVersion: hs.version.ServerVersion,
	}, nil
}
