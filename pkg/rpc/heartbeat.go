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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

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
	nodeID             *base.NodeIDContainer
	version            *cluster.ExposedClusterVersion

	// TestingAllowNamedRPCToAnonymousServer, when defined (in tests),
	// disables errors in case a heartbeat requests a specific node ID but
	// the remote node doesn't have a node ID yet. This testing knob is
	// currently used by the multiTestContext which does not suitably
	// populate separate node IDs for each heartbeat service.
	testingAllowNamedRPCToAnonymousServer bool
}

func checkVersion(
	clusterVersion *cluster.ExposedClusterVersion, peerVersion roachpb.Version,
) error {
	if !clusterVersion.IsInitialized() {
		// Cluster version has not yet been determined.
		return nil
	}
	minVersion := clusterVersion.MinSupportedVersion
	maxVersion := clusterVersion.ServerVersion
	if peerVersion == (roachpb.Version{}) {
		return errors.Errorf(
			"cluster requires at least version %s, but peer did not provide a version", minVersion)
	}
	if peerVersion.Less(minVersion) {
		// TODO(andrei): The peer is operating at a version that's too low. Perhaps
		// the peer could be operating at a higher version (i.e. perhaps it didn't
		// hear about a cluster version bump). Should we tell it that the bump
		// happened and teach it to react to that information?
		// E.g. Say we have two nodes:
		// - n1 is running with {MinSupportedVersion: 2, BinaryServerVersion: 3}
		// - n2 is running with {MinSupportedVersion: 1, BinaryServerVersion: 2}
		// The cluster version is 2, but n2 doesn't know that; it thinks it's still
		// 1. Say it never received the gossip update for that version bump.
		// n2 is prevented from connecting to n1.
		return errors.Errorf(
			"cluster requires at least version %s, but peer has version %s", minVersion, peerVersion)
	}
	if maxVersion.Less(peerVersion) {
		// The peer's version is too high. We won't be able to support its requests.
		return errors.Errorf(
			"node max version %s, but peer has version %s", maxVersion, peerVersion)
	}
	return nil
}

// Ping echos the contents of the request to the response, and returns the
// server's current clock value, allowing the requester to measure its clock.
// The requester should also estimate its offset from this server along
// with the requester's address.
func (hs *HeartbeatService) Ping(ctx context.Context, args *PingRequest) (*PingResponse, error) {
	if log.V(2) {
		log.Infof(ctx, "received heartbeat: %+v vs local cluster %+v node %+v", args, hs.clusterID, hs.nodeID)
	}
	// Check that cluster IDs match.
	clusterID := hs.clusterID.Get()
	if args.ClusterID != nil && *args.ClusterID != uuid.Nil && clusterID != uuid.Nil &&
		*args.ClusterID != clusterID {
		return nil, errors.Errorf(
			"client cluster ID %q doesn't match server cluster ID %q", args.ClusterID, clusterID)
	}
	// Check that node IDs match.
	var nodeID roachpb.NodeID
	if hs.nodeID != nil {
		nodeID = hs.nodeID.Get()
	}
	if args.NodeID != 0 && (!hs.testingAllowNamedRPCToAnonymousServer || nodeID != 0) && args.NodeID != nodeID {
		// If nodeID != 0, the situation is clear (we are checking that
		// the other side is talking to the right node).
		//
		// If nodeID == 0 this means that this node (serving the
		// heartbeat) doesn't have a node ID yet. Then we can't serve
		// connections for other nodes that want a specific node ID,
		// however we can still serve connections that don't need a node
		// ID, e.g. during initial gossip.
		return nil, errors.Errorf(
			"client requested node ID %d doesn't match server node ID %d", args.NodeID, nodeID)
	}

	// Check version compatibility.
	if err := checkVersion(hs.version, args.ClientClusterVersion); err != nil {
		return nil, errors.Wrap(err, "version compatibility check failed on ping request")
	}

	// Enforce that clock max offsets are identical between nodes.
	// Commit suicide in the event that this is ever untrue.
	// This check is ignored if either offset is set to 0 (for unittests).
	// Note that we validated this connection already. Different clusters
	// could very well have different max offsets.
	mo, amo := hs.clock.MaxOffset(), time.Duration(args.MaxOffsetNanos)
	if mo != 0 && amo != 0 &&
		mo != timeutil.ClocklessMaxOffset && amo != timeutil.ClocklessMaxOffset &&
		mo != amo {

		panic(fmt.Sprintf("locally configured maximum clock offset (%s) "+
			"does not match that of node %s (%s)", mo, args.Addr, amo))
	}

	serverOffset := args.Offset
	// The server offset should be the opposite of the client offset.
	serverOffset.Offset = -serverOffset.Offset
	hs.remoteClockMonitor.UpdateOffset(ctx, args.Addr, serverOffset, 0 /* roundTripLatency */)
	return &PingResponse{
		Pong:                 args.Ping,
		ServerTime:           hs.clock.PhysicalNow(),
		ServerClusterVersion: hs.version.Version().Version,
	}, nil
}
