// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// MockNodeLiveness is responsible for tracking the liveness status of nodes
// without the need for real heartbeating. Instead, this mock implementation
// manages the statuses of all nodes by manually updating and accessing the
// internal map. It implements the NodeVitalityInterface to enable the
// SpanConfigConformance reporter to call it. Thus, we only expect calls to the
// ScanNodeVitalityFromCache function.
type MockNodeLiveness struct {
	clock     *hlc.Clock
	statusMap map[NodeID]livenesspb.NodeLivenessStatus
}

var _ livenesspb.NodeVitalityInterface = &MockNodeLiveness{}

func (m MockNodeLiveness) GetNodeVitalityFromCache(roachpb.NodeID) livenesspb.NodeVitality {
	panic("GetNodeVitalityFromCache is not expected to be called on MockNodeLiveness")
}

func (m MockNodeLiveness) ScanNodeVitalityFromKV(
	context.Context,
) (livenesspb.NodeVitalityMap, error) {
	panic("ScanNodeVitalityFromKV is not expected to be called on MockNodeLiveness")
}

// convertNodeStatusToNodeVitality constructs a node vitality in a manner that
// respects all node liveness status properties. The node vitality record for
// nodes in different states are set as follows:
// timeUntilNodeDead = time.Minute
// - unknown: invalid NodeVitality
// - live, decommissioning: expirationWallTime = MaxTimeStamp - timeUntilNodeDead
// - unavailable: expirationWallTime = now - time.Second
// - dead, decommissioned: expirationWallTime = 0
//
// Explanation: refer to liveness.LivenessStatus for an overview of the states a
// liveness record goes through.
//
// - live, decommissioning: set liveness expiration to be in the
// future(MaxTimestamp) and minus timeUntilNodeDead to avoid overflow in
// liveness status check tExp+timeUntilNodeDead.
// - unavailable: set liveness expiration to be just recently expired. This
// needs to be within now - timeUntilNodeDead <= expirationWallTime < now so
// that the status is not dead nor alive.
// - dead, decommissioned: set liveness expiration to be in the
// past(MinTimestamp).
func convertNodeStatusToNodeVitality(
	nid roachpb.NodeID, status livenesspb.NodeLivenessStatus, now hlc.Timestamp,
) livenesspb.NodeVitality {
	const timeUntilNodeDead = time.Minute
	var liveTs = hlc.MaxTimestamp.AddDuration(-timeUntilNodeDead).ToLegacyTimestamp()
	var deadTs = hlc.MinTimestamp.ToLegacyTimestamp()
	var unavailableTs = now.AddDuration(-time.Second).ToLegacyTimestamp()
	l := livenesspb.Liveness{
		NodeID:     nid,
		Draining:   false,
		Epoch:      1,
		Membership: livenesspb.MembershipStatus_ACTIVE,
	}
	switch status {
	case livenesspb.NodeLivenessStatus_UNKNOWN:
		return livenesspb.NodeVitality{}
	case livenesspb.NodeLivenessStatus_DEAD:
		l.Expiration = deadTs
	case livenesspb.NodeLivenessStatus_UNAVAILABLE:
		l.Expiration = unavailableTs
	case livenesspb.NodeLivenessStatus_LIVE:
		l.Expiration = liveTs
	case livenesspb.NodeLivenessStatus_DECOMMISSIONING:
		l.Expiration = liveTs
		l.Membership = livenesspb.MembershipStatus_DECOMMISSIONING
	case livenesspb.NodeLivenessStatus_DECOMMISSIONED:
		l.Expiration = deadTs
		l.Membership = livenesspb.MembershipStatus_DECOMMISSIONED
	case livenesspb.NodeLivenessStatus_DRAINING:
		l.Expiration = liveTs
		l.Draining = true
	}
	entry := l.CreateNodeVitality(
		now,               /* now */
		hlc.Timestamp{},   /* descUpdateTime */
		hlc.Timestamp{},   /* descUnavailableTime */
		true,              /* connected */
		timeUntilNodeDead, /* timeUntilNodeDead */
		0,                 /* timeAfterNodeSuspect */
	)
	return entry
}

func (m MockNodeLiveness) ScanNodeVitalityFromCache() livenesspb.NodeVitalityMap {
	isLiveMap := livenesspb.NodeVitalityMap{}
	for nodeID, status := range m.statusMap {
		nid := roachpb.NodeID(nodeID)
		now := m.clock.Now()
		isLiveMap[nid] = convertNodeStatusToNodeVitality(nid, status, now)
	}
	return isLiveMap
}
