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

// LivenessState represents the liveness of a store.
// They are ordered from best (healthy) to worst.
type LivenessState byte

const (
	// LivenessLive means the store is alive and healthy.
	LivenessLive LivenessState = iota
	// LivenessUnavailable means the store is recently down but not yet marked dead.
	LivenessUnavailable
	// LivenessDead means the store has been down long enough to be considered dead.
	LivenessDead
)

// StoreStatus represents the status of a store in the simulation. For MMA,
// liveness is tracked per-store and to support this, asim does as well.
// Whenever we simulate the single-metric allocator, it sees a node as live if
// all of its stores are live, i.e. we make the assumption that an unhealthy
// store would also render the node unhealthy (to sma). This should generally
// hold in practice (for example, liveness heartbeats synchronously write to all
// engines).
type StoreStatus struct {
	Liveness LivenessState
}

// NodeStatus represents the status of a node in the simulation. This holds
// signals that are genuinely per-node: membership and draining.
type NodeStatus struct {
	Membership livenesspb.MembershipStatus
	Draining   bool
}

// StatusTracker is responsible for tracking the liveness status of stores
// and the membership/draining status of nodes without real heartbeating.
// It implements the NodeVitalityInterface to enable the SpanConfigConformance
// reporter to call it.
//
// Liveness is tracked per-store (for MMA). When SMA needs node-level liveness,
// it is aggregated from the stores: the node takes the "worst" liveness state
// of any of its stores.
type StatusTracker struct {
	clock          *hlc.Clock
	storeStatusMap map[StoreID]StoreStatus
	nodeStatusMap  map[NodeID]NodeStatus
	// storeToNode maps each store to its node for aggregation.
	storeToNode map[StoreID]NodeID
}

var _ livenesspb.NodeVitalityInterface = &StatusTracker{}

// registerStore registers a new store with its node mapping and initializes it as live.
func (st *StatusTracker) registerStore(storeID StoreID, nodeID NodeID) {
	st.storeStatusMap[storeID] = StoreStatus{Liveness: LivenessLive}
	st.storeToNode[storeID] = nodeID
}

func (st StatusTracker) GetNodeVitalityFromCache(roachpb.NodeID) livenesspb.NodeVitality {
	panic("GetNodeVitalityFromCache is not expected to be called on StatusTracker")
}

func (st StatusTracker) ScanNodeVitalityFromKV(
	context.Context,
) (livenesspb.NodeVitalityMap, error) {
	panic("ScanNodeVitalityFromKV is not expected to be called on StatusTracker")
}

// worstLivenessForStoresOnNode returns the "worst" liveness state across all
// stores on a node. The ordering is: Dead is worse than Unavailable is worse
// than Live.
func (st StatusTracker) worstLivenessForStoresOnNode(nodeID NodeID) LivenessState {
	worst := LivenessLive
	for storeID, ss := range st.storeStatusMap {
		if st.storeToNode[storeID] == nodeID {
			worst = max(worst, ss.Liveness)
		}
	}
	return worst
}

// convertToNodeVitality constructs a NodeVitality for a node by combining
// store-level liveness (aggregated) with node-level membership and draining.
//
// This is used by the old allocator (SMA) which operates on nodes. MMA uses
// store-level liveness directly and doesn't need NodeVitality.
//
// The liveness affects the expiration timestamp:
//   - live: expiration far in the future (node passes isAlive checks)
//   - unavailable: expiration just recently passed (node is unavailable but not dead)
//   - dead: expiration far in the past (node fails isAlive checks)
//
// timeUntilNodeDead is set to 1 minute.
func (st StatusTracker) convertToNodeVitality(
	nodeID NodeID, now hlc.Timestamp,
) livenesspb.NodeVitality {
	const timeUntilNodeDead = time.Minute
	var liveTs = hlc.MaxTimestamp.AddDuration(-timeUntilNodeDead).ToLegacyTimestamp()
	var unavailableTs = now.AddDuration(-time.Second).ToLegacyTimestamp()
	var deadTs = hlc.MinTimestamp.ToLegacyTimestamp()

	ns := st.nodeStatusMap[nodeID]
	liveness := st.worstLivenessForStoresOnNode(nodeID)

	nid := roachpb.NodeID(nodeID)
	l := livenesspb.Liveness{
		NodeID:     nid,
		Epoch:      1,
		Membership: ns.Membership,
		Draining:   ns.Draining,
	}

	switch liveness {
	case LivenessLive:
		l.Expiration = liveTs
	case LivenessUnavailable:
		l.Expiration = unavailableTs
	case LivenessDead:
		l.Expiration = deadTs
	}

	ncs := livenesspb.NewNodeConnectionStatus(nid, nil)
	ncs.SetIsConnected(liveness == LivenessLive)
	entry := l.CreateNodeVitality(
		now,               /* now */
		hlc.Timestamp{},   /* descUpdateTime */
		hlc.Timestamp{},   /* descUnavailableTime */
		ncs,               /* nodeConnectionStatus */
		timeUntilNodeDead, /* timeUntilNodeDead */
		0,                 /* timeAfterNodeSuspect */
	)
	return entry
}

func (st StatusTracker) ScanNodeVitalityFromCache() livenesspb.NodeVitalityMap {
	isLiveMap := livenesspb.NodeVitalityMap{}
	now := st.clock.Now()
	for nodeID := range st.nodeStatusMap {
		nid := roachpb.NodeID(nodeID)
		isLiveMap[nid] = st.convertToNodeVitality(nodeID, now)
	}
	return isLiveMap
}
