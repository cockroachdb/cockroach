// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregion

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// LatencyTracker tracks network latency between nodes based on locality.
// It is used by side transport and raft to estimate the amount of time it
// takes for closed timestamp updates to propagate and determine lead time for
// global reads.
type LatencyTracker struct {
	enabled    atomic.Bool
	getLatency func(roachpb.NodeID) (time.Duration, bool)
	rtt        syncutil.Map[roachpb.NodeID, int32]
}

// NewLatencyTracker creates a new LatencyTracker instance.
// getLatency is a function that returns the latency for a given node ID.
func NewLatencyTracker(getLatency func(roachpb.NodeID) (time.Duration, bool)) *LatencyTracker {
	if getLatency == nil {
		panic("getLatency function cannot be nil")
	}
	l := &LatencyTracker{
		getLatency: getLatency,
	}
	l.enabled.Store(false)
	return l
}

// GetLatencyByLocalityProximity returns the locality-based latency policy for a given node.
// If tracking is disabled or no data exists for the node, it returns a default policy.
func (l *LatencyTracker) GetLatencyByLocalityProximity(
	nodeID roachpb.NodeID,
) ctpb.RangeClosedTimestampByPolicyLocality {
	if !l.enabled.Load() {
		return ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY
	}

	policyLocality, ok := l.rtt.Load(nodeID)
	if !ok {
		return ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY
	}
	return ctpb.RangeClosedTimestampByPolicyLocality(*policyLocality)
}

// Enabled returns whether latency tracking is currently enabled.
func (l *LatencyTracker) Enabled() bool {
	return l.enabled.Load()
}

// updateLatencyForLocalityProximity updates the latency policy for a specific node.
func (l *LatencyTracker) updateLatencyForLocalityProximity(
	nodeID roachpb.NodeID, policyLocality ctpb.RangeClosedTimestampByPolicyLocality,
) {
	val := int32(policyLocality)
	l.rtt.Store(nodeID, &val)
}

// Disable turns off latency tracking.
func (l *LatencyTracker) Disable() {
	l.enabled.Store(false)
	// Clear existing data when disabled
	l.rtt = syncutil.Map[roachpb.NodeID, int32]{}
}

// RefreshLatency updates latency information for the provided node IDs.
// It enables tracking and updates the latency buckets based on current measurements.
func (l *LatencyTracker) RefreshLatency(nodeIDs roachpb.NodeIDSlice) {
	l.enabled.Store(true)
	for _, nodeID := range nodeIDs {
		if latency, ok := l.getLatency(nodeID); ok {
			latencyBucket := int32(latency%30*time.Millisecond + 1)
			l.rtt.Store(nodeID, &latencyBucket)
		}
	}
}
