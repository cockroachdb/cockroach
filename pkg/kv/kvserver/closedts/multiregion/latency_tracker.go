// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregion

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const maxLocalityComparisonType = roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE + 1

// LatencyTracker tracks network latency between nodes based on locality.
// It is used by side transport and raft to estimate the amount of time it
// takes for closed timestamp updates to propagate and determine lead time for
// global reads.
type LatencyTracker struct {
	// enabled is set to true if the latency tracker is enabled and has been refreshed.
	enabled atomic.Bool
	// nodeLocality represents the locality information of the current node.
	nodeLocality roachpb.Locality
	// getNodeDesc is a function that returns the descriptor for a given node ID.
	getNodeDesc func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error)
	// getLatency is a function that returns the network latency to a given node ID.
	getLatency func(roachpb.NodeID) (time.Duration, bool)
	// rtt stores round-trip times for each locality comparison type (e.g.
	// cross-region, cross-zone). We leave LocalityComparisonType_UNDEFINED in
	// this slice to avoid the complexity with one-off indices. But
	// LocalityComparisonType_UNDEFINED should always correspond to
	// DefaultMaxNetworkRTT.
	rtt [maxLocalityComparisonType]atomic.Int64
}

func NewLatencyTracker(
	nodeLocality roachpb.Locality,
	getLatency func(roachpb.NodeID) (time.Duration, bool),
	getNodeDesc func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error),
) *LatencyTracker {
	if getLatency == nil || getNodeDesc == nil {
		log.Fatalf(context.Background(), "getLatency and getNodeDesc must be provided")
		return nil
	}
	l := &LatencyTracker{
		nodeLocality: nodeLocality,
		getLatency:   getLatency,
		getNodeDesc:  getNodeDesc,
	}
	l.enabled.Store(false)
	// Initialize all locality comparison types with default latency including UNDEFINED.
	// UNDEFINED should never be updated after initialization.
	for i := 0; i < len(l.rtt); i++ {
		l.rtt[roachpb.LocalityComparisonType(i)].Store(int64(closedts.DefaultMaxNetworkRTT))
	}
	return l
}

// GetLatencyByLocalityProximity returns latency for given locality comparison
// type. roachpb.LocalityComparisonType_UNDEFINED should always return
// DefaultMaxNetworkRTT. If lead_for_global_reads_auto_tune_interval is 0, the
// latency tracker returns closedts.DefaultMaxNetworkRTT for all locality
// comparison types.
func (l *LatencyTracker) GetLatencyByLocalityProximity(
	lct roachpb.LocalityComparisonType,
) time.Duration {
	if lct == roachpb.LocalityComparisonType_UNDEFINED || !l.enabled.Load() {
		return closedts.DefaultMaxNetworkRTT
	}
	return time.Duration(l.rtt[lct].Load())
}

func (l *LatencyTracker) Enabled() bool {
	return l.enabled.Load()
}

// updateLatencyForLocalityProximity updates latency for given locality comparison type.
func (l *LatencyTracker) updateLatencyForLocalityProximity(
	lct roachpb.LocalityComparisonType, updatedLatency time.Duration,
) {
	if lct == roachpb.LocalityComparisonType_UNDEFINED {
		return
	}
	l.rtt[lct].Store(int64(updatedLatency))
}

// Disable disables the latency tracker. All future calls to
// GetLatencyByLocalityProximity will return DefaultMaxNetworkRTT until
// RefreshLatency is called to enable the tracker.
func (l *LatencyTracker) Disable() {
	l.enabled.Store(false)
}

// RefreshLatency updates latencies for all locality comparison types based on
// given node IDs.
func (l *LatencyTracker) RefreshLatency(nodeIDs roachpb.NodeIDSlice) {
	l.enabled.Store(true)
	maxLatencies := map[roachpb.LocalityComparisonType]time.Duration{}
	for _, nodeID := range nodeIDs {
		// nodeID might be the same as the current node. It is fine since we're
		// taking max latency for each locality comparison type.
		toNodeDesc, err := l.getNodeDesc(nodeID)
		if err != nil {
			continue
		}
		comparisonResult, _, _ := l.nodeLocality.CompareWithLocality(toNodeDesc.Locality)
		if latency, ok := l.getLatency(nodeID); ok {
			maxLatencies[comparisonResult] = max(maxLatencies[comparisonResult], latency)
		}
	}
	// Skip LocalityComparisonType_UNDEFINED since it should always be
	// DefaultMaxNetworkRTT.
	for i := roachpb.LocalityComparisonType_CROSS_REGION; i < maxLocalityComparisonType; i++ {
		if maxLatency, ok := maxLatencies[i]; ok {
			l.updateLatencyForLocalityProximity(i, maxLatency)
		}
	}
}
