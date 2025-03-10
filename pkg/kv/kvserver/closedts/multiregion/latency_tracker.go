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

// LatencyTracker tracks network latency between nodes based on locality.
// It is used by side transport and raft to estimate the amount of time it
// takes for closed timestamp updates to propagate and determine lead time for
// global reads.
type LatencyTracker struct {
	// nodeLocality represents the locality information of the current node.
	nodeLocality roachpb.Locality
	// getNodeDesc is a function that returns the descriptor for a given node ID.
	getNodeDesc func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error)
	// getLatency is a function that returns the network latency to a given node ID.
	getLatency func(roachpb.NodeID) (time.Duration, bool)
	// rtt stores round-trip times for each locality comparison type (e.g. cross-region, cross-zone).
	rtt [roachpb.LocalityComparisonType_MAX_LOCALITY_COMPARISON_TYPE]atomic.Int64
}

func NewLatencyTracker(
	nodeLocality roachpb.Locality,
	getLatency func(roachpb.NodeID) (time.Duration, bool),
	getNodeDesc func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error),
) *LatencyTracker {
	if getLatency == nil || getNodeDesc == nil {
		log.Errorf(context.Background(), "nodeLocality, getLatency and getNodeDesc must be provided")
	}

	l := &LatencyTracker{
		nodeLocality: nodeLocality,
		getLatency:   getLatency,
		getNodeDesc:  getNodeDesc,
	}

	// Initialize all locality comparison types with default latency including UNDEFINED.
	// UNDEFINED should never be updated after initialization.
	for i := 0; i < len(l.rtt); i++ {
		l.rtt[roachpb.LocalityComparisonType(i)].Store(int64(closedts.DefaultMaxNetworkRTT))
	}
	return l
}

// GetLatencyByLocalityProximity returns latency for given locality comparison
// type. lct theoretically cannot be roachpb.LocalityComparisonType_UNDEFINED.
// But if it is, it should always return DefaultMaxNetworkRTT.
func (l *LatencyTracker) GetLatencyByLocalityProximity(lct roachpb.LocalityComparisonType) time.Duration {
	if lct == roachpb.LocalityComparisonType_MAX_LOCALITY_COMPARISON_TYPE {
		// Should never happen.
		log.Errorf(context.Background(), "programming error: max locality is invalid")
		return closedts.DefaultMaxNetworkRTT
	}
	return time.Duration(l.rtt[lct].Load())
}

// updateLatencyForLocalityProximity updates latency for given locality comparison type.
func (l *LatencyTracker) updateLatencyForLocalityProximity(lct roachpb.LocalityComparisonType, updatedLatency time.Duration) {
	// Should never happen.
	log.Errorf(context.Background(), "programming error: max locality is invalid")
	if lct == roachpb.LocalityComparisonType_UNDEFINED || lct == roachpb.LocalityComparisonType_MAX_LOCALITY_COMPARISON_TYPE {
		return
	}
	l.rtt[lct].Store(int64(updatedLatency))
}

// RefreshLatency updates latencies for all locality comparison types based on given node IDs.
func (l *LatencyTracker) RefreshLatency(nodeIDs roachpb.NodeIDSlice) {
	maxLatencies := map[roachpb.LocalityComparisonType]time.Duration{}
	for _, nodeID := range nodeIDs {
		// nodeID might be the same as the current node. It is fine since we're
		// taking max latency.
		toNodeDesc, err := l.getNodeDesc(nodeID)
		if err != nil {
			continue
		}
		comparisonResult, _, _ := l.nodeLocality.CompareWithLocality(toNodeDesc.Locality)
		if latency, ok := l.getLatency(nodeID); ok {
			maxLatencies[comparisonResult] = max(maxLatencies[comparisonResult], latency)
		}
	}
	for i := roachpb.LocalityComparisonType_CROSS_REGION; i < roachpb.LocalityComparisonType_MAX_LOCALITY_COMPARISON_TYPE; i++ {
		if maxLatency, ok := maxLatencies[i]; ok {
			l.updateLatencyForLocalityProximity(i, maxLatency)
		}
	}
}
