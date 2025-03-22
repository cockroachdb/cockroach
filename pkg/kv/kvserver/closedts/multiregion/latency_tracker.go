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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const maxLocalityComparisonType = roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE + 1

// LatencyTracker tracks network latency between nodes based on locality.
// It is used by side transport and raft to estimate the amount of time it
// takes for closed timestamp updates to propagate and determine lead time for
// global reads.
type LatencyTracker struct {
	enabled      atomic.Bool
	nodeLocality roachpb.Locality
	getNodeDesc  func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error)
	getLatency   func(roachpb.NodeID) (time.Duration, bool)
	rtt          syncutil.Map[roachpb.NodeID, int64]
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
	return l
}

// GetLatencyByLocalityProximity returns latency for given locality comparison
// type. roachpb.LocalityComparisonType_UNDEFINED should always return
// DefaultMaxNetworkRTT. If lead_for_global_reads_auto_tune_interval is 0, the
// latency tracker returns closedts.DefaultMaxNetworkRTT for all locality
// comparison types.
func (l *LatencyTracker) GetLatencyByLocalityProximity(nodeID roachpb.NodeID) time.Duration {
	if !l.enabled.Load() {
		return closedts.DefaultMaxNetworkRTT
	}

	if rtt, ok := l.rtt.Load(nodeID); ok {
		return time.Duration(*rtt)
	}
	return closedts.DefaultMaxNetworkRTT
}

func (l *LatencyTracker) Enabled() bool {
	return l.enabled.Load()
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
	for _, nodeID := range nodeIDs {
		// nodeID might be the same as the current node. It is fine since we're
		// taking max latency for each locality comparison type.
		toNodeDesc, err := l.getNodeDesc(nodeID)
		if err != nil {
			continue
		}
		latency, ok := l.getLatency(toNodeDesc.NodeID)
		if !ok {
			continue
		}
		latencyAsInt := int64(latency)
		l.rtt.Store(nodeID, &latencyAsInt)
	}
}
