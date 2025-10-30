// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/types"
)

// StoreMetrics tracks metrics per-store in a simulation run. Each metrics
// struct is associated with a tick.
type StoreMetrics struct {
	Tick                types.Tick
	StoreID             int64
	QPS                 int64
	CPU                 int64
	NodeCPUUtilization  float64
	WriteKeys           int64
	WriteBytes          int64
	WriteBytesPerSecond int64
	ReadKeys            int64
	ReadBytes           int64
	Replicas            int64
	Leases              int64
	// LeaseTransfers tracks the number of lease transfer that this store has
	// authored. Only the leaseholder store authors transfers.
	LeaseTransfers int64
	// Rebalances tracks the number of replica rebalances that a store has
	// authored. Only the leaseholder store for a range authors rebalances of
	// replicas belonging to that range.
	Rebalances         int64
	RebalanceSentBytes int64
	RebalanceRcvdBytes int64
	RangeSplits        int64
	DiskFractionUsed   float64
}

// GetMetricValue extracts the requested metric value from StoreMetrics.
func (sm *StoreMetrics) GetMetricValue(stat string) float64 {
	switch stat {
	case "qps":
		return float64(sm.QPS)
	case "cpu":
		return float64(sm.CPU)
	case "write":
		return float64(sm.WriteKeys)
	case "write_b":
		return float64(sm.WriteBytes)
	case "write_bytes_per_second":
		return float64(sm.WriteBytesPerSecond)
	case "read":
		return float64(sm.ReadKeys)
	case "read_b":
		return float64(sm.ReadBytes)
	case "replicas":
		return float64(sm.Replicas)
	case "leases":
		return float64(sm.Leases)
	case "lease_moves":
		return float64(sm.LeaseTransfers)
	case "replica_moves":
		return float64(sm.Rebalances)
	case "replica_b_rcvd":
		return float64(sm.RebalanceRcvdBytes)
	case "replica_b_sent":
		return float64(sm.RebalanceSentBytes)
	case "range_splits":
		return float64(sm.RangeSplits)
	case "disk_fraction_used":
		return sm.DiskFractionUsed
	case "cpu_util":
		return sm.NodeCPUUtilization
	default:
		return 0
	}
}

// the MetricsTracker to report new store metrics for a tick.
type StoreMetricsListener interface {
	Listen(context.Context, []StoreMetrics)
}

// Tracker maintains a list of listeners and updates them with new
// StoreMetrics information when ticked.
type Tracker struct {
	storeListeners []StoreMetricsListener
	lastTick       types.Tick
	interval       time.Duration
}

// NewTracker returns a new MetricsTracker.
func NewTracker(interval time.Duration, listeners ...StoreMetricsListener) *Tracker {
	return &Tracker{
		storeListeners: listeners,
		interval:       interval,
	}
}

// Register registers StoreMetricsListener's against the tracker. Subsequent
// calls to Tick will also udpate the listeners if new metrics are available.
func (mt *Tracker) Register(listeners ...StoreMetricsListener) {
	mt.storeListeners = append(mt.storeListeners, listeners...)
}

// Tick updates all listeners attached to the metrics tracker with the state at
// the tick given.
func (mt *Tracker) Tick(ctx context.Context, tick types.Tick, s state.State) {
	// On the first call (lastTick is zero-valued), we should output metrics
	// immediately. After that, check if enough time has passed.
	if mt.lastTick.Tick != 0 && mt.lastTick.FromWallTime(mt.lastTick.WallTime().Add(mt.interval)).After(tick) {
		// Nothing to do yet.
		return
	}

	if len(mt.storeListeners) < 1 {
		// There are no listeners, so there is no point updating metrics here.
		return
	}

	sms := []StoreMetrics{}
	usage := s.ClusterUsageInfo()

	storeIDs := []state.StoreID{}
	for _, store := range s.Stores() {
		storeIDs = append(storeIDs, store.StoreID())
	}

	// Recompute the store descriptors. We access them directly via the store
	// interface below.
	_ = s.StoreDescriptors(false, storeIDs...)

	for storeID, u := range usage.StoreUsage {
		store, ok := s.Store(storeID)
		if !ok {
			panic(fmt.Sprintf("store reported in cluster usage but not found in actual state storeID=%d", storeID))
		}

		desc := store.Descriptor()
		nodeCapacity := s.NodeCapacity(store.NodeID())

		// NodeCPURateUsage is the same as StoresCPURate in asim.
		if nodeCapacity.NodeCPURateCapacity == 0 {
			panic(fmt.Sprintf("unexpected: node cpu rate capacity is 0 (node cpu rate usage = %d)",
				nodeCapacity.NodeCPURateUsage))
		}
		cpuUtil := float64(nodeCapacity.NodeCPURateUsage) / float64(nodeCapacity.NodeCPURateCapacity)

		sm := StoreMetrics{
			Tick:                tick,
			StoreID:             int64(storeID),
			QPS:                 int64(desc.Capacity.QueriesPerSecond),
			CPU:                 int64(desc.Capacity.CPUPerSecond),
			NodeCPUUtilization:  cpuUtil,
			WriteKeys:           u.WriteKeys,
			WriteBytes:          u.WriteBytes,
			WriteBytesPerSecond: int64(desc.Capacity.WriteBytesPerSecond),
			ReadKeys:            u.ReadKeys,
			ReadBytes:           u.ReadBytes,
			Replicas:            int64(desc.Capacity.RangeCount),
			Leases:              int64(desc.Capacity.LeaseCount),
			LeaseTransfers:      u.LeaseTransfers,
			Rebalances:          u.Rebalances,
			RebalanceSentBytes:  u.RebalanceSentBytes,
			RebalanceRcvdBytes:  u.RebalanceRcvdBytes,
			RangeSplits:         u.RangeSplits,
			DiskFractionUsed:    desc.Capacity.FractionUsed(),
		}
		sms = append(sms, sm)
	}

	slices.SortFunc(sms, func(a, b StoreMetrics) int {
		return cmp.Compare(a.StoreID, b.StoreID)
	})

	for _, listener := range mt.storeListeners {
		listener.Listen(ctx, sms)
	}

	// Update lastTick to mark when we output metrics.
	mt.lastTick = tick
}
