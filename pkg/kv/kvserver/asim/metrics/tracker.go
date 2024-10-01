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
)

// StoreMetrics tracks metrics per-store in a simulation run. Each metrics
// struct is associated with a tick.
type StoreMetrics struct {
	Tick       time.Time
	StoreID    int64
	QPS        int64
	WriteKeys  int64
	WriteBytes int64
	ReadKeys   int64
	ReadBytes  int64
	Replicas   int64
	Leases     int64
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

// the MetricsTracker to report new store metrics for a tick.
type StoreMetricsListener interface {
	Listen(context.Context, []StoreMetrics)
}

// Tracker maintains a list of listeners and updates them with new
// StoreMetrics information when ticked.
type Tracker struct {
	storeListeners []StoreMetricsListener
	lastTick       time.Time
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
func (mt *Tracker) Tick(ctx context.Context, tick time.Time, s state.State) {
	if mt.lastTick.Add(mt.interval).After(tick) {
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

		sm := StoreMetrics{
			Tick:               tick,
			StoreID:            int64(storeID),
			QPS:                int64(desc.Capacity.QueriesPerSecond),
			WriteKeys:          u.WriteKeys,
			WriteBytes:         u.WriteBytes,
			ReadKeys:           u.ReadKeys,
			ReadBytes:          u.ReadBytes,
			Replicas:           int64(desc.Capacity.RangeCount),
			Leases:             int64(desc.Capacity.LeaseCount),
			LeaseTransfers:     u.LeaseTransfers,
			Rebalances:         u.Rebalances,
			RebalanceSentBytes: u.RebalanceSentBytes,
			RebalanceRcvdBytes: u.RebalanceRcvdBytes,
			RangeSplits:        u.RangeSplits,
			DiskFractionUsed:   desc.Capacity.FractionUsed(),
		}
		sms = append(sms, sm)
	}

	slices.SortFunc(sms, func(a, b StoreMetrics) int {
		return cmp.Compare(a.StoreID, b.StoreID)
	})

	for _, listener := range mt.storeListeners {
		listener.Listen(ctx, sms)
	}
}
