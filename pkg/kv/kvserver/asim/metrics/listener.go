// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metrics

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

// StoreMetrics tracks metrics per-store in a simulation run. Each metrics
// struct is associated with a tick.
type StoreMetrics struct {
	Tick               time.Time
	StoreID            int64
	QPS                int64
	WriteKeys          int64
	WriteBytes         int64
	ReadKeys           int64
	ReadBytes          int64
	Replicas           int64
	Leases             int64
	LeaseTransfers     int64
	Rebalances         int64
	RebalanceSentBytes int64
	RebalanceRcvdBytes int64
	RangeSplits        int64
}

// Tracker maintains a list of listeners and updates them with new
// StoreMetrics information when ticked.
type Tracker struct {
	storeListeners []StoreMetricsListener
}

// StoreMetricsListener implements the Listen method, which can be called by
// the MetricsTracker to report new store metrics for a tick.
type StoreMetricsListener interface {
	Listen(context.Context, []StoreMetrics)
}

// NewTracker returns a new MetricsTracker.
func NewTracker(listeners ...StoreMetricsListener) *Tracker {
	return &Tracker{
		storeListeners: listeners,
	}
}

// Tick updates all listeners attached to the metrics tracker with the state at
// the tick given.
func (mt *Tracker) Tick(ctx context.Context, tick time.Time, s state.State) {
	if len(mt.storeListeners) == 0 {
		return
	}
	sms := []StoreMetrics{}
	usage := s.ClusterUsageInfo()

	for storeID, u := range usage.StoreUsage {
		store, ok := s.Store(storeID)
		if !ok {
			panic(fmt.Sprintf("store reported in cluster usage but not found in actual state storeID=%d", storeID))
		}

		sm := StoreMetrics{
			Tick:               tick,
			StoreID:            int64(storeID),
			QPS:                int64(store.Descriptor().Capacity.QueriesPerSecond),
			WriteKeys:          u.WriteKeys,
			WriteBytes:         u.WriteBytes,
			ReadKeys:           u.ReadKeys,
			ReadBytes:          u.ReadBytes,
			Replicas:           u.Replicas,
			Leases:             u.Leases,
			LeaseTransfers:     u.LeaseTransfers,
			Rebalances:         u.Rebalances,
			RebalanceSentBytes: u.RebalanceSentBytes,
			RebalanceRcvdBytes: u.RebalanceRcvdBytes,
			RangeSplits:        u.RangeSplits,
		}
		sms = append(sms, sm)
	}

	sort.Slice(sms, func(i, j int) bool {
		return sms[i].StoreID < sms[j].StoreID
	})

	for _, listener := range mt.storeListeners {
		listener.Listen(ctx, sms)
	}
}
