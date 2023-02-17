// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package state

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// NewShuffler returns a function which will shuffle elements determinstically
// but without order.
func NewShuffler(seed int64) func(n int, swap func(i, j int)) {
	r := rand.New(rand.NewSource(seed))
	return func(n int, swap func(i, j int)) {
		r.Shuffle(n, swap)
	}
}

// TestingStartTime returns a start time that may be used for tests.
func TestingStartTime() time.Time {
	return time.Date(2022, 03, 21, 11, 0, 0, 0, time.UTC)
}

// TestingWorkloadSeed returns a seed to use for constructing a workload
// generator in unit tests.
func TestingWorkloadSeed() int64 {
	return 42
}

// TestingSetRangeQPS sets the QPS for the range with ID rangeID. This will
// show on the current leaseholder replica load for this range and persist
// between transfers.
func TestingSetRangeQPS(s State, rangeID RangeID, qps float64) bool {
	store, ok := s.LeaseholderStore(rangeID)
	if !ok {
		return false
	}

	rlc := s.ReplicaLoad(rangeID, store.StoreID()).(*ReplicaLoadCounter)
	rlc.loadStats.TestingSetStat(load.Queries, qps)
	return true
}

// NewStorePool returns a store pool with no gossip instance and default values
// for configuration.
func NewStorePool(
	nodeCountFn storepool.NodeCountFunc, nodeLivenessFn storepool.NodeLivenessFunc, hlc *hlc.Clock,
) (*storepool.StorePool, *cluster.Settings) {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	st := cluster.MakeTestingClusterSettings()
	ambientCtx := log.MakeTestingAmbientContext(stopper.Tracer())

	// Never gossip, pass in nil values.
	g := gossip.NewTest(1, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
	sp := storepool.NewStorePool(
		ambientCtx,
		st,
		g,
		hlc,
		nodeCountFn,
		nodeLivenessFn,
		/* deterministic */ true,
	)
	return sp, st
}

// OffsetTick offsets start time by adding tick number of seconds to it.
// TODO(kvoli): Use a dedicated tick package, which would contain methods such
// as this. Deprecating direct use of time.
func OffsetTick(start time.Time, tick int64) time.Time {
	tickTime := start.Add(time.Duration(tick) * time.Second)
	return tickTime
}

// ReverseOffsetTick converts an offset time from the start time, into the
// number of ticks (seconds) since the start.
func ReverseOffsetTick(start, tickTime time.Time) int64 {
	offSetTickTime := tickTime.Sub(start)
	return int64(offSetTickTime.Seconds())
}

// TestDistributeQPSCounts distributes QPS evenly among the leaseholder
// replicas on a store, such that the total QPS for the store matches the
// qpsCounts argument passed in.
func TestDistributeQPSCounts(s State, qpsCounts []float64) {
	stores := s.Stores()
	if len(stores) != len(qpsCounts) {
		return
	}

	for x, qpsCount := range qpsCounts {
		storeID := StoreID(x + 1)
		lhs := []Range{}
		for _, replica := range s.Replicas(storeID) {
			if replica.HoldsLease() {
				rng, _ := s.Range(replica.Range())
				lhs = append(lhs, rng)
			}
		}

		qpsPerRange := qpsCount / float64(len(lhs))
		for _, rng := range lhs {
			rl := s.ReplicaLoad(rng.RangeID(), storeID)
			rlc := rl.(*ReplicaLoadCounter)
			rlc.loadStats.TestingSetStat(load.Queries, qpsPerRange)
		}
	}
}
