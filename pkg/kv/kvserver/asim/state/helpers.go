// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/types"
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
func TestingSetRangeQPS(s State, rangeID RangeID, qps float64) {
	st := s.(*state)
	rlc := st.load[rangeID].(*ReplicaLoadCounter)

	rlc.loadStats.TestingSetStat(load.Queries, qps)
}

func testingResetLoad(s State, rangeID RangeID) {
	st := s.(*state)
	rlc := st.load[rangeID].(*ReplicaLoadCounter)
	rlc.ResetLoad()
}

// NewStorePool returns a store pool with no gossip instance and default values
// for configuration.
func NewStorePool(
	nodeCountFn storepool.NodeCountFunc,
	nodeLivenessFn storepool.NodeLivenessFunc,
	hlc *hlc.Clock,
	st *cluster.Settings,
) *storepool.StorePool {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	ambientCtx := log.MakeTestingAmbientContext(stopper.Tracer())

	// Never gossip, pass in nil values.
	g := gossip.NewTest(1, stopper, metric.NewRegistry())
	sp := storepool.NewStorePool(
		ambientCtx,
		st,
		g,
		hlc,
		nodeCountFn,
		nodeLivenessFn,
		/* deterministic */ true,
	)
	return sp
}

// OffsetTick creates a Tick from a start time and tick count, where each tick
// represents one second.
func OffsetTick(start time.Time, tick int64) types.Tick {
	return types.Tick{
		Start: start,
		Tick:  time.Second,
		Count: int(tick),
	}
}

// ReverseOffsetTick converts a Tick back to its tick count (number of seconds
// since the start time).
func ReverseOffsetTick(start time.Time, tick types.Tick) int64 {
	return int64(tick.Count)
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
			TestingSetRangeQPS(s, rng.RangeID(), qpsPerRange)
		}
	}
}
