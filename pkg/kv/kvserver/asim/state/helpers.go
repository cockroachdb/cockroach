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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
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

// TestingSetRangeQPS sets the QPS for the range with ID rangeID. This will
// show on the current leaseholder replica load for this range and persist
// between transfers.
func TestingSetRangeQPS(s State, rangeID RangeID, qps float64) bool {
	store, ok := s.LeaseholderStore(rangeID)
	if !ok {
		return false
	}

	rlc := s.ReplicaLoad(rangeID, store.StoreID()).(*ReplicaLoadCounter)
	rlc.QPS.SetMeanRateForTesting(qps)

	if qps != s.ReplicaLoad(rangeID, store.StoreID()).Load().QueriesPerSecond {
		panic("qps didn't get set")
	}
	return true
}

// NewStorePool returns a store pool with no gossip instance and default values
// for configuration.
func NewStorePool(
	nodeCountFn storepool.NodeCountFunc, nodeLivenessFn storepool.NodeLivenessFunc, hlc *hlc.Clock,
) *storepool.StorePool {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	st := cluster.MakeTestingClusterSettings()
	ambientCtx := log.MakeTestingAmbientContext(stopper.Tracer())

	// Never gossip, pass in nil values.
	g := gossip.NewTest(1, nil, nil, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
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

// NewTestState returns a State populated with the specification given.
func NewTestState(
	nodes int,
	storesPerNode int,
	startKeys []Key,
	replicas map[Key][]StoreID,
	leaseholders map[Key]StoreID,
) State {
	state := newState(config.DefaultSimulationSettings())
	for i := 0; i < nodes; i++ {
		node := state.AddNode()
		for j := 0; j < storesPerNode; j++ {
			state.AddStore(node.NodeID())
		}
	}

	ranges := make([]Range, len(startKeys))
	for i, key := range startKeys {
		_, rhs, _ := state.SplitRange(key)
		ranges[i] = rhs
	}

	for key, stores := range replicas {
		rng := state.RangeFor(key)
		for _, storeID := range stores {
			state.AddReplica(rng.RangeID(), storeID)
		}
	}

	for key, storeID := range leaseholders {
		rng := state.RangeFor(key)
		state.TransferLease(rng.RangeID(), storeID)
	}

	return state
}

type storeRangeCount struct {
	requestedReplicas int
	storeID           StoreID
}

type storeRangeCounts []storeRangeCount

func (s storeRangeCounts) Len() int           { return len(s) }
func (s storeRangeCounts) Less(i, j int) bool { return s[i].requestedReplicas > s[j].requestedReplicas }
func (s storeRangeCounts) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// NewTestStateReplDistribution returns a State that may be used for testing,
// where the stores given are initialized with the specified % of the replicas.
// This is done on a best effort basis, given the replication factor. It may be
// impossible to satisfy some distributions, for example: percentOfReplicas {1:
// 0.40, 2: 0.20, 3: 0.20}, replicationFactor 3, would be impossible to satisfy
// as the only distribution possible is {1: 0.33, 2: 0.33, 3: 0.33} given a
// replication factor of 3. A best effort distribution is applied in these
// cases.
func NewTestStateReplDistribution(
	percentOfReplicas []float64, ranges, replicationFactor, keyspace int,
) State {
	targetRangeCount := make(storeRangeCounts, len(percentOfReplicas))
	for i, percent := range percentOfReplicas {
		requiredRanges := int(float64(ranges*replicationFactor) * (percent))
		targetRangeCount[i] = storeRangeCount{requestedReplicas: requiredRanges, storeID: StoreID(i + 1)}
	}

	// If there are no ranges specified, default to 1 range.
	if ranges == 0 {
		ranges = 1
	}

	// There cannot be less keys than there are ranges.
	if ranges > keyspace {
		keyspace = ranges
	}
	rangeInterval := keyspace / ranges

	startKeys := make([]Key, ranges)
	replicas := make(map[Key][]StoreID)
	for i := 0; i < ranges; i++ {
		key := Key(i * rangeInterval)
		startKeys[i] = key
		replicas[key] = make([]StoreID, replicationFactor)

		sort.Sort(targetRangeCount)
		for j := 0; j < replicationFactor; j++ {
			targetRangeCount[j].requestedReplicas--
			replicas[key][j] = targetRangeCount[j].storeID
		}
	}

	s := NewTestState(len(percentOfReplicas), 1, startKeys, replicas, map[Key]StoreID{})
	spanconfig := defaultSpanConfig
	spanconfig.NumVoters = int32(replicationFactor)
	spanconfig.NumReplicas = int32(replicationFactor)
	for _, r := range s.Ranges() {
		s.SetSpanConfig(r.RangeID(), spanconfig)
	}
	return s
}

// NewTestStateEvenDistribution returns a new State that may be used for
// testing, where the replica count per store is equal.
func NewTestStateEvenDistribution(stores, ranges, replicationFactor, keyspace int) State {
	distribution := []float64{}
	frac := 1.0 / float64(stores)
	for i := 0; i < stores; i++ {
		distribution = append(distribution, frac)
	}
	return NewTestStateReplDistribution(distribution, ranges, replicationFactor, keyspace)
}

// AddEmptyRanges adds empty ranges to the end of the keyspace, where the
// placement follows whatever the last range is.
func AddEmptyRanges(state State, n int) {
	for key := 1; key < n; key++ {
		_, _, _ = state.SplitRange(MaxKey - Key(key))
	}
}

// NewTestStateSkewedDistribution returns a new State that may be used for
// testing, where the replica count per store is skewed.
func NewTestStateSkewedDistribution(stores, ranges, replicationFactor, keyspace int) State {
	distribution := []float64{}
	rem := ranges
	for i := 0; i < stores; i++ {
		rem /= 2
		distribution = append(distribution, float64(rem))
	}
	return NewTestStateReplDistribution(distribution, ranges, replicationFactor, keyspace)
}

// NewTestStateReplCounts returns a new test state where each store is
// initialized the given number of replicas. The required number of ranges is
// inferred from the replication factor and the replica count.
func NewTestStateReplCounts(replCounts map[StoreID]int, replicationFactor, keyspace int) State {
	total := 0
	nStores := len(replCounts)
	for _, count := range replCounts {
		total += count
	}

	replDistribution := make([]float64, nStores)
	ranges := total / replicationFactor
	for store, count := range replCounts {
		replDistribution[store-1] = float64(count) / float64(ranges)
	}

	return NewTestStateReplDistribution(replDistribution, ranges, replicationFactor, keyspace)
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
			rlc.QPS.SetMeanRateForTesting(qpsPerRange)
		}
	}
}
