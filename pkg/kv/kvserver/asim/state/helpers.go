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
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestingStartTime returns a start time that may be used for tests.
func TestingStartTime() time.Time {
	return time.Date(2022, 03, 21, 11, 0, 0, 0, time.UTC)
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
	clock := hlc

	// Never gossip, pass in nil values.
	g := gossip.NewTest(1, nil, nil, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
	sp := storepool.NewStorePool(
		ambientCtx,
		st,
		g,
		clock,
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
	state := newState()
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
			state.addReplica(rng.RangeID(), storeID)
		}
	}

	for key, storeID := range leaseholders {
		rng := state.RangeFor(key)
		state.TransferLease(rng.RangeID(), storeID)
	}
	return state
}

// NewTestStateReplCounts returns a state that may be used for testing, where
// the stores given are initialized with the specified replica counts.
func NewTestStateReplCounts(storeReplicas map[StoreID]int) State {
	rangesRequired := 0
	for _, replCount := range storeReplicas {
		if replCount > rangesRequired {
			rangesRequired = replCount
		}
	}

	splitKeys := []Key{}
	for i := 0; i < rangesRequired; i++ {
		splitKeys = append(splitKeys, Key(i+1))
	}

	replicas := make(map[Key][]StoreID)
	for _, key := range splitKeys {
		replicas[key] = []StoreID{}
	}

	for storeID, replicaCount := range storeReplicas {
		for i := 0; i < replicaCount; i++ {
			replicas[splitKeys[i]] = append(replicas[splitKeys[i]], storeID)
		}
	}

	return NewTestState(len(storeReplicas), 1, splitKeys, replicas, map[Key]StoreID{})
}
