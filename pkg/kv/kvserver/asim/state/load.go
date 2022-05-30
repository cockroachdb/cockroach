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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ReplicaLoad defines the methods a datastructure is required to perform in
// order to record and report load events.
type ReplicaLoad interface {
	// ApplyLoad applies a load event to a replica.
	ApplyLoad(le workload.LoadEvent)
	// Load translates the recorded load events into usage information of the
	// replica.
	Load() allocator.RangeUsageInfo
}

// ReplicaLoadCounter is the sum of all key accesses and size of bytes, both written
// and read.
// TODO(kvoli): In the non-simulated code, replica_stats currently maintains
// this structure, which is rated. This datastructure needs to be adapated by
// the user to be rated over time. In the future we should introduce a better
// general pupose stucture that enables rating.
type ReplicaLoadCounter struct {
	WriteKeys  int64
	WriteBytes int64
	ReadKeys   int64
	ReadBytes  int64
}

// ApplyLoad applies a load event onto a replica load counter.
func (rl *ReplicaLoadCounter) ApplyLoad(le workload.LoadEvent) {
	if le.IsWrite {
		rl.WriteBytes += le.Size
		rl.WriteKeys++
	} else {
		rl.ReadBytes += le.Size
		rl.ReadKeys++
	}
}

// Load translates the recorded key accesses and size into range usage
// information.
func (rl *ReplicaLoadCounter) Load() allocator.RangeUsageInfo {
	return allocator.RangeUsageInfo{
		LogicalBytes:     rl.WriteBytes,
		QueriesPerSecond: float64(rl.WriteKeys + rl.ReadKeys),
		WritesPerSecond:  float64(rl.WriteKeys),
	}
}

// Capacity returns the store capacity for the store with id storeid. it
// aggregates the load from each replica within the store.
func Capacity(state State, storeID StoreID) roachpb.StoreCapacity {
	// TODO(kvoli,lidorcarmel): Store capacity will need to be populated with
	// the following missing fields: capacity, available, used, l0sublevels,
	// bytesperreplica, writesperreplica.
	capacity := roachpb.StoreCapacity{}
	store, ok := state.Store(storeID)
	if !ok {
		return capacity
	}

	for rangeID, replicaID := range store.Replicas() {
		rng, _ := state.Range(rangeID)
		if rng.Leaseholder() == replicaID {
			// TODO(kvoli): We currently only consider load on the leaseholder
			// replica for a range. The other replicas have an estimate that is
			// calculated within the allocation algorithm.
			usage := state.UsageInfo(rng.RangeID())
			capacity.QueriesPerSecond += usage.QueriesPerSecond
			capacity.WritesPerSecond += usage.WritesPerSecond
			capacity.LogicalBytes += usage.LogicalBytes

			capacity.LeaseCount++
		}

		capacity.RangeCount++
	}
	return capacity
}
