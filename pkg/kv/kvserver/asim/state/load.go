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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ReplicaLoad defines the methods a datastructure is required to perform in
// order to record and report load events.
type ReplicaLoad interface {
	// ApplyLoad applies a load event to a replica.
	ApplyLoad(le workload.LoadEvent)
	// Load translates the recorded load events into usage information of the
	// replica.
	Load() allocator.RangeUsageInfo
	// Split halves the load of the ReplicaLoad this method is called on and
	// assigns the other half to a new ReplicaLoad that is returned i.e. 50/50.
	Split() ReplicaLoad
	// ResetLoad resets the load of the ReplicaLoad. This only affects rated
	// counters.
	ResetLoad()
}

// LoadEventQPS returns the QPS for a given workload event.
func LoadEventQPS(le workload.LoadEvent) float64 {
	return float64(le.Reads) + float64(le.Writes)
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
	clock      *ManualSimClock
	loadStats  *load.ReplicaLoad
}

// NewReplicaLoadCounter returns a new replica load counter.
func NewReplicaLoadCounter(clock *ManualSimClock) *ReplicaLoadCounter {
	return &ReplicaLoadCounter{
		clock:     clock,
		loadStats: load.NewReplicaLoad(hlc.NewClock(clock, 0), nil),
	}
}

// ApplyLoad applies a load event onto a replica load counter.
func (rl *ReplicaLoadCounter) ApplyLoad(le workload.LoadEvent) {
	rl.ReadBytes += le.ReadSize
	rl.ReadKeys += le.Reads
	rl.WriteBytes += le.WriteSize
	rl.WriteKeys += le.Writes

	rl.loadStats.RecordBatchRequests(LoadEventQPS(le), 0)
	rl.loadStats.RecordRequests(LoadEventQPS(le))
	rl.loadStats.RecordReadKeys(float64(le.Reads))
	rl.loadStats.RecordReadBytes(float64(le.ReadSize))
	rl.loadStats.RecordWriteKeys(float64(le.Writes))
	rl.loadStats.RecordWriteBytes(float64(le.WriteSize))
}

// Load translates the recorded key accesses and size into range usage
// information.
func (rl *ReplicaLoadCounter) Load() allocator.RangeUsageInfo {
	stats := rl.loadStats.Stats()

	return allocator.RangeUsageInfo{
		LogicalBytes:     rl.WriteBytes,
		QueriesPerSecond: stats.QueriesPerSecond,
		WritesPerSecond:  float64(rl.WriteKeys),
	}
}

// ResetLoad resets the load of the ReplicaLoad. This only affects rated
// counters.
func (rl *ReplicaLoadCounter) ResetLoad() {
	rl.loadStats.Reset()
}

// Split halves the load of the ReplicaLoad this method is called on and
// assigns the other half to a new ReplicaLoad that is returned i.e. 50/50.
func (rl *ReplicaLoadCounter) Split() ReplicaLoad {
	rl.WriteKeys /= 2
	rl.WriteBytes /= 2
	rl.ReadKeys /= 2
	rl.ReadBytes /= 2

	otherLoadStats := load.NewReplicaLoad(hlc.NewClock(rl.clock, 0), nil)
	rl.loadStats.Split(otherLoadStats)

	return &ReplicaLoadCounter{
		WriteKeys:  rl.WriteKeys,
		WriteBytes: rl.WriteBytes,
		ReadKeys:   rl.ReadKeys,
		ReadBytes:  rl.ReadBytes,
		clock:      rl.clock,
		loadStats:  otherLoadStats,
	}
}

// Capacity returns the store capacity for the store with id storeID. It
// aggregates the load from each replica within the store.
func Capacity(state State, storeID StoreID) roachpb.StoreCapacity {
	// TODO(kvoli,lidorcarmel): Store capacity will need to be populated with
	// the following missing fields: capacity, available, used, l0sublevels,
	// bytesperreplica, writesperreplica.
	capacity := roachpb.StoreCapacity{}

	for _, repl := range state.Replicas(storeID) {
		rangeID := repl.Range()
		replicaID := repl.ReplicaID()
		rng, _ := state.Range(rangeID)
		if rng.Leaseholder() == replicaID {
			// TODO(kvoli): We currently only consider load on the leaseholder
			// replica for a range. The other replicas have an estimate that is
			// calculated within the allocation algorithm. Adapt this to
			// support follower reads, when added to the workload generator.
			usage := state.ReplicaLoad(rng.RangeID(), storeID).Load()
			capacity.QueriesPerSecond += usage.QueriesPerSecond
			capacity.WritesPerSecond += usage.WritesPerSecond
			capacity.LogicalBytes += usage.LogicalBytes
			capacity.LeaseCount++
		}

		capacity.RangeCount++
	}
	return capacity
}

// StoreUsageInfo contains the load on a single store.
type StoreUsageInfo struct {
	WriteKeys  int64
	WriteBytes int64
	ReadKeys   int64
	ReadBytes  int64
}

// ClusterUsageInfo contains the load and state of the cluster. Using this we
// can answer questions such as how balanced the load is, and how much data got
// rebalanced.
type ClusterUsageInfo struct {
	LeaseTransfers  int64
	Rebalances      int64
	BytesRebalanced int64
	StoreUsage      map[StoreID]*StoreUsageInfo
}

func newClusterUsageInfo() *ClusterUsageInfo {
	return &ClusterUsageInfo{
		StoreUsage: make(map[StoreID]*StoreUsageInfo),
	}
}

// ApplyLoad applies the load event on the right stores.
func (u *ClusterUsageInfo) ApplyLoad(r *rng, le workload.LoadEvent) {
	for _, rep := range r.replicas {
		s, ok := u.StoreUsage[rep.storeID]
		if !ok {
			// First time we see this store ID, add it.
			s = &StoreUsageInfo{}
			u.StoreUsage[rep.storeID] = s
		}
		// Writes are added to all replicas, reads are added to the leaseholder
		// only.
		// Note that the accounting here is different from ReplicaLoadCounter above:
		// here we try to track the actual load on the store, regardless of the
		// allocator implementation details, and ReplicaLoadCounter tries to follow
		// the logic of the production code were, for example, read QPS is applied
		// to all replicas.
		s.WriteBytes += le.WriteSize
		s.WriteKeys += le.Writes
		if rep.holdsLease {
			s.ReadBytes += le.ReadSize
			s.ReadKeys += le.Reads
		}
	}
}
