// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
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
		loadStats: load.NewReplicaLoad(hlc.NewClockForTesting(clock), nil),
	}
}

// ApplyLoad applies a load event onto a replica load counter.
func (rl *ReplicaLoadCounter) ApplyLoad(le workload.LoadEvent) {
	rl.ReadBytes += le.ReadSize
	rl.ReadKeys += le.Reads
	rl.WriteBytes += le.WriteSize
	rl.WriteKeys += le.Writes

	rl.loadStats.RecordBatchRequests(LoadEventQPS(le), 0)
	// TODO(kvoli): Recording the load on every load counter is horribly
	// inefficient at the moment. It multiplies the time taken per test almost
	// linearly by the number of load stats counters we bump. The other load
	// stats are not used currently, re-enable them when perf is fixed and they
	// are used.
}

// Load translates the recorded key accesses and size into range usage
// information.
func (rl *ReplicaLoadCounter) Load() allocator.RangeUsageInfo {
	stats := rl.loadStats.Stats()

	return allocator.RangeUsageInfo{
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

	otherLoadStats := load.NewReplicaLoad(hlc.NewClockForTesting(rl.clock), nil)
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

// capacityOverrideSentinel is used to signal that the override value has not
// been set for a field.
const capacityOverrideSentinel = -1

// CapacityOverride is used to override some field(s) of a store's capacity.
type CapacityOverride roachpb.StoreCapacity

// NewCapacityOverride returns a capacity override where no overrides are set.
func NewCapacityOverride() CapacityOverride {
	return CapacityOverride{
		Capacity:         capacityOverrideSentinel,
		Available:        capacityOverrideSentinel,
		Used:             capacityOverrideSentinel,
		LogicalBytes:     capacityOverrideSentinel,
		RangeCount:       capacityOverrideSentinel,
		LeaseCount:       capacityOverrideSentinel,
		QueriesPerSecond: capacityOverrideSentinel,
		WritesPerSecond:  capacityOverrideSentinel,
		CPUPerSecond:     capacityOverrideSentinel,
		IOThresholdMax: admissionpb.IOThreshold{
			L0NumSubLevels:           capacityOverrideSentinel,
			L0NumSubLevelsThreshold:  capacityOverrideSentinel,
			L0NumFiles:               capacityOverrideSentinel,
			L0NumFilesThreshold:      capacityOverrideSentinel,
			L0Size:                   capacityOverrideSentinel,
			L0MinimumSizePerSubLevel: capacityOverrideSentinel,
		},
	}
}

func (co CapacityOverride) String() string {
	return fmt.Sprintf(
		"capacity=%d, available=%d, used=%d, logical_bytes=%d, range_count=%d, lease_count=%d, "+
			"queries_per_sec=%.2f, writes_per_sec=%.2f, cpu_per_sec=%.2f, io_threshold_max=%v",
		co.Capacity,
		co.Available,
		co.Used,
		co.LogicalBytes,
		co.RangeCount,
		co.LeaseCount,
		co.QueriesPerSecond,
		co.WritesPerSecond,
		co.CPUPerSecond,
		co.IOThresholdMax,
	)
}

func mergeOverride(
	capacity roachpb.StoreCapacity, override CapacityOverride,
) roachpb.StoreCapacity {
	ret := capacity
	if override.Capacity != capacityOverrideSentinel {
		ret.Capacity = override.Capacity
	}
	if override.Available != capacityOverrideSentinel {
		ret.Available = override.Available
	}
	if override.Used != capacityOverrideSentinel {
		ret.Used = override.Used
	}
	if override.LogicalBytes != capacityOverrideSentinel {
		ret.LogicalBytes = override.LogicalBytes
	}
	if override.RangeCount != capacityOverrideSentinel {
		ret.RangeCount = override.RangeCount
	}
	if override.LeaseCount != capacityOverrideSentinel {
		ret.LeaseCount = override.LeaseCount
	}
	if override.QueriesPerSecond != capacityOverrideSentinel {
		ret.QueriesPerSecond = override.QueriesPerSecond
	}
	if override.WritesPerSecond != capacityOverrideSentinel {
		ret.WritesPerSecond = override.WritesPerSecond
	}
	if override.CPUPerSecond != capacityOverrideSentinel {
		ret.CPUPerSecond = override.CPUPerSecond
	}
	if override.IOThresholdMax.L0NumFiles != capacityOverrideSentinel {
		ret.IOThresholdMax.L0NumFiles = override.IOThresholdMax.L0NumFiles
	}
	if override.IOThresholdMax.L0NumFilesThreshold != capacityOverrideSentinel {
		ret.IOThresholdMax.L0NumFilesThreshold = override.IOThresholdMax.L0NumFilesThreshold
	}
	if override.IOThreshold.L0NumSubLevels != capacityOverrideSentinel {
		ret.IOThresholdMax.L0NumSubLevels = override.IOThresholdMax.L0NumSubLevels
	}
	if override.IOThresholdMax.L0NumSubLevelsThreshold != capacityOverrideSentinel {
		ret.IOThresholdMax.L0NumSubLevelsThreshold = override.IOThresholdMax.L0NumSubLevelsThreshold
	}
	if override.IOThresholdMax.L0Size != capacityOverrideSentinel {
		ret.IOThresholdMax.L0Size = override.IOThresholdMax.L0Size
	}
	if override.IOThresholdMax.L0MinimumSizePerSubLevel != capacityOverrideSentinel {
		ret.IOThresholdMax.L0MinimumSizePerSubLevel = override.IOThresholdMax.L0MinimumSizePerSubLevel
	}
	return ret
}

// StoreUsageInfo contains the load on a single store.
type StoreUsageInfo struct {
	WriteKeys          int64
	WriteBytes         int64
	ReadKeys           int64
	ReadBytes          int64
	LeaseTransfers     int64
	Rebalances         int64
	RebalanceSentBytes int64
	RebalanceRcvdBytes int64
	RangeSplits        int64
}

// ClusterUsageInfo contains the load and state of the cluster. Using this we
// can answer questions such as how balanced the load is, and how much data got
// rebalanced.
type ClusterUsageInfo struct {
	StoreUsage map[StoreID]*StoreUsageInfo
}

func newClusterUsageInfo() *ClusterUsageInfo {
	return &ClusterUsageInfo{
		StoreUsage: make(map[StoreID]*StoreUsageInfo),
	}
}

func (u *ClusterUsageInfo) storeRef(storeID StoreID) *StoreUsageInfo {
	var s *StoreUsageInfo
	var ok bool
	if s, ok = u.StoreUsage[storeID]; !ok {
		s = &StoreUsageInfo{}
		u.StoreUsage[storeID] = s
	}
	return s
}

// ApplyLoad applies the load event on the right stores.
func (u *ClusterUsageInfo) ApplyLoad(r *rng, le workload.LoadEvent) {
	for _, rep := range r.replicas {
		s := u.storeRef(rep.storeID)
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
