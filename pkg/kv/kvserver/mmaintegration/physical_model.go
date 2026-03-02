// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// physicalStore holds the physical load, capacity, and amplification
// factors for a store across all load dimensions. Each vector is indexed by
// mmaprototype.LoadDimension.
type physicalStore struct {
	// load is the per-store physical load in each dimension's native unit
	// (ns/s for CPURate, bytes/s for WriteBandwidth, bytes for ByteSize).
	load mmaprototype.LoadVector
	// capacity is the per-store physical capacity in each dimension's native
	// unit.
	capacity mmaprototype.LoadVector
	// amplificationFactors converts logical per-range loads to physical units.
	amplificationFactors mmaprototype.AmpVector
}

// physicalDimension holds the outputs of the physical model for a single
// load dimension.
type physicalDimension struct {
	load                mmaprototype.LoadValue
	capacity            mmaprototype.LoadValue
	amplificationFactor mmaprototype.Amp
}

// computePhysicalCPU computes per-store physical CPU load, capacity, and
// amplification factor from the node capacity in the store descriptor.
//
// All outputs are in physical CPU units (ns/s). The key idea is that MMA
// should only see load it can redistribute by moving ranges. Background CPU
// (SQL gateway work, GC, background jobs) is immovable and must be excluded
// from MMA's view of both load and capacity.
//
// The model uses a capped-multiplier approach to separate MMA-attributed CPU
// from background:
//
//  1. ampFactor = clamp(NodeCPURateUsage / StoresCPURate, 1, cpuIndirectOverheadMultiplier)
//     Each unit of direct KV CPU (StoresCPURate) is assumed to cause up to
//     ampFactor units of total CPU (including indirect overhead like RPC
//     handling, compactions, DistSQL). The cap prevents pathological behavior
//     when StoresCPURate is small relative to total node usage.
//
//  2. mmaAttributedLoad = StoresCPURate * ampFactor
//     The portion of node CPU usage attributed to MMA-tracked work (both
//     direct replica CPU and its indirect overhead).
//
//  3. backgroundLoad = max(0, NodeCPURateUsage - mmaAttributedLoad)
//     CPU usage not attributed to MMA: SQL gateway work, Go GC, background
//     jobs, etc. This is zero when the implicit multiplier is below the cap.
//
//  4. load = mmaAttributedLoad / numStores
//     Per-store MMA-attributed physical CPU. This is what MMA can control.
//
//  5. capacity = max(0, NodeCPURateCapacity - backgroundLoad) / numStores
//     Per-store capacity available for MMA work, after reserving headroom
//     for background CPU.
//
// When storesCPURate is 0 (no replicas reporting CPU yet), ampFactor defaults
// to the cap. This makes mmaAttributedLoad = 0 and all node CPU usage is
// treated as background.
//
// Load and capacity are split evenly across stores. A proportional
// distribution (weighted by each store's CPUPerSecond) would be more precise
// for multi-store; even splitting matches how we split capacity.
// TODO(mma): consider using desc.Capacity.CPUPerSecond to weight the
// per-store share instead of splitting evenly.
func computePhysicalCPU(desc roachpb.StoreDescriptor) (res physicalDimension) {
	defer func() { res.capacity = max(minCapacity, res.capacity) }()

	nc := desc.NodeCapacity
	numStores := max(1, float64(nc.NumStores))
	nodeUsage := float64(nc.NodeCPURateUsage)
	nodeCap := float64(nc.NodeCPURateCapacity)
	storesCPU := float64(nc.StoresCPURate)

	// Step 1: Estimate the amplification factor.
	ampFactor := cpuIndirectOverheadMultiplier
	if storesCPU > 0 {
		ampFactor = max(1, min(nodeUsage/storesCPU, cpuIndirectOverheadMultiplier))
	}

	// Step 2: Attribute CPU to MMA-tracked work vs background.
	mmaLoad := storesCPU * ampFactor
	background := max(0, nodeUsage-mmaLoad)

	// Step 3: Compute per-store load and capacity.
	// Load uses this store's direct replica CPU (CPUPerSecond) rather than
	// splitting the node total evenly, so stores with more KV work report
	// higher load. Falls back to even split when CPUPerSecond is unavailable.
	storeCPU := desc.Capacity.CPUPerSecond
	if storeCPU <= 0 {
		storeCPU = storesCPU / numStores
	}
	load := mmaprototype.LoadValue(max(0, storeCPU*ampFactor))
	// Capacity is split evenly (not weighted by store load). CPU cores are
	// a shared node-level resource, so each store has equal access. Weighting
	// capacity by storeFraction would cause storeCPU to cancel in the
	// utilization ratio (load/capacity), making every store on the same node
	// show identical utilization regardless of workload imbalance. Even split
	// preserves per-store differentiation: a hot store shows high utilization
	// (shed candidate) while a cold store shows low utilization (receive
	// candidate). The node-level overload check in canShedAndAddLoad prevents
	// over-accepting on an already-loaded node.
	capacity := mmaprototype.LoadValue(max(0, (nodeCap-background)/numStores))
	if nodeCap <= 0 {
		capacity = load * 2
	}

	return physicalDimension{
		load:                load,
		capacity:            capacity,
		amplificationFactor: mmaprototype.Amp(ampFactor),
	}
}

// minCapacity is the floor for per-store physical capacity in any dimension.
// This prevents zero-capacity values that could arise during early node startup
// or on empty stores.
const minCapacity mmaprototype.LoadValue = 1.0

// maxDiskSpaceAmplification caps the ratio of physical disk bytes used to
// logical (MVCC) bytes. Values above this are treated as if the extra physical
// usage is independent of range data (e.g. WAL, auxiliary files, tombstone
// bloat). Without this cap, transient LSM bloat could produce extreme
// amplification factors that massively overcount per-range physical footprint.
const maxDiskSpaceAmplification = 5.0

// computePhysicalDisk computes physical disk load, capacity, and space
// amplification factor from the store capacity in the store descriptor.
//
// Both load and capacity are in physical bytes so that load/capacity =
// Used/(Used+Available) = actual disk utilization. The amplification factor
// is Used/LogicalBytes, capped at maxDiskSpaceAmplification. Values below 1.0
// (from compression) are preserved. Defaults to 1.0 for empty/new stores.
func computePhysicalDisk(desc roachpb.StoreDescriptor) (res physicalDimension) {
	defer func() { res.capacity = max(minCapacity, res.capacity) }()

	sc := desc.Capacity
	var ampFactor float64
	if sc.LogicalBytes > 0 && sc.Used > 0 {
		ampFactor = min(float64(sc.Used)/float64(sc.LogicalBytes), maxDiskSpaceAmplification)
	} else {
		ampFactor = 1.0
	}
	return physicalDimension{
		load:                mmaprototype.LoadValue(sc.Used),
		capacity:            mmaprototype.LoadValue(sc.Used + sc.Available),
		amplificationFactor: mmaprototype.Amp(ampFactor),
	}
}

// computePhysicalWriteBandwidth computes physical write-bandwidth load from the
// store descriptor.
//
// WriteBytesPerSecond is the sum of per-replica logical write bytes/s, not the
// actual physical disk write throughput. The amplification factor is set to 1.0
// because we don't have a measure of physical disk writes to compare against,
// so there is no meaningful ratio to compute. Capacity is unknown for the same
// reason: disk write throughput limits depend on hardware and workload
// characteristics that aren't available in the store descriptor.
func computePhysicalWriteBandwidth(desc roachpb.StoreDescriptor) physicalDimension {
	return physicalDimension{
		load:                mmaprototype.LoadValue(desc.Capacity.WriteBytesPerSecond),
		capacity:            mmaprototype.UnknownCapacity,
		amplificationFactor: mmaprototype.Amp(1.0),
	}
}

// computePhysicalStore computes the physical load, capacity, and amplification
// factors for a store across all load dimensions given its descriptor.
//
// Design note: MakeStoreLoadMsg and ComputeAmplificationFactors both consume a
// StoreDescriptor but obtain it through different paths. MakeStoreLoadMsg
// receives the descriptor from a gossip callback (the snapshot the store most
// recently published), while ComputeAmplificationFactors is called from
// mmaStore.amplificationFactors which constructs a descriptor from separately
// cached local values (CachedCapacity and GetNodeCapacity). Ideally both would
// use the exact same descriptor snapshot, but in practice the two may be from
// slightly different points in time. This is acceptable because:
//  1. The underlying inputs (node CPU EWMA, space amplification) are
//     slow-moving; the drift between two successive reads is negligible.
//  2. MMA already tolerates the store-level load not equaling the sum of its
//     per-range loads: not all ranges report load every cycle, follower
//     replicas contribute CPU to the store total but don't report range-level
//     load, and ranges may join or leave between snapshots. A small drift in
//     amplification factors is negligible compared to these existing gaps.
//
// If tighter consistency is ever needed, the result can be returned as a
// byproduct of MakeStoreLoadMsg and cached alongside the StoreLoadMsg.
func computePhysicalStore(desc roachpb.StoreDescriptor) (res physicalStore) {
	res.setDimension(mmaprototype.CPURate, computePhysicalCPU(desc))
	res.setDimension(mmaprototype.ByteSize, computePhysicalDisk(desc))
	res.setDimension(mmaprototype.WriteBandwidth, computePhysicalWriteBandwidth(desc))
	return res
}

func (r *physicalStore) setDimension(dim mmaprototype.LoadDimension, d physicalDimension) {
	r.load[dim] = d.load
	r.capacity[dim] = d.capacity
	r.amplificationFactors[dim] = d.amplificationFactor
}

// ComputeAmplificationFactors returns the per-dimension amplification factors
// for a store, given its descriptor. These factors convert logical per-range
// loads (direct replica CPU, MVCC bytes) into physical units for use at the
// MMA integration boundary.
func ComputeAmplificationFactors(desc roachpb.StoreDescriptor) mmaprototype.AmpVector {
	return computePhysicalStore(desc).amplificationFactors
}

// MakePhysicalRangeLoad converts logical per-range load measurements into a
// physical RangeLoad by applying the amplification factors. This is the single
// entry point for all logical-to-physical range load conversion and should be
// called at the integration boundary before passing range loads to MMA.
func MakePhysicalRangeLoad(
	requestCPUNanos, raftCPUNanos, writeBytesPerSec float64,
	logicalBytes int64,
	amp mmaprototype.AmpVector,
) mmaprototype.RangeLoad {
	var rl mmaprototype.RangeLoad
	cpuNanos := requestCPUNanos + raftCPUNanos
	rl.Load[mmaprototype.CPURate] = mmaprototype.LoadValue(float64(cpuNanos) * float64(amp[mmaprototype.CPURate]))
	rl.RaftCPU = mmaprototype.LoadValue(float64(raftCPUNanos) * float64(amp[mmaprototype.CPURate]))
	rl.Load[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(float64(writeBytesPerSec) * float64(amp[mmaprototype.WriteBandwidth]))
	rl.Load[mmaprototype.ByteSize] = mmaprototype.LoadValue(float64(logicalBytes) * float64(amp[mmaprototype.ByteSize]))
	return rl
}
