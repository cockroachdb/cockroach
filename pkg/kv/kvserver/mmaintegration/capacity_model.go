// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"

// This file contains the capacity model functions used by MMA to derive
// per-store capacity from node-level and disk-level metrics. These are
// extracted here so they can be unit tested directly.

// computeStoreCPURateCapacity computes the CPU rate capacity for a single store
// given node-level CPU metrics. The model ensures that the per-store utilization
// (load/capacity) equals the node-level CPU utilization, so that overload is
// detected at the store level exactly when it is detected at the node level.
//
// The three cases are:
//
//  1. Normal: nodeCPURateCapacity > 0, storesCPURate > 0, cpuUtil >= 0.01.
//     We compute cpuUtil = nodeCPURateUsage / nodeCPURateCapacity, then
//     derive a per-store capacity = (storesCPURate / cpuUtil) / numStores.
//     See MakeStoreLoadMsg for why this construction preserves the
//     node-level utilization as the mean store-level utilization.
//
//  2. Low utilization fallback: nodeCPURateCapacity > 0, but storesCPURate
//     is zero or cpuUtil < 0.01. We assume 50% of the node capacity is
//     attributable to stores and split evenly.
//
//  3. Unknown capacity: nodeCPURateCapacity <= 0 (should not happen in
//     production). Falls back to assuming 50% utilization from the store's
//     own CPU usage.
func computeStoreCPURateCapacity(
	currentStoreCPUUsage mmaprototype.LoadValue,
	nodeCPURateUsage float64,
	nodeCPURateCapacity float64,
	storesCPURate float64,
	numStores int32,
) mmaprototype.LoadValue {
	if nodeCPURateCapacity > 0 {
		// CPU is a shared resource across all stores on a node, and we choose to
		// divide the CPU capacity evenly across stores on the node (any other
		// choice would be arbitrary).
		//
		// Furthermore, there are CPU consumers that we don't track on a
		// per-replica (and thus per-store) level. Examples include RPC work, Go
		// GC, SQL work etc. Some of the distributed SQL work could be happening
		// because of a local replica, so ideally we should improve our
		// instrumentation to track it per replica. Due to these gaps, we expect
		// the NodeCPURateCapacity to be higher than StoresCPURate. So simply
		// using NodeCPURateCapacity/NumStores as the per-store capacity would
		// lead to over-utilization.
		//
		// Our approach is to apply the CPU utilization to StoresCPURate to
		// compute a capacity and divide that evenly across stores. Specifically,
		//
		//   cpuUtil = NodeCPURateUsage/NodeCPURateCapacity
		//
		// we want to define StoresCPURateCapacity such that
		//
		//    StoresCPURate/StoresCPURateCapacity = cpuUtil,
		//
		// i.e. we want to give the (collective) stores a capacity that results
		// in the same utilization as reported at the process (node) level, i.e.
		//
		//    StoresCPURateCapacity = StoresCPURate/cpuUtil.
		//
		// Finally, we split StoresCPURateCapacity evenly across the stores.
		//
		// This construction ensures that there is overload as measured by node
		// CPU usage exactly when there is overload as measured by mean store
		// CPU usage:
		//
		// meanCPUUtil = sum_i StoreCPURate_i / sum_i StoreCPURateCapacity_i
		//             = 1/StoresCPURateCapacity * sum_i StoreCPURate_i
		//             = StoresCPURate / StoresCPURateCapacity
		//             = cpuUtil
		//
		// The above mathematical property is used to avoid having any explicit
		// communication of node load to MMA. The
		// NodeLoad.{ReportedCPU,CapacityCPU} is incrementally maintained as a sum
		// of the load and capacity reported by each store (at different times).
		//
		// Additionally, when the meanCPUUtil indicates overload, at least one
		// store will be above that mean, so it is overloaded as well and will
		// induce load shedding.
		//
		// It's worth noting that this construction assumes that all load on the
		// node is due to the stores. Take an extreme example in which there is
		// a single store using up 1vcpu, but the node is fully utilized (at,
		// say, 16 vcpus). In this case, the node will be at 100%, and we will
		// assign a capacity of 1 to the store, i.e. the store will also be at
		// 100% utilization despite contributing only 1/16th of the node CPU
		// utilization. The effect of the construction is that the store will
		// take on responsibility for shedding load to compensate for auxiliary
		// consumption of CPU, which is generally sensible.
		cpuUtil := nodeCPURateUsage / nodeCPURateCapacity
		// cpuUtil can be zero or close to zero.
		almostZeroUtil := cpuUtil < 0.01
		if storesCPURate != 0 && !almostZeroUtil {
			// cpuUtil is distributed across the stores, by constructing a
			// nodeCapacity, and then splitting nodeCapacity evenly across all the
			// stores. If the cpuUtil of a node is higher than the mean across nodes
			// of the cluster, then cpu util of at least one store on that node will
			// be higher than the mean across all stores in the cluster (since the
			// cpu util of a node is simply the mean across all its stores), which
			// will result in load shedding. Note that this can cause cpu util of a
			// store to be > 100% e.g. if a node is at 80% cpu util and has 10
			// stores, and all the cpu usage is due to store s1, then s1 will have
			// 800% util.
			nodeCapacity := storesCPURate / cpuUtil
			storeCapacity := nodeCapacity / float64(numStores)
			return mmaprototype.LoadValue(storeCapacity)
		}
		// almostZeroUtil or StoresCPURate is zero. We assume that only 50% of
		// the usage can be accounted for in StoresCPURate, so we divide 50% of
		// the NodeCPURateCapacity among all the stores.
		return mmaprototype.LoadValue(nodeCPURateCapacity / 2 / float64(numStores))
	}
	// TODO(sumeer): remove this hack of defaulting to 50% utilization, since
	// NodeCPURateCapacity should never be 0.
	// TODO(tbg): when do we expect to hit this branch? Mixed version cluster?
	return currentStoreCPUUsage * 2
}

// computeStoreByteSizeCapacity computes the byte size (disk) capacity for a
// store. The load is LogicalBytes (uncompressed) and the capacity is expressed
// in the same LogicalBytes-space, so that load/capacity recovers the actual
// disk utilization.
//
// The two cases are:
//
//  1. Normal: logicalBytes > 0 and diskFractionUsed >= 0.01.
//     capacity = logicalBytes / diskFractionUsed.
//     This encodes the space amplification factor (physical/logical) into
//     the capacity, so that highDiskSpaceUtilization(load, capacity, threshold)
//     recovers the real disk utilization.
//
//  2. Empty/new store: logicalBytes is 0 or diskFractionUsed < 0.01.
//     We fall back to availableBytes (compressed/physical), which is
//     pessimistic since LogicalBytes are uncompressed.
//
// diskFractionUsed should be computed via StoreCapacity.FractionUsed(), which
// prefers Used/(Available+Used) to scope utilization to the store's own data.
// Available does not compensate for the ballast, so utilization will look
// slightly higher than actual. This is acceptable since the ballast is small
// (default 1% of capacity) and is for emergency use.
func computeStoreByteSizeCapacity(
	logicalBytes mmaprototype.LoadValue, diskFractionUsed float64, availableBytes int64,
) mmaprototype.LoadValue {
	almostZeroUtil := diskFractionUsed < 0.01
	if logicalBytes != 0 && !almostZeroUtil {
		// Normal case. The store has some ranges, and is not almost empty.
		return mmaprototype.LoadValue(float64(logicalBytes) / diskFractionUsed)
	}
	// Has no ranges or is almost empty. This is likely a new store. Since
	// LogicalBytes are uncompressed, we start with the compressed available,
	// which is desirably pessimistic.
	return mmaprototype.LoadValue(availableBytes)
}
