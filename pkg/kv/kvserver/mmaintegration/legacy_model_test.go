// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// computeStoreCPURateCapacityNaive is the original (naive) CPU capacity model,
// retained here only for comparison with the capped multiplier approach
// (computeCPUCapacityWithCap).
//
// The model ensures that the mean store utilization (sum of store loads / sum of
// store capacities) equals the node-level CPU utilization, so that overload is
// detected at the store level exactly when it is detected at the node level.
// Individual stores may have different utilizations depending on their load.
//
// The two cases are:
//
//  1. Normal: storesCPURate > 0, cpuUtil >= 0.01.
//     We compute cpuUtil = nodeCPURateUsage / nodeCPURateCapacity, then
//     derive a per-store capacity = (storesCPURate / cpuUtil) / numStores.
//     See MakeStoreLoadMsg for why this construction preserves the
//     node-level utilization as the mean store-level utilization.
//
//  2. Low utilization fallback: storesCPURate is zero or cpuUtil < 0.01.
//     We assume 50% of the node capacity is attributable to stores and
//     split evenly.
//
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
//	cpuUtil = NodeCPURateUsage/NodeCPURateCapacity
//
// we want to define StoresCPURateCapacity such that
//
//	StoresCPURate/StoresCPURateCapacity = cpuUtil,
//
// i.e. we want to give the (collective) stores a capacity that results
// in the same utilization as reported at the process (node) level, i.e.
//
//	StoresCPURateCapacity = StoresCPURate/cpuUtil.
//
// Finally, we split StoresCPURateCapacity evenly across the stores.
//
// This construction ensures that there is overload as measured by node
// CPU usage exactly when there is overload as measured by mean store
// CPU usage:
//
//	meanCPUUtil = sum_i StoreCPURate_i / sum_i StoreCPURateCapacity_i
//	            = 1/StoresCPURateCapacity * sum_i StoreCPURate_i
//	            = StoresCPURate / StoresCPURateCapacity
//	            = cpuUtil
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
//
// Panics if numStores <= 0 or nodeCPURateCapacity <= 0.
func computeStoreCPURateCapacityNaive(in storeCPURateCapacityInput) mmaprototype.LoadValue {
	if in.numStores <= 0 || in.nodeCPURateCapacity <= 0 {
		log.KvDistribution.Fatalf(context.Background(), "numStores and nodeCPURateCapacity must be > 0")
	}
	cpuUtil := in.nodeCPURateUsage / in.nodeCPURateCapacity
	// cpuUtil can be zero or close to zero.
	almostZeroUtil := cpuUtil < 0.01
	if in.storesCPURate == 0 || almostZeroUtil {
		// almostZeroUtil or StoresCPURate is zero. We assume that only 50% of
		// the usage can be accounted for in StoresCPURate, so we divide 50% of
		// the NodeCPURateCapacity among all the stores.
		return mmaprototype.LoadValue(in.nodeCPURateCapacity / 2 / float64(in.numStores))
	}
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
	nodeCapacity := in.storesCPURate / cpuUtil
	storeCapacity := nodeCapacity / float64(in.numStores)
	return mmaprototype.LoadValue(storeCapacity)
}

// computeStoreByteSizeCapacityNaive uses (Total-Available)/Total as the
// disk fraction instead of Used/(Used+Available). This is the naive model
// because Total-Available includes filesystem reserved blocks, ballast, and
// other non-store files on the same mount — all of which inflate the fraction
// and produce an unrealistically small capacity for stores with little data.
// Retained here only for comparison with the correct model
// (computeStoreByteSizeCapacity).
func computeStoreByteSizeCapacityNaive(
	logicalBytes mmaprototype.LoadValue, total int64, available int64,
) mmaprototype.LoadValue {
	var fullDiskFraction float64
	if total > 0 {
		fullDiskFraction = float64(total-available) / float64(total)
	}
	return computeStoreByteSizeCapacity(logicalBytes, fullDiskFraction, available)
}
