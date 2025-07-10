// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototypehelpers

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func MakeStoreLoadMsg(
	desc roachpb.StoreDescriptor, origTimestampNanos int64,
) mmaprototype.StoreLoadMsg {
	var load, capacity mmaprototype.LoadVector
	load[mmaprototype.CPURate] = mmaprototype.LoadValue(desc.Capacity.CPUPerSecond)
	if desc.NodeCapacity.NodeCPURateCapacity > 0 {
		// CPU is a shared resource across all stores on a node, and furthermore
		// there are consumers that we don't track on a per-replica (and thus
		// per-store) level (anything in SQL, for example). So we generally
		// expect the NodeCPURateCapacity to be higher than the sum of all
		// StoresCPURate. We do not currently have a configured per-store
		// capacity (there is no configuration setting that says "this store
		// gets at most 4 vcpus", so we need to derive a sensible store-level
		// capacity from the node-level capacity (roughly, vcpus*seconds).
		//
		// We have
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
		// mean = sum_i StoreCPURate_i / sum_i StoreCPURateCapacity_i
		//      = 1/StoresCPURateCapacity * sum_i StoreCPURate_i
		//      = StoresCPURate / StoresCPURateCapacity
		//      = cpuUtil
		//
		// and in particular, when the mean indicates overload, at least one
		// store will be above the meaning, meaning it is overloaded as well
		// and will induce load shedding.
		//
		// It's worth noting that this construction assumes that all load on the
		// node is due to the stores. Take an extreme example in which there is
		// a single store using up 1vcpu, but the node is fully utilized (at,
		// say, 16 vcpus). In this case, the node will be at 100%, and we will
		// assign a capacity of 1 to the store, i.e. the store will also be at
		// 100% utilization despite contributing only 1/16th of the node CPU
		// utilization. The effect of the construction is that the stores will
		// take on responsibility for shedding load to compensate for auxiliary
		// consumption of CPU, which is generally sensible.
		cpuUtil :=
			float64(desc.NodeCapacity.NodeCPURateUsage) / float64(desc.NodeCapacity.NodeCPURateCapacity)
		// cpuUtil can be zero or close to zero.
		almostZeroUtil := cpuUtil < 0.01
		if desc.NodeCapacity.StoresCPURate != 0 && !almostZeroUtil {
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
			nodeCapacity := float64(desc.NodeCapacity.StoresCPURate) / cpuUtil
			storeCapacity := nodeCapacity / float64(desc.NodeCapacity.NumStores)
			capacity[mmaprototype.CPURate] = mmaprototype.LoadValue(storeCapacity)
		} else {
			// almostZeroUtil or StoresCPURate is zero. We assume that only 50% of
			// the usage can be accounted for in StoresCPURate, so we divide 50% of
			// the NodeCPURateCapacity among all the stores.
			capacity[mmaprototype.CPURate] = mmaprototype.LoadValue(
				float64(desc.NodeCapacity.NodeCPURateCapacity/2) / float64(desc.NodeCapacity.NumStores))
		}
	} else {
		// TODO(sumeer): remove this hack of defaulting to 50% utilization, since
		// NodeCPURateCapacity should never be 0.
		// TODO(tbg): when do we expect to hit this branch? Mixed version cluster?
		capacity[mmaprototype.CPURate] = load[mmaprototype.CPURate] * 2
	}
	load[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(desc.Capacity.WriteBytesPerSecond)
	capacity[mmaprototype.WriteBandwidth] = mmaprototype.UnknownCapacity
	// ByteSize is based on LogicalBytes since that is how we measure the size
	// of each range
	load[mmaprototype.ByteSize] = mmaprototype.LoadValue(desc.Capacity.LogicalBytes)
	// Available does not compensate for the ballast, so utilization will look
	// higher than actual. This is fine since the ballast is small (default is
	// 1% of capacity) and is for use in an emergency.
	byteSizeUtil :=
		float64(desc.Capacity.Capacity-desc.Capacity.Available) / float64(desc.Capacity.Capacity)
	almostZeroUtil := byteSizeUtil < 0.01
	if load[mmaprototype.ByteSize] != 0 && !almostZeroUtil {
		// Normal case. The store has some ranges, and is not almost empty.
		capacity[mmaprototype.ByteSize] = mmaprototype.LoadValue(float64(load[mmaprototype.ByteSize]) / byteSizeUtil)
	} else {
		// Has no ranges or is almost empty. This is likely a new store. Since
		// LogicalBytes are uncompressed, we start with the compressed available,
		// which is desirably pessimistic.
		capacity[mmaprototype.ByteSize] = mmaprototype.LoadValue(desc.Capacity.Available)
	}
	var secondaryLoad mmaprototype.SecondaryLoadVector
	secondaryLoad[mmaprototype.LeaseCount] = mmaprototype.LoadValue(desc.Capacity.LeaseCount)
	secondaryLoad[mmaprototype.ReplicaCount] = mmaprototype.LoadValue(desc.Capacity.RangeCount)
	// TODO(tbg): this triggers early in tests, probably we're making load messages
	// before having received the first capacity. Still, this is bad, should fix.
	// or handle properly by communicating an unknown capacity.
	// if capacity[mmaprototype.CPURate] == 0 {
	// 	panic("ouch")
	// }
	return mmaprototype.StoreLoadMsg{
		NodeID:        desc.Node.NodeID,
		StoreID:       desc.StoreID,
		Load:          load,
		Capacity:      capacity,
		SecondaryLoad: secondaryLoad,
		LoadTime:      timeutil.FromUnixNanos(origTimestampNanos),
	}
}

// UsageInfoToMMALoad converts a RangeUsageInfo to a mmaprototype.RangeLoad.
func UsageInfoToMMALoad(usage allocator.RangeUsageInfo) mmaprototype.RangeLoad {
	lv := mmaprototype.LoadVector{}
	lv[mmaprototype.CPURate] = mmaprototype.LoadValue(usage.RequestCPUNanosPerSecond) + mmaprototype.LoadValue(usage.RaftCPUNanosPerSecond)
	lv[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(usage.WriteBytesPerSecond)
	lv[mmaprototype.ByteSize] = mmaprototype.LoadValue(usage.LogicalBytes)
	return mmaprototype.RangeLoad{
		Load:    lv,
		RaftCPU: mmaprototype.LoadValue(usage.RaftCPUNanosPerSecond),
	}
}

// ReplicaDescriptorToReplicaIDAndType converts a ReplicaDescriptor to a
// StoreIDAndReplicaState. The leaseholder store is passed in as lh.
func ReplicaDescriptorToReplicaIDAndType(
	desc roachpb.ReplicaDescriptor, lh roachpb.StoreID,
) mmaprototype.StoreIDAndReplicaState {
	return mmaprototype.StoreIDAndReplicaState{
		StoreID: desc.StoreID,
		ReplicaState: mmaprototype.ReplicaState{
			ReplicaIDAndType: mmaprototype.ReplicaIDAndType{
				ReplicaID: desc.ReplicaID,
				ReplicaType: mmaprototype.ReplicaType{
					ReplicaType:   desc.Type,
					IsLeaseholder: desc.StoreID == lh,
				},
			},
		},
	}
}
