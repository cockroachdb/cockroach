// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// MakeStoreLoadMsg makes a store load message. The capacity model logic is in
// computeStoreCPURateCapacity and computeStoreByteSizeCapacity.
func MakeStoreLoadMsg(
	desc roachpb.StoreDescriptor, origTimestampNanos int64,
) mmaprototype.StoreLoadMsg {
	var load, capacity mmaprototype.LoadVector

	load[mmaprototype.CPURate] = mmaprototype.LoadValue(desc.Capacity.CPUPerSecond)
	cpuCap := computeCPUCapacityWithCap(
		load[mmaprototype.CPURate],
		float64(desc.NodeCapacity.StoresCPURate),
		float64(desc.NodeCapacity.NodeCPURateUsage),
		float64(desc.NodeCapacity.NodeCPURateCapacity),
		desc.NodeCapacity.NumStores,
		func(_ float64, _ float64, _ float64, _ float64) {},
	)
	// cpuCap can be 0 when the node is overloaded (nodeCPURateUsage >
	// nodeCPURateCapacity). This is correct behavior - it signals that the
	// store has no CPU capacity available and should trigger load shedding.
	capacity[mmaprototype.CPURate] = mmaprototype.LoadValue(cpuCap)

	load[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(desc.Capacity.WriteBytesPerSecond)
	capacity[mmaprototype.WriteBandwidth] = mmaprototype.UnknownCapacity

	load[mmaprototype.ByteSize] = mmaprototype.LoadValue(desc.Capacity.LogicalBytes)
	capacity[mmaprototype.ByteSize] = computeStoreByteSizeCapacity(
		load[mmaprototype.ByteSize],
		desc.Capacity.FractionUsed(),
		desc.Capacity.Available,
	)

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
