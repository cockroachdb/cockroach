// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package allocator

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mma"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func MakeStoreLoadMsg(desc roachpb.StoreDescriptor, origTimestampNanos int64) mma.StoreLoadMsg {
	var load, capacity mma.LoadVector
	load[mma.CPURate] = mma.LoadValue(desc.Capacity.CPUPerSecond)
	if desc.NodeCapacity.NodeCPURateCapacity > 0 {
		cpuUtil :=
			float64(desc.NodeCapacity.NodeCPURateUsage) / float64(desc.NodeCapacity.NodeCPURateCapacity)
		// cpuUtil can be zero or close to zero.
		almostZeroUtil := cpuUtil < 0.01
		if desc.NodeCapacity.StoresCPURate != 0 && !almostZeroUtil {
			nodeCapacity := float64(desc.NodeCapacity.StoresCPURate) / cpuUtil
			storeCapacity := nodeCapacity / float64(desc.NodeCapacity.NumStores)
			capacity[mma.CPURate] = mma.LoadValue(storeCapacity)
		} else {
			// almostZeroUtil or StoresCPURate is zero. We assume that only 50% of
			// the usage can be accounted for in StoresCPURate, so we divide 50% of
			// the NodeCPURateCapacity among all the stores.
			capacity[mma.CPURate] = mma.LoadValue(
				float64(desc.NodeCapacity.NodeCPURateCapacity/2) / float64(desc.NodeCapacity.NumStores))
		}
	} else {
		// TODO(sumeer): remove this hack of defaulting to 50% utilization, since
		// NodeCPURateCapacity should never be 0.
		capacity[mma.CPURate] = load[mma.CPURate] * 2
	}
	load[mma.WriteBandwidth] = mma.LoadValue(desc.Capacity.WriteBytesPerSecond)
	capacity[mma.WriteBandwidth] = mma.UnknownCapacity
	// ByteSize is based on LogicalBytes since that is how we measure the size
	// of each range
	load[mma.ByteSize] = mma.LoadValue(desc.Capacity.LogicalBytes)
	// Available does not compensate for the ballast, so utilization will look
	// higher than actual. This is fine since the ballast is small (default is
	// 1% of capacity) and is for use in an emergency.
	byteSizeUtil :=
		float64(desc.Capacity.Capacity-desc.Capacity.Available) / float64(desc.Capacity.Capacity)
	almostZeroUtil := byteSizeUtil < 0.01
	if load[mma.ByteSize] != 0 && !almostZeroUtil {
		// Normal case. The store has some ranges, and is not almost empty.
		capacity[mma.ByteSize] = mma.LoadValue(float64(load[mma.ByteSize]) / byteSizeUtil)
	} else {
		// Has no ranges or is almost empty. This is likely a new store. Since
		// LogicalBytes are uncompressed, we start with the compressed available,
		// which is desirably pessimistic.
		capacity[mma.ByteSize] = mma.LoadValue(desc.Capacity.Available)
	}
	var secondaryLoad mma.SecondaryLoadVector
	secondaryLoad[mma.LeaseCount] = mma.LoadValue(desc.Capacity.LeaseCount)
	return mma.StoreLoadMsg{
		NodeID:        desc.Node.NodeID,
		StoreID:       desc.StoreID,
		Load:          load,
		Capacity:      capacity,
		SecondaryLoad: secondaryLoad,
		LoadTime:      timeutil.FromUnixNanos(origTimestampNanos),
	}
}
