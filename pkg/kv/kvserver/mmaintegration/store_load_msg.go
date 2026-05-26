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

// MakeStoreLoadMsg constructs a StoreLoadMsg with load and capacity expressed
// in physical units. MMA receives only this message for aggregate store state;
// amplification factors for per-range loads are obtained separately via
// ComputeAmplificationFactors.
func MakeStoreLoadMsg(
	desc roachpb.StoreDescriptor, origTimestampNanos int64,
) mmaprototype.StoreLoadMsg {
	var load, capacity mmaprototype.LoadVector

	ps := computePhysicalStore(desc)
	load = ps.load
	capacity = ps.capacity

	var secondaryLoad mmaprototype.SecondaryLoadVector
	secondaryLoad[mmaprototype.LeaseCount] = mmaprototype.LoadValue(desc.Capacity.LeaseCount)
	secondaryLoad[mmaprototype.ReplicaCount] = mmaprototype.LoadValue(desc.Capacity.RangeCount)

	return mmaprototype.StoreLoadMsg{
		NodeID:          desc.Node.NodeID,
		StoreID:         desc.StoreID,
		Load:            load,
		Capacity:        capacity,
		SecondaryLoad:   secondaryLoad,
		NodeCPULoad:     mmaprototype.LoadValue(desc.NodeCapacity.NodeCPURateUsage),
		NodeCPUCapacity: mmaprototype.LoadValue(desc.NodeCapacity.NodeCPURateCapacity),
		LoadTime:        timeutil.FromUnixNanos(origTimestampNanos),
	}
}
