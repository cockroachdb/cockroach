// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mma"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// MakeStoreLeaseholderMsgFromState creates a StoreLeaseholderMsg from the
// state of the simulator.
func MakeStoreLeaseholderMsgFromState(
	s state.State, storeID state.StoreID,
) mma.StoreLeaseholderMsg {
	var rangeMessages []mma.RangeMsg
	for _, replica := range s.Replicas(state.StoreID(storeID)) {
		if replica.HoldsLease() {
			// We only want to send messages for ranges that have a leaseholder
			// replica on this store.
			continue
		}
		rng, ok := s.Range(replica.Range())
		if !ok {
			panic("simulator state is missing a range that should exist")
		}
		simReplicas := rng.Replicas()
		replicas := make([]mma.StoreIDAndReplicaState, 0, len(simReplicas))
		for _, replica := range simReplicas {
			replicas = append(replicas, mma.StoreIDAndReplicaState{
				StoreID: roachpb.StoreID(replica.StoreID()),
				ReplicaState: mma.ReplicaState{
					ReplicaIDAndType: mma.ReplicaIDAndType{
						ReplicaID: roachpb.ReplicaID(replica.ReplicaID()),
						ReplicaType: mma.ReplicaType{
							ReplicaType:   replica.Descriptor().Type,
							IsLeaseholder: rng.Leaseholder() == replica.ReplicaID(),
						},
					},
				},
			})
		}

		var rl mma.RangeLoad
		load := s.RangeUsageInfo(rng.RangeID(), replica.StoreID())
		rl.Load[mma.WriteBandwidth] = mma.LoadValue(load.WriteBytesPerSecond)
		rl.Load[mma.ByteSize] = mma.LoadValue(load.LogicalBytes)
		rl.Load[mma.CPURate] = mma.LoadValue(load.RaftCPUNanosPerSecond + load.RequestCPUNanosPerSecond)
		rl.RaftCPU = mma.LoadValue(load.RaftCPUNanosPerSecond)

		rangeMessages = append(rangeMessages, mma.RangeMsg{
			RangeID:   roachpb.RangeID(replica.Range()),
			Replicas:  replicas,
			Conf:      *rng.SpanConfig(),
			RangeLoad: rl,
		})
	}

	return mma.StoreLeaseholderMsg{
		StoreID: roachpb.StoreID(storeID),
		Ranges:  rangeMessages,
	}
}
