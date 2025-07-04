// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"fmt"

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
	for _, replica := range s.Replicas(storeID) {
		if !replica.HoldsLease() {
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
		foundSelf := false
		for _, r := range simReplicas {
			rs := mma.StoreIDAndReplicaState{
				StoreID: roachpb.StoreID(r.StoreID()),
				ReplicaState: mma.ReplicaState{
					ReplicaIDAndType: mma.ReplicaIDAndType{
						ReplicaID: roachpb.ReplicaID(r.ReplicaID()),
						ReplicaType: mma.ReplicaType{
							ReplicaType:   r.Descriptor().Type,
							IsLeaseholder: rng.Leaseholder() == r.ReplicaID(),
						},
					},
				},
			}
			if rs.StoreID == roachpb.StoreID(storeID) {
				if !rs.IsLeaseholder {
					panic(fmt.Sprintf(
						"simulator state inconsistent for r%d when constructing "+
							"leaseholder msg for s%d: local store is not leaseholder",
						replica.Range(), storeID))
				}
			}
			if rs.IsLeaseholder {
				if rs.StoreID != roachpb.StoreID(storeID) {
					panic(fmt.Sprintf(
						"simulator state inconsistent for r%d when constructing leaseholder "+
							"msg for s%d: remote store s%d is leaseholder",
						replica.Range(), storeID, rs.StoreID))
				}
				foundSelf = true
			}
			replicas = append(replicas, rs)
		}
		if !foundSelf {
			panic(fmt.Sprintf("simulator state inconsistent for r%d when constructing leaseholder "+
				"msg for s%d: did not find itself in the set of replicas", replica.Range(), storeID))
		}

		var rl mma.RangeLoad
		load := s.RangeUsageInfo(rng.RangeID(), replica.StoreID())
		rl.Load[mma.WriteBandwidth] = mma.LoadValue(load.WriteBytesPerSecond)
		rl.Load[mma.ByteSize] = mma.LoadValue(load.LogicalBytes)
		rl.Load[mma.CPURate] = mma.LoadValue(load.RaftCPUNanosPerSecond + load.RequestCPUNanosPerSecond)
		rl.RaftCPU = mma.LoadValue(load.RaftCPUNanosPerSecond)

		rangeMessages = append(rangeMessages, mma.RangeMsg{
			RangeID:   roachpb.RangeID(replica.Range()),
			Populated: true,
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
