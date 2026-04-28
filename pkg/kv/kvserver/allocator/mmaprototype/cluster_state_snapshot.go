// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype/mmasnappb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Snapshot returns a structured proto representation of cs suitable for
// diagnostics and offline analysis. The returned proto can be marshaled to
// proto binary, jsonpb, or text.
//
// The snapshot is not a complete round-trip representation of clusterState:
// scratch/workspace fields, runtime injections (clocks, interface back-refs),
// and pure caches are intentionally omitted. The
// TestSnapshotCoversAllFields test enforces that every state-bearing field
// reachable from clusterState is either represented in the snapshot or has a
// recorded omission reason, so adding a new field to clusterState (or any
// owned struct it transitively references) forces a deliberate decision about
// snapshot inclusion.
func (cs *clusterState) Snapshot() *mmasnappb.ClusterStateSnapshot {
	out := &mmasnappb.ClusterStateSnapshot{
		MMAID:                   int32(cs.mmaid),
		DiskUtilRefuseThreshold: cs.diskUtilRefuseThreshold,
		DiskUtilShedThreshold:   cs.diskUtilShedThreshold,
		ChangeSeqGen:            uint64(cs.changeSeqGen),
		Nodes:                   make(map[int32]*mmasnappb.NodeSnapshot, len(cs.nodes)),
		Stores:                  make(map[int32]*mmasnappb.StoreSnapshot, len(cs.stores)),
	}
	for id, n := range cs.nodes {
		out.Nodes[int32(id)] = snapshotNode(n)
	}
	for id, s := range cs.stores {
		out.Stores[int32(id)] = snapshotStore(s, cs)
	}
	return out
}

func snapshotNode(n *nodeState) *mmasnappb.NodeSnapshot {
	stores := make([]roachpb.StoreID, len(n.stores))
	copy(stores, n.stores)
	return &mmasnappb.NodeSnapshot{
		StoreIDs: stores,
		NodeLoad: mmasnappb.NodeLoad{
			NodeID:          n.NodeLoad.NodeID,
			NodeCPULoad:     int64(n.NodeLoad.NodeCPULoad),
			NodeCPUCapacity: int64(n.NodeLoad.NodeCPUCapacity),
		},
		AdjustedCPU: int64(n.adjustedCPU),
	}
}

func snapshotStore(s *storeState, cs *clusterState) *mmasnappb.StoreSnapshot {
	return &mmasnappb.StoreSnapshot{
		Status: mmasnappb.Status{
			Health: uint32(s.status.Health),
			Disposition: mmasnappb.Disposition{
				Lease:   uint32(s.status.Disposition.Lease),
				Replica: uint32(s.status.Disposition.Replica),
			},
		},
		StoreLoad: mmasnappb.StoreLoad{
			ReportedLoad:          loadVectorToSlice(s.reportedLoad),
			Capacity:              loadVectorToSlice(s.capacity),
			ReportedSecondaryLoad: secondaryLoadVectorToSlice(s.reportedSecondaryLoad),
		},
		StoreAttributes: mmasnappb.StoreAttributes{
			StoreID:      s.storeAttributesAndLocalityWithNodeTier.StoreID,
			NodeID:       s.storeAttributesAndLocalityWithNodeTier.NodeID,
			NodeAttrs:    s.storeAttributesAndLocalityWithNodeTier.NodeAttrs,
			NodeLocality: s.storeAttributesAndLocalityWithNodeTier.NodeLocality,
			StoreAttrs:   s.storeAttributesAndLocalityWithNodeTier.StoreAttrs,
		},
		Adjusted:                   snapshotStoreAdjusted(s),
		LoadSeqNum:                 s.loadSeqNum,
		MaxFractionPendingIncrease: s.maxFractionPendingIncrease,
		MaxFractionPendingDecrease: s.maxFractionPendingDecrease,
		LocalityTiers:              []string(cs.localityTierInterner.unintern(s.localityTiers)),
		OverloadStartTime:          nullableTime(s.overloadStartTime),
		OverloadEndTime:            nullableTime(s.overloadEndTime),
	}
}

// snapshotStoreAdjusted builds the snapshot of the unnamed storeState.adjusted
// struct. It takes the enclosing storeState rather than a pointer to the
// anonymous struct itself because Go has no convenient way to reference that
// nameless type from outside its enclosing declaration.
func snapshotStoreAdjusted(s *storeState) mmasnappb.StoreAdjusted {
	pendingIDs := make([]uint64, 0, len(s.adjusted.loadPendingChanges))
	for id := range s.adjusted.loadPendingChanges {
		pendingIDs = append(pendingIDs, uint64(id))
	}
	replicas := make(map[int64]*mmasnappb.ReplicaState, len(s.adjusted.replicas))
	for rid, r := range s.adjusted.replicas {
		replicas[int64(rid)] = snapshotReplicaState(r)
	}
	topK := make(map[int32]*mmasnappb.TopKReplicas, len(s.adjusted.topKRanges))
	for sid, t := range s.adjusted.topKRanges {
		topK[int32(sid)] = snapshotTopKReplicas(t)
	}
	return mmasnappb.StoreAdjusted{
		Load:                 loadVectorToSlice(s.adjusted.load),
		SecondaryLoad:        secondaryLoadVectorToSlice(s.adjusted.secondaryLoad),
		LoadPendingChangeIds: pendingIDs,
		Replicas:             replicas,
		TopKRanges:           topK,
	}
}

func snapshotReplicaState(r ReplicaState) *mmasnappb.ReplicaState {
	return &mmasnappb.ReplicaState{
		ReplicaID:        r.ReplicaIDAndType.ReplicaID,
		ReplicaType:      r.ReplicaIDAndType.ReplicaType.ReplicaType,
		IsLeaseholder:    r.ReplicaIDAndType.ReplicaType.IsLeaseholder,
		LeaseDisposition: uint32(r.LeaseDisposition),
	}
}

func snapshotTopKReplicas(t *topKReplicas) *mmasnappb.TopKReplicas {
	out := &mmasnappb.TopKReplicas{
		K:         int32(t.k),
		Dim:       uint32(t.dim),
		Threshold: int64(t.threshold),
		Replicas:  make([]mmasnappb.ReplicaLoad, len(t.replicas)),
	}
	for i, rl := range t.replicas {
		out.Replicas[i] = mmasnappb.ReplicaLoad{
			RangeID: rl.RangeID,
			Load:    int64(rl.load),
		}
	}
	return out
}

// nullableTime returns nil if t is the zero time.Time, else &t. Used to
// populate stdtime nullable=true proto fields so zero timestamps are omitted
// on the wire.
func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func loadVectorToSlice(lv LoadVector) []int64 {
	out := make([]int64, NumLoadDimensions)
	for i, v := range lv {
		out[i] = int64(v)
	}
	return out
}

func secondaryLoadVectorToSlice(lv SecondaryLoadVector) []int64 {
	out := make([]int64, NumSecondaryLoadDimensions)
	for i, v := range lv {
		out[i] = int64(v)
	}
	return out
}
