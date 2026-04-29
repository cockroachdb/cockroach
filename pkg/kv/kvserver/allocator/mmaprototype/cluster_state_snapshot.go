// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype/mmasnappb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
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
		Ranges:                  make(map[int64]*mmasnappb.RangeSnapshot, len(cs.ranges)),
		PendingChanges:          make(map[uint64]*mmasnappb.PendingReplicaChange, len(cs.pendingChanges)),
	}
	reg := newSpanConfigRegistry()
	for id, n := range cs.nodes {
		out.Nodes[int32(id)] = snapshotNode(n)
	}
	for id, s := range cs.stores {
		out.Stores[int32(id)] = snapshotStore(s, cs)
	}
	for id, r := range cs.ranges {
		out.Ranges[int64(id)] = snapshotRange(r, cs, reg)
	}
	for id, c := range cs.pendingChanges {
		out.PendingChanges[uint64(id)] = snapshotPendingReplicaChange(c)
	}
	out.SpanConfigs = reg.byID
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
	out := snapshotReplicaStateValue(r)
	return &out
}

func snapshotReplicaStateValue(r ReplicaState) mmasnappb.ReplicaState {
	return mmasnappb.ReplicaState{
		ReplicaIDAndType: snapshotReplicaIDAndType(r.ReplicaIDAndType),
		LeaseDisposition: uint32(r.LeaseDisposition),
	}
}

func snapshotReplicaIDAndType(rit ReplicaIDAndType) mmasnappb.ReplicaIDAndType {
	return mmasnappb.ReplicaIDAndType{
		ReplicaID: rit.ReplicaID,
		ReplicaType: mmasnappb.ReplicaType{
			ReplicaType:   rit.ReplicaType.ReplicaType,
			IsLeaseholder: rit.ReplicaType.IsLeaseholder,
		},
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

func snapshotRange(
	r *rangeState, cs *clusterState, reg *spanConfigRegistry,
) *mmasnappb.RangeSnapshot {
	replicas := make([]mmasnappb.StoreIDAndReplicaState, len(r.replicas))
	for i, sr := range r.replicas {
		replicas[i] = mmasnappb.StoreIDAndReplicaState{
			StoreID:      sr.StoreID,
			ReplicaState: snapshotReplicaStateValue(sr.ReplicaState),
		}
	}
	pendingIDs := make([]uint64, len(r.pendingChanges))
	for i, pc := range r.pendingChanges {
		pendingIDs[i] = uint64(pc.changeID)
	}
	return &mmasnappb.RangeSnapshot{
		LocalRangeOwner:                    r.localRangeOwner,
		Replicas:                           replicas,
		HasNormalizationError:              r.hasNormalizationError,
		Load:                               snapshotRangeLoad(r.load),
		PendingChangeIds:                   pendingIDs,
		LastFailedChange:                   nullableTime(r.lastFailedChange),
		DiversityIncreaseLastFailedAttempt: nullableTime(r.diversityIncreaseLastFailedAttempt),
		ConfID:                             reg.intern(snapshotNormalizedSpanConfig(r.conf, cs.localityTierInterner.si)),
	}
}

// snapshotNormalizedSpanConfig un-interns conf back into the
// roachpb.SpanConfig view that MMA effectively sees. Only the fields
// normalizedSpanConfig captures (NumVoters, NumReplicas, Constraints,
// VoterConstraints, LeasePreferences) are populated; all other SpanConfig
// fields remain at their zero value, faithful to MMA's input.
//
// Returns nil if conf is nil.
func snapshotNormalizedSpanConfig(
	conf *normalizedSpanConfig, interner *stringInterner,
) *roachpb.SpanConfig {
	if conf == nil {
		return nil
	}
	out := &roachpb.SpanConfig{
		NumVoters:   conf.numVoters,
		NumReplicas: conf.numReplicas,
	}
	for _, c := range conf.constraints {
		out.Constraints = append(out.Constraints, c.unintern(interner))
	}
	for _, c := range conf.voterConstraints {
		out.VoterConstraints = append(out.VoterConstraints, c.unintern(interner))
	}
	for _, lp := range conf.leasePreferences {
		var rp roachpb.LeasePreference
		for _, c := range lp.constraints {
			rp.Constraints = append(rp.Constraints, c.unintern(interner))
		}
		out.LeasePreferences = append(out.LeasePreferences, rp)
	}
	return out
}

func snapshotRangeLoad(rl RangeLoad) mmasnappb.RangeLoad {
	return mmasnappb.RangeLoad{
		Load:    loadVectorToSlice(rl.Load),
		RaftCPU: int64(rl.RaftCPU),
	}
}

func snapshotPendingReplicaChange(c *pendingReplicaChange) *mmasnappb.PendingReplicaChange {
	return &mmasnappb.PendingReplicaChange{
		ChangeID:      uint64(c.changeID),
		Change:        snapshotReplicaChange(c.ReplicaChange),
		StartTime:     c.startTime,
		GcTime:        c.gcTime,
		EnactedAtTime: nullableTime(c.enactedAtTime),
	}
}

func snapshotReplicaChange(rc ReplicaChange) mmasnappb.ReplicaChange {
	loadDelta := loadVectorToSlice(rc.loadDelta)
	secondaryDelta := secondaryLoadVectorToSlice(rc.secondaryLoadDelta)
	return mmasnappb.ReplicaChange{
		LoadDelta:          loadDelta,
		SecondaryLoadDelta: secondaryDelta,
		Target:             rc.target,
		RangeID:            rc.rangeID,
		Prev:               snapshotReplicaStateValue(rc.prev),
		Next:               snapshotReplicaIDAndType(rc.next),
	}
}

// spanConfigRegistry deduplicates roachpb.SpanConfig values across all the
// ranges in a snapshot. Ranges with structurally identical normalized span
// configs share an id, keeping the snapshot small even when thousands of
// ranges share the same config.
//
// Lifetime is the single Snapshot call: the registry is built up while
// walking ranges and then drained into ClusterStateSnapshot.SpanConfigs.
type spanConfigRegistry struct {
	byBytes map[string]uint32
	byID    map[uint32]*roachpb.SpanConfig
}

func newSpanConfigRegistry() *spanConfigRegistry {
	return &spanConfigRegistry{
		byBytes: map[string]uint32{},
		byID:    map[uint32]*roachpb.SpanConfig{},
	}
}

// intern returns 0 if c is nil, else a 1-based id that is stable for the
// lifetime of the registry. Identical (post-marshal) span configs share an
// id.
func (r *spanConfigRegistry) intern(c *roachpb.SpanConfig) uint32 {
	if c == nil {
		return 0
	}
	b, err := protoutil.Marshal(c)
	if err != nil {
		// Marshaling a roachpb.SpanConfig built from MMA's own state should
		// never fail; treat as a programmer error.
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "marshal SpanConfig"))
	}
	if id, ok := r.byBytes[string(b)]; ok {
		return id
	}
	id := uint32(len(r.byID)) + 1
	r.byID[id] = c
	r.byBytes[string(b)] = id
	return id
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
