// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storepool

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/redact"
)

// OverrideStorePool is an implementation of AllocatorStorePool that allows
// the ability to override a node's liveness status for the purposes of
// evaluation for the allocator, otherwise delegating to an actual StorePool
// for all its logic, including management and lookup of store descriptors.
//
// The OverrideStorePool is meant to provide a read-only overlay to an
// StorePool, and as such, read-only methods are dispatched to the underlying
// StorePool using the configured NodeLivenessFunc override. Methods that
// mutate the state of the StorePool such as UpdateLocalStoreAfterRebalance
// are instead no-ops.
//
// NB: Despite the fact that StorePool.DetailsMu is held in write mode in
// some of the dispatched functions, these do not mutate the state of the
// underlying StorePool.
type OverrideStorePool struct {
	sp *StorePool

	overrideNodeLivenessFn NodeLivenessFunc
	overrideNodeCountFn    NodeCountFunc
}

var _ AllocatorStorePool = &OverrideStorePool{}

// OverrideNodeLivenessFunc constructs a NodeLivenessFunc based on a set of
// predefined overrides. If any nodeID does not have an override, the liveness
// status is looked up using the passed-in real node liveness function.
func OverrideNodeLivenessFunc(
	overrides map[roachpb.NodeID]livenesspb.NodeLivenessStatus, realNodeLivenessFunc NodeLivenessFunc,
) NodeLivenessFunc {
	return func(nid roachpb.NodeID) livenesspb.NodeLivenessStatus {
		if override, ok := overrides[nid]; ok {
			return override
		}
		return realNodeLivenessFunc(nid)
	}
}

// OverrideNodeCountFunc constructs a NodeCountFunc based on a set of predefined
// overrides. If any nodeID does not have an override, the real liveness is used
// for the count for the number of nodes not decommissioning or decommissioned.
func OverrideNodeCountFunc(
	overrides map[roachpb.NodeID]livenesspb.NodeLivenessStatus, nodeLiveness *liveness.NodeLiveness,
) NodeCountFunc {
	return func() int {
		var count int
		for id, nv := range nodeLiveness.ScanNodeVitalityFromCache() {
			if !nv.IsDecommissioning() && !nv.IsDecommissioned() {
				if overrideStatus, ok := overrides[id]; !ok ||
					(overrideStatus != livenesspb.NodeLivenessStatus_DECOMMISSIONING &&
						overrideStatus != livenesspb.NodeLivenessStatus_DECOMMISSIONED) {
					count++
				}
			}
		}
		return count
	}
}

// NewOverrideStorePool constructs an OverrideStorePool that can use its own
// view of node liveness while falling through to an underlying store pool for
// the state of peer stores.
func NewOverrideStorePool(
	storePool *StorePool, nl NodeLivenessFunc, nc NodeCountFunc,
) *OverrideStorePool {
	return &OverrideStorePool{
		sp:                     storePool,
		overrideNodeLivenessFn: nl,
		overrideNodeCountFn:    nc,
	}
}

func (o *OverrideStorePool) String() string {
	return redact.StringWithoutMarkers(o)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (o *OverrideStorePool) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Print(o.sp.statusString(o.overrideNodeLivenessFn))
}

// IsStoreReadyForRoutineReplicaTransfer implements the AllocatorStorePool interface.
func (o *OverrideStorePool) IsStoreReadyForRoutineReplicaTransfer(
	ctx context.Context, targetStoreID roachpb.StoreID,
) bool {
	return o.sp.isStoreReadyForRoutineReplicaTransferInternal(ctx, targetStoreID, o.overrideNodeLivenessFn)
}

// DecommissioningReplicas implements the AllocatorStorePool interface.
func (o *OverrideStorePool) DecommissioningReplicas(
	repls []roachpb.ReplicaDescriptor,
) []roachpb.ReplicaDescriptor {
	return o.sp.decommissioningReplicasWithLiveness(repls, o.overrideNodeLivenessFn)
}

// GetStoreList implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetStoreList(
	filter StoreFilter,
) (StoreList, int, ThrottledStoreReasons) {
	o.sp.DetailsMu.Lock()
	defer o.sp.DetailsMu.Unlock()

	var storeIDs roachpb.StoreIDSlice
	for storeID := range o.sp.DetailsMu.StoreDetails {
		storeIDs = append(storeIDs, storeID)
	}
	return o.sp.getStoreListFromIDsLocked(storeIDs, o.overrideNodeLivenessFn, filter)
}

// GetStoreListFromIDs implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetStoreListFromIDs(
	storeIDs roachpb.StoreIDSlice, filter StoreFilter,
) (StoreList, int, ThrottledStoreReasons) {
	o.sp.DetailsMu.Lock()
	defer o.sp.DetailsMu.Unlock()
	return o.sp.getStoreListFromIDsLocked(storeIDs, o.overrideNodeLivenessFn, filter)
}

// GetStoreListForTargets implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetStoreListForTargets(
	candidates []roachpb.ReplicationTarget, filter StoreFilter,
) (StoreList, int, ThrottledStoreReasons) {
	o.sp.DetailsMu.Lock()
	defer o.sp.DetailsMu.Unlock()

	storeIDs := make(roachpb.StoreIDSlice, 0, len(candidates))
	for _, tgt := range candidates {
		storeIDs = append(storeIDs, tgt.StoreID)
	}

	return o.sp.getStoreListFromIDsLocked(storeIDs, o.overrideNodeLivenessFn, filter)
}

// LiveAndDeadReplicas implements the AllocatorStorePool interface.
func (o *OverrideStorePool) LiveAndDeadReplicas(
	repls []roachpb.ReplicaDescriptor, includeSuspectAndDrainingStores bool,
) (liveReplicas, deadReplicas []roachpb.ReplicaDescriptor) {
	return o.sp.liveAndDeadReplicasWithLiveness(repls, o.overrideNodeLivenessFn, includeSuspectAndDrainingStores)
}

// ClusterNodeCount implements the AllocatorStorePool interface.
func (o *OverrideStorePool) ClusterNodeCount() int {
	return o.overrideNodeCountFn()
}

// IsDeterministic implements the AllocatorStorePool interface.
func (o *OverrideStorePool) IsDeterministic() bool {
	return o.sp.deterministic
}

// Clock implements the AllocatorStorePool interface.
func (o *OverrideStorePool) Clock() *hlc.Clock {
	return o.sp.clock
}

// GetLocalitiesByNode implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetLocalitiesByNode(
	replicas []roachpb.ReplicaDescriptor,
) map[roachpb.NodeID]roachpb.Locality {
	return o.sp.GetLocalitiesByNode(replicas)
}

// GetLocalitiesByStore implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetLocalitiesByStore(
	replicas []roachpb.ReplicaDescriptor,
) map[roachpb.StoreID]roachpb.Locality {
	return o.sp.GetLocalitiesByStore(replicas)
}

// GetStores implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetStores() map[roachpb.StoreID]roachpb.StoreDescriptor {
	return o.sp.GetStores()
}

// GetStoreDescriptor implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetStoreDescriptor(
	storeID roachpb.StoreID,
) (roachpb.StoreDescriptor, bool) {
	return o.sp.GetStoreDescriptor(storeID)
}

// UpdateLocalStoreAfterRebalance implements the AllocatorStorePool interface.
// This override method is a no-op, as
// StorePool.UpdateLocalStoreAfterRebalance(..) is not a read-only method and
// mutates the state of the held store details.
func (o *OverrideStorePool) UpdateLocalStoreAfterRebalance(
	_ roachpb.StoreID, _ allocator.RangeUsageInfo, _ roachpb.ReplicaChangeType,
) {
}

// UpdateLocalStoresAfterLeaseTransfer implements the AllocatorStorePool interface.
// This override method is a no-op, as
// StorePool.UpdateLocalStoresAfterLeaseTransfer(..) is not a read-only method and
// mutates the state of the held store details.
func (o *OverrideStorePool) UpdateLocalStoresAfterLeaseTransfer(
	_ roachpb.StoreID, _ roachpb.StoreID, _ allocator.RangeUsageInfo,
) {
}

// UpdateLocalStoreAfterRelocate implements the AllocatorStorePool interface.
// This override method is a no-op, as
// StorePool.UpdateLocalStoreAfterRelocate(..) is not a read-only method and
// mutates the state of the held store details.
func (o *OverrideStorePool) UpdateLocalStoreAfterRelocate(
	_, _ []roachpb.ReplicationTarget,
	_, _ []roachpb.ReplicaDescriptor,
	_ roachpb.StoreID,
	_ allocator.RangeUsageInfo,
) {
}

// SetOnCapacityChange installs a callback to be called when any store
// capacity changes in the storepool. This currently doesn't consider local
// updates (UpdateLocalStoreAfterRelocate, UpdateLocalStoreAfterRebalance,
// UpdateLocalStoresAfterLeaseTransfer) as capacity changes.
func (o *OverrideStorePool) SetOnCapacityChange(fn CapacityChangeFn) {
}
