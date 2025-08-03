// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type SyncChangeID uint64

// TODO(wenyihu6): make sure allocator sync can tolerate cluster setting
// changes not happening consistently or atomically across components. (For
// example, replicate queue may call into allocator sync when mma is enabled but
// has been disabled postapply or other components call into allocator sync when
// mma is disabled.)

// AllocatorSync is a component that coordinates changes from external
// components (e.g. replicate/lease queue) with mma. When mma is disabled,
// its sole purpose is to track and apply changes to the store pool upon
// success.
type AllocatorSync struct {
	sp           *storepool.StorePool
	st           *cluster.Settings
	mmaAllocator mmaprototype.Allocator
	mu           struct {
		syncutil.Mutex
		// changeSeqGen is a monotonically increasing sequence number for
		// tracked changes.
		changeSeqGen SyncChangeID
		// trackedChanges is a map of tracked changes. Added right before
		// trackedAllocatorChange is being applied, deleted when it has been
		// applied. SyncChangeID is used as an identifier for the replica
		// changes.
		trackedChanges map[SyncChangeID]trackedAllocatorChange
	}
}

func NewAllocatorSync(
	sp *storepool.StorePool, mmaAllocator mmaprototype.Allocator, st *cluster.Settings,
) *AllocatorSync {
	as := &AllocatorSync{
		sp:           sp,
		st:           st,
		mmaAllocator: mmaAllocator,
	}
	as.mu.trackedChanges = make(map[SyncChangeID]trackedAllocatorChange)
	return as
}

// mmaRangeLoad converts range load usage to mma range load.
//
// TODO(wenyihu6): This is bit redundant to mmaRangeLoad in kvserver. See if we
// can refactor to use the same helper function.
func mmaRangeLoad(rangeUsageInfo allocator.RangeUsageInfo) mmaprototype.RangeLoad {
	var rl mmaprototype.RangeLoad
	rl.Load[mmaprototype.CPURate] = mmaprototype.LoadValue(
		rangeUsageInfo.RequestCPUNanosPerSecond + rangeUsageInfo.RaftCPUNanosPerSecond)
	rl.RaftCPU = mmaprototype.LoadValue(rangeUsageInfo.RaftCPUNanosPerSecond)
	rl.Load[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(rangeUsageInfo.WriteBytesPerSecond)
	// Note that LogicalBytes is already populated as enginepb.MVCCStats.Total()
	// in repl.RangeUsageInfo().
	rl.Load[mmaprototype.ByteSize] = mmaprototype.LoadValue(rangeUsageInfo.LogicalBytes)
	return rl
}

// addTrackedChange adds a tracked change to the allocator sync.
func (as *AllocatorSync) addTrackedChange(change trackedAllocatorChange) SyncChangeID {
	as.mu.Lock()
	defer as.mu.Unlock()
	as.mu.changeSeqGen += 1
	syncChangeID := as.mu.changeSeqGen
	as.mu.trackedChanges[syncChangeID] = change
	return syncChangeID
}

// getTrackedChange gets a tracked change from the allocator sync. It deletes
// the change from the map.
func (as *AllocatorSync) getTrackedChange(syncChangeID SyncChangeID) trackedAllocatorChange {
	as.mu.Lock()
	defer as.mu.Unlock()
	change, ok := as.mu.trackedChanges[syncChangeID]
	if !ok {
		panic("AllocatorSync: change not found")
	}
	delete(as.mu.trackedChanges, syncChangeID)
	return change
}

// NonMMAPreTransferLease is called by the lease/replicate queue to register a
// transfer operation. SyncChangeID is returned to the caller. It is an
// identifier that can be used to call PostApply to apply the change to the
// store pool upon success.
func (as *AllocatorSync) NonMMAPreTransferLease(
	desc *roachpb.RangeDescriptor,
	usage allocator.RangeUsageInfo,
	transferFrom, transferTo roachpb.ReplicationTarget,
) SyncChangeID {
	var changeIDs []mmaprototype.ChangeID
	if kvserverbase.LoadBasedRebalancingMode.Get(&as.st.SV) == kvserverbase.LBRebalancingMultiMetric {
		changeIDs = as.mmaAllocator.RegisterExternalChanges(convertLeaseTransferToMMA(desc, usage, transferFrom, transferTo))
	}
	trackedChange := trackedAllocatorChange{
		changeIDs: changeIDs,
		usage:     usage,
		leaseTransferOp: &leaseTransferOp{
			transferFrom: transferFrom.StoreID,
			transferTo:   transferTo.StoreID,
		},
	}
	return as.addTrackedChange(trackedChange)
}

// NonMMAPreChangeReplicas is called by the replicate queue to register a
// change replicas operation. SyncChangeID is returned to the caller. It is an
// identifier that can be used to call PostApply to apply the change to the
// store pool upon success.
func (as *AllocatorSync) NonMMAPreChangeReplicas(
	desc *roachpb.RangeDescriptor,
	usage allocator.RangeUsageInfo,
	changes kvpb.ReplicationChanges,
	leaseholderStoreID roachpb.StoreID,
) SyncChangeID {
	var changeIDs []mmaprototype.ChangeID
	if kvserverbase.LoadBasedRebalancingMode.Get(&as.st.SV) == kvserverbase.LBRebalancingMultiMetric {
		changeIDs = as.mmaAllocator.RegisterExternalChanges(convertReplicaChangeToMMA(desc, usage, changes, leaseholderStoreID))
	}
	trackedChange := trackedAllocatorChange{
		changeIDs: changeIDs,
		usage:     usage,
		changeReplicasOp: &changeReplicasOp{
			chgs: changes,
		},
	}
	return as.addTrackedChange(trackedChange)
}

// MMAPreApply is called by the mma to register a change with AllocatorSync.
// It is called before the change is applied. SyncChangeID is returned to the
// caller. It is an identifier that can be used to call PostApply to apply the
// change to the store pool upon success.
func (as *AllocatorSync) MMAPreApply(
	usage allocator.RangeUsageInfo, pendingChange mmaprototype.PendingRangeChange,
) SyncChangeID {
	trackedChange := trackedAllocatorChange{
		changeIDs: pendingChange.ChangeIDs(),
		usage:     usage,
	}
	switch {
	case pendingChange.IsTransferLease():
		trackedChange.leaseTransferOp = &leaseTransferOp{
			transferFrom: pendingChange.LeaseTransferFrom(),
			transferTo:   pendingChange.LeaseTransferTarget(),
		}
	case pendingChange.IsChangeReplicas():
		trackedChange.changeReplicasOp = &changeReplicasOp{
			chgs: pendingChange.ReplicationChanges(),
		}
	default:
		panic("unexpected change type")
	}
	return as.addTrackedChange(trackedChange)
}

// MarkChangesAsFailed marks the given change IDs as failed without going
// through allocator sync. This is used when mma changes fail before even
// registering with mma via MMAPreApply.
func (as *AllocatorSync) MarkChangesAsFailed(changeIDs []mmaprototype.ChangeID) {
	as.mmaAllocator.AdjustPendingChangesDisposition(changeIDs, false /* success */)
}

// PostApply is called by the lease/replicate queue to apply a change to the
// store pool upon success. It is called with the SyncChangeID returned by
// NonMMAPreTransferLease or NonMMAPreChangeReplicas.
func (as *AllocatorSync) PostApply(syncChangeID SyncChangeID, success bool) {
	trackedChange := as.getTrackedChange(syncChangeID)
	if changeIDs := trackedChange.changeIDs; changeIDs != nil {
		// Call into without checking cluster setting.
		as.mmaAllocator.AdjustPendingChangesDisposition(changeIDs, success)
	}
	if !success {
		return
	}
	switch {
	case trackedChange.leaseTransferOp != nil:
		as.sp.UpdateLocalStoresAfterLeaseTransfer(trackedChange.leaseTransferOp.transferFrom,
			trackedChange.leaseTransferOp.transferTo, trackedChange.usage)
	case trackedChange.changeReplicasOp != nil:
		for _, chg := range trackedChange.changeReplicasOp.chgs {
			as.sp.UpdateLocalStoreAfterRebalance(
				chg.Target.StoreID, trackedChange.usage, chg.ChangeType)
		}
	}
}
