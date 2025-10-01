// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// InvalidSyncChangeID is a sentinel value for an invalid sync change ID. It is
// used only for asim to indicate that a change is not in progress.
const InvalidSyncChangeID SyncChangeID = 0

func (id SyncChangeID) IsValid() bool {
	return id != 0
}

type SyncChangeID uint64

// storePool is an interface that defines the methods that the allocator sync
// needs to call on the store pool. Using an interface to simplify testing.
type storePool interface {
	// UpdateLocalStoresAfterLeaseTransfer is called by the allocator sync to
	// update the store pool after a lease transfer operation.
	UpdateLocalStoresAfterLeaseTransfer(transferFrom, transferTo roachpb.StoreID, usage allocator.RangeUsageInfo)
	// UpdateLocalStoreAfterRebalance is called by the allocator sync to update
	// the store pool after a rebalance operation.
	UpdateLocalStoreAfterRebalance(storeID roachpb.StoreID, rangeUsageInfo allocator.RangeUsageInfo, changeType roachpb.ReplicaChangeType)
}

// mmaState is an interface that defines the methods that the allocator sync
// needs to call on the mma. Using an interface to simplify testing.
type mmaState interface {
	// RegisterExternalChanges is called by the allocator sync to register
	// external changes with the mma.
	RegisterExternalChanges(changes []mmaprototype.ReplicaChange) []mmaprototype.ChangeID
	// AdjustPendingChangesDisposition is called by the allocator sync to adjust
	// the disposition of pending changes.
	AdjustPendingChangesDisposition(changeIDs []mmaprototype.ChangeID, success bool)
	// BuildMMARebalanceAdvisor is called by the allocator sync to build a
	// MMARebalanceAdvisor for the given existing store and candidates. The
	// advisor should be later passed to IsInConflictWithMMA to determine if a
	// given candidate is in conflict with the existing store.
	BuildMMARebalanceAdvisor(existing roachpb.StoreID, cands []roachpb.StoreID) *mmaprototype.MMARebalanceAdvisor
	// IsInConflictWithMMA is called by the allocator sync to determine if the
	// given candidate is in conflict with the existing store.
	IsInConflictWithMMA(ctx context.Context, cand roachpb.StoreID, advisor *mmaprototype.MMARebalanceAdvisor, cpuOnly bool) bool
}

// TODO(wenyihu6): make sure allocator sync can tolerate cluster setting
// changes not happening consistently or atomically across components. (For
// example, replicate queue may call into allocator sync when mma is enabled but
// has been disabled postapply or other components call into allocator sync when
// mma is disabled.)

// AllocatorSync is a component that coordinates changes from all components
// (including mma-store-rebalancer/replicate/lease queue) with mma and store
// pool. When mma is disabled, its sole purpose is to track and apply changes
// to the store pool upon success.
type AllocatorSync struct {
	knobs        *TestingKnobs
	sp           storePool
	st           *cluster.Settings
	mmaAllocator mmaState
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
	sp storePool, mmaAllocator mmaState, st *cluster.Settings, knobs *TestingKnobs,
) *AllocatorSync {
	as := &AllocatorSync{
		sp:           sp,
		st:           st,
		mmaAllocator: mmaAllocator,
		knobs:        knobs,
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
	as.mu.changeSeqGen++
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
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	usage allocator.RangeUsageInfo,
	transferFrom, transferTo roachpb.ReplicationTarget,
) SyncChangeID {
	var changeIDs []mmaprototype.ChangeID
	if kvserverbase.LoadBasedRebalancingModeIsMMA(&as.st.SV) {
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
	log.KvDistribution.VEventf(ctx, 2, "non-mma: adding lease transfer from s%s to s%s", transferFrom.StoreID, transferTo.StoreID)
	return as.addTrackedChange(trackedChange)
}

// NonMMAPreChangeReplicas is called by the replicate queue to register a
// change replicas operation. SyncChangeID is returned to the caller. It is an
// identifier that can be used to call PostApply to apply the change to the
// store pool upon success.
func (as *AllocatorSync) NonMMAPreChangeReplicas(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	usage allocator.RangeUsageInfo,
	changes kvpb.ReplicationChanges,
	leaseholderStoreID roachpb.StoreID,
) SyncChangeID {
	var changeIDs []mmaprototype.ChangeID
	if kvserverbase.LoadBasedRebalancingModeIsMMA(&as.st.SV) {
		changeIDs = as.mmaAllocator.RegisterExternalChanges(convertReplicaChangeToMMA(desc, usage, changes, leaseholderStoreID))
	}
	trackedChange := trackedAllocatorChange{
		changeIDs: changeIDs,
		usage:     usage,
		changeReplicasOp: &changeReplicasOp{
			chgs: changes,
		},
	}
	for _, chg := range changes {
		log.KvDistribution.VEventf(ctx, 2, "non-mma: adding s%s with change=%s", chg.Target.StoreID, chg.ChangeType)
	}
	return as.addTrackedChange(trackedChange)
}

// MMAPreApply is called by the mma to register a change with AllocatorSync.
// It is called before the change is applied. SyncChangeID is returned to the
// caller. It is an identifier that can be used to call PostApply to apply the
// change to the store pool upon success.
func (as *AllocatorSync) MMAPreApply(
	ctx context.Context,
	usage allocator.RangeUsageInfo,
	pendingChange mmaprototype.PendingRangeChange,
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
		log.KvDistribution.VEventf(ctx, 2, "mma: adding lease transfer from s%s to s%s", pendingChange.LeaseTransferFrom(), pendingChange.LeaseTransferTarget())
	case pendingChange.IsChangeReplicas():
		trackedChange.changeReplicasOp = &changeReplicasOp{
			chgs: pendingChange.ReplicationChanges(),
		}
		for _, chg := range pendingChange.ReplicationChanges() {
			log.KvDistribution.VEventf(ctx, 2, "mma: adding s%s with change=%s", chg.Target.StoreID, chg.ChangeType)
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
