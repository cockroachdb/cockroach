// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftutil"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// MMARangeLoad constructs a mmaprototype.RangeLoad from the replica's LoadStats.
// The returned RangeLoad contains stats across multiple dimensions that MMA uses
// to determine top-k replicas and evaluate the load impact of rebalancing them.
func (r *Replica) MMARangeLoad() mmaprototype.RangeLoad {
	loadStats := r.LoadStats()
	var rl mmaprototype.RangeLoad
	rl.Load[mmaprototype.CPURate] = mmaprototype.LoadValue(
		loadStats.RequestCPUNanosPerSecond + loadStats.RaftCPUNanosPerSecond)
	rl.RaftCPU = mmaprototype.LoadValue(loadStats.RaftCPUNanosPerSecond)
	rl.Load[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(loadStats.WriteBytesPerSecond)
	rl.Load[mmaprototype.ByteSize] = mmaprototype.LoadValue(r.GetMVCCStats().Total())
	return rl
}

// isLeaseholderWithDescAndConfig checks if the replica is the leaseholder and
// returns the range descriptor and span config. Lease status and range
// descriptor need to be mutually consistent (i.e., if the RangeDescriptor
// changes to remove this replica, we don't want to continue thinking
// isLeaseholder is true), so we read them under the same lock.
func (r *Replica) isLeaseholderWithDescAndConfig(
	ctx context.Context,
) (bool, *roachpb.RangeDescriptor, roachpb.SpanConfig) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := r.store.Clock().NowAsClockTimestamp()
	status := r.leaseStatusAtRLocked(ctx, now)
	// TODO(wenyihu6): we are doing the proper but more expensive way of checking
	// for lease status here. It may not be necessary for mma since the lease can
	// be lost before calling into MMA anyway. Highlight the diff with prototype
	// here during review.
	isLeaseholder := status.IsValid() && status.Lease.OwnedBy(r.store.StoreID())
	desc := r.descRLocked()
	conf := r.mu.conf
	return isLeaseholder, desc, conf
}

// constructMMAReplicas constructs the mmaprototype.StoreIDAndReplicaState from
// the range descriptor.
func (r *Replica) constructMMAReplicas(
	desc *roachpb.RangeDescriptor,
) []mmaprototype.StoreIDAndReplicaState {
	voterIsLagging := func(repl roachpb.ReplicaDescriptor) bool {
		if !repl.IsAnyVoter() {
			return false
		}
		// TODO(wenyihu6): check if we can use r.raftSparseStatusRLocked() here
		// (cheaper).
		raftStatus := r.RaftStatus()
		return raftutil.ReplicaIsBehind(raftStatus, repl.ReplicaID)
	}

	replicas := make([]mmaprototype.StoreIDAndReplicaState, 0, len(desc.InternalReplicas))
	for _, repl := range desc.InternalReplicas {
		replica := mmaprototype.StoreIDAndReplicaState{
			StoreID: repl.StoreID,
			ReplicaState: mmaprototype.ReplicaState{
				VoterIsLagging: voterIsLagging(repl),
				ReplicaIDAndType: mmaprototype.ReplicaIDAndType{
					ReplicaID: repl.ReplicaID,
					ReplicaType: mmaprototype.ReplicaType{
						ReplicaType: repl.Type,
						// Caller only calls this method if r is the leaseholder replica.
						IsLeaseholder: repl.StoreID == r.store.StoreID(),
					},
				},
			},
		}
		replicas = append(replicas, replica)
	}
	return replicas
}

// TryConstructMMARangeMsg attempts to construct a mmaprototype.RangeMsg for the
// Replica iff the replica is currently the leaseholder, in which case it
// returns lh=true.
//
// If it cannot construct the RangeMsg because one of the replicas is on a
// store that is not included in knownStores, it returns lh=true,
// ignored=true. mma would drop this RangeMsg and log an error.
//
// When lh=true and ignored=false, there are two possibilities:
//
//   - There is at least one change that warrants informing MMA, in which case
//     all the fields are populated, and Populated is true. See
//     mmaRangeMessageNeeded.checkIfNeeded for more details.
//
//   - Nothing has changed (including membership), in which case only the
//     RangeID and Replicas are set and Populated is false.
//
// Called periodically by the same entity (and must not be called
// concurrently). If this method returned lh=true and ignored=false the last
// time, that message must have been fed to the allocator.
func (r *Replica) TryConstructMMARangeMsg(
	ctx context.Context, knownStores map[roachpb.StoreID]struct{},
) (isLeaseholder bool, shouldBeSkipped bool, msg mmaprototype.RangeMsg) {
	if !r.IsInitialized() {
		return false, false, mmaprototype.RangeMsg{}
	}

	// Note that we do not access wasLeaseholder under the same lock as r.mu since
	// TryConstructMMARangeMsg is the only one accessing the field and cannot
	// be changed concurrently.
	isLeaseholder, desc, conf := r.isLeaseholderWithDescAndConfig(ctx)

	// TODO(wenyihu6): highlight the diff with prototype here. Prototype hold r.mu
	// lock while clearing mmaRangeMessageNeeded and setting wasLeaseholder which
	// seems unnecessary since we are the only one accessing them and stale view
	// should be fine for mma.

	// Update the mmaRangeMessageNeeded state if no longer the leaseholder.
	if !isLeaseholder {
		return false, false, mmaprototype.RangeMsg{}
	}

	// Check if any replicas are on an unknown store to mma.
	for _, repl := range desc.InternalReplicas {
		if _, ok := knownStores[repl.StoreID]; !ok {
			// If any of the replicas is on a store that is not included in
			// knownStores, we cannot construct the RangeMsg. NB: very rare. Set
			// needed to be true since knownStores might be different in the next
			// call.
			r.mmaRangeMessageNeeded.Store(true)
			return true, true, mmaprototype.RangeMsg{}
		}
	}

	replicas := r.constructMMAReplicas(desc)
	rLoad := r.MMARangeLoad()
	// TODO(wenyihu6): check if we can use r.raftSparseStatusRLocked() here
	// (cheaper).
	// TODO(wenyihu6): highlight the diff with prototype here. Prototype hold r.mu
	// lock while calling getNeededAndReset and accessing raft status. I don't
	// think we require consistency between stateChangeTriggered and raft status.
	if r.mmaRangeMessageNeeded.Swap(false) {
		return true, false, mmaprototype.RangeMsg{
			RangeID:   r.RangeID,
			Replicas:  replicas,
			RangeLoad: rLoad,
			Populated: true,
			Conf:      conf,
		}
	}

	return true, false, mmaprototype.RangeMsg{
		RangeID:   r.RangeID,
		Replicas:  replicas,
		RangeLoad: rLoad,
		Populated: false,
	}
}
