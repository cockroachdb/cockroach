// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftutil"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
)

type mmaReplica Replica

// The returned RangeLoad contains stats across multiple dimensions that MMA uses
// to determine top-k replicas and evaluate the load impact of rebalancing them.
func mmaRangeLoad(
	loadStats load.ReplicaLoadStats, mvccStats enginepb.MVCCStats,
) mmaprototype.RangeLoad {
	var rl mmaprototype.RangeLoad
	rl.Load[mmaprototype.CPURate] = mmaprototype.LoadValue(
		loadStats.RequestCPUNanosPerSecond + loadStats.RaftCPUNanosPerSecond)
	rl.RaftCPU = mmaprototype.LoadValue(loadStats.RaftCPUNanosPerSecond)
	rl.Load[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(loadStats.WriteBytesPerSecond)
	rl.Load[mmaprototype.ByteSize] = mmaprototype.LoadValue(mvccStats.Total())
	return rl
}

// mmaRangeLoad constructs a mmaprototype.RangeLoad from the replica's LoadStats.
// The returned RangeLoad contains stats across multiple dimensions that MMA uses
// to determine top-k replicas and evaluate the load impact of rebalancing them.
func (mr *mmaReplica) mmaRangeLoad() mmaprototype.RangeLoad {
	r := (*Replica)(mr)
	return mmaRangeLoad(r.LoadStats(), r.GetMVCCStats())
}

// isLeaseholderWithDescAndConfig checks if the replica is the leaseholder and
// returns the range descriptor and span config. Lease status and range
// descriptor need to be mutually consistent (i.e., if the RangeDescriptor
// changes to remove this replica, we don't want to continue thinking
// isLeaseholder is true), so we read them under the same lock.
func (mr *mmaReplica) isLeaseholderWithDescAndConfig(
	ctx context.Context,
) (bool, bool, *roachpb.RangeDescriptor, roachpb.SpanConfig, *raft.Status) {
	r := (*Replica)(mr)
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.store.Clock().NowAsClockTimestamp()
	status := r.leaseStatusAtRLocked(ctx, now)
	// TODO(wenyihu6): Alternatively, we could just read r.shMu.state.Leas` and
	// check if it is non-nil and call OwnedBy(r.store.StoreID()), which would be
	// cheaper. One tradeoff is that if the lease has expired, and a new lease has
	// not been installed in the state machine, the cheaper approach would think
	// this replica is still the leaseholder.
	isLeaseholder := status.IsValid() && status.Lease.OwnedBy(r.store.StoreID())
	desc := r.descRLocked()
	conf := r.mu.conf
	r.mu.mmaFullRangeMessageNeeded = false
	return isLeaseholder, r.mu.mmaFullRangeMessageNeeded, desc, conf, r.raftStatusRLocked()
}

func (mr *mmaReplica) setMMAFullRangeMessageNeeded() {
	r := (*Replica)(mr)
	r.mu.Lock()
	defer r.mu.Unlock()
	mr.setMMAFullRangeMessageNeededRLocked()
}

func (mr *mmaReplica) setMMAFullRangeMessageNeededRLocked() {
	r := (*Replica)(mr)
	r.mu.mmaFullRangeMessageNeeded = true
}

// constructMMAUpdate constructs the mmaprototype.StoreIDAndReplicaState from
// the range descriptor. This method is only valid when called on the
// leaseholder replica.
func constructMMAUpdate(
	desc *roachpb.RangeDescriptor, raftStatus *raft.Status, leaseholderReplicaStoreID roachpb.StoreID,
) []mmaprototype.StoreIDAndReplicaState {
	if raftStatus == nil {
		if buildutil.CrdbTestBuild {
			panic("programming error: raftStatus is nil when constructing range msg")
		}
	}
	// TODO(mma): this is called on every leaseholder replica every minute. We
	// should pass scratch memory to avoid unnecessary allocation.
	replicas := make([]mmaprototype.StoreIDAndReplicaState, 0, len(desc.InternalReplicas))
	for _, repl := range desc.InternalReplicas {
		replica := mmaprototype.StoreIDAndReplicaState{
			StoreID: repl.StoreID,
			ReplicaState: mmaprototype.ReplicaState{
				VoterIsLagging: repl.IsAnyVoter() && raftutil.ReplicaIsBehind(raftStatus, repl.ReplicaID),
				ReplicaIDAndType: mmaprototype.ReplicaIDAndType{
					ReplicaID: repl.ReplicaID,
					ReplicaType: mmaprototype.ReplicaType{
						ReplicaType:   repl.Type,
						IsLeaseholder: repl.StoreID == leaseholderReplicaStoreID,
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
// returns isLeaseholder=true.
//
// If it cannot construct the RangeMsg because one of the replicas is on a
// store that is not included in knownStores, it returns isLeaseholder=true,
// shouldBeSkipped=true. mma would drop this RangeMsg and log an error.
//
// When isLeaseholder=true and shouldBeSkipped=false, there are two possibilities:
//
//   - There is at least one change that warrants informing MMA, in which case
//     all the fields are populated, and Populated is true. See
//     mmaRangeMessageNeeded.checkIfNeeded for more details.
//
//   - Nothing has changed (including membership), in which case only the
//     RangeID and Replicas are set and Populated is false.
//
// Called periodically by the same entity (and must not be called concurrently).
// If this method returned isLeaseholder=true and shouldBeSkipped=false the last
// time, that message must have been fed to the allocator.
func (mr *mmaReplica) tryConstructMMARangeMsg(
	ctx context.Context, knownStores map[roachpb.StoreID]struct{},
) (isLeaseholder bool, shouldBeSkipped bool, msg mmaprototype.RangeMsg) {
	r := (*Replica)(mr)
	if !r.IsInitialized() {
		return false, false, mmaprototype.RangeMsg{}
	}
	isLeaseholder, mmaFullRangeMessageNeeded, desc, conf, raftStatus := mr.isLeaseholderWithDescAndConfig(ctx)
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
			mr.setMMAFullRangeMessageNeeded()
			return true, true, mmaprototype.RangeMsg{}
		}
	}
	// At this point, we know this replica is the leaseholder, so we can safely
	// cast to the leaseholder replica type for clarity.
	replicas := constructMMAUpdate(desc, raftStatus, r.StoreID())
	rLoad := mr.mmaRangeLoad()
	if mmaFullRangeMessageNeeded {
		return true, false, mmaprototype.RangeMsg{
			RangeID:                  r.RangeID,
			Replicas:                 replicas,
			RangeLoad:                rLoad,
			MaybeSpanConfIsPopulated: true,
			MaybeSpanConf:            conf,
		}
	}
	return true, false, mmaprototype.RangeMsg{
		RangeID:                  r.RangeID,
		Replicas:                 replicas,
		RangeLoad:                rLoad,
		MaybeSpanConfIsPopulated: false,
	}
}

type mmaStore Store

func (ms *mmaStore) StoreID() roachpb.StoreID {
	s := (*Store)(ms)
	return s.StoreID()
}

// GetReplicaIfExists returns the replica with the given range ID if it exists.
// Returns nil if the replica does not exist.
func (ms *mmaStore) GetReplicaIfExists(id roachpb.RangeID) replicaToApplyChanges {
	s := (*Store)(ms)
	r := s.GetReplicaIfExists(id)
	if r == nil {
		// This is needed because r here would be (*Replica)(nil). Returning
		// directly would wrap (*Replica)(nil) in replicaToApplyChanges. So we
		// return the nil interface directly to simplify caller's nil check
		// handling.
		return nil
	}
	return r
}

// MakeStoreLeaseholderMsg constructs the StoreLeaseholderMsg by iterating over
// all the replicas in the store and constructing the RangeMsg for each leaseholder
// replica. KnownStores includes all the stores that are known to mma. If some
// replicas in the range descriptor are not known to mma, we skip including them
// in the range message, and numIgnoredRanges is returned here just for
// logging purpose.
func (ms *mmaStore) MakeStoreLeaseholderMsg(
	ctx context.Context, knownStores map[roachpb.StoreID]struct{},
) (msg mmaprototype.StoreLeaseholderMsg, numIgnoredRanges int) {
	var msgs []mmaprototype.RangeMsg
	s := (*Store)(ms)
	// TODO(wenyihu6): this is called on every leaseholder replica every minute.
	// We should pass scratch memory to avoid unnecessary allocation.
	newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
		mr := (*mmaReplica)(r)
		isLeaseholder, shouldBeSkipped, msg := mr.tryConstructMMARangeMsg(ctx, knownStores)
		if isLeaseholder {
			if shouldBeSkipped {
				numIgnoredRanges++
			} else {
				msgs = append(msgs, msg)
			}
		}
		return true
	})
	return mmaprototype.StoreLeaseholderMsg{
		StoreID: s.StoreID(),
		Ranges:  msgs,
	}, numIgnoredRanges
}
