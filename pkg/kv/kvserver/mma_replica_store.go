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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
)

type mmaReplica Replica

// mmaLeaseholderReplica represents a replica that is guaranteed to be the leaseholder.
// This type provides methods that are only valid when called on the leaseholder replica.
type mmaLeaseholderReplica Replica

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
) (bool, bool, *roachpb.RangeDescriptor, roachpb.SpanConfig) {
	r := (*Replica)(mr)
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.store.Clock().NowAsClockTimestamp()
	status := r.leaseStatusAtRLocked(ctx, now)
	// TODO(wenyihu6): we are doing the proper but more expensive way of checking
	// for lease status here. It may not be necessary for mma since the lease can
	// be lost before calling into MMA anyway. Highlight the diff with prototype
	// here during review.
	isLeaseholder := status.IsValid() && status.Lease.OwnedBy(r.store.StoreID())
	desc := r.descRLocked()
	conf := r.mu.conf
	r.mu.mmaFullRangeMessageNeeded = false
	return isLeaseholder, r.mu.mmaFullRangeMessageNeeded, desc, conf
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
// the range descriptor. This method is only valid when called on the leaseholder replica.
func (mlr *mmaLeaseholderReplica) constructMMAUpdate(
	desc *roachpb.RangeDescriptor,
) []mmaprototype.StoreIDAndReplicaState {
	replicas := make([]mmaprototype.StoreIDAndReplicaState, 0, len(desc.InternalReplicas))
	for _, repl := range desc.InternalReplicas {
		replica := mlr.buildReplicaState(repl)
		replicas = append(replicas, replica)
	}
	return replicas
}

// buildReplicaState constructs a StoreIDAndReplicaState for the given replica.
func (mlr *mmaLeaseholderReplica) buildReplicaState(
	repl roachpb.ReplicaDescriptor,
) mmaprototype.StoreIDAndReplicaState {
	return mmaprototype.StoreIDAndReplicaState{
		StoreID: repl.StoreID,
		ReplicaState: mmaprototype.ReplicaState{
			VoterIsLagging:   mlr.isVoterLagging(repl),
			ReplicaIDAndType: mlr.buildReplicaIDAndType(repl),
		},
	}
}

// buildReplicaIDAndType constructs the ReplicaIDAndType for the given replica.
func (mlr *mmaLeaseholderReplica) buildReplicaIDAndType(
	repl roachpb.ReplicaDescriptor,
) mmaprototype.ReplicaIDAndType {
	r := (*Replica)(mlr)
	return mmaprototype.ReplicaIDAndType{
		ReplicaID: repl.ReplicaID,
		ReplicaType: mmaprototype.ReplicaType{
			ReplicaType: repl.Type,
			// mlr is only called on the leaseholder replica.
			IsLeaseholder: repl.StoreID == r.StoreID(),
		},
	}
}

// isVoterLagging checks if a voter replica is lagging behind the leaseholder.
func (mlr *mmaLeaseholderReplica) isVoterLagging(
	repl roachpb.ReplicaDescriptor,
) bool {
	r := (*Replica)(mlr)
	if !repl.IsAnyVoter() {
		return false
	}
	// TODO(wenyihu6): check if we can use r.raftSparseStatusRLocked() here
	// (cheaper).
	raftStatus := r.RaftStatus()
	return raftutil.ReplicaIsBehind(raftStatus, repl.ReplicaID)
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
func (mr *mmaReplica) tryConstructMMARangeMsg(
	ctx context.Context, knownStores map[roachpb.StoreID]struct{},
) (isLeaseholder bool, shouldBeSkipped bool, msg mmaprototype.RangeMsg) {
	r := (*Replica)(mr)
	if !r.IsInitialized() {
		return false, false, mmaprototype.RangeMsg{}
	}

	isLeaseholder, mmaFullRangeMessageNeeded, desc, conf := mr.isLeaseholderWithDescAndConfig(ctx)

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
	mlr := (*mmaLeaseholderReplica)(mr)
	replicas := mlr.constructMMAUpdate(desc)
	rLoad := mr.mmaRangeLoad()
	if mmaFullRangeMessageNeeded {
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

type mmaStore Store

func (ms *mmaStore) StoreID() roachpb.StoreID {
	s := (*Store)(ms)
	return s.StoreID()
}

func (ms *mmaStore) GetReplicaIfExists(id roachpb.RangeID) replicaToApplyChanges {
	s := (*Store)(ms)
	return s.GetReplicaIfExists(id)
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
