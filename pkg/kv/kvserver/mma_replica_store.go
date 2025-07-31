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

// maybePromiseSpanConfigUpdate determines whether an up-to-date span config
// should be sent to mma and overrides mmaSpanConfigIsUpToDate as true. The
// contract is: if this returns true, mmaReplica must send a fully populated
// range message to mma.
func (mr *mmaReplica) maybePromiseSpanConfigUpdate() (needed bool) {
	r := (*Replica)(mr)
	r.mu.Lock()
	defer r.mu.Unlock()
	needed = !r.mu.mmaSpanConfigIsUpToDate
	r.mu.mmaSpanConfigIsUpToDate = true
	return needed
}

// markSpanConfigNeedsUpdate marks the span config as needing an update. This is
// called by tryConstructMMARangeMsg when it cannot construct the RangeMsg. It
// signals that mma might have deleted this range state (including the span
// config), so we need to send a full range message to mma the next time
// mmaReplica is the leaseholder.
func (mr *mmaReplica) markSpanConfigNeedsUpdate() {
	r := (*Replica)(mr)
	r.mu.Lock()
	defer r.mu.Unlock()
	mr.markSpanConfigNeedsUpdateLocked()
}

// markSpanConfigNeedsUpdateLocked marks the span config as needing an update
// while holding the mu lock on the replica. It is called during
// markSpanConfigNeedsUpdate and when there is a span config change that warrants
// sending a full range message to mma.
func (mr *mmaReplica) markSpanConfigNeedsUpdateLocked() {
	r := (*Replica)(mr)
	r.mu.AssertHeld()
	r.mu.mmaSpanConfigIsUpToDate = false
}

// isLeaseholderWithDescAndConfig checks if the replica is the leaseholder and
// returns the range descriptor and span config. Lease status and range
// descriptor need to be mutually consistent (i.e., if the RangeDescriptor
// changes to remove this replica, we don't want to continue thinking
// isLeaseholder is true), so we read them under the same lock.
//
// NB: raftStatus returned here might be nil if mmaReplica is not the
// leaseholder replica. It is up to the caller to only use it if isLeaseholder
// returned is true.
func (mr *mmaReplica) isLeaseholderWithDescAndConfig(
	ctx context.Context,
) (
	isLeaseholder bool,
	desc *roachpb.RangeDescriptor,
	conf roachpb.SpanConfig,
	raftStatus *raft.Status,
) {
	r := (*Replica)(mr)
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := r.store.Clock().NowAsClockTimestamp()
	leaseStatus := r.leaseStatusAtRLocked(ctx, now)
	// TODO(wenyihu6): Alternatively, we could just read r.shMu.state.Lease and
	// check if it is non-nil and call OwnedBy(r.store.StoreID()), which would be
	// cheaper. One tradeoff is that if the lease has expired, and a new lease has
	// not been installed in the state machine, the cheaper approach would think
	// this replica is still the leaseholder.
	isLeaseholder = leaseStatus.IsValid() && leaseStatus.Lease.OwnedBy(r.store.StoreID())
	desc = r.descRLocked()
	conf = r.mu.conf
	if isLeaseholder {
		raftStatus = r.raftStatusRLocked()
	}
	return
}

// constructRangeMsgReplicas constructs the mmaprototype.StoreIDAndReplicaState from
// the range descriptor. This method is only valid when called on the
// leaseholder replica.
func constructRangeMsgReplicas(
	desc *roachpb.RangeDescriptor, raftStatus *raft.Status, leaseholderReplicaStoreID roachpb.StoreID,
) []mmaprototype.StoreIDAndReplicaState {
	if raftStatus == nil && buildutil.CrdbTestBuild {
		panic("programming error: raftStatus is nil when constructing range msg")
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
// When isLeaseholder = true and shouldBeSkipped = false, all fields in RangeMsg
// are populated except possibly MaybeSpanConf. If the span config has changed
// or mma is known to lack a valid span config for this range (e.g. because it
// deleted the range state after not receiving a range message in the store
// leaseholder message), then MaybeSpanConfIsPopulated is set to true and
// MaybeSpanConf is populated. Otherwise, MaybeSpanConfIsPopulated is false and
// MaybeSpanConf is left unset.
//
// Called periodically by the same entity (and must not be called concurrently).
// If this method returned isLeaseholder=true and shouldBeSkipped=false the last
// time, that message must have been fed to the allocator.
//
// Concurrency concerns:
// Caller1 may call into maybePromiseSpanConfigUpdate() and include the SpanConfig
// (marking it as no longer needed an update), and then Caller2 decides not to
// include the SpanConfig in the rangeMsg. But caller2 may get ahead and call
// into mma first with a RangeMsg that is lacking a SpanConfig and mma wouldn't
// know how to initialize a range it knows nothing about. More thinking is needed
// to claim this method is not racy.
func (mr *mmaReplica) tryConstructMMARangeMsg(
	ctx context.Context, knownStores map[roachpb.StoreID]struct{},
) (isLeaseholder bool, shouldBeSkipped bool, msg mmaprototype.RangeMsg) {
	r := (*Replica)(mr)
	if !r.IsInitialized() {
		return false, false, mmaprototype.RangeMsg{}
	}
	isLeaseholder, desc, conf, raftStatus := mr.isLeaseholderWithDescAndConfig(ctx)
	// Update the mmaRangeMessageNeeded state if no longer the leaseholder.
	if !isLeaseholder {
		// We are not providing a range message since this replica is not the
		// leaseholder replica, mma might delete this range state if this store
		// is the range owner store. Mark span config as needing an update so
		// that tryConstructMMARangeMsg would provide a range message with
		// up-to-date span config next time it sends a message.
		mr.markSpanConfigNeedsUpdate()
		return false, false, mmaprototype.RangeMsg{}
	}
	// Check if any replicas are on an unknown store to mma.
	for _, repl := range desc.InternalReplicas {
		if _, ok := knownStores[repl.StoreID]; !ok {
			// If any of the replicas is on a store that is not included in
			// knownStores, we cannot construct the RangeMsg. NB: very rare.
			//
			// We are dropping a range message since there are some unknown
			// stores, mma might delete this range state if this store is the
			// range owner store. Mark span config as needing an update so
			// that tryConstructMMARangeMsg would provide a range message with
			// up-to-date span config next time it sends a message.
			mr.markSpanConfigNeedsUpdate()
			return true, true, mmaprototype.RangeMsg{}
		}
	}
	// At this point, we know r is the leaseholder replica.
	replicas := constructRangeMsgReplicas(desc, raftStatus, r.StoreID() /*leaseholderReplicaStoreID*/)
	rLoad := mr.mmaRangeLoad()
	if mr.maybePromiseSpanConfigUpdate() {
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
