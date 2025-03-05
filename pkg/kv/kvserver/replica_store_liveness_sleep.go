// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
)

// RaftStoreLivenessQuiescenceEnabled controls whether store liveness quiescence
// is enabled.
var RaftStoreLivenessQuiescenceEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.raft.store_liveness.quiescence.enabled",
	"controls whether store liveness quiescence is enabled",
	metamorphic.ConstantWithTestChoice("kv.raft.store_liveness.quiescence.enabled",
		false, /* defaultValue */
		true /* otherValues */),
)

// goToSleepAfterTicks is the number of Raft ticks after which a follower can
// fall asleep.
var goToSleepAfterTicks = envutil.EnvOrDefaultInt64("COCKROACH_SLEEP_AFTER_TICKS", 6)

// maybeFallAsleepRMuLocked marks a follower as asleep if possible. Returns
// true if the follower transitioned from awake to asleep.
func (r *Replica) maybeFallAsleepRMuLocked(leaseStatus kvserverpb.LeaseStatus) bool {
	// If the setting is off, do not fall asleep.
	if !RaftStoreLivenessQuiescenceEnabled.Get(&r.store.ClusterSettings().SV) {
		return false
	}
	// If already asleep, do not fall asleep.
	if r.mu.asleep {
		return false
	}
	// If the lease is not a leader lease, do not fall asleep.
	if !leaseStatus.Lease.SupportsSleep() {
		return false
	}
	// If currently the leader, do not fall asleep.
	if r.isRaftLeaderRLocked() {
		return false
	}
	// If there was a recent Raft message, do not fall asleep.
	if ticks := r.ticksSinceLastMessageRLocked(); ticks < goToSleepAfterTicks {
		return false
	}
	// Grab the quiescence lock here to prevent races between support withdrawal
	// and falling asleep.
	r.store.quiescence.Lock()
	defer r.store.quiescence.Unlock()
	// If not supporting a fortified leader, do not fall asleep.
	if !r.raftSupportingFortifiedLeaderRLocked() {
		return false
	}
	leader, ok := r.lookupLeaderRLocked()
	// If the leader is not found, do no fall asleep.
	if !ok {
		return false
	}
	// It's safe to fall asleep here: after quiescence.Lock() above, this follower
	// supports the leader. If it withdrew support for the leader since then, the
	// callback to wake up the replica will wait for the quiescence.Unlock().
	r.mu.asleep = true
	delete(r.store.quiescence.unquiescedOrAwake, r.RangeID)
	_, ok = r.store.quiescence.asleepByLeaderStore[leader.StoreID]
	if !ok {
		r.store.quiescence.asleepByLeaderStore[leader.StoreID] = make(map[*Replica]struct{})
	}
	r.store.quiescence.asleepByLeaderStore[leader.StoreID][r] = struct{}{}
	return true
}

// maybeWakeUpReplicaMuLocked marks a follower as awake.
func (r *Replica) maybeWakeUpReplicaMuLocked() {
	r.mu.AssertHeld()
	if !r.mu.asleep {
		return
	}
	r.maybeRemoveAsleepReplicaFromQuiescenceStateReplicaMuLocked()
	// Prevent immediate falling asleep.
	r.mu.lastMessageAtTicks = r.mu.ticks
	r.mu.asleep = false
}

func (r *Replica) maybeRemoveAsleepReplicaFromQuiescenceStateReplicaMuLocked() {
	r.mu.AssertHeld()
	if !r.mu.asleep {
		return
	}
	r.store.quiescence.Lock()
	defer r.store.quiescence.Unlock()
	r.store.quiescence.unquiescedOrAwake[r.RangeID] = struct{}{}
	leader, ok := r.lookupLeaderRLocked()
	// This shouldn't be possible because the replica's view of its leader can't
	// change while it's asleep. A change in the leader's descriptor would require
	// consensus, which would wake up the replica.
	// TODO(mira): turn into an assertion?
	if !ok {
		ctx := r.AnnotateCtx(context.TODO())
		log.Warningf(ctx, "asleep replica %v can't find its leader", r.replicaID)
		return
	}
	delete(r.store.quiescence.asleepByLeaderStore[leader.StoreID], r)
	if len(r.store.quiescence.asleepByLeaderStore[leader.StoreID]) == 0 {
		delete(r.store.quiescence.asleepByLeaderStore, leader.StoreID)
	}
}

// lookupLeaderRLocked attempts to find the leader's descriptor.
func (r *Replica) lookupLeaderRLocked() (roachpb.ReplicaDescriptor, bool) {
	return r.shMu.state.Desc.GetReplicaDescriptorByID(r.mu.leaderID)
}
