// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
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
	// Grab the unquiescedOrAwakeReplicas lock here to prevent races between
	// support withdrawal and falling asleep.
	r.store.unquiescedOrAwakeReplicas.Lock()
	defer r.store.unquiescedOrAwakeReplicas.Unlock()
	// If not supporting a fortified leader, do not fall asleep.
	if !r.raftSupportingFortifiedLeaderRLocked() {
		return false
	}
	// It's safe to fall asleep here: after locking unquiescedOrAwakeReplicas
	// above, this follower supports the leader. If it withdrew support since
	// then, the call to maybeWakeUpRMuLocked will wait for the unlock and wake up
	// the replica.
	r.mu.asleep = true
	delete(r.store.unquiescedOrAwakeReplicas.m, r.RangeID)
	return true
}

// maybeWakeUpRMuLocked marks a follower as awake.
func (r *Replica) maybeWakeUpRMuLocked() {
	if !r.mu.asleep {
		return
	}
	// Prevent immediate falling asleep.
	r.mu.lastMessageAtTicks = r.mu.ticks
	r.mu.asleep = false
	r.store.unquiescedOrAwakeReplicas.Lock()
	defer r.store.unquiescedOrAwakeReplicas.Unlock()
	r.store.unquiescedOrAwakeReplicas.m[r.RangeID] = struct{}{}
}
