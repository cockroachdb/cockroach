// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

// RaftStoreLivenessQuiescenceEnabled controls whether store liveness quiescence
// is enabled.
var RaftStoreLivenessQuiescenceEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.raft.store_liveness.quiescence.enabled",
	"controls whether store liveness quiescence is enabled",
	true,
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
	// If not supporting a fortified leader, do not fall asleep.
	if !r.raftSupportingFortifiedLeaderRLocked() {
		return false
	}
	r.mu.asleep = true
	r.store.unquiescedOrAwakeReplicas.Lock()
	defer r.store.unquiescedOrAwakeReplicas.Unlock()
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
