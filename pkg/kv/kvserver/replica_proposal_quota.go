// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
)

var enableRaftProposalQuota = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.raft.proposal_quota.enabled",
	"set to true to enable waiting for and acquiring quota before issuing Raft "+
		"proposals, false to disable",
	true,
)

func (r *Replica) maybeAcquireProposalQuota(
	ctx context.Context, ba *kvpb.BatchRequest, quota uint64,
) (*quotapool.IntAlloc, error) {
	// We don't want to delay lease requests or transfers, in particular
	// expiration lease extensions. These are small and latency-sensitive.
	if ba.IsSingleRequestLeaseRequest() || ba.IsSingleTransferLeaseRequest() {
		return nil, nil
	}

	r.mu.RLock()
	enabled := r.getQuotaPoolEnabledRLocked(ctx)
	quotaPool := r.mu.proposalQuota
	r.mu.RUnlock()

	if !enabled {
		return nil, nil
	}
	// Quota acquisition only takes place on the leader replica,
	// r.mu.proposalQuota is set to nil if a node is a follower (see
	// updateProposalQuotaRaftMuLocked). For the cases where the range lease
	// holder is not the same as the range leader, i.e. the lease holder is a
	// follower, r.mu.proposalQuota == nil. This means all quota acquisitions
	// go through without any throttling whatsoever but given how short lived
	// these scenarios are we don't try to remedy any further.
	//
	// NB: It is necessary to allow proposals with a nil quota pool to go
	// through, for otherwise a follower could never request the lease.
	if quotaPool == nil {
		return nil, nil
	}

	// Trace if we're running low on available proposal quota; it might explain
	// why we're taking so long.
	if log.HasSpan(ctx) {
		if q := quotaPool.ApproximateQuota(); q < quotaPool.Capacity()/10 {
			log.Eventf(ctx, "quota running low, currently available ~%d", q)
		}
	}
	alloc, err := quotaPool.Acquire(ctx, quota)
	// Let quotapool errors due to being closed pass through.
	if errors.HasType(err, (*quotapool.ErrClosed)(nil)) {
		err = nil
	}
	return alloc, err
}

func quotaPoolEnabledForRange(desc *roachpb.RangeDescriptor) bool {
	// The NodeLiveness range does not use a quota pool. We don't want to
	// throttle updates to the NodeLiveness range even if a follower is falling
	// behind because this could result in cascading failures.
	return !bytes.HasPrefix(desc.StartKey, keys.NodeLivenessPrefix)
}

var logSlowRaftProposalQuotaAcquisition = quotapool.OnSlowAcquisition(
	base.SlowRequestThreshold, quotapool.LogSlowAcquisition,
)

// getQuotaPoolEnabledRLocked returns whether the quota pool is enabled for the
// replica. The quota pool is enabled iff all of the following conditions are
// met:
//
//  1. "kv.raft.proposal_quota.enabled" is true
//  2. replication admission control is not using pull mode
//  3. the range is not the NodeLiveness range
//
// Replica.mu must be RLocked.
func (r *Replica) getQuotaPoolEnabledRLocked(ctx context.Context) bool {
	return enableRaftProposalQuota.Get(&r.store.cfg.Settings.SV) &&
		!r.shouldReplicationAdmissionControlUsePullMode(ctx) &&
		quotaPoolEnabledForRange(r.shMu.state.Desc)
}

// shouldReplicationAdmissionControlUsePullMode returns whether replication
// admission/flow control should use pull mode, which allows for a send-queue
// and disabled raft's own flow control.
func (r *Replica) shouldReplicationAdmissionControlUsePullMode(ctx context.Context) bool {
	return r.store.cfg.Settings.Version.IsActive(ctx, clusterversion.V24_3_UseRACV2Full) &&
		kvflowcontrol.Mode.Get(&r.store.cfg.Settings.SV) == kvflowcontrol.ApplyToAll &&
		kvflowcontrol.Enabled.Get(&r.store.cfg.Settings.SV)
}

func (r *Replica) replicationAdmissionControlModeToUse(ctx context.Context) rac2.RaftMsgAppMode {
	// TODO(sumeer): remove the false.
	usePullMode := r.shouldReplicationAdmissionControlUsePullMode(ctx) && false
	if usePullMode {
		return rac2.MsgAppPull
	}
	return rac2.MsgAppPush
}

func (r *Replica) updateProposalQuotaRaftMuLocked(
	ctx context.Context, lastLeaderID roachpb.ReplicaID,
) {
	now := r.Clock().PhysicalTime()
	r.mu.Lock()
	defer r.mu.Unlock()

	status := r.mu.internalRaftGroup.BasicStatus()
	enabled := r.getQuotaPoolEnabledRLocked(ctx)
	// NB: We initialize and destroy the quota pool here, on leadership changes
	// and when it's enabled/disabled via settings (see below). This obviates the
	// need for a setting change callback, as handleRaftReady (the caller), will
	// be called at least at the tick interval for a non-quiescent range.
	shouldInitQuotaPool := false
	if r.mu.leaderID != lastLeaderID {
		if r.replicaID == r.mu.leaderID {
			r.mu.lastUpdateTimes = make(map[roachpb.ReplicaID]time.Time)
			r.mu.lastUpdateTimes.updateOnBecomeLeader(r.shMu.state.Desc.Replicas().Descriptors(), now)
			r.mu.replicaFlowControlIntegration.onBecameLeader(ctx)
			r.mu.lastProposalAtTicks = r.mu.ticks // delay imminent quiescence
			// We're the new leader but we only create the quota pool if it's enabled
			// for the range and generally enabled for the cluster, see
			// getQuotaPoolEnabledRLocked.
			if enabled {
				// Raft may propose commands itself (specifically the empty
				// commands when leadership changes), and these commands don't go
				// through the code paths where we acquire quota from the pool. To
				// offset this we reset the quota pool whenever leadership changes
				// hands.
				shouldInitQuotaPool = true
			}
		} else {
			// We're becoming a follower.
			r.mu.lastUpdateTimes = nil
			r.mu.replicaFlowControlIntegration.onBecameFollower(ctx)
			if r.mu.proposalQuota != nil {
				// We unblock all ongoing and subsequent quota acquisition goroutines
				// (if any) and release the quotaReleaseQueue so its allocs are pooled.
				r.mu.proposalQuota.Close("leader change")
				r.mu.proposalQuota.Release(r.mu.quotaReleaseQueue...)
				r.mu.quotaReleaseQueue = nil
				r.mu.proposalQuota = nil
			}
			return
		}
	} else if r.replicaID == r.mu.leaderID && r.mu.proposalQuota == nil && enabled {
		r.mu.lastProposalAtTicks = r.mu.ticks // delay imminent quiescence
		shouldInitQuotaPool = true
	} else if r.replicaID != r.mu.leaderID {
		// We're a follower and have been since the last update. Nothing to do.
		return
	}

	if shouldInitQuotaPool {
		// We're becoming the leader and the quota pool is enabled for the range,
		// or we were the leader and the quota pool has dynamically been enabled.
		//
		// Initialize the proposalQuotaBaseIndex at the applied index.
		// After the proposal quota is enabled all entries applied by this replica
		// will be appended to the quotaReleaseQueue. The proposalQuotaBaseIndex
		// and the quotaReleaseQueue together track status.Applied exactly.
		r.mu.proposalQuotaBaseIndex = kvpb.RaftIndex(status.Applied)
		if r.mu.proposalQuota != nil {
			log.Fatal(ctx, "proposalQuota was not nil before becoming the leader")
		}
		if releaseQueueLen := len(r.mu.quotaReleaseQueue); releaseQueueLen != 0 {
			log.Fatalf(ctx, "len(r.mu.quotaReleaseQueue) = %d, expected 0", releaseQueueLen)
		}
		r.mu.proposalQuota = quotapool.NewIntPool(
			"raft proposal",
			uint64(r.store.cfg.RaftProposalQuota),
			logSlowRaftProposalQuotaAcquisition,
		)
	}

	// We're still the leader.
	// Find the minimum index that active followers have acknowledged.

	// commitIndex is used to determine whether a newly added replica has fully
	// caught up.
	commitIndex := kvpb.RaftIndex(status.Commit)
	// Initialize minIndex to the currently applied index. The below progress
	// checks will only decrease the minIndex. Given that the quotaReleaseQueue
	// cannot correspond to values beyond the applied index there's no reason
	// to consider progress beyond it as meaningful.
	minIndex := kvpb.RaftIndex(status.Applied)

	r.mu.internalRaftGroup.WithBasicProgress(func(id raftpb.PeerID, progress tracker.BasicProgress) {
		rep, ok := r.shMu.state.Desc.GetReplicaDescriptorByID(roachpb.ReplicaID(id))
		if !ok {
			return
		}

		// Only consider followers that are active. Inactive ones don't decrease
		// minIndex - i.e. they don't hold up releasing quota.
		//
		// The policy for determining who's active is stricter than the one used
		// for purposes of quiescing. Failure to consider a dead/stuck node as
		// such for the purposes of releasing quota can have bad consequences
		// (writes will stall), whereas for quiescing the downside is lower.

		if !r.mu.lastUpdateTimes.isFollowerActiveSince(rep.ReplicaID, now, r.store.cfg.RangeLeaseDuration) {
			return
		}
		// At this point, we know that either we communicated with this replica
		// recently, or we became the leader recently. The latter case is ambiguous
		// w.r.t. the actual state of that replica, but it is temporary.

		// Note that the Match field has different semantics depending on
		// the State.
		//
		// In state ProgressStateReplicate, the Match index is optimistically
		// updated whenever a message is *sent* (not received). Due to Raft
		// flow control, only a reasonably small amount of data can be en
		// route to a given follower at any point in time.
		//
		// In state ProgressStateProbe, the Match index equals Next-1, and
		// it tells us the leader's optimistic best guess for the right log
		// index (and will try once per heartbeat interval to update its
		// estimate). In the usual case, the follower responds with a hint
		// when it rejects the first probe and the leader replicates or
		// sends a snapshot. In the case in which the follower does not
		// respond, the leader reduces Match by one each heartbeat interval.
		// But if the follower does not respond, we've already filtered it
		// out above. We use the Match index as is, even though the follower
		// likely isn't there yet because that index won't go up unless the
		// follower is actually catching up, so it won't cause it to fall
		// behind arbitrarily.
		//
		// Another interesting tidbit about this state is that the Paused
		// field is usually true as it is used to limit the number of probes
		// (i.e. appends) sent to this follower to one per heartbeat
		// interval.
		//
		// In state ProgressStateSnapshot, the Match index is the last known
		// (possibly optimistic, depending on previous state) index before
		// the snapshot went out. Once the snapshot applies, the follower
		// will enter ProgressStateReplicate again. So here the Match index
		// works as advertised too.

		// Only consider followers who are in advance of the quota base
		// index. This prevents a follower from coming back online and
		// preventing throughput to the range until it has caught up.
		if kvpb.RaftIndex(progress.Match) < r.mu.proposalQuotaBaseIndex {
			return
		}
		if _, paused := r.mu.pausedFollowers[roachpb.ReplicaID(id)]; paused {
			// We are dropping MsgApp to this store, so we are effectively treating
			// it as non-live for the purpose of replication and are letting it fall
			// behind intentionally.
			//
			// See #79215.
			return
		}
		if progress.Match > 0 && kvpb.RaftIndex(progress.Match) < minIndex {
			minIndex = kvpb.RaftIndex(progress.Match)
		}
		// If this is the most recently added replica, and it has caught up, clear
		// our state that was tracking it. This is unrelated to managing proposal
		// quota, but this is a convenient place to do so.
		if rep.ReplicaID == r.mu.lastReplicaAdded && kvpb.RaftIndex(progress.Match) >= commitIndex {
			r.mu.lastReplicaAdded = 0
			r.mu.lastReplicaAddedTime = time.Time{}
		}
	})

	if enabled {
		if r.mu.proposalQuotaBaseIndex < minIndex {
			// We've persisted at least minIndex-r.mu.proposalQuotaBaseIndex entries
			// to the raft log on all 'active' replicas and applied at least minIndex
			// entries locally since last we checked, so we are able to release the
			// difference back to the quota pool.
			numReleases := minIndex - r.mu.proposalQuotaBaseIndex

			// NB: Release deals with cases where allocs being released do not originate
			// from this incarnation of quotaReleaseQueue, which can happen if a
			// proposal acquires quota while this replica is the raft leader in some
			// term and then commits while at a different term.
			r.mu.proposalQuota.Release(r.mu.quotaReleaseQueue[:numReleases]...)
			r.mu.quotaReleaseQueue = r.mu.quotaReleaseQueue[numReleases:]
			r.mu.proposalQuotaBaseIndex += numReleases
		}
		// Assert the sanity of the base index and the queue. Queue entries should
		// correspond to applied entries. It should not be possible for the base
		// index and the not yet released applied entries to not equal the applied
		// index.
		releasableIndex := r.mu.proposalQuotaBaseIndex + kvpb.RaftIndex(len(r.mu.quotaReleaseQueue))
		if releasableIndex != kvpb.RaftIndex(status.Applied) {
			log.Fatalf(ctx, "proposalQuotaBaseIndex (%d) + quotaReleaseQueueLen (%d) = %d"+
				" must equal the applied index (%d)",
				r.mu.proposalQuotaBaseIndex, len(r.mu.quotaReleaseQueue), releasableIndex,
				status.Applied)
		}
	} else if !enabled && r.mu.proposalQuota != nil {
		// The quota pool was previously enabled on this leader, but it is no longer
		// enabled. We release all quota back to the pool and close the pool.
		r.mu.proposalQuota.Close("quota pool disabled")
		r.mu.proposalQuota.Release(r.mu.quotaReleaseQueue...)
		r.mu.quotaReleaseQueue = nil
		r.mu.proposalQuota = nil
	}

	// Tick the replicaFlowControlIntegration interface. This is as convenient a
	// place to do it as any other. Much like the quota pool code above, the
	// flow control integration layer considers raft progress state for
	// individual replicas, and whether they've been recently active.
	r.mu.replicaFlowControlIntegration.onRaftTicked(ctx)
}
