// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func (r *Replica) quiesceLocked(ctx context.Context, lagging laggingReplicaSet) {
	if !r.mu.quiescent {
		if log.V(3) {
			log.Infof(ctx, "quiescing %d", r.RangeID)
		}
		r.mu.quiescent = true
		r.mu.laggingFollowersOnQuiesce = lagging
		r.store.unquiescedReplicas.Lock()
		delete(r.store.unquiescedReplicas.m, r.RangeID)
		r.store.unquiescedReplicas.Unlock()
	} else if log.V(4) {
		log.Infof(ctx, "already quiesced")
	}
}

func (r *Replica) maybeUnquiesce() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maybeUnquiesceLocked()
}

func (r *Replica) maybeUnquiesceLocked() bool {
	return r.maybeUnquiesceWithOptionsLocked(true /* campaignOnWake */)
}

func (r *Replica) maybeUnquiesceWithOptionsLocked(campaignOnWake bool) bool {
	if !r.canUnquiesceRLocked() {
		return false
	}
	ctx := r.AnnotateCtx(context.TODO())
	if log.V(3) {
		log.Infof(ctx, "unquiescing %d", r.RangeID)
	}
	r.mu.quiescent = false
	r.mu.laggingFollowersOnQuiesce = nil
	r.store.unquiescedReplicas.Lock()
	r.store.unquiescedReplicas.m[r.RangeID] = struct{}{}
	r.store.unquiescedReplicas.Unlock()
	if campaignOnWake {
		r.maybeCampaignOnWakeLocked(ctx)
	}
	// NB: we know there's a non-nil RaftStatus because internalRaftGroup isn't nil.
	r.mu.lastUpdateTimes.updateOnUnquiesce(
		r.mu.state.Desc.Replicas().Descriptors(), r.raftSparseStatusRLocked().Progress, timeutil.Now(),
	)
	return true
}

func (r *Replica) maybeUnquiesceAndWakeLeaderLocked() bool {
	if !r.canUnquiesceRLocked() {
		return false
	}
	ctx := r.AnnotateCtx(context.TODO())
	if log.V(3) {
		log.Infof(ctx, "unquiescing %d: waking leader", r.RangeID)
	}
	r.mu.quiescent = false
	r.mu.laggingFollowersOnQuiesce = nil
	r.store.unquiescedReplicas.Lock()
	r.store.unquiescedReplicas.m[r.RangeID] = struct{}{}
	r.store.unquiescedReplicas.Unlock()
	r.maybeCampaignOnWakeLocked(ctx)
	// Propose an empty command which will wake the leader.
	data := raftlog.EncodeRaftCommand(raftlog.EntryEncodingStandardWithoutAC, makeIDKey(), nil)
	_ = r.mu.internalRaftGroup.Propose(data)
	return true
}

func (r *Replica) canUnquiesceRLocked() bool {
	return r.mu.quiescent &&
		// If the replica is uninitialized (i.e. it contains no replicated state),
		// it is not allowed to unquiesce and begin Tick()'ing itself.
		//
		// Keeping uninitialized replicas quiesced even in the presence of Raft
		// traffic avoids wasted work. We could Tick() these replicas, but doing so
		// is unnecessary because uninitialized replicas can never win elections, so
		// there is no reason for them to ever call an election. In fact,
		// uninitialized replicas do not even know who their peers are, so there
		// would be no way for them to call an election or for them to send any
		// other non-reactive message. As a result, all work performed by an
		// uninitialized replica is reactive and in response to incoming messages
		// (see processRequestQueue).
		//
		// There are multiple ways for an uninitialized replica to be created and
		// then abandoned, and we don't do a good job garbage collecting them at a
		// later point (see https://github.com/cockroachdb/cockroach/issues/73424),
		// so it is important that they are cheap. Keeping them quiesced instead of
		// letting them unquiesce and tick every 200ms indefinitely avoids a
		// meaningful amount of periodic work for each uninitialized replica.
		r.IsInitialized() &&
		// A replica's Raft group begins in a dormant state and is initialized
		// lazily in response to any Raft traffic (see stepRaftGroup) or KV request
		// traffic (see maybeInitializeRaftGroup). If it has yet to be initialized,
		// let it remain quiesced. The Raft group will be initialized soon enough.
		r.mu.internalRaftGroup != nil
}

// maybeQuiesceRaftMuLockedReplicaMuLocked checks to see if the replica is
// quiescable and initiates quiescence if it is. Returns true if the replica has
// been quiesced and false otherwise.
//
// A quiesced range is not ticked and thus doesn't create MsgHeartbeat requests
// or cause elections. The Raft leader for a range checks various
// pre-conditions: no pending raft commands, no pending raft ready, all of the
// followers are up to date, etc. Quiescence is initiated by a special
// MsgHeartbeat that is tagged as Quiesce. Upon receipt (see
// Store.processRaftRequestWithReplica), the follower checks to see if the
// term/commit matches and marks the local replica as quiescent. If the
// term/commit do not match the MsgHeartbeat is passed through to Raft which
// will generate a MsgHeartbeatResp that will unquiesce the sender.
//
// Any Raft operation on the local replica will unquiesce the Replica. For
// example, a Raft operation initiated on a follower will unquiesce the
// follower which will send a MsgProp to the leader that will unquiesce the
// leader. If the leader of a quiesced range dies, followers will not notice,
// though any request directed to the range will eventually end up on a
// follower which will unquiesce the follower and lead to an election. When a
// follower unquiesces for a reason other than receiving a raft message or
// proposing a raft command (for example the concurrent enqueuing of a tick),
// it wakes the leader by sending an empty message proposal. This avoids
// unnecessary elections due to bugs in which a follower is left unquiesced
// while the leader is quiesced.
//
// Note that both the quiesce and wake-the-leader messages can be dropped or
// reordered by the transport. The wake-the-leader message is termless so it
// won't affect elections and, while it triggers reproprosals that won't cause
// problems on reordering. If the wake-the-leader message is dropped the leader
// won't be woken and the follower will eventually call an election.
//
// If the quiesce message is dropped the follower which missed it will not
// quiesce and will eventually cause an election. The quiesce message is tagged
// with the current term and commit index. If the quiesce message is reordered
// it will either still apply to the recipient or the recipient will have moved
// forward and the quiesce message will fall back to being a heartbeat.
//
// The supplied livenessMap maps from node ID to a boolean indicating liveness.
// A range may be quiesced in the presence of non-live replicas if the remaining
// live replicas all meet the quiesce requirements. When a node considered
// non-live becomes live, the node liveness instance invokes a callback which
// causes all nodes to wake up any ranges containing replicas owned by the
// newly-live node that were out-of-date at the time of quiescence, allowing the
// out-of-date replicas to be brought back up to date. If livenessMap is nil,
// liveness data will not be used, meaning no range will quiesce if any replicas
// are behind, whether or not they are live. If any entry in the livenessMap is
// nil, then the missing node ID is treated as live and will prevent the range
// from quiescing.
//
// TODO(peter): There remains a scenario in which a follower is left unquiesced
// while the leader is quiesced: the follower's receive queue is full and the
// "quiesce" message is dropped. This seems very very unlikely because if the
// follower isn't keeping up with raft messages it is unlikely that the leader
// would quiesce. The fallout from this situation are undesirable raft
// elections which will cause throughput hiccups to the range, but not
// correctness issues.
func (r *Replica) maybeQuiesceRaftMuLockedReplicaMuLocked(
	ctx context.Context, now hlc.ClockTimestamp, livenessMap livenesspb.IsLiveMap,
) bool {
	status, lagging, ok := shouldReplicaQuiesce(ctx, r, now, livenessMap, r.mu.pausedFollowers)
	if !ok {
		return false
	}
	return r.quiesceAndNotifyRaftMuLockedReplicaMuLocked(ctx, status, lagging)
}

type quiescer interface {
	descRLocked() *roachpb.RangeDescriptor
	isRaftLeaderRLocked() bool
	raftSparseStatusRLocked() *raftSparseStatus
	raftBasicStatusRLocked() raft.BasicStatus
	raftLastIndexRLocked() uint64
	hasRaftReadyRLocked() bool
	hasPendingProposalsRLocked() bool
	hasPendingProposalQuotaRLocked() bool
	ownsValidLeaseRLocked(ctx context.Context, now hlc.ClockTimestamp) bool
	mergeInProgressRLocked() bool
	isDestroyedRLocked() (DestroyReason, error)
}

// laggingReplicaSet is a set containing liveness information about replicas
// that were dead when a Raft leader decided to quiesce its range and were
// lagging behind the quiescence log index (meaning they would have prevented
// quiescence had they been alive). If any replica (leader or follower) becomes
// aware that a replica in this set has become live, it should unquiesce the
// range so that the replica can be caught back up.
type laggingReplicaSet []livenesspb.Liveness

// MemberStale returns whether the provided piece of liveness information is
// related to a node contained in the set and is newer information than the
// respective information in the set.
func (s laggingReplicaSet) MemberStale(l livenesspb.Liveness) bool {
	for _, laggingL := range s {
		if laggingL.NodeID == l.NodeID {
			return laggingL.Compare(l) < 0
		}
	}
	return false
}

// AnyMemberStale returns whether any liveness information in the set is older
// than liveness information contained in the IsLiveMap.
func (s laggingReplicaSet) AnyMemberStale(livenessMap livenesspb.IsLiveMap) bool {
	for _, laggingL := range s {
		if l, ok := livenessMap[laggingL.NodeID]; ok {
			if laggingL.Compare(l.Liveness) < 0 {
				return true
			}
		}
	}
	return false
}

// Implement Sort.Interface.
func (s laggingReplicaSet) Len() int           { return len(s) }
func (s laggingReplicaSet) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s laggingReplicaSet) Less(i, j int) bool { return s[i].NodeID < s[j].NodeID }

// shouldReplicaQuiesce determines if a replica should be quiesced. All of the
// access to Replica internals are gated by the quiescer interface to
// facilitate testing. Returns the raft.Status and true on success, and (nil,
// false) on failure.
//
// Deciding to quiesce can race with requests being evaluated and their
// proposals. Any proposal happening after the range has quiesced will
// un-quiesce the range.
//
// A replica should quiesce if all the following hold:
// a) The leaseholder and the leader are collocated. We don't want to quiesce
// otherwise as we don't want to quiesce while a leader election is in progress,
// and also we don't want to quiesce if another replica might have commands
// pending that require this leader for proposing them. Note that, after the
// leaseholder decides to quiesce, followers can still refuse quiescing if they
// have pending commands.
// b) There are no commands in-flight proposed by this leaseholder. Clients can
// be waiting for results while there's pending proposals.
// c) There is no outstanding proposal quota. Quiescing while there's quota
// outstanding can lead to deadlock. See #46699.
// d) All the live followers are caught up. We don't want to quiesce when
// followers are behind because then they might not catch up until we
// un-quiesce. We like it when everybody is caught up because otherwise
// failovers can take longer.
//
// NOTE: The last 3 conditions are fairly, but not completely, overlapping.
func shouldReplicaQuiesce(
	ctx context.Context,
	q quiescer,
	now hlc.ClockTimestamp,
	livenessMap livenesspb.IsLiveMap,
	pausedFollowers map[roachpb.ReplicaID]struct{},
) (*raftSparseStatus, laggingReplicaSet, bool) {
	if testingDisableQuiescence {
		return nil, nil, false
	}
	if !q.isRaftLeaderRLocked() { // fast path
		if log.V(4) {
			log.Infof(ctx, "not quiescing: not leader")
		}
		return nil, nil, false
	}
	if q.hasPendingProposalsRLocked() {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: proposals pending")
		}
		return nil, nil, false
	}
	// Don't quiesce if there's outstanding quota - it can lead to deadlock. This
	// condition is largely subsumed by the upcoming replication state check,
	// except that the conditions for replica availability are different, and the
	// timing of releasing quota is unrelated to this function.
	if q.hasPendingProposalQuotaRLocked() {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: replication quota outstanding")
		}
		return nil, nil, false
	}
	if q.mergeInProgressRLocked() {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: merge in progress")
		}
		return nil, nil, false
	}
	if _, err := q.isDestroyedRLocked(); err != nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: replica destroyed")
		}
		return nil, nil, false
	}

	if q.hasRaftReadyRLocked() {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: raft ready")
		}
		return nil, nil, false
	}

	status := q.raftSparseStatusRLocked()
	if status == nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: dormant Raft group")
		}
		return nil, nil, false
	}
	if status.SoftState.RaftState != raft.StateLeader {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: not leader")
		}
		return nil, nil, false
	}
	if status.LeadTransferee != 0 {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: leader transfer to %d in progress", status.LeadTransferee)
		}
		return nil, nil, false
	}
	// Only quiesce if this replica is the leaseholder as well;
	// otherwise the replica which is the valid leaseholder may have
	// pending commands which it's waiting on this leader to propose.
	if !q.ownsValidLeaseRLocked(ctx, now) {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: not leaseholder")
		}
		return nil, nil, false
	}
	// We need all of Applied, Commit, LastIndex and Progress.Match indexes to be
	// equal in order to quiesce.
	if status.Applied != status.Commit {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: applied (%d) != commit (%d)",
				status.Applied, status.Commit)
		}
		return nil, nil, false
	}
	lastIndex := q.raftLastIndexRLocked()
	if status.Commit != lastIndex {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: commit (%d) != lastIndex (%d)",
				status.Commit, lastIndex)
		}
		return nil, nil, false
	}

	if len(pausedFollowers) > 0 {
		// TODO(tbg): we should use a mechanism similar to livenessMap below (including a
		// callback that unquiesces when paused followers unpause, since they will by
		// definition be lagging). This was a little too much code churn at the time
		// at which this comment was written.
		//
		// See: https://github.com/cockroachdb/cockroach/issues/84252
		if log.V(4) {
			log.Infof(ctx, "not quiescing: overloaded followers %v", pausedFollowers)
		}
		return nil, nil, false
	}

	var foundSelf bool
	var lagging laggingReplicaSet
	for _, rep := range q.descRLocked().Replicas().Descriptors() {
		if uint64(rep.ReplicaID) == status.ID {
			foundSelf = true
		}
		if progress, ok := status.Progress[uint64(rep.ReplicaID)]; !ok {
			if log.V(4) {
				log.Infof(ctx, "not quiescing: could not locate replica %d in progress: %+v",
					rep.ReplicaID, progress)
			}
			return nil, nil, false
		} else if progress.Match != status.Applied {
			// Skip any node in the descriptor which is not live. Instead, add
			// the node to the set of replicas lagging the quiescence index.
			if l, ok := livenessMap[rep.NodeID]; ok && !l.IsLive {
				if log.V(4) {
					log.Infof(ctx, "skipping node %d because not live. Progress=%+v",
						rep.NodeID, progress)
				}
				lagging = append(lagging, l.Liveness)
				continue
			}
			if log.V(4) {
				log.Infof(ctx, "not quiescing: replica %d match (%d) != applied (%d)",
					rep.ReplicaID, progress.Match, status.Applied)
			}
			return nil, nil, false
		}
	}
	sort.Sort(lagging)
	if !foundSelf {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: %d not found in progress: %+v",
				status.ID, status.Progress)
		}
		return nil, nil, false
	}
	return status, lagging, true
}

func (r *Replica) quiesceAndNotifyRaftMuLockedReplicaMuLocked(
	ctx context.Context, status *raftSparseStatus, lagging laggingReplicaSet,
) bool {
	lastToReplica, lastFromReplica := r.getLastReplicaDescriptors()
	fromReplica, fromErr := r.getReplicaDescriptorByIDRLocked(r.replicaID, lastToReplica)
	if fromErr != nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: cannot find from replica (%d)", r.replicaID)
		}
		return false
	}

	r.quiesceLocked(ctx, lagging)

	for id, prog := range status.Progress {
		if roachpb.ReplicaID(id) == r.replicaID {
			continue
		}
		toReplica, toErr := r.getReplicaDescriptorByIDRLocked(
			roachpb.ReplicaID(id), lastFromReplica)
		if toErr != nil {
			if log.V(4) {
				log.Infof(ctx, "failed to quiesce: cannot find to replica (%d)", id)
			}
			r.maybeUnquiesceLocked()
			return false
		}

		// Attach the commit as min(prog.Match, status.Commit). This is exactly
		// the same as what raft.sendHeartbeat does. See the comment there for
		// an explanation.
		//
		// If the follower is behind, we don't tell it that we're quiescing.
		// This ensures that if the follower receives the heartbeat then it will
		// unquiesce the Range and be caught up by the leader. Remember that we
		// only allow Ranges to quiesce with straggling Replicas if we believe
		// those Replicas are on dead nodes.
		commit := status.Commit
		quiesce := true
		curLagging := lagging
		if prog.Match < status.Commit {
			commit = prog.Match
			quiesce = false
			curLagging = nil
		}
		msg := raftpb.Message{
			From:   uint64(r.replicaID),
			To:     id,
			Type:   raftpb.MsgHeartbeat,
			Term:   status.Term,
			Commit: commit,
		}

		if !r.maybeCoalesceHeartbeat(ctx, msg, toReplica, fromReplica, quiesce, curLagging) {
			log.Fatalf(ctx, "failed to coalesce known heartbeat: %v", msg)
		}
	}
	return true
}

func shouldFollowerQuiesceOnNotify(
	ctx context.Context,
	q quiescer,
	msg raftpb.Message,
	lagging laggingReplicaSet,
	livenessMap livenesspb.IsLiveMap,
) bool {
	// If another replica tells us to quiesce, we verify that according to
	// it, we are fully caught up, and that we believe it to be the leader.
	// If we didn't do this, this replica could only unquiesce by means of
	// an election, which means that the request prompting the unquiesce
	// would end up with latency on the order of an election timeout.
	status := q.raftBasicStatusRLocked()
	if status.Term != msg.Term {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: local raft term is %d, incoming term is %d", status.Term, msg.Term)
		}
		return false
	}
	if status.Commit != msg.Commit {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: local raft commit index is %d, incoming commit index is %d", status.Commit, msg.Commit)
		}
		return false
	}
	if status.Lead != msg.From {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: local raft leader is %d, incoming message from %d", status.Lead, msg.From)
		}
		return false
	}

	// Don't quiesce if there's outstanding work on this replica.
	if q.hasPendingProposalsRLocked() {
		if log.V(3) {
			log.Infof(ctx, "not quiescing: pending commands")
		}
		return false
	}
	// Note that we're not checking for outstanding proposal quota. That has
	// been checked on the leaseholder before deciding to quiesce. There's
	// no point in checking it followers since the quota is reset when the
	// lease changes.
	// if q.hasPendingProposalQuotaRLocked() { ... }

	// If the leader included any lagging replicas that it decided to
	// quiesce in spite of, that means that it believed they were dead.
	// Verify that we don't know anything about these replicas that the
	// leader did not. If we do, the leader based its decision to quiesce
	// off of stale information and we should refuse to quiesce so that
	// the range can properly catch the lagging replicas up.
	//
	// This check is critical to provide the guarantee that:
	//
	//   If a quorum of replica in a Raft group is alive and at least
	//   one of these replicas is up-to-date, the Raft group will catch
	//   up any of the live, lagging replicas.
	//
	// The other two checks that combine to provide this guarantee are:
	// 1. a leader will not quiesce if it believes any lagging replicas
	//    are alive (see shouldReplicaQuiesce).
	// 2. any up-to-date replica that learns that a lagging replica is
	//    alive will unquiesce the range (see Store.nodeIsLiveCallback).
	//
	// In most cases, even if we did quiesce, the leader would unquiesce
	// once it learned of the new liveness information. However, if the
	// leader was to crash after quiescing but before learning of this
	// new information, it would never unquiesce the range, so it is
	// important that we don't quiesce so we are ready to campaign, if
	// necessary.
	if lagging.AnyMemberStale(livenessMap) {
		if log.V(3) {
			log.Infof(ctx, "not quiescing: liveness info about lagging replica stale")
		}
		return false
	}
	return true
}

func (r *Replica) maybeQuiesceOnNotify(
	ctx context.Context, msg raftpb.Message, lagging laggingReplicaSet,
) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	// NOTE: it is important that we grab the livenessMap under lock so
	// that we properly synchronize with Store.nodeIsLiveCallback, which
	// updates the map and then tries to unquiesce.
	livenessMap, _ := r.store.livenessMap.Load().(livenesspb.IsLiveMap)
	if !shouldFollowerQuiesceOnNotify(ctx, r, msg, lagging, livenessMap) {
		return false
	}

	r.quiesceLocked(ctx, lagging)
	return true
}
