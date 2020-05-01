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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

func (r *Replica) quiesceLocked() bool {
	ctx := r.AnnotateCtx(context.TODO())
	if r.hasPendingProposalsRLocked() {
		if log.V(3) {
			log.Infof(ctx, "not quiescing: pending commands")
		}
		return false
	}
	// Note that we're not calling r.hasPendingProposalQuotaRLocked(). That has
	// been checked on the leaseholder before deciding to quiesce. There's no
	// point in checking it followers since the quota is reset when the lease
	// changes.

	if !r.mu.quiescent {
		if log.V(3) {
			log.Infof(ctx, "quiescing %d", r.RangeID)
		}
		r.mu.quiescent = true
		r.store.unquiescedReplicas.Lock()
		delete(r.store.unquiescedReplicas.m, r.RangeID)
		r.store.unquiescedReplicas.Unlock()
	} else if log.V(4) {
		log.Infof(ctx, "already quiesced")
	}
	return true
}

func (r *Replica) unquiesce() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unquiesceLocked()
}

func (r *Replica) unquiesceLocked() {
	r.unquiesceWithOptionsLocked(true /* campaignOnWake */)
}

func (r *Replica) unquiesceWithOptionsLocked(campaignOnWake bool) {
	if r.mu.quiescent && r.mu.internalRaftGroup != nil {
		ctx := r.AnnotateCtx(context.TODO())
		if log.V(3) {
			log.Infof(ctx, "unquiescing %d", r.RangeID)
		}
		r.mu.quiescent = false
		r.store.unquiescedReplicas.Lock()
		r.store.unquiescedReplicas.m[r.RangeID] = struct{}{}
		r.store.unquiescedReplicas.Unlock()
		if campaignOnWake {
			r.maybeCampaignOnWakeLocked(ctx)
		}
		// NB: we know there's a non-nil RaftStatus because internalRaftGroup isn't nil.
		r.mu.lastUpdateTimes.updateOnUnquiesce(
			r.mu.state.Desc.Replicas().All(), r.raftStatusRLocked().Progress, timeutil.Now(),
		)
	}
}

func (r *Replica) unquiesceAndWakeLeaderLocked() {
	if r.mu.quiescent && r.mu.internalRaftGroup != nil {
		ctx := r.AnnotateCtx(context.TODO())
		if log.V(3) {
			log.Infof(ctx, "unquiescing %d: waking leader", r.RangeID)
		}
		r.mu.quiescent = false
		r.store.unquiescedReplicas.Lock()
		r.store.unquiescedReplicas.m[r.RangeID] = struct{}{}
		r.store.unquiescedReplicas.Unlock()
		r.maybeCampaignOnWakeLocked(ctx)
		// Propose an empty command which will wake the leader.
		data := encodeRaftCommand(raftVersionStandard, makeIDKey(), nil)
		_ = r.mu.internalRaftGroup.Propose(data)
	}
}

// maybeQuiesceLocked checks to see if the replica is quiescable and initiates
// quiescence if it is. Returns true if the replica has been quiesced and false
// otherwise.
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
// The supplied livenessMap maps from node ID to a boolean indicating
// liveness. A range may be quiesced in the presence of non-live
// replicas if the remaining live replicas all meet the quiesce
// requirements. When a node considered non-live becomes live, the
// node liveness instance invokes a callback which causes all nodes to
// wakes up any ranges containing replicas owned by the newly-live
// node, allowing the out-of-date replicas to be brought back up to date.
// If livenessMap is nil, liveness data will not be used, meaning no range
// will quiesce if any replicas are behind, whether or not they are live.
// If any entry in the livenessMap is nil, then the missing node ID is
// treated as not live.
//
// TODO(peter): There remains a scenario in which a follower is left unquiesced
// while the leader is quiesced: the follower's receive queue is full and the
// "quiesce" message is dropped. This seems very very unlikely because if the
// follower isn't keeping up with raft messages it is unlikely that the leader
// would quiesce. The fallout from this situation are undesirable raft
// elections which will cause throughput hiccups to the range, but not
// correctness issues.
func (r *Replica) maybeQuiesceLocked(ctx context.Context, livenessMap IsLiveMap) bool {
	status, ok := shouldReplicaQuiesce(ctx, r, r.store.Clock().Now(), livenessMap)
	if !ok {
		return false
	}
	return r.quiesceAndNotifyLocked(ctx, status)
}

type quiescer interface {
	descRLocked() *roachpb.RangeDescriptor
	raftStatusRLocked() *raft.Status
	raftLastIndexLocked() (uint64, error)
	hasRaftReadyRLocked() bool
	hasPendingProposalsRLocked() bool
	hasPendingProposalQuotaRLocked() bool
	ownsValidLeaseRLocked(ts hlc.Timestamp) bool
	mergeInProgressRLocked() bool
	isDestroyedRLocked() (DestroyReason, error)
}

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
	ctx context.Context, q quiescer, now hlc.Timestamp, livenessMap IsLiveMap,
) (*raft.Status, bool) {
	if testingDisableQuiescence {
		return nil, false
	}
	if q.hasPendingProposalsRLocked() {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: proposals pending")
		}
		return nil, false
	}
	// Don't quiesce if there's outstanding quota - it can lead to deadlock. This
	// condition is largely subsumed by the upcoming replication state check,
	// except that the conditions for replica availability are different, and the
	// timing of releasing quota is unrelated to this function.
	if q.hasPendingProposalQuotaRLocked() {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: replication quota outstanding")
		}
		return nil, false
	}
	if q.mergeInProgressRLocked() {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: merge in progress")
		}
		return nil, false
	}
	if _, err := q.isDestroyedRLocked(); err != nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: replica destroyed")
		}
		return nil, false
	}
	status := q.raftStatusRLocked()
	if status == nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: dormant Raft group")
		}
		return nil, false
	}
	if status.SoftState.RaftState != raft.StateLeader {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: not leader")
		}
		return nil, false
	}
	if status.LeadTransferee != 0 {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: leader transfer to %d in progress", status.LeadTransferee)
		}
		return nil, false
	}
	// Only quiesce if this replica is the leaseholder as well;
	// otherwise the replica which is the valid leaseholder may have
	// pending commands which it's waiting on this leader to propose.
	if !q.ownsValidLeaseRLocked(now) {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: not leaseholder")
		}
		return nil, false
	}
	// We need all of Applied, Commit, LastIndex and Progress.Match indexes to be
	// equal in order to quiesce.
	if status.Applied != status.Commit {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: applied (%d) != commit (%d)",
				status.Applied, status.Commit)
		}
		return nil, false
	}
	lastIndex, err := q.raftLastIndexLocked()
	if err != nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: %v", err)
		}
		return nil, false
	}
	if status.Commit != lastIndex {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: commit (%d) != lastIndex (%d)",
				status.Commit, lastIndex)
		}
		return nil, false
	}

	var foundSelf bool
	for _, rep := range q.descRLocked().Replicas().All() {
		if uint64(rep.ReplicaID) == status.ID {
			foundSelf = true
		}
		if progress, ok := status.Progress[uint64(rep.ReplicaID)]; !ok {
			if log.V(4) {
				log.Infof(ctx, "not quiescing: could not locate replica %d in progress: %+v",
					rep.ReplicaID, progress)
			}
			return nil, false
		} else if progress.Match != status.Applied {
			// Skip any node in the descriptor which is not live.
			if livenessMap != nil && !livenessMap[rep.NodeID].IsLive {
				if log.V(4) {
					log.Infof(ctx, "skipping node %d because not live. Progress=%+v",
						rep.NodeID, progress)
				}
				continue
			}
			if log.V(4) {
				log.Infof(ctx, "not quiescing: replica %d match (%d) != applied (%d)",
					rep.ReplicaID, progress.Match, status.Applied)
			}
			return nil, false
		}
	}
	if !foundSelf {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: %d not found in progress: %+v",
				status.ID, status.Progress)
		}
		return nil, false
	}
	if q.hasRaftReadyRLocked() {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: raft ready")
		}
		return nil, false
	}
	return status, true
}

func (r *Replica) quiesceAndNotifyLocked(ctx context.Context, status *raft.Status) bool {
	fromReplica, fromErr := r.getReplicaDescriptorByIDRLocked(r.mu.replicaID, r.mu.lastToReplica)
	if fromErr != nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: cannot find from replica (%d)", r.mu.replicaID)
		}
		return false
	}

	if !r.quiesceLocked() {
		return false
	}

	for id, prog := range status.Progress {
		if roachpb.ReplicaID(id) == r.mu.replicaID {
			continue
		}
		toReplica, toErr := r.getReplicaDescriptorByIDRLocked(
			roachpb.ReplicaID(id), r.mu.lastFromReplica)
		if toErr != nil {
			if log.V(4) {
				log.Infof(ctx, "failed to quiesce: cannot find to replica (%d)", id)
			}
			r.unquiesceLocked()
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
		if prog.Match < status.Commit {
			commit = prog.Match
			quiesce = false
		}
		msg := raftpb.Message{
			From:   uint64(r.mu.replicaID),
			To:     id,
			Type:   raftpb.MsgHeartbeat,
			Term:   status.Term,
			Commit: commit,
		}

		if !r.maybeCoalesceHeartbeat(ctx, msg, toReplica, fromReplica, quiesce) {
			log.Fatalf(ctx, "failed to coalesce known heartbeat: %v", msg)
		}
	}
	return true
}
