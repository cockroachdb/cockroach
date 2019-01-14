// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// insertProposalLocked assigns a MaxLeaseIndex to a proposal and adds
// it to the pending map. Returns the assigned MaxLeaseIndex, if any.
func (r *Replica) insertProposalLocked(
	proposal *ProposalData, proposerReplica roachpb.ReplicaDescriptor, proposerLease roachpb.Lease,
) int64 {
	// Assign a lease index. Note that we do this as late as possible
	// to make sure (to the extent that we can) that we don't assign
	// (=predict) the index differently from the order in which commands are
	// proposed (and thus likely applied).
	if r.mu.lastAssignedLeaseIndex < r.mu.state.LeaseAppliedIndex {
		r.mu.lastAssignedLeaseIndex = r.mu.state.LeaseAppliedIndex
	}
	isLease := proposal.Request.IsLeaseRequest()
	if !isLease {
		r.mu.lastAssignedLeaseIndex++
	}
	proposal.command.MaxLeaseIndex = r.mu.lastAssignedLeaseIndex
	proposal.command.ProposerReplica = proposerReplica
	proposal.command.ProposerLeaseSequence = proposerLease.Sequence

	if log.V(4) {
		log.Infof(proposal.ctx, "submitting proposal %x: maxLeaseIndex=%d",
			proposal.idKey, proposal.command.MaxLeaseIndex)
	}

	if _, ok := r.mu.proposals[proposal.idKey]; ok {
		ctx := r.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "pending command already exists for %s", proposal.idKey)
	}
	r.mu.proposals[proposal.idKey] = proposal
	if isLease {
		// For lease requests, we return zero because no real MaxLeaseIndex is assigned.
		// We could also return the lastAssignedIndex but this invites confusion.
		return 0
	}
	return int64(proposal.command.MaxLeaseIndex)
}

func makeIDKey() storagebase.CmdIDKey {
	idKeyBuf := make([]byte, 0, raftCommandIDLen)
	idKeyBuf = encoding.EncodeUint64Ascending(idKeyBuf, uint64(rand.Int63()))
	return storagebase.CmdIDKey(idKeyBuf)
}

// propose prepares the necessary pending command struct and initializes a
// client command ID if one hasn't been. A verified lease is supplied
// as a parameter if the command requires a lease; nil otherwise. It
// then proposes the command to Raft if necessary and returns
// - a channel which receives a response or error upon application
// - a closure used to attempt to abandon the command. When called, it tries to
//   remove the pending command from the internal commands map. This is
//   possible until execution of the command at the local replica has already
//   begun, in which case false is returned and the client needs to continue
//   waiting for successful execution.
// - a callback to undo quota acquisition if the attempt to propose the batch
//   request to raft fails. This also cleans up the command sizes stored for
//   the corresponding proposal.
// - the MaxLeaseIndex of the resulting proposal, if any.
// - any error obtained during the creation or proposal of the command, in
//   which case the other returned values are zero.
func (r *Replica) propose(
	ctx context.Context,
	lease roachpb.Lease,
	ba roachpb.BatchRequest,
	endCmds *endCmds,
	spans *spanset.SpanSet,
) (_ chan proposalResult, _ func() bool, _ int64, pErr *roachpb.Error) {
	// TODO(nvanbenschoten): Can this be moved into Replica.requestCanProceed?
	r.mu.RLock()
	if !r.mu.destroyStatus.IsAlive() {
		err := r.mu.destroyStatus.err
		r.mu.RUnlock()
		return nil, nil, 0, roachpb.NewError(err)
	}
	r.mu.RUnlock()

	rSpan, err := keys.Range(ba)
	if err != nil {
		return nil, nil, 0, roachpb.NewError(err)
	}

	// Checking the context just before proposing can help avoid ambiguous errors.
	if err := ctx.Err(); err != nil {
		errStr := fmt.Sprintf("%s before proposing: %s", err, ba.Summary())
		log.Warning(ctx, errStr)
		return nil, nil, 0, roachpb.NewError(errors.Wrap(err, "aborted before proposing"))
	}

	// Only need to check that the request is in bounds at proposal time, not at
	// application time, because the spanlatch manager will synchronize all
	// requests (notably EndTransaction with SplitTrigger) that may cause this
	// condition to change.
	if err := r.requestCanProceed(rSpan, ba.Timestamp); err != nil {
		return nil, nil, 0, roachpb.NewError(err)
	}

	idKey := makeIDKey()
	proposal, pErr := r.requestToProposal(ctx, idKey, ba, endCmds, spans)
	log.Event(proposal.ctx, "evaluated request")

	// Pull out proposal channel to return. proposal.doneCh may be set to
	// nil if it is signaled in this function.
	proposalCh := proposal.doneCh

	// There are two cases where request evaluation does not lead to a Raft
	// proposal:
	// 1. proposal.command == nil indicates that the evaluation was a no-op
	//    and that no Raft command needs to be proposed.
	// 2. pErr != nil corresponds to a failed proposal - the command resulted
	//    in an error.
	if proposal.command == nil {
		intents := proposal.Local.DetachIntents()
		endTxns := proposal.Local.DetachEndTxns(pErr != nil /* alwaysOnly */)
		r.handleLocalEvalResult(ctx, *proposal.Local)

		pr := proposalResult{
			Reply:   proposal.Local.Reply,
			Err:     pErr,
			Intents: intents,
			EndTxns: endTxns,
		}
		proposal.finishApplication(pr)
		return proposalCh, func() bool { return false }, 0, nil
	}

	// If the request requested that Raft consensus be performed asynchronously,
	// return a proposal result immediately on the proposal's done channel.
	// The channel's capacity will be large enough to accommodate this.
	if ba.AsyncConsensus {
		if ets := proposal.Local.DetachEndTxns(false /* alwaysOnly */); len(ets) != 0 {
			// Disallow async consensus for commands with EndTxnIntents because
			// any !Always EndTxnIntent can't be cleaned up until after the
			// command succeeds.
			return nil, nil, 0, roachpb.NewErrorf("cannot perform consensus asynchronously for "+
				"proposal with EndTxnIntents=%v; %v", ets, ba)
		}

		// Fork the proposal's context span so that the proposal's context
		// can outlive the original proposer's context.
		proposal.ctx, proposal.sp = tracing.ForkCtxSpan(ctx, "async consensus")

		// Signal the proposal's response channel immediately.
		reply := *proposal.Local.Reply
		reply.Responses = append([]roachpb.ResponseUnion(nil), reply.Responses...)
		pr := proposalResult{
			Reply:   &reply,
			Intents: proposal.Local.DetachIntents(),
		}
		proposal.signalProposalResult(pr)

		// Continue with proposal...
	}

	// TODO(irfansharif): This int cast indicates that if someone configures a
	// very large max proposal size, there is weird overflow behavior and it
	// will not work the way it should.
	proposalSize := proposal.command.Size()
	if proposalSize > int(MaxCommandSize.Get(&r.store.cfg.Settings.SV)) {
		// Once a command is written to the raft log, it must be loaded
		// into memory and replayed on all replicas. If a command is
		// too big, stop it here.
		return nil, nil, 0, roachpb.NewError(errors.Errorf(
			"command is too large: %d bytes (max: %d)",
			proposalSize, MaxCommandSize.Get(&r.store.cfg.Settings.SV),
		))
	}

	// TODO(tschottdorf): blocking a proposal here will leave it dangling in the
	// closed timestamp tracker for an extended period of time, which will in turn
	// prevent the node-wide closed timestamp from making progress. This is quite
	// unfortunate; we should hoist the quota pool before the reference with the
	// closed timestamp tracker is acquired. This is better anyway; right now many
	// commands can evaluate but then be blocked on quota, which has worse memory
	// behavior.
	if err := r.maybeAcquireProposalQuota(ctx, int64(proposalSize)); err != nil {
		return nil, nil, 0, roachpb.NewError(err)
	}

	// submitProposalLocked calls withRaftGroupLocked which requires that raftMu
	// is held, but only if Replica.mu.internalRaftGroup == nil. To avoid
	// locking Replica.raftMu in the common case where the raft group is
	// non-nil, we lock only Replica.mu at first before checking the status of
	// the internal raft group. If it equals nil then we fall back to the slow
	// path of locking Replica.raftMu. However, in order to maintain our lock
	// ordering we need to lock Replica.raftMu here before locking Replica.mu,
	// so we unlock Replica.mu before locking them both again.
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.internalRaftGroup == nil {
		// Unlock first before locking in {raft,replica}mu order.
		r.mu.Unlock()

		r.raftMu.Lock()
		defer r.raftMu.Unlock()
		r.mu.Lock()
		log.Event(proposal.ctx, "acquired {raft,replica}mu")
	} else {
		log.Event(proposal.ctx, "acquired replica mu")
	}

	// Add size of proposal to commandSizes map.
	if r.mu.commandSizes != nil {
		r.mu.commandSizes[proposal.idKey] = proposalSize
	}
	// Make sure we clean up the proposal if we fail to submit it successfully.
	// This is important both to ensure that that the proposals map doesn't
	// grow without bound and to ensure that we always release any quota that
	// we acquire.
	defer func() {
		if pErr != nil {
			r.cleanupFailedProposalLocked(proposal)
		}
	}()

	// NB: We need to check Replica.mu.destroyStatus again in case the Replica has
	// been destroyed between the initial check at the beginning of this method
	// and the acquisition of Replica.mu. Failure to do so will leave pending
	// proposals that never get cleared.
	if !r.mu.destroyStatus.IsAlive() {
		return nil, nil, 0, roachpb.NewError(r.mu.destroyStatus.err)
	}

	repDesc, err := r.getReplicaDescriptorRLocked()
	if err != nil {
		return nil, nil, 0, roachpb.NewError(err)
	}
	maxLeaseIndex := r.insertProposalLocked(proposal, repDesc, lease)
	if maxLeaseIndex == 0 && !ba.IsLeaseRequest() {
		log.Fatalf(ctx, "no MaxLeaseIndex returned for %s", ba)
	}

	if filter := r.store.TestingKnobs().TestingProposalFilter; filter != nil {
		filterArgs := storagebase.ProposalFilterArgs{
			Ctx:   ctx,
			Cmd:   *proposal.command,
			CmdID: idKey,
			Req:   ba,
		}
		if pErr := filter(filterArgs); pErr != nil {
			return nil, nil, 0, pErr
		}
	}

	if err := r.submitProposalLocked(proposal); err == raft.ErrProposalDropped {
		// Silently ignore dropped proposals (they were always silently ignored
		// prior to the introduction of ErrProposalDropped).
		// TODO(bdarnell): Handle ErrProposalDropped better.
		// https://github.com/cockroachdb/cockroach/issues/21849
	} else if err != nil {
		return nil, nil, 0, roachpb.NewError(err)
	}

	// Must not use `proposal` in the closure below as a proposal which is not
	// present in r.mu.proposals is no longer protected by the mutex. Abandoning
	// a command only abandons the associated context. As soon as we propose a
	// command to Raft, ownership passes to the "below Raft" machinery. In
	// particular, endCmds will be invoked when the command is applied. There are
	// a handful of cases where the command may not be applied (or even
	// processed): the process crashes or the local replica is removed from the
	// range.
	tryAbandon := func() bool {
		r.mu.Lock()
		p, ok := r.mu.proposals[idKey]
		if ok {
			// TODO(radu): Should this context be created via tracer.ForkCtxSpan?
			// We'd need to make sure the span is finished eventually.
			p.ctx = r.AnnotateCtx(context.TODO())
		}
		r.mu.Unlock()
		return ok
	}
	return proposalCh, tryAbandon, maxLeaseIndex, nil
}

// submitProposalLocked proposes or re-proposes a command in r.mu.proposals.
// The replica lock must be held.
func (r *Replica) submitProposalLocked(p *ProposalData) error {
	p.proposedAtTicks = r.mu.ticks

	if r.mu.submitProposalFn != nil {
		return r.mu.submitProposalFn(p)
	}
	return defaultSubmitProposalLocked(r, p)
}

func defaultSubmitProposalLocked(r *Replica, p *ProposalData) error {
	data, err := protoutil.Marshal(p.command)
	if err != nil {
		return err
	}
	defer r.store.enqueueRaftUpdateCheck(r.RangeID)

	// Too verbose even for verbose logging, so manually enable if you want to
	// debug proposal sizes.
	if false {
		log.Infof(p.ctx, `%s: proposal: %d
  RaftCommand.ProposerReplica:               %d
  RaftCommand.ReplicatedEvalResult:          %d
  RaftCommand.ReplicatedEvalResult.Delta:    %d
  RaftCommand.WriteBatch:                    %d
`, p.Request.Summary(), len(data),
			p.command.ProposerReplica.Size(),
			p.command.ReplicatedEvalResult.Size(),
			p.command.ReplicatedEvalResult.Delta.Size(),
			p.command.WriteBatch.Size(),
		)
	}

	const largeProposalEventThresholdBytes = 2 << 19 // 512kb

	// Log an event if this is a large proposal. These are more likely to cause
	// blips or worse, and it's good to be able to pick them from traces.
	//
	// TODO(tschottdorf): can we mark them so lightstep can group them?
	if size := len(data); size > largeProposalEventThresholdBytes {
		log.Eventf(p.ctx, "proposal is large: %s", humanizeutil.IBytes(int64(size)))
	}

	if crt := p.command.ReplicatedEvalResult.ChangeReplicas; crt != nil {
		// EndTransactionRequest with a ChangeReplicasTrigger is special
		// because raft needs to understand it; it cannot simply be an
		// opaque command.
		log.Infof(p.ctx, "proposing %s", crt)

		// Ensure that we aren't trying to remove ourselves from the range without
		// having previously given up our lease, since the range won't be able
		// to make progress while the lease is owned by a removed replica (and
		// leases can stay in such a state for a very long time when using epoch-
		// based range leases). This shouldn't happen often, but has been seen
		// before (#12591).
		if crt.ChangeType == roachpb.REMOVE_REPLICA && crt.Replica.ReplicaID == r.mu.replicaID {
			log.Errorf(p.ctx, "received invalid ChangeReplicasTrigger %s to remove self (leaseholder)", crt)
			return errors.Errorf("%s: received invalid ChangeReplicasTrigger %s to remove self (leaseholder)", r, crt)
		}

		confChangeCtx := ConfChangeContext{
			CommandID: string(p.idKey),
			Payload:   data,
			Replica:   crt.Replica,
		}
		encodedCtx, err := protoutil.Marshal(&confChangeCtx)
		if err != nil {
			return err
		}

		return r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
			// We're proposing a command here so there is no need to wake the
			// leader if we were quiesced.
			r.unquiesceLocked()
			return false, /* unquiesceAndWakeLeader */
				raftGroup.ProposeConfChange(raftpb.ConfChange{
					Type:    changeTypeInternalToRaft[crt.ChangeType],
					NodeID:  uint64(crt.Replica.ReplicaID),
					Context: encodedCtx,
				})
		})
	}

	return r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		encode := encodeRaftCommandV1
		if p.command.ReplicatedEvalResult.AddSSTable != nil {
			if p.command.ReplicatedEvalResult.AddSSTable.Data == nil {
				return false, errors.New("cannot sideload empty SSTable")
			}
			encode = encodeRaftCommandV2
			r.store.metrics.AddSSTableProposals.Inc(1)
			log.Event(p.ctx, "sideloadable proposal detected")
		}

		if log.V(4) {
			log.Infof(p.ctx, "proposing command %x: %s", p.idKey, p.Request.Summary())
		}
		// We're proposing a command so there is no need to wake the leader if
		// we're quiesced.
		r.unquiesceLocked()
		return false /* unquiesceAndWakeLeader */, raftGroup.Propose(encode(p.idKey, data))
	})
}

// stepRaftGroup calls Step on the replica's RawNode with the provided request's
// message. Before doing so, it assures that the replica is unquiesced and ready
// to handle the request.
func (r *Replica) stepRaftGroup(req *RaftMessageRequest) error {
	// We're processing an incoming raft message (from a batch that may
	// include MsgVotes), so don't campaign if we wake up our raft
	// group.
	return r.withRaftGroup(false, func(raftGroup *raft.RawNode) (bool, error) {
		// We're processing a message from another replica which means that the
		// other replica is not quiesced, so we don't need to wake the leader.
		// Note that we avoid campaigning when receiving raft messages, because
		// we expect the originator to campaign instead.
		r.unquiesceWithOptionsLocked(false /* campaignOnWake */)
		r.mu.lastUpdateTimes.update(req.FromReplica.ReplicaID, timeutil.Now())
		err := raftGroup.Step(req.Message)
		if err == raft.ErrProposalDropped {
			// A proposal was forwarded to this replica but we couldn't propose it.
			// Swallow the error since we don't have an effective way of signaling
			// this to the sender.
			// TODO(bdarnell): Handle ErrProposalDropped better.
			// https://github.com/cockroachdb/cockroach/issues/21849
			err = nil
		}
		return false /* unquiesceAndWakeLeader */, err
	})
}

type handleRaftReadyStats struct {
	processed int
}

// noSnap can be passed to handleRaftReady when no snapshot should be processed.
var noSnap IncomingSnapshot

// handleRaftReady processes a raft.Ready containing entries and messages that
// are ready to read, be saved to stable storage, committed or sent to other
// peers. It takes a non-empty IncomingSnapshot to indicate that it is
// about to process a snapshot.
//
// The returned string is nonzero whenever an error is returned to give a
// non-sensitive cue as to what happened.
func (r *Replica) handleRaftReady(inSnap IncomingSnapshot) (handleRaftReadyStats, string, error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	return r.handleRaftReadyRaftMuLocked(inSnap)
}

// handleRaftReadyLocked is the same as handleRaftReady but requires that the
// replica's raftMu be held.
//
// The returned string is nonzero whenever an error is returned to give a
// non-sensitive cue as to what happened.
func (r *Replica) handleRaftReadyRaftMuLocked(
	inSnap IncomingSnapshot,
) (handleRaftReadyStats, string, error) {
	var stats handleRaftReadyStats

	ctx := r.AnnotateCtx(context.TODO())
	var hasReady bool
	var rd raft.Ready
	r.mu.Lock()

	lastIndex := r.mu.lastIndex // used for append below
	lastTerm := r.mu.lastTerm
	raftLogSize := r.mu.raftLogSize
	leaderID := r.mu.leaderID
	lastLeaderID := leaderID

	// We defer the check to Replica.updateProposalQuotaRaftMuLocked because we need
	// to check it in both cases, if hasReady is false and otherwise.
	// If hasReady == false:
	//     Consider the case when our quota is of size 1 and two out of three
	//     replicas have committed one log entry while the third is lagging
	//     behind. When the third replica finally does catch up and sends
	//     along a MsgAppResp, since the entry is already committed on the
	//     leader replica, no Ready is emitted. But given that the third
	//     replica has caught up, we can release
	//     some quota back to the pool.
	// Otherwise:
	//     Consider the case where there are two replicas and we have a quota
	//     of size 1. We acquire the quota when the write gets proposed on the
	//     leader and expect it to be released when the follower commits it
	//     locally. In order to do so we need to have the entry 'come out of
	//     raft' and in the case of a two node raft group, this only happens if
	//     hasReady == true.
	//     If we don't release quota back at the end of
	//     handleRaftReadyRaftMuLocked, the next write will get blocked.
	defer r.updateProposalQuotaRaftMuLocked(ctx, lastLeaderID)

	err := r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		if hasReady = raftGroup.HasReady(); hasReady {
			rd = raftGroup.Ready()
		}
		return hasReady /* unquiesceAndWakeLeader */, nil
	})
	r.mu.Unlock()
	if err != nil {
		const expl = "while checking raft group for Ready"
		return stats, expl, errors.Wrap(err, expl)
	}

	if !hasReady {
		return stats, "", nil
	}

	logRaftReady(ctx, rd)

	refreshReason := noReason
	if rd.SoftState != nil && leaderID != roachpb.ReplicaID(rd.SoftState.Lead) {
		// Refresh pending commands if the Raft leader has changed. This is usually
		// the first indication we have of a new leader on a restarted node.
		//
		// TODO(peter): Re-proposing commands when SoftState.Lead changes can lead
		// to wasteful multiple-reproposals when we later see an empty Raft command
		// indicating a newly elected leader or a conf change. Replay protection
		// prevents any corruption, so the waste is only a performance issue.
		if log.V(3) {
			log.Infof(ctx, "raft leader changed: %d -> %d", leaderID, rd.SoftState.Lead)
		}
		if !r.store.TestingKnobs().DisableRefreshReasonNewLeader {
			refreshReason = reasonNewLeader
		}
		leaderID = roachpb.ReplicaID(rd.SoftState.Lead)
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		snapUUID, err := uuid.FromBytes(rd.Snapshot.Data)
		if err != nil {
			const expl = "invalid snapshot id"
			return stats, expl, errors.Wrap(err, expl)
		}
		if inSnap.SnapUUID == (uuid.UUID{}) {
			log.Fatalf(ctx, "programming error: a snapshot application was attempted outside of the streaming snapshot codepath")
		}
		if snapUUID != inSnap.SnapUUID {
			log.Fatalf(ctx, "incoming snapshot id doesn't match raft snapshot id: %s != %s", snapUUID, inSnap.SnapUUID)
		}

		// Applying this snapshot may require us to subsume one or more of our right
		// neighbors. This occurs if this replica is informed about the merges via a
		// Raft snapshot instead of a MsgApp containing the merge commits, e.g.,
		// because it went offline before the merge commits applied and did not come
		// back online until after the merge commits were truncated away.
		subsumedRepls, releaseMergeLock := r.maybeAcquireSnapshotMergeLock(ctx, inSnap)
		defer releaseMergeLock()

		if err := r.applySnapshot(ctx, inSnap, rd.Snapshot, rd.HardState, subsumedRepls); err != nil {
			const expl = "while applying snapshot"
			return stats, expl, errors.Wrap(err, expl)
		}

		// r.mu.lastIndex, r.mu.lastTerm and r.mu.raftLogSize were updated in
		// applySnapshot, but we also want to make sure we reflect these changes in
		// the local variables we're tracking here.
		r.mu.RLock()
		lastIndex = r.mu.lastIndex
		lastTerm = r.mu.lastTerm
		raftLogSize = r.mu.raftLogSize
		r.mu.RUnlock()

		// We refresh pending commands after applying a snapshot because this
		// replica may have been temporarily partitioned from the Raft group and
		// missed leadership changes that occurred. Suppose node A is the leader,
		// and then node C gets partitioned away from the others. Leadership passes
		// back and forth between A and B during the partition, but when the
		// partition is healed node A is leader again.
		if !r.store.TestingKnobs().DisableRefreshReasonSnapshotApplied &&
			refreshReason == noReason {
			refreshReason = reasonSnapshotApplied
		}
	}

	// Separate the MsgApp messages from all other Raft message types so that we
	// can take advantage of the optimization discussed in the Raft thesis under
	// the section: `10.2.1 Writing to the leader’s disk in parallel`. The
	// optimization suggests that instead of a leader writing new log entries to
	// disk before replicating them to its followers, the leader can instead
	// write the entries to disk in parallel with replicating to its followers
	// and them writing to their disks.
	//
	// Here, we invoke this optimization by:
	// 1. sending all MsgApps.
	// 2. syncing all entries and Raft state to disk.
	// 3. sending all other messages.
	//
	// Since this is all handled in handleRaftReadyRaftMuLocked, we're assured
	// that even though we may sync new entries to disk after sending them in
	// MsgApps to followers, we'll always have them synced to disk before we
	// process followers' MsgAppResps for the corresponding entries because this
	// entire method requires RaftMu to be locked. This is a requirement because
	// etcd/raft does not support commit quorums that do not include the leader,
	// even though the Raft thesis states that this would technically be safe:
	// > The leader may even commit an entry before it has been written to its
	// > own disk, if a majority of followers have written it to their disks;
	// > this is still safe.
	//
	// However, MsgApps are also used to inform followers of committed entries
	// through the Commit index that they contains. Because the optimization
	// sends all MsgApps before syncing to disc, we may send out a commit index
	// in a MsgApp that we have not ourselves written in HardState.Commit. This
	// is ok, because the Commit index can be treated as volatile state, as is
	// supported by raft.MustSync. The Raft thesis corroborates this, stating in
	// section: `3.8 Persisted state and server restarts` that:
	// > Other state variables are safe to lose on a restart, as they can all be
	// > recreated. The most interesting example is the commit index, which can
	// > safely be reinitialized to zero on a restart.
	msgApps, otherMsgs := splitMsgApps(rd.Messages)
	r.sendRaftMessages(ctx, msgApps)

	// Use a more efficient write-only batch because we don't need to do any
	// reads from the batch. Any reads are performed via the "distinct" batch
	// which passes the reads through to the underlying DB.
	batch := r.store.Engine().NewWriteOnlyBatch()
	defer batch.Close()

	// We know that all of the writes from here forward will be to distinct keys.
	writer := batch.Distinct()
	prevLastIndex := lastIndex
	if len(rd.Entries) > 0 {
		// All of the entries are appended to distinct keys, returning a new
		// last index.
		thinEntries, sideLoadedEntriesSize, err := r.maybeSideloadEntriesRaftMuLocked(ctx, rd.Entries)
		if err != nil {
			const expl = "during sideloading"
			return stats, expl, errors.Wrap(err, expl)
		}
		raftLogSize += sideLoadedEntriesSize
		if lastIndex, lastTerm, raftLogSize, err = r.append(
			ctx, writer, lastIndex, lastTerm, raftLogSize, thinEntries,
		); err != nil {
			const expl = "during append"
			return stats, expl, errors.Wrap(err, expl)
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if !r.IsInitialized() && rd.HardState.Commit != 0 {
			log.Fatalf(ctx, "setting non-zero HardState.Commit on uninitialized replica %s. HS=%+v", r, rd.HardState)
		}
		if err := r.raftMu.stateLoader.SetHardState(ctx, writer, rd.HardState); err != nil {
			const expl = "during setHardState"
			return stats, expl, errors.Wrap(err, expl)
		}
	}
	writer.Close()
	// Synchronously commit the batch with the Raft log entries and Raft hard
	// state as we're promising not to lose this data.
	//
	// Note that the data is visible to other goroutines before it is synced to
	// disk. This is fine. The important constraints are that these syncs happen
	// before Raft messages are sent and before the call to RawNode.Advance. Our
	// regular locking is sufficient for this and if other goroutines can see the
	// data early, that's fine. In particular, snapshots are not a problem (I
	// think they're the only thing that might access log entries or HardState
	// from other goroutines). Snapshots do not include either the HardState or
	// uncommitted log entries, and even if they did include log entries that
	// were not persisted to disk, it wouldn't be a problem because raft does not
	// infer the that entries are persisted on the node that sends a snapshot.
	start := timeutil.Now()
	if err := batch.Commit(rd.MustSync && !disableSyncRaftLog.Get(&r.store.cfg.Settings.SV)); err != nil {
		const expl = "while committing batch"
		return stats, expl, errors.Wrap(err, expl)
	}
	elapsed := timeutil.Since(start)
	r.store.metrics.RaftLogCommitLatency.RecordValue(elapsed.Nanoseconds())

	if len(rd.Entries) > 0 {
		// We may have just overwritten parts of the log which contain
		// sideloaded SSTables from a previous term (and perhaps discarded some
		// entries that we didn't overwrite). Remove any such leftover on-disk
		// payloads (we can do that now because we've committed the deletion
		// just above).
		firstPurge := rd.Entries[0].Index // first new entry written
		purgeTerm := rd.Entries[0].Term - 1
		lastPurge := prevLastIndex // old end of the log, include in deletion
		purgedSize, err := maybePurgeSideloaded(ctx, r.raftMu.sideloaded, firstPurge, lastPurge, purgeTerm)
		if err != nil {
			const expl = "while purging sideloaded storage"
			return stats, expl, err
		}
		raftLogSize -= purgedSize
		if raftLogSize < 0 {
			// Might have gone negative if node was recently restarted.
			raftLogSize = 0
		}

	}

	// Update protected state - last index, last term, raft log size, and raft
	// leader ID.
	r.mu.Lock()
	r.mu.lastIndex = lastIndex
	r.mu.lastTerm = lastTerm
	r.mu.raftLogSize = raftLogSize
	var becameLeader bool
	if r.mu.leaderID != leaderID {
		r.mu.leaderID = leaderID
		// Clear the remote proposal set. Would have been nil already if not
		// previously the leader.
		becameLeader = r.mu.leaderID == r.mu.replicaID
	}
	r.mu.Unlock()

	// When becoming the leader, proactively add the replica to the replicate
	// queue. We might have been handed leadership by a remote node which wanted
	// to remove itself from the range.
	if becameLeader && r.store.replicateQueue != nil {
		r.store.replicateQueue.MaybeAdd(r, r.store.Clock().Now())
	}

	// Update raft log entry cache. We clear any older, uncommitted log entries
	// and cache the latest ones.
	r.store.raftEntryCache.Add(r.RangeID, rd.Entries)

	r.sendRaftMessages(ctx, otherMsgs)

	for _, e := range rd.CommittedEntries {
		switch e.Type {
		case raftpb.EntryNormal:
			// NB: Committed entries are handed to us by Raft. Raft does not
			// know about sideloading. Consequently the entries here are all
			// already inlined.

			var commandID storagebase.CmdIDKey
			var command storagepb.RaftCommand

			// Process committed entries. etcd raft occasionally adds a nil entry
			// (our own commands are never empty). This happens in two situations:
			// When a new leader is elected, and when a config change is dropped due
			// to the "one at a time" rule. In both cases we may need to resubmit our
			// pending proposals (In the former case we resubmit everything because
			// we proposed them to a former leader that is no longer able to commit
			// them. In the latter case we only need to resubmit pending config
			// changes, but it's hard to distinguish so we resubmit everything
			// anyway). We delay resubmission until after we have processed the
			// entire batch of entries.
			if len(e.Data) == 0 {
				// Overwrite unconditionally since this is the most aggressive
				// reproposal mode.
				if !r.store.TestingKnobs().DisableRefreshReasonNewLeaderOrConfigChange {
					refreshReason = reasonNewLeaderOrConfigChange
				}
				commandID = "" // special-cased value, command isn't used
			} else {
				var encodedCommand []byte
				commandID, encodedCommand = DecodeRaftCommand(e.Data)
				// An empty command is used to unquiesce a range and wake the
				// leader. Clear commandID so it's ignored for processing.
				if len(encodedCommand) == 0 {
					commandID = ""
				} else if err := protoutil.Unmarshal(encodedCommand, &command); err != nil {
					const expl = "while unmarshalling entry"
					return stats, expl, errors.Wrap(err, expl)
				}
			}

			if changedRepl := r.processRaftCommand(ctx, commandID, e.Term, e.Index, command); changedRepl {
				log.Fatalf(ctx, "unexpected replication change from command %s", &command)
			}
			r.store.metrics.RaftCommandsApplied.Inc(1)
			stats.processed++

			r.mu.Lock()
			if r.mu.replicaID == r.mu.leaderID {
				// At this point we're not guaranteed to have proposalQuota
				// initialized, the same is true for quotaReleaseQueue and
				// commandSizes. By checking if the specified commandID is
				// present in commandSizes, we'll only queue the cmdSize if
				// they're all initialized.
				if cmdSize, ok := r.mu.commandSizes[commandID]; ok {
					r.mu.quotaReleaseQueue = append(r.mu.quotaReleaseQueue, cmdSize)
					delete(r.mu.commandSizes, commandID)
				}
			}
			r.mu.Unlock()

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := protoutil.Unmarshal(e.Data, &cc); err != nil {
				const expl = "while unmarshaling ConfChange"
				return stats, expl, errors.Wrap(err, expl)
			}
			var ccCtx ConfChangeContext
			if err := protoutil.Unmarshal(cc.Context, &ccCtx); err != nil {
				const expl = "while unmarshaling ConfChangeContext"
				return stats, expl, errors.Wrap(err, expl)

			}
			var command storagepb.RaftCommand
			if err := protoutil.Unmarshal(ccCtx.Payload, &command); err != nil {
				const expl = "while unmarshaling RaftCommand"
				return stats, expl, errors.Wrap(err, expl)
			}
			commandID := storagebase.CmdIDKey(ccCtx.CommandID)
			if changedRepl := r.processRaftCommand(
				ctx, commandID, e.Term, e.Index, command,
			); !changedRepl {
				// If we did not apply the config change, tell raft that the config change was aborted.
				cc = raftpb.ConfChange{}
			}
			stats.processed++

			r.mu.Lock()
			if r.mu.replicaID == r.mu.leaderID {
				if cmdSize, ok := r.mu.commandSizes[commandID]; ok {
					r.mu.quotaReleaseQueue = append(r.mu.quotaReleaseQueue, cmdSize)
					delete(r.mu.commandSizes, commandID)
				}
			}
			r.mu.Unlock()

			if err := r.withRaftGroup(true, func(raftGroup *raft.RawNode) (bool, error) {
				raftGroup.ApplyConfChange(cc)
				return true, nil
			}); err != nil {
				const expl = "during ApplyConfChange"
				return stats, expl, errors.Wrap(err, expl)
			}
		default:
			log.Fatalf(ctx, "unexpected Raft entry: %v", e)
		}
	}
	if refreshReason != noReason {
		r.mu.Lock()
		r.refreshProposalsLocked(0, refreshReason)
		r.mu.Unlock()
	}

	// TODO(bdarnell): need to check replica id and not Advance if it
	// has changed. Or do we need more locking to guarantee that replica
	// ID cannot change during handleRaftReady?
	const expl = "during advance"
	if err := r.withRaftGroup(true, func(raftGroup *raft.RawNode) (bool, error) {
		raftGroup.Advance(rd)

		// If the Raft group still has more to process then we immediately
		// re-enqueue it for another round of processing. This is possible if
		// the group's committed entries were paginated due to size limitations
		// and we didn't apply all of them in this pass.
		if raftGroup.HasReady() {
			r.store.enqueueRaftUpdateCheck(r.RangeID)
		}
		return true, nil
	}); err != nil {
		return stats, expl, errors.Wrap(err, expl)
	}
	return stats, "", nil
}

// splitMsgApps splits the Raft message slice into two slices, one containing
// MsgApps and one containing all other message types. Each slice retains the
// relative ordering between messages in the original slice.
func splitMsgApps(msgs []raftpb.Message) (msgApps, otherMsgs []raftpb.Message) {
	splitIdx := 0
	for i, msg := range msgs {
		if msg.Type == raftpb.MsgApp {
			msgs[i], msgs[splitIdx] = msgs[splitIdx], msgs[i]
			splitIdx++
		}
	}
	return msgs[:splitIdx], msgs[splitIdx:]
}

func fatalOnRaftReadyErr(ctx context.Context, expl string, err error) {
	// Mimic the behavior in processRaft.
	log.Fatalf(ctx, "%s: %s", log.Safe(expl), err) // TODO(bdarnell)
}

// tick the Raft group, returning true if the raft group exists and is
// unquiesced; false otherwise.
func (r *Replica) tick(livenessMap IsLiveMap) (bool, error) {
	ctx := r.AnnotateCtx(context.TODO())

	r.unreachablesMu.Lock()
	remotes := r.unreachablesMu.remotes
	r.unreachablesMu.remotes = nil
	r.unreachablesMu.Unlock()

	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	// If the raft group is uninitialized, do not initialize on tick.
	if r.mu.internalRaftGroup == nil {
		return false, nil
	}

	for remoteReplica := range remotes {
		r.mu.internalRaftGroup.ReportUnreachable(uint64(remoteReplica))
	}

	if r.mu.quiescent {
		return false, nil
	}
	if r.maybeQuiesceLocked(ctx, livenessMap) {
		return false, nil
	}

	r.maybeTransferRaftLeadershipLocked(ctx)

	r.mu.ticks++
	r.mu.internalRaftGroup.Tick()

	refreshAtDelta := r.store.cfg.RaftElectionTimeoutTicks
	if knob := r.store.TestingKnobs().RefreshReasonTicksPeriod; knob > 0 {
		refreshAtDelta = knob
	}
	if !r.store.TestingKnobs().DisableRefreshReasonTicks && r.mu.ticks%refreshAtDelta == 0 {
		// RaftElectionTimeoutTicks is a reasonable approximation of how long we
		// should wait before deciding that our previous proposal didn't go
		// through. Note that the combination of the above condition and passing
		// RaftElectionTimeoutTicks to refreshProposalsLocked means that commands
		// will be refreshed when they have been pending for 1 to 2 election
		// cycles.
		r.refreshProposalsLocked(refreshAtDelta, reasonTicks)
	}
	return true, nil
}

func (r *Replica) hasRaftReadyRLocked() bool {
	return r.mu.internalRaftGroup.HasReady()
}

//go:generate stringer -type refreshRaftReason
type refreshRaftReason int

const (
	noReason refreshRaftReason = iota
	reasonNewLeader
	reasonNewLeaderOrConfigChange
	// A snapshot was just applied and so it may have contained commands that we
	// proposed whose proposal we still consider to be inflight. These commands
	// will never receive a response through the regular channel.
	reasonSnapshotApplied
	reasonReplicaIDChanged
	reasonTicks
)

// refreshProposalsLocked goes through the pending proposals, notifying
// proposers whose proposals need to be retried, and resubmitting proposals
// which were likely dropped (but may still apply at a legal Lease index) -
// ensuring that the proposer will eventually get a reply on the channel it's
// waiting on.
// mu must be held.
//
// refreshAtDelta only applies for reasonTicks and specifies how old (in ticks)
// a command must be for it to be inspected; the usual value is the number of
// ticks of an election timeout (affect only proposals that have had ample time
// to apply but didn't).
func (r *Replica) refreshProposalsLocked(refreshAtDelta int, reason refreshRaftReason) {
	if refreshAtDelta != 0 && reason != reasonTicks {
		log.Fatalf(context.TODO(), "refreshAtDelta specified for reason %s != reasonTicks", reason)
	}

	numShouldRetry := 0
	var reproposals pendingCmdSlice
	for _, p := range r.mu.proposals {
		if p.command.MaxLeaseIndex == 0 {
			// Commands without a MaxLeaseIndex cannot be reproposed, as they might
			// apply twice. We also don't want to ask the proposer to retry these
			// special commands.
			r.cleanupFailedProposalLocked(p)
			log.VEventf(p.ctx, 2, "refresh (reason: %s) returning AmbiguousResultError for command "+
				"without MaxLeaseIndex: %v", reason, p.command)
			p.finishApplication(proposalResult{Err: roachpb.NewError(
				roachpb.NewAmbiguousResultError(
					fmt.Sprintf("unknown status for command without MaxLeaseIndex "+
						"at refreshProposalsLocked time (refresh reason: %s)", reason)))})
		} else if cannotApplyAnyMore := !p.command.ReplicatedEvalResult.IsLeaseRequest &&
			p.command.MaxLeaseIndex <= r.mu.state.LeaseAppliedIndex; cannotApplyAnyMore {
			// The command's designated lease index slot was filled up. We got to
			// LeaseAppliedIndex and p is still pending in r.mu.proposals; generally
			// this means that proposal p didn't commit, and it will be sent back to
			// the proposer for a retry - the request needs to be re-evaluated and the
			// command re-proposed with a new MaxLeaseIndex. Note that this branch is not
			// taken for leases as their MaxLeaseIndex plays no role in deciding whether
			// they can apply or not -- the sequence number of the previous lease matters.
			//
			// An exception is the case when we're refreshing because of
			// reasonSnapshotApplied - in that case we don't know if p or some other
			// command filled the p.command.MaxLeaseIndex slot (i.e. p might have been
			// applied, but we're not watching for every proposal when applying a
			// snapshot, so nobody removed p from r.mu.proposals). In this
			// ambiguous case, we return an ambiguous error.
			//
			// NOTE: We used to perform a re-evaluation here in order to avoid the
			// ambiguity, but it was cumbersome because the higher layer needed to be
			// aware that if the re-evaluation failed in any way, it must rewrite the
			// error to reflect the ambiguity. This mechanism also proved very hard to
			// test and also is of questionable utility since snapshots are only
			// applied in the first place if the leaseholder is divorced from the Raft
			// leader.
			//
			// Note that we can't use the commit index here (which is typically a
			// little ahead), because a pending command is removed only as it
			// applies. Thus we'd risk reproposing a command that has been committed
			// but not yet applied.
			r.cleanupFailedProposalLocked(p)
			log.Eventf(p.ctx, "retry proposal %x: %s", p.idKey, reason)
			if reason == reasonSnapshotApplied {
				p.finishApplication(proposalResult{Err: roachpb.NewError(
					roachpb.NewAmbiguousResultError(
						fmt.Sprintf("unable to determin whether command was applied via snapshot")))})
			} else {
				p.finishApplication(proposalResult{ProposalRetry: proposalIllegalLeaseIndex})
			}
			numShouldRetry++
		} else if reason == reasonTicks && p.proposedAtTicks > r.mu.ticks-refreshAtDelta {
			// The command was proposed too recently, don't bother reproprosing it
			// yet.
		} else {
			// The proposal can still apply according to its MaxLeaseIndex. But it's
			// likely that the proposal was dropped, so we're going to repropose it
			// directly; it's cheaper than asking the proposer to re-evaluate and
			// re-propose. Note that we don't need to worry about receiving ambiguous
			// responses for this reproposal - we'll only be handling one response
			// across the original proposal and the reproposal because we're
			// reproposing with the same MaxLeaseIndex and the same idKey as the
			// original (a possible second response for the same idKey would be
			// ignored).
			//
			// This situation happens when we've proposed commands that never made it
			// to the leader - usually because we didn't know who the leader is. When
			// we do discover the leader we need to repropose the command. In local
			// testing, by far the most common reason for these reproposals is the
			// initial leader election after a split.  Specifically, when a range is
			// split when the next command is proposed to the RHS the leader is
			// elected. After the election completes the command is reproposed for
			// both reasonNewLeader and reasonNewLeaderOrConfigChange.
			reproposals = append(reproposals, p)
		}
	}
	if log.V(1) && (numShouldRetry > 0 || len(reproposals) > 0) {
		ctx := r.AnnotateCtx(context.TODO())
		log.Infof(ctx,
			"pending commands: sent %d back to client, reproposing %d (at %d.%d) %s",
			numShouldRetry, len(reproposals), r.mu.state.RaftAppliedIndex,
			r.mu.state.LeaseAppliedIndex, reason)
	}

	// Reproposals are those commands which we weren't able to send back to the
	// client (since we're not sure that another copy of them could apply at
	// the "correct" index). For reproposals, it's generally pretty unlikely
	// that they can make it in the right place. Reproposing in order is
	// definitely required, however.
	//
	// TODO(tschottdorf): evaluate whether `r.mu.proposals` should
	// be a list/slice.
	sort.Sort(reproposals)
	for _, p := range reproposals {
		log.Eventf(p.ctx, "re-submitting command %x to Raft: %s", p.idKey, reason)
		if err := r.submitProposalLocked(p); err == raft.ErrProposalDropped {
			// TODO(bdarnell): Handle ErrProposalDropped better.
			// https://github.com/cockroachdb/cockroach/issues/21849
		} else if err != nil {
			r.cleanupFailedProposalLocked(p)
			p.finishApplication(proposalResult{
				Err: roachpb.NewError(roachpb.NewAmbiguousResultError(err.Error())),
			})
		}
	}
}

// maybeCoalesceHeartbeat returns true if the heartbeat was coalesced and added
// to the appropriate queue.
func (r *Replica) maybeCoalesceHeartbeat(
	ctx context.Context,
	msg raftpb.Message,
	toReplica, fromReplica roachpb.ReplicaDescriptor,
	quiesce bool,
) bool {
	var hbMap map[roachpb.StoreIdent][]RaftHeartbeat
	switch msg.Type {
	case raftpb.MsgHeartbeat:
		r.store.coalescedMu.Lock()
		hbMap = r.store.coalescedMu.heartbeats
	case raftpb.MsgHeartbeatResp:
		r.store.coalescedMu.Lock()
		hbMap = r.store.coalescedMu.heartbeatResponses
	default:
		return false
	}
	beat := RaftHeartbeat{
		RangeID:       r.RangeID,
		ToReplicaID:   toReplica.ReplicaID,
		FromReplicaID: fromReplica.ReplicaID,
		Term:          msg.Term,
		Commit:        msg.Commit,
		Quiesce:       quiesce,
	}
	if log.V(4) {
		log.Infof(ctx, "coalescing beat: %+v", beat)
	}
	toStore := roachpb.StoreIdent{
		StoreID: toReplica.StoreID,
		NodeID:  toReplica.NodeID,
	}
	hbMap[toStore] = append(hbMap[toStore], beat)
	r.store.coalescedMu.Unlock()
	return true
}

func (r *Replica) sendRaftMessages(ctx context.Context, messages []raftpb.Message) {
	var lastAppResp raftpb.Message
	for _, message := range messages {
		drop := false
		switch message.Type {
		case raftpb.MsgApp:
			if util.RaceEnabled {
				// Iterate over the entries to assert that all sideloaded commands
				// are already inlined. replicaRaftStorage.Entries already performs
				// the sideload inlining for stable entries and raft.unstable always
				// contain fat entries. Since these are the only two sources that
				// raft.sendAppend gathers entries from to populate MsgApps, we
				// should never see thin entries here.
				for j := range message.Entries {
					assertSideloadedRaftCommandInlined(ctx, &message.Entries[j])
				}
			}

		case raftpb.MsgAppResp:
			// A successful (non-reject) MsgAppResp contains one piece of
			// information: the highest log index. Raft currently queues up
			// one MsgAppResp per incoming MsgApp, and we may process
			// multiple messages in one handleRaftReady call (because
			// multiple messages may arrive while we're blocked syncing to
			// disk). If we get redundant MsgAppResps, drop all but the
			// last (we've seen that too many MsgAppResps can overflow
			// message queues on the receiving side).
			//
			// Note that this reorders the chosen MsgAppResp relative to
			// other messages (including any MsgAppResps with the Reject flag),
			// but raft is fine with this reordering.
			//
			// TODO(bdarnell): Consider pushing this optimization into etcd/raft.
			// Similar optimizations may be possible for other message types,
			// although MsgAppResp is the only one that has been seen as a
			// problem in practice.
			if !message.Reject && message.Index > lastAppResp.Index {
				lastAppResp = message
				drop = true
			}
		}

		if !drop {
			r.sendRaftMessage(ctx, message)
		}
	}
	if lastAppResp.Index > 0 {
		r.sendRaftMessage(ctx, lastAppResp)
	}
}

// sendRaftMessage sends a Raft message.
func (r *Replica) sendRaftMessage(ctx context.Context, msg raftpb.Message) {
	r.mu.Lock()
	fromReplica, fromErr := r.getReplicaDescriptorByIDRLocked(roachpb.ReplicaID(msg.From), r.mu.lastToReplica)
	toReplica, toErr := r.getReplicaDescriptorByIDRLocked(roachpb.ReplicaID(msg.To), r.mu.lastFromReplica)
	var startKey roachpb.RKey
	if msg.Type == raftpb.MsgHeartbeat {
		if r.mu.replicaID == 0 {
			log.Fatalf(ctx, "preemptive snapshot attempted to send a heartbeat: %+v", msg)
		}
		// For followers, we update lastUpdateTimes when we step a message from
		// them into the local Raft group. The leader won't hit that path, so we
		// update it whenever it sends a heartbeat. In effect, this makes sure
		// it always sees itself as alive.
		r.mu.lastUpdateTimes.update(r.mu.replicaID, timeutil.Now())
	} else if msg.Type == raftpb.MsgApp && r.mu.internalRaftGroup != nil {
		// When the follower is potentially an uninitialized replica waiting for
		// a split trigger, send the replica's StartKey along. See the method
		// below for more context:
		_ = maybeDropMsgApp
		// NB: this code is allocation free.
		r.mu.internalRaftGroup.WithProgress(func(id uint64, _ raft.ProgressType, pr raft.Progress) {
			if id == msg.To && pr.State == raft.ProgressStateProbe {
				// It is moderately expensive to attach a full key to the message, but note that
				// a probing follower will only be appended to once per heartbeat interval (i.e.
				// on the order of seconds). See:
				//
				// https://github.com/etcd-io/etcd/blob/7f450bf6967638673dd88fd4e730b01d1303d5ff/raft/progress.go#L41
				startKey = r.descRLocked().StartKey
			}
		})
	}
	r.mu.Unlock()

	if fromErr != nil {
		log.Warningf(ctx, "failed to look up sender replica %d in r%d while sending %s: %s",
			msg.From, r.RangeID, msg.Type, fromErr)
		return
	}
	if toErr != nil {
		log.Warningf(ctx, "failed to look up recipient replica %d in r%d while sending %s: %s",
			msg.To, r.RangeID, msg.Type, toErr)
		return
	}

	// Raft-initiated snapshots are handled by the Raft snapshot queue.
	if msg.Type == raftpb.MsgSnap {
		if _, err := r.store.raftSnapshotQueue.Add(r, raftSnapshotPriority); err != nil {
			log.Errorf(ctx, "unable to add replica to Raft repair queue: %s", err)
		}
		return
	}

	if r.maybeCoalesceHeartbeat(ctx, msg, toReplica, fromReplica, false) {
		return
	}

	if !r.sendRaftMessageRequest(ctx, &RaftMessageRequest{
		RangeID:       r.RangeID,
		ToReplica:     toReplica,
		FromReplica:   fromReplica,
		Message:       msg,
		RangeStartKey: startKey, // usually nil
	}) {
		if err := r.withRaftGroup(true, func(raftGroup *raft.RawNode) (bool, error) {
			r.mu.droppedMessages++
			raftGroup.ReportUnreachable(msg.To)
			return true, nil
		}); err != nil {
			log.Fatal(ctx, err)
		}
	}
}

// addUnreachableRemoteReplica adds the given remote ReplicaID to be reported
// as unreachable on the next tick.
func (r *Replica) addUnreachableRemoteReplica(remoteReplica roachpb.ReplicaID) {
	r.unreachablesMu.Lock()
	if r.unreachablesMu.remotes == nil {
		r.unreachablesMu.remotes = make(map[roachpb.ReplicaID]struct{})
	}
	r.unreachablesMu.remotes[remoteReplica] = struct{}{}
	r.unreachablesMu.Unlock()
}

// sendRaftMessageRequest sends a raft message, returning false if the message
// was dropped. It is the caller's responsibility to call ReportUnreachable on
// the Raft group.
func (r *Replica) sendRaftMessageRequest(ctx context.Context, req *RaftMessageRequest) bool {
	if log.V(4) {
		log.Infof(ctx, "sending raft request %+v", req)
	}

	ok := r.store.cfg.Transport.SendAsync(req)
	// TODO(peter): Looping over all of the outgoing Raft message queues to
	// update this stat on every send is a bit expensive.
	r.store.metrics.RaftEnqueuedPending.Update(r.store.cfg.Transport.queuedMessageCount())
	return ok
}

func (r *Replica) reportSnapshotStatus(ctx context.Context, to roachpb.ReplicaID, snapErr error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()

	snapStatus := raft.SnapshotFinish
	if snapErr != nil {
		snapStatus = raft.SnapshotFailure
	}

	if err := r.withRaftGroup(true, func(raftGroup *raft.RawNode) (bool, error) {
		raftGroup.ReportSnapshot(uint64(to), snapStatus)
		return true, nil
	}); err != nil {
		log.Fatal(ctx, err)
	}
}

func (r *Replica) checkForcedErrLocked(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	raftCmd storagepb.RaftCommand,
	proposal *ProposalData,
	proposedLocally bool,
) (uint64, proposalReevaluationReason, *roachpb.Error) {
	leaseIndex := r.mu.state.LeaseAppliedIndex

	isLeaseRequest := raftCmd.ReplicatedEvalResult.IsLeaseRequest
	var requestedLease roachpb.Lease
	if isLeaseRequest {
		requestedLease = *raftCmd.ReplicatedEvalResult.State.Lease
	}
	if idKey == "" {
		// This is an empty Raft command (which is sent by Raft after elections
		// to trigger reproposals or during concurrent configuration changes).
		// Nothing to do here except making sure that the corresponding batch
		// (which is bogus) doesn't get executed (for it is empty and so
		// properties like key range are undefined).
		return leaseIndex, proposalNoReevaluation, roachpb.NewErrorf("no-op on empty Raft entry")
	}

	// Verify the lease matches the proposer's expectation. We rely on
	// the proposer's determination of whether the existing lease is
	// held, and can be used, or is expired, and can be replaced.
	// Verify checks that the lease has not been modified since proposal
	// due to Raft delays / reorderings.
	// To understand why this lease verification is necessary, see comments on the
	// proposer_lease field in the proto.
	leaseMismatch := false
	if raftCmd.DeprecatedProposerLease != nil {
		// VersionLeaseSequence must not have been active when this was proposed.
		//
		// This does not prevent the lease race condition described below. The
		// reason we don't fix this here as well is because fixing the race
		// requires a new cluster version which implies that we'll already be
		// using lease sequence numbers and will fall into the case below.
		leaseMismatch = !raftCmd.DeprecatedProposerLease.Equivalent(*r.mu.state.Lease)
	} else {
		leaseMismatch = raftCmd.ProposerLeaseSequence != r.mu.state.Lease.Sequence
		if !leaseMismatch && isLeaseRequest {
			// Lease sequence numbers are a reflection of lease equivalency
			// between subsequent leases. However, Lease.Equivalent is not fully
			// symmetric, meaning that two leases may be Equivalent to a third
			// lease but not Equivalent to each other. If these leases are
			// proposed under that same third lease, neither will be able to
			// detect whether the other has applied just by looking at the
			// current lease sequence number because neither will will increment
			// the sequence number.
			//
			// This can lead to inversions in lease expiration timestamps if
			// we're not careful. To avoid this, if a lease request's proposer
			// lease sequence matches the current lease sequence and the current
			// lease sequence also matches the requested lease sequence, we make
			// sure the requested lease is Equivalent to current lease.
			if r.mu.state.Lease.Sequence == requestedLease.Sequence {
				// It is only possible for this to fail when expiration-based
				// lease extensions are proposed concurrently.
				leaseMismatch = !r.mu.state.Lease.Equivalent(requestedLease)
			}

			// This is a check to see if the lease we proposed this lease request against is the same
			// lease that we're trying to update. We need to check proposal timestamps because
			// extensions don't increment sequence numbers. Without this check a lease could
			// be extended and then another lease proposed against the original lease would
			// be applied over the extension.
			if raftCmd.ReplicatedEvalResult.PrevLeaseProposal != nil &&
				(*raftCmd.ReplicatedEvalResult.PrevLeaseProposal != *r.mu.state.Lease.ProposedTS) {
				leaseMismatch = true
			}
		}
	}
	if leaseMismatch {
		log.VEventf(
			ctx, 1,
			"command proposed from replica %+v with lease #%d incompatible to %v",
			raftCmd.ProposerReplica, raftCmd.ProposerLeaseSequence, *r.mu.state.Lease,
		)
		if isLeaseRequest {
			// For lease requests we return a special error that
			// redirectOnOrAcquireLease() understands. Note that these
			// requests don't go through the DistSender.
			return leaseIndex, proposalNoReevaluation, roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  *r.mu.state.Lease,
				Requested: requestedLease,
				Message:   "proposed under invalid lease",
			})
		}
		// We return a NotLeaseHolderError so that the DistSender retries.
		nlhe := newNotLeaseHolderError(
			r.mu.state.Lease, raftCmd.ProposerReplica.StoreID, r.mu.state.Desc)
		nlhe.CustomMsg = fmt.Sprintf(
			"stale proposal: command was proposed under lease #%d but is being applied "+
				"under lease: %s", raftCmd.ProposerLeaseSequence, r.mu.state.Lease)
		return leaseIndex, proposalNoReevaluation, roachpb.NewError(nlhe)
	}

	if isLeaseRequest {
		// Lease commands are ignored by the counter (and their MaxLeaseIndex is ignored). This
		// makes sense since lease commands are proposed by anyone, so we can't expect a coherent
		// MaxLeaseIndex. Also, lease proposals are often replayed, so not making them update the
		// counter makes sense from a testing perspective.
		//
		// However, leases get special vetting to make sure we don't give one to a replica that was
		// since removed (see #15385 and a comment in redirectOnOrAcquireLease).
		if _, ok := r.mu.state.Desc.GetReplicaDescriptor(requestedLease.Replica.StoreID); !ok {
			return leaseIndex, proposalNoReevaluation, roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  *r.mu.state.Lease,
				Requested: requestedLease,
				Message:   "replica not part of range",
			})
		}
	} else if r.mu.state.LeaseAppliedIndex < raftCmd.MaxLeaseIndex {
		// The happy case: the command is applying at or ahead of the minimal
		// permissible index. It's ok if it skips a few slots (as can happen
		// during rearrangement); this command will apply, but later ones which
		// were proposed at lower indexes may not. Overall though, this is more
		// stable and simpler than requiring commands to apply at their exact
		// lease index: Handling the case in which MaxLeaseIndex > oldIndex+1
		// is otherwise tricky since we can't tell the client to try again
		// (reproposals could exist and may apply at the right index, leading
		// to a replay), and assigning the required index would be tedious
		// seeing that it would have to rewind sometimes.
		leaseIndex = raftCmd.MaxLeaseIndex
	} else {
		// The command is trying to apply at a past log position. That's
		// unfortunate and hopefully rare; the client on the proposer will try
		// again. Note that in this situation, the leaseIndex does not advance.
		retry := proposalNoReevaluation
		if proposedLocally {
			log.VEventf(
				ctx, 1,
				"retry proposal %x: applied at lease index %d, required <= %d",
				proposal.idKey, leaseIndex, raftCmd.MaxLeaseIndex,
			)
			retry = proposalIllegalLeaseIndex
		}
		return leaseIndex, retry, roachpb.NewErrorf(
			"command observed at lease index %d, but required < %d", leaseIndex, raftCmd.MaxLeaseIndex,
		)
	}
	return leaseIndex, proposalNoReevaluation, nil
}

type snapTruncationInfo struct {
	index    uint64
	deadline time.Time
}

func (r *Replica) addSnapshotLogTruncationConstraintLocked(
	ctx context.Context, snapUUID uuid.UUID, index uint64,
) {
	if r.mu.snapshotLogTruncationConstraints == nil {
		r.mu.snapshotLogTruncationConstraints = make(map[uuid.UUID]snapTruncationInfo)
	}
	item, ok := r.mu.snapshotLogTruncationConstraints[snapUUID]
	if ok {
		// Uh-oh, there's either a programming error (resulting in the same snapshot
		// fed into this method twice) or a UUID collision. We discard the update
		// (which is benign) but log it loudly. If the index is the same, it's
		// likely the former, otherwise the latter.
		log.Warningf(ctx, "UUID collision at %s for %+v (index %d)", snapUUID, item, index)
		return
	}

	r.mu.snapshotLogTruncationConstraints[snapUUID] = snapTruncationInfo{index: index}
}

func (r *Replica) completeSnapshotLogTruncationConstraint(
	ctx context.Context, snapUUID uuid.UUID, now time.Time,
) {
	deadline := now.Add(raftLogQueuePendingSnapshotGracePeriod)

	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.mu.snapshotLogTruncationConstraints[snapUUID]
	if !ok {
		// UUID collision while adding the snapshot in originally. Nothing
		// else to do.
		return
	}

	item.deadline = deadline
	r.mu.snapshotLogTruncationConstraints[snapUUID] = item
}

func (r *Replica) getAndGCSnapshotLogTruncationConstraintsLocked(
	now time.Time,
) (minSnapIndex uint64) {
	for snapUUID, item := range r.mu.snapshotLogTruncationConstraints {
		if item.deadline != (time.Time{}) && item.deadline.Before(now) {
			// The snapshot has finished and its grace period has passed.
			// Ignore it when making truncation decisions.
			delete(r.mu.snapshotLogTruncationConstraints, snapUUID)
			continue
		}
		if minSnapIndex == 0 || minSnapIndex > item.index {
			minSnapIndex = item.index
		}
	}
	if len(r.mu.snapshotLogTruncationConstraints) == 0 {
		// Save a little bit of memory.
		r.mu.snapshotLogTruncationConstraints = nil
	}
	return minSnapIndex
}

func isRaftLeader(raftStatus *raft.Status) bool {
	return raftStatus != nil && raftStatus.SoftState.RaftState == raft.StateLeader
}

// HasRaftLeader returns true if the raft group has a raft leader currently.
func HasRaftLeader(raftStatus *raft.Status) bool {
	return raftStatus != nil && raftStatus.SoftState.Lead != 0
}

// pendingCmdSlice sorts by increasing MaxLeaseIndex.
type pendingCmdSlice []*ProposalData

func (s pendingCmdSlice) Len() int      { return len(s) }
func (s pendingCmdSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s pendingCmdSlice) Less(i, j int) bool {
	return s[i].command.MaxLeaseIndex < s[j].command.MaxLeaseIndex
}

// withRaftGroupLocked calls the supplied function with the (lazily
// initialized) Raft group. The supplied function should return true for the
// unquiesceAndWakeLeader argument if the replica should be unquiesced (and the
// leader awoken). See handleRaftReady for an instance of where this value
// varies.
//
// Requires that Replica.mu is held. Also requires that Replica.raftMu is held
// if either the caller can't guarantee that r.mu.internalRaftGroup != nil or
// the provided function requires Replica.raftMu.
func (r *Replica) withRaftGroupLocked(
	mayCampaignOnWake bool, f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
	if r.mu.destroyStatus.RemovedOrCorrupt() {
		// Silently ignore all operations on destroyed replicas. We can't return an
		// error here as all errors returned from this method are considered fatal.
		return nil
	}

	if r.mu.replicaID == 0 {
		// The replica's raft group has not yet been configured (i.e. the replica
		// was created from a preemptive snapshot).
		return nil
	}

	if r.mu.internalRaftGroup == nil {
		r.raftMu.Mutex.AssertHeld()

		ctx := r.AnnotateCtx(context.TODO())
		raftGroup, err := raft.NewRawNode(newRaftConfig(
			raft.Storage((*replicaRaftStorage)(r)),
			uint64(r.mu.replicaID),
			r.mu.state.RaftAppliedIndex,
			r.store.cfg,
			&raftLogger{ctx: ctx},
		), nil)
		if err != nil {
			return err
		}
		r.mu.internalRaftGroup = raftGroup

		if mayCampaignOnWake {
			r.maybeCampaignOnWakeLocked(ctx)
		}
	}

	// This wrapper function is a hack to add range IDs to stack traces
	// using the same pattern as Replica.sendWithRangeID.
	unquiesce, err := func(rangeID roachpb.RangeID, raftGroup *raft.RawNode) (bool, error) {
		return f(raftGroup)
	}(r.RangeID, r.mu.internalRaftGroup)
	if unquiesce {
		r.unquiesceAndWakeLeaderLocked()
	}
	return err
}

// withRaftGroup calls the supplied function with the (lazily initialized)
// Raft group. It acquires and releases the Replica lock, so r.mu must not be
// held (or acquired by the supplied function).
//
// If mayCampaignOnWake is true, the replica may initiate a raft
// election if it was previously in a dormant state. Most callers
// should set this to true, because the prevote feature minimizes the
// disruption from unnecessary elections. The exception is that we
// should not initiate an election while handling incoming raft
// messages (which may include MsgVotes from an election in progress,
// and this election would be disrupted if we started our own).
//
// Has the same requirement for Replica.raftMu as withRaftGroupLocked.
func (r *Replica) withRaftGroup(
	mayCampaignOnWake bool, f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.withRaftGroupLocked(mayCampaignOnWake, f)
}

func shouldCampaignOnWake(
	leaseStatus storagepb.LeaseStatus,
	lease roachpb.Lease,
	storeID roachpb.StoreID,
	raftStatus raft.Status,
) bool {
	// When waking up a range, campaign unless we know that another
	// node holds a valid lease (this is most important after a split,
	// when all replicas create their raft groups at about the same
	// time, with a lease pre-assigned to one of them). Note that
	// thanks to PreVote, unnecessary campaigns are not disruptive so
	// we should err on the side of campaigining here.
	anotherOwnsLease := leaseStatus.State == storagepb.LeaseState_VALID && !lease.OwnedBy(storeID)

	// If we're already campaigning or know who the leader is, don't
	// start a new term.
	noLeader := raftStatus.RaftState == raft.StateFollower && raftStatus.Lead == 0
	return !anotherOwnsLease && noLeader
}

// maybeCampaignOnWakeLocked is called when the range wakes from a
// dormant state (either the initial "raftGroup == nil" state or after
// being quiescent) and campaigns for raft leadership if appropriate.
func (r *Replica) maybeCampaignOnWakeLocked(ctx context.Context) {
	// Raft panics if a node that is not currently a member of the
	// group tries to campaign. That happens primarily when we apply
	// preemptive snapshots.
	if _, currentMember := r.mu.state.Desc.GetReplicaDescriptorByID(r.mu.replicaID); !currentMember {
		return
	}

	leaseStatus := r.leaseStatus(*r.mu.state.Lease, r.store.Clock().Now(), r.mu.minLeaseProposedTS)
	raftStatus := r.mu.internalRaftGroup.Status()
	if shouldCampaignOnWake(leaseStatus, *r.mu.state.Lease, r.store.StoreID(), *raftStatus) {
		log.VEventf(ctx, 3, "campaigning")
		if err := r.mu.internalRaftGroup.Campaign(); err != nil {
			log.VEventf(ctx, 1, "failed to campaign: %s", err)
		}
	}
}

// a lastUpdateTimesMap is maintained on the Raft leader to keep track of the
// last communication received from followers, which in turn informs the quota
// pool and log truncations.
type lastUpdateTimesMap map[roachpb.ReplicaID]time.Time

func (m lastUpdateTimesMap) update(replicaID roachpb.ReplicaID, now time.Time) {
	if m == nil {
		return
	}
	m[replicaID] = now
}

// isFollowerActive returns whether the specified follower has made
// communication with the leader in the last MaxQuotaReplicaLivenessDuration.
func (m lastUpdateTimesMap) isFollowerActive(
	ctx context.Context, replicaID roachpb.ReplicaID, now time.Time,
) bool {
	lastUpdateTime, ok := m[replicaID]
	if !ok {
		// If the follower has no entry in lastUpdateTimes, it has not been
		// updated since r became the leader (at which point all then-existing
		// replicas were updated).
		return false
	}
	return now.Sub(lastUpdateTime) <= MaxQuotaReplicaLivenessDuration
}
