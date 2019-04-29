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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// insertProposalLocked assigns a MaxLeaseIndex to a proposal and adds
// it to the pending map. Returns the assigned MaxLeaseIndex, if any.
func (r *Replica) insertProposalLocked(proposal *ProposalData) int64 {
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
	if proposal.command.ProposerReplica == (roachpb.ReplicaDescriptor{}) {
		// 0 is a valid LeaseSequence value so we can't actually enforce it.
		log.Fatalf(context.TODO(), "ProposerReplica and ProposerLeaseSequence must be filled in")
	}

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

// evalAndPropose prepares the necessary pending command struct and initializes a
// client command ID if one hasn't been. A verified lease is supplied
// as a parameter if the command requires a lease; nil otherwise. It
// then evaluates the command and proposes it to Raft on success.
//
// Return values:
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
func (r *Replica) evalAndPropose(
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

	// Record the proposer and lease sequence.
	repDesc, err := r.getReplicaDescriptorRLocked()
	if err != nil {
		return nil, nil, 0, roachpb.NewError(err)
	}
	proposal.command.ProposerReplica = repDesc
	proposal.command.ProposerLeaseSequence = lease.Sequence

	maxLeaseIndex, pErr := r.proposeLocked(ctx, proposal)
	if pErr != nil {
		return nil, nil, 0, pErr
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

// proposeLocked starts tracking a command and proposes it to raft. If
// this method succeeds, the caller is responsible for eventually
// removing the proposal from the pending map (on success, in
// processRaftCommand, or on failure via cleanupFailedProposalLocked).
//
// This method requires that r.mu is held in all cases, and if
// r.mu.internalRaftGroup is nil, r.raftMu must also be held (note
// that lock ordering requires that raftMu be acquired first).
func (r *Replica) proposeLocked(
	ctx context.Context, proposal *ProposalData,
) (_ int64, pErr *roachpb.Error) {
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
		return 0, roachpb.NewError(r.mu.destroyStatus.err)
	}

	maxLeaseIndex := r.insertProposalLocked(proposal)

	if err := r.submitProposalLocked(proposal); err == raft.ErrProposalDropped {
		// Silently ignore dropped proposals (they were always silently ignored
		// prior to the introduction of ErrProposalDropped).
		// TODO(bdarnell): Handle ErrProposalDropped better.
		// https://github.com/cockroachdb/cockroach/issues/21849
	} else if err != nil {
		return 0, roachpb.NewError(err)
	}

	return maxLeaseIndex, nil
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
	cmdSize := p.command.Size()
	data := make([]byte, raftCommandPrefixLen+cmdSize)
	_, err := protoutil.MarshalToWithoutFuzzing(p.command, data[raftCommandPrefixLen:])
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
`, p.Request.Summary(), cmdSize,
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
	if size := cmdSize; size > largeProposalEventThresholdBytes {
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
			Payload:   data[raftCommandPrefixLen:], // chop off prefix
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

	if log.V(4) {
		log.Infof(p.ctx, "proposing command %x: %s", p.idKey, p.Request.Summary())
	}

	encodingVersion := raftVersionStandard
	if p.command.ReplicatedEvalResult.AddSSTable != nil {
		if p.command.ReplicatedEvalResult.AddSSTable.Data == nil {
			return errors.New("cannot sideload empty SSTable")
		}
		encodingVersion = raftVersionSideloaded
		r.store.metrics.AddSSTableProposals.Inc(1)
		log.Event(p.ctx, "sideloadable proposal detected")
	}
	encodeRaftCommandPrefix(data[:raftCommandPrefixLen], encodingVersion, p.idKey)

	return r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		// We're proposing a command so there is no need to wake the leader if
		// we're quiesced.
		r.unquiesceLocked()
		return false /* unquiesceAndWakeLeader */, raftGroup.Propose(data)
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
func (r *Replica) handleRaftReady(
	ctx context.Context, inSnap IncomingSnapshot,
) (handleRaftReadyStats, string, error) {
	defer func(start time.Time) {
		elapsed := timeutil.Since(start)
		r.store.metrics.RaftHandleReadyLatency.RecordValue(elapsed.Nanoseconds())
	}(timeutil.Now())
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	return r.handleRaftReadyRaftMuLocked(ctx, inSnap)
}

// handleRaftReadyLocked is the same as handleRaftReady but requires that the
// replica's raftMu be held.
//
// The returned string is nonzero whenever an error is returned to give a
// non-sensitive cue as to what happened.
func (r *Replica) handleRaftReadyRaftMuLocked(
	ctx context.Context, inSnap IncomingSnapshot,
) (handleRaftReadyStats, string, error) {
	var stats handleRaftReadyStats

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
	r.traceMessageSends(msgApps, "sending msgApp")
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
	commitStart := timeutil.Now()
	if err := batch.Commit(rd.MustSync && !disableSyncRaftLog.Get(&r.store.cfg.Settings.SV)); err != nil {
		const expl = "while committing batch"
		return stats, expl, errors.Wrap(err, expl)
	}
	elapsed := timeutil.Since(commitStart)
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
		r.store.replicateQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
	}

	// Update raft log entry cache. We clear any older, uncommitted log entries
	// and cache the latest ones.
	r.store.raftEntryCache.Add(r.RangeID, rd.Entries, true /* truncate */)
	r.sendRaftMessages(ctx, otherMsgs)
	r.traceEntries(rd.CommittedEntries, "committed, before applying any entries")
	applicationStart := timeutil.Now()
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
	applicationElapsed := timeutil.Since(applicationStart).Nanoseconds()
	r.store.metrics.RaftApplyCommittedLatency.RecordValue(applicationElapsed)
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
			continue
		}
		switch reason {
		case reasonSnapshotApplied:
			// If we applied a snapshot, check the MaxLeaseIndexes of all
			// pending commands to see if any are now prevented from
			// applying, and if so make them return an ambiguous error. We
			// can't tell at this point (which should be rare) whether they
			// were included in the snapshot we received or not.
			if p.command.MaxLeaseIndex <= r.mu.state.LeaseAppliedIndex {
				r.cleanupFailedProposalLocked(p)
				log.Eventf(p.ctx, "retry proposal %x: %s", p.idKey, reason)
				p.finishApplication(proposalResult{Err: roachpb.NewError(
					roachpb.NewAmbiguousResultError(
						fmt.Sprintf("unable to determine whether command was applied via snapshot")))})
			}
			continue

		case reasonTicks:
			if p.proposedAtTicks <= r.mu.ticks-refreshAtDelta {
				// The command was proposed a while ago and may have been dropped. Try it again.
				reproposals = append(reproposals, p)
			}

		default:
			// We have reason to believe that all pending proposals were
			// dropped on the floor (e.g. because of a leader election), so
			// repropose everything.
			reproposals = append(reproposals, p)
		}
	}

	if log.V(1) && len(reproposals) > 0 {
		ctx := r.AnnotateCtx(context.TODO())
		log.Infof(ctx,
			"pending commands: reproposing %d (at %d.%d) %s",
			len(reproposals), r.mu.state.RaftAppliedIndex,
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
		r.store.raftSnapshotQueue.AddAsync(ctx, r, raftSnapshotPriority)
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
				"retry proposal %x: applied at lease index %d, required < %d",
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
	if r.mu.destroyStatus.Removed() {
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

// updateOnUnquiesce is called when the leader unquiesces. In that case, we
// don't want live followers to appear as dead before their next message reaches
// us; to achieve that, we optimistically mark all followers that are in
// ProgressStateReplicate (or rather, were in that state when the group
// quiesced) as live as of `now`. We don't want to mark other followers as
// live as they may be down and could artificially seem alive forever assuming
// a suitable pattern of quiesce and unquiesce operations (and this in turn
// can interfere with Raft log truncations).
func (m lastUpdateTimesMap) updateOnUnquiesce(
	descs []roachpb.ReplicaDescriptor, prs map[uint64]raft.Progress, now time.Time,
) {
	for _, desc := range descs {
		if prs[uint64(desc.ReplicaID)].State == raft.ProgressStateReplicate {
			m.update(desc.ReplicaID, now)
		}
	}
}

// updateOnBecomeLeader is similar to updateOnUnquiesce, but is called when the
// replica becomes the Raft leader. It updates all followers irrespective of
// their Raft state, for the Raft state is not yet populated by the time this
// callback is invoked. Raft leadership is usually stable, so there is no danger
// of artificially keeping down followers alive, though if it started
// flip-flopping at a <10s cadence there would be a risk of that happening.
func (m lastUpdateTimesMap) updateOnBecomeLeader(descs []roachpb.ReplicaDescriptor, now time.Time) {
	for _, desc := range descs {
		m.update(desc.ReplicaID, now)
	}
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

// processRaftCommand handles the complexities involved in moving the Raft
// state of a Replica forward. At a high level, it receives a proposal, which
// contains the evaluation of a batch (at its heart a WriteBatch, to be applied
// to the underlying storage engine), which it applies and for which it signals
// the client waiting for it (if it's waiting on this Replica).
//
// The proposal also contains auxiliary data which needs to be verified in order
// to decide whether the proposal should be applied: the command's MaxLeaseIndex
// must move the state machine's LeaseAppliedIndex forward, and the proposer's
// lease (or rather its sequence number) must match that of the state machine.
// Furthermore, the GCThreshold is validated and it is checked whether the
// request's key span is contained in the Replica's (it is unclear whether all
// of these checks are necessary). If any of the checks fail, the proposal's
// content is wiped and we apply an empty log entry instead, returning an error
// to the caller to handle. The two typical cases are the lease mismatch (in
// which case the caller tries to send the command to the actual leaseholder)
// and violations of the LeaseAppliedIndex (in which the caller tries again).
//
// Assuming all checks were passed, the command should be applied to the engine,
// which is done by the aptly named applyRaftCommand.
//
// For simple proposals this is the whole story, but some commands trigger
// additional code in this method. The standard way in which this is triggered
// is via a side effect communicated in the proposal's ReplicatedEvalResult and,
// for local proposals, the LocalEvalResult. These might, for example, trigger
// an update of the Replica's in-memory state to match updates to the on-disk
// state, or pass intents to the intent resolver. Some commands don't fit this
// simple schema and need to hook deeper into the code. Notably splits and merges
// need to acquire locks on their right-hand side Replicas and may need to add
// data to the WriteBatch before it is applied; similarly, changes to the disk
// layout of internal state typically require a migration which shows up here.
//
// This method returns true if the command successfully applied a replica
// change.
func (r *Replica) processRaftCommand(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	term, raftIndex uint64,
	raftCmd storagepb.RaftCommand,
) (changedRepl bool) {
	if raftIndex == 0 {
		log.Fatalf(ctx, "processRaftCommand requires a non-zero index")
	}

	if log.V(4) {
		log.Infof(ctx, "processing command %x: maxLeaseIndex=%d", idKey, raftCmd.MaxLeaseIndex)
	}

	var ts hlc.Timestamp
	if idKey != "" {
		ts = raftCmd.ReplicatedEvalResult.Timestamp
	}

	r.mu.Lock()
	proposal, proposedLocally := r.mu.proposals[idKey]

	// TODO(tschottdorf): consider the Trace situation here.
	if proposedLocally {
		// We initiated this command, so use the caller-supplied context.
		ctx = proposal.ctx
		delete(r.mu.proposals, idKey)
	}

	leaseIndex, proposalRetry, forcedErr := r.checkForcedErrLocked(ctx, idKey, raftCmd, proposal, proposedLocally)

	r.mu.Unlock()

	if forcedErr == nil {
		// Verify that the batch timestamp is after the GC threshold. This is
		// necessary because not all commands declare read access on the GC
		// threshold key, even though they implicitly depend on it. This means
		// that access to this state will not be serialized by latching,
		// so we must perform this check upstream and downstream of raft.
		// See #14833.
		//
		// We provide an empty key span because we already know that the Raft
		// command is allowed to apply within its key range. This is guaranteed
		// by checks upstream of Raft, which perform the same validation, and by
		// span latches, which assure that any modifications to the range's
		// boundaries will be serialized with this command. Finally, the
		// leaseAppliedIndex check in checkForcedErrLocked ensures that replays
		// outside of the spanlatch manager's control which break this
		// serialization ordering will already by caught and an error will be
		// thrown.
		forcedErr = roachpb.NewError(r.requestCanProceed(roachpb.RSpan{}, ts))
	}

	// applyRaftCommand will return "expected" errors, but may also indicate
	// replica corruption (as of now, signaled by a replicaCorruptionError).
	// We feed its return through maybeSetCorrupt to act when that happens.
	if forcedErr != nil {
		log.VEventf(ctx, 1, "applying command with forced error: %s", forcedErr)
	} else {
		log.Event(ctx, "applying command")

		if splitMergeUnlock, err := r.maybeAcquireSplitMergeLock(ctx, raftCmd); err != nil {
			log.Eventf(ctx, "unable to acquire split lock: %s", err)
			// Send a crash report because a former bug in the error handling might have
			// been the root cause of #19172.
			_ = r.store.stopper.RunAsyncTask(ctx, "crash report", func(ctx context.Context) {
				log.SendCrashReport(
					ctx,
					&r.store.cfg.Settings.SV,
					0, // depth
					"while acquiring split lock: %s",
					[]interface{}{err},
					log.ReportTypeError,
				)
			})

			forcedErr = roachpb.NewError(err)
		} else if splitMergeUnlock != nil {
			// Close over raftCmd to capture its value at execution time; we clear
			// ReplicatedEvalResult on certain errors.
			defer func() {
				splitMergeUnlock(raftCmd.ReplicatedEvalResult)
			}()
		}
	}

	var response proposalResult
	var writeBatch *storagepb.WriteBatch
	{
		if filter := r.store.cfg.TestingKnobs.TestingApplyFilter; forcedErr == nil && filter != nil {
			var newPropRetry int
			newPropRetry, forcedErr = filter(storagebase.ApplyFilterArgs{
				CmdID:                idKey,
				ReplicatedEvalResult: raftCmd.ReplicatedEvalResult,
				StoreID:              r.store.StoreID(),
				RangeID:              r.RangeID,
			})
			if proposalRetry == 0 {
				proposalRetry = proposalReevaluationReason(newPropRetry)
			}
		}

		if forcedErr != nil {
			// Apply an empty entry.
			raftCmd.ReplicatedEvalResult = storagepb.ReplicatedEvalResult{}
			raftCmd.WriteBatch = nil
			raftCmd.LogicalOpLog = nil
		}

		// Update the node clock with the serviced request. This maintains
		// a high water mark for all ops serviced, so that received ops without
		// a timestamp specified are guaranteed one higher than any op already
		// executed for overlapping keys.
		r.store.Clock().Update(ts)

		var pErr *roachpb.Error
		if raftCmd.WriteBatch != nil {
			writeBatch = raftCmd.WriteBatch
		}

		if deprecatedDelta := raftCmd.ReplicatedEvalResult.DeprecatedDelta; deprecatedDelta != nil {
			raftCmd.ReplicatedEvalResult.Delta = deprecatedDelta.ToStatsDelta()
			raftCmd.ReplicatedEvalResult.DeprecatedDelta = nil
		}

		// AddSSTable ingestions run before the actual batch. This makes sure
		// that when the Raft command is applied, the ingestion has definitely
		// succeeded. Note that we have taken precautions during command
		// evaluation to avoid having mutations in the WriteBatch that affect
		// the SSTable. Not doing so could result in order reversal (and missing
		// values) here. If the key range we are ingesting into isn't empty,
		// we're not using AddSSTable but a plain WriteBatch.
		if raftCmd.ReplicatedEvalResult.AddSSTable != nil {
			copied := addSSTablePreApply(
				ctx,
				r.store.cfg.Settings,
				r.store.engine,
				r.raftMu.sideloaded,
				term,
				raftIndex,
				*raftCmd.ReplicatedEvalResult.AddSSTable,
				r.store.limiters.BulkIOWriteRate,
			)
			r.store.metrics.AddSSTableApplications.Inc(1)
			if copied {
				r.store.metrics.AddSSTableApplicationCopies.Inc(1)
			}
			raftCmd.ReplicatedEvalResult.AddSSTable = nil
		}

		if raftCmd.ReplicatedEvalResult.Split != nil {
			// Splits require a new HardState to be written to the new RHS
			// range (and this needs to be atomic with the main batch). This
			// cannot be constructed at evaluation time because it differs
			// on each replica (votes may have already been cast on the
			// uninitialized replica). Transform the write batch to add the
			// updated HardState.
			// See https://github.com/cockroachdb/cockroach/issues/20629
			//
			// This is not the most efficient, but it only happens on splits,
			// which are relatively infrequent and don't write much data.
			tmpBatch := r.store.engine.NewBatch()
			if err := tmpBatch.ApplyBatchRepr(writeBatch.Data, false); err != nil {
				log.Fatal(ctx, err)
			}
			splitPreApply(ctx, tmpBatch, raftCmd.ReplicatedEvalResult.Split.SplitTrigger)
			writeBatch.Data = tmpBatch.Repr()
			tmpBatch.Close()
		}

		if merge := raftCmd.ReplicatedEvalResult.Merge; merge != nil {
			// Merges require the subsumed range to be atomically deleted when the
			// merge transaction commits.
			//
			// This is not the most efficient, but it only happens on merges,
			// which are relatively infrequent and don't write much data.
			tmpBatch := r.store.engine.NewBatch()
			if err := tmpBatch.ApplyBatchRepr(writeBatch.Data, false); err != nil {
				log.Fatal(ctx, err)
			}
			rhsRepl, err := r.store.GetReplica(merge.RightDesc.RangeID)
			if err != nil {
				log.Fatal(ctx, err)
			}
			const destroyData = false
			err = rhsRepl.preDestroyRaftMuLocked(ctx, tmpBatch, tmpBatch, merge.RightDesc.NextReplicaID, destroyData)
			if err != nil {
				log.Fatal(ctx, err)
			}
			writeBatch.Data = tmpBatch.Repr()
			tmpBatch.Close()
		}

		{
			var err error
			raftCmd.ReplicatedEvalResult, err = r.applyRaftCommand(
				ctx, idKey, raftCmd.ReplicatedEvalResult, raftIndex, leaseIndex, writeBatch)

			// applyRaftCommand returned an error, which usually indicates
			// either a serious logic bug in CockroachDB or a disk
			// corruption/out-of-space issue. Make sure that these fail with
			// descriptive message so that we can differentiate the root causes.
			if err != nil {
				log.Errorf(ctx, "unable to update the state machine: %s", err)
				// Report the fatal error separately and only with the error, as that
				// triggers an optimization for which we directly report the error to
				// sentry (which in turn allows sentry to distinguish different error
				// types).
				log.Fatal(ctx, err)
			}
		}

		if filter := r.store.cfg.TestingKnobs.TestingPostApplyFilter; pErr == nil && filter != nil {
			var newPropRetry int
			newPropRetry, pErr = filter(storagebase.ApplyFilterArgs{
				CmdID:                idKey,
				ReplicatedEvalResult: raftCmd.ReplicatedEvalResult,
				StoreID:              r.store.StoreID(),
				RangeID:              r.RangeID,
			})
			if proposalRetry == 0 {
				proposalRetry = proposalReevaluationReason(newPropRetry)
			}

		}

		// calling maybeSetCorrupt here is mostly for tests and looks. The
		// interesting errors originate in applyRaftCommand, and they are
		// already handled above.
		pErr = r.maybeSetCorrupt(ctx, pErr)
		if pErr == nil {
			pErr = forcedErr
		}

		var lResult *result.LocalResult
		if proposedLocally {
			if proposalRetry != proposalNoReevaluation && pErr == nil {
				log.Fatalf(ctx, "proposal with nontrivial retry behavior, but no error: %+v", proposal)
			}
			if pErr != nil {
				// A forced error was set (i.e. we did not apply the proposal,
				// for instance due to its log position) or the Replica is now
				// corrupted.
				// If proposalRetry is set, we don't also return an error, as per the
				// proposalResult contract.
				if proposalRetry == proposalNoReevaluation {
					response.Err = pErr
				}
			} else if proposal.Local.Reply != nil {
				response.Reply = proposal.Local.Reply
			} else {
				log.Fatalf(ctx, "proposal must return either a reply or an error: %+v", proposal)
			}
			response.Intents = proposal.Local.DetachIntents()
			response.EndTxns = proposal.Local.DetachEndTxns(pErr != nil)
			if pErr == nil {
				lResult = proposal.Local
			}
		}
		if pErr != nil && lResult != nil {
			log.Fatalf(ctx, "shouldn't have a local result if command processing failed. pErr: %s", pErr)
		}
		if log.ExpensiveLogEnabled(ctx, 2) {
			log.VEvent(ctx, 2, lResult.String())
		}

		// Handle the Result, executing any side effects of the last
		// state machine transition.
		//
		// Note that this must happen after committing (the engine.Batch), but
		// before notifying a potentially waiting client.
		r.handleEvalResultRaftMuLocked(ctx, lResult,
			raftCmd.ReplicatedEvalResult, raftIndex, leaseIndex)

		// Provide the command's corresponding logical operations to the
		// Replica's rangefeed. Only do so if the WriteBatch is non-nil,
		// otherwise it's valid for the logical op log to be nil, which
		// would shut down all rangefeeds. If no rangefeed is running,
		// this call will be a no-op.
		if raftCmd.WriteBatch != nil {
			r.handleLogicalOpLogRaftMuLocked(ctx, raftCmd.LogicalOpLog)
		} else if raftCmd.LogicalOpLog != nil {
			log.Fatalf(ctx, "non-nil logical op log with nil write batch: %v", raftCmd)
		}
	}

	// When set to true, recomputes the stats for the LHS and RHS of splits and
	// makes sure that they agree with the state's range stats.
	const expensiveSplitAssertion = false

	if expensiveSplitAssertion && raftCmd.ReplicatedEvalResult.Split != nil {
		split := raftCmd.ReplicatedEvalResult.Split
		lhsStatsMS := r.GetMVCCStats()
		lhsComputedMS, err := rditer.ComputeStatsForRange(&split.LeftDesc, r.store.Engine(), lhsStatsMS.LastUpdateNanos)
		if err != nil {
			log.Fatal(ctx, err)
		}

		rightReplica, err := r.store.GetReplica(split.RightDesc.RangeID)
		if err != nil {
			log.Fatal(ctx, err)
		}

		rhsStatsMS := rightReplica.GetMVCCStats()
		rhsComputedMS, err := rditer.ComputeStatsForRange(&split.RightDesc, r.store.Engine(), rhsStatsMS.LastUpdateNanos)
		if err != nil {
			log.Fatal(ctx, err)
		}

		if diff := pretty.Diff(lhsStatsMS, lhsComputedMS); len(diff) > 0 {
			log.Fatalf(ctx, "LHS split stats divergence: diff(claimed, computed) = %s", pretty.Diff(lhsStatsMS, lhsComputedMS))
		}
		if diff := pretty.Diff(rhsStatsMS, rhsComputedMS); len(diff) > 0 {
			log.Fatalf(ctx, "RHS split stats divergence diff(claimed, computed) = %s", pretty.Diff(rhsStatsMS, rhsComputedMS))
		}
	}

	if proposedLocally {
		// If we failed to apply at the right lease index, try again with
		// a new one. This is important for pipelined writes, since they
		// don't have a client watching to retry, so a failure to
		// eventually apply the proposal would be a user-visible error.
		// TODO(nvanbenschoten): This reproposal is not tracked by the
		// quota pool. We should fix that.
		if proposalRetry == proposalIllegalLeaseIndex && r.tryReproposeWithNewLeaseIndex(proposal) {
			return false
		}
		// Otherwise, signal the command's status to the client.
		proposal.finishApplication(response)
	} else if response.Err != nil {
		log.VEventf(ctx, 1, "applying raft command resulted in error: %s", response.Err)
	}

	return raftCmd.ReplicatedEvalResult.ChangeReplicas != nil
}

// tryReproposeWithNewLeaseIndex is used by processRaftCommand to
// repropose commands that have gotten an illegal lease index error,
// and that we know could not have applied while their lease index was
// valid (that is, we observed all applied entries between proposal
// and the lease index becoming invalid, as opposed to skipping some
// of them by applying a snapshot).
//
// It is not intended for use elsewhere and is only a top-level
// function so that it can avoid the below_raft_protos check. Returns
// true if the command has been successfully reproposed (not
// necessarily by this method! But if this method returns true, the
// command will be in the local proposals map).
func (r *Replica) tryReproposeWithNewLeaseIndex(proposal *ProposalData) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Note that we don't need to validate anything about the proposal's
	// lease here - if we got this far, we know that everything but the
	// index is valid at this point in the log.
	if proposal.command.MaxLeaseIndex > r.mu.state.LeaseAppliedIndex {
		// If the command's MaxLeaseIndex is greater than the
		// LeaseAppliedIndex, it must have already been reproposed (this
		// can happen if there are multiple copies of the command in the
		// logs; see TestReplicaRefreshMultiple). We must not create
		// multiple copies with multiple lease indexes, so don't repropose
		// it again. This ensures that at any time, there is only up to a
		// single lease index that has a chance of succeeding in the Raft
		// log for a given command.
		//
		// Note that the caller has already removed the current version of
		// the proposal from the pending proposals map. We must re-add it
		// since it's still pending.
		log.VEventf(proposal.ctx, 2, "skipping reproposal, already reproposed at index %d",
			proposal.command.MaxLeaseIndex)
		r.mu.proposals[proposal.idKey] = proposal
		return true
	}
	// Some tests check for this log message in the trace.
	log.VEventf(proposal.ctx, 2, "retry: proposalIllegalLeaseIndex")
	if _, pErr := r.proposeLocked(proposal.ctx, proposal); pErr != nil {
		log.Warningf(proposal.ctx, "failed to repropose with new lease index: %s", pErr)
		return false
	}
	return true
}

// maybeAcquireSnapshotMergeLock checks whether the incoming snapshot subsumes
// any replicas and, if so, locks them for subsumption. See acquireMergeLock
// for details about the lock itself.
func (r *Replica) maybeAcquireSnapshotMergeLock(
	ctx context.Context, inSnap IncomingSnapshot,
) (subsumedRepls []*Replica, releaseMergeLock func()) {
	// Any replicas that overlap with the bounds of the incoming snapshot are ours
	// to subsume; further, the end of the last overlapping replica will exactly
	// align with the end of the snapshot. How are we guaranteed this? Each merge
	// could not have committed unless this store had an up-to-date replica of the
	// RHS at the time of the merge. Nothing could have removed that RHS replica,
	// as the replica GC queue cannot GC a replica unless it can prove its
	// left-hand neighbor has no pending merges to apply. And that RHS replica
	// could not have been further split or merged, as it never processes another
	// command after the merge commits.
	endKey := r.Desc().EndKey
	if endKey == nil {
		// The existing replica is unitialized, in which case we've already
		// installed a placeholder for snapshot's keyspace. No merge lock needed.
		return nil, func() {}
	}
	for endKey.Less(inSnap.State.Desc.EndKey) {
		sRepl := r.store.LookupReplica(endKey)
		if sRepl == nil || !endKey.Equal(sRepl.Desc().StartKey) {
			log.Fatalf(ctx, "snapshot widens existing replica, but no replica exists for subsumed key %s", endKey)
		}
		sRepl.raftMu.Lock()
		subsumedRepls = append(subsumedRepls, sRepl)
		endKey = sRepl.Desc().EndKey
	}
	// TODO(benesch): we may be unnecessarily forcing another Raft snapshot here
	// by subsuming too much. Consider the case where [a, b) and [c, e) first
	// merged into [a, e), then split into [a, d) and [d, e), and we're applying a
	// snapshot that spans this merge and split. The bounds of this snapshot will
	// be [a, d), so we'll subsume [c, e). But we're still a member of [d, e)!
	// We'll currently be forced to get a Raft snapshot to catch up. Ideally, we'd
	// subsume only half of [c, e) and synthesize a new RHS [d, e), effectively
	// applying both the split and merge during snapshot application. This isn't a
	// huge deal, though: we're probably behind enough that the RHS would need to
	// get caught up with a Raft snapshot anyway, even if we synthesized it
	// properly.
	return subsumedRepls, func() {
		for _, sr := range subsumedRepls {
			sr.raftMu.Unlock()
		}
	}
}

// maybeAcquireSplitMergeLock examines the given raftCmd (which need
// not be evaluated yet) and acquires the split or merge lock if
// necessary (in addition to other preparation). It returns a function
// which will release any lock acquired (or nil) and use the result of
// applying the command to perform any necessary cleanup.
func (r *Replica) maybeAcquireSplitMergeLock(
	ctx context.Context, raftCmd storagepb.RaftCommand,
) (func(storagepb.ReplicatedEvalResult), error) {
	if split := raftCmd.ReplicatedEvalResult.Split; split != nil {
		return r.acquireSplitLock(ctx, &split.SplitTrigger)
	} else if merge := raftCmd.ReplicatedEvalResult.Merge; merge != nil {
		return r.acquireMergeLock(ctx, &merge.MergeTrigger)
	}
	return nil, nil
}

func (r *Replica) acquireSplitLock(
	ctx context.Context, split *roachpb.SplitTrigger,
) (func(storagepb.ReplicatedEvalResult), error) {
	rightRng, created, err := r.store.getOrCreateReplica(ctx, split.RightDesc.RangeID, 0, nil)
	if err != nil {
		return nil, err
	}

	// It would be nice to assert that rightRng is not initialized
	// here. Unfortunately, due to reproposals and retries we might be executing
	// a reproposal for a split trigger that was already executed via a
	// retry. The reproposed command will not succeed (the transaction has
	// already committed).
	//
	// TODO(peter): It might be okay to return an error here, but it is more
	// conservative to hit the exact same error paths that we would hit for other
	// commands that have reproposals interacting with retries (i.e. we don't
	// treat splits differently).

	return func(rResult storagepb.ReplicatedEvalResult) {
		if rResult.Split == nil && created && !rightRng.IsInitialized() {
			// An error occurred during processing of the split and the RHS is still
			// uninitialized. Mark the RHS destroyed and remove it from the replica's
			// map as it is likely detritus. One reason this can occur is when
			// concurrent splits on the same key are executed. Only one of the splits
			// will succeed while the other will allocate a range ID, but fail to
			// commit.
			//
			// We condition this removal on whether the RHS was newly created in
			// order to be conservative. If a Raft message had created the Replica
			// then presumably it was alive for some reason other than a concurrent
			// split and shouldn't be destroyed.
			rightRng.mu.Lock()
			rightRng.mu.destroyStatus.Set(errors.Errorf("%s: failed to initialize", rightRng), destroyReasonRemoved)
			rightRng.mu.Unlock()
			r.store.mu.Lock()
			r.store.unlinkReplicaByRangeIDLocked(rightRng.RangeID)
			r.store.mu.Unlock()
		}
		rightRng.raftMu.Unlock()
	}, nil
}

func (r *Replica) acquireMergeLock(
	ctx context.Context, merge *roachpb.MergeTrigger,
) (func(storagepb.ReplicatedEvalResult), error) {
	// The merge lock is the right-hand replica's raftMu. The right-hand replica
	// is required to exist on this store. Otherwise, an incoming snapshot could
	// create the right-hand replica before the merge trigger has a chance to
	// widen the left-hand replica's end key. The merge trigger would then fatal
	// the node upon realizing the right-hand replica already exists. With a
	// right-hand replica in place, any snapshots for the right-hand range will
	// block on raftMu, waiting for the merge to complete, after which the replica
	// will realize it has been destroyed and reject the snapshot.
	rightRepl, _, err := r.store.getOrCreateReplica(ctx, merge.RightDesc.RangeID, 0, nil)
	if err != nil {
		return nil, err
	}
	rightDesc := rightRepl.Desc()
	if !rightDesc.StartKey.Equal(merge.RightDesc.StartKey) || !rightDesc.EndKey.Equal(merge.RightDesc.EndKey) {
		log.Fatalf(ctx, "RHS of merge %s <- %s not present on store; found %s in place of the RHS",
			merge.LeftDesc, merge.RightDesc, rightDesc)
	}
	return func(storagepb.ReplicatedEvalResult) {
		rightRepl.raftMu.Unlock()
	}, nil
}

// applyRaftCommand applies a raft command from the replicated log to the
// underlying state machine (i.e. the engine). When the state machine can not be
// updated, an error (which is likely fatal!) is returned and must be handled by
// the caller.
// The returned ReplicatedEvalResult replaces the caller's.
func (r *Replica) applyRaftCommand(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	rResult storagepb.ReplicatedEvalResult,
	raftAppliedIndex, leaseAppliedIndex uint64,
	writeBatch *storagepb.WriteBatch,
) (storagepb.ReplicatedEvalResult, error) {
	if writeBatch != nil && len(writeBatch.Data) > 0 {
		// Record the write activity, passing a 0 nodeID because replica.writeStats
		// intentionally doesn't track the origin of the writes.
		mutationCount, err := engine.RocksDBBatchCount(writeBatch.Data)
		if err != nil {
			log.Errorf(ctx, "unable to read header of committed WriteBatch: %s", err)
		} else {
			r.writeStats.recordCount(float64(mutationCount), 0 /* nodeID */)
		}
	}

	r.mu.Lock()
	usingAppliedStateKey := r.mu.state.UsingAppliedStateKey
	oldRaftAppliedIndex := r.mu.state.RaftAppliedIndex
	oldLeaseAppliedIndex := r.mu.state.LeaseAppliedIndex
	oldTruncatedState := r.mu.state.TruncatedState

	// Exploit the fact that a split will result in a full stats
	// recomputation to reset the ContainsEstimates flag.
	//
	// TODO(tschottdorf): We want to let the usual MVCCStats-delta
	// machinery update our stats for the left-hand side. But there is no
	// way to pass up an MVCCStats object that will clear out the
	// ContainsEstimates flag. We should introduce one, but the migration
	// makes this worth a separate effort (ContainsEstimates would need to
	// have three possible values, 'UNCHANGED', 'NO', and 'YES').
	// Until then, we're left with this rather crude hack.
	if rResult.Split != nil {
		r.mu.state.Stats.ContainsEstimates = false
	}
	ms := *r.mu.state.Stats
	r.mu.Unlock()

	if raftAppliedIndex != oldRaftAppliedIndex+1 {
		// If we have an out of order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return
		// a corruption error.
		return storagepb.ReplicatedEvalResult{}, errors.Errorf("applied index jumped from %d to %d",
			oldRaftAppliedIndex, raftAppliedIndex)
	}

	haveTruncatedState := rResult.State != nil && rResult.State.TruncatedState != nil
	var batch engine.Batch
	if !haveTruncatedState {
		batch = r.store.Engine().NewWriteOnlyBatch()
	} else {
		// When we update the truncated state, we may need to read the batch
		// and can't use a WriteOnlyBatch. This is fine since log truncations
		// are tiny batches.
		batch = r.store.Engine().NewBatch()
	}
	defer batch.Close()

	if writeBatch != nil {
		if err := batch.ApplyBatchRepr(writeBatch.Data, false); err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to apply WriteBatch")
		}
	}

	// The only remaining use of the batch is for range-local keys which we know
	// have not been previously written within this batch.
	writer := batch.Distinct()

	// Special-cased MVCC stats handling to exploit commutativity of stats delta
	// upgrades. Thanks to commutativity, the spanlatch manager does not have to
	// serialize on the stats key.
	deltaStats := rResult.Delta.ToStats()

	if !usingAppliedStateKey && rResult.State != nil && rResult.State.UsingAppliedStateKey {
		// The Raft command wants us to begin using the RangeAppliedState key
		// and we haven't performed the migration yet. Delete the old keys
		// that this new key is replacing.
		err := r.raftMu.stateLoader.MigrateToRangeAppliedStateKey(ctx, writer, &deltaStats)
		if err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to migrate to range applied state")
		}
		usingAppliedStateKey = true
	}

	if usingAppliedStateKey {
		// Note that calling ms.Add will never result in ms.LastUpdateNanos
		// decreasing (and thus LastUpdateNanos tracks the maximum LastUpdateNanos
		// across all deltaStats).
		ms.Add(deltaStats)

		// Set the range applied state, which includes the last applied raft and
		// lease index along with the mvcc stats, all in one key.
		if err := r.raftMu.stateLoader.SetRangeAppliedState(ctx, writer,
			raftAppliedIndex, leaseAppliedIndex, &ms); err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to set range applied state")
		}
	} else {
		// Advance the last applied index. We use a blind write in order to avoid
		// reading the previous applied index keys on every write operation. This
		// requires a little additional work in order maintain the MVCC stats.
		var appliedIndexNewMS enginepb.MVCCStats
		if err := r.raftMu.stateLoader.SetLegacyAppliedIndexBlind(ctx, writer, &appliedIndexNewMS,
			raftAppliedIndex, leaseAppliedIndex); err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to set applied index")
		}
		deltaStats.SysBytes += appliedIndexNewMS.SysBytes -
			r.raftMu.stateLoader.CalcAppliedIndexSysBytes(oldRaftAppliedIndex, oldLeaseAppliedIndex)

		// Note that calling ms.Add will never result in ms.LastUpdateNanos
		// decreasing (and thus LastUpdateNanos tracks the maximum LastUpdateNanos
		// across all deltaStats).
		ms.Add(deltaStats)
		if err := r.raftMu.stateLoader.SetMVCCStats(ctx, writer, &ms); err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to update MVCCStats")
		}
	}

	if haveTruncatedState {
		apply, err := handleTruncatedStateBelowRaft(ctx, oldTruncatedState, rResult.State.TruncatedState, r.raftMu.stateLoader, writer)
		if err != nil {
			return storagepb.ReplicatedEvalResult{}, err
		}
		if !apply {
			// The truncated state was discarded, so make sure we don't apply
			// it to our in-memory state.
			rResult.State.TruncatedState = nil
			rResult.RaftLogDelta = 0
			// We received a truncation that doesn't apply to us, so we know that
			// there's a leaseholder out there with a log that has earlier entries
			// than ours. That leader also guided our log size computations by
			// giving us RaftLogDeltas for past truncations, and this was likely
			// off. Mark our Raft log size is not trustworthy so that, assuming
			// we step up as leader at some point in the future, we recompute
			// our numbers.
			r.mu.Lock()
			r.mu.raftLogSizeTrusted = false
			r.mu.Unlock()
		}
	}

	// TODO(peter): We did not close the writer in an earlier version of
	// the code, which went undetected even though we used the batch after
	// (though only to commit it). We should add an assertion to prevent that in
	// the future.
	writer.Close()

	start := timeutil.Now()

	var assertHS *raftpb.HardState
	if util.RaceEnabled && rResult.Split != nil && r.store.cfg.Settings.Version.IsActive(cluster.VersionSplitHardStateBelowRaft) {
		rsl := stateloader.Make(rResult.Split.RightDesc.RangeID)
		oldHS, err := rsl.LoadHardState(ctx, r.store.Engine())
		if err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to load HardState")
		}
		assertHS = &oldHS
	}
	if err := batch.Commit(false); err != nil {
		return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "could not commit batch")
	}

	if assertHS != nil {
		// Load the HardState that was just committed (if any).
		rsl := stateloader.Make(rResult.Split.RightDesc.RangeID)
		newHS, err := rsl.LoadHardState(ctx, r.store.Engine())
		if err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to load HardState")
		}
		// Assert that nothing moved "backwards".
		if newHS.Term < assertHS.Term || (newHS.Term == assertHS.Term && newHS.Commit < assertHS.Commit) {
			log.Fatalf(ctx, "clobbered HardState: %s\n\npreviously: %s\noverwritten with: %s",
				pretty.Diff(newHS, *assertHS), pretty.Sprint(*assertHS), pretty.Sprint(newHS))
		}
	}

	elapsed := timeutil.Since(start)
	r.store.metrics.RaftCommandCommitLatency.RecordValue(elapsed.Nanoseconds())
	rResult.Delta = deltaStats.ToStatsDelta()
	return rResult, nil
}

// handleTruncatedStateBelowRaft is called when a Raft command updates the truncated
// state. This isn't 100% trivial for two reasons:
// - in 19.1 we're making the TruncatedState key unreplicated, so there's a migration
// - we're making use of the above by not sending the Raft log in snapshots (the truncated
//   state effectively determines the first index of the log, which requires it to be unreplicated).
//   Updates to the HardState are sent out by a leaseholder truncating the log based on its local
//   knowledge. For example, the leader might have a log 10..100 and truncates to 50, and will send
//   out a TruncatedState with Index 50 to that effect. However, some replicas may not even have log
//   entries that old, and must make sure to ignore this update to the truncated state, as it would
//   otherwise clobber their "newer" truncated state.
//
// The returned boolean tells the caller whether to apply the truncated state's
// side effects, which means replacing the in-memory TruncatedState and applying
// the associated RaftLogDelta. It is usually expected to be true, but may not
// be for the first truncation after on a replica that recently received a
// snapshot.
func handleTruncatedStateBelowRaft(
	ctx context.Context,
	oldTruncatedState, newTruncatedState *roachpb.RaftTruncatedState,
	loader stateloader.StateLoader,
	distinctEng engine.ReadWriter,
) (_apply bool, _ error) {
	// If this is a log truncation, load the resulting unreplicated or legacy
	// replicated truncated state (in that order). If the migration is happening
	// in this command, the result will be an empty message. In steady state
	// after the migration, it's the unreplicated truncated state not taking
	// into account the current truncation (since the key is unreplicated).
	// Either way, we'll update it below.
	//
	// See VersionUnreplicatedRaftTruncatedState for details.
	truncStatePostApply, truncStateIsLegacy, err := loader.LoadRaftTruncatedState(ctx, distinctEng)
	if err != nil {
		return false, errors.Wrap(err, "loading truncated state")
	}

	// Truncate the Raft log from the entry after the previous
	// truncation index to the new truncation index. This is performed
	// atomically with the raft command application so that the
	// TruncatedState index is always consistent with the state of the
	// Raft log itself. We can use the distinct writer because we know
	// all writes will be to distinct keys.
	//
	// Intentionally don't use range deletion tombstones (ClearRange())
	// due to performance concerns connected to having many range
	// deletion tombstones. There is a chance that ClearRange will
	// perform well here because the tombstones could be "collapsed",
	// but it is hardly worth the risk at this point.
	prefixBuf := &loader.RangeIDPrefixBuf
	for idx := oldTruncatedState.Index + 1; idx <= newTruncatedState.Index; idx++ {
		// NB: RangeIDPrefixBufs have sufficient capacity (32 bytes) to
		// avoid allocating when constructing Raft log keys (16 bytes).
		unsafeKey := prefixBuf.RaftLogKey(idx)
		if err := distinctEng.Clear(engine.MakeMVCCMetadataKey(unsafeKey)); err != nil {
			return false, errors.Wrapf(err, "unable to clear truncated Raft entries for %+v", newTruncatedState)
		}
	}

	if !truncStateIsLegacy {
		if truncStatePostApply.Index < newTruncatedState.Index {
			// There are two cases here (though handled just the same). In the
			// first case, the Raft command has just deleted the legacy
			// replicated truncated state key as part of the migration (so
			// truncStateIsLegacy is now false for the first time and
			// truncStatePostApply is zero) and we need to atomically write the
			// new, unreplicated, key. Or we've already migrated earlier, in
			// which case truncStatePostApply equals the current value of the
			// new key (which wasn't touched by the batch), and we need to
			// overwrite it if this truncation "moves it forward".

			// NB: before the first log truncation evaluated under the
			// cluster version which activates this code (see anchor below) this
			// block is never reached as truncStatePostApply will equal newTruncatedState.
			_ = cluster.VersionUnreplicatedRaftTruncatedState

			if err := engine.MVCCPutProto(
				ctx, distinctEng, nil /* ms */, prefixBuf.RaftTruncatedStateKey(),
				hlc.Timestamp{}, nil /* txn */, newTruncatedState,
			); err != nil {
				return false, errors.Wrap(err, "unable to migrate RaftTruncatedState")
			}
			// Have migrated and this new truncated state is moving us forward.
			// Tell caller that we applied it and that so should they.
			return true, nil
		}
		// Have migrated, but this truncated state moves the existing one
		// backwards, so instruct caller to not update in-memory state.
		return false, nil
	}
	// Haven't migrated yet, don't ever discard the update.
	return true, nil
}

// ComputeRaftLogSize computes the size (in bytes) of the Raft log from the
// storage engine. This will iterate over the Raft log and sideloaded files, so
// depending on the size of these it can be mildly to extremely expensive and
// thus should not be called frequently.
//
// The sideloaded storage may be nil, in which case it is treated as empty.
func ComputeRaftLogSize(
	ctx context.Context, rangeID roachpb.RangeID, reader engine.Reader, sideloaded SideloadStorage,
) (int64, error) {
	prefix := keys.RaftLogPrefix(rangeID)
	prefixEnd := prefix.PrefixEnd()
	iter := reader.NewIterator(engine.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixEnd,
	})
	defer iter.Close()
	from := engine.MakeMVCCMetadataKey(prefix)
	to := engine.MakeMVCCMetadataKey(prefixEnd)
	ms, err := iter.ComputeStats(from, to, 0 /* nowNanos */)
	if err != nil {
		return 0, err
	}
	var totalSideloaded int64
	if sideloaded != nil {
		var err error
		// Truncating all indexes strictly smaller than zero is a no-op but
		// gives us the number of bytes in the storage back.
		_, totalSideloaded, err = sideloaded.TruncateTo(ctx, 0)
		if err != nil {
			return 0, err
		}
	}
	return ms.SysBytes + totalSideloaded, nil
}
