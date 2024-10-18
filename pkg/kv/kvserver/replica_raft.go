// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/poison"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var (
	// raftLogTruncationClearRangeThreshold is the number of entries at which Raft
	// log truncation uses a Pebble range tombstone rather than point deletes. It
	// is set high enough to avoid writing too many range tombstones to Pebble,
	// but low enough that we don't do too many point deletes either (in
	// particular, we don't want to overflow the Pebble write batch).
	//
	// In the steady state, Raft log truncation occurs when RaftLogQueueStaleSize
	// (64 KB) or RaftLogQueueStaleThreshold (100 entries) is exceeded, so
	// truncations are generally small. If followers are lagging, we let the log
	// grow to RaftLogTruncationThreshold (16 MB) before truncating.
	//
	// 100k was chosen because it is unlikely to be hit in most common cases,
	// keeping the number of range tombstones low, but will trigger when Raft logs
	// have grown abnormally large. RaftLogTruncationThreshold will typically not
	// trigger it, unless the average log entry is <= 160 bytes. The key size is
	// ~16 bytes, so Pebble point deletion batches will be bounded at ~1.6MB.
	raftLogTruncationClearRangeThreshold = kvpb.RaftIndex(metamorphic.ConstantWithTestRange(
		"raft-log-truncation-clearrange-threshold", 100000 /* default */, 1 /* min */, 1e6 /* max */))

	// raftDisableLeaderFollowsLeaseholder disables lease/leader colocation.
	raftDisableLeaderFollowsLeaseholder = envutil.EnvOrDefaultBool(
		"COCKROACH_DISABLE_LEADER_FOLLOWS_LEASEHOLDER", false)
)

// evalAndPropose prepares the necessary pending command struct and initializes
// a client command ID if one hasn't been. A verified lease is supplied as a
// parameter if the command requires a lease; nil otherwise. It then evaluates
// the command and proposes it to Raft on success.
//
// The method accepts a concurrency guard, which it assumes responsibility for
// if it succeeds in proposing a command into Raft. If the method does not
// return an error, the guard is guaranteed to be eventually freed and the
// caller should relinquish all ownership of it. If it does return an error, the
// caller retains full ownership over the guard.
//
// evalAndPropose takes ownership of the supplied token; the caller should
// tok.Move() it into this method. It will be used to untrack the request once
// it comes out of the proposal buffer.
//
// Nothing here or below can take out a raftMu lock, since executeWriteBatch()
// is already holding readOnlyCmdMu when calling this. Locking raftMu after it
// would violate the locking order specified for Store.mu.
//
// Return values:
//   - a channel which receives a response or error upon application
//   - a closure used to attempt to abandon the command. When called, it unbinds
//     the command's context from its Raft proposal. The client is then free to
//     terminate execution, although it is given no guarantee that the proposal
//     won't still go on to commit and apply at some later time.
//   - the proposal's ID.
//   - any error obtained during the creation or proposal of the command, in
//     which case the other returned values are zero.
func (r *Replica) evalAndPropose(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	g *concurrency.Guard,
	st *kvserverpb.LeaseStatus,
	ui uncertainty.Interval,
	tok TrackedRequestToken,
) (
	chan proposalResult,
	func(),
	kvserverbase.CmdIDKey,
	*kvadmission.StoreWriteBytes,
	*kvpb.Error,
) {
	defer tok.DoneIfNotMoved(ctx)
	idKey := raftlog.MakeCmdIDKey()
	proposal, pErr := r.requestToProposal(ctx, idKey, ba, g, st, ui)
	ba = proposal.Request // may have been updated
	log.Event(proposal.Context(), "evaluated request")

	// If the request hit a server-side concurrency retry error, immediately
	// propagate the error. Don't assume ownership of the concurrency guard.
	if isConcurrencyRetryError(pErr) {
		pErr = maybeAttachLease(pErr, &st.Lease)
		return nil, nil, "", nil, pErr
	} else if _, ok := pErr.GetDetail().(*kvpb.ReplicaCorruptionError); ok {
		return nil, nil, "", nil, pErr
	}

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
		if proposal.Local.RequiresRaft() {
			return nil, nil, "", nil, kvpb.NewError(errors.AssertionFailedf(
				"proposal resulting from batch %s erroneously bypassed Raft", ba))
		}
		intents := proposal.Local.DetachEncounteredIntents()
		endTxns := proposal.Local.DetachEndTxns(pErr != nil /* alwaysOnly */)
		r.handleReadWriteLocalEvalResult(ctx, *proposal.Local)

		// NB: it is intentional that this returns both an error and results.
		// Some actions should also be taken if the command itself fails. For
		// example, discovered intents should be pushed to make sure they get
		// dealt with proactively rather than waiting for a future command to
		// find them.
		proposal.ec = makeUnreplicatedEndCmds(r, g, *st)
		pr := makeProposalResult(proposal.Local.Reply, pErr, intents, endTxns)
		proposal.finishApplication(ctx, pr)
		return proposalCh, func() {}, "", nil, nil
	}

	// Make it a truly replicated proposal. We measure the replication latency
	// from this point on.
	proposal.ec = makeReplicatedEndCmds(r, g, *st, timeutil.Now())

	log.VEventf(proposal.Context(), 2,
		"proposing command to write %d new keys, %d new values, %d new intents, "+
			"write batch size=%d bytes",
		proposal.command.ReplicatedEvalResult.Delta.KeyCount,
		proposal.command.ReplicatedEvalResult.Delta.ValCount,
		proposal.command.ReplicatedEvalResult.Delta.IntentCount,
		proposal.command.WriteBatch.Size(),
	)
	// NB: if ba.AsyncConsensus is true, we will tell admission control about
	// writes that may not have happened yet. We consider this ok, since (a) the
	// typical lag in consensus is expected to be small compared to the time
	// granularity of admission control doing token and size estimation (which
	// is 15s). Also, admission control corrects for gaps in reporting.
	writeBytes := kvadmission.NewStoreWriteBytes()
	if proposal.command.WriteBatch != nil {
		writeBytes.WriteBytes = int64(len(proposal.command.WriteBatch.Data))
	}
	if proposal.command.ReplicatedEvalResult.AddSSTable != nil {
		writeBytes.IngestedBytes = int64(len(proposal.command.ReplicatedEvalResult.AddSSTable.Data))
	}
	// If the request requested that Raft consensus be performed asynchronously,
	// return a proposal result immediately on the proposal's done channel.
	// The channel's capacity will be large enough to accommodate this.
	maybeFinishSpan := func() {}
	defer func() { maybeFinishSpan() }() // NB: late binding is important
	if ba.AsyncConsensus {
		if ets := proposal.Local.DetachEndTxns(false /* alwaysOnly */); len(ets) != 0 {
			// Disallow async consensus for commands with EndTxnIntents because
			// any !Always EndTxnIntent can't be cleaned up until after the
			// command succeeds.
			return nil, nil, "", writeBytes, kvpb.NewErrorf("cannot perform consensus asynchronously for "+
				"proposal with EndTxnIntents=%v; %v", ets, ba)
		}

		// Fork the proposal's context span so that the proposal's context
		// can outlive the original proposer's context.
		ctx, sp := tracing.ForkSpan(ctx, "async consensus")
		proposal.ctx.Store(&ctx)
		proposal.sp = sp
		if proposal.sp != nil {
			// We can't leak this span if we fail to hand the proposal to the
			// replication layer, so finish it later in this method if we are to
			// return with an error. (On success, we'll reset this to a noop).
			maybeFinishSpan = proposal.sp.Finish
		}

		// Signal the proposal's response channel immediately. Return a shallow-ish
		// copy of the response to avoid aliasing issues if the client mutates the
		// batch response header or individual response headers before replication
		// completes.
		reply := *proposal.Local.Reply
		reply.Responses = append([]kvpb.ResponseUnion(nil), reply.Responses...)
		for i, ru := range reply.Responses {
			reply.Responses[i].MustSetInner(ru.GetInner().ShallowCopy())
		}
		pr := makeProposalResult(&reply, nil /* pErr */, proposal.Local.DetachEncounteredIntents(), nil /* eti */)
		proposal.signalProposalResult(pr)

		// Continue with proposal...
	}

	if meta := kvflowcontrol.MetaFromContext(ctx); meta != nil {
		proposal.raftAdmissionMeta = meta
	}

	// Attach information about the proposer's lease to the command, for
	// verification below raft. Lease requests are special since they are not
	// necessarily proposed under a valid lease (by necessity). Instead, they
	// reference the previous lease. Note that TransferLease also skip lease
	// checks (for technical reasons, see `TransferLease.flags`) and uses the
	// same mechanism.
	if ba.IsSingleSkipsLeaseCheckRequest() {
		// Lease-related commands have below-raft special casing and will carry the
		// lease sequence of the lease they are intending to follow.
		// The remaining requests that skip a lease check (at the time of writing
		// ProbeRequest) will assign a zero lease sequence and thus won't be able
		// to mutate state.
		var seq roachpb.LeaseSequence
		switch t := ba.Requests[0].GetInner().(type) {
		case *kvpb.RequestLeaseRequest:
			seq = t.PrevLease.Sequence
		case *kvpb.TransferLeaseRequest:
			seq = t.PrevLease.Sequence
		default:
		}
		proposal.command.ProposerLeaseSequence = seq
	} else if !st.Lease.OwnedBy(r.store.StoreID()) {
		// Perform a sanity check that the lease is owned by this replica. This must
		// have been ascertained by the callers in
		// checkExecutionCanProceedBeforeStorageSnapshot.
		log.Fatalf(ctx, "cannot propose %s on follower with remotely owned lease %s", ba, st.Lease)
	} else {
		proposal.command.ProposerLeaseSequence = st.Lease.Sequence
	}

	// Once a command is written to the raft log, it must be loaded into memory
	// and replayed on all replicas. If a command is too big, stop it here. If
	// the command is not too big, acquire an appropriate amount of quota from
	// the replica's proposal quota pool.
	//
	// TODO(tschottdorf): blocking a proposal here will leave it dangling in the
	// closed timestamp tracker for an extended period of time, which will in turn
	// prevent the node-wide closed timestamp from making progress. This is quite
	// unfortunate; we should hoist the quota pool before the reference with the
	// closed timestamp tracker is acquired. This is better anyway; right now many
	// commands can evaluate but then be blocked on quota, which has worse memory
	// behavior.
	quotaSize := uint64(proposal.command.Size())
	if maxSize := uint64(kvserverbase.MaxCommandSize.Get(&r.store.cfg.Settings.SV)); quotaSize > maxSize {
		return nil, nil, "", nil, kvpb.NewError(errors.Errorf(
			"command is too large: %d bytes (max: %d)", quotaSize, maxSize,
		))
	}
	log.VEventf(proposal.Context(), 2, "acquiring proposal quota (%d bytes)", quotaSize)
	var err error
	proposal.quotaAlloc, err = r.maybeAcquireProposalQuota(ctx, ba, quotaSize)
	if err != nil {
		return nil, nil, "", nil, kvpb.NewError(err)
	}
	// Make sure we clean up the proposal if we fail to insert it into the
	// proposal buffer successfully. This ensures that we always release any
	// quota that we acquire.
	defer func() {
		if pErr != nil {
			proposal.releaseQuota()
		}
	}()

	if filter := r.store.TestingKnobs().TestingProposalFilter; filter != nil {
		filterArgs := kvserverbase.ProposalFilterArgs{
			Ctx:        ctx,
			RangeID:    r.RangeID,
			StoreID:    r.store.StoreID(),
			ReplicaID:  r.replicaID,
			Cmd:        proposal.command,
			QuotaAlloc: proposal.quotaAlloc,
			CmdID:      idKey,
			Req:        proposal.Request,
			// SeedID not set, since this is not a reproposal.
		}
		if pErr = filter(filterArgs); pErr != nil {
			return nil, nil, "", nil, pErr
		}
	}

	pErr = r.propose(ctx, proposal, tok.Move(ctx))
	if pErr != nil {
		return nil, nil, "", nil, pErr
	}
	// We've successfully handed the proposal to the replication layer, so this
	// method should not finish the trace span if we forked one off above.
	maybeFinishSpan = func() {}
	// Abandoning a proposal unbinds its context so that the proposal's client
	// is free to terminate execution. However, it does nothing to try to
	// prevent the command from succeeding. In particular, endCmds will still be
	// invoked when the command is applied. There are a handful of cases where
	// the command may not be applied (or even processed): the process crashes
	// or the local replica is removed from the range.
	abandon := func() {
		// The proposal may or may not be in the Replica's proposals map.
		// Instead of trying to look it up, simply modify the captured object
		// directly. The raftMu must be locked to modify the context of a
		// proposal because as soon as we propose a command to Raft, ownership
		// passes to the "below Raft" machinery.
		//
		// See the comment on ProposalData.
		r.raftMu.Lock()
		defer r.raftMu.Unlock()
		r.mu.Lock()
		defer r.mu.Unlock()
		// When the caller abandons the request, it Finishes its trace. By that
		// time, multiple reproposals can have occurred, and still running and
		// attempting to post tracing updates through the context. This can cause a
		// "use after Finish" race in the span. All the (re-)proposal contexts have
		// been unbound except for the latest one. Unbind it to eliminate the race.
		//
		// See https://github.com/cockroachdb/cockroach/issues/107521
		last := proposal
		if p := proposal.lastReproposal; p != nil {
			last = p
		}
		// TODO(radu): Should this context be created via tracer.ForkSpan?
		// We'd need to make sure the span is finished eventually.
		ctx := r.AnnotateCtx(context.TODO())
		last.ctx.Store(&ctx)
	}
	return proposalCh, abandon, idKey, writeBytes, nil
}

func (r *Replica) encodePriorityForRACv2() bool {
	return r.flowControlV2.GetEnabledWhenLeader() == kvflowcontrol.V2EnabledWhenLeaderV2Encoding
}

// propose encodes a command, starts tracking it, and proposes it to Raft.
//
// The method hands ownership of the command over to the Raft machinery. After
// the method returns, all access to the command must be performed while holding
// Replica.mu and Replica.raftMu.
//
// propose takes ownership of the supplied token; the caller should tok.Move()
// it into this method. It will be used to untrack the request once it comes out
// of the proposal buffer.
//
// Note that this method is called for "new" proposals but also by
// `tryReproposeWithNewLeaseIndexRaftMuLocked`. This second call leaves questions on what
// exactly the desired semantics are - some fields (MaxLeaseIndex,
// ClosedTimestamp) will be set and this re-entrance into `propose`
// is hard to fully understand. (The reset of `MaxLeaseIndex`	inside this
// method is a faer-fueled but likely unneeded consequence of this).
//
// TODO(repl): adopt the below issue which will see each proposal passed to this
// method exactly once:
//
// https://github.com/cockroachdb/cockroach/issues/98477
func (r *Replica) propose(
	ctx context.Context, p *ProposalData, tok TrackedRequestToken,
) (pErr *kvpb.Error) {
	defer tok.DoneIfNotMoved(ctx)

	if p.command.MaxLeaseIndex > 0 {
		// TODO: there are a number of other fields that should still be unset.
		// Verify them all. Some architectural improvements where we pass in a
		// subset of ProposalData and then complete it here would be even better.
		return kvpb.NewError(errors.AssertionFailedf("MaxLeaseIndex is set: %+v", p))
	}

	if crt := p.command.ReplicatedEvalResult.ChangeReplicas; crt != nil {
		if err := checkReplicationChangeAllowed(p.command, r.Desc(), r.StoreID()); err != nil {
			log.Errorf(ctx, "%v", err)
			return kvpb.NewError(err)
		}
		log.KvDistribution.Infof(p.Context(), "proposing %s", crt)
	} else if p.command.ReplicatedEvalResult.AddSSTable != nil {
		log.VEvent(p.Context(), 4, "sideloadable proposal detected")
		r.store.metrics.AddSSTableProposals.Inc(1)
	} else if log.V(4) {
		log.Infof(p.Context(), "proposing command %x: %s", p.idKey, p.Request.Summary())
	}

	raftAdmissionMeta := p.raftAdmissionMeta
	if !p.useReplicationAdmissionControl() {
		raftAdmissionMeta = nil
	}

	encodePriority := r.encodePriorityForRACv2()
	if encodePriority && raftAdmissionMeta != nil {
		// AdmissionPriority is the same for both v1 and v2 replication flow control
		// until we get here. If the v2 encoding is enabled, we need to convert the
		// priority to the v2 encoding.
		raftAdmissionMeta.AdmissionPriority = int32(rac2.AdmissionToRaftPriority(
			admissionpb.WorkPriority(raftAdmissionMeta.AdmissionPriority)))
	}
	data, err := raftlog.EncodeCommand(ctx, p.command, p.idKey,
		raftlog.EncodeOptions{
			RaftAdmissionMeta: raftAdmissionMeta,
			EncodePriority:    encodePriority,
		})
	if err != nil {
		return kvpb.NewError(err)
	}
	p.encodedCommand = data

	// Too verbose even for verbose logging, so manually enable if you want to
	// debug proposal sizes.
	if false {
		log.Infof(p.Context(), `%s: proposal: %d
  RaftCommand.ReplicatedEvalResult:          %d
  RaftCommand.ReplicatedEvalResult.Delta:    %d
  RaftCommand.WriteBatch:                    %d
`, p.Request.Summary(), p.command.Size(),
			p.command.ReplicatedEvalResult.Size(),
			p.command.ReplicatedEvalResult.Delta.Size(),
			p.command.WriteBatch.Size(),
		)
	}

	// Log an event if this is a large proposal. These are more likely to cause
	// blips or worse, and it's good to be able to pick them from traces.
	//
	// TODO(tschottdorf): can we mark them so lightstep can group them?
	const largeProposalEventThresholdBytes = 2 << 19 // 512kb
	if ln := len(p.encodedCommand); ln > largeProposalEventThresholdBytes {
		log.Eventf(p.Context(), "proposal is large: %s", humanizeutil.IBytes(int64(ln)))
	}

	// Insert into the proposal buffer, which passes the command to Raft to be
	// proposed. The proposal buffer assigns the command a maximum lease index
	// when it sequences it.
	//
	// NB: we must not hold r.mu while using the proposal buffer, see comment
	// on the field.
	log.VEvent(p.Context(), 2, "submitting proposal to proposal buffer")
	if err := r.mu.proposalBuf.Insert(ctx, p, tok.Move(ctx)); err != nil {
		return kvpb.NewError(err)
	}
	r.store.metrics.RaftCommandsProposed.Inc(1)
	return nil
}

func checkReplicationChangeAllowed(
	command *kvserverpb.RaftCommand, desc *roachpb.RangeDescriptor, storeID roachpb.StoreID,
) error {
	// The following deals with removing a leaseholder. A voter can be removed
	// in two ways. 1) Simple (old style) where there is a reconfiguration
	// turning a voter into a LEARNER / NON-VOTER. 2) Through an intermediate
	// joint configuration, where the replica remains in the descriptor, but
	// as VOTER_{OUTGOING, DEMOTING}. When leaving the JOINT config (a second
	// Raft operation), the removed replica transitions a LEARNER / NON-VOTER.
	//
	// In case (1) the lease needs to be transferred out before a removal is
	// proposed (cooperative transfer). The code below permits leaseholder
	// removal only if entering a joint configuration (option 2 above) in which
	// the leaseholder is (any kind of) voter, and in addition, this joint config
	// should include a VOTER_INCOMING replica. In this case, the lease is
	// transferred to this new replica in maybeLeaveAtomicChangeReplicas right
	// before we exit the joint configuration.
	//
	// When the leaseholder is replaced by a new replica, transferring the
	// lease in the joint config allows transferring directly from old to new,
	// since both are active in the joint config, without going through a third
	// node or adding the new node before transferring, which might reduce
	// fault tolerance. For example, consider v1 in region1 (leaseholder), v2
	// in region2 and v3 in region3. We want to relocate v1 to a new node v4 in
	// region1. We add v4 as LEARNER. At this point we can't transfer the lease
	// to v4, so we could transfer it to v2 first, but this is likely to hurt
	// application performance. We could instead add v4 as VOTER first, and
	// then transfer lease directly to v4, but this would change the number of
	// replicas to 4, and if region1 goes down, we loose a quorum. Instead,
	// we move to a joint config where v1 (VOTER_DEMOTING_LEARNER) transfer the
	// lease to v4 (VOTER_INCOMING) directly.
	//
	// Our implementation assumes that the intention of the caller is for the
	// VOTER_INCOMING node to be the replacement replica, and hence get the
	// lease. We therefore don't dynamically select a lease target during the
	// joint config, and hand it to the VOTER_INCOMING node. This means,
	// however, that we only allow a VOTER_DEMOTING to have the lease in a
	// joint configuration, when there's also a VOTER_INCOMING node (that
	// will be used as a target for the lease transfer). Otherwise, the caller
	// is expected to shed the lease before entering a joint configuration.
	// See also https://github.com/cockroachdb/cockroach/issues/67740.
	lhDesc, lhDescOK := desc.GetReplicaDescriptor(storeID)
	if !lhDescOK {
		return kvpb.NewRangeNotFoundError(desc.RangeID, storeID)
	}
	proposedDesc := command.ReplicatedEvalResult.State.Desc
	// This is a reconfiguration command, we make sure the proposed
	// config is legal w.r.t. the current leaseholder: we now allow the
	// leaseholder to be a VOTER_DEMOTING as long as there is a VOTER_INCOMING.
	// Otherwise, the leaseholder must be a full voter in the target config.
	// This check won't allow exiting the joint config before the lease is
	// transferred away. The previous leaseholder is a LEARNER in the target config,
	// and therefore shouldn't continue holding the lease.
	if err := roachpb.CheckCanReceiveLease(
		lhDesc, proposedDesc.Replicas(), true, /* wasLastLeaseholder */
	); err != nil {
		err = errors.Handled(err)
		err = errors.Mark(err, errMarkInvalidReplicationChange)
		err = errors.Wrapf(err, "%v received invalid ChangeReplicasTrigger %s to "+
			"remove self (leaseholder); lhRemovalAllowed: %v; current desc: %v; proposed desc: %v",
			lhDesc, command.ReplicatedEvalResult.ChangeReplicas, true /* lhRemovalAllowed */, desc, proposedDesc)
		return err
	}

	// Check against direct voter removal. Voters must first be demoted to
	// learners before they can be removed for at least two reasons:
	// 1. the leader (or any voter) may be needed to vote for a candidate who
	//    has not yet applied the configuration change. This is a liveness issue
	//    if the leader/voter is immediately removed without stepping down to a
	//    learner first and waiting for a second configuration change to
	//    succeed.
	//    For details, see: https://github.com/cockroachdb/cockroach/pull/42251.
	// 2. the leader may have fortified its leadership term, binding the
	//    liveness of the leader replica to the leader's store's store liveness
	//    heartbeats. Removal of the leader replica from a store while that
	//    store continues to heartbeat in the store liveness fabric will lead to
	//    the leader disappearing without any other replica deciding that the
	//    leader is gone and stepping up to campaign.
	//
	// This same check exists in the pkg/raft library, but we disable it with
	// DisableConfChangeValidation.
	for _, repl := range desc.Replicas().Voters().Descriptors() {
		if _, ok := proposedDesc.Replicas().GetReplicaDescriptorByID(repl.ReplicaID); !ok {
			err := errors.Errorf("cannot remove voter %s directly; must first demote to learner", repl)
			err = errors.Mark(err, errMarkInvalidReplicationChange)
			return err
		}
	}

	return nil
}

func (r *Replica) numPendingProposalsRLocked() int64 {
	return int64(len(r.mu.proposals) + r.mu.proposalBuf.AllocatedIdx())
}

// hasPendingProposalsRLocked is part of the quiescer interface.
// It returns true if this node has any outstanding proposals. A client might be
// waiting for the outcome of these proposals, so we definitely don't want to
// quiesce while such proposals are in-flight.
//
// Note that this method says nothing about other node's outstanding proposals:
// if this node is the current leaseholders, previous leaseholders might have
// proposals on which they're waiting. If this node is not the current
// leaseholder, then obviously whoever is the current leaseholder might have
// pending proposals. This method is called in two places: on the current
// leaseholder when deciding whether the leaseholder should attempt to quiesce
// the range, and then on every follower to confirm that the range can indeed be
// quiesced.
func (r *Replica) hasPendingProposalsRLocked() bool {
	return r.numPendingProposalsRLocked() > 0 ||
		// If slow proposals just finished, it's possible that
		// refreshProposalsLocked hasn't been invoked yet. We don't want to quiesce
		// until it has been, since otherwise we're never fully resetting this
		// Replica's contribution to `requests.slow.raft`. So we only claim to
		// have no pending proposals when we've done one last refresh that resets
		// the counter, i.e. in a few ticks at most.
		r.mu.slowProposalCount > 0
}

// hasPendingProposalQuotaRLocked is part of the quiescer interface. It returns
// true if there are any commands that haven't completed replicating that are
// tracked by this node's quota pool (i.e. commands that haven't been acked by
// all live replicas).
// We can't quiesce while there's outstanding quota because the respective quota
// would not be released while quiesced, and it might prevent the range from
// unquiescing (leading to deadlock). See #46699.
func (r *Replica) hasPendingProposalQuotaRLocked() bool {
	if r.mu.proposalQuota == nil {
		return false
	}
	return !r.mu.proposalQuota.Full()
}

// hasSendTokensRaftMuLockedReplicaMuLocked is part of the quiescer interface.
// It returns true if RACv2 holds any send tokens for this range.
//
// We can't quiesce while any send tokens are held because this could lead to
// never releasing them. Tokens must be released.
func (r *Replica) hasSendTokensRaftMuLockedReplicaMuLocked() bool {
	return r.flowControlV2.HoldsSendTokensLocked()
}

// ticksSinceLastProposalRLocked returns the number of ticks since the last
// proposal.
func (r *Replica) ticksSinceLastProposalRLocked() int {
	return r.mu.ticks - r.mu.lastProposalAtTicks
}

// isRaftLeader returns true if this replica believes it is the current
// Raft leader.
//
// NB: This can race with Raft ready processing, where the Raft group has
// processed a leader change before updating the replica state in a separate
// critical section. The caller should always verify this against the Raft
// status where necessary.
func (r *Replica) isRaftLeaderRLocked() bool {
	// Defensively check replicaID != 0.
	return r.replicaID != 0 && r.replicaID == r.mu.leaderID
}

var errRemoved = errors.New("replica removed")

// stepRaftGroupRaftMuLocked calls Step on the replica's RawNode with the
// provided request's message. Before doing so, it assures that the replica is
// unquiesced and ready to handle the request.
func (r *Replica) stepRaftGroupRaftMuLocked(req *kvserverpb.RaftMessageRequest) error {
	r.raftMu.AssertHeld()
	var sideChannelInfo replica_rac2.SideChannelInfoUsingRaftMessageRequest
	var admittedVector rac2.AdmittedVector
	err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		// If this message requested tracing, begin tracing it.
		for _, e := range req.TracedEntries {
			r.mu.raftTracer.RegisterRemote(e)
		}
		r.mu.raftTracer.MaybeTrace(req.Message)
		// We're processing an incoming raft message (from a batch that may
		// include MsgVotes), so don't campaign if we wake up our raft
		// group.
		//
		// If we're a follower, and we receive a message from a non-leader replica
		// while quiesced, we wake up the leader too to prevent spurious elections.
		//
		// This typically happens in the case of a partial network partition where
		// some other replica is partitioned away from the leader but can reach this
		// replica. In that case, the partitioned replica will send us a prevote
		// message, which we'll typically reject (e.g. because it's behind on its
		// log, or if CheckQuorum+PreVote is enabled because we have a current
		// leader). However, if we don't also wake the leader, we'll now have two
		// unquiesced followers, and eventually they'll call an election that
		// unseats the leader. If this replica wins (often the case since we're
		// up-to-date on the log), then we'll immediately transfer leadership back
		// to the leaseholder, i.e. the old leader, and the cycle repeats.
		//
		// Note that such partial partitions will typically result in persistent
		// mass unquiescence due to the continuous prevotes.
		if r.mu.quiescent {
			st := r.raftBasicStatusRLocked()
			hasLeader := st.RaftState == raftpb.StateFollower && st.Lead != 0
			fromLeader := raftpb.PeerID(req.FromReplica.ReplicaID) == st.Lead
			wakeLeader := hasLeader && !fromLeader
			r.maybeUnquiesceLocked(wakeLeader, false /* mayCampaign */)
		}
		if r.store.TestingKnobs() == nil ||
			!r.store.TestingKnobs().DisableUpdateLastUpdateTimesMapOnRaftGroupStep {
			r.mu.lastUpdateTimes.update(req.FromReplica.ReplicaID, r.Clock().PhysicalTime())
		}
		switch req.Message.Type {
		case raftpb.MsgPreVote, raftpb.MsgVote:
			// If we receive a (pre)vote request, and we find our leader to be dead or
			// removed, forget it so we can grant the (pre)votes.
			r.maybeForgetLeaderOnVoteRequestLocked()
		case raftpb.MsgSnap:
			// Occasionally a snapshot message may arrive under an outdated term,
			// which would lead to Raft discarding the snapshot. This should be
			// really rare in practice, but it does happen in tests and in particular
			// can happen to the synchronous snapshots on the learner path, which
			// will then have to wait for the raft snapshot queue to send another
			// snapshot. However, in some tests it is desirable to disable the
			// raft snapshot queue. This workaround makes that possible.
			//
			// See TestReportUnreachableRemoveRace for the test that prompted
			// this addition.
			if term := raftGroup.BasicStatus().Term; term > req.Message.Term {
				req.Message.Term = term
			}
		case raftpb.MsgApp:
			if n := len(req.Message.Entries); n > 0 {
				sideChannelInfo = replica_rac2.SideChannelInfoUsingRaftMessageRequest{
					UsingV2Protocol: req.UsingRac2Protocol,
					LeaderTerm:      req.Message.Term,
					First:           req.Message.Entries[0].Index,
					Last:            req.Message.Entries[n-1].Index,
					LowPriOverride:  req.LowPriorityOverride,
				}
			}
		case raftpb.MsgAppResp:
			// If there is an admitted vector annotation, pass it to RACv2 to release
			// the flow control tokens.
			if term := req.AdmittedState.Term; term != 0 {
				admittedVector = rac2.AdmittedVector{Term: term}
				copy(admittedVector.Admitted[:], req.AdmittedState.Admitted)
			}
		}
		err := raftGroup.Step(req.Message)
		if errors.Is(err, raft.ErrProposalDropped) {
			// A proposal was forwarded to this replica but we couldn't propose it.
			// Swallow the error since we don't have an effective way of signaling
			// this to the sender.
			// TODO(bdarnell): Handle ErrProposalDropped better.
			// https://github.com/cockroachdb/cockroach/issues/21849
			err = nil
		}
		return false /* unquiesceAndWakeLeader */, err
	})
	if sideChannelInfo != (replica_rac2.SideChannelInfoUsingRaftMessageRequest{}) {
		r.flowControlV2.SideChannelForPriorityOverrideAtFollowerRaftMuLocked(sideChannelInfo)
	}
	if admittedVector.Term != 0 {
		r.flowControlV2.AdmitRaftMuLocked(context.TODO(), req.FromReplica.ReplicaID, admittedVector)
	}
	return err
}

type handleSnapshotStats struct {
	offered bool
	applied bool
}

type handleRaftReadyStats struct {
	tBegin, tEnd time.Time

	append logstore.AppendStats

	tApplicationBegin, tApplicationEnd time.Time
	apply                              applyCommittedEntriesStats

	tSnapBegin, tSnapEnd time.Time
	snap                 handleSnapshotStats
}

// SafeFormat implements redact.SafeFormatter
func (s handleRaftReadyStats) SafeFormat(p redact.SafePrinter, _ rune) {
	dTotal := s.tEnd.Sub(s.tBegin)
	dAppend := s.append.End.Sub(s.append.Begin)
	dApply := s.tApplicationEnd.Sub(s.tApplicationBegin)
	dPebble := s.append.PebbleEnd.Sub(s.append.PebbleBegin)
	dSnap := s.tSnapEnd.Sub(s.tSnapBegin)
	dUnaccounted := dTotal - dSnap - dAppend - dApply - dPebble

	{
		p.Printf("raft ready handling: %.2fs [append=%.2fs, apply=%.2fs, ",
			dTotal.Seconds(), dAppend.Seconds(), dApply.Seconds())
		if s.append.Sync {
			var sync redact.SafeString
			if s.append.NonBlocking {
				sync = "non-blocking-sync" // actual sync time not reflected in this case
			} else {
				sync = "sync"
			}
			p.Printf("%s=%.2fs", sync, dPebble.Seconds())
		}
	}
	if dSnap > 0 {
		p.Printf(", snap=%.2fs", dSnap.Seconds())
	}
	p.Printf(", other=%.2fs]", dUnaccounted.Seconds())

	p.Printf(", wrote [")
	if b := s.append.PebbleBytes; b > 0 {
		p.Printf("append-batch=%s, ", humanizeutil.IBytes(b))
	}
	if b, n := s.append.RegularBytes, s.append.RegularEntries; n > 0 || b > 0 {
		p.Printf("append-ent=%s (%d), ", humanizeutil.IBytes(b), n)
	}
	if b, n := s.append.SideloadedBytes, s.append.SideloadedEntries; n > 0 || b > 0 {
		p.Printf("append-sst=%s (%d), ", humanizeutil.IBytes(b), n)
	}
	if b, n := s.apply.numEntriesProcessedBytes, s.apply.numEntriesProcessed; n > 0 || b > 0 {
		p.Printf("apply=%s (%d", humanizeutil.IBytes(b), n)
		if c := s.apply.numBatchesProcessed; c > 1 {
			p.Printf(" in %d batches", c)
		}
		p.SafeString(")")
	}
	if n := s.apply.numAddSST; n > 0 {
		p.Printf(", apply-sst=%d", n)
		if c := s.apply.numAddSSTCopies; c > 0 {
			p.Printf(" (copies=%d)", c)
		}
	}
	p.SafeString("]")

	if n := s.apply.stateAssertions; n > 0 {
		p.Printf(", state_assertions=%d", n)
	}
	if s.snap.offered {
		if s.snap.applied {
			p.Printf(", snapshot applied")
		} else {
			p.Printf(", snapshot ignored")
		}
	}
	if !(s.append.PebbleCommitStats == storage.BatchCommitStats{}) {
		p.Printf(" pebble stats: [%s]", s.append.PebbleCommitStats)
	}
}

func (s handleRaftReadyStats) String() string {
	return redact.StringWithoutMarkers(s)
}

// noSnap can be passed to handleRaftReady when no snapshot should be processed.
var noSnap IncomingSnapshot

// handleRaftReady processes a raft.Ready containing entries and messages that
// are ready to read, be saved to stable storage, committed, or sent to other
// peers. It takes a non-empty IncomingSnapshot to indicate that it is
// about to process a snapshot.
//
// The returned string is nonzero whenever an error is returned to give a
// non-sensitive cue as to what happened.
func (r *Replica) handleRaftReady(
	ctx context.Context, inSnap IncomingSnapshot,
) (handleRaftReadyStats, error) {

	// Don't process anything if this fn returns false.
	if fn := r.store.cfg.TestingKnobs.DisableProcessRaft; fn != nil && fn(r.store.StoreID()) {
		return handleRaftReadyStats{
			tBegin: timeutil.Now(),
			tEnd:   timeutil.Now(),
		}, nil
	}

	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	return r.handleRaftReadyRaftMuLocked(ctx, inSnap)
}

func (r *Replica) attachRaftEntriesMonitorRaftMuLocked() {
	r.raftMu.bytesAccount = r.store.cfg.RaftEntriesMonitor.NewAccount(
		r.store.metrics.RaftLoadedEntriesBytes)
}

func (r *Replica) detachRaftEntriesMonitorRaftMuLocked() {
	// Return all the used bytes back to the limiter.
	r.raftMu.bytesAccount.Clear()
	// De-initialize the account so that log storage Entries() calls don't track
	// the entries anymore.
	r.raftMu.bytesAccount = logstore.BytesAccount{}
}

// handleRaftReadyRaftMuLocked is the same as handleRaftReady but requires that
// the replica's raftMu be held.
//
// The returned string is nonzero whenever an error is returned to give a
// non-sensitive cue as to what happened.
func (r *Replica) handleRaftReadyRaftMuLocked(
	ctx context.Context, inSnap IncomingSnapshot,
) (stats handleRaftReadyStats, _ error) {
	// handleRaftReadyRaftMuLocked is not prepared to handle context cancellation,
	// so assert that it's given a non-cancellable context.
	if ctx.Done() != nil {
		return handleRaftReadyStats{}, errors.AssertionFailedf(
			"handleRaftReadyRaftMuLocked cannot be called with a cancellable context")
	}
	// Before doing anything, including calling Ready(), see if we need to
	// ratchet up the flow control level. This code will go away when RACv1 =>
	// RACv2 transition is complete and RACv1 code is removed.
	if r.raftMu.flowControlLevel < kvflowcontrol.V2EnabledWhenLeaderV2Encoding {
		// Not already at highest level.
		level := kvflowcontrol.GetV2EnabledWhenLeaderLevel(
			ctx, r.store.ClusterSettings(), r.store.TestingKnobs().FlowControlTestingKnobs)
		if level > r.raftMu.flowControlLevel {
			var basicState replica_rac2.RaftNodeBasicState
			func() {
				r.mu.Lock()
				defer r.mu.Unlock()
				basicState = replica_rac2.MakeRaftNodeBasicStateLocked(
					r.mu.internalRaftGroup, r.shMu.state.Lease.Replica.ReplicaID)
				if r.raftMu.flowControlLevel == kvflowcontrol.V2NotEnabledWhenLeader {
					// This will close all connected streams and consequently all
					// requests waiting on v1 kvflowcontrol.ReplicationAdmissionHandles
					// will return.
					r.mu.replicaFlowControlIntegration.onDestroyed(ctx)
					// Replace with a noop integration since want no code to execute on
					// various calls.
					r.mu.replicaFlowControlIntegration = noopReplicaFlowControlIntegration{}
				}
			}()
			r.raftMu.flowControlLevel = level
			r.flowControlV2.SetEnabledWhenLeaderRaftMuLocked(ctx, level, basicState)
		}
	}

	// NB: we need to reference the named return parameter here. If `stats` were
	// just a local, we'd be modifying the local but not the return value in the
	// defer below.
	stats = handleRaftReadyStats{
		tBegin: timeutil.Now(),
	}
	defer func() {
		stats.tEnd = timeutil.Now()
	}()

	if inSnap.Desc != nil {
		stats.snap.offered = true
	}

	var hasReady bool
	var outboundMsgs []raftpb.Message
	var msgStorageAppend, msgStorageApply raftpb.Message
	rac2ModeToUse := r.replicationAdmissionControlModeToUse(ctx)
	// Replication AC v2 state that is initialized while holding Replica.mu.
	replicaStateInfoMap := r.raftMu.replicaStateScratchForFlowControl
	var raftNodeBasicState replica_rac2.RaftNodeBasicState
	var logSnapshot raft.LogSnapshot
	r.mu.Lock()
	rac2ModeForReady := r.mu.currentRACv2Mode
	state := logstore.RaftState{ // used for append below
		LastIndex: r.shMu.lastIndexNotDurable,
		LastTerm:  r.shMu.lastTermNotDurable,
		ByteSize:  r.shMu.raftLogSize,
	}
	leaderID := r.mu.leaderID
	lastLeaderID := leaderID
	err := r.withRaftGroupLocked(func(raftGroup *raft.RawNode) (bool, error) {
		r.deliverLocalRaftMsgsRaftMuLockedReplicaMuLocked(ctx, raftGroup)

		numFlushed, err := r.mu.proposalBuf.FlushLockedWithRaftGroup(ctx, raftGroup)
		if err != nil {
			return false, err
		}
		switchToPullModeAfterReady := false
		if rac2ModeToUse != rac2ModeForReady {
			if rac2ModeToUse == rac2.MsgAppPush {
				raftGroup.SetLazyReplication(false)
				rac2ModeForReady = rac2.MsgAppPush
			} else {
				// There are some MsgApps buffered in RawNode. Pull those out in this
				// Ready, and switch to pull mode after that.
				switchToPullModeAfterReady = true
			}
			r.mu.currentRACv2Mode = rac2ModeToUse
		}
		if hasReady = raftGroup.HasReady(); hasReady {
			// Since we are holding raftMu, only this Ready() call will use
			// raftMu.bytesAccount. It tracks memory usage that this Ready incurs.
			r.attachRaftEntriesMonitorRaftMuLocked()
			// TODO(pav-kv): currently, Ready() only accounts for entry bytes loaded
			// from log storage, and ignores the in-memory unstable entries. Pass a
			// flow control struct down the stack, and do a more complete accounting
			// in raft. This will also eliminate the "side channel" plumbing hack with
			// this bytesAccount.
			syncRd := raftGroup.Ready()
			// We apply committed entries during this handleRaftReady, so it is ok to
			// release the corresponding memory tokens at the end of this func. Next
			// time we enter this function, the account will be empty again.
			defer r.detachRaftEntriesMonitorRaftMuLocked()

			logRaftReady(ctx, syncRd)
			asyncRd := makeAsyncReady(syncRd)
			outboundMsgs, msgStorageAppend, msgStorageApply = splitLocalStorageMsgs(asyncRd.Messages)
		}
		if switchToPullModeAfterReady {
			raftGroup.SetLazyReplication(true)
		}
		if rac2ModeForReady == rac2.MsgAppPull {
			logSnapshot = raftGroup.LogSnapshot()
		}
		raftNodeBasicState = replica_rac2.MakeRaftNodeBasicStateLocked(
			raftGroup, r.shMu.state.Lease.Replica.ReplicaID)
		replica_rac2.MakeReplicaStateInfos(raftGroup, replicaStateInfoMap)
		// We unquiesce if we have a Ready (= there's work to do). We also have
		// to unquiesce if we just flushed some proposals but there isn't a
		// Ready, which can happen if the proposals got dropped (raft does this
		// if it doesn't know who the leader is). And, for extra defense in depth,
		// we also unquiesce if there are outstanding proposals.
		//
		// NB: if we had the invariant that the group can only be in quiesced
		// state if it knows the leader (state.Lead) AND we knew that raft would
		// never give us an empty ready here (i.e. the only reason to drop a
		// proposal is not knowing the leader) then numFlushed would not be
		// necessary. The latter is likely true but we don't want to rely on
		// it. The former is maybe true, but there's no easy way to enforce it.
		unquiesceAndWakeLeader := hasReady || numFlushed > 0 || len(r.mu.proposals) > 0
		return unquiesceAndWakeLeader, nil
	})
	r.mu.applyingEntries = hasMsg(msgStorageApply)
	pausedFollowers := r.mu.pausedFollowers
	r.mu.Unlock()
	if errors.Is(err, errRemoved) {
		// If we've been removed then just return.
		return stats, nil
	} else if err != nil {
		return stats, errors.Wrap(err, "checking raft group for Ready")
	}
	// Even if we don't have a Ready, or entries in Ready,
	// replica_rac2.Processor may need to do some work.
	raftEvent := rac2.RaftEventFromMsgStorageAppendAndMsgApps(
		rac2ModeForReady, r.ReplicaID(), msgStorageAppend, outboundMsgs, logSnapshot,
		r.raftMu.msgAppScratchForFlowControl, replicaStateInfoMap)
	r.flowControlV2.HandleRaftReadyRaftMuLocked(ctx, raftNodeBasicState, raftEvent)
	if !hasReady {
		// We must update the proposal quota even if we don't have a ready.
		// Consider the case when our quota is of size 1 and two out of three
		// replicas have committed one log entry while the third is lagging
		// behind. When the third replica finally does catch up and sends
		// along a MsgAppResp, since the entry is already committed on the
		// leader replica, no Ready is emitted. But given that the third
		// replica has caught up, we can release
		// some quota back to the pool.
		r.updateProposalQuotaRaftMuLocked(ctx, lastLeaderID)
		return stats, nil
	}

	r.traceMessageSends(outboundMsgs, "sending messages")
	r.sendRaftMessages(ctx, outboundMsgs, pausedFollowers, true /* willDeliverLocal */)

	// If the ready struct includes entries that have been committed, these
	// entries will be applied to the Replica's replicated state machine down
	// below, after appending new entries to the raft log and sending messages
	// to peers. However, the process of appending new entries to the raft log
	// and then applying committed entries to the state machine can take some
	// time - and these entries are already durably committed. If they have
	// clients waiting on them, we'd like to acknowledge their success as soon
	// as possible. To facilitate this, we take a quick pass over the committed
	// entries and acknowledge as many as we can trivially prove will not be
	// rejected beneath raft.
	//
	// Note that the Entries slice in the MsgStorageApply cannot refer to entries
	// that are also in the Entries slice in the MsgStorageAppend. Raft will not
	// allow unstable entries to be applied when AsyncStorageWrites is enabled.
	//
	// If we disable AsyncStorageWrites in the future, this property will no
	// longer be true, and the two slices could overlap. For example, this can
	// happen when a follower is being caught up on committed commands. We could
	// acknowledge these commands early even though they aren't durably in the
	// local raft log yet (since they're committed via a quorum elsewhere), but
	// we'd likely want to revert to an earlier version of this code that chose to
	// be conservative and avoid this behavior by passing the last Ready cycle's
	// `lastIndex` for a maxIndex argument to
	// AckCommittedEntriesBeforeApplication.
	//
	// TODO(nvanbenschoten): this is less important with async storage writes.
	// Consider getting rid of it.
	sm := r.getStateMachine()
	dec := r.getDecoder()
	var appTask apply.Task
	if hasMsg(msgStorageApply) {
		appTask = apply.MakeTask(sm, dec)
		appTask.SetMaxBatchSize(r.store.TestingKnobs().MaxApplicationBatchSize)
		defer appTask.Close()
		if err := appTask.Decode(ctx, msgStorageApply.Entries); err != nil {
			return stats, err
		}
		if knobs := r.store.TestingKnobs(); knobs == nil || !knobs.DisableCanAckBeforeApplication {
			if err := appTask.AckCommittedEntriesBeforeApplication(ctx); err != nil {
				return stats, err
			}
		}
	}

	// Grab the known leaseholder before applying to the state machine.
	startingLeaseholderID := r.shMu.state.Lease.Replica.ReplicaID
	refreshReason := noReason
	if hasMsg(msgStorageAppend) {
		app := logstore.MakeMsgStorageAppend(msgStorageAppend)
		cb := (*replicaSyncCallback)(r)

		// Leadership changes, if any, are communicated through MsgStorageAppends.
		// Check if that's the case here.
		if app.Lead != raft.None && leaderID != roachpb.ReplicaID(app.Lead) {
			// Refresh pending commands if the Raft leader has changed. This is
			// usually the first indication we have of a new leader on a restarted
			// node.
			//
			// TODO(peter): Re-proposing commands when SoftState.Lead changes can lead
			// to wasteful multiple-reproposals when we later see an empty Raft command
			// indicating a newly elected leader or a conf change. Replay protection
			// prevents any corruption, so the waste is only a performance issue.
			if log.V(3) {
				log.Infof(ctx, "raft leader changed: %d -> %d", leaderID, app.Lead)
			}
			if !r.store.TestingKnobs().DisableRefreshReasonNewLeader {
				refreshReason = reasonNewLeader
			}
			leaderID = roachpb.ReplicaID(app.Lead)
		}

		if app.Snapshot != nil {
			if inSnap.Desc == nil {
				// If we didn't expect Raft to have a snapshot but it has one
				// regardless, that is unexpected and indicates a programming
				// error.
				return stats, errors.AssertionFailedf(
					"have inSnap=nil, but raft has a snapshot %s",
					raft.DescribeSnapshot(*app.Snapshot),
				)
			}

			snapUUID, err := uuid.FromBytes(app.Snapshot.Data)
			if err != nil {
				return stats, errors.Wrap(err, "invalid snapshot id")
			}
			if inSnap.SnapUUID == (uuid.UUID{}) {
				log.Fatalf(ctx, "programming error: a snapshot application was attempted outside of the streaming snapshot codepath")
			}
			if snapUUID != inSnap.SnapUUID {
				log.Fatalf(ctx, "incoming snapshot id doesn't match raft snapshot id: %s != %s", snapUUID, inSnap.SnapUUID)
			}

			snap := *app.Snapshot
			if len(app.Entries) != 0 {
				log.Fatalf(ctx, "found Entries in MsgStorageAppend with non-empty Snapshot")
			}

			// Applying this snapshot may require us to subsume one or more of our right
			// neighbors. This occurs if this replica is informed about the merges via a
			// Raft snapshot instead of a MsgApp containing the merge commits, e.g.,
			// because it went offline before the merge commits applied and did not come
			// back online until after the merge commits were truncated away.
			subsumedRepls, releaseMergeLock := r.maybeAcquireSnapshotMergeLock(ctx, inSnap)
			defer releaseMergeLock()

			stats.tSnapBegin = timeutil.Now()
			if err := r.applySnapshot(ctx, inSnap, snap, app.HardState(), subsumedRepls); err != nil {
				return stats, errors.Wrap(err, "while applying snapshot")
			}
			for _, msg := range app.Responses {
				// The caller would like to see the MsgAppResp that usually results from
				// applying the snapshot synchronously, so fish it out.
				if msg.To == raftpb.PeerID(inSnap.FromReplica.ReplicaID) &&
					msg.Type == raftpb.MsgAppResp &&
					!msg.Reject &&
					msg.Index == snap.Metadata.Index {

					inSnap.msgAppRespCh <- msg
					break
				}
			}
			stats.tSnapEnd = timeutil.Now()
			stats.snap.applied = true

			// lastIndexNotDurable, lastTermNotDurable and raftLogSize were updated in
			// applySnapshot, but we also want to make sure we reflect these changes
			// in the local variables we're tracking here.
			state = logstore.RaftState{
				LastIndex: r.shMu.lastIndexNotDurable,
				LastTerm:  r.shMu.lastTermNotDurable,
				ByteSize:  r.shMu.raftLogSize,
			}

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

			cb.OnSnapSync(ctx, app.OnDone())
		} else {
			// TODO(pavelkalinnikov): find a way to move it to storeEntries.
			if app.Commit != 0 && !r.IsInitialized() {
				log.Fatalf(ctx, "setting non-zero HardState.Commit on uninitialized replica %s", r)
			}
			// TODO(pavelkalinnikov): construct and store this in Replica.
			// TODO(pavelkalinnikov): fields like raftEntryCache are the same across all
			// ranges, so can be passed to LogStore methods instead of being stored in it.
			s := logstore.LogStore{
				RangeID:     r.RangeID,
				Engine:      r.store.TODOEngine(),
				Sideload:    r.raftMu.sideloaded,
				StateLoader: r.raftMu.stateLoader.StateLoader,
				// NOTE: we use the same SyncWaiter callback loop for all raft log
				// writes performed by a given range. This ensures that callbacks are
				// processed in order.
				SyncWaiter: r.store.syncWaiters[int(r.RangeID)%len(r.store.syncWaiters)],
				EntryCache: r.store.raftEntryCache,
				Settings:   r.store.cfg.Settings,
				Metrics: logstore.Metrics{
					RaftLogCommitLatency: r.store.metrics.RaftLogCommitLatency,
				},
				DisableSyncLogWriteToss: buildutil.CrdbTestBuild &&
					r.store.TestingKnobs().DisableSyncLogWriteToss,
			}
			// TODO(pav-kv): make this branch unconditional.
			if r.IsInitialized() && r.store.cfg.KVAdmissionController != nil {
				// Enqueue raft log entries into admission queues. This is
				// non-blocking; actual admission happens asynchronously.
				isUsingV2OrDestroyed := r.flowControlV2.AdmitRaftEntriesRaftMuLocked(ctx, raftEvent)
				if !isUsingV2OrDestroyed {
					// Leader is using RACv1 protocol.
					tenantID, _ := r.TenantID()
					for _, entry := range raftEvent.Entries {
						if len(entry.Data) == 0 {
							continue // nothing to do
						}
						r.store.cfg.KVAdmissionController.AdmitRaftEntry(
							ctx, tenantID, r.StoreID(), r.RangeID, r.replicaID, raftEvent.Term, entry,
						)
					}
				}
			}

			r.mu.raftTracer.MaybeTrace(msgStorageAppend)
			if state, err = s.StoreEntries(ctx, state, app, cb, &stats.append); err != nil {
				return stats, errors.Wrap(err, "while storing log entries")
			}
		}
	}

	// Update protected state - last index, last term, raft log size, and raft
	// leader ID.
	r.mu.Lock()
	// TODO(pavelkalinnikov): put logstore.RaftState to r.mu directly.
	r.shMu.lastIndexNotDurable = state.LastIndex
	r.shMu.lastTermNotDurable = state.LastTerm
	r.shMu.raftLogSize = state.ByteSize
	var becameLeader bool
	if r.mu.leaderID != leaderID {
		r.mu.leaderID = leaderID
		// Clear the remote proposal set. Would have been nil already if not
		// previously the leader.
		becameLeader = r.mu.leaderID == r.replicaID
	}
	r.mu.Unlock()

	// When becoming the leader, proactively add the replica to the replicate
	// queue. We might have been handed leadership by a remote node which wanted
	// to remove itself from the range.
	if becameLeader && r.store.replicateQueue != nil {
		r.store.replicateQueue.MaybeAddAsync(ctx, r, r.store.Clock().NowAsClockTimestamp())
	}

	stats.tApplicationBegin = timeutil.Now()
	if hasMsg(msgStorageApply) {
		r.mu.raftTracer.MaybeTrace(msgStorageApply)
		r.traceEntries(msgStorageApply.Entries, "committed, before applying any entries")

		err := appTask.ApplyCommittedEntries(ctx)
		stats.apply = sm.moveStats()
		if err != nil {
			// NB: this branch will be hit when the replica has been removed,
			// in which case errors.Is(err, apply.ErrRemoved). Our callers
			// special-case this.
			//
			// No future Ready objects will be processed by this Replica since
			// it is now marked as destroyed.
			return stats, err
		}

		if r.store.cfg.KVAdmissionController != nil &&
			stats.apply.followerStoreWriteBytes.NumEntries > 0 {
			r.store.cfg.KVAdmissionController.FollowerStoreWriteBytes(
				r.store.StoreID(), stats.apply.followerStoreWriteBytes)
		}

		// etcd raft occasionally adds a nil entry (our own commands are never
		// empty). This happens in two situations: When a new leader is elected, and
		// when a config change is dropped due to the "one at a time" rule. In both
		// cases we may need to resubmit our pending proposals (In the former case
		// we resubmit everything because we proposed them to a former leader that
		// is no longer able to commit them. In the latter case we only need to
		// resubmit pending config changes, but it's hard to distinguish so we
		// resubmit everything anyway). We delay resubmission until after we have
		// processed the entire batch of entries.
		if stats.apply.numEmptyEntries > 0 {
			// Overwrite unconditionally since this is the most aggressive
			// reproposal mode.
			if !r.store.TestingKnobs().DisableRefreshReasonNewLeaderOrConfigChange {
				refreshReason = reasonNewLeaderOrConfigChange
			}
		}

		// Send MsgStorageApply's responses.
		r.sendRaftMessages(ctx, msgStorageApply.Responses, nil /* blocked */, true /* willDeliverLocal */)
	}
	stats.tApplicationEnd = timeutil.Now()
	applicationElapsed := stats.tApplicationEnd.Sub(stats.tApplicationBegin).Nanoseconds()
	r.store.metrics.RaftApplyCommittedLatency.RecordValue(applicationElapsed)
	r.store.metrics.RaftCommandsApplied.Inc(int64(len(msgStorageApply.Entries)))
	if r.store.TestingKnobs().EnableUnconditionalRefreshesInRaftReady {
		refreshReason = reasonNewLeaderOrConfigChange
	}
	if refreshReason != noReason {
		r.mu.Lock()
		r.refreshProposalsLocked(ctx, 0 /* refreshAtDelta */, refreshReason)
		r.mu.Unlock()
	}

	// NB: if we just processed a command which removed this replica from the
	// raft group we will early return before this point. This, combined with
	// the fact that we'll refuse to process messages intended for a higher
	// replica ID ensures that our replica ID could not have changed.

	r.mu.Lock()
	err = r.withRaftGroupLocked(func(raftGroup *raft.RawNode) (bool, error) {
		r.deliverLocalRaftMsgsRaftMuLockedReplicaMuLocked(ctx, raftGroup)

		if stats.apply.numConfChangeEntries > 0 {
			// If the raft leader got removed, campaign on the leaseholder. Uses
			// forceCampaignLocked() to bypass PreVote+CheckQuorum, since we otherwise
			// wouldn't get prevotes from other followers who recently heard from the
			// old leader and haven't applied the conf change. We know the leader
			// isn't around anymore anyway.
			leaseStatus := r.leaseStatusAtRLocked(ctx, r.store.Clock().NowAsClockTimestamp())
			raftStatus := raftGroup.BasicStatus()
			if shouldCampaignAfterConfChange(ctx, r.store.ClusterSettings(), r.store.StoreID(),
				r.descRLocked(), raftStatus, leaseStatus) {
				r.forceCampaignLocked(ctx)
			}
		}

		// If the Raft group still has more to process then we immediately
		// re-enqueue it for another round of processing. This is possible if
		// the group's committed entries were paginated due to size limitations
		// and we didn't apply all of them in this pass.
		if raftGroup.HasReady() {
			r.store.enqueueRaftUpdateCheck(r.RangeID)
		}
		return true, nil
	})
	r.mu.applyingEntries = false
	r.mu.Unlock()
	if err != nil {
		return stats, errors.Wrap(err, "during advance")
	}

	if leaseholderID := r.shMu.state.Lease.Replica.ReplicaID; leaderID == r.replicaID &&
		leaseholderID != startingLeaseholderID &&
		leaseholderID != r.replicaID {
		// Leader is this replica and leaseholder changed and is some other replica.
		// RACv2 needs to know promptly about this in case it needs to force-flush the
		// send-queue for the new leaseholder.
		r.store.scheduler.EnqueueRaftReady(r.RangeID)
	}

	// NB: All early returns other than the one due to not having a ready
	// which also makes the below call are due to fatal errors.
	// We must also update the proposal quota when have a ready; consider the
	// case where there are two replicas and we have a quota of size 1. We
	// acquire the quota when the write gets proposed on the leader and expect it
	// to be released when the follower commits it locally. In order to do so we
	// need to have the entry 'come out of raft' and in the case of a two node
	// raft group, this only happens if hasReady == true. If we don't release
	// quota back at the end of handleRaftReadyRaftMuLocked, the next write will
	// get blocked.
	r.updateProposalQuotaRaftMuLocked(ctx, lastLeaderID)
	return stats, nil
}

// asyncReady encapsulates the messages that are ready to be sent to other peers
// or to be sent to local storage routines when async storage writes are enabled.
// All fields in asyncReady are read-only.
// TODO(nvanbenschoten): move this into go.etcd.io/raft.
type asyncReady struct {
	// Messages specifies outbound messages to other peers and to local storage
	// threads. These messages can be sent in any order.
	//
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []raftpb.Message
}

// makeAsyncReady constructs an asyncReady from the provided Ready.
func makeAsyncReady(rd raft.Ready) asyncReady {
	return asyncReady{
		Messages: rd.Messages,
	}
}

// hasMsg returns whether the provided raftpb.Message is present.
// It serves as a poor man's Optional[raftpb.Message].
func hasMsg(m raftpb.Message) bool { return m.Type != 0 }

// splitLocalStorageMsgs filters out local storage messages from the provided
// message slice and returns them separately.
func splitLocalStorageMsgs(
	msgs []raftpb.Message,
) (otherMsgs []raftpb.Message, msgStorageAppend, msgStorageApply raftpb.Message) {
	for i := len(msgs) - 1; i >= 0; i-- {
		switch msgs[i].Type {
		case raftpb.MsgStorageAppend:
			if hasMsg(msgStorageAppend) {
				panic("two MsgStorageAppend")
			}
			msgStorageAppend = msgs[i]
		case raftpb.MsgStorageApply:
			if hasMsg(msgStorageApply) {
				panic("two MsgStorageApply")
			}
			msgStorageApply = msgs[i]
		default:
			// Local storage messages will always be at the end of the messages slice,
			// so we can terminate iteration as soon as we reach any other message
			// type. This is leaking an implementation detail from etcd/raft which may
			// not always hold, but while it does, we use it for convenience and
			// assert against it changing in sendRaftMessages.
			return msgs[:i+1], msgStorageAppend, msgStorageApply
		}
	}
	// Only local storage messages.
	return nil, msgStorageAppend, msgStorageApply
}

// maybeFatalOnRaftReadyErr will fatal if err is neither nil nor
// apply.ErrRemoved.
func maybeFatalOnRaftReadyErr(ctx context.Context, err error) (removed bool) {
	switch {
	case err == nil:
		return false
	case errors.Is(err, apply.ErrRemoved):
		return true
	default:
		log.FatalfDepth(ctx, 1, "%+v", err)
		panic("unreachable")
	}
}

// tick the Raft group, returning true if the raft group exists and should
// be queued for Ready processing; false otherwise.
func (r *Replica) tick(
	ctx context.Context, livenessMap livenesspb.IsLiveMap, ioThresholdMap *ioThresholdMap,
) (exists bool, err error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	defer func() {
		if exists && err == nil {
			// NB: since we are returning true, there will be a Ready handling
			// immediately after this call, so any pings stashed in raft will be sent.
			// NB: Replica.mu must not be held here.
			r.flowControlV2.MaybeSendPingsRaftMuLocked()
		}
	}()

	r.mu.Lock()
	defer r.mu.Unlock()

	// If the replica has been destroyed or is quiesced, don't tick it.
	if r.mu.internalRaftGroup == nil {
		return false, nil
	}
	if r.mu.quiescent {
		return false, nil
	}

	r.unreachablesMu.Lock()
	remotes := r.unreachablesMu.remotes
	r.unreachablesMu.remotes = nil
	r.unreachablesMu.Unlock()
	bypassFn := r.store.TestingKnobs().RaftReportUnreachableBypass
	for remoteReplica := range remotes {
		if bypassFn != nil && bypassFn(remoteReplica) {
			continue
		}
		r.mu.internalRaftGroup.ReportUnreachable(raftpb.PeerID(remoteReplica))
	}

	r.updatePausedFollowersLocked(ctx, ioThresholdMap)

	storeClockTimestamp := r.store.Clock().NowAsClockTimestamp()
	leaseStatus := r.leaseStatusAtRLocked(ctx, storeClockTimestamp)
	// TODO(pav-kv): modify the quiescence criterion so that we don't quiesce if
	// RACv2 holds some send tokens.
	if r.maybeQuiesceRaftMuLockedReplicaMuLocked(ctx, leaseStatus, livenessMap) {
		return false, nil
	}

	r.maybeTransferRaftLeadershipToLeaseholderLocked(ctx, leaseStatus)

	// Eagerly acquire or extend leases. This only works for unquiesced ranges. We
	// never quiesce expiration leases, but for epoch leases we fall back to the
	// replicate queue which will do this within 10 minutes.
	if !r.store.cfg.TestingKnobs.DisableAutomaticLeaseRenewal {
		if shouldRequest, isExtension := r.shouldRequestLeaseRLocked(leaseStatus); shouldRequest {
			if _, requestPending := r.mu.pendingLeaseRequest.RequestPending(); !requestPending {
				var limiter *quotapool.IntPool
				if !isExtension { // don't limit lease extensions
					limiter = r.store.eagerLeaseAcquisitionLimiter
				}
				// Check quota first to avoid wasted work.
				if limiter == nil || limiter.ApproximateQuota() > 0 {
					_ = r.requestLeaseLocked(ctx, leaseStatus, limiter)
				}
			}
		}
	}

	// For followers, we update lastUpdateTimes when we step a message from them
	// into the local Raft group. The leader won't hit that path, so we update
	// it whenever it ticks. In effect, this makes sure it always sees itself as
	// alive.
	//
	// Note that in a workload where the leader doesn't have inflight requests
	// "most of the time" (i.e. occasional writes only on this range), it's quite
	// likely that we'll never reach this line, since we'll return in the
	// maybeQuiesceRaftMuLockedReplicaMuLocked branch above.
	//
	// This is likely unintentional, and the leader should likely consider itself
	// live even when quiesced.
	if r.isRaftLeaderRLocked() {
		r.mu.lastUpdateTimes.update(r.replicaID, r.Clock().PhysicalTime())
		// We also update lastUpdateTimes for replicas that provide store liveness
		// support to the leader.
		r.updateLastUpdateTimesUsingStoreLivenessRLocked(storeClockTimestamp)
	}

	r.mu.ticks++
	preTickState := r.mu.internalRaftGroup.BasicStatus().RaftState
	r.mu.internalRaftGroup.Tick()
	postTickState := r.mu.internalRaftGroup.BasicStatus().RaftState
	if preTickState != postTickState {
		if postTickState == raftpb.StatePreCandidate {
			r.store.Metrics().RaftTimeoutCampaign.Inc(1)
			if k := r.store.TestingKnobs(); k != nil && k.OnRaftTimeoutCampaign != nil {
				k.OnRaftTimeoutCampaign(r.RangeID)
			}
		}
	}

	refreshAtDelta := r.store.cfg.RaftReproposalTimeoutTicks
	if knob := r.store.TestingKnobs().RefreshReasonTicksPeriod; knob > 0 {
		refreshAtDelta = knob
	}
	if !r.store.TestingKnobs().DisableRefreshReasonTicks && r.mu.ticks%refreshAtDelta == 0 {
		// The combination of the above condition and passing refreshAtDelta to
		// refreshProposalsLocked means that commands will be refreshed when they
		// have been pending for 1 to 2 reproposal timeouts.
		r.refreshProposalsLocked(ctx, refreshAtDelta, reasonTicks)
	}
	return true, nil
}

func (r *Replica) processRACv2PiggybackedAdmitted(ctx context.Context) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.flowControlV2.ProcessPiggybackedAdmittedAtLeaderRaftMuLocked(ctx)
}

func (r *Replica) processRACv2RangeController(ctx context.Context) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	// Can read Replica.mu.currentRACv2Mode since updates require both raftMu
	// and Replica.mu.
	mode := r.mu.currentRACv2Mode
	var logSnapshot raft.LogSnapshot
	if mode == rac2.MsgAppPull {
		err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
			logSnapshot = raftGroup.LogSnapshot()
			return false, nil
		})
		if err != nil {
			// The only error here is errRemoved, so ignore.
			return
		}
	}
	r.flowControlV2.ProcessSchedulerEventRaftMuLocked(
		ctx, r.mu.currentRACv2Mode, logSnapshot)
}

// SendMsgApp implements rac2.MsgAppSender.
func (r *Replica) SendMsgApp(ctx context.Context, msg raftpb.Message, lowPriorityOverride bool) {
	r.sendRaftMessage(ctx, msg, lowPriorityOverride)
}

func (r *Replica) hasRaftReadyRLocked() bool {
	return r.mu.internalRaftGroup.HasReady()
}

// slowReplicationThreshold returns the threshold after which in-flight
// replicated commands should be considered "stuck" and should trip the
// per-Replica circuit breaker. The boolean indicates whether this
// mechanism is enabled; if it isn't no action should be taken.
func (r *Replica) slowReplicationThreshold(ba *kvpb.BatchRequest) (time.Duration, bool) {
	if knobs := r.store.TestingKnobs(); knobs != nil && knobs.SlowReplicationThresholdOverride != nil {
		if dur := knobs.SlowReplicationThresholdOverride(ba); dur > 0 {
			return dur, true
		}
		// Fall through.
	}
	dur := replicaCircuitBreakerSlowReplicationThreshold.Get(&r.store.cfg.Settings.SV)
	return dur, dur > 0
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
	reasonTicks
)

// refreshProposalsLocked goes through the pending proposals, notifying
// proposers whose proposals need to be retried, and resubmitting proposals
// which were likely dropped (but may still apply at a legal Lease index) -
// ensuring that the proposer will eventually get a reply on the channel it's
// waiting on.
// mu must be held.
//
// Note that reproposals don't need to worry about checking the closed timestamp
// before reproposing, since they're reusing the original LAI.
//
// refreshAtDelta only applies for reasonTicks and specifies how old (in ticks)
// a command must be for it to be inspected; the usual value is the number of
// ticks of an election timeout (affect only proposals that have had ample time
// to apply but didn't).
func (r *Replica) refreshProposalsLocked(
	ctx context.Context, refreshAtDelta int, reason refreshRaftReason,
) {
	if refreshAtDelta != 0 && reason != reasonTicks {
		log.Fatalf(ctx, "refreshAtDelta specified for reason %s != reasonTicks", reason)
	}

	var maxSlowProposalDurationRequest *kvpb.BatchRequest
	// TODO(tbg): don't track exempt requests for tripping the breaker?
	var maxSlowProposalDuration time.Duration
	var slowProposalCount int64
	var reproposals pendingCmdSlice
	for _, p := range r.mu.proposals {
		slowReplicationThreshold, ok := r.slowReplicationThreshold(p.Request)
		// NB: ticks can be delayed, in which this detection would kick in too late
		// as well. This is unlikely to become a concern since the configured
		// durations here should be very large compared to the refresh interval, and
		// so delays shouldn't dramatically change the detection latency.
		inflightDuration := r.store.cfg.RaftTickInterval * time.Duration(r.mu.ticks-p.createdAtTicks)
		if ok && inflightDuration > slowReplicationThreshold {
			slowProposalCount++
			if maxSlowProposalDuration < inflightDuration {
				maxSlowProposalDuration = inflightDuration
				maxSlowProposalDurationRequest = p.Request
			}
		} else if inflightDuration > defaultReplicaCircuitBreakerSlowReplicationThreshold {
			// If replica circuit breakers are disabled, we still want to
			// track the number of "slow" proposals for metrics, so fall
			// back to one minute.
			slowProposalCount++
		}
		switch reason {
		case reasonSnapshotApplied:
			// If we applied a snapshot, check the MaxLeaseIndexes of all
			// pending commands to see if any are now prevented from
			// applying, and if so make them return an ambiguous error. We
			// can't tell at this point (which should be rare) whether they
			// were included in the snapshot we received or not.
			//
			// NB: lease proposals have MaxLeaseIndex 0, so they are cleaned
			// up here too.
			if p.command.MaxLeaseIndex <= r.shMu.state.LeaseAppliedIndex {
				r.cleanupFailedProposalLocked(p)
				log.Eventf(p.Context(), "retry proposal %x: %s", p.idKey, reason)
				p.finishApplication(ctx, makeProposalResultErr(
					kvpb.NewAmbiguousResultErrorf(
						"unable to determine whether command was applied via snapshot",
					)))
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

	r.mu.slowProposalCount = slowProposalCount
	destroyed := r.mu.destroyStatus.Removed()

	// If the breaker isn't tripped yet but we've detected commands that have
	// taken too long to replicate, and the replica is not destroyed, trip the
	// breaker now.
	//
	// NB: we still keep reproposing commands on this and subsequent ticks
	// even though this seems strictly counter-productive, except perhaps
	// for the probe's proposals. We could consider being more strict here
	// which could avoid build-up of raft log entries during outages, see
	// for example:
	// https://github.com/cockroachdb/cockroach/issues/60612
	//
	// NB: the call to Err() here also re-triggers the probe if the breaker is
	// already tripped and no probe is running, thus ensuring that even if a
	// request got added in while the probe was about to shut down, there will
	// be regular attempts at healing the breaker.
	if maxSlowProposalDuration > 0 && r.breaker.Signal().Err() == nil && !destroyed {
		err := errors.Errorf("have been waiting %.2fs for slow proposal %s",
			maxSlowProposalDuration.Seconds(), maxSlowProposalDurationRequest)
		log.Warningf(ctx, "%s", err)
		// NB: this is async because we're holding lots of locks here, and we want
		// to avoid having to pass all the information about the replica into the
		// breaker (since the breaker needs access to this information at will to
		// power the probe anyway). Over time, we anticipate there being multiple
		// mechanisms which trip the breaker.
		r.breaker.TripAsync(err)
	}

	if len(reproposals) == 0 {
		return
	}

	log.VInfof(ctx, 2,
		"pending commands: reproposing %d (at applied index %d, lease applied index %d) %s",
		len(reproposals), r.shMu.state.RaftAppliedIndex,
		r.shMu.state.LeaseAppliedIndex, reason)

	// Reproposals are those commands which we weren't able to send back to the
	// client (since we're not sure that another copy of them could apply at
	// the "correct" index). For reproposals, it's generally pretty unlikely
	// that they can make it in the right place. Reproposing in order is
	// definitely required, however.
	sort.Sort(reproposals)
	for _, p := range reproposals {
		log.Eventf(p.Context(), "re-submitting command %x (MLI %d, CT %s): %s",
			p.idKey, p.command.MaxLeaseIndex, p.command.ClosedTimestamp, reason)
		if err := r.mu.proposalBuf.ReinsertLocked(ctx, p); err != nil {
			r.cleanupFailedProposalLocked(p)
			p.finishApplication(ctx, makeProposalResultErr(
				kvpb.NewAmbiguousResultError(err)))
			continue
		}
		r.store.metrics.RaftCommandsReproposed.Inc(1)
	}
}

func (r *Replica) poisonInflightLatches(err error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.mu.proposals {
		p.ec.poison()
		// TODO(tbg): find out how `p.ec.done()` can have been called at this point,
		// See: https://github.com/cockroachdb/cockroach/issues/86547
		if p.ec.g != nil && p.ec.g.Req.PoisonPolicy == poison.Policy_Error {
			aErr := kvpb.NewAmbiguousResultError(err)
			// NB: this does not release the request's latches. It's important that
			// the latches stay in place, since the command could still apply.
			p.signalProposalResult(makeProposalResultErr(aErr))
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
	lagging laggingReplicaSet,
) bool {
	var hbMap map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat
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
	beat := kvserverpb.RaftHeartbeat{
		RangeID:                           r.RangeID,
		ToReplicaID:                       toReplica.ReplicaID,
		FromReplicaID:                     fromReplica.ReplicaID,
		Term:                              kvpb.RaftTerm(msg.Term),
		Commit:                            kvpb.RaftIndex(msg.Commit),
		Quiesce:                           quiesce,
		LaggingFollowersOnQuiesce:         lagging,
		LaggingFollowersOnQuiesceAccurate: quiesce,
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

// replicaSyncCallback implements the logstore.SyncCallback interface.
type replicaSyncCallback Replica

func (r *replicaSyncCallback) OnLogSync(
	ctx context.Context, done logstore.MsgStorageAppendDone, commitStats storage.BatchCommitStats,
) {
	repl := (*Replica)(r)
	// The log mark is non-empty only if this was a non-empty log append that
	// updated the stable log mark.
	if mark := done.Mark(); mark.Term != 0 {
		repl.flowControlV2.SyncedLogStorage(ctx, mark)
	}
	// Block sending the responses back to raft, if a test needs to.
	if fn := repl.store.TestingKnobs().TestingAfterRaftLogSync; fn != nil {
		fn(repl.ID())
	}
	// Send MsgStorageAppend's responses.
	repl.sendRaftMessages(ctx, done.Responses(), nil /* blocked */, false /* willDeliverLocal */)
	if commitStats.TotalDuration > defaultReplicaRaftMuWarnThreshold {
		log.Infof(repl.raftCtx, "slow non-blocking raft commit: %s", commitStats)
	}
}

func (r *replicaSyncCallback) OnSnapSync(ctx context.Context, done logstore.MsgStorageAppendDone) {
	repl := (*Replica)(r)
	// NB: when storing snapshot, done always contains a non-zero log mark.
	repl.flowControlV2.SyncedLogStorage(ctx, done.Mark())
	repl.sendRaftMessages(ctx, done.Responses(), nil /* blocked */, true /* willDeliverLocal */)
}

// sendRaftMessages sends a slice of Raft messages.
//
// blocked is a set of replicas to which replication traffic is currently
// paused. Messages directed at a replica in this set will be dropped.
//
// willDeliverLocal, if true, indicates that the caller will ensure that any
// local messages get delivered to the local raft state machine. This flag is
// used to avoid unnecessary calls into the raft scheduler.
//
// When calling this method, the raftMu may be held, but it does not need to be.
// The Replica mu must not be held.
func (r *Replica) sendRaftMessages(
	ctx context.Context,
	messages []raftpb.Message,
	blocked map[roachpb.ReplicaID]struct{},
	willDeliverLocal bool,
) {
	var lastAppResp raftpb.Message
	for _, message := range messages {
		switch message.To {
		case raft.LocalAppendThread:
			// To local append thread.
			// NOTE: we don't currently split append work off into an async goroutine.
			// Instead, we handle messages to LocalAppendThread inline on the raft
			// scheduler goroutine, so this code path is unused.
			panic("unsupported, currently processed inline on raft scheduler goroutine")
		case raft.LocalApplyThread:
			// To local apply thread.
			// NOTE: we don't currently split apply work off into an async goroutine.
			// Instead, we handle messages to LocalAppendThread inline on the raft
			// scheduler goroutine, so this code path is unused.
			panic("unsupported, currently processed inline on raft scheduler goroutine")
		case raftpb.PeerID(r.ReplicaID()):
			// To local raft state machine, from local storage append and apply work.
			// NOTE: For async Raft log appends, these messages come from calls to
			// replicaSyncCallback.OnLogSync. For other local storage work (log
			// application and snapshot application), these messages come from
			// Replica.handleRaftReadyRaftMuLocked.
			r.sendLocalRaftMsg(message, willDeliverLocal)
		default:
			_, drop := blocked[roachpb.ReplicaID(message.To)]
			if drop {
				r.store.Metrics().RaftPausedFollowerDroppedMsgs.Inc(1)
			}
			switch message.Type {
			case raftpb.MsgApp:
				if util.RaceEnabled {
					// Iterate over the entries to assert that all sideloaded commands
					// are already inlined. replicaRaftStorage.Entries already performs
					// the sideload inlining for stable entries and raft.unstable always
					// contain fat entries. Since these are the only two sources that
					// raft.sendAppend gathers entries from to populate MsgApps, we
					// should never see thin entries here.
					//
					// Also assert that the log term only ever increases (most of the
					// time it stays constant, as term changes are rare), and that
					// the index increases by exactly one with each entry.
					//
					// This assertion came out of #61990.
					prevTerm := message.LogTerm // term of entry preceding the append
					prevIndex := message.Index  // index of entry preceding the append
					for j := range message.Entries {
						ent := &message.Entries[j]
						logstore.AssertSideloadedRaftCommandInlined(ctx, ent)

						if prevIndex+1 != ent.Index {
							log.Fatalf(ctx,
								"index gap in outgoing MsgApp: idx %d followed by %d",
								prevIndex, ent.Index,
							)
						}
						prevIndex = ent.Index
						if prevTerm > ent.Term {
							log.Fatalf(ctx,
								"term regression in outgoing MsgApp: idx %d at term=%d "+
									"appended with logterm=%d",
								ent.Index, ent.Term, message.LogTerm,
							)
						}
						prevTerm = ent.Term
					}
				}

			case raftpb.MsgAppResp:
				// A successful (non-reject) MsgAppResp contains two pieces of
				// information: the highest log index and the commit index. Raft
				// currently queues up one MsgAppResp per incoming MsgApp, and we may
				// process multiple messages in one handleRaftReady call (because
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
				r.sendRaftMessage(ctx, message, false)
			}
		}
	}
	if lastAppResp.Index > 0 {
		r.sendRaftMessage(ctx, lastAppResp, false)
	}
}

// sendLocalRaftMsg sends a message to the local raft state machine.
func (r *Replica) sendLocalRaftMsg(msg raftpb.Message, willDeliverLocal bool) {
	if msg.To != raftpb.PeerID(r.ReplicaID()) {
		panic("incorrect message target")
	}
	r.localMsgs.Lock()
	wasEmpty := len(r.localMsgs.active) == 0
	r.localMsgs.active = append(r.localMsgs.active, msg)
	r.localMsgs.Unlock()
	// If this is the first local message and the caller will not deliver local
	// messages itself, schedule a Raft update check to inform Raft processing
	// about the new local message. Everyone else can rely on the call that added
	// the first message having already scheduled a Raft update check.
	if wasEmpty && !willDeliverLocal {
		r.store.enqueueRaftUpdateCheck(r.RangeID)
	}
}

// deliverLocalRaftMsgsRaftMuLockedReplicaMuLocked delivers local messages to
// the provided raw node.
func (r *Replica) deliverLocalRaftMsgsRaftMuLockedReplicaMuLocked(
	ctx context.Context, raftGroup *raft.RawNode,
) {
	r.raftMu.AssertHeld()
	r.mu.AssertHeld()
	r.localMsgs.Lock()
	localMsgs := r.localMsgs.active
	r.localMsgs.active, r.localMsgs.recycled = r.localMsgs.recycled, r.localMsgs.active[:0]
	// Don't recycle large slices.
	if cap(r.localMsgs.recycled) > 16 {
		r.localMsgs.recycled = nil
	}
	r.localMsgs.Unlock()

	// If we are in a test build, shuffle the local messages before delivering
	// them. These are not required to be in order, so ensure that re-ordering is
	// handled properly.
	if buildutil.CrdbTestBuild {
		rand.Shuffle(len(localMsgs), func(i, j int) {
			localMsgs[i], localMsgs[j] = localMsgs[j], localMsgs[i]
		})
	}

	for i, m := range localMsgs {
		r.mu.raftTracer.MaybeTrace(m)
		if err := raftGroup.Step(m); err != nil {
			log.Fatalf(ctx, "unexpected error stepping local raft message [%s]: %v",
				raft.DescribeMessage(m, raftEntryFormatter), err)
		}
		// NB: we can reset messages in the localMsgs.recycled slice without holding
		// the localMsgs mutex because no-one ever writes to localMsgs.recycled and
		// we are holding raftMu, which must be held to switch localMsgs.active and
		// localMsgs.recycled.
		localMsgs[i].Reset() // for GC
	}
}

// sendRaftMessage sends a Raft message.
//
// When calling this method, the raftMu may be held, but it does not need to be.
// lowPriorityOverride may be set for a MsgApp. The Replica mu must not be
// held.
func (r *Replica) sendRaftMessage(
	ctx context.Context, msg raftpb.Message, lowPriorityOverride bool,
) {
	lastToReplica, lastFromReplica := r.getLastReplicaDescriptors()

	r.mu.RLock()
	traced := r.mu.raftTracer.MaybeTrace(msg)
	fromReplica, fromErr := r.getReplicaDescriptorByIDRLocked(roachpb.ReplicaID(msg.From), lastToReplica)
	toReplica, toErr := r.getReplicaDescriptorByIDRLocked(roachpb.ReplicaID(msg.To), lastFromReplica)
	var startKey roachpb.RKey
	if msg.Type == raftpb.MsgApp {
		// When the follower is potentially an uninitialized replica waiting for
		// a split trigger, send the replica's StartKey along. See the method
		// below for more context:
		_ = maybeDropMsgApp
		// NB: this code is allocation free.
		r.mu.internalRaftGroup.WithBasicProgress(func(id raftpb.PeerID, pr tracker.BasicProgress) {
			if id == msg.To && pr.State == tracker.StateProbe {
				// It is moderately expensive to attach a full key to the message, but note that
				// a probing follower will only be appended to once per heartbeat interval (i.e.
				// on the order of seconds). See:
				//
				// https://github.com/etcd-io/etcd/blob/7f450bf6967638673dd88fd4e730b01d1303d5ff/raft/progress.go#L41
				startKey = r.descRLocked().StartKey
			}
		})
	}
	r.mu.RUnlock()

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

	if r.maybeCoalesceHeartbeat(ctx, msg, toReplica, fromReplica, false, nil) {
		return
	}

	req := newRaftMessageRequest()
	*req = kvserverpb.RaftMessageRequest{
		RangeID:             r.RangeID,
		ToReplica:           toReplica,
		FromReplica:         fromReplica,
		Message:             msg,
		RangeStartKey:       startKey, // usually nil
		UsingRac2Protocol:   r.flowControlV2.GetEnabledWhenLeader() >= kvflowcontrol.V2EnabledWhenLeaderV1Encoding,
		LowPriorityOverride: lowPriorityOverride,
		TracedEntries:       traced,
	}
	// For RACv2, annotate successful MsgAppResp messages with the vector of
	// admitted log indices, by priority.
	if msg.Type == raftpb.MsgAppResp && !msg.Reject {
		admitted := r.flowControlV2.AdmittedState()
		// If admitted.Term is lagging msg.Term, sending the admitted vector has no
		// effect on the leader, so skip it.
		// If msg.Term is lagging the admitted.Term, this is a message to a stale
		// leader. Sending it allows that leader to release all tokens. It would
		// otherwise do so soon anyway, upon learning about the new leader.
		if admitted.Term >= msg.Term {
			req.AdmittedState = kvflowcontrolpb.AdmittedState{
				Term:     admitted.Term,
				Admitted: admitted.Admitted[:],
			}
		}
	}
	if !r.sendRaftMessageRequest(ctx, req) {
		r.mu.Lock()
		r.mu.droppedMessages++
		r.mu.Unlock()
		r.addUnreachableRemoteReplica(toReplica.ReplicaID)
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
func (r *Replica) sendRaftMessageRequest(
	ctx context.Context, req *kvserverpb.RaftMessageRequest,
) bool {
	if log.V(4) {
		log.Infof(ctx, "sending raft request %+v", req)
	}
	return r.store.cfg.Transport.SendAsync(req, r.connectionClass.get())
}

func (r *Replica) reportSnapshotStatus(ctx context.Context, to roachpb.ReplicaID, snapErr error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()

	snapStatus := raft.SnapshotFinish
	if snapErr != nil {
		snapStatus = raft.SnapshotFailure
	}

	// NB: we are technically violating raft's contract around which index the
	// snapshot is supposed to be at. Raft asked for a particular applied index,
	// but the snapshot we sent might have been at a higher (most of the time) or
	// lower (corner cases) index too. Luckily this is not an issue as the call
	// below will only inform at which index the follower is next probed (after
	// ReportSnapshot with a success). Raft does not a priori assume that the
	// index it requested is now actually durable on the follower. Note also that
	// the follower will generate an MsgAppResp reflecting the applied snapshot
	// which typically moves the follower to StateReplicate when (if) received
	// by the leader, which as of #106793 we do synchronously.
	if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		raftGroup.ReportSnapshot(raftpb.PeerID(to), snapStatus)
		return true, nil
	}); err != nil && !errors.Is(err, errRemoved) {
		log.Fatalf(ctx, "%v", err)
	}
}

type snapTruncationInfo struct {
	index          kvpb.RaftIndex
	recipientStore roachpb.StoreID
	initial        bool
}

// addSnapshotLogTruncation creates a log truncation record which will prevent
// the raft log from being truncated past this point until the cleanup function
// is called. This function will return the index that the truncation constraint
// is set at and a cleanup function to remove the constraint. We will fetch the
// applied index again in GetSnapshot and that is likely a different but higher
// index. The appliedIndex fetched here is narrowly used for adding a log
// truncation constraint to prevent log entries > appliedIndex from being
// removed. Note that the appliedIndex maintained in Replica actually lags the
// one in the engine, since replicaAppBatch.ApplyToStateMachine commits the
// engine batch and then acquires mu to update the RaftAppliedIndex. The use of
// a possibly stale value here is harmless since the values increases
// monotonically. The actual snapshot index, may preserve more from a log
// truncation perspective.
// If initial is true, the snapshot is marked as being sent by the replicate
// queue to a new replica; some callers only care about these snapshots.
func (r *Replica) addSnapshotLogTruncationConstraint(
	ctx context.Context, snapUUID uuid.UUID, initial bool, recipientStore roachpb.StoreID,
) (kvpb.RaftIndex, func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	appliedIndex := r.shMu.state.RaftAppliedIndex
	// Cleared when OutgoingSnapshot closes.
	if r.mu.snapshotLogTruncationConstraints == nil {
		r.mu.snapshotLogTruncationConstraints = make(map[uuid.UUID]snapTruncationInfo)
	}
	item, ok := r.mu.snapshotLogTruncationConstraints[snapUUID]
	if ok {
		// Uh-oh, there's either a programming error (resulting in the same snapshot
		// fed into this method twice) or a UUID collision. We discard the update
		// (which is benign) but log it loudly. If the index is the same, it's
		// likely the former, otherwise the latter.
		log.Warningf(ctx, "UUID collision at %s for %+v (index %d)", snapUUID, item, appliedIndex)
		return appliedIndex, func() {}
	}

	r.mu.snapshotLogTruncationConstraints[snapUUID] = snapTruncationInfo{
		index:          appliedIndex,
		recipientStore: recipientStore,
		initial:        initial,
	}

	return appliedIndex, func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		_, ok := r.mu.snapshotLogTruncationConstraints[snapUUID]
		if !ok {
			// UUID collision while adding the snapshot in originally. Nothing
			// else to do.
			return
		}
		delete(r.mu.snapshotLogTruncationConstraints, snapUUID)
		if len(r.mu.snapshotLogTruncationConstraints) == 0 {
			// Save a little bit of memory.
			r.mu.snapshotLogTruncationConstraints = nil
		}
	}
}

// getSnapshotLogTruncationConstraintsRLocked returns the minimum index of any
// currently outstanding snapshot being sent from this replica to the specified
// recipient or 0 if there isn't one. Passing 0 for recipientStore means any
// recipient. If initialOnly is set, only snapshots sent by the replicate queue
// to new replicas are considered.
func (r *Replica) getSnapshotLogTruncationConstraintsRLocked(
	recipientStore roachpb.StoreID, initialOnly bool,
) (_ []snapTruncationInfo, minSnapIndex kvpb.RaftIndex) {
	var sl []snapTruncationInfo
	for _, item := range r.mu.snapshotLogTruncationConstraints {
		if initialOnly && !item.initial {
			continue
		}
		if recipientStore != 0 && item.recipientStore != recipientStore {
			continue
		}
		if minSnapIndex == 0 || minSnapIndex > item.index {
			minSnapIndex = item.index
		}
		sl = append(sl, item)
	}
	return sl, minSnapIndex
}

// errOnOutstandingLearnerSnapshotInflight returns an error if there is a
// snapshot in progress from this replica to a learner replica for this range.
func (r *Replica) errOnOutstandingLearnerSnapshotInflight() error {
	learners := r.Desc().Replicas().LearnerDescriptors()
	for _, repl := range learners {
		sl, _ := r.hasOutstandingSnapshotInFlightToStore(repl.StoreID, true /* initialOnly */)
		if len(sl) > 0 {
			return errors.Errorf("INITIAL snapshots in flight to s%d: %v", repl.StoreID, sl)
		}
	}
	return nil
}

// hasOutstandingSnapshotInFlightToStore returns true if there is a snapshot in
// flight from this replica to the store with the given ID. If initialOnly is
// true, only snapshots sent by the replicate queue to new replicas are considered.
func (r *Replica) hasOutstandingSnapshotInFlightToStore(
	storeID roachpb.StoreID, initialOnly bool,
) ([]snapTruncationInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sl, idx := r.getSnapshotLogTruncationConstraintsRLocked(storeID, initialOnly)
	return sl, idx > 0
}

// HasRaftLeader returns true if the raft group has a raft leader currently.
func HasRaftLeader(raftStatus *raft.Status) bool {
	return raftStatus != nil && raftStatus.HardState.Lead != 0
}

// pendingCmdSlice sorts by increasing MaxLeaseIndex.
type pendingCmdSlice []*ProposalData

func (s pendingCmdSlice) Len() int      { return len(s) }
func (s pendingCmdSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s pendingCmdSlice) Less(i, j int) bool {
	return s[i].command.MaxLeaseIndex < s[j].command.MaxLeaseIndex
}

// withRaftGroupLocked calls the supplied function with the Raft group. The
// supplied function should return true for the unquiesceAndWakeLeader argument
// if the replica should be unquiesced (and the leader awoken). See
// handleRaftReady for an instance of where this value varies.
//
// Requires that Replica.mu is held.
//
// If this Replica is in the process of being removed this method will return
// errRemoved.
func (r *Replica) withRaftGroupLocked(
	f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
	if r.mu.destroyStatus.Removed() {
		// Callers know to detect errRemoved as non-fatal.
		return errRemoved
	}

	// INVARIANT: !r.mu.destroyStatus.Removed() → internalRaftGroup != nil

	unquiesce, err := f(r.mu.internalRaftGroup)
	if r.mu.internalRaftGroup.BasicStatus().Lead == 0 {
		// If we don't know the leader, unquiesce unconditionally. As a
		// follower, we can't wake up the leader if we don't know who that is,
		// so we should find out now before someone needs us to unquiesce.
		//
		// This situation should occur rarely or never (ever since we got
		// stricter about validating incoming Quiesce requests) but it's good
		// defense-in-depth.
		//
		// Note that maybeUnquiesceLocked won't manage to wake up the leader since
		// it's unknown to this replica, and at the time of writing the heuristics
		// for campaigning are defensive (won't campaign if there is a live
		// leaseholder). But if we are trying to unquiesce because this follower was
		// asked to propose something, then this means that a request is going to
		// have to wait until the leader next contacts us, or, in the worst case, an
		// election timeout. This is not ideal - if a node holds a live lease, we
		// should direct the client to it immediately.
		unquiesce = true
	}
	if unquiesce {
		r.maybeUnquiesceLocked(true /* wakeLeader */, true /* mayCampaign */)
	}
	return err
}

// withRaftGroup calls the supplied function with the replica's Raft group. It
// acquires and releases the Replica lock, so r.mu must not be held (or acquired
// by the supplied function).
//
// If this Replica is in the process of being removed this method will return
// errRemoved.
func (r *Replica) withRaftGroup(
	f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.withRaftGroupLocked(f)
}

func shouldCampaignOnWake(
	leaseStatus kvserverpb.LeaseStatus,
	storeID roachpb.StoreID,
	raftStatus raft.BasicStatus,
	livenessMap livenesspb.IsLiveMap,
	desc *roachpb.RangeDescriptor,
	requiresExpirationLease bool,
	now hlc.Timestamp,
) bool {
	// When waking up a range, campaign unless we know that another
	// node holds a valid lease (this is most important after a split,
	// when all replicas create their raft groups at about the same
	// time, with a lease pre-assigned to one of them). Note that
	// thanks to PreVote, unnecessary campaigns are not disruptive so
	// we should err on the side of campaigining here.
	if leaseStatus.IsValid() && !leaseStatus.OwnedBy(storeID) {
		return false
	}
	// If we're already campaigning don't start a new term.
	if raftStatus.RaftState != raftpb.StateFollower {
		return false
	}
	// If we don't know who the leader is, then campaign.
	if raftStatus.Lead == raft.None {
		return true
	}
	// Avoid a circular dependency on liveness and skip the is leader alive check
	// for ranges that require expiration based leases (the meta and liveness
	// ranges). We do want to check the liveness entry for other expiration
	// leases, since in the case of a dead leader it allows us to campaign
	// immediately without waiting out the election timeout.
	if requiresExpirationLease {
		return false
	}
	// Determine if we think the leader is alive, if we don't have the leader
	// in the descriptor we assume it is, since it could be an indication that this
	// replica is behind.
	replDesc, ok := desc.GetReplicaDescriptorByID(roachpb.ReplicaID(raftStatus.Lead))
	if !ok {
		return false
	}
	// If we don't know about the leader in our liveness map, then we err on the side
	// of caution and dont campaign.
	livenessEntry, ok := livenessMap[replDesc.NodeID]
	if !ok {
		return false
	}
	// NB: we intentionally do not look at the IsLiveMapEntry.IsLive field, which
	// accounts for whether the leader is reachable from this node (see
	// Store.updateLivenessMap). We only care whether the leader is currently live
	// according to node liveness because this determines whether it will be able
	// to hold an epoch-based lease.
	return !livenessEntry.Liveness.IsLive(now)
}

// maybeCampaignOnWakeLocked is called when the replica wakes from a quiesced
// state and campaigns for raft leadership if appropriate: if it has no leader,
// or it finds a dead leader in liveness.
//
// This will use PreVote+CheckQuorum, so it won't disturb an established leader
// if one currently exists. However, if other replicas wake up to find a dead
// leader in liveness, they will forget it and grant us a prevote, allowing us
// to win an election immediately if a quorum considers the leader dead.
//
// This may result in a tie if multiple replicas wake simultaneously. However,
// if other replicas wake in response to our (pre)vote request, they won't
// campaign to avoid the tie.
func (r *Replica) maybeCampaignOnWakeLocked(ctx context.Context) {
	// Raft panics if a node that is not currently a member of the
	// group tries to campaign. This method should never be called
	// otherwise and in fact the Replica should never be in such a
	// state but better not take any chances. For example, if this
	// method were to be called on an uninitialized replica (which
	// has no state and thus an empty raft config), this might cause
	// problems.
	if _, currentMember := r.shMu.state.Desc.GetReplicaDescriptorByID(r.replicaID); !currentMember {
		return
	}

	now := r.store.Clock().NowAsClockTimestamp()
	leaseStatus := r.leaseStatusAtRLocked(ctx, now)
	raftStatus := r.mu.internalRaftGroup.BasicStatus()
	livenessMap, _ := r.store.livenessMap.Load().(livenesspb.IsLiveMap)
	if shouldCampaignOnWake(leaseStatus, r.store.StoreID(), raftStatus, livenessMap, r.descRLocked(),
		r.requiresExpirationLeaseRLocked(), now.ToTimestamp()) {
		r.campaignLocked(ctx)
	}
}

// maybeForgetLeaderOnVoteRequestLocked is called when receiving a (Pre)Vote
// request. If the current leader is not live (according to liveness) or not in
// our range descriptor, we forget it and become a leaderless follower.
//
// Normally, with PreVote+CheckQuorum, we won't grant a (pre)vote if we've heard
// from a leader in the past election timeout interval. However, sometimes we
// want to call an election despite a recent leader. Forgetting the leader
// allows us to grant the (pre)vote, and if a quorum of replicas agree that the
// leader is dead they can win an election. Used specifically when:
//
//   - Unquiescing to a dead leader. The first replica to wake will campaign,
//     the others won't to avoid ties but should grant the vote. See
//     maybeUnquiesceLocked().
//
//   - Stealing leadership from a leader who can't heartbeat liveness.
//     Otherwise, noone will be able to acquire an epoch lease, and the range is
//     unavailable. See shouldCampaignOnLeaseRequestRedirect().
//
//   - For backwards compatibility in mixed 23.1/23.2 clusters, where 23.2 first
//     enabled CheckQuorum. The above two cases hold with 23.1. Additionally, when
//     the leader is removed from the range in 23.1, the first replica in the
//     range will campaign using (pre)vote, so 23.2 nodes must grant it.
//
// TODO(erikgrinaker): The above cases are only relevant with epoch leases and
// 23.1 compatibility. Consider removing this when no longer needed.
func (r *Replica) maybeForgetLeaderOnVoteRequestLocked() {
	raftStatus := r.mu.internalRaftGroup.BasicStatus()
	livenessMap, _ := r.store.livenessMap.Load().(livenesspb.IsLiveMap)
	now := r.store.Clock().Now()
	if shouldForgetLeaderOnVoteRequest(raftStatus, livenessMap, r.descRLocked(), now) {
		r.forgetLeaderLocked(r.AnnotateCtx(context.TODO()))
	}
}

func shouldForgetLeaderOnVoteRequest(
	raftStatus raft.BasicStatus,
	livenessMap livenesspb.IsLiveMap,
	desc *roachpb.RangeDescriptor,
	now hlc.Timestamp,
) bool {
	// If we're not a follower with a leader, there's noone to forget.
	if raftStatus.RaftState != raftpb.StateFollower || raftStatus.Lead == raft.None {
		return false
	}

	// If the leader isn't in our descriptor, assume it was removed and
	// forget about it.
	replDesc, ok := desc.GetReplicaDescriptorByID(roachpb.ReplicaID(raftStatus.Lead))
	if !ok {
		return true
	}

	// If we don't know about the leader's liveness, assume it's dead and forget it.
	livenessEntry, ok := livenessMap[replDesc.NodeID]
	if !ok {
		return true
	}

	// Forget the leader if it's no longer live.
	//
	// NB: we intentionally do not look at the IsLiveMapEntry.IsLive field, which
	// accounts for whether the leader is reachable from this node (see
	// Store.updateLivenessMap). We only care whether the leader is currently live
	// according to node liveness because this determines whether it will be able
	// to hold an epoch-based lease.
	return !livenessEntry.Liveness.IsLive(now)
}

// shouldCampaignOnLeaseRequestRedirect returns whether a replica that is
// redirecting a lease request to its range's Raft leader should simultaneously
// campaign to acquire Raft leadership itself. A follower replica may want to
// campaign in such a case if it determines that the Raft leader is non-live
// according to node liveness despite being able to retain Raft leadership
// within the range. In such cases, the Raft leader will be unable to acquire an
// epoch-based lease until it heartbeats its liveness record, so it would be
// beneficial for one of the followers to step forward as leader/leaseholder.
//
// In these cases, campaigning for Raft leadership is safer than blindly
// allowing the lease request to be proposed (through a redirected proposal).
// This is because the follower may be arbitrarily far behind on its Raft log
// and acquiring the lease in such cases could cause unavailability. By instead
// calling a Raft pre-vote election, the follower can determine whether it is
// behind on its log without risking disruption. If not, it will eventually
// become leader and can proceed with a future attempt to acquire the lease.
func shouldCampaignOnLeaseRequestRedirect(
	raftStatus raft.BasicStatus,
	livenessMap livenesspb.IsLiveMap,
	desc *roachpb.RangeDescriptor,
	leaseType roachpb.LeaseType,
	now hlc.Timestamp,
) bool {
	// If we're already campaigning don't start a new term.
	if raftStatus.RaftState != raftpb.StateFollower {
		return false
	}
	// If we don't know who the leader is, then campaign.
	// NOTE: at the time of writing, we only reject lease requests and call this
	// function when the leader is known, so this check is not needed. However, if
	// that ever changes, and we do decide to reject a lease request when the
	// leader is not known, we should immediately campaign.
	if raftStatus.Lead == raft.None {
		return true
	}
	// If we don't want to use an epoch-based lease then we don't need to campaign
	// based on liveness state because there can never be a case where a node can
	// retain Raft leadership but still be unable to acquire the lease. This is
	// possible on ranges that use epoch-based leases because the Raft leader may
	// be partitioned from the liveness range.
	// See TestRequestsOnFollowerWithNonLiveLeaseholder for an example of a test
	// that demonstrates this case.
	if leaseType != roachpb.LeaseEpoch {
		return false
	}
	// Determine if we think the leader is alive, if we don't have the leader in
	// the descriptor we assume it is, since it could be an indication that this
	// replica is behind.
	replDesc, ok := desc.GetReplicaDescriptorByID(roachpb.ReplicaID(raftStatus.Lead))
	if !ok {
		return false
	}
	// If we don't know about the leader in our liveness map, then we err on the
	// side of caution and don't campaign.
	livenessEntry, ok := livenessMap[replDesc.NodeID]
	if !ok {
		return false
	}
	// Otherwise, we check if the leader is live according to node liveness and
	// campaign if it is not.
	// NOTE: we intentionally do not look at the IsLiveMapEntry.IsLive field,
	// which accounts for whether the leader is reachable from this node (see
	// Store.updateLivenessMap). We only care whether the leader is currently live
	// according to node liveness because this determines whether it will be able
	// to acquire an epoch-based lease.
	return !livenessEntry.Liveness.IsLive(now)
}

// campaignLocked campaigns for raft leadership, using PreVote and, if
// CheckQuorum is enabled, the recent leader condition. That is, followers will
// not grant (pre)votes if we're behind on the log and, with CheckQuorum, if
// they've heard from a leader in the past election timeout interval.
// Additionally, the local replica will not even begin to campaign if the recent
// leader condition does not allow it to (i.e. this method will be a no-op).
//
// The "recent leader condition" is based on raft heartbeats for ranges that are
// not using the leader fortification protocol. Followers will not vote against
// a leader if they have recently received a heartbeat (or other message) from
// it. For ranges that are using the leader fortification protocol, the "recent
// leader condition" is based on whether a follower is supporting a fortified
// leader. Followers will not campaign or vote against a leader who's fortified
// store liveness epoch they currently support.
//
// The CheckQuorum condition can delay elections, particularly with quiesced
// ranges that don't tick. However, it is necessary to avoid spurious elections
// and stolen leaderships during partial/asymmetric network partitions, which
// can lead to permanent unavailability if the leaseholder can no longer reach
// the leader. For ranges using the leader fortification protocol, it is also
// necessary to implement irrevocable leader support upon which leader leases
// are built.
//
// Only followers enforce the CheckQuorum recent leader condition though, so if
// a quorum of followers consider the leader dead and choose to become
// pre-candidates and campaign then they will grant prevotes and can hold an
// election without waiting out the election timeout. This can result in
// election ties, so it's often better for followers to choose to forget their
// current leader via forgetLeaderLocked(), allowing them to immediately grant
// (pre)votes without campaigning themselves. Followers and pre-candidates will
// also grant any number of pre-votes, both for themselves and anyone else
// that's eligible.
func (r *Replica) campaignLocked(ctx context.Context) {
	log.VEventf(ctx, 3, "campaigning")
	if err := r.mu.internalRaftGroup.Campaign(); err != nil {
		log.VEventf(ctx, 1, "failed to campaign: %s", err)
	}
	r.store.enqueueRaftUpdateCheck(r.RangeID)
}

// forceCampaignLocked campaigns for raft leadership, but skips the
// pre-candidate/pre-vote stage, calling an immediate election as candidate, and
// bypasses the CheckQuorum recent leader condition for votes.
//
// This will disrupt an existing leader, and can cause prolonged unavailability
// under partial/asymmetric network partitions. It should only be used when the
// caller is certain that the current leader is actually dead, and we're not
// simply partitioned away from it and/or liveness.
//
// TODO(nvanbenschoten): this is the remaining logic which needs work in order
// to complete #125254. See the comment in raft.go about how even a local
// fortification check is not enough to make MsgTimeoutNow safe.
func (r *Replica) forceCampaignLocked(ctx context.Context) {
	log.VEventf(ctx, 3, "force campaigning")
	msg := raftpb.Message{To: raftpb.PeerID(r.replicaID), Type: raftpb.MsgTimeoutNow}
	if err := r.mu.internalRaftGroup.Step(msg); err != nil {
		log.VEventf(ctx, 1, "failed to campaign: %s", err)
	}
	r.store.enqueueRaftUpdateCheck(r.RangeID)
}

// forgetLeaderLocked forgets a follower's current raft leader, remaining a
// leaderless follower in the current term. The replica will not campaign unless
// the election timeout elapses. However, this allows it to grant (pre)votes if
// a different candidate is campaigning, or revert to a follower if it receives
// a message from the leader. It still won't grant prevotes to a lagging
// follower. This is a noop on non-followers.
//
// This is useful with PreVote+CheckQuorum, where a follower will not grant
// (pre)votes if it has a current leader. Forgetting the leader allows the
// replica to vote without waiting out the election timeout if it has good
// reason to believe the current leader is dead. If a quorum of followers
// independently consider the leader dead, a campaigner can win despite them
// having heard from a leader recently (in ticks).
//
// The motivating case is a quiesced range that unquiesces to a long-dead
// leader: the first replica will campaign and solicit pre-votes, but normally
// the other replicas won't grant the prevote because they heard from the leader
// in the past election timeout (in ticks), requiring waiting for an election
// timeout. However, if they independently see the leader dead in liveness when
// unquiescing, they can forget the leader and grant the prevote. If a quorum of
// replicas independently consider the leader dead, the candidate wins the
// election. If the leader isn't dead after all, the replica will revert to a
// follower upon hearing from it.
//
// In particular, since a quorum must agree that the leader is dead and forget
// it for a candidate to win a pre-campaign, this avoids disruptions during
// partial/asymmetric partitions where individual replicas can mistakenly
// believe the leader is dead and steal leadership away, which can otherwise
// lead to persistent unavailability.
func (r *Replica) forgetLeaderLocked(ctx context.Context) {
	log.VEventf(ctx, 3, "forgetting leader")
	msg := raftpb.Message{To: raftpb.PeerID(r.replicaID), Type: raftpb.MsgForgetLeader}
	if err := r.mu.internalRaftGroup.Step(msg); err != nil {
		log.VEventf(ctx, 1, "failed to forget leader: %s", err)
	}
}

// maybeTransferRaftLeadershipToLeaseholderLocked attempts to transfer the
// leadership away from this node to the leaseholder, if this node is the
// current raft leader but not the leaseholder. We don't attempt to transfer
// leadership if the leaseholder is behind on applying the log.
//
// We like it when leases and raft leadership are collocated because that
// facilitates quick command application (requests generally need to make it to
// both the lease holder and the raft leader before being applied by other
// replicas). Collocation also permits the use of Leader leases, which are more
// efficient than expiration-based leases.
func (r *Replica) maybeTransferRaftLeadershipToLeaseholderLocked(
	ctx context.Context, leaseStatus kvserverpb.LeaseStatus,
) {
	if r.store.TestingKnobs().DisableLeaderFollowsLeaseholder {
		return
	}
	raftStatus := r.mu.internalRaftGroup.SparseStatus()
	leaseAcquisitionPending := r.mu.pendingLeaseRequest.AcquisitionInProgress()
	ok := shouldTransferRaftLeadershipToLeaseholderLocked(
		raftStatus, leaseStatus, leaseAcquisitionPending, r.StoreID(), r.store.IsDraining())
	if ok {
		lhReplicaID := raftpb.PeerID(leaseStatus.Lease.Replica.ReplicaID)
		log.VEventf(ctx, 1, "transferring raft leadership to replica ID %v", lhReplicaID)
		r.store.metrics.RangeRaftLeaderTransfers.Inc(1)
		r.mu.internalRaftGroup.TransferLeader(lhReplicaID)
	}
}

func shouldTransferRaftLeadershipToLeaseholderLocked(
	raftStatus raft.SparseStatus,
	leaseStatus kvserverpb.LeaseStatus,
	leaseAcquisitionPending bool,
	storeID roachpb.StoreID,
	draining bool,
) bool {
	// If we're not the leader, there's nothing to do.
	if raftStatus.RaftState != raftpb.StateLeader {
		return false
	}

	// The status is invalid or its owned locally, there's nothing to do.
	// Otherwise, the lease is valid and owned by another store.
	if !leaseStatus.IsValid() || leaseStatus.OwnedBy(storeID) {
		return false
	}

	// If there is an attempt to acquire the lease in progress, we don't want to
	// transfer leadership away. This is more than just an optimization. If we
	// were to transfer away leadership while a lease request was in progress, we
	// may end up acquiring a leader lease after leadership has been transferred
	// away. Or worse, the leader lease acquisition may succeed and then at some
	// later point, the leadership transfer could succeed, leading to leadership
	// being stolen out from under the leader lease. This second case could lead
	// to a lease expiration regression, as the leadership term would end before
	// lead support had expired.
	//
	// This same form of race is not possible if the lease is transferred to us as
	// raft leader, because lease transfers always send targets expiration-based
	// leases and never leader leases.
	//
	// NOTE: this check may be redundant with the lease validity check above, as a
	// replica will not attempt to acquire a valid lease. We include it anyway for
	// defense-in-depth and so that the proper synchronization between leader
	// leases and leadership transfer makes fewer assumptions. A leader holding
	// a leader lease must never transfer leadership away before transferring the
	// lease away first.
	if leaseAcquisitionPending {
		return false
	}

	// If we're draining, begin the transfer regardless of the leaseholder's raft
	// progress. The leadership transfer itself will still need to wait for the
	// target replica to catch up on its log before it can tell the target to
	// campaign, but this ensures that we don't have to wait for another call to
	// maybeTransferRaftLeadershipToLeaseholderLocked after the target is caught
	// up before starting the process. See 68577d74.
	if draining {
		return true
	}

	// Otherwise, only transfer if the leaseholder is caught up on the raft log.
	lhReplicaID := raftpb.PeerID(leaseStatus.Lease.Replica.ReplicaID)
	lhProgress, ok := raftStatus.Progress[lhReplicaID]
	lhCaughtUp := ok && lhProgress.Match >= raftStatus.Commit
	return lhCaughtUp
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
	descs []roachpb.ReplicaDescriptor, prs map[raftpb.PeerID]tracker.Progress, now time.Time,
) {
	for _, desc := range descs {
		if prs[raftpb.PeerID(desc.ReplicaID)].State == tracker.StateReplicate {
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

// isFollowerActiveSince returns whether the specified follower has made
// communication with the leader recently (since threshold).
func (m lastUpdateTimesMap) isFollowerActiveSince(
	replicaID roachpb.ReplicaID, now time.Time, threshold time.Duration,
) bool {
	lastUpdateTime, ok := m[replicaID]
	if !ok {
		// If the follower has no entry in lastUpdateTimes, it has not been
		// updated since r became the leader (at which point all then-existing
		// replicas were updated).
		return false
	}
	return now.Sub(lastUpdateTime) <= threshold
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
	for endKey.Less(inSnap.Desc.EndKey) {
		sRepl := r.store.LookupReplica(endKey)
		if sRepl == nil || !endKey.Equal(sRepl.Desc().StartKey) {
			log.Fatalf(ctx, "snapshot widens existing replica, but no replica exists for subsumed key %s", endKey)
		}
		sRepl.raftMu.Lock()
		subsumedRepls = append(subsumedRepls, sRepl)
		endKey = sRepl.Desc().EndKey
	}
	return subsumedRepls, func() {
		for _, sr := range subsumedRepls {
			sr.raftMu.Unlock()
		}
	}
}

// maybeAcquireSplitMergeLock examines the given raftCmd (which need
// not be applied yet) and acquires the split or merge lock if
// necessary (in addition to other preparation). It returns a function
// which will release any lock acquired (or nil).
//
// After this method returns successfully the RHS of the split or merge
// is guaranteed to exist in the Store using GetReplica().
func (r *Replica) maybeAcquireSplitMergeLock(
	ctx context.Context, raftCmd kvserverpb.RaftCommand,
) (func(), error) {
	if split := raftCmd.ReplicatedEvalResult.Split; split != nil {
		return r.acquireSplitLock(ctx, &split.SplitTrigger)
	} else if merge := raftCmd.ReplicatedEvalResult.Merge; merge != nil {
		return r.acquireMergeLock(ctx, &merge.MergeTrigger)
	}
	return nil, nil
}

func (r *Replica) acquireSplitLock(
	ctx context.Context, split *roachpb.SplitTrigger,
) (func(), error) {
	rightReplDesc, _ := split.RightDesc.GetReplicaDescriptor(r.StoreID())
	rightRepl, _, err := r.store.getOrCreateReplica(
		ctx, split.RightDesc.RangeID, rightReplDesc.ReplicaID, nil, /* creatingReplica */
	)
	// If getOrCreateReplica returns RaftGroupDeletedError we know that the RHS
	// has already been removed. This case is handled properly in splitPostApply.
	if errors.HasType(err, (*kvpb.RaftGroupDeletedError)(nil)) {
		return func() {}, nil
	}
	if err != nil {
		return nil, err
	}
	// The right hand side of a split is always uninitialized since
	// the left hand side blocks snapshots to it, and a snapshot is
	// required to initialize it (if the split trigger doesn't - and
	// this code here is part of the split trigger).
	if rightRepl.IsInitialized() {
		return nil, errors.Errorf("RHS of split %s / %s already initialized before split application",
			&split.LeftDesc, &split.RightDesc)
	}
	return rightRepl.raftMu.Unlock, nil
}

func (r *Replica) acquireMergeLock(
	ctx context.Context, merge *roachpb.MergeTrigger,
) (func(), error) {
	// The merge lock is the right-hand replica's raftMu. The right-hand replica
	// is required to exist on this store at the merge implied replica ID.
	// Otherwise, an incoming snapshot could create the right-hand replica before
	// the merge trigger has a chance to widen the left-hand replica's end key.
	// The merge trigger would then fatal the node upon realizing the right-hand
	// replica already exists. With a right-hand replica in place, any snapshots
	// for the right-hand range will block on raftMu, waiting for the merge to
	// complete, after which the replica will realize it has been destroyed and
	// reject the snapshot.
	rightReplDesc, _ := merge.RightDesc.GetReplicaDescriptor(r.StoreID())
	rightRepl, _, err := r.store.getOrCreateReplica(
		ctx, merge.RightDesc.RangeID, rightReplDesc.ReplicaID, nil, /* creatingReplica */
	)
	if err != nil {
		return nil, err
	}
	rightDesc := rightRepl.Desc()
	if !rightDesc.StartKey.Equal(merge.RightDesc.StartKey) || !rightDesc.EndKey.Equal(merge.RightDesc.EndKey) {
		return nil, errors.Errorf("RHS of merge %s <- %s not present on store; found %s in place of the RHS",
			&merge.LeftDesc, &merge.RightDesc, rightDesc)
	}
	return rightRepl.raftMu.Unlock, nil
}

// handleTruncatedStateBelowRaftPreApply is called before applying a Raft
// command that updates the truncated state.
//
// The truncated state of a replica determines where its Raft log starts (by
// giving the last index that was already deleted). It's unreplicated -- it can
// differ between replicas at the same applied index. This divergence occurs
// primarily occurs through snapshots that contain no log entries; the truncated
// index in the snapshot is set to equal the applied index it was generated
// from. The truncation itself then is a purely replicated side effect.
//
// Updates to the HardState are sent out by a leaseholder truncating the log
// based on its local knowledge. For example, the leader might have a log
// 10..100 and truncates to 50, and will send out a TruncatedState with Index 50
// to that effect. However, some replicas may not even have log entries that
// old and must make sure to ignore this update to the truncated state, as it
// would otherwise clobber their "newer" truncated state. The truncated state
// provided by the leader then is merely a suggested one -- we could ignore it
// and still be correct.
//
// We also rely on log truncations happening in the apply loop -- this makes
// sure that a truncation does not remove entries to be applied that we haven't
// yet. Since a truncation only ever removes committed log indexes, and after
// choosing the index gets proposed, the truncation command itself will be
// assigned an index higher than the one it could possibly remove. By the time
// the truncation itself is handled, the state machine will have applied all
// entries the truncation could possibly affect.
//
// The returned boolean tells the caller whether to apply the truncated state's
// side effects, which means replacing the in-memory TruncatedState and applying
// the associated RaftLogDelta. It is usually expected to be true, but may not
// be for the first truncation after on a replica that recently received a
// snapshot.
func handleTruncatedStateBelowRaftPreApply(
	ctx context.Context,
	currentTruncatedState, suggestedTruncatedState *kvserverpb.RaftTruncatedState,
	loader stateloader.StateLoader,
	readWriter storage.ReadWriter,
) (_apply bool, _ error) {
	if suggestedTruncatedState.Index <= currentTruncatedState.Index {
		// The suggested truncated state moves us backwards; instruct the
		// caller to not update the in-memory state.
		return false, nil
	}

	// Truncate the Raft log from the entry after the previous
	// truncation index to the new truncation index. This is performed
	// atomically with the raft command application so that the
	// TruncatedState index is always consistent with the state of the
	// Raft log itself.
	prefixBuf := &loader.RangeIDPrefixBuf
	numTruncatedEntries := suggestedTruncatedState.Index - currentTruncatedState.Index
	if numTruncatedEntries >= raftLogTruncationClearRangeThreshold {
		start := prefixBuf.RaftLogKey(currentTruncatedState.Index + 1).Clone()
		end := prefixBuf.RaftLogKey(suggestedTruncatedState.Index + 1).Clone() // end is exclusive
		if err := readWriter.ClearRawRange(start, end, true, false); err != nil {
			return false, errors.Wrapf(err,
				"unable to clear truncated Raft entries for %+v between indexes %d-%d",
				suggestedTruncatedState, currentTruncatedState.Index+1, suggestedTruncatedState.Index+1)
		}
	} else {
		// NB: RangeIDPrefixBufs have sufficient capacity (32 bytes) to
		// avoid allocating when constructing Raft log keys (16 bytes).
		prefix := prefixBuf.RaftLogPrefix()
		for idx := currentTruncatedState.Index + 1; idx <= suggestedTruncatedState.Index; idx++ {
			if err := readWriter.ClearUnversioned(
				keys.RaftLogKeyFromPrefix(prefix, idx),
				storage.ClearOptions{},
			); err != nil {
				return false, errors.Wrapf(err, "unable to clear truncated Raft entries for %+v at index %d",
					suggestedTruncatedState, idx)
			}
		}
	}

	// The suggested truncated state moves us forward; apply it and tell
	// the caller as much.
	if err := storage.MVCCPutProto(
		ctx,
		readWriter,
		prefixBuf.RaftTruncatedStateKey(),
		hlc.Timestamp{},
		suggestedTruncatedState,
		storage.MVCCWriteOptions{Category: fs.ReplicationReadCategory},
	); err != nil {
		return false, errors.Wrap(err, "unable to write RaftTruncatedState")
	}

	return true, nil
}

// ComputeRaftLogSize computes the size (in bytes) of the Raft log from the
// storage engine. This will iterate over the Raft log and sideloaded files, so
// depending on the size of these it can be mildly to extremely expensive and
// thus should not be called frequently.
func ComputeRaftLogSize(
	ctx context.Context,
	rangeID roachpb.RangeID,
	reader storage.Reader,
	sideloaded logstore.SideloadStorage,
) (int64, error) {
	prefix := keys.RaftLogPrefix(rangeID)
	prefixEnd := prefix.PrefixEnd()
	ms, err := storage.ComputeStats(ctx, reader, prefix, prefixEnd, 0 /* nowNanos */)
	if err != nil {
		return 0, err
	}
	// The remaining bytes if one were to truncate [0, 0) gives us the total
	// number of bytes in sideloaded files.
	_, totalSideloaded, err := sideloaded.BytesIfTruncatedFromTo(ctx, 0, 0)
	if err != nil {
		return 0, err
	}
	return ms.SysBytes + totalSideloaded, nil
}

// shouldCampaignAfterConfChange returns true if the current replica should
// campaign after a conf change. If the leader replica is demoted or removed,
// the leaseholder should campaign. We don't want to campaign on multiple
// replicas, since that would cause ties.
//
// If there is no current leaseholder we'll have to wait out the election
// timeout before someone campaigns, but that's ok -- either we'll have to wait
// for the lease to expire anyway, or the range is presumably idle.
//
// The caller should campaign using forceCampaignLocked(), transitioning
// directly to candidate and bypassing PreVote+CheckQuorum. Otherwise it won't
// receive prevotes since other replicas have heard from the leader recently.
func shouldCampaignAfterConfChange(
	ctx context.Context,
	st *cluster.Settings,
	storeID roachpb.StoreID,
	desc *roachpb.RangeDescriptor,
	raftStatus raft.BasicStatus,
	leaseStatus kvserverpb.LeaseStatus,
) bool {
	if raftStatus.Lead == raft.None {
		// Leader unknown. We can't know if it was removed by the conf change, and
		// because we force an election without prevote we don't want to risk
		// throwing spurious elections.
		return false
	}
	if raftStatus.RaftState == raftpb.StateLeader {
		// We're already the leader, no point in campaigning.
		return false
	}
	if !desc.IsInitialized() {
		// No descriptor, so we don't know if the leader has been removed. We
		// don't expect to hit this, but let's be defensive.
		return false
	}
	if replDesc, ok := desc.GetReplicaDescriptorByID(roachpb.ReplicaID(raftStatus.Lead)); ok {
		if replDesc.IsAnyVoter() {
			// The leader is still a voter in the descriptor.
			return false
		}
	}
	if !leaseStatus.OwnedBy(storeID) || !leaseStatus.IsValid() {
		// We're not the leaseholder.
		return false
	}
	log.VEventf(ctx, 3, "leader got removed by conf change, campaigning")
	return true
}

// printRaftTail pretty-prints the tail of the log and returns it as a string,
// with the same format as `cockroach debug raft-log`. The entries are printed
// from newest to oldest. maxEntries and maxCharsPerEntry control the size of
// the output.
//
// If an error is returned, it's possible that a string with some entries is
// still returned.
func (r *Replica) printRaftTail(
	ctx context.Context, maxEntries, maxCharsPerEntry int,
) (string, error) {
	start := keys.RaftLogPrefix(r.RangeID)
	end := keys.RaftLogPrefix(r.RangeID).PrefixEnd()

	// NB: raft log does not have intents.
	it, err := r.store.TODOEngine().NewEngineIterator(
		ctx, storage.IterOptions{LowerBound: start, UpperBound: end})
	if err != nil {
		return "", err
	}
	valid, err := it.SeekEngineKeyLT(storage.EngineKey{Key: end})
	if err != nil {
		return "", err
	}
	if !valid {
		return "", errors.AssertionFailedf("iterator invalid but no error")
	}

	var sb strings.Builder
	for i := 0; i < maxEntries; i++ {
		key, err := it.EngineKey()
		if err != nil {
			return sb.String(), err
		}
		mvccKey, err := key.ToMVCCKey()
		if err != nil {
			return sb.String(), err
		}
		v, err := it.Value()
		if err != nil {
			return sb.String(), err
		}
		kv := storage.MVCCKeyValue{
			Key:   mvccKey,
			Value: v,
		}
		sb.WriteString(truncateEntryString(SprintMVCCKeyValue(kv, true /* printKey */), 2000))
		sb.WriteRune('\n')

		valid, err := it.PrevEngineKey()
		if err != nil {
			return sb.String(), err
		}
		if !valid {
			// We've finished the log.
			break
		}
	}
	return sb.String(), nil
}

// updateLastUpdateTimesUsingStoreLivenessRLocked updates the lastUpdateTimes
// map if the follower's store is providing store liveness support. This is
// useful because typically this map is updated on every message, but that
// assumes that raft will periodically heartbeat. This assumption doesn't hold
// under the raft fortification protocol, where failure detection is subsumed by
// store liveness.
//
// This method assume that Replica.mu is held in read mode.
func (r *Replica) updateLastUpdateTimesUsingStoreLivenessRLocked(
	storeClockTimestamp hlc.ClockTimestamp,
) {
	// If store liveness is not enabled, there is nothing to do.
	if !(*replicaRLockedStoreLiveness)(r).SupportFromEnabled() {
		return
	}

	for _, desc := range r.descRLocked().Replicas().Descriptors() {
		// If the replica's store if providing store liveness support, update
		// lastUpdateTimes to indicate that it is alive.
		_, curExp := (*replicaRLockedStoreLiveness)(r).SupportFrom(raftpb.PeerID(desc.ReplicaID))
		if storeClockTimestamp.ToTimestamp().LessEq(curExp) {
			r.mu.lastUpdateTimes.update(desc.ReplicaID, r.Clock().PhysicalTime())
		}
	}
}

func truncateEntryString(s string, maxChars int) string {
	res := s
	if len(s) > maxChars {
		if maxChars > 3 {
			maxChars -= 3
		}
		res = s[0:maxChars] + "..."
	}
	return res
}
