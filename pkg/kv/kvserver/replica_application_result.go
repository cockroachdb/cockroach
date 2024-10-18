// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// replica_application_*.go files provide concrete implementations of
// the interfaces defined in the storage/apply package:
//
// replica_application_state_machine.go  ->  apply.StateMachine
// replica_application_decoder.go        ->  apply.Decoder
// replica_application_cmd.go            ->  apply.Command         (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandIterator (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandList     (and variants)
//
// These allow Replica to interface with the storage/apply package.

// clearTrivialReplicatedEvalResultFields is used to zero out the fields of a
// ReplicatedEvalResult that have already been consumed when staging the
// corresponding command and applying it to the current batch's view of the
// ReplicaState. This function is called after a batch has been written to the
// storage engine. For trivial commands this function should result in a zero
// value replicatedResult.
func clearTrivialReplicatedEvalResultFields(r *kvserverpb.ReplicatedEvalResult) {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the application process
	// (which checks that all fields were handled).
	r.IsLeaseRequest = false
	r.IsLeaseRequestWithExpirationToEpochEquivalent = false
	r.WriteTimestamp = hlc.Timestamp{}
	r.PrevLeaseProposal = nil
	// The state fields cleared here were already applied to the in-memory view of
	// replica state for this batch.
	if haveState := r.State != nil; haveState {
		r.State.Stats = nil
		if *r.State == (kvserverpb.ReplicaState{}) {
			r.State = nil
		}
	}
	r.Delta = enginepb.MVCCStatsDelta{}
	// Rangefeeds have been disconnected prior to application.
	r.MVCCHistoryMutation = nil
}

// prepareLocalResult is performed after the command has been committed to the
// engine but before its side-effects have been applied to the Replica's
// in-memory state. This method gives the command an opportunity to interact
// with testing knobs and to set up its local result if it was proposed
// locally. This is performed prior to handling the command's
// ReplicatedEvalResult because the process of handling the replicated eval
// result will zero-out the struct to ensure that is has properly performed all
// of the implied side-effects.
func (r *Replica) prepareLocalResult(ctx context.Context, cmd *replicatedCmd) {
	if !cmd.IsLocal() {
		return
	}

	var pErr *kvpb.Error
	if filter := r.store.cfg.TestingKnobs.TestingPostApplyFilter; filter != nil {
		var newPropRetry int
		newPropRetry, pErr = filter(kvserverbase.ApplyFilterArgs{
			CmdID:                cmd.ID,
			Cmd:                  cmd.Cmd,
			Entry:                cmd.Entry.Entry,
			ReplicatedEvalResult: *cmd.ReplicatedResult(),
			StoreID:              r.store.StoreID(),
			RangeID:              r.RangeID,
			ReplicaID:            r.replicaID,
			Req:                  cmd.proposal.Request,
			ForcedError:          cmd.ForcedError,
		})
		if cmd.Rejection == 0 {
			cmd.Rejection = kvserverbase.ProposalRejectionType(newPropRetry)
		}
	}
	if pErr == nil {
		pErr = cmd.ForcedError
	}

	if cmd.Rejection != kvserverbase.ProposalRejectionPermanent && pErr == nil {
		log.Fatalf(ctx, "proposal with nontrivial retry behavior, but no error: %+v", cmd.proposal)
	}
	if pErr != nil {
		// A forced error was set (i.e. we did not apply the proposal,
		// for instance due to its log position).
		cmd.response.Err = pErr
		switch cmd.Rejection {
		case kvserverbase.ProposalRejectionPermanent:
		case kvserverbase.ProposalRejectionIllegalLeaseIndex:
			// Reset the error as it's now going to be determined by the outcome of
			// reproposing (or not); note that tryReproposeWithNewLeaseIndexRaftMuLocked will
			// return `nil` if the entry is not eligible for reproposals.
			//
			// If pErr gets "reset" here as a result, we will mark the proposal as
			// non-local at the end of this block and return, so we're not hitting an
			// NPE near the end of this method where we're attempting to reach into
			// `cmd.proposal`.
			//
			// This control flow is sketchy but it preserves existing behavior
			// that would be too risky to change at the time of writing.
			//
			// If we failed to apply at the right lease index, try again with a
			// new one. This is important for pipelined writes, since they don't
			// have a client watching to retry, so a failure to eventually apply
			// the proposal would be a user-visible error.
			//
			// If the command associated with this rejected raft entry already applied
			// then we don't want to repropose it. Doing so could lead to duplicate
			// application of the same proposal. (We can see hit this case if an application
			// batch contains multiple copies of the same proposal, in which case they are
			// all marked as local, the first one will apply (and set p.applied) and the
			// remaining copies will hit this branch).
			//
			// Similarly, if the proposal associated with this rejected raft entry is
			// superseded by a different (larger) MaxLeaseIndex than the one we decoded
			// from the entry itself, the command must have already passed through
			// tryReproposeWithNewLeaseIndexRaftMuLocked previously (this can happen if there are
			// multiple copies of the command in the logs; see
			// TestReplicaRefreshMultiple). We must not create multiple copies with
			// multiple lease indexes, so don't repropose it again. This ensures that at
			// any time, there is only up to a single lease index that has a chance of
			// succeeding in the Raft log for a given command.
			//
			// Taking a looking glass to the last paragraph, we see that the situation
			// is much more subtle. As tryReproposeWithNewLeaseIndexRaftMuLocked gets to the stage
			// where it calls `(*Replica).propose` (i.e. it passed the closed
			// timestamp checks), it *resets* `cmd.proposal.MaxLeaseIndex` to zero.
			// This means that the proposal immediately supersedes any other copy
			// presently in the log, including for the remainder of application of the
			// current log entry (since the current entry's LAI is certainly not equal
			// to zero). However, the proposal buffer adds another layer of
			// possibility on top of this. Typically, the buffer does not flush
			// immediately: this only happens at the beginning of the *next* raft
			// handling cycle, i.e. the proposal will not re-enter the proposals map
			// while the current batches of commands (recall, which may contain an
			// arbitrary number of copies of the current command, both with various
			// LAIs all but at most one of which are too small) are applied. *But*,
			// the proposal buffer has a capacity, and if it does fill up in
			// `(*Replica).propose` it will synchronously flush, meaning that a new
			// LAI will be assigned to `cmd.proposal.MaxLeaseIndex` AND the command
			// will have re-entered the proposals map. So even if we remove the
			// "zeroing" of the MaxLeaseIndex prior to proposing, we still have to
			// contend with the fact that once `tryReproposeWithNewLeaseIndexRaftMuLocked` may
			// or may not be in the map. (At least it is assigned a new LAI if and
			// only if it is back in the map!).
			//
			// These many possible worlds are a major source of complexity, a
			// reduction of which is postponed.
			pErr = nil
			if fn := r.store.TestingKnobs().InjectReproposalError; fn != nil {
				if err := fn(cmd.proposal); err != nil {
					pErr = kvpb.NewError(err)
				}
			}
			if pErr == nil { // since we might have injected an error
				pErr = kvpb.NewError(r.tryReproposeWithNewLeaseIndexRaftMuLocked(ctx, cmd))
				if pErr == nil {
					// Avoid falling through below. We managed to repropose, but this
					// proposal is still erroring out. We don't want to assign to
					// localResult. If there is an error though, we do fall through into
					// the existing tangle of correct but unreadable handling below.
					return
				}
			}

			if pErr != nil {
				// An error from tryReproposeWithNewLeaseIndexRaftMuLocked implies that the current
				// entry is not superseded (i.e. we don't have a reproposal at a higher
				// MaxLeaseIndex in the log).
				//
				// Commonly the error is an inability to repropose the command due to
				// the closed timestamp having advanced past the write timestamp at
				// which the command was originally evaluated, so this path is hit
				// in practice.
				//
				// An error here implies that any additional copies of the command
				// (which may be present in the log ahead of the current entry) will
				// also fail.
				//
				// It is thus safe to signal the error back to the client, which is also
				// the only sensible choice at this point.
				//
				// Note that the proposal may or may not be in the proposals map at this
				// point. For example, if we artificially inject invalid LAIs during
				// proposal, see the explanatory comment above. If, in the current app
				// batch, we had two identical copies of an entry (which maps to a local
				// proposal), and the LAI was stale, then both entries would be local.
				// The first could succeed to repropose, which, if the propBuf was full,
				// would immediately insert into the proposals map. Normally, when we
				// then apply the second entry, it would be superseded and not hit the
				// assertion. But, if we injected a stale LAI during this last
				// reproposal, we could "accidentally" assign the same LAI again. The
				// second entry would then try to repropose again, which is fine, but it
				// could bump into the closed timestamp, get an error, and now we are in
				// a situation where a reproposal attempt failed with the proposal being
				// present in the map.
				//
				// For proposed simplifications, see:
				// https://github.com/cockroachdb/cockroach/issues/97633
				log.Infof(ctx, "failed to repropose %s at idx %d with new lease index: %s", cmd.ID, cmd.Index(), pErr)
				// TODO(repl): we're replacing an error (illegal LAI) here with another error.
				// A pattern where the error is assigned exactly once would be simpler to
				// reason about. In particular, we want to make sure we never replace an
				// ambiguous error with an unambiguous one (at least if the resulting
				// error will reach the proposer, such as can be the case here, though
				// the error we're replacing is also definite so it's ok).
				cmd.response.Err = pErr
				// Fall through.
			}
		default:
			panic("unexpected")
		}
	} else if cmd.proposal.Local.Reply != nil {
		cmd.response.Reply = cmd.proposal.Local.Reply
	} else {
		log.Fatalf(ctx, "proposal must return either a reply or an error: %+v", cmd.proposal)
	}

	// The current proposal has no error (and wasn't reproposed successfully or we
	// would've early returned already) OR it has an error AND we failed to
	// repropose it.
	//
	// TODO(tbg): it doesn't make sense to assign to `cmd.response` unconditionally.
	// We're returning an error; the response should be nil. The error tracking in
	// this method should be cleaned up.
	// TODO(tbg): we should have an invariant about `cmd.response`: it is
	// initially nil (in particular, a command that evaluates with an error - say
	// a TransactionRetryError - must not enter the replication pipeline; we could
	// relax this if we ever want erroring commands to be able to mutate the state
	// machine but safe to say we don't have this now) and is only written once
	// (in this method).
	// Also, If a caller gets signaled early (ambiguous result, etc) this does not
	// affect `response.Err`.
	cmd.response.EncounteredIntents = cmd.proposal.Local.DetachEncounteredIntents()
	// TODO(tbg): this seems wrong. the "Always" (pErr != nil) flavor of intents is
	// for transaction aborts that still "know" about the ultimate fate of an intent.
	// But since we're never reaching this code for a proposal that evaluated to an
	// error, we can only reach it for illegal lease errors and the like, and even
	// though it might be "correct" to surface the "always" intents in the response
	// in that case, it would seem prudent not to take advantage of that. In other
	// words, the line below this comment should be conditional on `pErr == nil`.
	cmd.response.EndTxns = cmd.proposal.Local.DetachEndTxns(pErr != nil)

	// Populate BarrierResponse if requested.
	if pErr == nil && cmd.proposal.Local.DetachPopulateBarrierResponse() {
		if resp := cmd.response.Reply.Responses[0].GetBarrier(); resp != nil {
			resp.LeaseAppliedIndex = cmd.LeaseIndex
			resp.RangeDesc = *r.Desc()
		} else {
			log.Fatalf(ctx, "PopulateBarrierResponse for %T", cmd.response.Reply.Responses[0].GetInner())
		}
	}

	if pErr == nil {
		cmd.localResult = cmd.proposal.Local
	} else if cmd.localResult != nil {
		log.Fatalf(ctx, "shouldn't have a local result if command processing failed. pErr: %s", pErr)
	}
}

// makeReproposal returns a new re-proposal of the given proposal, and a
// function that must be called if this re-proposal is successfully proposed.
//
// We want to move a few items from origP to the new command, but only if we
// managed to propose the new command. For example, if we move the latches over
// too early but then fail to actually get the new proposal started, the old
// proposal will not release the latches. This would result in a lost latch.
func (r *Replica) makeReproposal(origP *ProposalData) (reproposal *ProposalData, success func()) {
	// NB: original command remains "Local". It's just not going to signal anyone
	// or release any latches.

	seedP := origP.seedProposal
	if seedP == nil {
		seedP = origP
	}

	// Go through the original proposal field by field and decide what transfers
	// to the new proposal (and how that affects the old proposal). The overall
	// goal is that the old proposal remains a local proposal (switching it to
	// non-local now invites logic bugs) but not bound to the caller.

	newCommand := kvserverpb.RaftCommand{
		ProposerLeaseSequence: origP.command.ProposerLeaseSequence,
		ReplicatedEvalResult:  origP.command.ReplicatedEvalResult,
		WriteBatch:            origP.command.WriteBatch,
		LogicalOpLog:          origP.command.LogicalOpLog,
		TraceData:             origP.command.TraceData,

		MaxLeaseIndex:       0,   // assigned on flush
		ClosedTimestamp:     nil, // assigned on flush
		AdmissionPriority:   0,   // assigned on flush
		AdmissionCreateTime: 0,   // assigned on flush
		AdmissionOriginNode: 0,   // assigned on flush
	}

	// Now we construct the remainder of the ProposalData. The pieces that
	// actively "move over", are removed from the original proposal in the
	// deferred func below. For example, those fields that have to do with the
	// latches held and the caller waiting to be signaled.

	// TODO(tbg): work on the lifecycle of ProposalData. This struct (and the
	// surrounding replicatedCmd) are populated in an overly ad-hoc manner.
	// TODO(tbg): the fields are spelled out here to make explicit what is being copied
	// here. Add a unit test that fails on addition of a new field and points at the
	// need to double check what the intended behavior of the new field in this method
	// is.
	newProposal := &ProposalData{
		// The proposal's context and span carry over. Recall that they are *not*
		// used for command application; `cmd.{ctx,sp}` are; and since this last
		// span "follows from" the proposal's span, if the proposal sticks around
		// for (some reincarnation of) the command to eventually apply, its trace
		// will reflect the reproposal as well.
		idKey:           raftlog.MakeCmdIDKey(),
		proposedAtTicks: 0, // set in registerProposalLocked
		createdAtTicks:  0, // set in registerProposalLocked
		command:         &newCommand,

		// Next comes the block of fields that are "moved" to the new proposal. See
		// the deferred function call below which, correspondingly, clears these
		// fields in the original proposal.
		sp: origP.sp,
		// NB: quotaAlloc is always nil here, because we already released the quota
		// unconditionally in retrieveLocalProposals. So the below is a no-op.
		//
		// TODO(tbg): if we shifted the release of proposal quota to *after*
		// successful application, we could move the quota over prematurely
		// releasing it here.
		quotaAlloc: origP.quotaAlloc,
		ec:         origP.ec,
		doneCh:     origP.doneCh,

		applied: false,

		// Local is copied over. It won't be used on the old proposal (since that
		// proposal got rejected), but since it's still "local" we don't want to put
		// it into  an undefined state by removing its response. The same goes for
		// Request.
		Local:                   origP.Local,
		Request:                 origP.Request,
		leaseStatus:             origP.leaseStatus,
		tok:                     TrackedRequestToken{}, // filled in in `propose`
		encodedCommand:          nil,
		raftAdmissionMeta:       nil,
		v2SeenDuringApplication: false,

		seedProposal: seedP,
	}
	origCtx := origP.Context()
	newProposal.ctx.Store(&origCtx)

	return newProposal, func() {
		// If the original proposal had an explicit span, it's an async consensus
		// proposal and the span would be finished momentarily (when we return to
		// the caller) if we didn't unlink it here, but we want it to continue
		// tracking newProposal. We leave it in `origP.ctx` though, since that
		// context will become unused once the application of this (soft-failed)
		// proposal concludes, i.e. soon after this method returns, in case there is
		// anything left to log into it.
		origP.sp = nil
		origP.quotaAlloc = nil
		origP.ec = makeEmptyEndCmds()
		origP.doneCh = nil

		// If the proposal is synchronous, the client is waiting on the seed
		// proposal. By the time it has to act on the result, a bunch of reproposals
		// can have happened, and some may still be running and using the
		// context/tracing span (probably only the latest one, but we assume any,
		// for defence-in-depth).
		//
		// Unbind the latest reproposal's context so that it no longer posts updates
		// to the tracing span (it won't apply anyway). Link to the new latest
		// reproposal, so that the client can clear its context at post-processing.
		// This is effectively a "move" of the context to the reproposal.
		//
		// TODO(pavelkalinnikov): there should be a better way, after ProposalData
		// lifecycle is reconsidered.
		//
		// TODO(radu): Should this context be created via tracer.ForkSpan?
		// We'd need to make sure the span is finished eventually.
		ctx := r.AnnotateCtx(context.TODO())
		origP.ctx.Store(&ctx)
		seedP.lastReproposal = newProposal
	}
}

func (r *Replica) tryReproposeWithNewLeaseIndexRaftMuLocked(
	ctx context.Context, origCmd *replicatedCmd,
) error {
	r.raftMu.AssertHeld() // we're in the applications stack

	newProposal, onSuccess := r.makeReproposal(origCmd.proposal)

	// We need to track the request again in order to protect its timestamp until
	// it gets reproposed.
	// TODO(andrei): Only track if the request consults the ts cache. Some
	// requests (e.g. EndTxn) don't care about closed timestamps.
	minTS, tok := r.mu.proposalBuf.TrackEvaluatingRequest(ctx, newProposal.Request.WriteTimestamp())
	defer tok.DoneIfNotMoved(ctx)

	// NB: newProposal.Request.Timestamp reflects the action of ba.SetActiveTimestamp.
	if newProposal.Request.AppliesTimestampCache() && newProposal.Request.WriteTimestamp().LessEq(minTS) {
		// The tracker wants us to forward the request timestamp, but we can't
		// do that without re-evaluating, so give up. The error returned here
		// will go to back to DistSender, so send something it can digest.
		return kvpb.NewNotLeaseHolderError(
			*r.shMu.state.Lease,
			r.store.StoreID(),
			r.shMu.state.Desc,
			"reproposal failed due to closed timestamp",
		)
	}
	// Some tests check for this log message in the trace.
	log.VEventf(ctx, 2, "retry: proposalIllegalLeaseIndex")

	// See I7 from kvflowcontrol/doc.go: we don't re-deduct flow tokens on
	// reproposals.
	newProposal.raftAdmissionMeta = nil

	if pErr := r.propose(ctx, newProposal, tok.Move(ctx)); pErr != nil {
		return pErr.GoError()
	}
	r.store.metrics.RaftCommandsReproposedLAI.Inc(1)
	log.VEventf(ctx, 2, "reproposed command %x", newProposal.idKey)

	onSuccess()
	return nil
}

// The following Replica.handleXYZResult methods are called when applying
// non-trivial side effects in replicaStateMachine.ApplySideEffects. As a
// general rule, there is a method for each of the non-trivial fields in
// ReplicatedEvalResult. Most methods are simple enough that they will be
// inlined.

func (r *Replica) handleSplitResult(ctx context.Context, split *kvserverpb.Split) {
	splitPostApply(ctx, split.RHSDelta, &split.SplitTrigger, r)
}

func (r *Replica) handleMergeResult(ctx context.Context, merge *kvserverpb.Merge) {
	if err := r.store.MergeRange(
		ctx,
		r,
		merge.LeftDesc,
		merge.RightDesc,
		merge.FreezeStart,
		merge.RightClosedTimestamp,
		merge.RightReadSummary,
	); err != nil {
		// Our in-memory state has diverged from the on-disk state.
		log.Fatalf(ctx, "failed to update store after merging range: %s", err)
	}
}

func (r *Replica) handleDescResult(ctx context.Context, desc *roachpb.RangeDescriptor) {
	r.setDescRaftMuLocked(ctx, desc)
}

func (r *Replica) handleLeaseResult(
	ctx context.Context, lease *roachpb.Lease, priorReadSum *rspb.ReadSummary,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leasePostApplyLocked(ctx,
		r.shMu.state.Lease, /* prevLease */
		lease,              /* newLease */
		priorReadSum,
		assertNoLeaseJump)
}

func (r *Replica) handleTruncatedStateResult(
	ctx context.Context,
	t *kvserverpb.RaftTruncatedState,
	expectedFirstIndexPreTruncation kvpb.RaftIndex,
) (raftLogDelta int64, expectedFirstIndexWasAccurate bool) {
	r.mu.Lock()
	expectedFirstIndexWasAccurate =
		r.shMu.state.TruncatedState.Index+1 == expectedFirstIndexPreTruncation
	r.shMu.state.TruncatedState = t
	r.mu.Unlock()

	// Clear any entries in the Raft log entry cache for this range up
	// to and including the most recently truncated index.
	r.store.raftEntryCache.Clear(r.RangeID, t.Index+1)

	// Truncate the sideloaded storage. This is safe only if the new truncated
	// state is durably stored on disk, i.e. synced.
	// TODO(#38566, #113135): this is unfortunately not true, need to fix this.
	//
	// TODO(sumeer): once we remove the legacy caller of
	// handleTruncatedStateResult, stop calculating the size of the removed
	// files and the remaining files.
	log.Eventf(ctx, "truncating sideloaded storage up to (and including) index %d", t.Index)
	size, _, err := r.raftMu.sideloaded.TruncateTo(ctx, t.Index+1)
	if err != nil {
		// We don't *have* to remove these entries for correctness. Log a
		// loud error, but keep humming along.
		log.Errorf(ctx, "while removing sideloaded files during log truncation: %+v", err)
	}
	// NB: we don't sync the sideloaded entry files removal here for performance
	// reasons. If a crash occurs, and these files get recovered after a restart,
	// we should clean them up on the server startup.
	//
	// TODO(#113135): this removal survives process crashes though, and system
	// crashes if the filesystem is quick enough to sync it for us. Add a test
	// that syncs the files removal here, and "crashes" right after, to help
	// reproduce and fix #113135.
	return -size, expectedFirstIndexWasAccurate
}

func (r *Replica) handleGCThresholdResult(ctx context.Context, thresh *hlc.Timestamp) {
	if thresh.IsEmpty() {
		return
	}
	r.mu.Lock()
	r.shMu.state.GCThreshold = thresh
	r.mu.Unlock()
}

func (r *Replica) handleGCHintResult(ctx context.Context, hint *roachpb.GCHint) {
	r.mu.Lock()
	r.shMu.state.GCHint = hint
	r.mu.Unlock()
}

func (r *Replica) handleVersionResult(ctx context.Context, version *roachpb.Version) {
	if (*version == roachpb.Version{}) {
		log.Fatal(ctx, "not expecting empty replica version downstream of raft")
	}
	r.mu.Lock()
	r.shMu.state.Version = version
	r.mu.Unlock()
}

func (r *Replica) handleComputeChecksumResult(ctx context.Context, cc *kvserverpb.ComputeChecksum) {
	err := r.computeChecksumPostApply(ctx, *cc)
	// Don't log errors caused by the store quiescing, they are expected.
	if err != nil && !errors.Is(err, stop.ErrUnavailable) {
		log.Errorf(ctx, "failed to start ComputeChecksum task %s: %v", cc.ChecksumID, err)
	}
}

func (r *Replica) handleChangeReplicasResult(
	ctx context.Context, chng *kvserverpb.ChangeReplicas,
) (changeRemovedReplica bool) {
	// If this command removes us then we would have set the destroy status
	// to destroyReasonRemoved which we detect here.
	//
	// Note that a replica's destroy status is only ever updated under the
	// raftMu and we validated that the replica was not RemovingOrRemoved
	// before processing this raft ready.
	if ds, _ := r.IsDestroyed(); ds != destroyReasonRemoved {
		return false // changeRemovedReplica
	}

	// If this command removes us then we need to go through the process of
	// removing our replica from the store. After this method returns, the code
	// should roughly return all the way up to whoever called handleRaftReady
	// and this Replica should never be heard from again. We can detect if this
	// change removed us by inspecting the replica's destroyStatus. We check the
	// destroy status before processing a raft ready so if we find ourselves with
	// removal pending at this point then we know that this command must be
	// responsible.
	if log.V(1) {
		log.Infof(ctx, "removing replica due to ChangeReplicasTrigger: %v", chng)
	}

	// This is currently executed before the conf change is applied to the Raft
	// node, so we still see ourselves as the leader.
	if r.raftBasicStatusRLocked().RaftState == raftpb.StateLeader {
		r.store.metrics.RangeRaftLeaderRemovals.Inc(1)
	}

	if _, err := r.store.removeInitializedReplicaRaftMuLocked(ctx, r, chng.NextReplicaID(), RemoveOptions{
		// We destroyed the data when the batch committed so don't destroy it again.
		DestroyData: false,
	}); err != nil {
		log.Fatalf(ctx, "failed to remove replica: %v", err)
	}

	// NB: postDestroyRaftMuLocked requires that the batch which removed the data
	// be durably synced to disk, which we have.
	// See replicaAppBatch.ApplyToStateMachine().
	if err := r.postDestroyRaftMuLocked(ctx, r.GetMVCCStats()); err != nil {
		log.Fatalf(ctx, "failed to run Replica postDestroy: %v", err)
	}

	return true
}

// TODO(sumeer): remove method when all truncation is loosely coupled.
func (r *Replica) handleRaftLogDeltaResult(ctx context.Context, delta int64, isDeltaTrusted bool) {
	(*raftTruncatorReplica)(r).setTruncationDeltaAndTrusted(delta, isDeltaTrusted)
}
