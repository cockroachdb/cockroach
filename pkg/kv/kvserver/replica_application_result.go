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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
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

	var pErr *roachpb.Error
	if filter := r.store.cfg.TestingKnobs.TestingPostApplyFilter; filter != nil {
		var newPropRetry int
		newPropRetry, pErr = filter(kvserverbase.ApplyFilterArgs{
			CmdID:                cmd.ID,
			ReplicatedEvalResult: *cmd.ReplicatedResult(),
			StoreID:              r.store.StoreID(),
			RangeID:              r.RangeID,
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
		switch cmd.Rejection {
		case kvserverbase.ProposalRejectionPermanent:
			cmd.response.Err = pErr
		case kvserverbase.ProposalRejectionIllegalLeaseIndex:
			// If we failed to apply at the right lease index, try again with a
			// new one. This is important for pipelined writes, since they don't
			// have a client watching to retry, so a failure to eventually apply
			// the proposal would be a user-visible error.
			pErr = tryReproposeWithNewLeaseIndex(ctx, cmd, (*replicaReproposer)(r))

			if pErr != nil {
				// An error from tryReproposeWithNewLeaseIndex implies that the current
				// entry is not superseded (i.e. we don't have a reproposal at a higher
				// MaxLeaseIndex in the log).
				//
				// This implies that any additional copies of the command (which may be present
				// in the log ahead of the current entry) will also fail.
				//
				// It is thus safe to signal the error back to the client, which is also
				// the only sensible choice at this point.
				//
				// We also know that the proposal is not in the proposals map, since the
				// command is local and wasn't superseded, which is the condition in
				// retrieveLocalProposals for removing from the map. So we're not leaking
				// a map entry here, which we assert against below (and which has coverage,
				// at time of writing, through TestReplicaReproposalWithNewLeaseIndexError).
				log.Warningf(ctx, "failed to repropose with new lease index: %s", pErr)
				cmd.response.Err = pErr

				r.mu.RLock()
				_, inMap := r.mu.proposals[cmd.ID]
				r.mu.RUnlock()

				if inMap {
					log.Fatalf(ctx, "failed reproposal unexpectedly in proposals map: %+v", cmd)
				}
			} else {
				// Unbind the entry's local proposal because we just succeeded
				// in reproposing it and we don't want to acknowledge the client
				// yet.
				cmd.proposal = nil
				return
			}
		default:
			panic("unexpected")
		}
	} else if cmd.proposal.Local.Reply != nil {
		cmd.response.Reply = cmd.proposal.Local.Reply
	} else {
		log.Fatalf(ctx, "proposal must return either a reply or an error: %+v", cmd.proposal)
	}
	cmd.response.EncounteredIntents = cmd.proposal.Local.DetachEncounteredIntents()
	cmd.response.EndTxns = cmd.proposal.Local.DetachEndTxns(pErr != nil)
	if pErr == nil {
		cmd.localResult = cmd.proposal.Local
	} else if cmd.localResult != nil {
		log.Fatalf(ctx, "shouldn't have a local result if command processing failed. pErr: %s", pErr)
	}
}

// reproposer is used by tryReproposeWithNewLeaseIndex.
type reproposer interface {
	trackEvaluatingRequest(context.Context, hlc.Timestamp) (hlc.Timestamp, TrackedRequestToken)
	propose(context.Context, *ProposalData, TrackedRequestToken) *roachpb.Error
	newNotLeaseHolderError(string) *roachpb.NotLeaseHolderError
}

type replicaReproposer Replica

var _ reproposer = (*replicaReproposer)(nil)

func (r *replicaReproposer) trackEvaluatingRequest(
	ctx context.Context, wts hlc.Timestamp,
) (hlc.Timestamp, TrackedRequestToken) {
	// NB: must not hold r.mu here, the propBuf acquires it itself.
	return r.mu.proposalBuf.TrackEvaluatingRequest(ctx, wts)
}

func (r *replicaReproposer) propose(
	ctx context.Context, p *ProposalData, tok TrackedRequestToken,
) *roachpb.Error {
	return (*Replica)(r).propose(ctx, p, tok)
}

func (r *replicaReproposer) newNotLeaseHolderError(msg string) *roachpb.NotLeaseHolderError {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return roachpb.NewNotLeaseHolderError(
		*r.mu.state.Lease,
		r.store.StoreID(),
		r.mu.state.Desc,
		msg,
	)
}

// tryReproposeWithNewLeaseIndex is used by prepareLocalResult to repropose
// commands that have gotten an illegal lease index error, and that we know
// could not have applied while their lease index was valid (that is, we
// observed all applied entries between proposal and the lease index becoming
// invalid, as opposed to skipping some of them by applying a snapshot).
//
// Returns a nil error if the command has already been successfully applied or
// has been reproposed here or by a different entry for the same proposal that
// hit an illegal lease index error.
//
// If this returns a nil error once, it will return a nil error for future calls
// as well, assuming that trackEvaluatingRequest returns monotonically increasing
// timestamps across subsequent calls.
func tryReproposeWithNewLeaseIndex(
	ctx context.Context, cmd *replicatedCmd, r reproposer,
) *roachpb.Error {
	// Note that we don't need to validate anything about the proposal's
	// lease here - if we got this far, we know that everything but the
	// index is valid at this point in the log.
	p := cmd.proposal
	if p.applied || p.Supersedes(cmd.Cmd.MaxLeaseIndex) {
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
		// tryReproposeWithNewLeaseIndex previously (this can happen if there are
		// multiple copies of the command in the logs; see
		// TestReplicaRefreshMultiple). We must not create multiple copies with
		// multiple lease indexes, so don't repropose it again. This ensures that at
		// any time, there is only up to a single lease index that has a chance of
		// succeeding in the Raft log for a given command.
		return nil
	}

	// We need to track the request again in order to protect its timestamp until
	// it gets reproposed.
	// TODO(andrei): Only track if the request consults the ts cache. Some
	// requests (e.g. EndTxn) don't care about closed timestamps.
	minTS, tok := r.trackEvaluatingRequest(ctx, p.Request.WriteTimestamp())
	defer tok.DoneIfNotMoved(ctx)

	// NB: p.Request.Timestamp reflects the action of ba.SetActiveTimestamp.
	if p.Request.AppliesTimestampCache() && p.Request.WriteTimestamp().LessEq(minTS) {
		// The tracker wants us to forward the request timestamp, but we can't
		// do that without re-evaluating, so give up. The error returned here
		// will go back to DistSender, so send something it can digest.
		return roachpb.NewError(r.newNotLeaseHolderError("reproposal failed due to closed timestamp"))
	}
	// Some tests check for this log message in the trace.
	log.VEventf(ctx, 2, "retry: proposalIllegalLeaseIndex")

	// Reset the command for reproposal.
	prevMaxLeaseIndex := p.command.MaxLeaseIndex
	prevEncodedCommand := p.encodedCommand
	p.command.MaxLeaseIndex = 0
	p.encodedCommand = nil
	pErr := r.propose(ctx, p, tok.Move(ctx))
	if pErr != nil {
		// On error, reset the fields we zeroed out to their old value.
		// This ensures that the proposal doesn't count as Superseded
		// now.
		p.command.MaxLeaseIndex = prevMaxLeaseIndex
		p.encodedCommand = prevEncodedCommand
		return pErr
	}
	log.VEventf(ctx, 2, "reproposed command %x", cmd.ID)
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
		r.mu.state.Lease, /* prevLease */
		lease,            /* newLease */
		priorReadSum,
		assertNoLeaseJump)
}

func (r *Replica) handleTruncatedStateResult(
	ctx context.Context, t *roachpb.RaftTruncatedState, expectedFirstIndexPreTruncation uint64,
) (raftLogDelta int64, expectedFirstIndexWasAccurate bool) {
	r.mu.Lock()
	expectedFirstIndexWasAccurate =
		r.mu.state.TruncatedState.Index+1 == expectedFirstIndexPreTruncation
	r.mu.state.TruncatedState = t
	r.mu.Unlock()

	// Clear any entries in the Raft log entry cache for this range up
	// to and including the most recently truncated index.
	r.store.raftEntryCache.Clear(r.RangeID, t.Index+1)

	// Truncate the sideloaded storage. Note that this is safe only if the new truncated state
	// is durably on disk (i.e.) synced. This is true at the time of writing but unfortunately
	// could rot.
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
	return -size, expectedFirstIndexWasAccurate
}

func (r *Replica) handleGCThresholdResult(ctx context.Context, thresh *hlc.Timestamp) {
	if thresh.IsEmpty() {
		return
	}
	r.mu.Lock()
	r.mu.state.GCThreshold = thresh
	r.mu.Unlock()
}

func (r *Replica) handleGCHintResult(ctx context.Context, hint *roachpb.GCHint) {
	r.mu.Lock()
	r.mu.state.GCHint = hint
	r.mu.Unlock()
}

func (r *Replica) handleVersionResult(ctx context.Context, version *roachpb.Version) {
	if (*version == roachpb.Version{}) {
		log.Fatal(ctx, "not expecting empty replica version downstream of raft")
	}
	r.mu.Lock()
	r.mu.state.Version = version
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
