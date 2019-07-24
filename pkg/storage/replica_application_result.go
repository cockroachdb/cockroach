// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// isTrivial determines whether the side-effects of a ReplicatedEvalResult are
// "trivial". A result is fundamentally considered "trivial" if it does not have
// side effects which rely on the written state of the replica exactly matching
// the in-memory state of the replica at the corresponding log position.
// Non-trivial commands must be the last entry in a batch so that after
// batch is applied the replica's written and in-memory state correspond to that
// log index.
//
// At the time of writing it is possible that the current conditions are too
// strict but they are certainly sufficient.
func isTrivial(r *storagepb.ReplicatedEvalResult, usingAppliedStateKey bool) (ret bool) {
	// Check if there are any non-trivial State updates.
	if r.State != nil {
		stateWhitelist := *r.State
		// An entry is non-trivial if an upgrade to UsingAppliedState is
		// required. If we're already usingAppliedStateKey or this entry does
		// not imply an upgrade then it is trivial.
		if usingAppliedStateKey {
			stateWhitelist.UsingAppliedStateKey = false
		}
		if stateWhitelist.TruncatedState != nil {
			stateWhitelist.TruncatedState = nil
		}
		if stateWhitelist != (storagepb.ReplicaState{}) {
			return false
		}
	}
	// Set whitelist to the value of r and clear the whitelisted fields.
	// If whitelist is zero-valued after clearing the whitelisted fields then
	// it is trivial.
	whitelist := *r
	whitelist.Delta = enginepb.MVCCStatsDelta{}
	whitelist.Timestamp = hlc.Timestamp{}
	whitelist.DeprecatedDelta = nil
	whitelist.PrevLeaseProposal = nil
	whitelist.State = nil
	whitelist.RaftLogDelta = 0
	whitelist.SuggestedCompactions = nil
	return whitelist.Equal(storagepb.ReplicatedEvalResult{})
}

// stageTrivialReplicatedEvalResult applies the trivial portions of replicatedResult to
// the supplied replicaState and returns whether the change implied an update to
// the replica's truncated state. This function modifies replicaState but does
// not modify replicatedResult in order to give the TestingPostApplyFilter testing knob
// an opportunity to inspect the command's ReplicatedEvalResult.
func stageTrivialReplicatedEvalResult(
	ctx context.Context,
	replicatedResult *storagepb.ReplicatedEvalResult,
	raftAppliedIndex, leaseAppliedIndex uint64,
	replicaState *storagepb.ReplicaState,
) (truncatedStateUpdated bool) {
	deltaStats := replicatedResult.Delta.ToStats()
	replicaState.Stats.Add(deltaStats)
	if raftAppliedIndex != 0 {
		replicaState.RaftAppliedIndex = raftAppliedIndex
	}
	if leaseAppliedIndex != 0 {
		replicaState.LeaseAppliedIndex = leaseAppliedIndex
	}
	haveState := replicatedResult.State != nil
	truncatedStateUpdated = haveState && replicatedResult.State.TruncatedState != nil
	if truncatedStateUpdated {
		replicaState.TruncatedState = replicatedResult.State.TruncatedState
	}
	return truncatedStateUpdated
}

// clearTrivialReplicatedEvalResultFields is used to zero out the fields of a
// ReplicatedEvalResult that have already been consumed when staging the
// corresponding command and applying it to the current batch's view of the
// ReplicaState. This function is called after a batch has been written to the
// storage engine. For trivial commands this function should result in a zero
// value replicatedResult.
func clearTrivialReplicatedEvalResultFields(
	replicatedResult *storagepb.ReplicatedEvalResult, usingAppliedStateKey bool,
) {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the application process
	// (which checks that all fields were handled).
	replicatedResult.IsLeaseRequest = false
	replicatedResult.Timestamp = hlc.Timestamp{}
	replicatedResult.PrevLeaseProposal = nil
	// The state fields cleared here were already applied to the in-memory view of
	// replica state for this batch.
	if haveState := replicatedResult.State != nil; haveState {
		replicatedResult.State.Stats = nil
		replicatedResult.State.TruncatedState = nil

		// Strip the DeprecatedTxnSpanGCThreshold. We don't care about it.
		// TODO(nvanbenschoten): Remove in 20.1.
		replicatedResult.State.DeprecatedTxnSpanGCThreshold = nil

		// If we're already using the AppliedStateKey then there's nothing
		// to do. This flag is idempotent so it's ok that we see this flag
		// multiple times, but we want to make sure it doesn't cause us to
		// perform repeated state assertions, so clear it before the
		// shouldAssert determination.
		// A reader might wonder if using the value of usingAppliedState key from
		// after applying an entire batch is valid to determine whether this command
		// implied a transition, but if it had implied a transition then the batch
		// would not have been considered trivial and the current view of will still
		// be false as complex state transitions are handled after this call.
		if usingAppliedStateKey {
			replicatedResult.State.UsingAppliedStateKey = false
		}
		// ReplicaState.Stats was previously non-nullable which caused nodes to
		// send a zero-value MVCCStats structure. If the proposal was generated by
		// an old node, we'll have decoded that zero-value structure setting
		// ReplicaState.Stats to a non-nil value which would trigger the "unhandled
		// field in ReplicatedEvalResult" assertion to fire if we didn't clear it.
		// TODO(ajwerner): eliminate this case that likely can no longer occur as of
		// at least 19.1.
		if replicatedResult.State.Stats != nil && (*replicatedResult.State.Stats == enginepb.MVCCStats{}) {
			replicatedResult.State.Stats = nil
		}
		if *replicatedResult.State == (storagepb.ReplicaState{}) {
			replicatedResult.State = nil
		}
	}
	replicatedResult.Delta = enginepb.MVCCStatsDelta{}
}

// handleRaftCommandResult is called after the current cmd's batch has been
// committed to the storage engine and the trivial side-effects have been
// applied to the Replica's in-memory state. This method deals with applying
// non-trivial side effects, notifying waiting clients, releasing latches,
// informing raft about applied config changes, and asserting hard state as
// required.
func (r *Replica) handleRaftCommandResult(
	ctx context.Context, cmd *cmdAppCtx, isNonTrivial, usingAppliedStateKey bool,
) (errExpl string, err error) {
	// Set up the local result prior to handling the ReplicatedEvalResult to
	// give testing knobs an opportunity to inspect it.
	r.prepareLocalResult(ctx, cmd)
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEvent(ctx, 2, cmd.localResult.String())
	}

	// Handle the ReplicatedEvalResult, executing any side effects of the last
	// state machine transition.
	//
	// Note that this must happen after committing (the engine.Batch), but
	// before notifying a potentially waiting client.
	clearTrivialReplicatedEvalResultFields(cmd.replicatedResult(), usingAppliedStateKey)
	if isNonTrivial {
		r.handleComplexReplicatedEvalResult(ctx, *cmd.replicatedResult())
	} else if !cmd.replicatedResult().Equal(storagepb.ReplicatedEvalResult{}) {
		log.Fatalf(ctx, "failed to handle all side-effects of ReplicatedEvalResult: %v",
			cmd.replicatedResult())
	}

	// NB: Perform state assertion before acknowledging the client.
	// Some tests (TestRangeStatsInit) assumes that once the store has started
	// and the first range has a lease that there will not be a later hard-state.
	if isNonTrivial {
		// Assert that the on-disk state doesn't diverge from the in-memory
		// state as a result of the side effects.
		r.mu.Lock()
		r.assertStateLocked(ctx, r.store.Engine())
		r.mu.Unlock()
	}

	if cmd.localResult != nil {
		r.handleLocalEvalResult(ctx, *cmd.localResult)
	}
	r.finishRaftCommand(ctx, cmd)
	switch cmd.e.Type {
	case raftpb.EntryNormal:
		if cmd.replicatedResult().ChangeReplicas != nil {
			log.Fatalf(ctx, "unexpected replication change from command %s", &cmd.raftCmd)
		}
	case raftpb.EntryConfChange:
		if cmd.replicatedResult().ChangeReplicas == nil {
			cmd.cc = raftpb.ConfChange{}
		}
		if err := r.withRaftGroup(true, func(raftGroup *raft.RawNode) (bool, error) {
			raftGroup.ApplyConfChange(cmd.cc)
			return true, nil
		}); err != nil {
			const errExpl = "during ApplyConfChange"
			return errExpl, errors.Wrap(err, errExpl)
		}
	}
	return "", nil
}

// handleComplexReplicatedEvalResult carries out the side-effects of non-trivial
// commands. It is run with the raftMu locked. It is illegal to pass a
// replicatedResult that does not imply any side-effects.
func (r *Replica) handleComplexReplicatedEvalResult(
	ctx context.Context, replicatedResult storagepb.ReplicatedEvalResult,
) {

	// Assert that this replicatedResult implies at least one side-effect.
	if replicatedResult.Equal(storagepb.ReplicatedEvalResult{}) {
		log.Fatalf(ctx, "zero-value ReplicatedEvalResult passed to handleComplexReplicatedEvalResult")
	}

	// Process Split or Merge. This needs to happen after stats update because
	// of the ContainsEstimates hack.
	if replicatedResult.Split != nil {
		splitPostApply(
			r.AnnotateCtx(ctx),
			replicatedResult.Split.RHSDelta,
			&replicatedResult.Split.SplitTrigger,
			r,
		)
		replicatedResult.Split = nil
	}

	if replicatedResult.Merge != nil {
		if err := r.store.MergeRange(
			ctx, r, replicatedResult.Merge.LeftDesc, replicatedResult.Merge.RightDesc, replicatedResult.Merge.FreezeStart,
		); err != nil {
			// Our in-memory state has diverged from the on-disk state.
			log.Fatalf(ctx, "failed to update store after merging range: %s", err)
		}
		replicatedResult.Merge = nil
	}

	// Update the remaining ReplicaState.
	if replicatedResult.State != nil {
		if newDesc := replicatedResult.State.Desc; newDesc != nil {
			r.setDesc(ctx, newDesc)
			replicatedResult.State.Desc = nil
		}

		if newLease := replicatedResult.State.Lease; newLease != nil {
			r.leasePostApply(ctx, *newLease, false /* permitJump */)
			replicatedResult.State.Lease = nil
		}

		if newThresh := replicatedResult.State.GCThreshold; newThresh != nil {
			if (*newThresh != hlc.Timestamp{}) {
				r.mu.Lock()
				r.mu.state.GCThreshold = newThresh
				r.mu.Unlock()
			}
			replicatedResult.State.GCThreshold = nil
		}

		if replicatedResult.State.UsingAppliedStateKey {
			r.mu.Lock()
			r.mu.state.UsingAppliedStateKey = true
			r.mu.Unlock()
			replicatedResult.State.UsingAppliedStateKey = false
		}

		if (*replicatedResult.State == storagepb.ReplicaState{}) {
			replicatedResult.State = nil
		}
	}

	if change := replicatedResult.ChangeReplicas; change != nil {
		if change.ChangeType == roachpb.REMOVE_REPLICA &&
			r.store.StoreID() == change.Replica.StoreID {
			// This wants to run as late as possible, maximizing the chances
			// that the other nodes have finished this command as well (since
			// processing the removal from the queue looks up the Range at the
			// lease holder, being too early here turns this into a no-op).
			r.store.replicaGCQueue.AddAsync(ctx, r, replicaGCPriorityRemoved)
		}
		replicatedResult.ChangeReplicas = nil
	}

	if replicatedResult.ComputeChecksum != nil {
		r.computeChecksumPostApply(ctx, *replicatedResult.ComputeChecksum)
		replicatedResult.ComputeChecksum = nil
	}

	if !replicatedResult.Equal(storagepb.ReplicatedEvalResult{}) {
		log.Fatalf(ctx, "unhandled field in ReplicatedEvalResult: %s", pretty.Diff(replicatedResult, storagepb.ReplicatedEvalResult{}))
	}
}

// prepareLocalResult is performed after the command has been committed to the
// engine but before its side-effects have been applied to the Replica's
// in-memory state. This method gives the command an opportunity to interact
// with testing knobs and to set up its local result if it was proposed
// locally.  This is performed prior to handling the command's
// ReplicatedEvalResult because the process of handling the replicated eval
// result will zero-out the struct to ensure that is has properly performed all
// of the implied side-effects.
func (r *Replica) prepareLocalResult(ctx context.Context, cmd *cmdAppCtx) {
	if !cmd.proposedLocally() {
		return
	}

	var pErr *roachpb.Error
	if filter := r.store.cfg.TestingKnobs.TestingPostApplyFilter; filter != nil {
		var newPropRetry int
		newPropRetry, pErr = filter(storagebase.ApplyFilterArgs{
			CmdID:                cmd.idKey,
			ReplicatedEvalResult: *cmd.replicatedResult(),
			StoreID:              r.store.StoreID(),
			RangeID:              r.RangeID,
		})
		if cmd.proposalRetry == 0 {
			cmd.proposalRetry = proposalReevaluationReason(newPropRetry)
		}
		// calling maybeSetCorrupt here is mostly for tests and looks. The
		// interesting errors originate in applyRaftCommandToBatch, and they are
		// already handled above.
		pErr = r.maybeSetCorrupt(ctx, pErr)
	}
	if pErr == nil {
		pErr = cmd.forcedErr
	}

	if cmd.proposalRetry != proposalNoReevaluation && pErr == nil {
		log.Fatalf(ctx, "proposal with nontrivial retry behavior, but no error: %+v", cmd.proposal)
	}
	if pErr != nil {
		// A forced error was set (i.e. we did not apply the proposal,
		// for instance due to its log position) or the Replica is now
		// corrupted.
		switch cmd.proposalRetry {
		case proposalNoReevaluation:
			cmd.response.Err = pErr
		case proposalIllegalLeaseIndex:
			// If we failed to apply at the right lease index, try again with a
			// new one. This is important for pipelined writes, since they don't
			// have a client watching to retry, so a failure to eventually apply
			// the proposal would be a user-visible error.
			pErr = r.tryReproposeWithNewLeaseIndex(ctx, cmd)
			if pErr != nil {
				cmd.response.Err = pErr
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
	cmd.response.Intents = cmd.proposal.Local.DetachIntents()
	cmd.response.EndTxns = cmd.proposal.Local.DetachEndTxns(pErr != nil)
	if pErr == nil {
		cmd.localResult = cmd.proposal.Local
	} else if cmd.localResult != nil {
		log.Fatalf(ctx, "shouldn't have a local result if command processing failed. pErr: %s", pErr)
	}
}

// tryReproposeWithNewLeaseIndex is used by prepareLocalResult to repropose
// commands that have gotten an illegal lease index error, and that we know
// could not have applied while their lease index was valid (that is, we
// observed all applied entries between proposal and the lease index becoming
// invalid, as opposed to skipping some of them by applying a snapshot).
//
// It is not intended for use elsewhere and is only a top-level function so that
// it can avoid the below_raft_protos check. Returns a nil error if the command
// has already been successfully applied or has been reproposed here or by a
// different entry for the same proposal that hit an illegal lease index error.
func (r *Replica) tryReproposeWithNewLeaseIndex(
	ctx context.Context, cmd *cmdAppCtx,
) *roachpb.Error {
	// Note that we don't need to validate anything about the proposal's
	// lease here - if we got this far, we know that everything but the
	// index is valid at this point in the log.
	p := cmd.proposal
	if p.applied || cmd.raftCmd.MaxLeaseIndex != p.command.MaxLeaseIndex {
		// If the command associated with this rejected raft entry already
		// applied then we don't want to repropose it. Doing so could lead
		// to duplicate application of the same proposal.
		//
		// Similarly, if the command associated with this rejected raft
		// entry has a different (larger) MaxLeaseIndex than the one we
		// decoded from the entry itself, the command must have already
		// been reproposed (this can happen if there are multiple copies
		// of the command in the logs; see TestReplicaRefreshMultiple).
		// We must not create multiple copies with multiple lease indexes,
		// so don't repropose it again. This ensures that at any time,
		// there is only up to a single lease index that has a chance of
		// succeeding in the Raft log for a given command.
		return nil
	}
	// Some tests check for this log message in the trace.
	log.VEventf(ctx, 2, "retry: proposalIllegalLeaseIndex")
	maxLeaseIndex, pErr := r.propose(ctx, p)
	if pErr != nil {
		log.Warningf(ctx, "failed to repropose with new lease index: %s", pErr)
		return pErr
	}
	log.VEventf(ctx, 2, "reproposed command %x at maxLeaseIndex=%d", cmd.idKey, maxLeaseIndex)
	return nil
}

// finishRaftCommand is called after a command's side effects have been applied
// in order to acknowledge clients and release latches.
func (r *Replica) finishRaftCommand(ctx context.Context, cmd *cmdAppCtx) {
	// When set to true, recomputes the stats for the LHS and RHS of splits and
	// makes sure that they agree with the state's range stats.
	const expensiveSplitAssertion = false

	if expensiveSplitAssertion && cmd.replicatedResult().Split != nil {
		split := cmd.replicatedResult().Split
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

	if cmd.proposedLocally() {
		cmd.proposal.finishApplication(cmd.response)
	} else if cmd.response.Err != nil {
		log.VEventf(ctx, 1, "applying raft command resulted in error: %s", cmd.response.Err)
	}
}
