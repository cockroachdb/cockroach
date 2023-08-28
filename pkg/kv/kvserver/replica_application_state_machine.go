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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"go.etcd.io/raft/v3"
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

// applyCommittedEntriesStats returns stats about what happened during the
// application of a set of raft entries.
//
// TODO(ajwerner): add metrics to go with these stats.
type applyCommittedEntriesStats struct {
	appBatchStats
	followerStoreWriteBytes kvadmission.FollowerStoreWriteBytes
	numBatchesProcessed     int // TODO(sep-raft-log): numBatches
	stateAssertions         int
	numConfChangeEntries    int
}

// replicaStateMachine implements the apply.StateMachine interface.
//
// The structure coordinates state transitions within the Replica state machine
// due to the application of replicated commands decoded from committed raft
// entries. Commands are applied to the state machine in a multi-stage process
// whereby individual commands are prepared for application relative to the
// current view of ReplicaState and staged in a replicaAppBatch, the batch is
// committed to the Replica's storage engine atomically, and finally the
// side-effects of each command is applied to the Replica's in-memory state.
type replicaStateMachine struct {
	r *Replica
	// batch is returned from NewBatch.
	batch replicaAppBatch
	// ephemeralBatch is returned from NewEphemeralBatch.
	ephemeralBatch ephemeralReplicaAppBatch
	// stats are updated during command application and reset by moveStats.
	applyStats applyCommittedEntriesStats

	// stats backs the currently open replicaAppBatch's view of the State field of
	// kvserverpb.ReplicaState (which is otherwise backed directly by the
	// Replica's in-memory state). This avoids an allocation per Stage() call as
	// Stage() is now alloed to update this memory directly, whereas all other
	// mutations need to copy-on-write.
	stats enginepb.MVCCStats
}

// getStateMachine returns the Replica's apply.StateMachine. The Replica's
// raftMu is held for the entire lifetime of the replicaStateMachine.
func (r *Replica) getStateMachine() *replicaStateMachine {
	sm := &r.raftMu.stateMachine
	sm.r = r
	return sm
}

// TODO(tbg): move this to replica_app_batch.go.
func replicaApplyTestingFilters(
	ctx context.Context, r *Replica, cmd *replicatedCmd, fr kvserverbase.ForcedErrResult,
) kvserverbase.ForcedErrResult {
	// By default, output is input.
	newFR := fr

	// Filters may change that.
	if filter := r.store.cfg.TestingKnobs.TestingApplyCalledTwiceFilter; fr.ForcedError != nil || filter != nil {
		args := kvserverbase.ApplyFilterArgs{
			CmdID:                cmd.ID,
			ReplicatedEvalResult: *cmd.ReplicatedResult(),
			StoreID:              r.store.StoreID(),
			RangeID:              r.RangeID,
			ForcedError:          fr.ForcedError,
		}
		if fr.ForcedError == nil {
			if cmd.IsLocal() {
				args.Req = cmd.proposal.Request
			}
			var newRej int
			newRej, newFR.ForcedError = filter(args)
			if fr.Rejection == 0 {
				newFR.Rejection = kvserverbase.ProposalRejectionType(newRej)
			}
		} else if feFilter := r.store.cfg.TestingKnobs.TestingApplyForcedErrFilter; feFilter != nil {
			var newRej int
			newRej, newFR.ForcedError = feFilter(args)
			if fr.Rejection == 0 {
				newFR.Rejection = kvserverbase.ProposalRejectionType(newRej)
			}
		}
	}
	return newFR
}

// NewEphemeralBatch implements the apply.StateMachine interface.
func (sm *replicaStateMachine) NewEphemeralBatch() apply.EphemeralBatch {
	r := sm.r
	mb := &sm.ephemeralBatch
	mb.r = r
	r.mu.RLock()
	mb.state = r.mu.state
	r.mu.RUnlock()
	return mb
}

// NewBatch implements the apply.StateMachine interface.
func (sm *replicaStateMachine) NewBatch() apply.Batch {
	r := sm.r
	b := &sm.batch
	b.r = r
	b.applyStats = &sm.applyStats
	b.batch = r.store.TODOEngine().NewBatch()
	r.mu.RLock()
	b.state = r.mu.state
	b.state.Stats = &sm.stats
	*b.state.Stats = *r.mu.state.Stats
	b.closedTimestampSetter = r.mu.closedTimestampSetter
	r.mu.RUnlock()
	b.start = timeutil.Now()
	return b
}

// ApplySideEffects implements the apply.StateMachine interface. The method
// handles the third phase of applying a command to the replica state machine.
//
// It is called with commands whose write batches have already been committed
// to the storage engine and whose trivial side-effects have been applied to
// the Replica's in-memory state. This method deals with applying non-trivial
// side effects of commands, such as finalizing splits/merges and informing
// raft about applied config changes.
func (sm *replicaStateMachine) ApplySideEffects(
	ctx context.Context, cmdI apply.CheckedCommand,
) (apply.AppliedCommand, error) {
	cmd := cmdI.(*replicatedCmd)

	// Deal with locking during side-effect handling, which is sometimes
	// associated with complex commands such as splits and merged.
	if unlock := cmd.splitMergeUnlock; unlock != nil {
		defer unlock()
	}

	// Set up the local result prior to handling the ReplicatedEvalResult to
	// give testing knobs an opportunity to inspect it. An injected corruption
	// error will lead to replica removal.
	sm.r.prepareLocalResult(ctx, cmd)
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventf(ctx, 2, "%v", cmd.localResult.String())
	}

	// Handle the ReplicatedEvalResult, executing any side effects of the last
	// state machine transition.
	//
	// Note that this must happen after committing (the engine.Batch), but
	// before notifying a potentially waiting client.
	clearTrivialReplicatedEvalResultFields(cmd.ReplicatedResult())
	if !cmd.IsTrivial() {
		shouldAssert, isRemoved := sm.handleNonTrivialReplicatedEvalResult(ctx, cmd.ReplicatedResult())
		if isRemoved {
			// The proposal must not have been local, because we don't allow a
			// proposing replica to remove itself from the Range.
			cmd.FinishNonLocal(ctx)
			return nil, apply.ErrRemoved
		}
		// NB: Perform state assertion before acknowledging the client.
		// Some tests (TestRangeStatsInit) assumes that once the store has started
		// and the first range has a lease that there will not be a later hard-state.
		if shouldAssert {
			// Assert that the on-disk state doesn't diverge from the in-memory
			// state as a result of the side effects.
			sm.r.mu.RLock()
			// TODO(sep-raft-log): either check only statemachine invariants or
			// pass both engines in.
			sm.r.assertStateRaftMuLockedReplicaMuRLocked(ctx, sm.r.store.TODOEngine())
			sm.r.mu.RUnlock()
			sm.applyStats.stateAssertions++
		}
	} else if res := cmd.ReplicatedResult(); !res.IsZero() {
		log.Fatalf(ctx, "failed to handle all side-effects of ReplicatedEvalResult: %v", res)
	}

	// On ConfChange entries, inform the raft.RawNode.
	if err := sm.maybeApplyConfChange(ctx, cmd); err != nil {
		return nil, errors.Wrapf(err, "unable to apply conf change")
	}

	// Mark the command as applied and return it as an apply.AppliedCommand.
	// NB: Commands which were reproposed at a higher MaxLeaseIndex will not be
	// considered local at this point as their proposal will have been detached
	// in prepareLocalResult().
	if cmd.IsLocal() {
		// Handle the LocalResult.
		if cmd.localResult != nil {
			sm.r.handleReadWriteLocalEvalResult(ctx, *cmd.localResult)
		}

		rejected := cmd.Rejected()
		higherReproposalsExist := cmd.Cmd.MaxLeaseIndex != cmd.proposal.command.MaxLeaseIndex
		if !rejected && higherReproposalsExist {
			log.Fatalf(ctx, "finishing proposal with outstanding reproposal at a higher max lease index")
		}
		if !rejected && cmd.proposal.applied {
			// If the command already applied then we shouldn't be "finishing" its
			// application again because it should only be able to apply successfully
			// once. We expect that when any reproposal for the same command attempts
			// to apply it will be rejected by the below raft lease sequence or lease
			// index check in checkForcedErr.
			log.Fatalf(ctx, "command already applied: %+v; unexpected successful result", cmd)
		}
		// If any reproposals at a higher MaxLeaseIndex exist we know that they will
		// never successfully apply, remove them from the map to avoid future
		// reproposals. If there is no command referencing this proposal at a higher
		// MaxLeaseIndex then it will already have been removed (see
		// shouldRemove in replicaDecoder.retrieveLocalProposals()). It is possible
		// that a later command in this batch referred to this proposal but it must
		// have failed because it carried the same MaxLeaseIndex.
		if higherReproposalsExist {
			sm.r.mu.Lock()
			delete(sm.r.mu.proposals, cmd.ID)
			sm.r.mu.Unlock()
		}
		cmd.proposal.applied = true
	}
	return cmd, nil
}

// handleNonTrivialReplicatedEvalResult carries out the side-effects of
// non-trivial commands. It is run with the raftMu locked. It is illegal
// to pass a replicatedResult that does not imply any side-effects.
func (sm *replicaStateMachine) handleNonTrivialReplicatedEvalResult(
	ctx context.Context, rResult *kvserverpb.ReplicatedEvalResult,
) (shouldAssert, isRemoved bool) {
	// Assert that this replicatedResult implies at least one side-effect.
	if rResult.IsZero() {
		log.Fatalf(ctx, "zero-value ReplicatedEvalResult passed to handleNonTrivialReplicatedEvalResult")
	}

	isRaftLogTruncationDeltaTrusted := true
	if rResult.State != nil {
		if newLease := rResult.State.Lease; newLease != nil {
			sm.r.handleLeaseResult(ctx, newLease, rResult.PriorReadSummary)
			rResult.State.Lease = nil
			rResult.PriorReadSummary = nil
		}

		// This strongly coupled truncation code will be removed in the release
		// following LooselyCoupledRaftLogTruncation.
		if newTruncState := rResult.State.TruncatedState; newTruncState != nil {
			raftLogDelta, expectedFirstIndexWasAccurate := sm.r.handleTruncatedStateResult(
				ctx, newTruncState, rResult.RaftExpectedFirstIndex)
			if !expectedFirstIndexWasAccurate && rResult.RaftExpectedFirstIndex != 0 {
				isRaftLogTruncationDeltaTrusted = false
			}
			rResult.RaftLogDelta += raftLogDelta
			rResult.State.TruncatedState = nil
			rResult.RaftExpectedFirstIndex = 0
		}

		if newVersion := rResult.State.Version; newVersion != nil {
			sm.r.handleVersionResult(ctx, newVersion)
			rResult.State.Version = nil
		}

		if rResult.State.GCHint != nil {
			sm.r.handleGCHintResult(ctx, rResult.State.GCHint)
			rResult.State.GCHint = nil
		}

		if (*rResult.State == kvserverpb.ReplicaState{}) {
			rResult.State = nil
		}
	}

	if rResult.RaftLogDelta != 0 {
		// This code path will be taken exactly when the preceding block has
		// newTruncState != nil. It is needlessly confusing that these two are not
		// in the same place.
		sm.r.handleRaftLogDeltaResult(ctx, rResult.RaftLogDelta, isRaftLogTruncationDeltaTrusted)
		rResult.RaftLogDelta = 0
	}

	// The rest of the actions are "nontrivial" and may have large effects on the
	// in-memory and on-disk ReplicaStates. If any of these actions are present,
	// we want to assert that these two states do not diverge.
	shouldAssert = !rResult.IsZero()
	if !shouldAssert {
		return false, false
	}

	if rResult.Split != nil {
		sm.r.handleSplitResult(ctx, rResult.Split)
		rResult.Split = nil
	}

	if rResult.Merge != nil {
		sm.r.handleMergeResult(ctx, rResult.Merge)
		rResult.Merge = nil
	}

	if rResult.State != nil {
		if newDesc := rResult.State.Desc; newDesc != nil {
			sm.r.handleDescResult(ctx, newDesc)
			rResult.State.Desc = nil
		}

		if (*rResult.State == kvserverpb.ReplicaState{}) {
			rResult.State = nil
		}
	}

	if rResult.ChangeReplicas != nil {
		isRemoved = sm.r.handleChangeReplicasResult(ctx, rResult.ChangeReplicas)
		rResult.ChangeReplicas = nil
	}

	if rResult.ComputeChecksum != nil {
		sm.r.handleComputeChecksumResult(ctx, rResult.ComputeChecksum)
		rResult.ComputeChecksum = nil
	}

	// NB: we intentionally never zero out rResult.IsProbe because probes are
	// implemented by always catching a forced error and thus never show up in
	// this method, which the next line will assert for us.
	if !rResult.IsZero() {
		log.Fatalf(ctx, "unhandled field in ReplicatedEvalResult: %s", pretty.Diff(rResult, &kvserverpb.ReplicatedEvalResult{}))
	}
	return true, isRemoved
}

func (sm *replicaStateMachine) maybeApplyConfChange(ctx context.Context, cmd *replicatedCmd) error {
	cc := cmd.ConfChange()
	if cc == nil {
		return nil
	}
	sm.applyStats.numConfChangeEntries++
	if cmd.Rejected() {
		// The command was rejected. There is no need to report a ConfChange
		// to raft.
		return nil
	}
	return sm.r.withRaftGroup(func(rn *raft.RawNode) (bool, error) {
		// NB: `etcd/raft` configuration changes diverge from the official Raft way
		// in that a configuration change becomes active when the corresponding log
		// entry is applied (rather than appended). This ultimately enables the way
		// we do things where the state machine's view of the range descriptor always
		// dictates the active replication config but it is much trickier to prove
		// correct. See:
		//
		// https://github.com/etcd-io/etcd/issues/7625#issuecomment-489232411
		//
		// INVARIANT: a leader will not append a config change to its logs when it
		// hasn't applied all previous config changes in its logs.
		//
		// INVARIANT: a node will not campaign until it has applied any
		// configuration changes with indexes less than or equal to its committed
		// index.
		//
		// INVARIANT: appending a config change to the log (at leader or follower)
		// implies that any previous config changes are durably known to be
		// committed. That is, a commit index is persisted (and synced) that
		// encompasses any earlier config changes before a new config change is
		// appended[1].
		//
		// Together, these invariants ensure that a follower that is behind by
		// multiple configuration changes will be using one of the two most recent
		// configuration changes "by the time it matters", which is what is
		// required for correctness (configuration changes are sequenced so that
		// neighboring configurations are mutually compatible, i.e. don't cause
		// split brain). To see this, consider a follower that is behind by
		// multiple configuration changes. This is fine unless this follower
		// becomes the leader (as it would then make quorum determinations based
		// on its active config). To become leader, it needs to campaign, and
		// thanks to the second invariant, it will only do so once it has applied
		// all the configuration changes in its committed log. If it is to win the
		// election, it will also have all committed configuration changes in its
		// log (though not necessarily knowing that they are all committed). But
		// the third invariant implies that when the follower received the most
		// recent configuration change into its log, the one preceding it was
		// durably marked as committed on the follower. In summary, we now know
		// that it will apply all the way up to and including the second most
		// recent configuration change, which is compatible with the most recent
		// one.
		//
		// [1]: this rests on shaky and, in particular, untested foundations in
		// etcd/raft and our syncing behavior. The argument goes as follows:
		// because the leader will have at most one config change in flight at a
		// given time, it will definitely wait until the previous config change is
		// committed until accepting the next one. `etcd/raft` will always attach
		// the optimal commit index to appends to followers, so each config change
		// will mark the previous one as committed upon receipt, since we sync on
		// append (as we have to) we make that HardState.Commit durable. Finally,
		// when a follower is catching up on larger chunks of the historical log,
		// it will receive batches of entries together with a committed index
		// encompassing the entire batch, again making sure that these batches are
		// durably committed upon receipt.
		rn.ApplyConfChange(cc)
		return true, nil
	})
}

func (sm *replicaStateMachine) moveStats() applyCommittedEntriesStats {
	stats := sm.applyStats
	sm.applyStats = applyCommittedEntriesStats{}
	return stats
}

// closedTimestampSetterInfo contains information about the command that last
// bumped the closed timestamp.
type closedTimestampSetterInfo struct {
	// lease represents the lease under which the command is being applied.
	lease *roachpb.Lease
	// leaseIdx is the LAI of the command.
	leaseIdx kvpb.LeaseAppliedIndex
	// leaseReq is set if the request that generated this command was a
	// RequestLeaseRequest. This is only ever set on the leaseholder replica since
	// only the leaseholder has information about the request corresponding to a
	// command.
	// NOTE: We only keep track of lease requests because keeping track of all
	// requests would be too expensive: cloning the request is expensive and also
	// requests can be large in memory.
	leaseReq *kvpb.RequestLeaseRequest
	// split and merge are set if the request was an EndTxn with the respective
	// commit trigger set.
	split, merge bool
}

// record saves information about the command that update's the replica's closed
// timestamp.
func (s *closedTimestampSetterInfo) record(cmd *replicatedCmd, lease *roachpb.Lease) {
	*s = closedTimestampSetterInfo{}
	s.leaseIdx = cmd.LeaseIndex
	s.lease = lease
	if !cmd.IsLocal() {
		return
	}
	req := cmd.proposal.Request
	et, ok := req.GetArg(kvpb.EndTxn)
	if ok {
		endTxn := et.(*kvpb.EndTxnRequest)
		if trig := endTxn.InternalCommitTrigger; trig != nil {
			if trig.SplitTrigger != nil {
				s.split = true
			} else if trig.MergeTrigger != nil {
				s.merge = true
			}
		}
	} else if req.IsSingleRequestLeaseRequest() {
		// Make a deep copy since we're not allowed to hold on to request
		// memory.
		lr, _ := req.GetArg(kvpb.RequestLease)
		s.leaseReq = protoutil.Clone(lr).(*kvpb.RequestLeaseRequest)
	}
}
