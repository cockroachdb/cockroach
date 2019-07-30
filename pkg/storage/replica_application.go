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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
)

// handleCommittedEntriesStats returns stats about what happened during the
// application of a set of raft entries.
//
// TODO(ajwerner): add metrics to go with these stats.
type handleCommittedEntriesStats struct {
	batchesProcessed int
	entriesProcessed int
	stateAssertions  int
	numEmptyEntries  int
}

// handleCommittedEntriesRaftMuLocked deals with the complexities involved in
// moving the Replica's replicated state machine forward given committed raft
// entries. All changes to r.mu.state occur downstream of this call.
//
// The stats return value reflects the number of entries which are processed,
// the number of batches used to process them, the number of entries which
// contained no data (added by etcd raft), and the number of entries which
// required asserting the on-disk state.
//
// Errors returned from this method are fatal! The errExpl is provided as a
// non-sensitive cue of where the error occurred.
// TODO(ajwerner): replace errExpl with proper error composition using the new
// errors library.
//
// At a high level, this method receives committed entries which each contains
// the evaluation of a batch (at its heart a WriteBatch, to be applied to the
// underlying storage engine), which it decodes into batches of entries which
// are safe to apply atomically together.
//
// The pseudocode looks like:
//
//   while entries:
//     for each entry in entries:
//        decode front into temp struct
//        if entry is non-trivial and batch is not empty:
//          break
//        add decoded command to batch
//        pop front from entries
//        if entry is non-trivial:
//          break
//     prepare local commands
//     for each entry in batch:
//       check if failed
//       if not:
//         stage in batch
//     commit batch to storage engine
//     update Replica.state
//     for each entry in batch:
//         apply side-effects, ack client, release latches
//
// The processing of committed entries proceeds in 4 stages: decoding,
// local preparation, staging, and application. Commands may be applied together
// so long as their implied state change is "trivial" (see isTrivial). Once
// decoding has discovered a batch boundary, the commands are prepared by
// reading the current replica state from underneath the Replica.mu and
// determining whether any of the commands were proposed locally. Next each
// command is written to the engine.Batch and has the "trivial" component of
// its ReplicatedEvalResult applied to the batch's view of the ReplicaState.
// Finally the batch is written to the storage engine, its side effects on
// the Replica's state are applied, and the clients acked with the respective
// results.
func (r *Replica) handleCommittedEntriesRaftMuLocked(
	ctx context.Context, committedEntries []raftpb.Entry,
) (stats handleCommittedEntriesStats, errExpl string, err error) {
	var (
		haveNonTrivialEntry bool
		toProcess           = committedEntries
		b                   = getCmdAppBatch()
	)
	defer releaseCmdAppBatch(b)
	for len(toProcess) > 0 {
		if haveNonTrivialEntry {
			// If the previous call to b.decode() informed us that it left a
			// non-trivial entry in decodeBuf, use it.
			haveNonTrivialEntry = false
			b.add(&toProcess[0], b.decodeBuf)
			toProcess = toProcess[1:]
		} else {
			// Decode zero or more trivial entries into b.
			var numEmptyEntries int
			haveNonTrivialEntry, numEmptyEntries, toProcess, errExpl, err =
				b.decode(ctx, toProcess, &b.decodeBuf)
			if err != nil {
				return stats, errExpl, err
			}
			// If no trivial entries were decoded go back around and process the
			// non-trivial entry.
			if b.cmdBuf.len == 0 {
				continue
			}
			stats.numEmptyEntries += numEmptyEntries
		}
		r.retrieveLocalProposals(ctx, b)
		b.batch = r.store.engine.NewBatch()
		// Stage each of the commands which will write them into the newly created
		// engine.Batch and update b's view of the replicaState.
		var it cmdAppCtxBufIterator
		for ok := it.init(&b.cmdBuf); ok; ok = it.next() {
			cmd := it.cur()
			r.stageRaftCommand(cmd.ctx, cmd, b.batch, &b.replicaState,
				it.isLast() /* writeAppliedState */)
			// We permit trivial commands to update the truncated state but we need to
			// track whether that happens so that the side-effects of truncation occur
			// after the batch is committed to storage.
			updatedTruncatedState := stageTrivialReplicatedEvalResult(cmd.ctx,
				cmd.replicatedResult(), cmd.e.Index, cmd.leaseIndex, &b.replicaState)
			b.updatedTruncatedState = b.updatedTruncatedState || updatedTruncatedState
		}
		if errExpl, err = r.applyCmdAppBatch(ctx, b, &stats); err != nil {
			return stats, errExpl, err
		}
	}
	return stats, "", nil
}

// retrieveLocalProposals populates the proposal and ctx fields for each of the
// commands in b.
func (r *Replica) retrieveLocalProposals(ctx context.Context, b *cmdAppBatch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b.replicaState = r.mu.state
	// Copy stats as it gets updated in-place in applyRaftCommandToBatch.
	b.replicaState.Stats = &b.stats
	*b.replicaState.Stats = *r.mu.state.Stats
	// Assign all the local proposals first then delete all of them from the map
	// in a second pass. This ensures that we retrieve all proposals correctly
	// even if the batch has multiple entries for the same proposal, in which
	// case the proposal was reproposed (either under its original or a new
	// MaxLeaseIndex) which we handle in a second pass below.
	var anyLocal bool
	var it cmdAppCtxBufIterator
	for ok := it.init(&b.cmdBuf); ok; ok = it.next() {
		cmd := it.cur()
		cmd.proposal = r.mu.proposals[cmd.idKey]
		if cmd.proposedLocally() {
			// We initiated this command, so use the caller-supplied context.
			cmd.ctx = cmd.proposal.ctx
			anyLocal = true
		} else {
			cmd.ctx = ctx
		}
	}
	if !anyLocal && r.mu.proposalQuota == nil {
		// Fast-path.
		return
	}
	for ok := it.init(&b.cmdBuf); ok; ok = it.next() {
		cmd := it.cur()
		toRelease := int64(0)
		shouldRemove := cmd.proposedLocally() &&
			// If this entry does not have the most up-to-date view of the
			// corresponding proposal's maximum lease index then the proposal
			// must have been reproposed with a higher lease index. (see
			// tryReproposeWithNewLeaseIndex). In that case, there's a newer
			// version of the proposal in the pipeline, so don't remove the
			// proposal from the map. We expect this entry to be rejected by
			// checkForcedErr.
			cmd.raftCmd.MaxLeaseIndex == cmd.proposal.command.MaxLeaseIndex
		if shouldRemove {
			// Delete the proposal from the proposals map. There may be reproposals
			// of the proposal in the pipeline, but those will all have the same max
			// lease index, meaning that they will all be rejected after this entry
			// applies (successfully or otherwise). If tryReproposeWithNewLeaseIndex
			// picks up the proposal on failure, it will re-add the proposal to the
			// proposal map, but this won't affect anything in this cmdAppBatch.
			//
			// While here, add the proposal's quota size to the quota release queue.
			// We check the proposal map again first to avoid double free-ing quota
			// when reproposals from the same proposal end up in the same entry
			// application batch.
			delete(r.mu.proposals, cmd.idKey)
			toRelease = cmd.proposal.quotaSize
		}
		// At this point we're not guaranteed to have proposalQuota initialized,
		// the same is true for quotaReleaseQueues. Only queue the proposal's
		// quota for release if the proposalQuota is initialized
		if r.mu.proposalQuota != nil {
			r.mu.quotaReleaseQueue = append(r.mu.quotaReleaseQueue, toRelease)
		}
	}
}

// stageRaftCommand handles the first phase of applying a command to the
// replica state machine.
//
// The proposal also contains auxiliary data which needs to be verified in order
// to decide whether the proposal should be applied: the command's MaxLeaseIndex
// must move the state machine's LeaseAppliedIndex forward, and the proposer's
// lease (or rather its sequence number) must match that of the state machine,
// and lastly the GCThreshold is validated. If any of the checks fail, the
// proposal's content is wiped and we apply an empty log entry instead. If an
// error occurs and the command was proposed locally, the error will be
// communicated to the waiting proposer. The two typical cases in which errors
// occur are lease mismatch (in which case the caller tries to send the command
// to the actual leaseholder) and violation of the LeaseAppliedIndex (in which
// case the proposal is retried if it was proposed locally).
//
// Assuming all checks were passed, the command is applied to the batch,
// which is done by the aptly named applyRaftCommandToBatch.
//
// For trivial proposals this is the whole story, but some commands trigger
// additional code in this method in this method via a side effect (in the
// proposal's ReplicatedEvalResult or, for local proposals,
// LocalEvalResult). These might, for example, trigger an update of the
// Replica's in-memory state to match updates to the on-disk state, or pass
// intents to the intent resolver. Some commands don't fit this simple schema
// and need to hook deeper into the code. Notably splits and merges need to
// acquire locks on their right-hand side Replicas and may need to add data to
// the WriteBatch before it is applied; similarly, changes to the disk layout of
// internal state typically require a migration which shows up here. Any of this
// logic however is deferred until after the batch has been written to the
// storage engine.
func (r *Replica) stageRaftCommand(
	ctx context.Context,
	cmd *cmdAppCtx,
	batch engine.Batch,
	replicaState *storagepb.ReplicaState,
	writeAppliedState bool,
) {
	if cmd.e.Index == 0 {
		log.Fatalf(ctx, "processRaftCommand requires a non-zero index")
	}
	if log.V(4) {
		log.Infof(ctx, "processing command %x: maxLeaseIndex=%d",
			cmd.idKey, cmd.raftCmd.MaxLeaseIndex)
	}

	var ts hlc.Timestamp
	if cmd.idKey != "" {
		ts = cmd.replicatedResult().Timestamp
	}

	cmd.leaseIndex, cmd.proposalRetry, cmd.forcedErr = checkForcedErr(ctx,
		cmd.idKey, cmd.raftCmd, cmd.proposal, cmd.proposedLocally(), replicaState)
	if cmd.forcedErr == nil {
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
		cmd.forcedErr = roachpb.NewError(r.requestCanProceed(roachpb.RSpan{}, ts))
	}
	if filter := r.store.cfg.TestingKnobs.TestingApplyFilter; cmd.forcedErr == nil && filter != nil {
		var newPropRetry int
		newPropRetry, cmd.forcedErr = filter(storagebase.ApplyFilterArgs{
			CmdID:                cmd.idKey,
			ReplicatedEvalResult: *cmd.replicatedResult(),
			StoreID:              r.store.StoreID(),
			RangeID:              r.RangeID,
		})
		if cmd.proposalRetry == 0 {
			cmd.proposalRetry = proposalReevaluationReason(newPropRetry)
		}
	}
	if cmd.forcedErr != nil {
		// Apply an empty entry.
		*cmd.replicatedResult() = storagepb.ReplicatedEvalResult{}
		cmd.raftCmd.WriteBatch = nil
		cmd.raftCmd.LogicalOpLog = nil
		log.VEventf(ctx, 1, "applying command with forced error: %s", cmd.forcedErr)
	} else {
		log.Event(ctx, "applying command")
	}

	// Acquire the split or merge lock, if necessary. If a split or merge
	// command was rejected with a below-Raft forced error then its replicated
	// result was just cleared and this will be a no-op.
	if splitMergeUnlock, err := r.maybeAcquireSplitMergeLock(ctx, cmd.raftCmd); err != nil {
		log.Fatalf(ctx, "unable to acquire split lock: %s", err)
	} else if splitMergeUnlock != nil {
		// Set the splitMergeUnlock on the cmdAppCtx to be called after the batch
		// has been applied (see applyBatch).
		cmd.splitMergeUnlock = splitMergeUnlock
	}

	// Update the node clock with the serviced request. This maintains
	// a high water mark for all ops serviced, so that received ops without
	// a timestamp specified are guaranteed one higher than any op already
	// executed for overlapping keys.
	// TODO(ajwerner): coalesce the clock update per batch.
	r.store.Clock().Update(ts)

	// If the command was using the deprecated version of the MVCCStats proto,
	// migrate it to the new version and clear out the field.
	if deprecatedDelta := cmd.replicatedResult().DeprecatedDelta; deprecatedDelta != nil {
		if cmd.replicatedResult().Delta != (enginepb.MVCCStatsDelta{}) {
			log.Fatalf(ctx, "stats delta not empty but deprecated delta provided: %+v", cmd)
		}
		cmd.replicatedResult().Delta = deprecatedDelta.ToStatsDelta()
		cmd.replicatedResult().DeprecatedDelta = nil
	}

	// Apply the Raft command to the batch's accumulated state. This may also
	// have the effect of mutating cmd.replicatedResult().
	// applyRaftCommandToBatch will return "expected" errors, but may also indicate
	// replica corruption (as of now, signaled by a replicaCorruptionError).
	// We feed its return through maybeSetCorrupt to act when that happens.
	err := r.applyRaftCommandToBatch(cmd.ctx, cmd, replicaState, batch, writeAppliedState)
	if err != nil {
		// applyRaftCommandToBatch returned an error, which usually indicates
		// either a serious logic bug in CockroachDB or a disk
		// corruption/out-of-space issue. Make sure that these fail with
		// descriptive message so that we can differentiate the root causes.
		log.Errorf(ctx, "unable to update the state machine: %+v", err)
		// Report the fatal error separately and only with the error, as that
		// triggers an optimization for which we directly report the error to
		// sentry (which in turn allows sentry to distinguish different error
		// types).
		log.Fatal(ctx, err)
	}

	// AddSSTable ingestions run before the actual batch gets written to the
	// storage engine. This makes sure that when the Raft command is applied,
	// the ingestion has definitely succeeded. Note that we have taken
	// precautions during command evaluation to avoid having mutations in the
	// WriteBatch that affect the SSTable. Not doing so could result in order
	// reversal (and missing values) here.
	//
	// NB: any command which has an AddSSTable is non-trivial and will be
	// applied in its own batch so it's not possible that any other commands
	// which precede this command can shadow writes from this SSTable.
	if cmd.replicatedResult().AddSSTable != nil {
		copied := addSSTablePreApply(
			ctx,
			r.store.cfg.Settings,
			r.store.engine,
			r.raftMu.sideloaded,
			cmd.e.Term,
			cmd.e.Index,
			*cmd.replicatedResult().AddSSTable,
			r.store.limiters.BulkIOWriteRate,
		)
		r.store.metrics.AddSSTableApplications.Inc(1)
		if copied {
			r.store.metrics.AddSSTableApplicationCopies.Inc(1)
		}
		cmd.replicatedResult().AddSSTable = nil
	}

	if cmd.replicatedResult().Split != nil {
		// Splits require a new HardState to be written to the new RHS
		// range (and this needs to be atomic with the main batch). This
		// cannot be constructed at evaluation time because it differs
		// on each replica (votes may have already been cast on the
		// uninitialized replica). Write this new hardstate to the batch too.
		// See https://github.com/cockroachdb/cockroach/issues/20629
		splitPreApply(ctx, batch, cmd.replicatedResult().Split.SplitTrigger)
	}

	if merge := cmd.replicatedResult().Merge; merge != nil {
		// Merges require the subsumed range to be atomically deleted when the
		// merge transaction commits.
		rhsRepl, err := r.store.GetReplica(merge.RightDesc.RangeID)
		if err != nil {
			log.Fatal(ctx, err)
		}
		const destroyData = false
		err = rhsRepl.preDestroyRaftMuLocked(ctx, batch, batch, merge.RightDesc.NextReplicaID, destroyData)
		if err != nil {
			log.Fatal(ctx, err)
		}
	}

	// Provide the command's corresponding logical operations to the Replica's
	// rangefeed. Only do so if the WriteBatch is non-nil, in which case the
	// rangefeed requires there to be a corresponding logical operation log or
	// it will shut down with an error. If the WriteBatch is nil then we expect
	// the logical operation log to also be nil. We don't want to trigger a
	// shutdown of the rangefeed in that situation, so we don't pass anything to
	// the rangefed. If no rangefeed is running at all, this call will be a noop.
	if cmd.raftCmd.WriteBatch != nil {
		r.handleLogicalOpLogRaftMuLocked(ctx, cmd.raftCmd.LogicalOpLog, batch)
	} else if cmd.raftCmd.LogicalOpLog != nil {
		log.Fatalf(ctx, "non-nil logical op log with nil write batch: %v", cmd.raftCmd)
	}
}

func checkForcedErr(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	raftCmd storagepb.RaftCommand,
	proposal *ProposalData,
	proposedLocally bool,
	replicaState *storagepb.ReplicaState,
) (uint64, proposalReevaluationReason, *roachpb.Error) {
	leaseIndex := replicaState.LeaseAppliedIndex
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
		leaseMismatch = !raftCmd.DeprecatedProposerLease.Equivalent(*replicaState.Lease)
	} else {
		leaseMismatch = raftCmd.ProposerLeaseSequence != replicaState.Lease.Sequence
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
			if replicaState.Lease.Sequence == requestedLease.Sequence {
				// It is only possible for this to fail when expiration-based
				// lease extensions are proposed concurrently.
				leaseMismatch = !replicaState.Lease.Equivalent(requestedLease)
			}

			// This is a check to see if the lease we proposed this lease request against is the same
			// lease that we're trying to update. We need to check proposal timestamps because
			// extensions don't increment sequence numbers. Without this check a lease could
			// be extended and then another lease proposed against the original lease would
			// be applied over the extension.
			if raftCmd.ReplicatedEvalResult.PrevLeaseProposal != nil &&
				(*raftCmd.ReplicatedEvalResult.PrevLeaseProposal != *replicaState.Lease.ProposedTS) {
				leaseMismatch = true
			}
		}
	}
	if leaseMismatch {
		log.VEventf(
			ctx, 1,
			"command proposed from replica %+v with lease #%d incompatible to %v",
			raftCmd.ProposerReplica, raftCmd.ProposerLeaseSequence, *replicaState.Lease,
		)
		if isLeaseRequest {
			// For lease requests we return a special error that
			// redirectOnOrAcquireLease() understands. Note that these
			// requests don't go through the DistSender.
			return leaseIndex, proposalNoReevaluation, roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  *replicaState.Lease,
				Requested: requestedLease,
				Message:   "proposed under invalid lease",
			})
		}
		// We return a NotLeaseHolderError so that the DistSender retries.
		nlhe := newNotLeaseHolderError(
			replicaState.Lease, raftCmd.ProposerReplica.StoreID, replicaState.Desc)
		nlhe.CustomMsg = fmt.Sprintf(
			"stale proposal: command was proposed under lease #%d but is being applied "+
				"under lease: %s", raftCmd.ProposerLeaseSequence, replicaState.Lease)
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
		if _, ok := replicaState.Desc.GetReplicaDescriptor(requestedLease.Replica.StoreID); !ok {
			return leaseIndex, proposalNoReevaluation, roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  *replicaState.Lease,
				Requested: requestedLease,
				Message:   "replica not part of range",
			})
		}
	} else if replicaState.LeaseAppliedIndex < raftCmd.MaxLeaseIndex {
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

// applyRaftCommandToBatch applies a raft command from the replicated log to the
// current batch's view of the underlying state machine. When the state machine
// cannot be updated, an error (which is fatal!) is returned and must be treated
// that way by the caller.
func (r *Replica) applyRaftCommandToBatch(
	ctx context.Context,
	cmd *cmdAppCtx,
	replicaState *storagepb.ReplicaState,
	batch engine.Batch,
	writeAppliedState bool,
) error {
	writeBatch := cmd.raftCmd.WriteBatch
	if writeBatch != nil && len(writeBatch.Data) > 0 {
		mutationCount, err := engine.RocksDBBatchCount(writeBatch.Data)
		if err != nil {
			log.Errorf(ctx, "unable to read header of committed WriteBatch: %+v", err)
		} else {
			cmd.mutationCount = mutationCount
		}
	}

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
	if cmd.replicatedResult().Split != nil {
		replicaState.Stats.ContainsEstimates = false
	}

	if cmd.e.Index != replicaState.RaftAppliedIndex+1 {
		// If we have an out of order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return
		// a corruption error.
		return errors.Errorf("applied index jumped from %d to %d",
			replicaState.RaftAppliedIndex, cmd.e.Index)
	}

	if writeBatch != nil {
		if err := batch.ApplyBatchRepr(writeBatch.Data, false); err != nil {
			return errors.Wrap(err, "unable to apply WriteBatch")
		}
	}

	// The only remaining use of the batch is for range-local keys which we know
	// have not been previously written within this batch.
	//
	// TODO(ajwerner): explore the costs and benefits of this use of Distinct().
	// This call requires flushing the batch's writes but should make subsequent
	// writes somewhat cheaper. Early exploration showed no win but found that if
	// a Distinct() batch could be used for all of command application it could
	// be a win. At the time of writing that approach was deemed not safe.
	writer := batch.Distinct()

	// Special-cased MVCC stats handling to exploit commutativity of stats delta
	// upgrades. Thanks to commutativity, the spanlatch manager does not have to
	// serialize on the stats key.
	deltaStats := cmd.replicatedResult().Delta.ToStats()
	usingAppliedStateKey := replicaState.UsingAppliedStateKey
	needAppliedStateMigration := !usingAppliedStateKey &&
		cmd.replicatedResult().State != nil &&
		cmd.replicatedResult().State.UsingAppliedStateKey
	if needAppliedStateMigration {
		// The Raft command wants us to begin using the RangeAppliedState key
		// and we haven't performed the migration yet. Delete the old keys
		// that this new key is replacing.
		//
		// NB: entering this branch indicates that the cmd was considered
		// non-trivial and therefore placed in its own batch.
		err := r.raftMu.stateLoader.MigrateToRangeAppliedStateKey(ctx, writer, &deltaStats)
		if err != nil {
			return errors.Wrap(err, "unable to migrate to range applied state")
		}
		usingAppliedStateKey = true
	}

	if !writeAppliedState {
		// Don't write any applied state, regardless of the technique we'd be using.
	} else if usingAppliedStateKey {
		// Note that calling ms.Add will never result in ms.LastUpdateNanos
		// decreasing (and thus LastUpdateNanos tracks the maximum LastUpdateNanos
		// across all deltaStats).
		ms := *replicaState.Stats
		ms.Add(deltaStats)

		// Set the range applied state, which includes the last applied raft and
		// lease index along with the mvcc stats, all in one key.
		if err := r.raftMu.stateLoader.SetRangeAppliedState(ctx, writer,
			cmd.e.Index, cmd.leaseIndex, &ms); err != nil {
			return errors.Wrap(err, "unable to set range applied state")
		}
	} else {
		// Advance the last applied index. We use a blind write in order to avoid
		// reading the previous applied index keys on every write operation. This
		// requires a little additional work in order maintain the MVCC stats.
		var appliedIndexNewMS enginepb.MVCCStats
		if err := r.raftMu.stateLoader.SetLegacyAppliedIndexBlind(ctx, writer, &appliedIndexNewMS,
			cmd.e.Index, cmd.leaseIndex); err != nil {
			return errors.Wrap(err, "unable to set applied index")
		}
		deltaStats.SysBytes += appliedIndexNewMS.SysBytes -
			r.raftMu.stateLoader.CalcAppliedIndexSysBytes(replicaState.RaftAppliedIndex, replicaState.LeaseAppliedIndex)

		// Note that calling ms.Add will never result in ms.LastUpdateNanos
		// decreasing (and thus LastUpdateNanos tracks the maximum LastUpdateNanos
		// across all deltaStats).
		ms := *replicaState.Stats
		ms.Add(deltaStats)
		if err := r.raftMu.stateLoader.SetMVCCStats(ctx, writer, &ms); err != nil {
			return errors.Wrap(err, "unable to update MVCCStats")
		}
	}
	// We may have modified the effect on the range's stats that the application
	// of the command will have. Update the command's stats delta to reflect this.
	cmd.replicatedResult().Delta = deltaStats.ToStatsDelta()

	// Close the Distinct() batch here now that we're done writing to it.
	writer.Close()

	// NB: it is not sane to use the distinct engine when updating truncated state
	// as multiple commands in the same batch could modify that state and the
	// below code reads before it writes.
	haveTruncatedState := cmd.replicatedResult().State != nil &&
		cmd.replicatedResult().State.TruncatedState != nil
	if haveTruncatedState {
		apply, err := handleTruncatedStateBelowRaft(ctx, replicaState.TruncatedState,
			cmd.replicatedResult().State.TruncatedState, r.raftMu.stateLoader, batch)
		if err != nil {
			return err
		}
		if !apply {
			// The truncated state was discarded, so make sure we don't apply
			// it to our in-memory state.
			cmd.replicatedResult().State.TruncatedState = nil
			cmd.replicatedResult().RaftLogDelta = 0
			// TODO(ajwerner): consider moving this code.
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

	start := timeutil.Now()

	// TODO(ajwerner): This assertion no longer makes much sense.
	var assertHS *raftpb.HardState
	if util.RaceEnabled && cmd.replicatedResult().Split != nil {
		rsl := stateloader.Make(cmd.replicatedResult().Split.RightDesc.RangeID)
		oldHS, err := rsl.LoadHardState(ctx, batch)
		if err != nil {
			return errors.Wrap(err, "unable to load HardState")
		}
		assertHS = &oldHS
	}

	if assertHS != nil {
		// Load the HardState that was just committed (if any).
		rsl := stateloader.Make(cmd.replicatedResult().Split.RightDesc.RangeID)
		newHS, err := rsl.LoadHardState(ctx, batch)
		if err != nil {
			return errors.Wrap(err, "unable to load HardState")
		}
		// Assert that nothing moved "backwards".
		if newHS.Term < assertHS.Term ||
			(newHS.Term == assertHS.Term && newHS.Commit < assertHS.Commit) {
			log.Fatalf(ctx, "clobbered HardState: %s\n\npreviously: %s\noverwritten with: %s",
				pretty.Diff(newHS, *assertHS), pretty.Sprint(*assertHS), pretty.Sprint(newHS))
		}
	}

	// TODO(ajwerner): This metric no longer has anything to do with the "Commit"
	// of anything. Unfortunately it's exposed in the admin ui of 19.1. We should
	// remove it from the admin ui for 19.2 and then in 20.1 we can rename it to
	// something more appropriate.
	elapsed := timeutil.Since(start)
	r.store.metrics.RaftCommandCommitLatency.RecordValue(elapsed.Nanoseconds())
	return nil
}

// applyCmdAppBatch handles the logic of writing a batch to the storage engine and
// applying it to the Replica state machine. This method clears b for reuse
// before returning.
func (r *Replica) applyCmdAppBatch(
	ctx context.Context, b *cmdAppBatch, stats *handleCommittedEntriesStats,
) (errExpl string, err error) {
	defer b.reset()
	if log.V(4) {
		log.Infof(ctx, "flushing batch %v of %d entries", b.replicaState, b.cmdBuf.len)
	}
	// Entry application is not done without syncing to disk.
	// The atomicity guarantees of the batch and the fact that the applied state
	// is stored in this batch, ensure that if the batch ends up not being durably
	// committed then the entries in this batch will be applied again upon
	// startup.
	if err := b.batch.Commit(false); err != nil {
		log.Fatalf(ctx, "failed to commit Raft entry batch: %v", err)
	}
	b.batch.Close()
	b.batch = nil
	// NB: we compute the triviality of the batch here again rather than storing
	// it as it may have changed during processing. In particular, the upgrade of
	// applied state will have already occurred.
	var batchIsNonTrivial bool
	// Non-trivial entries are always alone in a batch and thus are last.
	if cmd := b.cmdBuf.last(); !isTrivial(cmd.replicatedResult(), b.replicaState.UsingAppliedStateKey) {
		batchIsNonTrivial = true
		// Deal with locking sometimes associated with complex commands.
		if unlock := cmd.splitMergeUnlock; unlock != nil {
			defer unlock()
			cmd.splitMergeUnlock = nil
		}
		if cmd.replicatedResult().BlockReads {
			r.readOnlyCmdMu.Lock()
			defer r.readOnlyCmdMu.Unlock()
			cmd.replicatedResult().BlockReads = false
		}
	}
	// Now that the batch is committed we can go about applying the side effects
	// of the update to the truncated state. Note that this is safe only if the
	// new truncated state is durably on disk (i.e.) synced. See #38566.
	//
	// NB: even if multiple updates to TruncatedState occurred in this batch we
	// coalesce their side effects and only consult the latest
	// TruncatedState.Index.
	var sideloadTruncationDelta int64
	if b.updatedTruncatedState {
		truncState := b.replicaState.TruncatedState
		r.store.raftEntryCache.Clear(r.RangeID, truncState.Index+1)
		log.VEventf(ctx, 1, "truncating sideloaded storage up to (and including) index %d", truncState.Index)
		if sideloadTruncationDelta, _, err = r.raftMu.sideloaded.TruncateTo(ctx, truncState.Index+1); err != nil {
			// We don't *have* to remove these entries for correctness. Log a
			// loud error, but keep humming along.
			log.Errorf(ctx, "while removing sideloaded files during log truncation: %+v", err)
		}
	}

	// Iterate through the cmds to compute the raft log delta and writeStats.
	var mutationCount int
	var it cmdAppCtxBufIterator
	raftLogDelta := -sideloadTruncationDelta
	for ok := it.init(&b.cmdBuf); ok; ok = it.next() {
		cmd := it.cur()
		raftLogDelta += cmd.replicatedResult().RaftLogDelta
		cmd.replicatedResult().RaftLogDelta = 0
		mutationCount += cmd.mutationCount
	}
	// Record the write activity, passing a 0 nodeID because replica.writeStats
	// intentionally doesn't track the origin of the writes.
	r.writeStats.recordCount(float64(mutationCount), 0 /* nodeID */)
	// deltaStats will store the delta from the current state to the new state
	// which will be used to update the metrics.
	r.mu.Lock()
	r.mu.state.RaftAppliedIndex = b.replicaState.RaftAppliedIndex
	r.mu.state.LeaseAppliedIndex = b.replicaState.LeaseAppliedIndex
	prevStats := *r.mu.state.Stats
	*r.mu.state.Stats = *b.replicaState.Stats
	if raftLogDelta != 0 {
		r.mu.raftLogSize += raftLogDelta
		if r.mu.raftLogSize < 0 {
			r.mu.raftLogSize = 0
		}
		r.mu.raftLogLastCheckSize += raftLogDelta
		if r.mu.raftLogLastCheckSize < 0 {
			r.mu.raftLogLastCheckSize = 0
		}
	}
	if b.updatedTruncatedState {
		r.mu.state.TruncatedState = b.replicaState.TruncatedState
	}
	// Check the queuing conditions.
	checkRaftLog := r.mu.raftLogSize-r.mu.raftLogLastCheckSize >= RaftLogQueueStaleSize
	needsSplitBySize := r.needsSplitBySizeRLocked()
	needsMergeBySize := r.needsMergeBySizeRLocked()
	r.mu.Unlock()
	deltaStats := *b.replicaState.Stats
	deltaStats.Subtract(prevStats)
	r.store.metrics.addMVCCStats(deltaStats)
	// NB: the bootstrap store has a nil split queue.
	// TODO(tbg): the above is probably a lie now.
	if r.store.splitQueue != nil && needsSplitBySize && r.splitQueueThrottle.ShouldProcess(timeutil.Now()) {
		r.store.splitQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
	}
	// The bootstrap store has a nil merge queue.
	// TODO(tbg): the above is probably a lie now.
	if r.store.mergeQueue != nil && needsMergeBySize && r.mergeQueueThrottle.ShouldProcess(timeutil.Now()) {
		// TODO(tbg): for ranges which are small but protected from merges by
		// other means (zone configs etc), this is called on every command, and
		// fires off a goroutine each time. Make this trigger (and potentially
		// the split one above, though it hasn't been observed to be as
		// bothersome) less aggressive.
		r.store.mergeQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
	}
	if checkRaftLog {
		r.store.raftLogQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
	}
	for ok := it.init(&b.cmdBuf); ok; ok = it.next() {
		cmd := it.cur()
		for _, sc := range cmd.replicatedResult().SuggestedCompactions {
			r.store.compactor.Suggest(cmd.ctx, sc)
		}
		cmd.replicatedResult().SuggestedCompactions = nil
		isNonTrivial := batchIsNonTrivial && it.isLast()
		if errExpl, err = r.handleRaftCommandResult(cmd.ctx, cmd, isNonTrivial,
			b.replicaState.UsingAppliedStateKey); err != nil {
			return errExpl, err
		}
		if isNonTrivial {
			stats.stateAssertions++
		}
		stats.entriesProcessed++
	}
	stats.batchesProcessed++
	return "", nil
}
