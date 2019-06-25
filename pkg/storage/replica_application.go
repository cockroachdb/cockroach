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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

type handleCommittedEntriesStats struct {
	processed int
}

// handleCommittedEntriesRaftMuLocked deals with the complexities involved in
// moving the Replica's replicated state machine forward given committed raft
// entries. All changes to r.mu.state occur downstream of this call.
//
// The stats argument is updated to reflect the number of entries which are
// processed. The refreshReason argument is the current refreshReason for this
// raftReady and the returned refreshRaftReason reflects any update to this
// value encountered during the processing of these entries.
//
// At a high level, it receives committed entries which each contains the
// evaluation of a batch (at its heart a WriteBatch, to be applied to the
// underlying storage engine), which it decodes into batches of entries which
// are safe to apply atomically together.
//
// The pseudocode looks like:
//
//   while entries:
//     for each entry in entries:
//        decode front, add to batch, pop front from entries
//        if entry is non-trivial:
//          break
//     for each entry in batch:
//       check if failed
//       if not:
//         apply to batch
//     commit batch
//     update Replica.state
//     for each entry in batch:
//         ack client, release latches
//
// The processing of committed entries proceeds in 4 stages, decoding,
// local preparation, staging, and application. Commands may be applied together
// so long as their implied state change is "trivial" (see isTrivial). Once
// decoding has discovered a batch boundary, the commands are prepared by
// reading the current replica state from underneath the Replica.mu and
// determining whether any of the commands were proposed locally. Next each
// command is written to the engine.Batch and have the "trivial" component of
// their ReplicatedEvalResult applied to the batch's view of the ReplicaState.
// Finally the batch is written to the storage engine and its side effects on
// the Replica's state are applied.
func (r *Replica) handleCommittedEntriesRaftMuLocked(
	ctx context.Context, committedEntries []raftpb.Entry, prevReason refreshRaftReason,
) (
	stats handleCommittedEntriesStats,
	refreshReason refreshRaftReason,
	errExpl string,
	err error,
) {
	// TODO(ajwerner): replace errExpl with proper error composition using the new
	// errors library.

	// NB: We want to batch application of commands which have a "trivial"
	// impact on replicated range state.
	//
	// To deal with this we create a batch up front. For each command we check its
	// type. If it's a conf change we know it'll be handled on its own.
	// If it's a regular entry with an interesting ReplicatedEvalResult then we
	// also want to handle it on its own.
	//
	// For things which we do not want to handle on their own we call
	// processRaftCommand with the current batch and the current view of in-memory
	// state.
	b := getEntryApplicationBatch()
	defer releaseEntryApplicationBatch(b)
	for len(committedEntries) > 0 {
		var sawEmptyEntry bool
		sawEmptyEntry, committedEntries, errExpl, err = b.decode(ctx, committedEntries)
		// etcd raft occasionally adds a nil entry (our own commands are never
		// empty). This happens in two situations:
		// When a new leader is elected, and when a config change is dropped due
		// to the "one at a time" rule. In both cases we may need to resubmit our
		// pending proposals (In the former case we resubmit everything because
		// we proposed them to a former leader that is no longer able to commit
		// them. In the latter case we only need to resubmit pending config
		// changes, but it's hard to distinguish so we resubmit everything
		// anyway). We delay resubmission until after we have processed the
		// entire batch of entries.
		if sawEmptyEntry {
			// Overwrite unconditionally since this is the most aggressive
			// reproposal mode.
			if !r.store.TestingKnobs().DisableRefreshReasonNewLeaderOrConfigChange {
				refreshReason = reasonNewLeaderOrConfigChange
			}
		}
		if err != nil {
			return stats, refreshReason, errExpl, err
		}
		r.retrieveLocalProposals(ctx, b)
		b.batch = r.store.engine.NewBatch()
		// Stage each of the commands which will write them into the newly created
		// engine.Batch and update b's view of the replicaState.
		var it entryApplicationStateBufIterator
		for ok := it.init(&b.appStates); ok; ok = it.next() {
			s := it.state()
			r.stageRaftCommand(s.ctx, s, b.batch, &b.replicaState, it.isLast())
			updatedTruncatedState := stageTrivialReplicatedEvalResult(s.ctx,
				s.replicatedResult(), s.e.Index, s.leaseIndex, &b.replicaState)
			b.updatedTruncatedState = b.updatedTruncatedState || updatedTruncatedState
		}
		if errExpl, err = r.applyBatch(ctx, b, &stats); err != nil {
			return stats, refreshReason, errExpl, err
		}
	}
	return stats, refreshReason, "", nil
}

func (r *Replica) retrieveLocalProposals(ctx context.Context, b *entryApplicationBatch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b.replicaState = r.mu.state
	// Copy stats as it gets updated in-place in applyRaftCommandToBatch.
	b.replicaState.Stats = &b.stats
	*b.replicaState.Stats = *r.mu.state.Stats
	var it entryApplicationStateBufIterator
	for ok := it.init(&b.appStates); ok; ok = it.next() {
		s := it.state()
		s.proposal = r.mu.proposals[s.idKey]
		if s.proposedLocally() {
			// We initiated this command, so use the caller-supplied context.
			s.ctx = s.proposal.ctx
			delete(r.mu.proposals, s.idKey)
			// At this point we're not guaranteed to have proposalQuota initialized,
			// the same is true for quotaReleaseQueues. Only queue the proposal's
			// quota for release if the proposalQuota is initialized.
			if r.mu.proposalQuota != nil {
				r.mu.quotaReleaseQueue = append(r.mu.quotaReleaseQueue, s.proposal.quotaSize)
			}
		}
	}
}

// stageRaftCommand handles the first phase of applying a command to the
// replica state machine.
//
// The proposal also contains auxiliary data which needs to be verified in
// order to decide whether the proposal should be applied: the command's
// MaxLeaseIndex must move the state machine's LeaseAppliedIndex forward, and
// the proposer's lease (or rather its sequence number) must match that of the
// state machine, and lastly the GCThreshold is validated. If any of the checks
// fail, the proposal's content is wiped and we apply an empty log entry
// instead, returning an error to the caller to handle. The two typical cases
// are the lease mismatch (in which case the caller tries to send the command
// to the actual leaseholder) and violations of the LeaseAppliedIndex (in which
// case the proposal is retried if it was proposed locally).
//
// Assuming all checks were passed, the command is applied to the batch,
// which is done by the aptly named applyRaftCommandToBatch.
//
// For trivial proposals this is the whole story, but some commands trigger
// additional code in this method. The standard way in which this is triggered
// is via a side effect communicated in the proposal's ReplicatedEvalResult
// and, for local proposals, the LocalEvalResult. These might, for example,
// trigger an update of the Replica's in-memory state to match updates to the
// on-disk state, or pass intents to the intent resolver. Some commands don't
// fit this simple schema and need to hook deeper into the code. Notably splits
// and merges need to acquire locks on their right-hand side Replicas and may
// need to add data to the WriteBatch before it is applied; similarly, changes
// to the disk layout of internal state typically require a migration which
// shows up here. Any of this logic however is deferred until after the batch
// has been written to the storage engine.
func (r *Replica) stageRaftCommand(
	ctx context.Context,
	s *entryApplicationState,
	batch engine.Batch,
	replicaState *storagepb.ReplicaState,
	writeAppliedState bool,
) {
	if s.e.Index == 0 {
		log.Fatalf(ctx, "processRaftCommand requires a non-zero index")
	}
	if log.V(4) {
		log.Infof(ctx, "processing command %x: maxLeaseIndex=%d",
			s.idKey, s.raftCmd.MaxLeaseIndex)
	}

	var ts hlc.Timestamp
	if s.idKey != "" {
		ts = s.replicatedResult().Timestamp
	}

	s.leaseIndex, s.proposalRetry, s.forcedErr = checkForcedErr(ctx,
		s.idKey, s.raftCmd, s.proposal, s.proposedLocally(), replicaState)
	if s.forcedErr == nil {
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
		s.forcedErr = roachpb.NewError(r.requestCanProceed(roachpb.RSpan{}, ts))
	}

	// applyRaftCommandToBatch will return "expected" errors, but may also indicate
	// replica corruption (as of now, signaled by a replicaCorruptionError).
	// We feed its return through maybeSetCorrupt to act when that happens.
	if s.forcedErr != nil {
		log.VEventf(ctx, 1, "applying command with forced error: %s", s.forcedErr)
	} else {
		log.Event(ctx, "applying command")

		if splitMergeUnlock, err := r.maybeAcquireSplitMergeLock(ctx, s.raftCmd); err != nil {
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

			s.forcedErr = roachpb.NewError(err)
		} else if splitMergeUnlock != nil {
			// Close over raftCmd to capture its value at execution time; we clear
			// ReplicatedEvalResult on certain errors.
			s.splitMergeUnlock = splitMergeUnlock
		}
	}

	var writeBatch *storagepb.WriteBatch
	{
		if filter := r.store.cfg.TestingKnobs.TestingApplyFilter; s.forcedErr == nil && filter != nil {
			var newPropRetry int
			newPropRetry, s.forcedErr = filter(storagebase.ApplyFilterArgs{
				CmdID:                s.idKey,
				ReplicatedEvalResult: *s.replicatedResult(),
				StoreID:              r.store.StoreID(),
				RangeID:              r.RangeID,
			})
			if s.proposalRetry == 0 {
				s.proposalRetry = proposalReevaluationReason(newPropRetry)
			}
		}

		if s.forcedErr != nil {
			// Apply an empty entry.
			*s.replicatedResult() = storagepb.ReplicatedEvalResult{}
			s.raftCmd.WriteBatch = nil
			s.raftCmd.LogicalOpLog = nil
		}

		// Update the node clock with the serviced request. This maintains
		// a high water mark for all ops serviced, so that received ops without
		// a timestamp specified are guaranteed one higher than any op already
		// executed for overlapping keys.
		// TODO(ajwerner): coalesce the clock update per batch.
		r.store.Clock().Update(ts)

		if s.raftCmd.WriteBatch != nil {
			writeBatch = s.raftCmd.WriteBatch
		}

		if deprecatedDelta := s.replicatedResult().DeprecatedDelta; deprecatedDelta != nil {
			s.replicatedResult().Delta = deprecatedDelta.ToStatsDelta()
			s.replicatedResult().DeprecatedDelta = nil
		}

		// AddSSTable ingestions run before the actual batch. This makes sure
		// that when the Raft command is applied, the ingestion has definitely
		// succeeded. Note that we have taken precautions during command
		// evaluation to avoid having mutations in the WriteBatch that affect
		// the SSTable. Not doing so could result in order reversal (and missing
		// values) here. If the key range we are ingesting into isn't empty,
		// we're not using AddSSTable but a plain WriteBatch.
		if s.replicatedResult().AddSSTable != nil {
			copied := addSSTablePreApply(
				ctx,
				r.store.cfg.Settings,
				r.store.engine,
				r.raftMu.sideloaded,
				s.e.Term,
				s.e.Index,
				*s.replicatedResult().AddSSTable,
				r.store.limiters.BulkIOWriteRate,
			)
			r.store.metrics.AddSSTableApplications.Inc(1)
			if copied {
				r.store.metrics.AddSSTableApplicationCopies.Inc(1)
			}
			s.replicatedResult().AddSSTable = nil
		}

		if s.replicatedResult().Split != nil {
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
			// NB: it is safe to use use the tmpBatch which does not include writes in
			// batch because the splitPreApply only writes data based on range state
			// which is never touched by "trivial" commands.
			splitPreApply(ctx, tmpBatch, s.replicatedResult().Split.SplitTrigger)
			writeBatch.Data = tmpBatch.Repr()
			tmpBatch.Close()
		}

		if merge := s.replicatedResult().Merge; merge != nil {
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
			*s.replicatedResult(), err = r.applyRaftCommandToBatch(
				s.ctx, s.idKey, *s.replicatedResult(), s.e.Index, s.leaseIndex, writeBatch, replicaState, batch, writeAppliedState)

			// applyRaftCommandToBatch returned an error, which usually indicates
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
// cannot be updated, an error (which is likely fatal!) is returned and must be
// handled by the caller.
// The returned ReplicatedEvalResult replaces the caller's.
func (r *Replica) applyRaftCommandToBatch(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	replicatedResult storagepb.ReplicatedEvalResult,
	raftAppliedIndex, leaseAppliedIndex uint64,
	writeBatch *storagepb.WriteBatch,
	replicaState *storagepb.ReplicaState,
	batch engine.Batch,
	writeAppliedState bool,
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
	if replicatedResult.Split != nil {
		replicaState.Stats.ContainsEstimates = false
	}
	ms := replicaState.Stats

	if raftAppliedIndex != replicaState.RaftAppliedIndex+1 {
		// If we have an out of order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return
		// a corruption error.
		return storagepb.ReplicatedEvalResult{}, errors.Errorf("applied index jumped from %d to %d",
			replicaState.RaftAppliedIndex, raftAppliedIndex)
	}

	haveTruncatedState := replicatedResult.State != nil && replicatedResult.State.TruncatedState != nil

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
	deltaStats := replicatedResult.Delta.ToStats()
	usingAppliedStateKey := replicaState.UsingAppliedStateKey
	if !usingAppliedStateKey && replicatedResult.State != nil && replicatedResult.State.UsingAppliedStateKey {
		// The Raft command wants us to begin using the RangeAppliedState key
		// and we haven't performed the migration yet. Delete the old keys
		// that this new key is replacing.
		err := r.raftMu.stateLoader.MigrateToRangeAppliedStateKey(ctx, writer, &deltaStats)
		if err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to migrate to range applied state")
		}
		usingAppliedStateKey = true
	}

	if !writeAppliedState {
		// Don't do anything with the truncated state.
	} else if usingAppliedStateKey {
		// Note that calling ms.Add will never result in ms.LastUpdateNanos
		// decreasing (and thus LastUpdateNanos tracks the maximum LastUpdateNanos
		// across all deltaStats).
		ms.Add(deltaStats)

		// Set the range applied state, which includes the last applied raft and
		// lease index along with the mvcc stats, all in one key.
		if err := r.raftMu.stateLoader.SetRangeAppliedState(ctx, writer,
			raftAppliedIndex, leaseAppliedIndex, ms); err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to set range applied state")
		}
		ms.Subtract(deltaStats)
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
			r.raftMu.stateLoader.CalcAppliedIndexSysBytes(replicaState.RaftAppliedIndex, replicaState.LeaseAppliedIndex)

		// Note that calling ms.Add will never result in ms.LastUpdateNanos
		// decreasing (and thus LastUpdateNanos tracks the maximum LastUpdateNanos
		// across all deltaStats).
		ms.Add(deltaStats)
		if err := r.raftMu.stateLoader.SetMVCCStats(ctx, writer, ms); err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to update MVCCStats")
		}
		ms.Subtract(deltaStats)
	}

	if haveTruncatedState {
		apply, err := handleTruncatedStateBelowRaft(ctx, replicaState.TruncatedState, replicatedResult.State.TruncatedState, r.raftMu.stateLoader, writer)
		if err != nil {
			return storagepb.ReplicatedEvalResult{}, err
		}
		if !apply {
			// The truncated state was discarded, so make sure we don't apply
			// it to our in-memory state.
			replicatedResult.State.TruncatedState = nil
			replicatedResult.RaftLogDelta = 0
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

	// TODO(peter): We did not close the writer in an earlier version of
	// the code, which went undetected even though we used the batch after
	// (though only to commit it). We should add an assertion to prevent that in
	// the future.
	writer.Close()

	start := timeutil.Now()

	var assertHS *raftpb.HardState
	if util.RaceEnabled && replicatedResult.Split != nil {
		rsl := stateloader.Make(replicatedResult.Split.RightDesc.RangeID)
		oldHS, err := rsl.LoadHardState(ctx, batch)
		if err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to load HardState")
		}
		assertHS = &oldHS
	}

	if assertHS != nil {
		// Load the HardState that was just committed (if any).
		rsl := stateloader.Make(replicatedResult.Split.RightDesc.RangeID)
		newHS, err := rsl.LoadHardState(ctx, batch)
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
	replicatedResult.Delta = deltaStats.ToStatsDelta()
	return replicatedResult, nil
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

// applyBatch handles the logic of writing a batch to the storage engine and
// applying it to the Replica state machine. This method clears b for reuse
// before returning.
func (r *Replica) applyBatch(
	ctx context.Context, b *entryApplicationBatch, stats *handleCommittedEntriesStats,
) (errExpl string, err error) {
	defer b.reset()
	if log.V(4) {
		log.Infof(ctx, "flushing batch %v of %d entries", b.replicaState, b.appStates.len)
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
	// Non-trivial entries are always last in a batch
	if s := b.appStates.last(); !isTrivial(s.replicatedResult(), b.replicaState.UsingAppliedStateKey) {
		batchIsNonTrivial = true
		// Deal with locking sometimes associated with complex commands.
		if unlock := s.splitMergeUnlock; unlock != nil {
			defer unlock(s.replicatedResult())
			s.splitMergeUnlock = nil
		}
		if s.replicatedResult().BlockReads {
			r.readOnlyCmdMu.Lock()
			defer r.readOnlyCmdMu.Unlock()
			s.replicatedResult().BlockReads = false
		}
	}
	// Now that the batch is committed we can go about applying the side effects
	// of the update to the truncated state. Note that this is safe only if the
	// new truncated state is durably on disk (i.e.) synced. See #38566.
	var sideloadTruncationDelta int64
	if b.updatedTruncatedState {
		truncState := b.replicaState.TruncatedState
		r.store.raftEntryCache.Clear(r.RangeID, truncState.Index+1)
		// Truncate the sideloaded storage.  This is true at the time of writing but unfortunately
		// could rot.
		{
			log.VEventf(ctx, 1, "truncating sideloaded storage up to (and including) index %d", truncState.Index)
			if sideloadTruncationDelta, _, err = r.raftMu.sideloaded.TruncateTo(ctx, truncState.Index+1); err != nil {
				// We don't *have* to remove these entries for correctness. Log a
				// loud error, but keep humming along.
				log.Errorf(ctx, "while removing sideloaded files during log truncation: %s", err)
			}
		}
	}
	applyRaftLogDelta := func(raftLogDelta int64) {
		if raftLogDelta == 0 {
			return
		}
		r.mu.raftLogSize += raftLogDelta
		if r.mu.raftLogSize < 0 {
			r.mu.raftLogSize = 0
		}
		r.mu.raftLogLastCheckSize += raftLogDelta
		if r.mu.raftLogLastCheckSize < 0 {
			r.mu.raftLogLastCheckSize = 0
		}
	}
	// deltaStats will store the delta from the current state to the new state
	// which will be used to update the metrics.
	r.mu.Lock()
	r.mu.state.RaftAppliedIndex = b.replicaState.RaftAppliedIndex
	r.mu.state.LeaseAppliedIndex = b.replicaState.LeaseAppliedIndex
	prevStats := *r.mu.state.Stats
	*r.mu.state.Stats = *b.replicaState.Stats
	// Iterate through the commands and their replicated eval results to apply
	// their raft log deltas.
	// Finally apply the truncation delta.
	// Store the queuing conditions
	var it entryApplicationStateBufIterator
	for ok := it.init(&b.appStates); ok; ok = it.next() {
		s := it.state()
		applyRaftLogDelta(s.replicatedResult().RaftLogDelta)
		s.replicatedResult().RaftLogDelta = 0
	}
	if b.updatedTruncatedState {
		// If we updated the truncated state we may have removed some sideloaded entries.
		// Update the raft log to reflect any change due to the sideloaded truncation.
		applyRaftLogDelta(-1 * sideloadTruncationDelta)
		r.mu.state.TruncatedState = b.replicaState.TruncatedState
	}
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
	for ok := it.init(&b.appStates); ok; ok = it.next() {
		s := it.state()
		replicatedResult := s.replicatedResult()
		for _, sc := range replicatedResult.SuggestedCompactions {
			r.store.compactor.Suggest(s.ctx, sc)
		}
		replicatedResult.SuggestedCompactions = nil
	}

	for ok := it.init(&b.appStates); ok; ok = it.next() {
		s := it.state()
		// Set up the local result prior to handling the ReplicatedEvalResult to
		// give testing knobs an opportunity to inspect it.
		r.prepareLocalResult(s.ctx, s)
		// Handle the ReplicatedEvalResult, executing any side effects of the last
		// state machine transition.
		//
		// Note that this must happen after committing (the engine.Batch), but
		// before notifying a potentially waiting client.
		clearTrivialReplicatedEvalResultFields(s.replicatedResult(), b.replicaState.UsingAppliedStateKey)
		isNonTrivialEntry := batchIsNonTrivial && it.isLast()
		if isNonTrivialEntry {
			r.handleComplexReplicatedEvalResult(s.ctx, *s.replicatedResult())
		} else if !s.replicatedResult().Equal(storagepb.ReplicatedEvalResult{}) {
			log.Fatalf(ctx, "failed to handle all side-effects of ReplicatedEvalResult: %v", s.replicatedResult())
		}

		// NB: Perform state assertion before acknowledging the client.
		// Some tests (TestRangeStatsInit) assumes that once the store has started
		// and the first range has a lease that there will not be a later hard-state.
		if isNonTrivialEntry {
			// Assert that the on-disk state doesn't diverge from the in-memory
			// state as a result of the side effects.
			r.mu.Lock()
			r.assertStateLocked(ctx, r.store.Engine())
			r.mu.Unlock()
		}

		if s.localResult != nil {
			r.handleLocalEvalResult(s.ctx, *s.localResult)
		}
		r.finishRaftCommand(s.ctx, s)
		stats.processed++
		switch s.e.Type {
		case raftpb.EntryNormal:
			if s.replicatedResult().ChangeReplicas != nil {
				log.Fatalf(s.ctx, "unexpected replication change from command %s", &s.raftCmd)
			}
		case raftpb.EntryConfChange:
			if s.replicatedResult().ChangeReplicas == nil {
				s.cc = raftpb.ConfChange{}
			}
			if err := r.withRaftGroup(true, func(raftGroup *raft.RawNode) (bool, error) {
				raftGroup.ApplyConfChange(s.cc)
				return true, nil
			}); err != nil {
				const errExpl = "during ApplyConfChange"
				return errExpl, errors.Wrap(err, errExpl)
			}
		}

	}
	return "", nil
}

// clearTrivialReplicatedEvalResultFields is used to zero out the fields of a
// ReplicatedEvalResult that have already been consumed when staging the
// corresponding command and applying it to the current batch's view of the
// ReplicaState. This function is called after a batch has been written to the
// storage engine. For trivial commands this function should result is a zero
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
		if replicatedResult.State.Stats != nil && (*replicatedResult.State.Stats == enginepb.MVCCStats{}) {
			replicatedResult.State.Stats = nil
		}
		if *replicatedResult.State == (storagepb.ReplicaState{}) {
			replicatedResult.State = nil
		}
	}
	replicatedResult.Delta = enginepb.MVCCStatsDelta{}
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

		if newThresh := replicatedResult.State.TxnSpanGCThreshold; newThresh != nil {
			if (*newThresh != hlc.Timestamp{}) {
				r.mu.Lock()
				r.mu.state.TxnSpanGCThreshold = newThresh
				r.mu.Unlock()
			}
			replicatedResult.State.TxnSpanGCThreshold = nil
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
			// Lock ordering dictates that we don't hold any mutexes when adding,
			// so we fire it off in a task.
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
func (r *Replica) prepareLocalResult(ctx context.Context, s *entryApplicationState) {
	var pErr *roachpb.Error
	if filter := r.store.cfg.TestingKnobs.TestingPostApplyFilter; filter != nil {
		var newPropRetry int
		newPropRetry, pErr = filter(storagebase.ApplyFilterArgs{
			CmdID:                s.idKey,
			ReplicatedEvalResult: *s.replicatedResult(),
			StoreID:              r.store.StoreID(),
			RangeID:              r.RangeID,
		})
		if s.proposalRetry == 0 {
			s.proposalRetry = proposalReevaluationReason(newPropRetry)
		}
		// calling maybeSetCorrupt here is mostly for tests and looks. The
		// interesting errors originate in applyRaftCommandToBatch, and they are
		// already handled above.
		pErr = r.maybeSetCorrupt(ctx, pErr)
	}
	if pErr == nil {
		pErr = s.forcedErr
	}

	if s.proposedLocally() {
		if s.proposalRetry != proposalNoReevaluation && pErr == nil {
			log.Fatalf(ctx, "proposal with nontrivial retry behavior, but no error: %+v", s.proposal)
		}
		if pErr != nil {
			// A forced error was set (i.e. we did not apply the proposal,
			// for instance due to its log position) or the Replica is now
			// corrupted.
			// If proposalRetry is set, we don't also return an error, as per the
			// proposalResult contract.
			if s.proposalRetry == proposalNoReevaluation {
				s.response.Err = pErr
			}
		} else if s.proposal.Local.Reply != nil {
			s.response.Reply = s.proposal.Local.Reply
		} else {
			log.Fatalf(ctx, "proposal must return either a reply or an error: %+v", s.proposal)
		}
		s.response.Intents = s.proposal.Local.DetachIntents()
		s.response.EndTxns = s.proposal.Local.DetachEndTxns(pErr != nil)
		if pErr == nil {
			s.localResult = s.proposal.Local
		}
	}
	if pErr != nil && s.localResult != nil {
		log.Fatalf(ctx, "shouldn't have a local result if command processing failed. pErr: %s", pErr)
	}
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEvent(ctx, 2, s.localResult.String())
	}
}

// finishRaftCommand is called after a command's side effects have been applied
// in order to acknowledge clients and release latches.
func (r *Replica) finishRaftCommand(ctx context.Context, s *entryApplicationState) {

	// Provide the command's corresponding logical operations to the
	// Replica's rangefeed. Only do so if the WriteBatch is nonnil,
	// otherwise it's valid for the logical op log to be nil, which
	// would shut down all rangefeeds. If no rangefeed is running,
	// this call will be a noop.
	if s.raftCmd.WriteBatch != nil {
		r.handleLogicalOpLogRaftMuLocked(ctx, s.raftCmd.LogicalOpLog)
	} else if s.raftCmd.LogicalOpLog != nil {
		log.Fatalf(ctx, "nonnil logical op log with nil write batch: %v", s.raftCmd)
	}

	// When set to true, recomputes the stats for the LHS and RHS of splits and
	// makes sure that they agree with the state's range stats.
	const expensiveSplitAssertion = false

	if expensiveSplitAssertion && s.replicatedResult().Split != nil {
		split := s.replicatedResult().Split
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

	if s.proposedLocally() {
		// If we failed to apply at the right lease index, try again with
		// a new one. This is important for pipelined writes, since they
		// don't have a client watching to retry, so a failure to
		// eventually apply the proposal would be a uservisible error.
		// TODO(nvanbenschoten): This reproposal is not tracked by the
		// quota pool. We should fix that.
		if s.proposalRetry == proposalIllegalLeaseIndex && r.tryReproposeWithNewLeaseIndex(s.proposal) {
			return
		}
		// Otherwise, signal the command's status to the client.
		s.proposal.finishApplication(s.response)
	} else if s.response.Err != nil {
		log.VEventf(ctx, 1, "applying raft command resulted in error: %s", s.response.Err)
	}
}

// entryApplicationState stores the state required to apply a single raft
// entry to a replica. The state is accumulated in stages which occur in
// Replica.handleCommittedEntriesRaftMuLocked. From a high level, a command is
// decoded into an entryApplicationBatch, then if it was proposed locally the
// proposal is populated from the replica's proposals map, then the command
// is staged into the batch by writing its update to the batch's engine.Batch
// and applying its "trivial" side-effects to the batch's view of ReplicaState.
// Then the batch is committed, the side-effects are applied and the local
// result is processed.
type entryApplicationState struct {
	// ctx is initialized to a non-nil value but may be overwritten with the
	// proposal's context if this entry was proposed locally.
	ctx context.Context
	// e is the Entry being applied.
	e *raftpb.Entry

	// These four fields are decoded from e.
	idKey   storagebase.CmdIDKey
	raftCmd storagepb.RaftCommand
	cc      raftpb.ConfChange // only populated for conf changes
	ccCtx   ConfChangeContext // only populated for conf changes

	// proposal is populated on the proposing Replica only and comes from the
	// Replica's proposal map.
	proposal *ProposalData

	// The below fields are set during stageRaftCommand when we validate that
	// a command applies given the current lease in checkForcedErr.
	leaseIndex    uint64
	forcedErr     *roachpb.Error
	proposalRetry proposalReevaluationReason
	// splitMergeUnlock is acquired for splits and merges.
	splitMergeUnlock func(*storagepb.ReplicatedEvalResult)

	// The below fields are set after the data has been written to the storage
	// engine in prepareLocalResult.
	localResult *result.LocalResult
	response    proposalResult
}

func (s *entryApplicationState) replicatedResult() *storagepb.ReplicatedEvalResult {
	return &s.raftCmd.ReplicatedEvalResult
}

func (s *entryApplicationState) proposedLocally() bool {
	return s.proposal != nil
}

// decode initializes the entryApplicationState with ctx and e and then
// decodes the entry into the decoded fields of s.
func (s *entryApplicationState) decode(
	ctx context.Context, e *raftpb.Entry,
) (errExpl string, err error) {
	*s = entryApplicationState{ctx: ctx, e: e}
	// etcd raft sometimes inserts nil commands, ours are never nil.
	// This case is handled upstream of this call.
	if len(s.e.Data) == 0 {
		return "", nil
	}
	switch s.e.Type {
	case raftpb.EntryNormal:
		return s.decodeNormalEntry(s.e)
	case raftpb.EntryConfChange:
		return s.decodeConfChangeEntry(s.e)
	default:
		log.Fatalf(s.ctx, "unexpected Raft entry: %v", s.e)
		return "", nil // unreachable
	}
}

func (s *entryApplicationState) decodeNormalEntry(e *raftpb.Entry) (errExpl string, err error) {
	var encodedCommand []byte
	s.idKey, encodedCommand = DecodeRaftCommand(e.Data)
	// An empty command is used to unquiesce a range and wake the
	// leader. Clear commandID so it's ignored for processing.
	if len(encodedCommand) == 0 {
		s.idKey = ""
	} else if err := protoutil.Unmarshal(encodedCommand, &s.raftCmd); err != nil {
		const errExpl = "while unmarshalling entry"
		return errExpl, errors.Wrap(err, errExpl)
	}
	return "", nil
}

func (s *entryApplicationState) decodeConfChangeEntry(e *raftpb.Entry) (errExpl string, err error) {
	if err := protoutil.Unmarshal(e.Data, &s.cc); err != nil {
		const errExpl = "while unmarshaling ConfChange"
		return errExpl, errors.Wrap(err, errExpl)
	}
	if err := protoutil.Unmarshal(s.cc.Context, &s.ccCtx); err != nil {
		const errExpl = "while unmarshaling ConfChangeContext"
		return errExpl, errors.Wrap(err, errExpl)
	}
	if err := protoutil.Unmarshal(s.ccCtx.Payload, &s.raftCmd); err != nil {
		const errExpl = "while unmarshaling RaftCommand"
		return errExpl, errors.Wrap(err, errExpl)
	}
	s.idKey = storagebase.CmdIDKey(s.ccCtx.CommandID)
	return "", nil
}

// entryApplicationBatch accumulates state due to the application of raft
// commands. Committed raft commands are applied to the batch in a multi-stage
// process whereby individual commands are decoded, prepared for application
// relative to the current view of replicaState, committed to the storage
// engine, applied to the Replica's in-memory state and then finished by
// releasing their latches and notifying clients.
type entryApplicationBatch struct {
	batch engine.Batch

	// replicaState is this batch's view of the replica's state.
	// It is copied from under the Replica.mu when the batch is initialized and
	// is updated in stageTrivialReplicatedEvalResult.
	replicaState storagepb.ReplicaState

	// stats is stored on the application batch to avoid an allocation in tracking
	// the batch's view of replicaState. All pointer fields in replicaState other
	// than Stats are overwritten completely rather than updated in-place.
	stats enginepb.MVCCStats

	// updatedTruncatedState tracks whether any command in the batch updated the
	// replica's truncated state. Truncated state updates are considered trivial
	// but represent something of a special case but given their relative
	// frequency and the fact that multiple updates can be trivially coalesced, we
	// treat updates to truncated state as trivial. If the batch updated the
	// truncated state then after it has been committed, then the side-loaded data
	// and raftentry.Cache should be truncated to the index indicated.
	// TODO(ajwerner): consider whether this logic should imply that commands
	// which update truncated state are non-trivial.
	updatedTruncatedState bool

	appStates entryApplicationStateBuf
}

// entryApplicationBatch structs are needed to apply raft commands, which is to
// say, frequently, so best to pool them rather than allocated under the raftMu.
var entryApplicationBatchSyncPool = sync.Pool{
	New: func() interface{} {
		return new(entryApplicationBatch)
	},
}

func getEntryApplicationBatch() *entryApplicationBatch {
	return entryApplicationBatchSyncPool.Get().(*entryApplicationBatch)
}

func releaseEntryApplicationBatch(b *entryApplicationBatch) {
	b.appStates.destroy()
	*b = entryApplicationBatch{}
	entryApplicationBatchSyncPool.Put(b)
}

// decode commands into the batch until a non-trivial entry is found or all of
// toProcess has been added to the batch. sawEmptyEntry is true if the consumed
// portion of toProcess contained a command with a zero byte Data field which
// indicates that pending proposals need to be resubmitted.
func (b *entryApplicationBatch) decode(
	ctx context.Context, toProcess []raftpb.Entry,
) (sawEmptyEntry bool, remaining []raftpb.Entry, errExpl string, err error) {
	for len(toProcess) > 0 {
		e := &toProcess[0]
		toProcess = toProcess[1:]
		if len(e.Data) == 0 {
			sawEmptyEntry = true
		}
		// Allocate a *entryApplicationState for this entry.
		s := b.appStates.allocate()
		if errExpl, err := s.decode(ctx, e); err != nil {
			return sawEmptyEntry, nil, errExpl, err
		}
		// This is a non-trivial entry which needs to be processed last in this
		// batch.
		if !isTrivial(s.replicatedResult(), b.replicaState.UsingAppliedStateKey) {
			break
		}
	}
	return sawEmptyEntry, toProcess, "", nil
}

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
func isTrivial(r *storagepb.ReplicatedEvalResult, usingStateAppliedKey bool) bool {
	return !r.IsLeaseRequest &&
		!r.BlockReads &&
		r.Split == nil &&
		r.Merge == nil &&
		r.ComputeChecksum == nil &&
		r.ChangeReplicas == nil &&
		r.AddSSTable == nil &&
		(r.State == nil ||
			(r.State.GCThreshold == nil &&
				r.State.TxnSpanGCThreshold == nil &&
				r.State.Lease == nil &&
				r.State.Desc == nil &&
				// An entry is non-trivial if an upgrade to UsingAppliedState is
				// required. If we're already usingStateAppliedKey or this entry does
				// not imply an upgrade then it is trivial.
				(usingStateAppliedKey || !r.State.UsingAppliedStateKey)))
}

func (b *entryApplicationBatch) reset() {
	b.appStates.clear()
	b.batch = nil
	b.updatedTruncatedState = false
	b.replicaState = storagepb.ReplicaState{}
	b.stats = enginepb.MVCCStats{}
}
