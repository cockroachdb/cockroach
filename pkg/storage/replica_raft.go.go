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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
)

// processRaftCommand processes a raft command by unpacking the
// command struct to get args and reply and then applying the command
// to the state machine via applyRaftCommand(). The result is sent on
// the command's done channel, if available. As a special case, the
// zero idKey signifies an empty Raft command, which will apply as a
// no-op (without accessing raftCmd), updating only the applied index.
//
// This method returns true if the command successfully applied a
// replica change.
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
		proposal.ctx = nil // avoid confusion
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
			forcedErr = filter(storagebase.ApplyFilterArgs{
				CmdID:                idKey,
				ReplicatedEvalResult: raftCmd.ReplicatedEvalResult,
				StoreID:              r.store.StoreID(),
				RangeID:              r.RangeID,
			})
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

		var delta enginepb.MVCCStats
		{
			var err error
			delta, err = r.applyRaftCommand(
				ctx, idKey, raftCmd.ReplicatedEvalResult, raftIndex, leaseIndex, writeBatch)
			raftCmd.ReplicatedEvalResult.Delta = delta.ToStatsDelta()

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
			pErr = filter(storagebase.ApplyFilterArgs{
				CmdID:                idKey,
				ReplicatedEvalResult: raftCmd.ReplicatedEvalResult,
				StoreID:              r.store.StoreID(),
				RangeID:              r.RangeID,
			})
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
			if proposalRetry != proposalNoReevaluation {
				response.ProposalRetry = proposalRetry
				if pErr == nil {
					log.Fatalf(ctx, "proposal with nontrivial retry behavior, but no error: %+v", proposal)
				}
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
			response.EndTxns = proposal.Local.DetachEndTxns(response.Err != nil)
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
		proposal.finishApplication(response)
	} else if response.Err != nil {
		log.VEventf(ctx, 1, "applying raft command resulted in error: %s", response.Err)
	}

	return raftCmd.ReplicatedEvalResult.ChangeReplicas != nil
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
func (r *Replica) applyRaftCommand(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	rResult storagepb.ReplicatedEvalResult,
	raftAppliedIndex, leaseAppliedIndex uint64,
	writeBatch *storagepb.WriteBatch,
) (enginepb.MVCCStats, error) {
	if raftAppliedIndex <= 0 {
		return enginepb.MVCCStats{}, errors.New("raft command index is <= 0")
	}
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
		return enginepb.MVCCStats{}, errors.Errorf("applied index jumped from %d to %d",
			oldRaftAppliedIndex, raftAppliedIndex)
	}

	batch := r.store.Engine().NewWriteOnlyBatch()
	defer batch.Close()

	if writeBatch != nil {
		if err := batch.ApplyBatchRepr(writeBatch.Data, false); err != nil {
			return enginepb.MVCCStats{}, errors.Wrap(err, "unable to apply WriteBatch")
		}
	}

	// The only remaining use of the batch is for range-local keys which we know
	// have not been previously written within this batch. Currently the only
	// remaining writes are the raft applied index and the updated MVCC stats.
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
			return enginepb.MVCCStats{}, errors.Wrap(err, "unable to migrate to range applied state")
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
			return enginepb.MVCCStats{}, errors.Wrap(err, "unable to set range applied state")
		}
	} else {
		// Advance the last applied index. We use a blind write in order to avoid
		// reading the previous applied index keys on every write operation. This
		// requires a little additional work in order maintain the MVCC stats.
		var appliedIndexNewMS enginepb.MVCCStats
		if err := r.raftMu.stateLoader.SetLegacyAppliedIndexBlind(ctx, writer, &appliedIndexNewMS,
			raftAppliedIndex, leaseAppliedIndex); err != nil {
			return enginepb.MVCCStats{}, errors.Wrap(err, "unable to set applied index")
		}
		deltaStats.SysBytes += appliedIndexNewMS.SysBytes -
			r.raftMu.stateLoader.CalcAppliedIndexSysBytes(oldRaftAppliedIndex, oldLeaseAppliedIndex)

		// Note that calling ms.Add will never result in ms.LastUpdateNanos
		// decreasing (and thus LastUpdateNanos tracks the maximum LastUpdateNanos
		// across all deltaStats).
		ms.Add(deltaStats)
		if err := r.raftMu.stateLoader.SetMVCCStats(ctx, writer, &ms); err != nil {
			return enginepb.MVCCStats{}, errors.Wrap(err, "unable to update MVCCStats")
		}
	}

	if rResult.State != nil && rResult.State.TruncatedState != nil {
		newTruncatedState := rResult.State.TruncatedState

		if r.store.cfg.Settings.Version.IsActive(cluster.VersionRaftLogTruncationBelowRaft) {
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
			prefixBuf := &r.raftMu.stateLoader.RangeIDPrefixBuf
			for idx := oldTruncatedState.Index + 1; idx <= newTruncatedState.Index; idx++ {
				// NB: RangeIDPrefixBufs have sufficient capacity (32 bytes) to
				// avoid allocating when constructing Raft log keys (16 bytes).
				unsafeKey := prefixBuf.RaftLogKey(idx)
				if err := writer.Clear(engine.MakeMVCCMetadataKey(unsafeKey)); err != nil {
					err = errors.Wrapf(err, "unable to clear truncated Raft entries for %+v", newTruncatedState)
					return enginepb.MVCCStats{}, err
				}
			}
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
			return enginepb.MVCCStats{}, errors.Wrap(err, "unable to load HardState")
		}
		assertHS = &oldHS
	}
	if err := batch.Commit(false); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "could not commit batch")
	}

	if assertHS != nil {
		// Load the HardState that was just committed (if any).
		rsl := stateloader.Make(rResult.Split.RightDesc.RangeID)
		newHS, err := rsl.LoadHardState(ctx, r.store.Engine())
		if err != nil {
			return enginepb.MVCCStats{}, errors.Wrap(err, "unable to load HardState")
		}
		// Assert that nothing moved "backwards".
		if newHS.Term < assertHS.Term || (newHS.Term == assertHS.Term && newHS.Commit < assertHS.Commit) {
			log.Fatalf(ctx, "clobbered HardState: %s\n\npreviously: %s\noverwritten with: %s",
				pretty.Diff(newHS, *assertHS), pretty.Sprint(*assertHS), pretty.Sprint(newHS))
		}
	}

	elapsed := timeutil.Since(start)
	r.store.metrics.RaftCommandCommitLatency.RecordValue(elapsed.Nanoseconds())
	return deltaStats, nil
}
