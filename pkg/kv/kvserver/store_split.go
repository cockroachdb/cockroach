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
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// splitPostAddWithReplica is called when the raft command is applied. Any
// changes to the given ReadWriter will be written atomically with the
// split commit.
//
// initClosedTS is the closed timestamp carried by the split command. It will be
// used to initialize the new RHS range.
func splitPostAddWithReplica(
	ctx context.Context,
	r *Replica,
	readWriter storage.ReadWriter,
	split roachpb.SplitTrigger,
	initClosedTS *hlc.Timestamp,
) error {
	// Sanity check that the store is in the split.
	//
	// The exception to that is if the DisableEagerReplicaRemoval testing flag is
	// enabled.
	rightDesc, hasRightDesc := split.RightDesc.GetReplicaDescriptor(r.StoreID())
	_, hasLeftDesc := split.LeftDesc.GetReplicaDescriptor(r.StoreID())
	if !hasRightDesc || !hasLeftDesc {
		return errors.AssertionFailedf("cannot process split on s%s which does not exist in the split: %+v",
			r.StoreID(), split)
	}

	// Check on the RHS, we need to ensure that it exists and has a minReplicaID
	// less than or equal to the replica we're about to initialize.
	//
	// The right hand side of the split was already created (and its raftMu
	// acquired) in Replica.acquireSplitLock. It must be present here if it hasn't
	// been removed in the meantime (handled below).
	rightRepl := r.store.GetReplicaIfExists(split.RightDesc.RangeID)
	// Check to see if we know that the RHS has already been removed from this
	// store at the replica ID implied by the split.
	if rightRepl == nil || rightRepl.isNewerThanSplit(&split) {
		// We're in the rare case where we know that the RHS has been removed
		// and re-added with a higher replica ID (and then maybe removed again).
		//
		// If rightRepl is not nil, we are *not* holding raftMu.
		//
		// To apply the split, we need to "throw away" the data that would belong to
		// the RHS, i.e. we clear the user data the RHS would have inherited from the
		// LHS due to the split and additionally clear all of the range ID local state
		// that the split trigger writes into the RHS.
		//
		// We know we've never processed a snapshot for the right range because the
		// LHS prevents any incoming snapshots until the split has executed (i.e. now).
		// It is important to preserve the HardState because we might however have
		// already voted at a higher term. In general this shouldn't happen because
		// we add learners and then promote them only after we snapshot but we're
		// going to be extra careful in case future versions of cockroach somehow
		// promote replicas without ensuring that a snapshot has been received.
		//
		// Rather than specifically deleting around the data we want to preserve
		// we read the HardState to preserve it, clear everything and write back
		// the HardState and tombstone. Note that we only do this if rightRepl
		// exists; if it doesn't, there's no Raft state to massage (when rightRepl
		// was removed, a tombstone was written instead).
		//
		// TODO(tbg): it would be cleaner to teach clearRangeData to only remove
		// the replicated state if rightRepl != nil, as opposed to writing back
		// internal raft state. As is, it's going to break with any new piece of
		// local state that we add, and it introduces locking between two Replicas
		// that don't ever need to interact.
		var hs raftpb.HardState
		if rightRepl != nil {
			rightRepl.raftMu.Lock()
			defer rightRepl.raftMu.Unlock()
			// Assert that the rightRepl is not initialized. We're about to clear out
			// the data of the RHS of the split; we cannot have already accepted a
			// snapshot to initialize this newer RHS.
			if rightRepl.IsInitialized() {
				return errors.AssertionFailedf("unexpectedly found initialized newer RHS of split: %v", rightRepl.Desc())
			}
			var err error
			hs, err = rightRepl.raftMu.stateLoader.LoadHardState(ctx, readWriter)
			if err != nil {
				return errors.Wrap(err, "failed to load hard state for removed rhs")
			}
		}
		const rangeIDLocalOnly = false
		const mustUseClearRange = false
		if err := clearRangeData(&split.RightDesc, readWriter, readWriter, rangeIDLocalOnly, mustUseClearRange); err != nil {
			return errors.Wrap(err, "failed to clear range data for removed rhs")
		}
		if rightRepl != nil {
			// Cleared the HardState and RaftReplicaID, so rewrite them to the current
			// values. NB: rightRepl.raftMu is still locked since HardState was read,
			// so it can't have been rewritten in the meantime (fixed in #75918).
			if err := rightRepl.raftMu.stateLoader.SetHardState(ctx, readWriter, hs); err != nil {
				return errors.Wrap(err, "failed to set hard state with 0 commit index for removed rhs")
			}
			if err := rightRepl.raftMu.stateLoader.SetRaftReplicaID(
				ctx, readWriter, rightRepl.ReplicaID()); err != nil {
				return errors.Wrap(err, "failed to set RaftReplicaID for removed rhs")
			}
		}
		return nil
	}

	// Update the raft HardState with the new Commit value now that the
	// replica is initialized (combining it with existing or default
	// Term and Vote). This is the common case.
	rsl := stateloader.Make(split.RightDesc.RangeID)
	if err := rsl.SynthesizeRaftState(ctx, readWriter); err != nil {
		return err
	}
	// Write the RaftReplicaID for the RHS to maintain the invariant that any
	// replica (uninitialized or initialized), with persistent state, has a
	// RaftReplicaID. NB: this invariant will not be universally true until we
	// introduce node startup code that will write this value for existing
	// ranges.
	if err := rsl.SetRaftReplicaID(ctx, readWriter, rightDesc.ReplicaID); err != nil {
		return err
	}
	// Persist the closed timestamp.
	//
	// In order to tolerate a nil initClosedTS input, let's forward to
	// r.GetCurrentClosedTimestamp(). Generally, initClosedTS is not expected to
	// be nil (and is expected to be in advance of r.GetCurrentClosedTimestamp()
	// since it's coming hot off a Raft command), but let's not rely on the
	// non-nil. Note that r.GetCurrentClosedTimestamp() does not yet incorporate
	// initClosedTS because the split command has not been applied yet.
	if initClosedTS == nil {
		initClosedTS = &hlc.Timestamp{}
	}
	initClosedTS.Forward(r.GetCurrentClosedTimestamp(ctx))
	return rsl.SetClosedTimestamp(ctx, readWriter, *initClosedTS)
}

// splitPostApply is the part of the split trigger which coordinates the actual
// split with the Store. Requires that Replica.raftMu is held. The deltaMS are
// the MVCC stats which apply to the RHS and have already been removed from the
// LHS.
func splitPostApply(
	ctx context.Context, deltaMS enginepb.MVCCStats, split *roachpb.SplitTrigger, r *Replica,
) {
	// rightReplOrNil will be nil if the RHS replica at the ID of the split is
	// already known to be removed, generally because we know that this store has
	// been re-added at a higher replica ID.
	rightReplOrNil := prepareRightReplicaForSplit(ctx, split, r)
	// Add the RHS replica to the store. This step atomically updates
	// the EndKey of the LHS replica and also adds the RHS replica
	// to the store's replica map.
	if err := r.store.SplitRange(ctx, r, rightReplOrNil, split); err != nil {
		// Our in-memory state has diverged from the on-disk state.
		log.Fatalf(ctx, "%s: failed to update Store after split: %+v", r, err)
	}

	// Update store stats with difference in stats before and after split.
	if rightReplOrNil != nil {
		if _, ok := rightReplOrNil.TenantID(); ok {
			// TODO(tbg): why this check to get here? Is this really checking if the RHS
			// is already initialized? But isn't it always, at this point?
			rightReplOrNil.store.metrics.addMVCCStats(ctx, rightReplOrNil.tenantMetricsRef, deltaMS)
		} else {
			log.Fatalf(ctx, "%s: found replica which is RHS of a split "+
				"without a valid tenant ID", rightReplOrNil)
		}
	}

	now := r.store.Clock().NowAsClockTimestamp()

	// While performing the split, zone config changes or a newly created table
	// might require the range to be split again. Enqueue both the left and right
	// ranges to speed up such splits. See #10160.
	r.store.splitQueue.MaybeAddAsync(ctx, r, now)
	// If the range was not properly replicated before the split, the replicate
	// queue may not have picked it up (due to the need for a split). Enqueue
	// both the left and right ranges to speed up a potentially necessary
	// replication. See #7022 and #7800.
	r.store.replicateQueue.MaybeAddAsync(ctx, r, now)

	if rightReplOrNil != nil {
		r.store.splitQueue.MaybeAddAsync(ctx, rightReplOrNil, now)
		r.store.replicateQueue.MaybeAddAsync(ctx, rightReplOrNil, now)
		if len(split.RightDesc.Replicas().Descriptors()) == 1 {
			// TODO(peter): In single-node clusters, we enqueue the right-hand side of
			// the split (the new range) for Raft processing so that the corresponding
			// Raft group is created. This shouldn't be necessary for correctness, but
			// some tests rely on this (e.g. server.TestNodeStatusWritten).
			r.store.enqueueRaftUpdateCheck(rightReplOrNil.RangeID)
		}
	}
}

// prepareRightReplicaForSplit a helper for splitPostApply.
// Requires that r.raftMu is held.
func prepareRightReplicaForSplit(
	ctx context.Context, split *roachpb.SplitTrigger, r *Replica,
) (rightReplicaOrNil *Replica) {
	// Copy out the minLeaseProposedTS and minValidObservedTimestamp from the LHS,
	// so we can assign it to the RHS. minLeaseProposedTS ensures that if the LHS
	// was not able to use its current lease because of a restart or lease
	// transfer, the RHS will also not be able to. minValidObservedTS ensures that
	// the bounds for uncertainty interval are preserved.
	r.mu.RLock()
	minLeaseProposedTS := r.mu.minLeaseProposedTS
	minValidObservedTS := r.mu.minValidObservedTimestamp
	r.mu.RUnlock()

	// The right hand side of the split was already created (and its raftMu
	// acquired) in Replica.acquireSplitLock. It must be present here.
	rightRepl := r.store.GetReplicaIfExists(split.RightDesc.RangeID)
	// If the RHS replica at the point of the split was known to be removed
	// during the application of the split then we may not find it here. That's
	// fine, carry on. See also:
	_, _ = r.acquireSplitLock, splitPostApply
	if rightRepl == nil {
		return nil
	}

	// Already holding raftMu, see above.
	rightRepl.mu.Lock()
	defer rightRepl.mu.Unlock()

	// If we know that the RHS has already been removed at this replica ID
	// then we also know that its data has already been removed by the preApply
	// so we skip initializing it as the RHS of the split.
	if rightRepl.isNewerThanSplitRLocked(split) {
		return nil
	}

	// Finish initialization of the RHS.
	err := rightRepl.loadRaftMuLockedReplicaMuLocked(&split.RightDesc)
	if err != nil {
		log.Fatalf(ctx, "%v", err)
	}

	// Copy the minLeaseProposedTS from the LHS. loadRaftMuLockedReplicaMuLocked
	// has already assigned a value for this field; this will overwrite it.
	rightRepl.mu.minLeaseProposedTS = minLeaseProposedTS

	// Copy the minValidObservedTimestamp field from the LHS.
	rightRepl.mu.minValidObservedTimestamp = minValidObservedTS

	// Invoke the leasePostApplyLocked method to ensure we properly initialize
	// the replica according to whether it holds the lease. This enables the
	// txnWaitQueue.
	rightRepl.leasePostApplyLocked(ctx,
		rightRepl.mu.state.Lease, /* prevLease */
		rightRepl.mu.state.Lease, /* newLease - same as prevLease */
		nil,                      /* priorReadSum */
		assertNoLeaseJump)

	// We need to explicitly wake up the Raft group on the right-hand range or
	// else the range could be underreplicated for an indefinite period of time.
	//
	// Specifically, suppose one of the replicas of the left-hand range never
	// applies this split trigger, e.g., because it catches up via a snapshot that
	// advances it past this split. That store won't create the right-hand replica
	// until it receives a Raft message addressed to the right-hand range. But
	// since new replicas start out quiesced, unless we explicitly awaken the
	// Raft group, there might not be any Raft traffic for quite a while.
	err = rightRepl.withRaftGroupLocked(true, func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error) {
		return true, nil
	})
	if err != nil {
		log.Fatalf(ctx, "unable to create raft group for right-hand range in split: %+v", err)
	}

	return rightRepl
}

// SplitRange shortens the original range to accommodate the new range. The new
// range is added to the ranges map and the replicasByKey btree. origRng.raftMu
// and newRng.raftMu must be held.
//
// This is only called from the split trigger in the context of the execution
// of a Raft command. Note that rightRepl will be nil if the replica described
// by rightDesc is known to have been removed.
func (s *Store) SplitRange(
	ctx context.Context, leftRepl, rightReplOrNil *Replica, split *roachpb.SplitTrigger,
) error {
	rightDesc := &split.RightDesc
	newLeftDesc := &split.LeftDesc
	oldLeftDesc := leftRepl.Desc()
	if !bytes.Equal(oldLeftDesc.EndKey, rightDesc.EndKey) ||
		bytes.Compare(oldLeftDesc.StartKey, rightDesc.StartKey) >= 0 {
		return errors.Errorf("left range is not splittable by right range: %+v, %+v", oldLeftDesc, rightDesc)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if exRng, ok := s.mu.uninitReplicas[rightDesc.RangeID]; rightReplOrNil != nil && ok {
		// If we have an uninitialized replica of the new range we require pointer
		// equivalence with rightRepl. See Store.splitTriggerPostApply().
		if exRng != rightReplOrNil {
			log.Fatalf(ctx, "found unexpected uninitialized replica: %s vs %s", exRng, rightReplOrNil)
		}
		// NB: We only remove from uninitReplicas here so that we don't leave open a
		// window where a replica is temporarily not present in Store.mu.replicas.
		delete(s.mu.uninitReplicas, rightDesc.RangeID)
	}

	leftRepl.setDescRaftMuLocked(ctx, newLeftDesc)

	// Clear the LHS lock and txn wait-queues, to redirect to the RHS if
	// appropriate. We do this after setDescWithoutProcessUpdate to ensure
	// that no pre-split commands are inserted into the wait-queues after we
	// clear them.
	leftRepl.concMgr.OnRangeSplit()

	if rightReplOrNil == nil {
		// There is no rhs replica, so instead halve the load of the lhs
		// replica.
		throwawayRightStats := NewReplicaLoad(s.Clock(), nil)
		leftRepl.loadStats.split(throwawayRightStats)
	} else {
		rightRepl := rightReplOrNil
		// Split the replica load of the lhs evenly (50:50) with the rhs. NB:
		// that this ignores the split point and makes as simplifying
		// assumption that distribution across all tracked load stats is
		// identical.
		leftRepl.loadStats.split(rightRepl.loadStats)
		if err := s.addReplicaInternalLocked(rightRepl); err != nil {
			return errors.Wrapf(err, "unable to add replica %v", rightRepl)
		}

		// Update the replica's cached byte thresholds. This is a no-op if the system
		// config is not available, in which case we rely on the next gossip update
		// to perform the update.
		if err := rightRepl.updateRangeInfo(ctx, rightRepl.Desc()); err != nil {
			return err
		}
		// Add the range to metrics and maybe gossip on capacity change.
		s.metrics.ReplicaCount.Inc(1)
		s.maybeGossipOnCapacityChange(ctx, rangeAddEvent)
	}

	return nil
}
