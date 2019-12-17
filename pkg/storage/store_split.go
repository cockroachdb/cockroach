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
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
)

// splitPreApply is called when the raft command is applied. Any
// changes to the given ReadWriter will be written atomically with the
// split commit.
func splitPreApply(
	ctx context.Context, readWriter engine.ReadWriter, split roachpb.SplitTrigger, r *Replica,
) {
	// Check on the RHS, we need to ensure that it exists and has a minReplicaID
	// less than or equal to the replica we're about to initialize.
	//
	// The right hand side of the split was already created (and its raftMu
	// acquired) in Replica.acquireSplitLock. It must be present here.
	rightRepl, err := r.store.GetReplica(split.RightDesc.RangeID)
	if err != nil {
		log.Fatalf(ctx, "unable to find RHS replica: %+v", err)
	}

	// If the RHS is not in the split, sanity check that the LHS is currently
	// catching up from a preemptive snapshot. A preemptive snapshot is
	// the only reason we should here; replicas delete themselves when they
	// apply a command which removes them from the range. The exception to that
	// is if the DisableEagerReplicaRemoval testing flag is enabled.
	//
	// TODO(ajwerner): rethink DisableEagerReplicaRemoval and remove this in
	// 20.1 after there are no more preemptive snapshots.
	_, hasRightDesc := split.RightDesc.GetReplicaDescriptor(r.StoreID())
	if !hasRightDesc {
		_, lhsExists := r.Desc().GetReplicaDescriptor(r.StoreID())
		if lhsExists {
			log.Fatalf(ctx, "cannot process split on s%s which exists in LHS and not in RHS: %+v",
				r.StoreID(), split)
		}
	}

	// Check to see if we know that the RHS has already been removed from this
	// store at the replica ID implied by the split.
	if rightRepl.isNewerThanSplit(&split) {
		// We're in the rare case where we know that the RHS has been removed
		// and re-added with a higher replica ID. We know we've never processed a
		// snapshot for the right range because up to this point it would overlap
		// with the left and ranges cannot move rightwards.
		//
		// It is important to preserve the HardState because we might however have
		// already voted at a higher term. In general this shouldn't happen because
		// we add learners and then promote them only after we snapshot but we're
		// going to be extra careful in case future versions of cockroach somehow
		// promote replicas without ensuring that a snapshot has been received.
		//
		// Clear the user data the RHS would have inherited from the LHS due to the
		// split and additionally clear all of the range ID local state that the
		// split trigger writes into the RHS.
		//
		// Rather than specifically deleting around the data we want to preserve
		// we read the HardState to preserve it, clear everything and write back
		// the HardState and tombstone.
		hs, err := rightRepl.raftMu.stateLoader.LoadHardState(ctx, readWriter)
		if err != nil {
			log.Fatalf(ctx, "failed to load hard state for removed rhs: %v", err)
		}
		const rangeIDLocalOnly = false
		const mustUseClearRange = false
		if err := clearRangeData(&split.RightDesc, readWriter, readWriter, rangeIDLocalOnly, mustUseClearRange); err != nil {
			log.Fatalf(ctx, "failed to clear range data for removed rhs: %v", err)
		}
		if err := rightRepl.raftMu.stateLoader.SetHardState(ctx, readWriter, hs); err != nil {
			log.Fatalf(ctx, "failed to set hard state with 0 commit index for removed rhs: %v", err)
		}
		if err := r.setTombstoneKey(ctx, readWriter, r.minReplicaID()); err != nil {
			log.Fatalf(ctx, "failed to set tombstone for removed rhs: %v", err)
		}
		return
	}

	// Update the raft HardState with the new Commit value now that the
	// replica is initialized (combining it with existing or default
	// Term and Vote). This is the common case.
	rsl := stateloader.Make(split.RightDesc.RangeID)
	if err := rsl.SynthesizeRaftState(ctx, readWriter); err != nil {
		log.Fatal(ctx, err)
	}
}

// splitPostApply is the part of the split trigger which coordinates the actual
// split with the Store. Requires that Replica.raftMu is held.
//
// TODO(tschottdorf): Want to merge this with SplitRange, but some legacy
// testing code calls SplitRange directly.
func splitPostApply(
	ctx context.Context, deltaMS enginepb.MVCCStats, split *roachpb.SplitTrigger, r *Replica,
) {
	// The right hand side of the split was already created (and its raftMu
	// acquired) in Replica.acquireSplitLock. It must be present here.
	rightRngOrNil, err := r.store.GetReplica(split.RightDesc.RangeID)
	if err != nil {
		log.Fatalf(ctx, "unable to find RHS replica: %+v", err)
	}
	// Already holding raftMu, see above.
	rightRngOrNil.mu.Lock()

	// If we know that the RHS has already been removed at this replica ID
	// then we also know that its data has already been removed by the preApply
	// so we skip initializing it as the RHS of the split.
	if rightRngOrNil.isNewerThanSplitRLocked(split) {
		rightRngOrNil.mu.Unlock()
		rightRngOrNil = nil
	} else {
		rightRng := rightRngOrNil
		// Finish initialization of the RHS.
		err := rightRng.initRaftMuLockedReplicaMuLocked(&split.RightDesc, r.store.Clock(), 0)
		rightRng.mu.Unlock()
		if err != nil {
			log.Fatal(ctx, err)
		}

		// This initialMaxClosedValue is created here to ensure that follower reads
		// do not regress following the split. After the split occurs there will be no
		// information in the closedts subsystem about the newly minted RHS range from
		// its leaseholder's store. Furthermore, the RHS will have a lease start time
		// equal to that of the LHS which might be quite old. This means that
		// timestamps which follow the least StartTime for the LHS part are below the
		// current closed timestamp for the LHS would no longer be readable on the RHS
		// after the split. It is critical that this call to maxClosed happen during
		// the splitPostApply so that it refers to a LAI that is equal to the index at
		// which this lease was applied. If it were to refer to a LAI after the split
		// then the value of initialMaxClosed might be unsafe.
		initialMaxClosed := r.maxClosed(ctx)
		r.mu.Lock()
		rightRng.mu.Lock()
		// Copy the minLeaseProposedTS from the LHS.
		rightRng.mu.minLeaseProposedTS = r.mu.minLeaseProposedTS
		rightRng.mu.initialMaxClosed = initialMaxClosed
		rightLease := *rightRng.mu.state.Lease
		rightRng.mu.Unlock()
		r.mu.Unlock()

		// We need to explicitly wake up the Raft group on the right-hand range or
		// else the range could be underreplicated for an indefinite period of time.
		//
		// Specifically, suppose one of the replicas of the left-hand range never
		// applies this split trigger, e.g., because it catches up via a snapshot that
		// advances it past this split. That store won't create the right-hand replica
		// until it receives a Raft message addressed to the right-hand range. But
		// since new replicas start out quiesced, unless we explicitly awaken the
		// Raft group, there might not be any Raft traffic for quite a while.
		err = rightRng.withRaftGroup(true, func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error) {
			return true, nil
		})
		if err != nil {
			log.Fatalf(ctx, "unable to create raft group for right-hand range in split: %+v", err)
		}

		// Invoke the leasePostApply method to ensure we properly initialize
		// the replica according to whether it holds the lease. This enables
		// the txnWaitQueue.
		rightRng.leasePostApply(ctx, rightLease, false /* permitJump */)
	}
	// Add the RHS replica to the store. This step atomically updates
	// the EndKey of the LHS replica and also adds the RHS replica
	// to the store's replica map.
	if err := r.store.SplitRange(ctx, r, rightRngOrNil, split); err != nil {
		// Our in-memory state has diverged from the on-disk state.
		log.Fatalf(ctx, "%s: failed to update Store after split: %+v", r, err)
	}

	// Update store stats with difference in stats before and after split.
	r.store.metrics.addMVCCStats(deltaMS)

	now := r.store.Clock().Now()

	// While performing the split, zone config changes or a newly created table
	// might require the range to be split again. Enqueue both the left and right
	// ranges to speed up such splits. See #10160.
	r.store.splitQueue.MaybeAddAsync(ctx, r, now)
	// If the range was not properly replicated before the split, the replicate
	// queue may not have picked it up (due to the need for a split). Enqueue
	// both the left and right ranges to speed up a potentially necessary
	// replication. See #7022 and #7800.
	r.store.replicateQueue.MaybeAddAsync(ctx, r, now)

	if rightRngOrNil != nil {
		r.store.splitQueue.MaybeAddAsync(ctx, rightRngOrNil, now)
		r.store.replicateQueue.MaybeAddAsync(ctx, rightRngOrNil, now)
		if len(split.RightDesc.Replicas().All()) == 1 {
			// TODO(peter): In single-node clusters, we enqueue the right-hand side of
			// the split (the new range) for Raft processing so that the corresponding
			// Raft group is created. This shouldn't be necessary for correctness, but
			// some tests rely on this (e.g. server.TestNodeStatusWritten).
			r.store.enqueueRaftUpdateCheck(rightRngOrNil.RangeID)
		}
	}

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
		// NB: We only remove from uninitReplicas and the replicaQueues maps here
		// so that we don't leave open a window where a replica is temporarily not
		// present in Store.mu.replicas.
		delete(s.mu.uninitReplicas, rightDesc.RangeID)
		s.replicaQueues.Delete(int64(rightDesc.RangeID))
	}

	leftRepl.setDesc(ctx, newLeftDesc)

	// Clear the LHS txn wait queue, to redirect to the RHS if
	// appropriate. We do this after setDescWithoutProcessUpdate
	// to ensure that no pre-split commands are inserted into the
	// txnWaitQueue after we clear it.
	leftRepl.txnWaitQueue.Clear(false /* disable */)

	// The rangefeed processor will no longer be provided logical ops for
	// its entire range, so it needs to be shut down and all registrations
	// need to retry.
	// TODO(nvanbenschoten): It should be possible to only reject registrations
	// that overlap with the new range of the split and keep registrations that
	// are only interested in keys that are still on the original range running.
	leftRepl.disconnectRangefeedWithReason(
		roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT,
	)

	// Clear the original range's request stats, since they include requests for
	// spans that are now owned by the new range.
	leftRepl.leaseholderStats.resetRequestCounts()

	if rightReplOrNil == nil {
		throwawayRightWriteStats := new(replicaStats)
		leftRepl.writeStats.splitRequestCounts(throwawayRightWriteStats)
	} else {
		rightRepl := rightReplOrNil
		leftRepl.writeStats.splitRequestCounts(rightRepl.writeStats)
		if err := s.addReplicaInternalLocked(rightRepl); err != nil {
			return errors.Errorf("unable to add replica %v: %s", rightRepl, err)
		}

		// Update the replica's cached byte thresholds. This is a no-op if the system
		// config is not available, in which case we rely on the next gossip update
		// to perform the update.
		if err := rightRepl.updateRangeInfo(rightRepl.Desc()); err != nil {
			return err
		}
		// Add the range to metrics and maybe gossip on capacity change.
		s.metrics.ReplicaCount.Inc(1)
		s.maybeGossipOnCapacityChange(ctx, rangeAddEvent)
	}

	return nil
}
