// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// splitPreApplyInput contains input for the RHS replica required for
// splitPreApply.
type splitPreApplyInput struct {
	// destroyed is set to true iff the RHS replica (the one with the matching
	// ReplicaID from the SplitTrigger used to construct a splitPreApplyInput) has
	// already been removed from the store.
	//
	// If the RHS replica has already been destroyed on the store, then split
	// application entails "throwing away" the data that would have belonged to
	// the RHS. Simply put, user data belonging to the RHS needs to be cleared and
	// any RangeID-local replicated state in the split batch also needs to be
	// cleared.
	destroyed bool
	// rhsDesc is the descriptor for the post split RHS range.
	rhsDesc roachpb.RangeDescriptor
	// initClosedTimestamp is the initial closed timestamp that the RHS replica
	// inherits from the pre-split range. Set iff destroyed is false.
	initClosedTimestamp hlc.Timestamp
}

// validateAndPrepareSplit performs invariant checks on the supplied
// splitTrigger and, assuming they hold, returns the corresponding input that
// should be passed to splitPreApply.
//
// initClosedTS is the closed timestamp carried by the split command. It will be
// used to initialize the closed timestamp of the RHS replica.
func validateAndPrepareSplit(
	ctx context.Context, r *Replica, split roachpb.SplitTrigger, initClosedTS *hlc.Timestamp,
) (splitPreApplyInput, error) {
	// Sanity check that the store is in the split.
	splitRightReplDesc, hasRightDesc := split.RightDesc.GetReplicaDescriptor(r.StoreID())
	_, hasLeftDesc := split.LeftDesc.GetReplicaDescriptor(r.StoreID())
	if !hasRightDesc || !hasLeftDesc {
		return splitPreApplyInput{}, errors.AssertionFailedf("cannot process split on s%s which does not exist in the split: %+v",
			r.StoreID(), split)
	}

	// Try to obtain the RHS replica. In the common case, it exists and its
	// ReplicaID matches the one in the split trigger. In the less common case,
	// the ReplicaID has already been removed from this store, and it may have
	// been re-added with a higher ReplicaID one or more times. We use this to
	// inform the destroyed field.
	rightRepl := r.store.GetReplicaIfExists(split.RightDesc.RangeID)
	if rightRepl == nil || rightRepl.isNewerThanSplit(&split) {
		// We're in the rare case where we know that the RHS has been removed or
		// re-added with a higher replica ID (one or more times).
		//
		// NB: the rightRepl == nil condition is flaky, in a sense that the RHS
		// replica can be created or destroyed concurrently here, one or more times.
		// This is because the RHS replica is not locked if its ReplicaID does not
		// match the one in the SplitTrigger. But we only use it for a best-effort
		// assertion, so this is not critical.
		if rightRepl != nil {
			// Assert that the rightRepl is not initialized. We're about to clear out
			// the data of the RHS of the split; we cannot have already accepted a
			// snapshot to initialize this newer RHS.
			if rightRepl.IsInitialized() {
				return splitPreApplyInput{}, errors.AssertionFailedf(
					"unexpectedly found initialized newer RHS of split: %v", rightRepl.Desc(),
				)
			}
		}

		return splitPreApplyInput{destroyed: true, rhsDesc: split.RightDesc}, nil
	}
	// Sanity check the common case -- the RHS replica that exists should match
	// the ReplicaID in the split trigger. In particular, it shouldn't older than
	// the one in the split trigger; we've already checked for the newer case
	// above.
	testingAssert(rightRepl.replicaID == splitRightReplDesc.ReplicaID,
		"expected RHS replica ID to match split trigger replica ID",
	)

	// In order to tolerate a nil initClosedTS input, let's forward to
	// r.GetCurrentClosedTimestamp(). Generally, initClosedTS is not expected to
	// be nil (and is expected to be in advance of r.GetCurrentClosedTimestamp()
	// since it's coming hot off a Raft command), but let's not rely on the
	// non-nil. Note that r.GetCurrentClosedTimestamp() does not yet incorporate
	// initClosedTS because the split command has not been applied yet.
	//
	// TODO(arul): we should avoid this and have splits always carry a non-nil
	// initial closed timestamp; see
	// https://github.com/cockroachdb/cockroach/issues/148972.
	if initClosedTS == nil {
		initClosedTS = &hlc.Timestamp{}
	}
	initClosedTS.Forward(r.GetCurrentClosedTimestamp(ctx))

	return splitPreApplyInput{
		destroyed:           false,
		rhsDesc:             split.RightDesc,
		initClosedTimestamp: *initClosedTS,
	}, nil
}

// splitPreApply is called when the raft command is applied. Any
// changes to the given ReadWriter will be written atomically with the
// split commit.
func splitPreApply(
	ctx context.Context, stateRW kvstorage.StateRW, raftRW kvstorage.Raft, in splitPreApplyInput,
) {
	rsl := kvstorage.MakeStateLoader(in.rhsDesc.RangeID)
	// After PR #149620, the split trigger batch may only contain replicated state
	// machine keys, and never contains unreplicated / raft keys. One exception:
	// there can still be historical split proposals that write the initial
	// RaftTruncatedState of the RHS. Remove this key (if exists), and set it
	// below only if necessary.
	//
	// Note that if the RHS range is already present or being created concurrently
	// on this Store, it doesn't have a RaftTruncatedState (which only initialized
	// replicas can have), so this deletion will not conflict with or corrupt it.
	//
	// NB: the key is cleared in stateRW rather than raftRW, deliberately. It
	// lives in the raft engine, but here we want to clear it from the state
	// engine batch, so that it doesn't make it to the state engine.
	//
	// TODO(#152847): remove this workaround when there are no historical
	// proposals with RaftTruncatedState, e.g. after a below-raft migration.
	if ts, err := rsl.LoadRaftTruncatedState(ctx, stateRW); err != nil {
		log.KvExec.Fatalf(ctx, "cannot load RaftTruncatedState: %v", err)
	} else if ts == (kvserverpb.RaftTruncatedState{}) {
		// Common case. Do nothing.
	} else if err := rsl.ClearRaftTruncatedState(stateRW); err != nil {
		log.KvExec.Fatalf(ctx, "cannot clear RaftTruncatedState: %v", err)
	}

	if in.destroyed {
		// The RHS replica has already been removed from the store. To apply the
		// split, we must clear the user data the RHS would have inherited from the
		// LHS due to the split. Additionally, we also want to clear any
		// RangeID-local replicated keys in the split batch.
		//
		// Note that we leave the RangeID-local state intact, since it either
		// belongs to a newer replica or does not exist. For the former case, when a
		// newer RHS replica exists on this store, it must be uninitialized (this
		// was asserted in validateAndPrepareSplit). Uninitialized replicas do not have any
		// replicated state; however, at the time of writing, they do have non-empty
		// RaftReplicaID and RaftHardState keys in storage. More generally, they are
		// allowed to have unreplicated keys. As a rule of thumb, all unreplicated
		// keys belong to the *current* ReplicaID in the store, rather than the
		// ReplicaID in the split trigger (which in this case, is stale).
		if err := kvstorage.RemoveStaleRHSFromSplit(
			ctx, kvstorage.WrapState(stateRW), in.rhsDesc.RangeID, in.rhsDesc.RSpan(),
		); err != nil {
			log.KvExec.Fatalf(ctx, "failed to clear range data for removed rhs: %v", err)
		}
		return
	}

	// The RHS replica exists and is uninitialized. We are initializing it here.
	// This is the common case.
	//
	// Update the raft HardState with the new Commit index (taken from the applied
	// state in the write batch), and use existing[*] or default Term and Vote.
	// Also write the initial RaftTruncatedState.
	//
	// [*] Note that uninitialized replicas may cast votes, and if they have, we
	// can't load the default Term and Vote values.
	if err := rsl.SynthesizeRaftState(ctx, stateRW, raftRW); err != nil {
		log.KvExec.Fatalf(ctx, "%v", err)
	}
	if err := rsl.SetRaftTruncatedState(ctx, raftRW.WO, &kvserverpb.RaftTruncatedState{
		Index: kvstorage.RaftInitialLogIndex,
		Term:  kvstorage.RaftInitialLogTerm,
	}); err != nil {
		log.KvExec.Fatalf(ctx, "%v", err)
	}
	// Persist the closed timestamp.
	if err := rsl.SetClosedTimestamp(ctx, stateRW, in.initClosedTimestamp); err != nil {
		log.KvExec.Fatalf(ctx, "%s", err)
	}
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
		log.KvExec.Fatalf(ctx, "%s: failed to update Store after split: %+v", r, err)
	}

	// Explicitly unquiesce the Raft group on the right-hand range or else the
	// range could be underreplicated for an indefinite period of time.
	//
	// Specifically, suppose one of the replicas of the left-hand range never
	// applies this split trigger, e.g., because it catches up via a snapshot that
	// advances it past this split. That store won't create the right-hand replica
	// until it receives a Raft message addressed to the right-hand range. But
	// since new replicas start out quiesced, unless we explicitly awaken the
	// Raft group, there might not be any Raft traffic for quite a while.
	if rightReplOrNil != nil {
		rightReplOrNil.mu.Lock()
		rightReplOrNil.maybeUnquiesceLocked(true /* wakeLeader */, true /* mayCampaign */)
		rightReplOrNil.mu.Unlock()
	}

	// Update store stats with difference in stats before and after split.
	if rightReplOrNil != nil {
		rightReplOrNil.store.metrics.addMVCCStats(ctx, rightReplOrNil.tenantMetricsRef, deltaMS)
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

	// If the RHS replica of the split is not removed, then it has been obtained
	// (and its raftMu acquired) in Replica.acquireSplitLock.
	rightRepl := r.store.GetReplicaIfExists(split.RightDesc.RangeID)
	// If the RHS replica of the split has been removed then we either not find it
	// here, or find a one with a later replica ID. In this case we also know that
	// its data has already been removed by splitPreApply, so we skip initializing
	// this replica. See also:
	_, _ = r.acquireSplitLock, splitPostApply
	if rightRepl == nil || rightRepl.isNewerThanSplit(split) {
		return nil
	}
	// Finish initialization of the RHS replica.

	state, err := kvstorage.LoadReplicaState(
		ctx, r.store.StateEngine(), r.store.LogEngine(),
		r.StoreID(), &split.RightDesc, rightRepl.replicaID,
	)
	if err != nil {
		log.KvExec.Fatalf(ctx, "%v", err)
	}

	// Already holding raftMu, see above.
	rightRepl.mu.Lock()
	defer rightRepl.mu.Unlock()
	if err := rightRepl.initRaftMuLockedReplicaMuLocked(
		state, false, /* waitForPrevLeaseToExpire */
	); err != nil {
		log.KvExec.Fatalf(ctx, "%v", err)
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
		rightRepl.shMu.state.Lease, /* prevLease */
		rightRepl.shMu.state.Lease, /* newLease - same as prevLease */
		nil,                        /* priorReadSum */
		assertNoLeaseJump)

	if fn := r.store.TestingKnobs().AfterSplitApplication; fn != nil {
		// TODO(pav-kv): we have already checked up the stack that rightDesc exists,
		// but maybe it would be better to carry a "bus" struct with these kinds of
		// data post checks so that we (a) don't need to repeat the computation /
		// validation, (b) can be sure that it's consistent.
		rightDesc, _ := split.RightDesc.GetReplicaDescriptor(r.StoreID())
		fn(rightDesc, rightRepl.shMu.state)
	}

	return rightRepl
}

// SplitRange shortens the original range to accommodate the new range. The new
// range is added to the ranges map and the replicasByKey btree. origRng.raftMu
// and newRng.raftMu must be held.
//
// This is only called from the split trigger in the context of the execution of
// a Raft command. rightReplOrNil will be nil if it is known to have been
// removed. Otherwise, it is marked as initialized before SplitRange returns.
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
	leftRepl.setDescRaftMuLocked(ctx, newLeftDesc)

	// Clear or split the LHS lock and txn wait-queues, to redirect to the RHS if
	// appropriate. We do this after setDescWithoutProcessUpdate to ensure
	// that no pre-split commands are inserted into the wait-queues after we
	// clear them.
	locksToAcquireOnRHS := leftRepl.concMgr.OnRangeSplit(roachpb.Key(rightDesc.StartKey))

	if rightReplOrNil == nil {
		// There is no RHS replica, so (heuristically) halve the load stats for the
		// LHS, instead of splitting it between LHS and RHS.
		throwawayRightStats := load.NewReplicaLoad(s.Clock(), nil)
		leftRepl.loadStats.Split(throwawayRightStats)
		return nil
	}
	rightRepl := rightReplOrNil

	// Acquire unreplicated locks on the RHS. We expect locksToAcquireOnRHS to be
	// empty if UnreplicatedLockReliabilityUpgrade is false.
	if beforeFn := s.TestingKnobs().BeforeSplitAcquiresLocksOnRHS; beforeFn != nil {
		beforeFn(ctx, rightRepl)
	}
	log.KvExec.VInfof(ctx, 2, "acquiring %d locks on the RHS", len(locksToAcquireOnRHS))
	for _, l := range locksToAcquireOnRHS {
		rightRepl.concMgr.OnLockAcquired(ctx, &l)
	}

	// Split the replica load of the LHS evenly (50:50) with the RHS. NB: this
	// ignores the split point, and makes as simplifying assumption that
	// distribution across all tracked load stats is identical.
	leftRepl.loadStats.Split(rightRepl.loadStats)

	// Update the replica's cached byte thresholds. This is a no-op if the system
	// config is not available, in which case we rely on the next gossip update to
	// perform the update.
	if err := rightRepl.updateRangeInfo(ctx, rightDesc); err != nil {
		return err
	}

	rightRepl.mu.Lock()
	defer rightRepl.mu.Unlock()
	rightRepl.isInitialized.Store(true)
	return s.markReplicaInitializedLockedReplLocked(ctx, rightRepl)
}
