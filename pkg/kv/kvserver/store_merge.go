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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// MergeRange expands the left-hand replica, leftRepl, to absorb the right-hand
// replica, identified by rightDesc. freezeStart specifies the time at which the
// right-hand replica promised to stop serving traffic and is used to initialize
// the timestamp cache's low water mark for the right-hand keyspace. The
// right-hand replica must exist on this store and the raftMus for both the
// left-hand and right-hand replicas must be held.
func (s *Store) MergeRange(
	ctx context.Context,
	leftRepl *Replica,
	newLeftDesc, rightDesc roachpb.RangeDescriptor,
	freezeStart hlc.ClockTimestamp,
	rightClosedTS hlc.Timestamp,
	rightReadSum *rspb.ReadSummary,
) error {
	if oldLeftDesc := leftRepl.Desc(); !oldLeftDesc.EndKey.Less(newLeftDesc.EndKey) {
		return errors.Errorf("the new end key is not greater than the current one: %+v <= %+v",
			newLeftDesc.EndKey, oldLeftDesc.EndKey)
	}

	rightRepl, err := s.GetReplica(rightDesc.RangeID)
	if err != nil {
		return err
	}

	leftRepl.raftMu.AssertHeld()
	rightRepl.raftMu.AssertHeld()

	if err := rightRepl.postDestroyRaftMuLocked(ctx, rightRepl.GetMVCCStats()); err != nil {
		return err
	}

	// Note that we were called (indirectly) from raft processing so we must
	// call removeInitializedReplicaRaftMuLocked directly to avoid deadlocking
	// on the right-hand replica's raftMu.
	if err := s.removeInitializedReplicaRaftMuLocked(ctx, rightRepl, rightDesc.NextReplicaID, RemoveOptions{
		// The replica was destroyed by the tombstones added to the batch in
		// runPreApplyTriggersAfterStagingWriteBatch.
		DestroyData: false,
	}); err != nil {
		return errors.Errorf("cannot remove range: %s", err)
	}

	if leftRepl.leaseholderStats != nil {
		leftRepl.leaseholderStats.resetRequestCounts()
	}
	if leftRepl.writeStats != nil {
		// Note: this could be drastically improved by adding a replicaStats method
		// that merges stats. Resetting stats is typically bad for the rebalancing
		// logic that depends on them.
		leftRepl.writeStats.resetRequestCounts()
	}

	// Clear the concurrency manager's lock and txn wait-queues to redirect the
	// queued transactions to the left-hand replica, if necessary.
	rightRepl.concMgr.OnRangeMerge()

	leftLease, _ := leftRepl.GetLease()
	rightLease, _ := rightRepl.GetLease()
	if leftLease.OwnedBy(s.Ident.StoreID) {
		if !rightLease.OwnedBy(s.Ident.StoreID) {
			// We hold the lease for the LHS, but do not hold the lease for the RHS.
			// That means we don't have up-to-date timestamp cache entries for the
			// keyspace previously owned by the RHS. Update the timestamp cache for
			// the RHS keyspace. If the merge trigger included a prior read summary
			// then we can use that directly to update the timestamp cache.
			// Otherwise, we pessimistically assume that the right-hand side served
			// reads all the way up to freezeStart, the time at which the RHS
			// promised to stop serving traffic.
			//
			// Note that we need to update our clock with freezeStart to preserve
			// the invariant that our clock is always greater than or equal to any
			// timestamps in the timestamp cache. For a full discussion, see the
			// comment on TestStoreRangeMergeTimestampCacheCausality.
			s.Clock().Update(freezeStart)

			var sum rspb.ReadSummary
			if rightReadSum != nil {
				sum = *rightReadSum
			} else {
				sum = rspb.FromTimestamp(freezeStart.ToTimestamp())
			}
			applyReadSummaryToTimestampCache(s.tsCache, &rightDesc, sum)
		}
		// When merging ranges, the closed timestamp of the RHS can regress. It's
		// possible that, at subsumption time, the RHS had a high closed timestamp.
		// Being ingested by the LHS, the closed timestamp of the RHS is lost, and
		// the LHS's closed timestamp takes over the respective keys. In order to
		// not violate reads that might have been performed by the RHS according to
		// the old closed ts (either by the leaseholder or by followers), we bump
		// the timestamp cache.
		// In the case when the RHS lease was not collocated with the LHS, this bump
		// is frequently (but not necessarily) redundant with the bumping to the
		// freeze time done above.
		sum := rspb.FromTimestamp(rightClosedTS)
		applyReadSummaryToTimestampCache(s.tsCache, &rightDesc, sum)
	}

	// Update the subsuming range's descriptor.
	leftRepl.setDescRaftMuLocked(ctx, &newLeftDesc)
	return nil
}
