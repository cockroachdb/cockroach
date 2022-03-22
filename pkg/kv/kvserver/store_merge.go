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
	"runtime"
	"runtime/debug"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// maybeAssertNoHole, if enabled (see within), starts a watcher that
// periodically checks the replicasByKey btree for any gaps in the monitored
// span `[from,to)`. Any gaps trigger a fatal error. The caller must eventually
// invoke the returned closure, which will stop the checks.
func (s *Store) maybeAssertNoHole(ctx context.Context, from, to roachpb.RKey) func() {
	// Access to replicasByKey is unfortunately deadlock-prone. We can enable this
	// after we've revisited the locking, see:
	//
	// https://github.com/cockroachdb/cockroach/issues/74384.
	//
	// Until then, this is still useful for checking individual tests known not to
	// experience the deadlock. Even tests that have the deadlock can still be checked
	// meaningfully by removing the `<-goroutineStopped` from the returned closure;
	// this allows for a theoretical false positive should the watched keyspace later
	// experience a replicaGC, but this should be rare and in many tests impossible.
	const disabled = true
	if disabled {
		return func() {}
	}

	goroutineStopped := make(chan struct{})
	caller := string(debug.Stack())
	if from.Equal(roachpb.RKeyMax) {
		// There will be a hole to the right of RKeyMax but it's just the end of
		// the addressable keyspace.
		return func() {}
	}
	if from.Equal(to) {
		// Nothing to do.
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	if s.stopper.RunAsyncTask(ctx, "force-assertion", func(ctx context.Context) {
		defer close(goroutineStopped)
		for ctx.Err() == nil {
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				var last replicaOrPlaceholder
				err := s.mu.replicasByKey.VisitKeyRange(
					context.Background(), from, to, AscendingKeyOrder,
					func(ctx context.Context, cur replicaOrPlaceholder) error {
						// TODO(tbg): this deadlocks, see #74384. By disabling this branch,
						// the only check that we perform is the one that the monitored
						// keyspace isn't all a gap.
						if last.item != nil {
							gapStart, gapEnd := last.Desc().EndKey, cur.Desc().StartKey
							if !gapStart.Equal(gapEnd) {
								return errors.AssertionFailedf(
									"found hole [%s,%s) in keyspace [%s,%s), during:\n%s",
									gapStart, gapEnd, from, to, caller,
								)
							}
						}
						last = cur
						return nil
					})
				if err != nil {
					log.Fatalf(ctx, "%v", err)
				}
				if last.item == nil {
					log.Fatalf(ctx, "found hole in keyspace [%s,%s), during:\n%s", from, to, caller)
				}
				runtime.Gosched()
			}()
		}
	}) != nil {
		close(goroutineStopped)
	}
	return func() {
		cancel()
		<-goroutineStopped
	}
}

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
	defer s.maybeAssertNoHole(ctx, leftRepl.Desc().EndKey, newLeftDesc.EndKey)()
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

	// Note that we were called (indirectly) from raft processing so we must
	// call removeInitializedReplicaRaftMuLocked directly to avoid deadlocking
	// on the right-hand replica's raftMu.
	//
	// We ask removeInitializedReplicaRaftMuLocked to install a placeholder which
	// we'll drop atomically with extending the right-hand side down below.
	ph, err := s.removeInitializedReplicaRaftMuLocked(ctx, rightRepl, rightDesc.NextReplicaID, RemoveOptions{
		// The replica was destroyed by the tombstones added to the batch in
		// runPreApplyTriggersAfterStagingWriteBatch.
		DestroyData:       false,
		InsertPlaceholder: true,
	})
	if err != nil {
		return errors.Wrap(err, "cannot remove range")
	}

	if err := rightRepl.postDestroyRaftMuLocked(ctx, rightRepl.GetMVCCStats()); err != nil {
		return err
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
			// For an explanation about why we need to update out clock with the
			// merge's freezeStart, see "Range merges" in pkg/util/hlc/doc.go. Also,
			// see the comment on TestStoreRangeMergeTimestampCacheCausality.
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

	// Update the subsuming range's descriptor, atomically widening it while
	// dropping the placeholder representing the right-hand side.
	s.mu.Lock()
	defer s.mu.Unlock()
	removed, err := s.removePlaceholderLocked(ctx, ph, removePlaceholderFilled)
	if err != nil {
		return err
	}
	if !removed {
		return errors.AssertionFailedf("did not find placeholder %s", ph)
	}
	// NB: we have to be careful not to lock leftRepl before this step, as
	// removePlaceholderLocked traverses the replicasByKey btree and may call
	// leftRepl.Desc().
	leftRepl.mu.Lock()
	defer leftRepl.mu.Unlock()
	leftRepl.setDescLockedRaftMuLocked(ctx, &newLeftDesc)
	return nil
}
