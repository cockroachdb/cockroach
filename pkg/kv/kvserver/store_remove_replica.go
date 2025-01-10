// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RemoveOptions bundles boolean parameters for Store.RemoveReplica.
type RemoveOptions struct {
	// If true, the replica's destroyStatus must be marked as removed.
	DestroyData bool
	// InsertPlaceholder can be specified when removing an initialized Replica
	// and will result in the insertion of a ReplicaPlaceholder covering the
	// keyspace previously occupied by the (now deleted) Replica.
	InsertPlaceholder bool
}

// RemoveReplica removes the replica from the store's replica map and from the
// sorted replicasByKey btree.
//
// The NextReplicaID from the replica descriptor that was used to make the
// removal decision is passed in. Removal is aborted if the replica ID has
// advanced to or beyond the NextReplicaID since the removal decision was made.
//
// If opts.DestroyReplica is false, replica.destroyRaftMuLocked is not called.
//
// The passed replica must be initialized.
func (s *Store) RemoveReplica(
	ctx context.Context, rep *Replica, nextReplicaID roachpb.ReplicaID, opts RemoveOptions,
) error {
	rep.raftMu.Lock()
	defer rep.raftMu.Unlock()
	if opts.InsertPlaceholder {
		return errors.Errorf("InsertPlaceholder not supported in RemoveReplica")
	}
	_, err := s.removeInitializedReplicaRaftMuLocked(ctx, rep, nextReplicaID, opts)
	return err
}

// removeReplicaRaftMuLocked removes the passed replica. If the replica is
// initialized the RemoveOptions will be consulted.
func (s *Store) removeReplicaRaftMuLocked(
	ctx context.Context, rep *Replica, nextReplicaID roachpb.ReplicaID, opts RemoveOptions,
) error {
	rep.raftMu.AssertHeld()
	if rep.IsInitialized() {
		if opts.InsertPlaceholder {
			return errors.Errorf("InsertPlaceholder unsupported in removeReplicaRaftMuLocked")
		}
		_, err := s.removeInitializedReplicaRaftMuLocked(ctx, rep, nextReplicaID, opts)
		return errors.Wrap(err,
			"failed to remove replica")
	}
	s.removeUninitializedReplicaRaftMuLocked(ctx, rep, nextReplicaID)
	return nil
}

// removeInitializedReplicaRaftMuLocked is the implementation of RemoveReplica,
// which is sometimes called directly when the necessary lock is already held.
// It requires that Replica.raftMu is held and that s.mu is not held.
func (s *Store) removeInitializedReplicaRaftMuLocked(
	ctx context.Context, rep *Replica, nextReplicaID roachpb.ReplicaID, opts RemoveOptions,
) (*ReplicaPlaceholder, error) {
	rep.raftMu.AssertHeld()
	if !rep.IsInitialized() {
		return nil, errors.AssertionFailedf("cannot remove uninitialized replica %s", rep)
	}

	if opts.InsertPlaceholder {
		if opts.DestroyData {
			return nil, errors.AssertionFailedf("cannot specify both InsertPlaceholder and DestroyData")
		}

	}

	// Run sanity checks and on success commit to the removal by setting the
	// destroy status. If (nil, nil) is returned, there's nothing to do.
	desc, err := func() (*roachpb.RangeDescriptor, error) {
		rep.readOnlyCmdMu.Lock()
		defer rep.readOnlyCmdMu.Unlock()
		s.mu.Lock()
		defer s.mu.Unlock()
		rep.mu.Lock()
		defer rep.mu.Unlock()

		if opts.DestroyData {
			// Detect if we were already removed.
			if rep.mu.destroyStatus.Removed() {
				return nil, nil // already removed, noop
			}
		} else {
			// If the caller doesn't want to destroy the data because it already
			// has done so, then it must have already also set the destroyStatus.
			if !rep.mu.destroyStatus.Removed() {
				return nil, errors.AssertionFailedf("replica not marked as destroyed but data already destroyed: %v", rep)
			}
		}

		// Check if Replica is in the Store.
		//
		// There is a certain amount of idempotency in this method (repeat attempts
		// to destroy an already destroyed replica are allowed when DestroyData is
		// set, see the early return above), but if we get here then the Replica
		// better be in the Store (since the Store enforces ownership over the
		// keyspace, so deleting data for a Replica that's not in the Store can
		// delete random active data). Note that if the caller has !DestroyData,
		// this means it needs to already know that the Replica is still in the
		// Store.
		if existing, ok := s.mu.replicasByRangeID.Load(rep.RangeID); !ok {
			return nil, errors.AssertionFailedf("cannot remove replica which does not exist in Store")
		} else if existing != rep {
			return nil, errors.AssertionFailedf("replica %v replaced by %v before being removed",
				rep, existing)
		}

		// Now we know that the Store's Replica is identical to the passed-in
		// Replica.
		desc := rep.shMu.state.Desc
		if repDesc, ok := desc.GetReplicaDescriptor(s.StoreID()); ok && repDesc.ReplicaID >= nextReplicaID {
			// The ReplicaID of a Replica is immutable.
			return nil, errors.AssertionFailedf("replica descriptor's ID has changed (%s >= %s)",
				repDesc.ReplicaID, nextReplicaID)
		}

		// Sanity checks passed. Mark the replica as removed before deleting data.
		rep.mu.destroyStatus.Set(kvpb.NewRangeNotFoundError(rep.RangeID, rep.StoreID()),
			destroyReasonRemoved)
		return desc, nil
	}()
	if err != nil {
		return nil, err
	}
	if desc == nil {
		// Already removed/removing, no-op.
		return nil, nil
	}

	// Proceed with the removal, all errors encountered from here down are fatal.

	// During merges, the context might have the subsuming range, so we explicitly
	// log the replica to be removed.
	log.Infof(ctx, "removing replica r%d/%d", rep.RangeID, rep.replicaID)

	s.mu.Lock()
	if it := s.getOverlappingKeyRangeLocked(desc); it.repl != rep {
		// This is a fatal error because uninitialized replicas shouldn't make it
		// this far. This method will need some changes when we introduce GC of
		// uninitialized replicas.
		s.mu.Unlock()
		log.Fatalf(ctx, "replica %+v unexpectedly overlapped by %+v", rep, it.item)
	}
	// Adjust stats before calling Destroy. This can be called before or after
	// Destroy, but this configuration helps avoid races in stat verification
	// tests.

	s.metrics.subtractMVCCStats(ctx, rep.tenantMetricsRef, rep.GetMVCCStats())
	s.metrics.ReplicaCount.Dec(1)
	s.mu.Unlock()

	// The replica will no longer exist, so cancel any rangefeed registrations.
	rep.disconnectRangefeedWithReason(
		kvpb.RangeFeedRetryError_REASON_REPLICA_REMOVED,
	)

	// Mark the replica as destroyed and (optionally) destroy the on-disk data
	// while not holding Store.mu. This is safe because we're holding
	// Replica.raftMu and the replica is present in Store.mu.replicasByKey
	// (preventing any concurrent access to the replica's key range).
	rep.disconnectReplicationRaftMuLocked(ctx)
	if opts.DestroyData {
		if err := rep.destroyRaftMuLocked(ctx, nextReplicaID); err != nil {
			return nil, err
		}
	}

	ph := func() *ReplicaPlaceholder {
		s.mu.Lock()
		defer s.mu.Unlock() // must unlock before s.scanner.RemoveReplica(), to avoid deadlock

		s.unlinkReplicaByRangeIDLocked(ctx, rep.RangeID)
		// There can't be a placeholder, as the replica is still in replicasByKey
		// and it is initialized. (A placeholder would also be in replicasByKey
		// and overlap the replica, which is impossible).
		if ph, ok := s.mu.replicaPlaceholders[rep.RangeID]; ok {
			log.Fatalf(ctx, "initialized replica %s unexpectedly had a placeholder: %+v", rep, ph)
		}
		desc := rep.Desc()
		ph := &ReplicaPlaceholder{
			rangeDesc: *roachpb.NewRangeDescriptor(desc.RangeID, desc.StartKey, desc.EndKey, desc.Replicas()),
		}

		if it := s.mu.replicasByKey.ReplaceOrInsertPlaceholder(ctx, ph); it.repl != rep {
			// We already checked that our replica was present in replicasByKey
			// above. Nothing should have been able to change that.
			log.Fatalf(ctx, "replica %+v unexpectedly overlapped by %+v", rep, it.item)
		}
		if exPH, ok := s.mu.replicaPlaceholders[desc.RangeID]; ok {
			log.Fatalf(ctx, "cannot insert placeholder %s, already have %s", ph, exPH)
		}
		s.mu.replicaPlaceholders[desc.RangeID] = ph

		if opts.InsertPlaceholder {
			return ph
		}
		// If placeholder not desired, remove it now, otherwise, that's the caller's
		// job. We could elide the placeholder altogether but wish to instead
		// minimize the divergence between the two code paths.

		s.mu.replicasByKey.DeletePlaceholder(ctx, ph)
		delete(s.mu.replicaPlaceholders, desc.RangeID)
		if it := s.getOverlappingKeyRangeLocked(desc); it.item != nil && it.item != ph {
			log.Fatalf(ctx, "corrupted replicasByKey map: %s and %s overlapped", rep, it.item)
		}
		return nil
	}()

	s.storeGossip.MaybeGossipOnCapacityChange(ctx, RangeRemoveEvent)
	s.scanner.RemoveReplica(rep)
	return ph, nil
}

// removeUninitializedReplicaRaftMuLocked removes an uninitialized replica.
// All paths which call this code held the raftMu before calling this method
// and ensured that the removal was sane given the current replicaID and
// initialization status (which only changes under the raftMu).
func (s *Store) removeUninitializedReplicaRaftMuLocked(
	ctx context.Context, rep *Replica, nextReplicaID roachpb.ReplicaID,
) {
	rep.raftMu.AssertHeld()

	// Sanity check this removal and set the destroyStatus.
	{
		rep.readOnlyCmdMu.Lock()
		rep.mu.Lock()

		// Detect if we were already removed, this is a fatal error
		// because we should have already checked this under the raftMu
		// before calling this method.
		if rep.mu.destroyStatus.Removed() {
			rep.mu.Unlock()
			rep.readOnlyCmdMu.Unlock()
			log.Fatalf(ctx, "uninitialized replica unexpectedly already removed")
		}

		if rep.IsInitialized() {
			rep.mu.Unlock()
			rep.readOnlyCmdMu.Unlock()
			log.Fatalf(ctx, "cannot remove initialized replica in removeUninitializedReplica: %v", rep)
		}

		// Mark the replica as removed before deleting data.
		rep.mu.destroyStatus.Set(kvpb.NewRangeNotFoundError(rep.RangeID, rep.StoreID()),
			destroyReasonRemoved)

		rep.mu.Unlock()
		rep.readOnlyCmdMu.Unlock()
	}

	// Proceed with the removal.

	rep.disconnectReplicationRaftMuLocked(ctx)
	if err := rep.destroyRaftMuLocked(ctx, nextReplicaID); err != nil {
		log.Fatalf(ctx, "failed to remove uninitialized replica %v: %v", rep, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Sanity check, could be removed.
	existing, stillExists := s.mu.replicasByRangeID.Load(rep.RangeID)
	if !stillExists {
		log.Fatalf(ctx, "uninitialized replica was removed in the meantime")
	}
	if existing == rep {
		log.Infof(ctx, "removing uninitialized replica %v", rep)
	} else {
		log.Fatalf(ctx, "uninitialized replica %v was unexpectedly replaced", existing)
	}

	s.unlinkReplicaByRangeIDLocked(ctx, rep.RangeID)
}

// unlinkReplicaByRangeIDLocked removes all of the store's references to the
// provided replica that are keyed by its range ID. The replica may also need
// to be removed from the replicasByKey map.
//
// store.mu must be held.
func (s *Store) unlinkReplicaByRangeIDLocked(ctx context.Context, rangeID roachpb.RangeID) {
	s.mu.AssertHeld()
	s.unquiescedOrAwakeReplicas.Lock()
	delete(s.unquiescedOrAwakeReplicas.m, rangeID)
	s.unquiescedOrAwakeReplicas.Unlock()
	delete(s.mu.uninitReplicas, rangeID)
	s.mu.replicasByRangeID.Delete(rangeID)
	s.unregisterLeaseholderByID(ctx, rangeID)
	s.raftRecvQueues.Delete(rangeID)
	s.scheduler.RemovePriorityID(rangeID)
}

// removePlaceholder removes a placeholder for the specified range.
// Requires that the raftMu of the replica whose place is being held
// is locked. See removePlaceholderType for existence semantics.
func (s *Store) removePlaceholder(
	ctx context.Context, ph *ReplicaPlaceholder, typ removePlaceholderType,
) (removed bool, _ error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removePlaceholderLocked(ctx, ph, typ)
}

type removePlaceholderType byte

const (
	// The placeholder was filled, i.e. the snapshot was applied successfully.
	// This is only legal to use when the placeholder exists, and so in particular
	// it can't be invoked multiple times.
	removePlaceholderFilled removePlaceholderType = iota
	// Raft didn't apply snapshot. Note that this is only counting snapshots
	// that raft dropped in the presence of a placeholder, which means snapshots
	// that targeted an uninitialized replica. There is currently no reason for
	// those to ever be dropped, if anything snapshots that get dropped would
	// target an already initialized replica, but those will not augment the
	// metric related to this const.
	//
	// This type allows idempotent deletion, i.e. it can be invoked even if the
	// placeholder has already been filled or removed before, simplifying the
	// cleanup on failed operations.
	removePlaceholderDropped
	// The snapshot never got to raft, i.e. failed during receipt, for example.
	// This does not account for failed snapshots targeting initialized replicas.
	//
	// This type allows idempotent deletion, i.e. it can be invoked even if the
	// placeholder has already been filled or removed before, simplifying the
	// cleanup on failed operations.
	removePlaceholderFailed
)

// removePlaceholderLocked removes a placeholder for the specified range.
// Requires that the raftMu of the replica whose place is being held
// is locked. See removePlaceholderType for existence semantics.
//
// If typ is removePlaceholderFilled, an error is returned unless `removed`
// is true. For the other types, removal is idempotent.
func (s *Store) removePlaceholderLocked(
	ctx context.Context, inPH *ReplicaPlaceholder, typ removePlaceholderType,
) (removed bool, _ error) {
	rngID := inPH.Desc().RangeID
	placeholder, ok := s.mu.replicaPlaceholders[rngID]

	if wasTainted := !atomic.CompareAndSwapInt32(&inPH.tainted, 0, 1); wasTainted {
		if typ == removePlaceholderFilled {
			// If we're filling a placeholder, we do so exactly once. This is a bug,
			// and could cause correctness problems due to improperly synchronized
			// overlapping snapshots.
			return false, errors.AssertionFailedf(
				"attempting to fill tainted placeholder %+v (stored placeholder: %+v)", inPH, placeholder,
			)
		}
		// "Our" placeholder (inPH) was already handled by an earlier call, so if
		// there is one now, it better be someone else's.
		if ok && inPH == placeholder {
			return false, errors.AssertionFailedf(
				"tainted placeholder %+v unexpectedly present in replicaPlaceholders: %+v", inPH, placeholder,
			)

		}
		return false, nil
	}

	// We were the ones to taint the placeholder, so it must exist in
	// replicaPlaceholders at this point. We really shouldn't hit this assertion
	// even if there is a bug; if anything we would hit the one above in the
	// wasTainted branch, but better safe than sorry.
	if !ok {
		return false, errors.AssertionFailedf("expected placeholder %+v to exist", inPH)
	}

	if placeholder != inPH {
		// The placeholder acts as a lock, and when we're filling or dropping it we
		// only do so once, so how would we now see a different placeholder from the
		// one we previously inserted? There must be a bug.
		return false, errors.AssertionFailedf(
			"placeholder %+v is being dropped or filled, but store has conflicting placeholder %+v",
			inPH, placeholder,
		)
	}

	// Unlink the placeholder from the store.
	if it := s.mu.replicasByKey.DeletePlaceholder(ctx, placeholder); it.ph != placeholder {
		return false, errors.AssertionFailedf("placeholder %+v not found, got %+v", placeholder, it)
	}
	delete(s.mu.replicaPlaceholders, rngID)
	if it := s.getOverlappingKeyRangeLocked(&placeholder.rangeDesc); it.item != nil {
		return false, errors.AssertionFailedf("corrupted replicasByKey map: %+v and %+v overlapped", it.ph, it.item)
	}
	switch typ {
	case removePlaceholderDropped:
		atomic.AddInt32(&s.counts.droppedPlaceholders, 1)
	case removePlaceholderFailed:
		atomic.AddInt32(&s.counts.failedPlaceholders, 1)
	case removePlaceholderFilled:
		atomic.AddInt32(&s.counts.filledPlaceholders, 1)
	}
	return true, nil
}
