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
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RemoveOptions bundles boolean parameters for Store.RemoveReplica.
type RemoveOptions struct {
	DestroyData bool

	// ignoreDestroyStatus allows a caller to instruct the store to remove
	// replicas which are already marked as destroyed. This is helpful in cases
	// where the caller knows that it set the destroy status and cannot have raced
	// with another goroutine. See Replica.handleChangeReplicasResult().
	ignoreDestroyStatus bool
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
	return s.removeInitializedReplicaRaftMuLocked(ctx, rep, nextReplicaID, opts)
}

// removeReplicaRaftMuLocked removes the passed replica. If the replica is
// initialized the RemoveOptions will be consulted.
func (s *Store) removeReplicaRaftMuLocked(
	ctx context.Context, rep *Replica, nextReplicaID roachpb.ReplicaID, opts RemoveOptions,
) error {
	rep.raftMu.AssertHeld()
	if rep.IsInitialized() {
		return errors.Wrap(s.removeInitializedReplicaRaftMuLocked(ctx, rep, nextReplicaID, opts),
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
) error {
	rep.raftMu.AssertHeld()

	// Sanity checks before committing to the removal by setting the
	// destroy status.
	var desc *roachpb.RangeDescriptor
	var replicaID roachpb.ReplicaID
	{
		rep.mu.Lock()

		// Detect if we were already removed.
		if !opts.ignoreDestroyStatus && rep.mu.destroyStatus.Removed() {
			rep.mu.Unlock()
			return nil // already removed, noop
		}

		desc = rep.mu.state.Desc
		if repDesc, ok := desc.GetReplicaDescriptor(s.StoreID()); ok && repDesc.ReplicaID >= nextReplicaID {
			rep.mu.Unlock()
			// NB: This should not in any way be possible starting in 20.1.
			log.Fatalf(ctx, "replica descriptor's ID has changed (%s >= %s)",
				repDesc.ReplicaID, nextReplicaID)
		}

		// This is a fatal error as an initialized replica can never become
		/// uninitialized.
		if !rep.isInitializedRLocked() {
			rep.mu.Unlock()
			log.Fatalf(ctx, "uninitialized replica cannot be removed with removeInitializedReplica: %v",
				rep)
		}

		// Mark the replica as removed before deleting data.
		rep.mu.destroyStatus.Set(roachpb.NewRangeNotFoundError(rep.RangeID, rep.StoreID()),
			destroyReasonRemoved)
		replicaID = rep.mu.replicaID
		rep.mu.Unlock()
	}

	// Proceed with the removal, all errors encountered from here down are fatal.

	// Another sanity check that this replica is a part of this store.
	existing, err := s.GetReplica(rep.RangeID)
	if err != nil {
		log.Fatalf(ctx, "cannot remove replica which does not exist: %v", err)
	} else if existing != rep {
		log.Fatalf(ctx, "replica %v replaced by %v before being removed",
			rep, existing)
	}

	// During merges, the context might have the subsuming range, so we explicitly
	// log the replica to be removed.
	log.Infof(ctx, "removing replica r%d/%d", rep.RangeID, replicaID)

	s.mu.Lock()
	if placeholder := s.getOverlappingKeyRangeLocked(desc); placeholder != rep {
		// This is a fatal error because uninitialized replicas shouldn't make it
		// this far. This method will need some changes when we introduce GC of
		// uninitialized replicas.
		s.mu.Unlock()
		log.Fatalf(ctx, "replica %+v unexpectedly overlapped by %+v", rep, placeholder)
	}
	// Adjust stats before calling Destroy. This can be called before or after
	// Destroy, but this configuration helps avoid races in stat verification
	// tests.
	s.metrics.subtractMVCCStats(rep.GetMVCCStats())
	s.metrics.ReplicaCount.Dec(1)
	s.mu.Unlock()

	// The replica will no longer exist, so cancel any rangefeed registrations.
	rep.disconnectRangefeedWithReason(
		roachpb.RangeFeedRetryError_REASON_REPLICA_REMOVED,
	)

	// Mark the replica as destroyed and (optionally) destroy the on-disk data
	// while not holding Store.mu. This is safe because we're holding
	// Replica.raftMu and the replica is present in Store.mu.replicasByKey
	// (preventing any concurrent access to the replica's key range).
	rep.disconnectReplicationRaftMuLocked(ctx)
	if opts.DestroyData {
		if err := rep.destroyRaftMuLocked(ctx, nextReplicaID); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.unlinkReplicaByRangeIDLocked(rep.RangeID)
	if placeholder := s.mu.replicasByKey.Delete(rep); placeholder != rep {
		// We already checked that our replica was present in replicasByKey
		// above. Nothing should have been able to change that.
		log.Fatalf(ctx, "replica %+v unexpectedly overlapped by %+v", rep, placeholder)
	}
	if rep2 := s.getOverlappingKeyRangeLocked(desc); rep2 != nil {
		log.Fatalf(ctx, "corrupted replicasByKey map: %s and %s overlapped", rep, rep2)
	}
	delete(s.mu.replicaPlaceholders, rep.RangeID)
	// TODO(peter): Could release s.mu.Lock() here.
	s.maybeGossipOnCapacityChange(ctx, rangeRemoveEvent)
	s.scanner.RemoveReplica(rep)
	return nil
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
		rep.mu.Lock()

		// Detect if we were already removed, this is a fatal error
		// because we should have already checked this under the raftMu
		// before calling this method.
		if rep.mu.destroyStatus.Removed() {
			rep.mu.Unlock()
			log.Fatalf(ctx, "uninitialized replica unexpectedly already removed")
		}

		if rep.isInitializedRLocked() {
			rep.mu.Unlock()
			log.Fatalf(ctx, "cannot remove initialized replica in removeUninitializedReplica: %v", rep)
		}

		// Mark the replica as removed before deleting data.
		rep.mu.destroyStatus.Set(roachpb.NewRangeNotFoundError(rep.RangeID, rep.StoreID()),
			destroyReasonRemoved)

		rep.mu.Unlock()
	}

	// Proceed with the removal.

	rep.disconnectReplicationRaftMuLocked(ctx)
	if err := rep.destroyRaftMuLocked(ctx, nextReplicaID); err != nil {
		log.Fatalf(ctx, "failed to remove uninitialized replica %v: %v", rep, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Sanity check, could be removed.
	value, stillExists := s.mu.replicas.Load(int64(rep.RangeID))
	if !stillExists {
		log.Fatalf(ctx, "uninitialized replica was removed in the meantime")
	}
	existing := (*Replica)(value)
	if existing == rep {
		log.Infof(ctx, "removing uninitialized replica %v", rep)
	} else {
		log.Fatalf(ctx, "uninitialized replica %v was unexpectedly replaced", existing)
	}

	// Only an uninitialized replica can have a placeholder since, by
	// definition, an initialized replica will be present in the
	// replicasByKey map. While the replica will usually consume the
	// placeholder itself, that isn't guaranteed and so this invocation
	// here is crucial (i.e. don't remove it).
	if s.removePlaceholderLocked(ctx, rep.RangeID) {
		atomic.AddInt32(&s.counts.droppedPlaceholders, 1)
	}
	s.unlinkReplicaByRangeIDLocked(rep.RangeID)
}

// unlinkReplicaByRangeIDLocked removes all of the store's references to the
// provided replica that are keyed by its range ID. The replica may also need
// to be removed from the replicasByKey map.
//
// store.mu must be held.
func (s *Store) unlinkReplicaByRangeIDLocked(rangeID roachpb.RangeID) {
	s.mu.AssertHeld()
	s.unquiescedReplicas.Lock()
	delete(s.unquiescedReplicas.m, rangeID)
	s.unquiescedReplicas.Unlock()
	delete(s.mu.uninitReplicas, rangeID)
	s.replicaQueues.Delete(int64(rangeID))
	s.mu.replicas.Delete(int64(rangeID))
}

// removePlaceholder removes a placeholder for the specified range if it
// exists, returning true if a placeholder was present and removed and false
// otherwise. Requires that the raftMu of the replica whose place is being held
// is locked.
func (s *Store) removePlaceholder(ctx context.Context, rngID roachpb.RangeID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removePlaceholderLocked(ctx, rngID)
}

// removePlaceholderLocked removes the specified placeholder. Requires that
// Store.mu and the raftMu of the replica whose place is being held are locked.
func (s *Store) removePlaceholderLocked(ctx context.Context, rngID roachpb.RangeID) bool {
	placeholder, ok := s.mu.replicaPlaceholders[rngID]
	if !ok {
		return false
	}
	switch exRng := s.mu.replicasByKey.Delete(placeholder).(type) {
	case *ReplicaPlaceholder:
		delete(s.mu.replicaPlaceholders, rngID)
		if exRng2 := s.getOverlappingKeyRangeLocked(&exRng.rangeDesc); exRng2 != nil {
			log.Fatalf(ctx, "corrupted replicasByKey map: %s and %s overlapped", exRng, exRng2)
		}
		return true
	case nil:
		log.Fatalf(ctx, "r%d: placeholder not found", rngID)
	default:
		log.Fatalf(ctx, "r%d: expected placeholder, got %T", rngID, exRng)
	}
	return false // appease the compiler
}
