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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

var errRetry = errors.New("retry: orphaned replica")

// getOrCreateReplica returns an existing or newly created replica with the
// given replicaID for the given rangeID, or roachpb.RaftGroupDeletedError if
// this replicaID has been deleted. A returned replica's Replica.raftMu is
// locked, and the caller is responsible for unlocking it.
//
// Commonly, if the requested replica is present in Store's memory and is not
// being destroyed, it gets returned. Otherwise, this replica is either deleted
// (which is confirmed by reading the RangeTombstone), or gets created (as an
// uninitialized replica) in memory and storage, and loaded from storage if it
// was stored before.
//
// The above assertions and actions on the in-memory (Store, Replica) and stored
// (RaftReplicaID, RangeTombstone) state can't all be done atomically, but this
// method effectively makes them appear atomically done under the returned
// replica's Replica.raftMu.
//
// In particular, if getOrCreateReplica returns a replica, the guarantee is that
// the following invariants (derived from raftMu and Store invariants) are true
// while Replica.raftMu is held:
//
//   - Store.GetReplica(rangeID) successfully returns this and only this replica
//   - The Replica is not being removed as seen by its Replica.mu.destroyStatus
//   - The RangeTombstone in storage does not see this replica as removed
//
// If getOrCreateReplica returns roachpb.RaftGroupDeletedError, the guarantee is:
//
//   - getOrCreateReplica will never return this replica
//   - Store.GetReplica(rangeID) can now only return replicas with higher IDs
//   - The RangeTombstone in storage does see this replica as removed
//
// The caller must not hold the store's lock.
func (s *Store) getOrCreateReplica(
	ctx context.Context,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	creatingReplica *roachpb.ReplicaDescriptor,
) (_ *Replica, created bool, _ error) {
	if replicaID == 0 {
		log.Fatalf(ctx, "cannot construct a Replica for range %d with 0 id", rangeID)
	}
	// We need a retry loop as the replica we find in the map may be in the
	// process of being removed or may need to be removed. Retries in the loop
	// imply that a removal is actually being carried out, not that we're waiting
	// on a queue.
	r := retry.Start(retry.Options{
		InitialBackoff: time.Microsecond,
		// Set the backoff up to only a small amount to wait for data that
		// might need to be cleared.
		MaxBackoff: 10 * time.Millisecond,
	})
	for {
		r.Next()
		r, created, err := s.tryGetOrCreateReplica(
			ctx,
			rangeID,
			replicaID,
			creatingReplica,
		)
		if errors.Is(err, errRetry) {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		return r, created, err
	}
}

// tryGetReplica returns the Replica with the given range/replica ID if it
// exists in the Store's memory, or nil if it does not exist or has been
// removed. Returns errRetry error if the replica is in a transitional state and
// its retrieval needs to be retried. Other errors are permanent.
func (s *Store) tryGetReplica(
	ctx context.Context,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	creatingReplica *roachpb.ReplicaDescriptor,
) (*Replica, error) {
	repl, found := s.mu.replicasByRangeID.Load(rangeID)
	if !found {
		return nil, nil
	}

	repl.raftMu.Lock() // not unlocked on success
	repl.mu.RLock()

	// The current replica is removed, go back around.
	if repl.mu.destroyStatus.Removed() {
		repl.mu.RUnlock()
		repl.raftMu.Unlock()
		return nil, errRetry
	}

	// Drop messages from replicas we know to be too old.
	if fromReplicaIsTooOldRLocked(repl, creatingReplica) {
		repl.mu.RUnlock()
		repl.raftMu.Unlock()
		return nil, roachpb.NewReplicaTooOldError(creatingReplica.ReplicaID)
	}

	// The current replica needs to be removed, remove it and go back around.
	if toTooOld := repl.replicaID < replicaID; toTooOld {
		if shouldLog := log.V(1); shouldLog {
			log.Infof(ctx, "found message for replica ID %d which is newer than %v",
				replicaID, repl)
		}

		repl.mu.RUnlock()
		if err := s.removeReplicaRaftMuLocked(ctx, repl, replicaID, RemoveOptions{
			DestroyData: true,
		}); err != nil {
			log.Fatalf(ctx, "failed to remove replica: %v", err)
		}
		repl.raftMu.Unlock()
		return nil, errRetry
	}
	defer repl.mu.RUnlock()

	if repl.replicaID > replicaID {
		// The sender is behind and is sending to an old replica.
		// We could silently drop this message but this way we'll inform the
		// sender that they may no longer exist.
		repl.raftMu.Unlock()
		return nil, &roachpb.RaftGroupDeletedError{}
	}
	if repl.replicaID != replicaID {
		// This case should have been caught by handleToReplicaTooOld.
		log.Fatalf(ctx, "intended replica id %d unexpectedly does not match the current replica %v",
			replicaID, repl)
	}
	return repl, nil
}

// tryGetOrCreateReplica performs a single attempt at trying to lookup or
// create a replica. It will fail with errRetry if it finds a Replica that has
// been destroyed (and is no longer in Store.mu.replicas) or if during creation
// another goroutine gets there first. In either case, a subsequent call to
// tryGetOrCreateReplica will likely succeed, hence the loop in
// getOrCreateReplica.
func (s *Store) tryGetOrCreateReplica(
	ctx context.Context,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	creatingReplica *roachpb.ReplicaDescriptor,
) (_ *Replica, created bool, _ error) {
	// The common case: look up an existing replica.
	if repl, err := s.tryGetReplica(ctx, rangeID, replicaID, creatingReplica); err != nil {
		return nil, false, err
	} else if repl != nil {
		return repl, false, nil
	}

	// No replica currently exists, so try to create one. Multiple goroutines may
	// be racing at this point, so grab a "lock" over this rangeID (represented by
	// s.mu.creatingReplicas[rangeID]) by one goroutine, and retry others.
	s.mu.Lock()
	if _, ok := s.mu.creatingReplicas[rangeID]; ok {
		// Lost the race - another goroutine is currently creating that replica. Let
		// the caller retry so that they can eventually see it.
		s.mu.Unlock()
		return nil, false, errRetry
	}
	s.mu.creatingReplicas[rangeID] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.mu.creatingReplicas, rangeID)
		s.mu.Unlock()
	}()
	// Now we are the only goroutine trying to create a replica for this rangeID.

	// Repeat the quick path in case someone has overtaken us while we were
	// grabbing the "lock".
	if repl, err := s.tryGetReplica(ctx, rangeID, replicaID, creatingReplica); err != nil {
		return nil, false, err
	} else if repl != nil {
		return repl, false, nil
	}
	// Now we have the guarantee that s.mu.replicasByRangeID does not contain
	// rangeID, and only we can insert this rangeID. This also implies that the
	// RangeTombstone in storage for this rangeID is "locked" because it can only
	// be accessed by someone holding a reference to, or currently creating a
	// Replica for this rangeID, and that's us.

	if err := kvstorage.CreateUninitializedReplica(
		// TODO(sep-raft-log): needs both engines due to tombstone (which lives on
		// statemachine).
		ctx, s.TODOEngine(), s.StoreID(), rangeID, replicaID,
	); err != nil {
		return nil, false, err
	}

	// Create a new uninitialized replica and lock it for raft processing.
	repl := newUninitializedReplica(s, rangeID, replicaID)
	repl.raftMu.Lock() // not unlocked

	// Install the replica in the store's replica map.
	s.mu.Lock()
	// Add the range to range map, but not replicasByKey since the range's start
	// key is unknown. The range will be added to replicasByKey later when a
	// snapshot is applied.
	// TODO(pavelkalinnikov): make this branch error-less.
	if err := s.addToReplicasByRangeIDLocked(repl); err != nil {
		s.mu.Unlock()
		repl.raftMu.Unlock()
		return nil, false, err
	}
	s.mu.uninitReplicas[repl.RangeID] = repl
	s.mu.Unlock()
	// TODO(pavelkalinnikov): since we were holding s.mu anyway, consider
	// dropping the extra Lock/Unlock in the defer deleting from creatingReplicas.

	return repl, true, nil
}

// fromReplicaIsTooOldRLocked returns true if the creatingReplica is deemed to
// be a member of the range which has been removed.
// Assumes toReplica.mu is locked for (at least) reading.
func fromReplicaIsTooOldRLocked(toReplica *Replica, fromReplica *roachpb.ReplicaDescriptor) bool {
	toReplica.mu.AssertRHeld()
	if fromReplica == nil {
		return false
	}
	desc := toReplica.mu.state.Desc
	_, found := desc.GetReplicaDescriptorByID(fromReplica.ReplicaID)
	return !found && fromReplica.ReplicaID < desc.NextReplicaID
}

// addToReplicasByKeyLockedReplicaRLocked adds the replica to the replicasByKey
// btree. The replica must already be in replicasByRangeID. Returns an error if
// a different replica with the same range ID, or an overlapping replica or
// placeholder exists in this Store. Replica.mu must be at least read-locked.
func (s *Store) addToReplicasByKeyLockedReplicaRLocked(repl *Replica) error {
	desc := repl.descRLocked()
	if !desc.IsInitialized() {
		return errors.Errorf("%s: attempted to add uninitialized replica %s", s, repl)
	}
	if got := s.GetReplicaIfExists(repl.RangeID); got != repl { // NB: got can be nil too
		return errors.Errorf("%s: replica %s not in replicasByRangeID; got %s", s, repl, got)
	}
	if it := s.getOverlappingKeyRangeLocked(desc); it.item != nil {
		return errors.Errorf(
			"%s: cannot add to replicasByKey: range %s overlaps with %s", s, repl, it.Desc())
	}
	if it := s.mu.replicasByKey.ReplaceOrInsertReplica(context.Background(), repl); it.item != nil {
		return errors.Errorf(
			"%s: cannot add to replicasByKey: key %v already exists in the btree", s, it.item.key())
	}
	return nil
}

// addPlaceholderLocked adds the specified placeholder. Requires that Store.mu
// and the raftMu of the replica whose place is being held are locked.
func (s *Store) addPlaceholderLocked(placeholder *ReplicaPlaceholder) error {
	rangeID := placeholder.Desc().RangeID
	if it := s.mu.replicasByKey.ReplaceOrInsertPlaceholder(context.Background(), placeholder); it.item != nil {
		return errors.Errorf("%s overlaps with existing replicaOrPlaceholder %+v in replicasByKey btree", placeholder, it.item)
	}
	if exRng, ok := s.mu.replicaPlaceholders[rangeID]; ok {
		return errors.Errorf("%s has ID collision with placeholder %+v", placeholder, exRng)
	}
	s.mu.replicaPlaceholders[rangeID] = placeholder
	return nil
}

// addToReplicasByRangeIDLocked adds the replica to the replicas map.
func (s *Store) addToReplicasByRangeIDLocked(repl *Replica) error {
	// It's ok for the replica to exist in the replicas map as long as it is the
	// same replica object. This does not happen, to the best of our knowledge.
	// TODO(pavelkalinnikov): consider asserting that existing == nil.
	if existing, loaded := s.mu.replicasByRangeID.LoadOrStore(
		repl.RangeID, repl); loaded && existing != repl {
		return errors.Errorf("%s: replica already exists", repl)
	}
	return nil
}

// maybeMarkReplicaInitializedLocked should be called whenever a previously
// uninitialized replica has become initialized so that the store can update its
// internal bookkeeping. It requires that Store.mu and Replica.raftMu
// are locked.
func (s *Store) maybeMarkReplicaInitializedLockedReplLocked(
	ctx context.Context, lockedRepl *Replica,
) error {
	desc := lockedRepl.descRLocked()
	if !desc.IsInitialized() {
		return errors.Errorf("attempted to process uninitialized range %s", desc)
	}

	rangeID := lockedRepl.RangeID
	if _, ok := s.mu.uninitReplicas[rangeID]; !ok {
		// Do nothing if the range has already been initialized.
		return nil
	}
	delete(s.mu.uninitReplicas, rangeID)

	// Copy of the start key needs to be set before inserting into replicasByKey.
	lockedRepl.setStartKeyLocked(desc.StartKey)

	if err := s.addToReplicasByKeyLockedReplicaRLocked(lockedRepl); err != nil {
		return err
	}

	// Unquiesce the replica. We don't allow uninitialized replicas to unquiesce,
	// but now that the replica has been initialized, we unquiesce it as soon as
	// possible. This replica was initialized in response to the reception of a
	// snapshot from another replica. This means that the other replica is not
	// quiesced, so we don't need to campaign or wake the leader. We just want
	// to start ticking.
	//
	// NOTE: The fact that this replica is being initialized in response to the
	// receipt of a snapshot means that its r.mu.internalRaftGroup must not be
	// nil.
	//
	// NOTE: Unquiescing the replica here is not strictly necessary. As of the
	// time of writing, this function is only ever called below handleRaftReady,
	// which will always unquiesce any eligible replicas before completing. So in
	// marking this replica as initialized, we have made it eligible to unquiesce.
	// However, there is still a benefit to unquiecing here instead of letting
	// handleRaftReady do it for us. The benefit is that handleRaftReady cannot
	// make assumptions about the state of the other replicas in the range when it
	// unquieces a replica, so when it does so, it also instructs the replica to
	// campaign and to wake the leader (see maybeUnquiesceAndWakeLeaderLocked).
	// We have more information here (see "This means that the other replica ..."
	// above) and can make assumptions about the state of the other replicas in
	// the range, so we can unquiesce without campaigning or waking the leader.
	if !lockedRepl.maybeUnquiesceWithOptionsLocked(false /* campaignOnWake */) {
		return errors.AssertionFailedf("expected replica %s to unquiesce after initialization", desc)
	}
	// Add the range to metrics and maybe gossip on capacity change.
	s.metrics.ReplicaCount.Inc(1)
	s.storeGossip.MaybeGossipOnCapacityChange(ctx, RangeAddEvent)

	return nil
}
