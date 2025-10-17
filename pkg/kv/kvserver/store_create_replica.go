// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

var errRetry = errors.New("retry: orphaned replica")

// getOrCreateReplica returns an existing or newly created replica with the
// given replicaID for the given rangeID, or kvpb.RaftGroupDeletedError if
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
// If getOrCreateReplica returns kvpb.RaftGroupDeletedError, the guarantee is:
//
//   - getOrCreateReplica will never return this replica
//   - Store.GetReplica(rangeID) can now only return replicas with higher IDs
//   - The RangeTombstone in storage does see this replica as removed
//
// The caller must not hold the store's lock.
func (s *Store) getOrCreateReplica(
	ctx context.Context, id roachpb.FullReplicaID, creatingReplica *roachpb.ReplicaDescriptor,
) (_ *Replica, created bool, _ error) {
	if id.ReplicaID == 0 {
		log.KvExec.Fatalf(ctx, "cannot construct a Replica for range %d with 0 id", id.RangeID)
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
		r, created, err := s.tryGetOrCreateReplica(ctx, id, creatingReplica)
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
	ctx context.Context, id roachpb.FullReplicaID, creatingReplica *roachpb.ReplicaDescriptor,
) (*Replica, error) {
	repl, found := s.mu.replicasByRangeID.Load(id.RangeID)
	if !found {
		return nil, nil
	}
	repl.raftMu.Lock() // not unlocked on success

	// The current replica is removed, go back around.
	if repl.shMu.destroyStatus.Removed() {
		repl.raftMu.Unlock()
		return nil, errRetry
	}

	// Drop messages from replicas we know to be too old.
	if fromReplicaIsTooOldRaftMuLocked(repl, creatingReplica) {
		repl.raftMu.Unlock()
		return nil, kvpb.NewReplicaTooOldError(creatingReplica.ReplicaID)
	}

	// The current replica needs to be removed, remove it and go back around.
	if toTooOld := repl.replicaID < id.ReplicaID; toTooOld {
		if shouldLog := log.V(1); shouldLog {
			log.KvExec.Infof(ctx, "found message for replica ID %d which is newer than %v",
				id.ReplicaID, repl)
		}

		if err := s.removeReplicaRaftMuLocked(
			ctx, repl, id.ReplicaID, "superseded by newer Replica",
		); err != nil {
			log.KvExec.Fatalf(ctx, "failed to remove replica: %v", err)
		}
		repl.raftMu.Unlock()
		return nil, errRetry
	}

	if repl.replicaID > id.ReplicaID {
		// The sender is behind and is sending to an old replica.
		// We could silently drop this message but this way we'll inform the
		// sender that they may no longer exist.
		repl.raftMu.Unlock()
		return nil, &kvpb.RaftGroupDeletedError{}
	}
	if repl.replicaID != id.ReplicaID {
		// This case should have been caught by handleToReplicaTooOld.
		log.KvExec.Fatalf(ctx, "intended replica id %d unexpectedly does not match the current replica %v",
			id.ReplicaID, repl)
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
	ctx context.Context, id roachpb.FullReplicaID, creatingReplica *roachpb.ReplicaDescriptor,
) (_ *Replica, created bool, _ error) {
	// The common case: look up an existing replica.
	if repl, err := s.tryGetReplica(ctx, id, creatingReplica); err != nil {
		return nil, false, err
	} else if repl != nil {
		return repl, false, nil
	}

	// No replica currently exists, so try to create one. Multiple goroutines may
	// be racing at this point, so grab a "lock" over this rangeID (represented by
	// s.mu.creatingReplicas[rangeID]) by one goroutine, and retry others.
	s.mu.Lock()
	if _, ok := s.mu.creatingReplicas[id.RangeID]; ok {
		// Lost the race - another goroutine is currently creating that replica. Let
		// the caller retry so that they can eventually see it.
		s.mu.Unlock()
		return nil, false, errRetry
	}
	s.mu.creatingReplicas[id.RangeID] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.mu.creatingReplicas, id.RangeID)
		s.mu.Unlock()
	}()
	// Now we are the only goroutine trying to create a replica for this rangeID.

	// Repeat the quick path in case someone has overtaken us while we were
	// grabbing the "lock".
	if repl, err := s.tryGetReplica(ctx, id, creatingReplica); err != nil {
		return nil, false, err
	} else if repl != nil {
		return repl, false, nil
	}
	// Now we have the guarantee that s.mu.replicasByRangeID does not contain
	// rangeID, and only we can insert this rangeID. This also implies that the
	// RangeTombstone in storage for this rangeID is "locked" because it can only
	// be accessed by someone holding a reference to, or currently creating a
	// Replica for this rangeID, and that's us.

	_ = kvstorage.CreateUninitReplicaTODO
	// TODO(sep-raft-log): needs both engines due to tombstone (which lives on
	// statemachine).
	if err := kvstorage.CreateUninitializedReplica(
		ctx, kvstorage.TODOState(s.StateEngine()), s.LogEngine(), s.StoreID(), id,
	); err != nil {
		return nil, false, err
	}

	// Create a new uninitialized replica and lock it for raft processing.
	repl, err := newUninitializedReplica(s, id)
	if err != nil {
		return nil, false, err
	}
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

// fromReplicaIsTooOldRaftMuLocked returns true if the creatingReplica is deemed
// to be a member of the range which has been removed.
//
// Assumes toReplica.raftMu is locked. This could be relaxed to Replica.mu
// locked for reads, but the only user of it holds raftMu.
func fromReplicaIsTooOldRaftMuLocked(
	toReplica *Replica, fromReplica *roachpb.ReplicaDescriptor,
) bool {
	toReplica.raftMu.AssertHeld()
	if fromReplica == nil {
		return false
	}
	desc := toReplica.shMu.state.Desc
	_, found := desc.GetReplicaDescriptorByID(fromReplica.ReplicaID)
	return !found && fromReplica.ReplicaID < desc.NextReplicaID
}

// addToReplicasByKeyLocked adds the replica to the replicasByKey btree. The
// replica must already be in replicasByRangeID. Returns an error if a different
// replica with the same range ID, or an overlapping replica or placeholder
// exists in this Store.
//
// The descriptor must match repl.Desc() and repl.descRLocked(). It is passed in
// separately to allow callers both holding and not holding Replica.mu.
func (s *Store) addToReplicasByKeyLocked(repl *Replica, desc *roachpb.RangeDescriptor) error {
	if !desc.IsInitialized() {
		return errors.Errorf("%s: attempted to add uninitialized replica %s", s, repl)
	}
	if got := s.GetReplicaIfExists(repl.RangeID); got != repl { // NB: got can be nil too
		return errors.Errorf("%s: replica %s not in replicasByRangeID; got %s", s, repl, got)
	}
	if it := s.getOverlappingKeyRangeLocked(desc); !it.isEmpty() {
		return errors.Errorf(
			"%s: cannot add to replicasByKey: range %s overlaps with %s", s, repl, it.Desc())
	}
	if it := s.mu.replicasByKey.ReplaceOrInsertReplica(context.Background(), repl); !it.isEmpty() {
		return errors.Errorf(
			"%s: cannot add to replicasByKey: key %v already exists in the btree", s, it.key())
	}
	return nil
}

// addPlaceholderLocked adds the specified placeholder. Requires that Store.mu
// and the raftMu of the replica whose place is being held are locked.
func (s *Store) addPlaceholderLocked(placeholder *ReplicaPlaceholder) error {
	rangeID := placeholder.Desc().RangeID
	if it := s.mu.replicasByKey.ReplaceOrInsertPlaceholder(context.Background(), placeholder); !it.isEmpty() {
		return errors.Errorf("%s overlaps with existing replicaOrPlaceholder %+v in replicasByKey btree", placeholder, it.item())
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
	if existing, loaded := s.mu.replicasByRangeID.LoadOrStore(repl.RangeID, repl); loaded && existing != repl {
		return errors.Errorf("%s: replica already exists", repl)
	}
	return nil
}

// markReplicaInitializedLocked updates the Store bookkeeping to reflect that
// the given replica has transitioned from uninitialized to initialized state.
// Requires that Store.mu and Replica.mu are locked.
func (s *Store) markReplicaInitializedLockedReplLocked(ctx context.Context, r *Replica) error {
	if !r.IsInitialized() {
		return errors.AssertionFailedf("attempted to process uninitialized replica %s", r)
	}

	if have, ok := s.mu.uninitReplicas[r.RangeID]; !ok || have != r {
		return errors.AssertionFailedf("%s not in uninitReplicas, found %v", r, have)
	}
	delete(s.mu.uninitReplicas, r.RangeID)

	// NB: there is a risk that this func tries to lock an already locked r.mu
	// while calling r.Desc() in getOverlappingKeyRangeLocked. This can only
	// happen if r is already in replicasByKey, which must not be the case by the
	// time we get here.
	// TODO(pavelkalinnikov): make locking in replicasByKey less subtle.
	if err := s.addToReplicasByKeyLocked(r, r.descRLocked()); err != nil {
		return err
	}

	// Add the range to metrics and maybe gossip on capacity change.
	s.metrics.ReplicaCount.Inc(1)
	s.storeGossip.MaybeGossipOnCapacityChange(ctx, RangeAddEvent)

	return nil
}
