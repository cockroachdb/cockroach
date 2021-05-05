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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

var errRetry = errors.New("retry: orphaned replica")

// getOrCreateReplica returns a replica for the given RangeID, creating an
// uninitialized replica if necessary. The caller must not hold the store's
// lock. The returned replica has Replica.raftMu locked and it is the caller's
// responsibility to unlock it.
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
	// The common case: look up an existing (initialized) replica.
	if value, ok := s.mu.replicas.Load(int64(rangeID)); ok {
		repl := (*Replica)(value)
		repl.raftMu.Lock() // not unlocked on success
		repl.mu.Lock()

		// The current replica is removed, go back around.
		if repl.mu.destroyStatus.Removed() {
			repl.mu.Unlock()
			repl.raftMu.Unlock()
			return nil, false, errRetry
		}

		// Drop messages from replicas we know to be too old.
		if fromReplicaIsTooOld(repl, creatingReplica) {
			repl.mu.Unlock()
			repl.raftMu.Unlock()
			return nil, false, roachpb.NewReplicaTooOldError(creatingReplica.ReplicaID)
		}

		// The current replica needs to be removed, remove it and go back around.
		if toTooOld := repl.mu.replicaID < replicaID; toTooOld {
			if shouldLog := log.V(1); shouldLog {
				log.Infof(ctx, "found message for replica ID %d which is newer than %v",
					replicaID, repl)
			}

			repl.mu.Unlock()
			if err := s.removeReplicaRaftMuLocked(ctx, repl, replicaID, RemoveOptions{
				DestroyData: true,
			}); err != nil {
				log.Fatalf(ctx, "failed to remove replica: %v", err)
			}
			repl.raftMu.Unlock()
			return nil, false, errRetry
		}
		defer repl.mu.Unlock()

		if repl.mu.replicaID > replicaID {
			// The sender is behind and is sending to an old replica.
			// We could silently drop this message but this way we'll inform the
			// sender that they may no longer exist.
			repl.raftMu.Unlock()
			return nil, false, &roachpb.RaftGroupDeletedError{}
		}
		if repl.mu.replicaID != replicaID {
			// This case should have been caught by handleToReplicaTooOld.
			log.Fatalf(ctx, "intended replica id %d unexpectedly does not match the current replica %v",
				replicaID, repl)
		}
		return repl, false, nil
	}

	// No replica currently exists, so we'll try to create one. Before creating
	// the replica, see if there is a tombstone which would indicate that this
	// is a stale message.
	// NB: we check this before creating a new Replica and adding it to the
	// Store's Range map even though we must check it again after to avoid race
	// conditions. This double-checked locking is an optimization to avoid this
	// work when we know the Replica should not be created ahead of time.
	tombstoneKey := keys.RangeTombstoneKey(rangeID)
	var tombstone roachpb.RangeTombstone
	if ok, err := storage.MVCCGetProto(
		ctx, s.Engine(), tombstoneKey, hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{},
	); err != nil {
		return nil, false, err
	} else if ok && replicaID != 0 && replicaID < tombstone.NextReplicaID {
		return nil, false, &roachpb.RaftGroupDeletedError{}
	}

	// Create a new replica and lock it for raft processing.
	uninitializedDesc := &roachpb.RangeDescriptor{
		RangeID: rangeID,
		// NB: other fields are unknown; need to populate them from
		// snapshot.
	}
	repl := newUnloadedReplica(ctx, uninitializedDesc, s, replicaID)
	repl.creatingReplica = creatingReplica
	repl.raftMu.Lock() // not unlocked

	// Take out read-only lock. Not strictly necessary here, but follows the
	// normal lock protocol for destroyStatus.Set().
	repl.readOnlyCmdMu.Lock()
	// Install the replica in the store's replica map. The replica is in an
	// inconsistent state, but nobody will be accessing it while we hold its
	// locks.
	s.mu.Lock()
	// Grab the internal Replica state lock to ensure nobody mucks with our
	// replica even outside of raft processing. Have to do this after grabbing
	// Store.mu to maintain lock ordering invariant.
	repl.mu.Lock()
	repl.mu.tombstoneMinReplicaID = tombstone.NextReplicaID

	// NB: A Replica should never be in the store's replicas map with a nil
	// descriptor. Assign it directly here. In the case that the Replica should
	// exist (which we confirm with another check of the Tombstone below), we'll
	// re-initialize the replica with the same uninitializedDesc.
	//
	// During short window between here and call to s.unlinkReplicaByRangeIDLocked()
	// in the failure branch below, the Replica used to have a nil descriptor and
	// was present in the map. While it was the case that the destroy status had
	// been set, not every code path which inspects the descriptor checks the
	// destroy status.
	repl.mu.state.Desc = uninitializedDesc

	// Add the range to range map, but not replicasByKey since the range's start
	// key is unknown. The range will be added to replicasByKey later when a
	// snapshot is applied. After unlocking Store.mu above, another goroutine
	// might have snuck in and created the replica, so we retry on error.
	if err := s.addReplicaToRangeMapLocked(repl); err != nil {
		repl.mu.Unlock()
		s.mu.Unlock()
		repl.readOnlyCmdMu.Unlock()
		repl.raftMu.Unlock()
		return nil, false, errRetry
	}
	s.mu.uninitReplicas[repl.RangeID] = repl
	s.mu.Unlock() // NB: unlocking out of order

	// Initialize the Replica with the replicaID.
	if err := func() error {
		// Check for a tombstone again now that we've inserted into the Range
		// map. This double-checked locking ensures that we avoid a race where a
		// replica is created and destroyed between the initial unsynchronized
		// tombstone check and the Range map linearization point. By checking
		// again now, we make sure to synchronize with any goroutine that wrote
		// a tombstone and then removed an old replica from the Range map.
		if ok, err := storage.MVCCGetProto(
			ctx, s.Engine(), tombstoneKey, hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{},
		); err != nil {
			return err
		} else if ok && replicaID < tombstone.NextReplicaID {
			return &roachpb.RaftGroupDeletedError{}
		}

		// An uninitialized replica should have an empty HardState.Commit at
		// all times. Failure to maintain this invariant indicates corruption.
		// And yet, we have observed this in the wild. See #40213.
		if hs, err := repl.mu.stateLoader.LoadHardState(ctx, s.Engine()); err != nil {
			return err
		} else if hs.Commit != 0 {
			log.Fatalf(ctx, "found non-zero HardState.Commit on uninitialized replica %s. HS=%+v", repl, hs)
		}
		return repl.loadRaftMuLockedReplicaMuLocked(uninitializedDesc)
	}(); err != nil {
		// Mark the replica as destroyed and remove it from the replicas maps to
		// ensure nobody tries to use it.
		repl.mu.destroyStatus.Set(errors.Wrapf(err, "%s: failed to initialize", repl), destroyReasonRemoved)
		repl.mu.Unlock()
		s.mu.Lock()
		s.unlinkReplicaByRangeIDLocked(ctx, rangeID)
		s.mu.Unlock()
		repl.readOnlyCmdMu.Unlock()
		repl.raftMu.Unlock()
		return nil, false, err
	}
	repl.mu.Unlock()
	repl.readOnlyCmdMu.Unlock()
	return repl, true, nil
}

// isFromReplicaTooOld returns an true if the creatingReplica is deemed to be
// a member of the range which has been removed.
// Assumes toReplica.mu is held.
func fromReplicaIsTooOld(toReplica *Replica, fromReplica *roachpb.ReplicaDescriptor) bool {
	toReplica.mu.AssertHeld()
	if fromReplica == nil {
		return false
	}
	desc := toReplica.mu.state.Desc
	_, found := desc.GetReplicaDescriptorByID(fromReplica.ReplicaID)
	return !found && fromReplica.ReplicaID < desc.NextReplicaID
}

// addReplicaInternalLocked adds the replica to the replicas map and the
// replicasByKey btree. Returns an error if a replica with
// the same Range ID or an overlapping replica or placeholder exists in
// this store. addReplicaInternalLocked requires that the store lock is held.
func (s *Store) addReplicaInternalLocked(repl *Replica) error {
	if !repl.IsInitialized() {
		return errors.Errorf("attempted to add uninitialized replica %s", repl)
	}

	if err := s.addReplicaToRangeMapLocked(repl); err != nil {
		return err
	}

	if it := s.getOverlappingKeyRangeLocked(repl.Desc()); it.item != nil {
		return errors.Errorf("%s: cannot addReplicaInternalLocked; range %s has overlapping range %s", s, repl, it.Desc())
	}

	if it := s.mu.replicasByKey.ReplaceOrInsertReplica(context.Background(), repl); it.item != nil {
		return errors.Errorf("%s: cannot addReplicaInternalLocked; range for key %v already exists in replicasByKey btree", s,
			it.item.key())
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

// addReplicaToRangeMapLocked adds the replica to the replicas map.
func (s *Store) addReplicaToRangeMapLocked(repl *Replica) error {
	// It's ok for the replica to exist in the replicas map as long as it is the
	// same replica object. This occurs during splits where the right-hand side
	// is added to the replicas map before it is initialized.
	if existing, loaded := s.mu.replicas.LoadOrStore(
		int64(repl.RangeID), unsafe.Pointer(repl)); loaded && (*Replica)(existing) != repl {
		return errors.Errorf("%s: replica already exists", repl)
	}
	// Check whether the replica is unquiesced but not in the map. This
	// can happen during splits and merges, where the uninitialized (but
	// also unquiesced) replica is removed from the unquiesced replica
	// map in advance of this method being called.
	s.unquiescedReplicas.Lock()
	if _, ok := s.unquiescedReplicas.m[repl.RangeID]; !repl.mu.quiescent && !ok {
		s.unquiescedReplicas.m[repl.RangeID] = struct{}{}
	}
	s.unquiescedReplicas.Unlock()
	return nil
}

// maybeMarkReplicaInitializedLocked should be called whenever a previously
// unintialized replica has become initialized so that the store can update its
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

	if it := s.getOverlappingKeyRangeLocked(desc); it.item != nil {
		return errors.Errorf("%s: cannot initialize replica; %s has overlapping range %s",
			s, desc, it.Desc())
	}

	// Copy of the start key needs to be set before inserting into replicasByKey.
	lockedRepl.setStartKeyLocked(desc.StartKey)
	if it := s.mu.replicasByKey.ReplaceOrInsertReplica(ctx, lockedRepl); it.item != nil {
		return errors.Errorf("range for key %v already exists in replicasByKey btree: %+v",
			it.item.key(), it)
	}

	// Add the range to metrics and maybe gossip on capacity change.
	s.metrics.ReplicaCount.Inc(1)
	s.maybeGossipOnCapacityChange(ctx, rangeAddEvent)

	return nil
}
