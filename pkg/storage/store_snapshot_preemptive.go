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
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	crdberrors "github.com/cockroachdb/errors"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// canApplyPreemptiveSnapshot returns (_, nil) if the preemptive snapshot can be
// applied to this store's replica (i.e. the snapshot is not from an older
// incarnation of the replica) and a placeholder can be added to the
// replicasByKey map (if necessary). If a placeholder is required, it is
// returned as the first value. The authoritative bool determines whether the
// check is carried out with the intention of actually applying the snapshot (in
// which case an existing replica must exist and have its raftMu locked) or as a
// preliminary check.
func (s *Store) canApplyPreemptiveSnapshot(
	ctx context.Context, snapHeader *SnapshotRequest_Header, authoritative bool,
) (*ReplicaPlaceholder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.canApplyPreemptiveSnapshotLocked(ctx, snapHeader, authoritative)
}

func (s *Store) canApplyPreemptiveSnapshotLocked(
	ctx context.Context, snapHeader *SnapshotRequest_Header, authoritative bool,
) (*ReplicaPlaceholder, error) {
	if !snapHeader.IsPreemptive() {
		return nil, crdberrors.AssertionFailedf(`expected a preemptive snapshot`)
	}

	// TODO(tbg): see the comment on desc.Generation for what seems to be a much
	// saner way to handle overlap via generational semantics.
	desc := *snapHeader.State.Desc

	// First, check for an existing Replica.
	//
	// We call canApplyPreemptiveSnapshotLocked twice for each snapshot
	// application. In the first case, it's an optimization early before having
	// received any data (and we don't use the placeholder if one is returned),
	// and the replica may or may not be present.
	//
	// The second call happens right before we actually plan to apply the snapshot
	// (and a Replica is always in place at that point). This means that without a
	// Replica, we can have false positives, but if we have a replica it needs to
	// take everything into account.
	if v, ok := s.mu.replicas.Load(
		int64(desc.RangeID),
	); !ok {
		if authoritative {
			return nil, errors.Errorf("authoritative call requires a replica present")
		}
	} else {
		existingRepl := (*Replica)(v)
		// The raftMu is held which allows us to use the existing replica as a
		// placeholder when we decide that the snapshot can be applied. As long
		// as the caller releases the raftMu only after feeding the snapshot
		// into the replica, this is safe.
		if authoritative {
			existingRepl.raftMu.AssertHeld()
		}

		existingRepl.mu.RLock()
		existingDesc := existingRepl.descRLocked()
		existingIsInitialized := existingRepl.isInitializedRLocked()
		existingIsPreemptive := existingRepl.mu.replicaID == 0
		existingRepl.mu.RUnlock()

		if existingIsInitialized {
			if existingIsPreemptive {
				// Allow applying a preemptive snapshot on top of another
				// preemptive snapshot. We only need to acquire a placeholder
				// for the part (if any) of the new snapshot that extends past
				// the old one. If there's no such overlap, return early; if
				// there is, "forward" the descriptor's StartKey so that the
				// later code will only check the overlap.
				//
				// NB: morally it would be cleaner to ask for the existing
				// replica to be GC'ed first, but consider that the preemptive
				// snapshot was likely left behind by a failed attempt to
				// up-replicate. This is a relatively common scenario and not
				// worth discarding and resending another snapshot for. Let the
				// snapshot through, which means "pretending that it doesn't
				// intersect the existing replica".
				if !existingDesc.EndKey.Less(desc.EndKey) {
					return nil, nil
				}
				desc.StartKey = existingDesc.EndKey
			}
			// NB: If the existing snapshot is *not* preemptive (i.e. the above
			// branch wasn't taken), the overlap check below will hit an error.
			// This path is hit after a rapid up-down-upreplication to the same
			// store and will resolve as the replicaGCQueue removes the existing
			// replica. We are pretty sure that the existing replica is gc'able,
			// because a preemptive snapshot implies that someone is trying to
			// add this replica to the group at the moment. (We are not however,
			// sure enough that this couldn't happen by accident to GC the
			// replica ourselves - the replica GC queue will perform the proper
			// check).
		} else {
			// Morally, the existing replica now has a nonzero replica ID
			// because we already know that it is not initialized (i.e. has no
			// data). Interestingly, the case in which it has a zero replica ID
			// is also possible and should see the snapshot accepted as it
			// occurs when a preemptive snapshot is handled: we first create a
			// Replica in this state, run this check, and then apply the
			// preemptive snapshot.
			if !existingIsPreemptive {
				// This is similar to the case of a preemptive snapshot hitting
				// a fully initialized replica (i.e. not a preemptive snapshot)
				// at the end of the last branch (which we don't allow), so we
				// want to reject the snapshot. There is a tricky problem to
				// to solve here, though: existingRepl doesn't know anything
				// about its key bounds, and so to check whether it is actually
				// gc'able would require a full scan of the meta2 entries (and
				// we would also need to teach the queues how to deal with un-
				// initialized replicas).
				//
				// So we let the snapshot through (by falling through to the
				// overlap check, where it either picks up placeholders or
				// fails). This is safe (or at least we assume so) because we
				// carry out all snapshot decisions through Raft (though it
				// still is an odd path that we would be wise to avoid if it
				// weren't so difficult).
				//
				// A consequence of letting this snapshot through is opening this
				// replica up to the possibility of erroneous replicaGC. This is
				// because it will retain the replicaID of the current replica,
				// which is going to be initialized after the snapshot (and thus
				// gc'able).
				_ = 0 // avoid staticcheck failure
			}
		}
	}

	// We have a key range [desc.StartKey,desc.EndKey) which we want to apply a
	// snapshot for. Is there a conflicting existing placeholder or an
	// overlapping range?
	if err := s.checkSnapshotOverlapLocked(ctx, snapHeader); err != nil {
		return nil, err
	}

	placeholder := &ReplicaPlaceholder{
		rangeDesc: desc,
	}
	return placeholder, nil
}

// processPreemptiveSnapshotRequest processes the incoming preemptive snapshot
// request on the request's specified replica.
func (s *Store) processPreemptiveSnapshotRequest(
	ctx context.Context, snapHeader *SnapshotRequest_Header, inSnap IncomingSnapshot,
) *roachpb.Error {
	if !snapHeader.IsPreemptive() {
		return roachpb.NewError(crdberrors.AssertionFailedf(`expected a preemptive snapshot`))
	}

	return s.withReplicaForRequest(ctx, &snapHeader.RaftMessageRequest, func(
		ctx context.Context, r *Replica,
	) (pErr *roachpb.Error) {
		if snapHeader.RaftMessageRequest.Message.Type != raftpb.MsgSnap {
			log.Fatalf(ctx, "expected snapshot: %+v", snapHeader.RaftMessageRequest)
		}

		// Check to see if a snapshot can be applied. Snapshots can always be applied
		// to initialized replicas. Note that if we add a placeholder we need to
		// already be holding Replica.raftMu in order to prevent concurrent
		// raft-ready processing of uninitialized replicas.
		var addedPlaceholder bool
		var removePlaceholder bool
		if err := func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			placeholder, err := s.canApplyPreemptiveSnapshotLocked(ctx, snapHeader, true /* authoritative */)
			if err != nil {
				// If the storage cannot accept the snapshot, return an
				// error before passing it to RawNode.Step, since our
				// error handling options past that point are limited.
				log.Infof(ctx, "cannot apply snapshot: %s", err)
				return err
			}

			if placeholder != nil {
				// NB: The placeholder added here is either removed below after a
				// preemptive snapshot is applied or after the next call to
				// Replica.handleRaftReady. Note that we can only get here if the
				// replica doesn't exist or is uninitialized.
				if err := s.addPlaceholderLocked(placeholder); err != nil {
					log.Fatalf(ctx, "could not add vetted placeholder %s: %+v", placeholder, err)
				}
				addedPlaceholder = true
			}
			return nil
		}(); err != nil {
			return roachpb.NewError(err)
		}

		if addedPlaceholder {
			// If we added a placeholder remove it before we return unless some other
			// part of the code takes ownership of the removal (indicated by setting
			// removePlaceholder to false).
			removePlaceholder = true
			defer func() {
				if removePlaceholder {
					if s.removePlaceholder(ctx, snapHeader.RaftMessageRequest.RangeID) {
						atomic.AddInt32(&s.counts.removedPlaceholders, 1)
					}
				}
			}()
		}

		// Requiring that the Term is set in a message makes sure that we
		// get all of Raft's internal safety checks (it confuses messages
		// at term zero for internal messages). The sending side uses the
		// term from the snapshot itself, but we'll just check nonzero.
		if snapHeader.RaftMessageRequest.Message.Term == 0 {
			return roachpb.NewErrorf(
				"preemptive snapshot from term %d received with zero term",
				snapHeader.RaftMessageRequest.Message.Snapshot.Metadata.Term,
			)
		}
		// TODO(tschottdorf): A lot of locking of the individual Replica
		// going on below as well. I think that's more easily refactored
		// away; what really matters is that the Store doesn't do anything
		// else with that same Replica (or one that might conflict with us
		// while we still run). In effect, we'd want something like:
		//
		// 1. look up the snapshot's key range
		// 2. get an exclusive lock for operations on that key range from
		//    the store (or discard the snapshot)
		//    (at the time of writing, we have checked the key range in
		//    canApplyPreemptiveSnapshotLocked above, but there are concerns
		//    about two conflicting operations passing that check simultaneously,
		//    see #7830)
		// 3. do everything below (apply the snapshot through temp Raft group)
		// 4. release the exclusive lock on the snapshot's key range
		//
		// There are two future outcomes: Either we begin receiving
		// legitimate Raft traffic for this Range (hence learning the
		// ReplicaID and becoming a real Replica), or the Replica GC queue
		// decides that the ChangeReplicas as a part of which the
		// preemptive snapshot was sent has likely failed and removes both
		// in-memory and on-disk state.
		r.mu.Lock()
		// We are paranoid about applying preemptive snapshots (which
		// were constructed via our code rather than raft) to the "real"
		// raft group. Instead, we destroy the "real" raft group if one
		// exists (this is rare in production, although it occurs in
		// tests), apply the preemptive snapshot to a temporary raft
		// group, then discard that one as well to be replaced by a real
		// raft group when we get a new replica ID.
		//
		// It might be OK instead to apply preemptive snapshots just
		// like normal ones (essentially switching between regular and
		// preemptive mode based on whether or not we have a raft group,
		// instead of based on the replica ID of the snapshot message).
		// However, this is a risk that we're not yet willing to take.
		// Additionally, without some additional plumbing work, doing so
		// would limit the effectiveness of RaftTransport.SendSync for
		// preemptive snapshots.
		r.mu.internalRaftGroup = nil
		needTombstone := r.mu.state.Desc.NextReplicaID != 0
		r.mu.Unlock()

		appliedIndex, _, err := r.raftMu.stateLoader.LoadAppliedIndex(ctx, r.store.Engine())
		if err != nil {
			return roachpb.NewError(err)
		}
		// We need to create a temporary RawNode to process the snapshot. Raft
		// internally runs safety checks on the snapshot, among them one that
		// verifies that the peer is actually part of the configuration encoded
		// in the snapshot. Awkwardly, it isn't actually a peer (preemptive
		// snapshot...). To get around this, pretend the RawNode has the ID of a
		// peer we know exists, namely the one that sent us the snap. This won't
		// be persisted anywhere, and since we're only using the RawNode for
		// this one snapshot, everything is ok. However, we'll make sure that
		// no messages are sent in the resulting Ready.
		preemptiveSnapshotRaftGroupID := uint64(snapHeader.RaftMessageRequest.FromReplica.ReplicaID)
		raftGroup, err := raft.NewRawNode(
			newRaftConfig(
				raft.Storage((*replicaRaftStorage)(r)),
				preemptiveSnapshotRaftGroupID,
				// We pass the "real" applied index here due to subtleties
				// arising in the case in which Raft discards the snapshot:
				// It would instruct us to apply entries, which would have
				// crashing potential for any choice of dummy value below.
				appliedIndex,
				r.store.cfg,
				&raftLogger{ctx: ctx},
			))
		if err != nil {
			return roachpb.NewError(err)
		}
		// We have a Raft group; feed it the message.
		if err := raftGroup.Step(snapHeader.RaftMessageRequest.Message); err != nil {
			return roachpb.NewError(errors.Wrap(err, "unable to process preemptive snapshot"))
		}
		// In the normal case, the group should ask us to apply a snapshot.
		// If it doesn't, our snapshot was probably stale. In that case we
		// still go ahead and apply a noop because we want that case to be
		// counted by stats as a successful application.
		var ready raft.Ready
		if raftGroup.HasReady() {
			ready = raftGroup.Ready()
			// See the comment above - we don't want this temporary Raft group
			// to contact the outside world. Apply the snapshot and that's it.
			ready.Messages = nil
		}

		if needTombstone {
			// Bump the min replica ID, but don't write the tombstone key. The
			// tombstone key is not expected to be present when normal replica data
			// is present and applySnapshot would delete the key in most cases. If
			// Raft has decided the snapshot shouldn't be applied we would be
			// writing the tombstone key incorrectly.
			r.mu.Lock()
			if r.mu.state.Desc.NextReplicaID > r.mu.minReplicaID {
				r.mu.minReplicaID = r.mu.state.Desc.NextReplicaID
			}
			r.mu.Unlock()
		}

		// Apply the snapshot, as Raft told us to. Preemptive snapshots never
		// subsume replicas (this is guaranteed by
		// Store.canApplyPreemptiveSnapshot), so we can simply pass nil for the
		// subsumedRepls parameter.
		if err := r.applySnapshot(
			ctx, inSnap, ready.Snapshot, ready.HardState, nil, /* subsumedRepls */
		); err != nil {
			return roachpb.NewError(err)
		}
		// applySnapshot has already removed the placeholder.
		removePlaceholder = false

		// At this point, the Replica has data but no ReplicaID. We hope
		// that it turns into a "real" Replica by means of receiving Raft
		// messages addressed to it with a ReplicaID, but if that doesn't
		// happen, at some point the Replica GC queue will have to grab it.
		return nil
	})
}
