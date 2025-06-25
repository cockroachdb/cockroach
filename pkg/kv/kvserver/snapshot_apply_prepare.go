// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// snapWriteBuilder contains the data needed to prepare the on-disk state for a
// snapshot.
type snapWriteBuilder struct {
	id roachpb.FullReplicaID

	todoEng  storage.Engine
	sl       stateloader.StateLoader
	writeSST func(context.Context, func(context.Context, storage.Writer) error) error

	truncState kvserverpb.RaftTruncatedState
	hardState  raftpb.HardState
	desc       *roachpb.RangeDescriptor // corresponds to the range descriptor in the snapshot
	origDesc   *roachpb.RangeDescriptor // pre-snapshot range desciptor
	// NB: subsumedDescs, if set, must be in sorted (by start key) order.
	subsumedDescs []*roachpb.RangeDescriptor

	// cleared contains the spans that this snapshot application clears before
	// writing new state on top.
	cleared []roachpb.Span
}

// prepareSnapApply writes the unreplicated SST for the snapshot and clears disk data for subsumed replicas.
func (s *snapWriteBuilder) prepareSnapApply(ctx context.Context) error {
	_ = applySnapshotTODO // 3.1 + 1.1 + 2.5.
	if err := s.writeSST(ctx, s.rewriteRaftState); err != nil {
		return err
	}
	_ = applySnapshotTODO // 3.2 + 2.1 + 2.2
	if err := s.clearSubsumedReplicaDiskData(ctx); err != nil {
		return err
	}

	_ = applySnapshotTODO // 2.3 (split)
	return s.clearResidualDiskDataOnReceivingNarrowerSnapshot(ctx)
}

// rewriteRaftState clears and rewrites the unreplicated rangeID-local key space
// of the given replica with the provided raft state. Note that it also clears
// the raft log contents.
//
// The caller must make sure the log does not have entries newer than the
// snapshot entry ID, and that clearing the log is applied atomically with the
// snapshot write, or after the latter is synced.
func (s *snapWriteBuilder) rewriteRaftState(ctx context.Context, w storage.Writer) error {
	// Clearing the unreplicated state.
	//
	// NB: We do not expect to see range keys in the unreplicated state, so
	// we don't drop a range tombstone across the range key space.
	unreplicatedPrefixKey := keys.MakeRangeIDUnreplicatedPrefix(s.id.RangeID)
	unreplicatedStart := unreplicatedPrefixKey
	unreplicatedEnd := unreplicatedPrefixKey.PrefixEnd()
	if err := w.ClearRawRange(
		unreplicatedStart, unreplicatedEnd, true /* pointKeys */, false, /* rangeKeys */
	); err != nil {
		return errors.Wrapf(err, "error clearing the unreplicated space")
	}

	// Update HardState.
	if err := s.sl.SetHardState(ctx, w, s.hardState); err != nil {
		return errors.Wrapf(err, "unable to write HardState")
	}
	// We've cleared all the raft state above, so we are forced to write the
	// RaftReplicaID again here.
	if err := s.sl.SetRaftReplicaID(ctx, w, s.id.ReplicaID); err != nil {
		return errors.Wrapf(err, "unable to write RaftReplicaID")
	}
	// Update the log truncation state.
	if err := s.sl.SetRaftTruncatedState(ctx, w, &s.truncState); err != nil {
		return errors.Wrapf(err, "unable to write RaftTruncatedState")
	}

	s.cleared = append(s.cleared, roachpb.Span{Key: unreplicatedStart, EndKey: unreplicatedEnd})
	return nil
}

// clearSubsumedReplicaDiskData clears the on disk data of the subsumed
// replicas by creating SSTs with range deletion tombstones. We have to be
// careful here not to have overlapping ranges with the SSTs we have already
// created since that will throw an error while we are ingesting them. This
// method requires that each of the subsumed replicas raftMu is held, and that
// the Reader reflects the latest I/O each of the subsumed replicas has done
// (i.e. Reader was instantiated after all raftMu were acquired).
//
// NB: does nothing if s.subsumedDescs is empty.
func (s *snapWriteBuilder) clearSubsumedReplicaDiskData(ctx context.Context) error {
	if len(s.subsumedDescs) == 0 {
		return nil // no subsumed replicas to speak of; early return
	}
	// NB: The snapshot must never subsume a replica that extends the range of the
	// replica to the left. This is because splits and merges (the only operation
	// that change the key bounds) always leave the start key intact. Extending to
	// the left implies that either we merged "to the left" (we don't), or that
	// we're applying a snapshot for another range (we don't do that either).
	// Something is severely wrong for this to happen, so perform a sanity check.
	if s.subsumedDescs[0].StartKey.Compare(s.desc.StartKey) < 0 { // subsumedDescs are sorted by StartKey
		log.Fatalf(ctx,
			"subsuming replica to our left; subsumed desc start key: %v; snapshot desc start key %v",
			s.subsumedDescs[0].StartKey, s.desc.StartKey,
		)
	}

	// In the common case, the subsumed replicas' end key does not extend beyond
	// the snapshot end key (sn <= b):
	//
	//	   subsumed: [a---s1---...---sn)
	//	   snapshot: [a---------------b)
	//	or snapshot: [a-------------------b)
	//
	// In this case, we do not need to clear the range-local, lock table, and user
	// keys owned by subsumed replicas, since clearing [a, b) will cover it. We
	// only need to clear the per-replica RangeID-local key spans here.
	//
	// This leaves the only other case, where the subsumed replicas extend past
	// the snapshot's end key (s[n-1] < b < sn):
	//
	//	subsumed: [a---s1---...---s[n-1]---sn)
	//	snapshot: [a--------------------b)
	//
	// This is only possible if we're not only learning about merges through the
	// snapshot, but also a split -- that's the only way the bounds of the
	// snapshot can be narrower than the bounds of all the subsumed replicas. In
	// this case, we do need to clear range-local, lock table, and user keys in
	// the span [b, sn). We do this in clearSplitReplicaDiskData, not here.

	// TODO(sep-raft-log): need different readers for raft and state engine.
	reader := storage.Reader(s.todoEng)
	for _, subDesc := range s.subsumedDescs {
		// We have to create an SST for the subsumed replica's range-id local keys.
		if err := s.writeSST(ctx, func(ctx context.Context, w storage.Writer) error {
			// NOTE: We set mustClearRange to true because we are setting
			// RangeTombstoneKey. Since Clears and Puts need to be done in increasing
			// order of keys, it is not safe to use ClearRangeIter.
			opts := kvstorage.ClearRangeDataOptions{
				ClearReplicatedByRangeID:   true,
				ClearUnreplicatedByRangeID: true,
				MustUseClearRange:          true,
			}
			s.cleared = append(s.cleared, rditer.Select(subDesc.RangeID, rditer.SelectOpts{
				ReplicatedByRangeID:   opts.ClearReplicatedByRangeID,
				UnreplicatedByRangeID: opts.ClearUnreplicatedByRangeID,
			})...)
			// NB: Actually clear RangeID local key spans.
			return kvstorage.DestroyReplica(ctx, subDesc.RangeID, reader, w, mergedTombstoneReplicaID, opts)
		}); err != nil {
			return err
		}
	}

	return nil
}

// clearResidualDiskDataOnReceivingNarrowerSnapshot clears any residual on-disk
// data that isn't covered by the snapshot being applied. In particular, a
// snapshot replaces one (s.origDesc) or more (s.subsumedReplicas) replicas on
// the store that overlap with it. The snapshot's start key must match
// s.origDesc; however, the end key may be less than that of the keyspace
// covered by s.origDesc + s.subsumedDescs. Put another way, the snapshot may be
// narrower, and if it is, we must clear the residual disk data that isn't
// covered by the snapshot.
//
// Visually, the picture looks as follows:
//
// The simplest case is when the snapshot isn't subsuming any replicas.
// original descriptor: [a-----------------------------c)
// snapshot descriptor: [a---------------------b)
// cleared: [b, c)
//
// In the more general case[*]:
//
// store descriptors:   [a----------------s1---...---sn)
// snapshot descriptor: [a---------------------b)
// cleared: [b, sn)
//
// Practically speaking, the simple case above corresponds to a replica learning
// about a split through a snapshot. The more general case corresponds to a
// replica learning about a series of merges followed by a single split through
// the snapshot.
//
// clearResidualDiskDataOnReceivingNarrowerSnapshot is a no-op for initial
// snapshots or when there is no narrowing business to speak of.
//
// [*] In the diagram , S1...Sn correspond to subsumed replicas with end keys
// s1...sn respectively. These are all replicas on the store that overlap with
// the snapshot descriptor, covering the range [a,b), and the right-most
// descriptor is that of replica Sn.
func (s *snapWriteBuilder) clearResidualDiskDataOnReceivingNarrowerSnapshot(
	ctx context.Context,
) error {
	if !s.origDesc.IsInitialized() {
		// Early return in the case where we're ingesting an initial snapshot. In
		// this case, the snapshot can't be making the keyspace narrower, as the
		// replica isn't initialized yet (i.e. no key bounds have been set).

		// NB: An initial snapshot shouldn't be subsuming any replicas.
		testingAssert(
			len(s.subsumedDescs) == 0, "expected no subsumed replicas when ingesting the initial snapshot",
		)
		return nil
	}

	rightMostDesc := s.origDesc
	if len(s.subsumedDescs) != 0 {
		// NB: s.subsumedDescs are non-overlapping (obviously) and sorted by start
		// key. Pick the last one to determine whether the snapshot is narrowing the
		// keyspace or not.
		rightMostDesc = s.subsumedDescs[len(s.subsumedDescs)-1]
	}

	if rightMostDesc.EndKey.Compare(s.desc.EndKey) <= 0 {
		return nil // we aren't narrowing anything; no-op
	}

	// TODO(sep-raft-log): read from the state machine engine here.
	reader := storage.Reader(s.todoEng)
	for _, span := range rditer.Select(0, rditer.SelectOpts{
		Ranged: rditer.SelectRangedOptions{RSpan: roachpb.RSpan{
			Key: s.desc.EndKey, EndKey: rightMostDesc.EndKey,
		},
			SystemKeys: true,
			LockTable:  true,
			UserKeys:   true,
		},
	}) {
		if err := s.writeSST(ctx, func(ctx context.Context, w storage.Writer) error {
			return storage.ClearRangeWithHeuristic(
				ctx, reader, w, span.Key, span.EndKey, kvstorage.ClearRangeThresholdPointKeys,
			)
		}); err != nil {
			return err
		}
		s.cleared = append(s.cleared, span)
	}

	return nil
}
