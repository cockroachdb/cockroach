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
	id storage.FullReplicaID

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
	_ = applySnapshotTODO // 3.2 + 2.1 + 2.2 + 2.3 (partial)
	err := s.clearSubsumedReplicaDiskData(ctx)
	if err != nil {
		return err
	}

	_ = applySnapshotTODO // 2.3 (partial)
	return s.clearSplitReplicaDiskData(ctx)
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

	// NB: We do not need to create SSTs for the range local keys, lock table
	// keys, and user keys owned by subsumed replicas here. In the common case,
	// where the subsumed replicas have the same bounds as the snapshot:
	//
	// subsumed replicas: [a---s1---...---sn)
	// snapshot:          [a---------------b)
	//
	// or are fully contained in the snapshot:
	//
	// subsumed replicas: [a---s1---...---sn)
	// snapshot:          [a---------------------b)
	//
	// We don't need to clear any additional keyspace, since clearing [a, b) will
	// also clear the keyspace owned by all the subsumed replicas. We only need to
	// clear per-replica key spans here.
	//
	// This leaves the only other case, where the subsumed replicas extend past
	// the snapshot's bounds:
	//
	// subsumed replicas: [a----------------s1---...---sn)
	// snapshot:          [a---------------------b)
	//
	// This is only possible if we're not only learning about merges through the
	// snapshot, but also a split -- that's the only way the bounds of the
	// snapshot can be narrower than the bounds of all the subsumed replicas. In
	// this case, we do need to clear range local keys, lock table keys, and user
	// keys in the span [b, sn). We do this in clearSplitReplicaDiskData, not
	// here.

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

// clearSplitReplicaDiskData clears the on disk data of any replica that has
// been split, and we've learned of this split when applying the snapshot
// because the snapshot narrows the range. We clear on-disk data by creating
// SSTs with range deletion tombstones. To do so, we use the right-most
// descriptor that overlaps with the descriptor in the snapshot, and clear out
// any disk data that extends beyond the snapshot descriptor's end key that
// overlaps with the right-most descriptor.
//
// In the simplest case, where a replica (LHS) learns about the split through the
// snapshot, we can simply use the descriptor of the replica itself:
//
// original descriptor: [a-----------------------------c)
// snapshot descriptor: [a---------------------b)
// cleared: [b, c)
//
// The more involved case is when one or more replicas have been merged into the
// LHS before the split, and the LHS is learning about all of these through the
// snapshot -- in this case, the right-most descriptor corresponds to the
// right-most subsumed replica:
//
// store descriptors:   [a----------------s1---...---sn)
// snapshot descriptor: [a---------------------b)
// cleared: [b, sn)
//
// In the diagram above, S1...Sn correspond to subsumed replicas with end keys
// s1...sn respectively. These are all replicas on the store that overlap with
// the snapshot descriptor, covering the range [a,b), and the right-most
// descriptor is that of replica Sn.
//
// clearSplitReplicaDiskState is a no-op if the right-most descriptor's EndKey
// is no wider than the snapshot's EndKey.
func (s *snapWriteBuilder) clearSplitReplicaDiskData(ctx context.Context) error {
	rightMostDesc := s.origDesc
	if len(s.subsumedDescs) != 0 {
		// NB: We might have to create SSTs for the range local keys, lock table
		// keys, and user keys depending on if the subsumed replicas are not fully
		// contained by the replica in our snapshot. The following is an example to
		// this case happening:
		//
		// a       b       c       d
		// |---1---|-------2-------|  S1
		// |---1-------------------|  S2
		// |---1-----------|---3---|  S3
		//
		// Since the merge is the first operation to happen, a follower could be
		// down before it completes. The range could then split, and it is
		// reasonable for S1 to learn about both these operations via a snapshot for
		// r1 from S3. In the general case, this can lead to the following
		// situation[*] where subsumed replicas may extend past the snapshot:
		//
		// [a----------------s1---...---sn)
		// [a---------------------b)
		//
		// So, we need to additionally clear [b,sn).
		//
		// [*] s1, ..., sn are the end keys for the subsumed replicas (for the
		// current keySpan).
		rightMostDesc = s.subsumedDescs[len(s.subsumedDescs)-1]

		// NB: In the other case, where the subsumed replicas are fully contained
		// in the snapshot:
		//
		// [a---s1---...---sn)
		// [a---------------------b)
		//
		// We don't need to clear additional keyspace here, since we aren't learning
		// about a split through this snapshot. This is handled by correctly setting
		// rightMostDesc, which then causes us to early return below.
	}

	if rightMostDesc.EndKey.Compare(s.desc.EndKey) <= 0 {
		return nil // no-op, no split
	}

	// TODO(sep-raft-log): need different readers for raft and state engine.
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
