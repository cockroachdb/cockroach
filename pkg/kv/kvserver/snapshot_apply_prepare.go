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

	truncState    kvserverpb.RaftTruncatedState
	hardState     raftpb.HardState
	desc          *roachpb.RangeDescriptor
	subsumedDescs []*roachpb.RangeDescriptor

	// cleared contains the spans that this snapshot application clears before
	// writing new state on top.
	cleared []roachpb.Span
}

// prepareSnapApply writes the unreplicated SST for the snapshot and clears disk data for subsumed replicas.
func (s *snapWriteBuilder) prepareSnapApply(ctx context.Context) error {
	_ = applySnapshotTODO // 1.1 + 1.3 + 2.4 + 3.1
	if err := s.writeSST(ctx, s.rewriteRaftState); err != nil {
		return err
	}
	_ = applySnapshotTODO // 1.2 + 2.1 + 2.2 + 2.3 (diff) + 3.2
	return s.clearSubsumedReplicaDiskData(ctx)
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
// NB: does nothing if subsumedDescs is empty.
func (s *snapWriteBuilder) clearSubsumedReplicaDiskData(ctx context.Context) error {
	// TODO(sep-raft-log): need different readers for raft and state engine.
	reader := storage.Reader(s.todoEng)

	// NB: we don't clear RangeID local key spans here. That happens
	// via the call to DestroyReplica.
	getKeySpans := func(d *roachpb.RangeDescriptor) []roachpb.Span {
		return rditer.Select(d.RangeID, rditer.SelectOpts{
			Ranged: rditer.SelectRangedOptions{
				RSpan:      d.RSpan(),
				SystemKeys: true,
				UserKeys:   true,
				LockTable:  true,
			},
		})
	}
	keySpans := getKeySpans(s.desc)
	totalKeySpans := append([]roachpb.Span(nil), keySpans...)
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
			return kvstorage.DestroyReplica(ctx, subDesc.RangeID, reader, w, mergedTombstoneReplicaID, opts)
		}); err != nil {
			return err
		}

		srKeySpans := getKeySpans(subDesc)
		// Compute the total key space covered by the current replica and all
		// subsumed replicas.
		for i := range srKeySpans {
			if srKeySpans[i].Key.Compare(totalKeySpans[i].Key) < 0 {
				totalKeySpans[i].Key = srKeySpans[i].Key
			}
			if srKeySpans[i].EndKey.Compare(totalKeySpans[i].EndKey) > 0 {
				totalKeySpans[i].EndKey = srKeySpans[i].EndKey
			}
		}
	}

	// We might have to create SSTs for the range local keys, lock table keys,
	// and user keys depending on if the subsumed replicas are not fully
	// contained by the replica in our snapshot. The following is an example to
	// this case happening.
	//
	// a       b       c       d
	// |---1---|-------2-------|  S1
	// |---1-------------------|  S2
	// |---1-----------|---3---|  S3
	//
	// Since the merge is the first operation to happen, a follower could be down
	// before it completes. It is reasonable for a snapshot for r1 from S3 to
	// subsume both r1 and r2 in S1.
	for i := range keySpans {
		// The snapshot must never subsume a replica that extends the range of the
		// replica to the left. This is because splits and merges (the only
		// operation that change the key bounds) always leave the start key intact.
		// Extending to the left implies that either we merged "to the left" (we
		// don't), or that we're applying a snapshot for another range (we don't do
		// that either). Something is severely wrong for this to happen.
		if totalKeySpans[i].Key.Compare(keySpans[i].Key) < 0 {
			log.Fatalf(ctx, "subsuming replica to our left; key span: %v; total key span %v",
				keySpans[i], totalKeySpans[i])
		}

		// In the comments below, s1, ..., sn are the end keys for the subsumed
		// replicas (for the current keySpan).
		// Note that if there aren't any subsumed replicas (the common case), the
		// next comparison is always zero and this loop is a no-op.

		if totalKeySpans[i].EndKey.Compare(keySpans[i].EndKey) <= 0 {
			// The subsumed replicas are fully contained in the snapshot:
			//
			// [a---s1---...---sn)
			// [a---------------------b)
			//
			// We don't need to clear additional keyspace here, since clearing `[a,b)`
			// will also clear all subsumed replicas.
			continue
		}

		// The subsumed replicas extend past the snapshot:
		//
		// [a----------------s1---...---sn)
		// [a---------------------b)
		//
		// We need to additionally clear [b,sn).

		if err := s.writeSST(ctx, func(ctx context.Context, w storage.Writer) error {
			return storage.ClearRangeWithHeuristic(
				ctx, reader, w,
				keySpans[i].EndKey, totalKeySpans[i].EndKey,
				kvstorage.ClearRangeThresholdPointKeys,
			)
		}); err != nil {
			return err
		}
		s.cleared = append(s.cleared,
			roachpb.Span{Key: keySpans[i].EndKey, EndKey: totalKeySpans[i].EndKey})
	}
	return nil
}
