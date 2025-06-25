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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// prepareSnapApplyInput contains the data needed to prepare the on-disk state for a snapshot.
type prepareSnapApplyInput struct {
	id storage.FullReplicaID

	st       *cluster.Settings
	todoEng  storage.Engine
	sl       stateloader.StateLoader
	writeSST func(context.Context, []byte) error

	truncState kvserverpb.RaftTruncatedState
	hardState  raftpb.HardState
	desc       *roachpb.RangeDescriptor // corresponds to the range descriptor in the snapshot
	origDesc   *roachpb.RangeDescriptor // pre-snapshot range desciptor
	// NB: subsumedDescs, if set, must be in sorted (by start key) order.
	subsumedDescs []*roachpb.RangeDescriptor
}

// prepareSnapApply writes the unreplicated SST for the snapshot and clears disk
// data for subsumed replicas.
func prepareSnapApply(
	ctx context.Context, input prepareSnapApplyInput,
) (
	clearedUnreplicatedSpan roachpb.Span,
	clearedSubsumedSpans []roachpb.Span,
	clearedSplitSpans []roachpb.Span,
	_ error,
) {
	_ = applySnapshotTODO // 3.1 + 1.1 + 2.5.
	unreplicatedSSTFile, clearedUnreplicatedSpan, err := writeUnreplicatedSST(
		ctx, input.id, input.st, input.truncState, input.hardState, input.sl,
	)
	if err != nil {
		return roachpb.Span{}, nil, nil, err
	}
	_ = applySnapshotTODO // add to 2.4.
	if err := input.writeSST(ctx, unreplicatedSSTFile.Data()); err != nil {
		return roachpb.Span{}, nil, nil, err
	}

	_ = applySnapshotTODO // 3.2 + 2.1 + 2.2 + 2.3
	clearedSubsumedSpans, err = clearSubsumedReplicaDiskData(
		ctx, input.st, input.todoEng, input.writeSST,
		input.desc, input.subsumedDescs,
	)
	if err != nil {
		return roachpb.Span{}, nil, nil, err
	}

	rightMostDesc := input.origDesc
	if len(input.subsumedDescs) != 0 {
		// NB: We might have to create SSTs for the range local keys, lock table
		// keys, and user keys depending on if the subsumed replicas are not fully
		// contained by the replica in our snapshot. The following is an example to
		// this case happening.
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
		// So, we need to additionally clear [b,sn). This is handled by the call to
		// clearSplitReplicaDiskData below by correctly passing in the
		// rightMostDesc. Also see the commentary on that function.
		//
		// [*] s1, ..., sn are the end keys for the subsumed replicas (for the
		// current keySpan).
		rightMostDesc = input.subsumedDescs[len(input.subsumedDescs)-1]

		// In the other case, where the subsumed replicas are fully contained in the
		// snapshot:
		//
		// [a---s1---...---sn)
		// [a---------------------b)
		//
		// We don't need to clear additional keyspace here, since clearing [a,b)
		// will also clear the keyspace owned by all the subsumed replicas. Any
		// other per-replica key spans were cleared by the call to
		// clearSubsumedReplicaDiskData above.
		// TODO(arul): ^ during review, lookling for suggestions for more precise
		// wording for this.
	}

	clearedSplitSpans, err = clearSplitReplicaDiskData(
		ctx, input.st, input.todoEng, input.writeSST,
		input.desc, rightMostDesc,
	)
	if err != nil {
		return roachpb.Span{}, nil, nil, err
	}

	return clearedUnreplicatedSpan, clearedSubsumedSpans, clearedSplitSpans, nil
}

// writeUnreplicatedSST creates an SST for snapshot application that
// covers the RangeID-unreplicated keyspace. A range tombstone is
// laid down and the Raft state provided by the arguments is overlaid
// onto it.
func writeUnreplicatedSST(
	ctx context.Context,
	id storage.FullReplicaID,
	st *cluster.Settings,
	ts kvserverpb.RaftTruncatedState,
	hs raftpb.HardState,
	sl stateloader.StateLoader,
) (_ *storage.MemObject, clearedSpan roachpb.Span, _ error) {
	unreplicatedSSTFile := &storage.MemObject{}
	unreplicatedSST := storage.MakeIngestionSSTWriter(
		ctx, st, unreplicatedSSTFile,
	)
	defer unreplicatedSST.Close()
	// Clear the raft state/log, and initialize it again with the provided
	// HardState and RaftTruncatedState.
	clearedSpan, err := rewriteRaftState(ctx, id, hs, ts, sl, &unreplicatedSST)
	if err != nil {
		return nil, roachpb.Span{}, err
	}
	if err := unreplicatedSST.Finish(); err != nil {
		return nil, roachpb.Span{}, err
	}
	return unreplicatedSSTFile, clearedSpan, nil
}

// rewriteRaftState clears and rewrites the unreplicated rangeID-local key space
// of the given replica with the provided raft state. Note that it also clears
// the raft log contents.
//
// The caller must make sure the log does not have entries newer than the
// snapshot entry ID, and that clearing the log is applied atomically with the
// snapshot write, or after the latter is synced.
func rewriteRaftState(
	ctx context.Context,
	id storage.FullReplicaID,
	hs raftpb.HardState,
	ts kvserverpb.RaftTruncatedState,
	sl stateloader.StateLoader,
	w storage.Writer,
) (clearedSpan roachpb.Span, _ error) {
	// Clearing the unreplicated state.
	//
	// NB: We do not expect to see range keys in the unreplicated state, so
	// we don't drop a range tombstone across the range key space.
	unreplicatedPrefixKey := keys.MakeRangeIDUnreplicatedPrefix(id.RangeID)
	unreplicatedStart := unreplicatedPrefixKey
	unreplicatedEnd := unreplicatedPrefixKey.PrefixEnd()
	clearedSpan = roachpb.Span{Key: unreplicatedStart, EndKey: unreplicatedEnd}
	if err := w.ClearRawRange(
		unreplicatedStart, unreplicatedEnd, true /* pointKeys */, false, /* rangeKeys */
	); err != nil {
		return roachpb.Span{}, errors.Wrapf(err, "error clearing the unreplicated space")
	}

	// Update HardState.
	if err := sl.SetHardState(ctx, w, hs); err != nil {
		return roachpb.Span{}, errors.Wrapf(err, "unable to write HardState")
	}
	// We've cleared all the raft state above, so we are forced to write the
	// RaftReplicaID again here.
	if err := sl.SetRaftReplicaID(ctx, w, id.ReplicaID); err != nil {
		return roachpb.Span{}, errors.Wrapf(err, "unable to write RaftReplicaID")
	}
	// Update the log truncation state.
	if err := sl.SetRaftTruncatedState(ctx, w, &ts); err != nil {
		return roachpb.Span{}, errors.Wrapf(err, "unable to write RaftTruncatedState")
	}
	return clearedSpan, nil
}

// clearSubsumedReplicaDiskData clears the on disk data of the subsumed
// replicas by creating SSTs with range deletion tombstones. We have to be
// careful here not to have overlapping ranges with the SSTs we have already
// created since that will throw an error while we are ingesting them. This
// method requires that each of the subsumed replicas raftMu is held, and that
// the Reader reflects the latest I/O each of the subsumed replicas has done
// (i.e. Reader was instantiated after all raftMu were acquired).
//
// NB: does nothing if subsumedDescs is empty. If non-empty, the caller must
// ensure that subsumedDescs is sorted in ascending order by start key.
func clearSubsumedReplicaDiskData(
	ctx context.Context,
	st *cluster.Settings,
	reader storage.Reader,
	writeSST func(context.Context, []byte) error,
	desc *roachpb.RangeDescriptor,
	subsumedDescs []*roachpb.RangeDescriptor,
) (clearedSpans []roachpb.Span, _ error) {
	if len(subsumedDescs) == 0 {
		return // no subsumed replicas to speak of; early return
	}
	// NB: The snapshot must never subsume a replica that extends the range of the
	// replica to the left. This is because splits and merges (the only
	// operation that change the key bounds) always leave the start key intact.
	// Extending to the left implies that either we merged "to the left" (we
	// don't), or that we're applying a snapshot for another range (we don't do
	// that either). Something is severely wrong for this to happen, so perform
	// a sanity check.
	if subsumedDescs[0].StartKey.Compare(desc.StartKey) < 0 { // subsumedDescs are sorted by StartKey
		log.Fatalf(ctx, "subsuming replica to our left; key span: %v; total key span %v",
			subsumedDescs[0].StartKey, desc.StartKey)
	}

	// NB: we don't clear RangeID local key spans here. That happens
	// via the call to DestroyReplica.
	for _, subDesc := range subsumedDescs {
		// We have to create an SST for the subsumed replica's range-id local keys.
		subsumedReplSSTFile := &storage.MemObject{}
		subsumedReplSST := storage.MakeIngestionSSTWriter(
			ctx, st, subsumedReplSSTFile,
		)
		// NOTE: We set mustClearRange to true because we are setting
		// RangeTombstoneKey. Since Clears and Puts need to be done in increasing
		// order of keys, it is not safe to use ClearRangeIter.
		opts := kvstorage.ClearRangeDataOptions{
			ClearReplicatedByRangeID:   true,
			ClearUnreplicatedByRangeID: true,
			MustUseClearRange:          true,
		}
		subsumedClearedSpans := rditer.Select(subDesc.RangeID, rditer.SelectOpts{
			ReplicatedByRangeID:   opts.ClearReplicatedByRangeID,
			UnreplicatedByRangeID: opts.ClearUnreplicatedByRangeID,
		})
		clearedSpans = append(clearedSpans, subsumedClearedSpans...)
		if err := kvstorage.DestroyReplica(ctx, subDesc.RangeID, reader, &subsumedReplSST, mergedTombstoneReplicaID, opts); err != nil {
			subsumedReplSST.Close()
			return nil, err
		}
		if err := subsumedReplSST.Finish(); err != nil {
			return nil, err
		}
		if subsumedReplSST.DataSize > 0 {
			// TODO(itsbilal): Write to SST directly in subsumedReplSST rather than
			// buffering in a MemObject first.
			if err := writeSST(ctx, subsumedReplSSTFile.Data()); err != nil {
				return nil, err
			}
		}
	}

	return clearedSpans, nil
}

// clearSplitReplicaDiskData clears the on disk data of any replica that has
// been split, and we've learned of this split when applying the snapshot
// because the snapshot narrows the range. We clear on-disk data by creating
// SSTs with range deletion tombstones.
//
// The supplied rightMostDesc corresponds to the right-most descriptor on the
// store that overlaps with the descriptor in the snapshot. In the simplest
// case, where a replica (LHS) learns about the split through the snapshot, this
// is the descriptor of the replica itself:
//
// original descriptor: [a-----------------------------c)
// snapshot descriptor: [a---------------------b)
//
// The more involved case is when one or more replicas have been merged into the
// LHS before the split, and the LHS is learning about all of these through the
// snapshot -- in this case, the rightMostDesc corresponds to the right-most
// subsumed replica:
//
// store descriptors:   [a----------------s1---...---sn)
// snapshot descriptor: [a---------------------b)
//
// In the diagram above, S1...Sn correspond to subsumed replicas with end keys
// s1...sn respectively. These are all replicas on the store that overlap with
// the snapshot descriptor, covering the range [a,b), and the rightMostDesc is
// the replica Sn.
//
// clearSplitReplicaDiskState is a no-op if there is no split, i.e, the
// rightMostDesc.EndKey indicates that it's narrower than the snapshot's
// descriptor.
func clearSplitReplicaDiskData(
	ctx context.Context,
	st *cluster.Settings,
	reader storage.Reader,
	writeSST func(context.Context, []byte) error,
	snapDesc *roachpb.RangeDescriptor,
	rightMostDesc *roachpb.RangeDescriptor,
) (clearedSpans []roachpb.Span, _ error) {
	if rightMostDesc.EndKey.Compare(snapDesc.EndKey) <= 0 {
		return // no-op, no split
	}

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
	keySpans := getKeySpans(snapDesc)
	origKeySpans := getKeySpans(rightMostDesc)

	for i := range origKeySpans {
		rhsReplSSTFile := &storage.MemObject{}
		rhsReplSST := storage.MakeIngestionSSTWriter(
			ctx, st, rhsReplSSTFile,
		)
		if err := storage.ClearRangeWithHeuristic(
			ctx,
			reader,
			&rhsReplSST,
			keySpans[i].EndKey,
			origKeySpans[i].EndKey,
			kvstorage.ClearRangeThresholdPointKeys,
		); err != nil {
			rhsReplSST.Close()
			return nil, err
		}
		clearedSpans = append(clearedSpans,
			roachpb.Span{Key: keySpans[i].EndKey, EndKey: origKeySpans[i].EndKey})
		if err := rhsReplSST.Finish(); err != nil {
			return nil, err
		}
		if rhsReplSST.DataSize > 0 {
			// TODO(arul): write to SST directly in rhsReplSST rather than
			// buffering in a MemObject first.
			if err := writeSST(ctx, rhsReplSSTFile.Data()); err != nil {
				return nil, err
			}
		}
	}
	return clearedSpans, nil
}
