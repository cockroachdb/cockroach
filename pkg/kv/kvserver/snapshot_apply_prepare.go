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

	truncState    kvserverpb.RaftTruncatedState
	hardState     raftpb.HardState
	desc          *roachpb.RangeDescriptor
	subsumedDescs []*roachpb.RangeDescriptor
}

// prepareSnapApply writes the unreplicated SST for the snapshot and clears disk data for subsumed replicas.
func prepareSnapApply(
	ctx context.Context, input prepareSnapApplyInput,
) (
	roachpb.Span, // clearedUnreplicatedSpan
	[]roachpb.Span, // clearedSubsumedSpans
	error,
) {
	// Step 1: Write unreplicated SST
	unreplicatedSSTFile, clearedUnreplicatedSpan, err := writeUnreplicatedSST(
		ctx, input.id, input.st, input.truncState, input.hardState, input.sl,
	)
	if err != nil {
		return roachpb.Span{}, nil, err
	}
	if err := input.writeSST(ctx, unreplicatedSSTFile.Data()); err != nil {
		return roachpb.Span{}, nil, err
	}

	clearedSubsumedSpans, err := clearSubsumedReplicaDiskData(
		ctx, input.st, input.todoEng, input.writeSST,
		input.desc, input.subsumedDescs,
	)
	if err != nil {
		return roachpb.Span{}, nil, err
	}

	return clearedUnreplicatedSpan, clearedSubsumedSpans, nil
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
// NB: does nothing if subsumedDescs is empty.
func clearSubsumedReplicaDiskData(
	ctx context.Context,
	st *cluster.Settings,
	reader storage.Reader,
	writeSST func(context.Context, []byte) error,
	desc *roachpb.RangeDescriptor,
	subsumedDescs []*roachpb.RangeDescriptor,
) (clearedSpans []roachpb.Span, _ error) {
	// NB: we don't clear RangeID local key spans here. That happens
	// via the call to DestroyReplica.
	getKeySpans := func(d *roachpb.RangeDescriptor) []roachpb.Span {
		return rditer.Select(d.RangeID, rditer.SelectOpts{
			ReplicatedBySpan: d.RSpan(),
		})
	}
	keySpans := getKeySpans(desc)
	totalKeySpans := append([]roachpb.Span(nil), keySpans...)
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

		subsumedReplSSTFile := &storage.MemObject{}
		subsumedReplSST := storage.MakeIngestionSSTWriter(
			ctx, st, subsumedReplSSTFile,
		)
		if err := storage.ClearRangeWithHeuristic(
			ctx,
			reader,
			&subsumedReplSST,
			keySpans[i].EndKey,
			totalKeySpans[i].EndKey,
			kvstorage.ClearRangeThresholdPointKeys,
		); err != nil {
			subsumedReplSST.Close()
			return nil, err
		}
		clearedSpans = append(clearedSpans,
			roachpb.Span{Key: keySpans[i].EndKey, EndKey: totalKeySpans[i].EndKey})
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
