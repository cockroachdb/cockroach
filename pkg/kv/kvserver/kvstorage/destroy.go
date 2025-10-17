// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

const (
	// ClearRangeThresholdPointKeys is the threshold (as number of point keys)
	// beyond which we'll clear range data using a Pebble range tombstone rather
	// than individual Pebble point tombstones.
	//
	// It is expensive for there to be many Pebble range tombstones in the same
	// sstable because all of the tombstones in an sstable are loaded whenever the
	// sstable is accessed. So we avoid using range deletion unless there is some
	// minimum number of keys. The value here was pulled out of thin air. It might
	// be better to make this dependent on the size of the data being deleted. Or
	// perhaps we should fix Pebble to handle large numbers of range tombstones in
	// an sstable better.
	ClearRangeThresholdPointKeys = 64

	// MergedTombstoneReplicaID is the replica ID written into the RangeTombstone
	// for replicas of a range which is known to have been merged. This value
	// should prevent any messages from stale replicas of that range from ever
	// resurrecting merged replicas. Whenever merging or subsuming a replica we
	// know new replicas can never be created so this value is used even if we
	// don't know the current replica ID.
	MergedTombstoneReplicaID roachpb.ReplicaID = math.MaxInt32
)

// clearRangeDataOptions specify which parts of a Replica are to be destroyed.
type clearRangeDataOptions struct {
	// clearReplicatedByRangeID indicates that replicated RangeID-based keys
	// (abort span, etc) should be removed.
	clearReplicatedByRangeID bool
	// clearUnreplicatedByRangeID indicates that unreplicated RangeID-based keys
	// (logstore state incl. HardState, etc) should be removed.
	clearUnreplicatedByRangeID bool
	// clearReplicatedBySpan causes the state machine data (i.e. the replicated state
	// for the given RSpan) that is key-addressable (i.e. range descriptor, user keys,
	// locks) to be removed. No data is removed if this is the zero span.
	clearReplicatedBySpan roachpb.RSpan

	// If mustUseClearRange is true, a Pebble range tombstone will always be used
	// to clear the key spans (unless empty). This is typically used when we need
	// to write additional keys to an SST after this clear, e.g. a replica
	// tombstone, since keys must be written in order. When this is false, a
	// heuristic will be used instead.
	mustUseClearRange bool
}

// clearRangeData clears the data associated with a range descriptor selected
// by the provided options.
//
// TODO(tbg): could rename this to XReplica. The use of "Range" in both the
// "CRDB Range" and "storage.ClearRange" context in the setting of this method could
// be confusing.
func clearRangeData(
	ctx context.Context,
	rangeID roachpb.RangeID,
	reader storage.Reader,
	writer storage.Writer,
	opts clearRangeDataOptions,
) error {
	keySpans := rditer.Select(rangeID, rditer.SelectOpts{
		Ranged: rditer.SelectRangedOptions{
			RSpan:      opts.clearReplicatedBySpan,
			SystemKeys: true,
			LockTable:  true,
			UserKeys:   true,
		},
		ReplicatedByRangeID:   opts.clearReplicatedByRangeID,
		UnreplicatedByRangeID: opts.clearUnreplicatedByRangeID,
	})

	pointKeyThreshold := ClearRangeThresholdPointKeys
	if opts.mustUseClearRange {
		pointKeyThreshold = 1
	}

	for _, keySpan := range keySpans {
		if err := storage.ClearRangeWithHeuristic(
			ctx, reader, writer, keySpan.Key, keySpan.EndKey, pointKeyThreshold,
		); err != nil {
			return err
		}
	}
	return nil
}

// DestroyReplicaTODO is the plan for splitting DestroyReplica into cross-engine
// writes.
//
//  1. Log storage write (durable):
//     1.1. WAG: apply to RaftAppliedIndex.
//     1.2. WAG: apply mutation (2).
//  2. State machine mutation:
//     2.1. Clear RangeID-local un-/replicated state.
//     2.2. (optional) Clear replicated MVCC span.
//     2.3. Write RangeTombstone with next ReplicaID.
//  3. Log engine GC (after state machine mutation 2 is durably applied):
//     3.1. Remove raft state.
//
// TODO(sep-raft-log): support the status quo in which 2+3 is written
// atomically, and 1 is not written.
const DestroyReplicaTODO = 0

// DestroyReplicaInfo contains the replica's metadata needed for its removal
// from storage.
//
// TODO(pav-kv): for separated storage, add the applied raft log span. #152845
type DestroyReplicaInfo struct {
	// FullReplicaID identifies the replica on its store.
	roachpb.FullReplicaID
	// Keys is the user key span of this replica, taken from its RangeDescriptor.
	// Non-empty iff the replica is initialized.
	Keys roachpb.RSpan
}

// DestroyReplica destroys the entirety of the replica's state in storage, and
// installs a RangeTombstone in its place. It handles both uninitialized and
// initialized replicas uniformly.
func DestroyReplica(
	ctx context.Context,
	reader storage.Reader,
	writer storage.Writer,
	info DestroyReplicaInfo,
	next roachpb.ReplicaID,
) error {
	return destroyReplicaImpl(ctx, reader, writer, info, next, clearRangeDataOptions{
		clearReplicatedByRangeID:   true,
		clearUnreplicatedByRangeID: true,
		clearReplicatedBySpan:      info.Keys,
	})
}

func destroyReplicaImpl(
	ctx context.Context,
	reader storage.Reader,
	writer storage.Writer,
	info DestroyReplicaInfo,
	next roachpb.ReplicaID,
	opts clearRangeDataOptions,
) error {
	if next <= info.ReplicaID {
		return errors.AssertionFailedf("%v must not survive its own tombstone", info.FullReplicaID)
	}
	sl := MakeStateLoader(info.RangeID)
	// Assert that the ReplicaID in storage matches the one being removed.
	if loaded, err := sl.LoadRaftReplicaID(ctx, reader); err != nil {
		return err
	} else if id := loaded.ReplicaID; id != info.ReplicaID {
		return errors.AssertionFailedf("%v has a mismatching ID %d", info.FullReplicaID, id)
	}
	// Assert that the provided tombstone moves the existing one strictly forward.
	// A failure would indicate that something is wrong in the replica lifecycle.
	if ts, err := sl.LoadRangeTombstone(ctx, reader); err != nil {
		return err
	} else if next <= ts.NextReplicaID {
		return errors.AssertionFailedf("%v cannot rewind tombstone from %d to %d",
			info.FullReplicaID, ts.NextReplicaID, next)
	}

	_ = DestroyReplicaTODO // 2.1 + 2.2 + 3.1
	if err := clearRangeData(ctx, info.RangeID, reader, writer, opts); err != nil {
		return err
	}
	// Save a tombstone to ensure that replica IDs never get reused.
	_ = DestroyReplicaTODO // 2.3
	return sl.SetRangeTombstone(ctx, writer, kvserverpb.RangeTombstone{
		NextReplicaID: next, // NB: NextReplicaID > 0
	})
}

// SubsumeReplica is like DestroyReplica, but it does not delete the user keys
// (and the corresponding system and lock table keys). The latter are inherited
// by the subsuming range.
//
// The forceSortedKeys flag, if true, forces the writes to be generated in the
// sorted order of keys. This is to support feeding the writes from this
// function to SSTables, in the snapshot ingestion path.
//
// Returns SelectOpts which can be used to reflect on the key spans that this
// function clears.
// TODO(pav-kv): get rid of SelectOpts.
func SubsumeReplica(
	ctx context.Context,
	reader storage.Reader,
	writer storage.Writer,
	info DestroyReplicaInfo,
	forceSortedKeys bool,
) (rditer.SelectOpts, error) {
	// NB: if required, set MustUseClearRange to true. This call can be used for
	// generating SSTables when ingesting a snapshot, which requires Clears and
	// Puts to be written in key order. DestroyReplica sets RangeTombstoneKey
	// after clearing the unreplicated span which may contain higher keys.
	opts := clearRangeDataOptions{
		clearReplicatedByRangeID:   true,
		clearUnreplicatedByRangeID: true,
		mustUseClearRange:          forceSortedKeys,
	}
	return rditer.SelectOpts{
		ReplicatedByRangeID:   opts.clearReplicatedByRangeID,
		UnreplicatedByRangeID: opts.clearUnreplicatedByRangeID,
	}, destroyReplicaImpl(ctx, reader, writer, info, MergedTombstoneReplicaID, opts)
}

// RemoveStaleRHSFromSplit removes all data for the RHS replica of a split. This
// is used in a situation when the RHS replica is already known to have been
// removed from our store, so any pending writes that were supposed to
// initialize the RHS replica should be dropped from the write batch.
//
// TODO(#152199): do not remove the unreplicated state which can belong to a
// newer (uninitialized) replica.
func RemoveStaleRHSFromSplit(
	ctx context.Context,
	reader storage.Reader,
	writer storage.Writer,
	rangeID roachpb.RangeID,
	keys roachpb.RSpan,
) error {
	return clearRangeData(ctx, rangeID, reader, writer, clearRangeDataOptions{
		// Since the RHS replica is uninitalized, we know there isn't anything in
		// the two replicated spans below, before the current batch. Setting these
		// options will in effect only clear the writes to the RHS replicated state
		// staged in the batch.
		clearReplicatedBySpan:    keys,
		clearReplicatedByRangeID: true,
		// TODO(tbg): we don't actually want to touch the raft state of the RHS
		// replica since it's absent or a more recent one than in the split. Now
		// that we have a bool targeting unreplicated RangeID-local keys, we can set
		// it to false and remove the HardState+ReplicaID write-back in the caller.
		// However, there can be historical split proposals with the
		// RaftTruncatedState key set in splitTriggerHelper[^1]. We must first make
		// sure that such proposals no longer exist, e.g. with a below-raft
		// migration.
		//
		// [^1]: https://github.com/cockroachdb/cockroach/blob/f263a765d750e41f2701da0a923a6e92d09159fa/pkg/kv/kvserver/batcheval/cmd_end_transaction.go#L1109-L1149
		//
		// See also: https://github.com/cockroachdb/cockroach/issues/94933
		clearUnreplicatedByRangeID: true,
	})
}
