// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// MergedTombstoneReplicaID is the replica ID written into the RangeTombstone
// for replicas of a range which is known to have been merged. This value
// should prevent any messages from stale replicas of that range from ever
// resurrecting merged replicas. Whenever merging or subsuming a replica we
// know new replicas can never be created so this value is used even if we
// don't know the current replica ID.
const MergedTombstoneReplicaID roachpb.ReplicaID = math.MaxInt32

// clearRangeThresholdPointKeys is the value of ClearRangeThresholdPointKeys().
// Can be overridden only in tests, in order to deterministically force using
// ClearRawRange instead of point clears, when destroying replicas.
var clearRangeThresholdPointKeys = 64

// ClearRangeThresholdPointKeys returns the threshold (as number of point keys)
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
func ClearRangeThresholdPointKeys() int {
	return clearRangeThresholdPointKeys
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
//
// This call issues all writes ordered by key. This is to support a large
// variety of uses, including SSTable generation for snapshot application.
func DestroyReplica(
	ctx context.Context,
	reader storage.Reader,
	writer storage.Writer,
	info DestroyReplicaInfo,
	next roachpb.ReplicaID,
) error {
	return destroyReplicaImpl(ctx, reader, writer, info, next)
}

func destroyReplicaImpl(
	ctx context.Context,
	reader storage.Reader,
	writer storage.Writer,
	info DestroyReplicaInfo,
	next roachpb.ReplicaID,
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

	_ = DestroyReplicaTODO
	// Clear all range data in sorted order of engine keys. This call can be used
	// for generating SSTables when ingesting a snapshot, which requires Clears
	// and Puts to be written in key order.
	//
	// All the code below is equivalent to clearing all the spans that the
	// following SelectOpts represents, and leaving only the RangeTombstoneKey
	// populated with the specified NextReplicaID.
	if buildutil.CrdbTestBuild {
		_ = rditer.SelectOpts{
			ReplicatedByRangeID:   true,
			UnreplicatedByRangeID: true,
			Ranged:                rditer.SelectAllRanged(info.Keys),
		}
	}

	// First, clear all RangeID-local replicated keys. Also, include all
	// RangeID-local unreplicated keys < RangeTombstoneKey as a drive-by, since we
	// decided that these (currently none) belong to the state machine engine.
	tombstoneKey := keys.RangeTombstoneKey(info.RangeID)
	if err := storage.ClearRangeWithHeuristic(
		ctx, reader, writer,
		keys.MakeRangeIDReplicatedPrefix(info.RangeID),
		tombstoneKey,
		ClearRangeThresholdPointKeys(),
	); err != nil {
		return err
	}
	// Save a tombstone to ensure that replica IDs never get reused.
	if err := sl.SetRangeTombstone(ctx, writer, kvserverpb.RangeTombstone{
		NextReplicaID: next, // NB: NextReplicaID > 0
	}); err != nil {
		return err
	}
	// Clear the rest of the RangeID-local unreplicated keys.
	// TODO(pav-kv): decompose this further to delete raft and state machine keys
	// separately. Currently, all the remaining keys in this span belong to the
	// raft engine except the RaftReplicaID.
	if err := storage.ClearRangeWithHeuristic(
		ctx, reader, writer,
		tombstoneKey.Next(),
		keys.MakeRangeIDUnreplicatedPrefix(info.RangeID).PrefixEnd(),
		ClearRangeThresholdPointKeys(),
	); err != nil {
		return err
	}
	// Finally, clear all the user keys (MVCC keyspace and the corresponding
	// system and lock table keys), if info.Keys is not empty.
	for _, span := range rditer.Select(info.RangeID, rditer.SelectOpts{
		Ranged: rditer.SelectAllRanged(info.Keys),
	}) {
		if err := storage.ClearRangeWithHeuristic(
			ctx, reader, writer, span.Key, span.EndKey, ClearRangeThresholdPointKeys(),
		); err != nil {
			return err
		}
	}
	return nil
}

// SubsumeReplica is like DestroyReplica, but it does not delete the user keys
// (and the corresponding system and lock table keys). The latter are inherited
// by the subsuming range.
//
// Returns SelectOpts which can be used to reflect on the key spans that this
// function clears.
// TODO(pav-kv): get rid of SelectOpts.
func SubsumeReplica(
	ctx context.Context, reader storage.Reader, writer storage.Writer, info DestroyReplicaInfo,
) (rditer.SelectOpts, error) {
	// Forget about the user keys.
	info.Keys = roachpb.RSpan{}
	return rditer.SelectOpts{
		ReplicatedByRangeID:   true,
		UnreplicatedByRangeID: true,
	}, destroyReplicaImpl(ctx, reader, writer, info, MergedTombstoneReplicaID)
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
	for _, span := range rditer.Select(rangeID, rditer.SelectOpts{
		// Since the RHS replica is uninitalized, we know there isn't anything in
		// the replicated spans below, before the current batch. Setting these
		// options will in effect only clear the writes to the RHS replicated state
		// staged in the batch.
		ReplicatedByRangeID: true,
		Ranged:              rditer.SelectAllRanged(keys),
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
		UnreplicatedByRangeID: true,
	}) {
		if err := storage.ClearRangeWithHeuristic(
			ctx, reader, writer, span.Key, span.EndKey, ClearRangeThresholdPointKeys(),
		); err != nil {
			return err
		}
	}
	return nil
}

// TestingForceClearRange changes the value of ClearRangeThresholdPointKeys to
// 1, which effectively forces ClearRawRange in the replica destruction path,
// instead of point deletions. This can be used for making the storage
// operations in tests deterministic.
//
// The caller must call the returned function at the end of the test, to restore
// the default value.
func TestingForceClearRange() func() {
	if !buildutil.CrdbTestBuild {
		panic("test-only function")
	}
	old := clearRangeThresholdPointKeys
	clearRangeThresholdPointKeys = 1 // forces ClearRawRange
	return func() {
		clearRangeThresholdPointKeys = old
	}
}
