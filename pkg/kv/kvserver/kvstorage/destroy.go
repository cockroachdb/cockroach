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
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
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
	ctx context.Context, rw ReadWriter, info DestroyReplicaInfo, next roachpb.ReplicaID,
) error {
	return destroyReplicaImpl(ctx, rw, info, next)
}

func destroyReplicaImpl(
	ctx context.Context, rw ReadWriter, info DestroyReplicaInfo, next roachpb.ReplicaID,
) error {
	if next <= info.ReplicaID {
		return errors.AssertionFailedf("%v must not survive its own tombstone", info.FullReplicaID)
	}
	sl := MakeStateLoader(info.RangeID)
	// Assert that the ReplicaID in storage matches the one being removed.
	if loaded, err := sl.LoadRaftReplicaID(ctx, rw.State.RO); err != nil {
		return err
	} else if id := loaded.ReplicaID; id != info.ReplicaID {
		return errors.AssertionFailedf("%v has a mismatching ID %d", info.FullReplicaID, id)
	}
	// Assert that the provided tombstone moves the existing one strictly forward.
	// A failure would indicate that something is wrong in the replica lifecycle.
	if ts, err := sl.LoadRangeTombstone(ctx, rw.State.RO); err != nil {
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
	span := roachpb.Span{
		Key:    keys.MakeRangeIDReplicatedPrefix(info.RangeID),
		EndKey: keys.RangeTombstoneKey(info.RangeID),
	}
	if err := storage.ClearRangeWithHeuristic(
		ctx, rw.State.RO, rw.State.WO,
		span.Key, span.EndKey, ClearRangeThresholdPointKeys(),
	); err != nil {
		return err
	}
	// Save a tombstone to ensure that replica IDs never get reused.
	if err := sl.SetRangeTombstone(ctx, rw.State.WO, kvserverpb.RangeTombstone{
		NextReplicaID: next, // NB: NextReplicaID > 0
	}); err != nil {
		return err
	}

	// Clear the rest of the RangeID-local unreplicated keys. These are all raft
	// engine keys, except for RaftReplicaIDKey. Make a stop at the latter, and
	// clear it manually (note that it always exists for an existing replica, and
	// we have asserted that above).
	//
	// TODO(pav-kv): make a helper for piece-wise clearing, instead of using a
	// series of ClearRangeWithHeuristic.
	span = roachpb.Span{
		Key:    span.EndKey.Next(), // RangeTombstoneKey.Next()
		EndKey: keys.RaftReplicaIDKey(info.RangeID),
	}
	// TODO(#152845): with separated raft storage, clear only the unapplied suffix
	// of the raft log, which is in this span.
	if err := storage.ClearRangeWithHeuristic(
		ctx, rw.Raft.RO, rw.Raft.WO,
		span.Key, span.EndKey, ClearRangeThresholdPointKeys(),
	); err != nil {
		return err
	}
	if err := sl.ClearRaftReplicaID(rw.State.WO); err != nil {
		return err
	}
	span = roachpb.Span{
		Key:    span.EndKey.Next(), // RaftReplicaIDKey.Next()
		EndKey: keys.MakeRangeIDUnreplicatedPrefix(info.RangeID).PrefixEnd(),
	}
	if err := storage.ClearRangeWithHeuristic(
		ctx, rw.Raft.RO, rw.Raft.WO,
		span.Key, span.EndKey, ClearRangeThresholdPointKeys(),
	); err != nil {
		return err
	}

	// Finally, clear all the user keys (MVCC keyspace and the corresponding
	// system and lock table keys), if info.Keys is not empty.
	for _, span := range rditer.Select(info.RangeID, rditer.SelectOpts{
		Ranged: rditer.SelectAllRanged(info.Keys),
	}) {
		if err := storage.ClearRangeWithHeuristic(
			ctx, rw.State.RO, rw.State.WO,
			span.Key, span.EndKey, ClearRangeThresholdPointKeys(),
		); err != nil {
			return err
		}
	}
	return nil
}

// SubsumeReplica is like DestroyReplica, but it does not delete the user keys
// (and the corresponding system and lock table keys). The latter are inherited
// by the subsuming range.
func SubsumeReplica(ctx context.Context, rw ReadWriter, info DestroyReplicaInfo) error {
	info.Keys = roachpb.RSpan{} // forget about the user keys
	return destroyReplicaImpl(ctx, rw, info, MergedTombstoneReplicaID)
}

// RemoveStaleRHSFromSplit removes all replicated data for the RHS replica of a
// split. This is used in a situation when the RHS replica is already known to
// have been removed from our store, so any pending writes that were supposed to
// initialize the RHS replica should be dropped from the write batch.
func RemoveStaleRHSFromSplit(
	ctx context.Context, stateRW State, rangeID roachpb.RangeID, keys roachpb.RSpan,
) error {
	for _, span := range rditer.Select(rangeID, rditer.SelectOpts{
		// Since the RHS replica is uninitalized, we know there isn't anything in
		// the replicated spans below, before the current batch. Setting these
		// options will in effect only clear the writes to the RHS replicated state
		// staged in the batch.
		ReplicatedByRangeID: true,
		Ranged:              rditer.SelectAllRanged(keys),
		// Leave the unreplicated keys intact. The unreplicated space either belongs
		// to a newer (uninitialized) replica, or is empty and only contains a
		// RangeTombstone with a higher ReplicaID than the RHS in the split trigger.
		UnreplicatedByRangeID: false,
	}) {
		if err := storage.ClearRangeWithHeuristic(
			ctx, stateRW.RO, stateRW.WO, span.Key, span.EndKey, ClearRangeThresholdPointKeys(),
		); err != nil {
			return err
		}
	}
	return nil
}

// RewriteRaftState rewrites the raft state of the given replica with the
// provided state. Specifically, it rewrites HardState and RaftTruncatedState,
// and clears the raft log. All writes are generated in the engine keys order.
func RewriteRaftState(
	ctx context.Context,
	raftWO RaftWO,
	sl StateLoader,
	hs raftpb.HardState,
	ts kvserverpb.RaftTruncatedState,
) error {
	// Update HardState.
	if err := sl.SetHardState(ctx, raftWO, hs); err != nil {
		return errors.Wrapf(err, "unable to write HardState")
	}
	// Clear the raft log. Note that there are no Pebble range keys in this span.
	raftLog := sl.RaftLogPrefix() // NB: use only until next StateLoader call
	if err := raftWO.ClearRawRange(
		raftLog, raftLog.PrefixEnd(), true /* pointKeys */, false, /* rangeKeys */
	); err != nil {
		return errors.Wrapf(err, "unable to clear the raft log")
	}
	// Update the log truncation state.
	if err := sl.SetRaftTruncatedState(ctx, raftWO, &ts); err != nil {
		return errors.Wrapf(err, "unable to write RaftTruncatedState")
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
