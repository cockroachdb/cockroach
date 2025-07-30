// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
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
)

// ClearRangeDataOptions specify which parts of a Replica are to be destroyed.
type ClearRangeDataOptions struct {
	// ClearReplicatedByRangeID indicates that replicated RangeID-based keys
	// (abort span, etc) should be removed.
	ClearReplicatedByRangeID bool
	// ClearUnreplicatedByRangeID indicates that unreplicated RangeID-based keys
	// (logstore state incl. HardState, etc) should be removed.
	ClearUnreplicatedByRangeID bool
	// ClearReplicatedBySpan causes the state machine data (i.e. the replicated state
	// for the given RSpan) that is key-addressable (i.e. range descriptor, user keys,
	// locks) to be removed. No data is removed if this is the zero span.
	ClearReplicatedBySpan roachpb.RSpan

	// If MustUseClearRange is true, a Pebble range tombstone will always be used
	// to clear the key spans (unless empty). This is typically used when we need
	// to write additional keys to an SST after this clear, e.g. a replica
	// tombstone, since keys must be written in order. When this is false, a
	// heuristic will be used instead.
	MustUseClearRange bool
}

// ClearRangeData clears the data associated with a range descriptor selected
// by the provided options.
//
// TODO(tbg): could rename this to XReplica. The use of "Range" in both the
// "CRDB Range" and "storage.ClearRange" context in the setting of this method could
// be confusing.
func ClearRangeData(
	ctx context.Context,
	rangeID roachpb.RangeID,
	reader storage.Reader,
	writer storage.Writer,
	opts ClearRangeDataOptions,
) error {
	keySpans := rditer.Select(rangeID, rditer.SelectOpts{
		Ranged: rditer.SelectRangedOptions{
			RSpan:      opts.ClearReplicatedBySpan,
			SystemKeys: true,
			LockTable:  true,
			UserKeys:   true,
		},
		ReplicatedByRangeID:   opts.ClearReplicatedByRangeID,
		UnreplicatedByRangeID: opts.ClearUnreplicatedByRangeID,
	})

	pointKeyThreshold := ClearRangeThresholdPointKeys
	if opts.MustUseClearRange {
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

// DestroyReplica destroys all or a part of the Replica's state, installing a
// RangeTombstone in its place. Due to merges, splits, etc, there is a need
// to control which part of the state this method actually gets to remove,
// which is done via the provided options[^1]; the caller is always responsible
// for managing the remaining disk state accordingly.
//
// [^1] e.g., on a merge, the user data moves to the subsuming replica and must
// not be cleared.
func DestroyReplica(
	ctx context.Context,
	rangeID roachpb.RangeID,
	reader storage.Reader,
	writer storage.Writer,
	nextReplicaID roachpb.ReplicaID,
	opts ClearRangeDataOptions,
) error {
	diskReplicaID, err := stateloader.Make(rangeID).LoadRaftReplicaID(ctx, reader)
	if err != nil {
		return err
	}
	if diskReplicaID.ReplicaID >= nextReplicaID {
		return errors.AssertionFailedf("replica r%d/%d must not survive its own tombstone", rangeID, diskReplicaID)
	}
	_ = DestroyReplicaTODO // 2.1 + 2.2 + 3.1
	if err := ClearRangeData(ctx, rangeID, reader, writer, opts); err != nil {
		return err
	}

	// Save a tombstone to ensure that replica IDs never get reused. Assert that
	// the provided tombstone moves the existing one strictly forward. Failure to
	// do so indicates that something is going wrong in the replica lifecycle.
	sl := stateloader.Make(rangeID)
	ts, err := sl.LoadRangeTombstone(ctx, reader)
	if err != nil {
		return err
	} else if ts.NextReplicaID >= nextReplicaID {
		return errors.AssertionFailedf(
			"cannot rewind tombstone from %d to %d", ts.NextReplicaID, nextReplicaID)
	}
	_ = DestroyReplicaTODO // 2.3
	return sl.SetRangeTombstone(ctx, writer, kvserverpb.RangeTombstone{
		NextReplicaID: nextReplicaID, // NB: nextReplicaID > 0
	})
}
