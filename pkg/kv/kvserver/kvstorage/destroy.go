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
const ClearRangeThresholdPointKeys = 64

// DestroyRangeIDOrdered is only used in the subsume path where all the keys
// must be ordered.
func DestroyRangeIDOrdered(
	ctx context.Context,
	r storage.Reader,
	w storage.Writer,
	rangeID roachpb.RangeID,
	nextReplicaID roachpb.ReplicaID,
) error {
	return destroyRangeIDImpl(ctx, r, w, rangeID, nextReplicaID, 1)
}

func DestroyRangeID(
	ctx context.Context,
	r storage.Reader,
	w storage.Writer,
	rangeID roachpb.RangeID,
	nextReplicaID roachpb.ReplicaID,
) error {
	return destroyRangeIDImpl(ctx, r, w, rangeID, nextReplicaID, ClearRangeThresholdPointKeys)
}

func destroyRangeIDImpl(
	ctx context.Context,
	r storage.Reader,
	w storage.Writer,
	rangeID roachpb.RangeID,
	nextReplicaID roachpb.ReplicaID,
	clearRangeThresholdPointKeys int,
) error {
	diskReplicaID, err := stateloader.Make(rangeID).LoadRaftReplicaID(ctx, r)
	if err != nil {
		return err
	}
	if diskReplicaID.ReplicaID >= nextReplicaID {
		return errors.AssertionFailedf("replica r%d/%d must not survive its own tombstone", rangeID, diskReplicaID)
	}

	rs := rditer.RangeIDReplicated(rangeID)
	if err := storage.ClearRangeWithHeuristic(
		ctx, r, w, rs.Key, rs.EndKey, clearRangeThresholdPointKeys,
	); err != nil {
		return err
	}

	// Save a tombstone to ensure that replica IDs never get reused. Assert that
	// the provided tombstone moves the existing one strictly forward. Failure to
	// do so indicates that something is going wrong in the replica lifecycle.
	sl := stateloader.Make(rangeID)
	ts, err := sl.LoadRangeTombstone(ctx, r)
	if err != nil {
		return err
	} else if ts.NextReplicaID >= nextReplicaID {
		return errors.AssertionFailedf(
			"cannot rewind tombstone from %d to %d", ts.NextReplicaID, nextReplicaID)
	}
	return sl.SetRangeTombstone(ctx, w, kvserverpb.RangeTombstone{
		NextReplicaID: nextReplicaID, // NB: nextReplicaID > 0
	})
}

func DestroyRaft(
	ctx context.Context, r storage.Reader, w storage.Writer, rangeID roachpb.RangeID,
) error {
	// TODO: touch only raft keys, and destroy other unreplicated keys (that are
	// part of the state machine) in DestroyRangeID.
	rs := rditer.RangeIDUnreplicated(rangeID)
	return storage.ClearRangeWithHeuristic(
		ctx, r, w, rs.Key, rs.EndKey, ClearRangeThresholdPointKeys,
	)
}

func ClearRSpan(ctx context.Context, r storage.Reader, w storage.Writer, rs roachpb.RSpan) error {
	for s := range rditer.RSpan(rs) {
		if err := storage.ClearRangeWithHeuristic(
			ctx, r, w, s.Key, s.EndKey, ClearRangeThresholdPointKeys,
		); err != nil {
			return err
		}
	}
	return nil
}
