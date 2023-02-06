// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstorage

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
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

	// ClearRangeThresholdRangeKeys is the threshold (as number of range keys)
	// beyond which we'll clear range data using a single RANGEKEYDEL across the
	// span rather than clearing individual range keys.
	ClearRangeThresholdRangeKeys = 8
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
	rangeID roachpb.RangeID, reader storage.Reader, writer storage.Writer, opts ClearRangeDataOptions,
) error {
	keySpans := rditer.Select(rangeID, rditer.SelectOpts{
		ReplicatedBySpan:      opts.ClearReplicatedBySpan,
		ReplicatedByRangeID:   opts.ClearReplicatedByRangeID,
		UnreplicatedByRangeID: opts.ClearUnreplicatedByRangeID,
	})

	pointKeyThreshold, rangeKeyThreshold := ClearRangeThresholdPointKeys, ClearRangeThresholdRangeKeys
	if opts.MustUseClearRange {
		pointKeyThreshold, rangeKeyThreshold = 1, 1
	}

	for _, keySpan := range keySpans {
		if err := storage.ClearRangeWithHeuristic(
			reader, writer, keySpan.Key, keySpan.EndKey, pointKeyThreshold, rangeKeyThreshold,
		); err != nil {
			return err
		}
	}
	return nil
}
