// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// MakeLookupBoundariesForGCRanges creates spans that could be used for latches
// and iterators when performing range tombstone garbage collection. Each of
// spans includes additional keys to the left and right of the GD'd range to
// ensure merging of range tombstones could be performed and at the same time
// no data is accessed outside of latches.
func MakeLookupBoundariesForGCRanges(
	rangeStart, rangeEnd roachpb.Key, rangeKeys []roachpb.GCRequest_GCRangeKey,
) []roachpb.Span {
	spans := make([]roachpb.Span, len(rangeKeys))
	for i := range rangeKeys {
		l, r := RangeTombstonePeekBounds(rangeKeys[i].StartKey, rangeKeys[i].EndKey, rangeStart, rangeEnd)
		spans[i] = roachpb.Span{
			Key:    l,
			EndKey: r,
		}
	}
	return spans
}

// RangeTombstonePeekBounds returns the left and right bounds that
// ExperimentalMVCCDeleteRangeUsingTombstone can read in order to detect
// adjacent range tombstones to merge with or fragment. The bounds will be
// truncated to the Raft range bounds if given.
func RangeTombstonePeekBounds(
	startKey, endKey, rangeStart, rangeEnd roachpb.Key,
) (roachpb.Key, roachpb.Key) {
	leftPeekBound := startKey.Prevish(roachpb.PrevishKeyLength)
	if len(rangeStart) > 0 && leftPeekBound.Compare(rangeStart) <= 0 {
		leftPeekBound = rangeStart
	}

	rightPeekBound := endKey.Next()
	if len(rangeEnd) > 0 && rightPeekBound.Compare(rangeEnd) >= 0 {
		rightPeekBound = rangeEnd
	}

	return leftPeekBound.Clone(), rightPeekBound.Clone()
}

// MakeCollectableGCRangesFromGCRequests creates GC collectable ranges
// containing ranges to be removed as well as safe iteration boundaries.
// See MakeLookupBoundariesForGCRanges for why additional boundaries are used.
func MakeCollectableGCRangesFromGCRequests(
	rangeStart, rangeEnd roachpb.Key, rangeKeys []roachpb.GCRequest_GCRangeKey,
) []CollectableGCRangeKey {
	latches := MakeLookupBoundariesForGCRanges(rangeStart, rangeEnd, rangeKeys)
	collectableKeys := make([]CollectableGCRangeKey, len(rangeKeys))
	for i, rk := range rangeKeys {
		collectableKeys[i] = CollectableGCRangeKey{
			MVCCRangeKey: MVCCRangeKey{
				StartKey:  rk.StartKey,
				EndKey:    rk.EndKey,
				Timestamp: rk.Timestamp,
			},
			LatchSpan: latches[i],
		}
	}
	return collectableKeys
}
