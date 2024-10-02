// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// rangeTombstonePeekBounds returns the left and right bounds that
// MVCCDeleteRangeUsingTombstone can read in order to detect adjacent range
// tombstones to merge with or fragment. The bounds will be truncated to the
// Raft range bounds if given.
func rangeTombstonePeekBounds(
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
