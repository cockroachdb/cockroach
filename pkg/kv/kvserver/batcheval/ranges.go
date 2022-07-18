// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
