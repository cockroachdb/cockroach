// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import "github.com/cockroachdb/cockroach/pkg/storage/enginepb"

// AddIgnoredSeqNumRange adds the given range to the given list of
// ignored seqnum ranges.
//
// The following invariants are assumed to hold and are preserved:
// - the list contains no overlapping ranges
// - the list contains no contiguous ranges
// - the list is sorted, with larger seqnums at the end
//
// Additionally, the caller must ensure:
//
// 1) if the new range overlaps with some range in the list, then it
//    also overlaps with every subsequent range in the list.
//
// 2) the new range's "end" seqnum is larger or equal to the "end"
//    seqnum of the last element in the list.
//
// For example:
//     current list [3 5] [10 20] [22 24]
//     new item:    [8 26]
//     final list:  [3 5] [8 26]
//
//     current list [3 5] [10 20] [22 24]
//     new item:    [28 32]
//     final list:  [3 5] [10 20] [22 24] [28 32]
//
// This corresponds to savepoints semantics:
//
// - Property 1 says that a rollback to an earlier savepoint
//   rolls back over all writes following that savepoint.
// - Property 2 comes from that the new range's 'end' seqnum is the
//   current write seqnum and thus larger than or equal to every
//   previously seen value.
func AddIgnoredSeqNumRange(
	list []enginepb.IgnoredSeqNumRange, newRange enginepb.IgnoredSeqNumRange,
) []enginepb.IgnoredSeqNumRange {
	// Truncate the list at the last element not included in the new range.
	i := 0
	for ; i < len(list); i++ {
		if list[i].End < newRange.Start {
			continue
		}
		break
	}
	list = list[:i]
	return append(list, newRange)
}
