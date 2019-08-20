// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanlatch

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type sortedSpanLatches []spanLatch

func (s sortedSpanLatches) Less(i, j int) bool {
	// Sort first on the start key and second on the end key. Note that we're
	// relying on EndKey = nil (and len(EndKey) == 0) sorting before other
	// EndKeys.
	c := s[i].span.Key.Compare(s[j].span.Key)
	if c != 0 {
		return c < 0
	}
	return s[i].span.EndKey.Compare(s[j].span.EndKey) < 0
}

func (s sortedSpanLatches) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortedSpanLatches) Len() int {
	return len(s)
}

// mergeSpanLatches sorts the given span latches and merges ones with overlapping
// spans. The implementation is a copy of roachpb.MergeSpans.
//
// Returns true iff all of the spans are distinct.
// The input spans are not safe for re-use.
func mergeSpanLatches(latches []spanLatch) ([]spanLatch, bool) {
	if len(latches) == 0 {
		return latches, true
	}

	sort.Sort(sortedSpanLatches(latches))

	// We build up the resulting slice of merged spans in place. This is safe
	// because "r" grows by at most 1 element on each iteration, staying abreast
	// or behind the iteration over "latches".
	r := latches[:1]
	distinct := true

	for _, cur := range latches[1:] {
		prev := &r[len(r)-1]
		if len(cur.span.EndKey) == 0 && len(prev.span.EndKey) == 0 {
			if cur.span.Key.Compare(prev.span.Key) != 0 {
				// [a, nil] merge [b, nil]
				r = append(r, cur)
			} else {
				// [a, nil] merge [a, nil]
				check(prev.ts, cur.ts)
				distinct = false
			}
			continue
		}
		if len(prev.span.EndKey) == 0 { // len(cur.span.EndKey) != 0
			if cur.span.Key.Compare(prev.span.Key) == 0 {
				// [a, nil] merge [a, b]
				check(prev.ts, cur.ts)
				prev.span.EndKey = cur.span.EndKey
				distinct = false
			} else {
				// [a, nil] merge [b, c]
				r = append(r, cur)
			}
			continue
		}

		if c := prev.span.EndKey.Compare(cur.span.Key); c >= 0 {
			if cur.span.EndKey != nil {
				if prev.span.EndKey.Compare(cur.span.EndKey) < 0 {
					// [a, c] merge [b, d]
					check(prev.ts, cur.ts)
					prev.span.EndKey = cur.span.EndKey
					if c > 0 {
						distinct = false
					}
				} else {
					// [a, c] merge [b, c]
					check(prev.ts, cur.ts)
					distinct = false
				}
			} else if c == 0 {
				// [a, b] merge [b, nil]
				check(prev.ts, cur.ts)
				prev.span.EndKey = cur.span.Key.Next()
			} else {
				// [a, c] merge [b, nil]
				check(prev.ts, cur.ts)
				distinct = false
			}
			continue
		}
		r = append(r, cur)
	}
	return r, distinct
}

func check(t1, t2 hlc.Timestamp) {
	if t1 != t2 {
		panic("differing timestamps for mergeable spans unexpected")
	}
}
