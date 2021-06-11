// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanset

import "sort"

type sortedSpans []Span

func (s *sortedSpans) Less(i, j int) bool {
	// Sort first on the start key and second on the end key. Note that we're
	// relying on EndKey = nil (and len(EndKey) == 0) sorting before other
	// EndKeys.
	c := (*s)[i].Key.Compare((*s)[j].Key)
	if c != 0 {
		return c < 0
	}
	return (*s)[i].EndKey.Compare((*s)[j].EndKey) < 0
}

func (s *sortedSpans) Swap(i, j int) {
	(*s)[i], (*s)[j] = (*s)[j], (*s)[i]
}

func (s *sortedSpans) Len() int {
	return len(*s)
}

// mergeSpans sorts the given spans and merges ones with overlapping
// spans and equal access timestamps. The implementation is a copy of
// roachpb.MergeSpans.
//
// Returns true iff all of the spans are distinct.
// The input spans are not safe for re-use.
func mergeSpans(latches *[]Span) ([]Span, bool) {
	if len(*latches) == 0 {
		return *latches, true
	}

	sort.Sort((*sortedSpans)(latches))

	// We build up the resulting slice of merged spans in place. This is safe
	// because "r" grows by at most 1 element on each iteration, staying abreast
	// or behind the iteration over "latches".
	r := (*latches)[:1]
	distinct := true

	for _, cur := range (*latches)[1:] {
		prev := &r[len(r)-1]
		if len(cur.EndKey) == 0 && len(prev.EndKey) == 0 {
			if cur.Key.Compare(prev.Key) != 0 {
				// [a, nil] merge [b, nil]
				r = append(r, cur)
			} else {
				// [a, nil] merge [a, nil]
				if cur.Timestamp != prev.Timestamp {
					r = append(r, cur)
				}
				distinct = false
			}
			continue
		}
		if len(prev.EndKey) == 0 {
			if cur.Key.Compare(prev.Key) == 0 {
				// [a, nil] merge [a, b]
				if cur.Timestamp != prev.Timestamp {
					r = append(r, cur)
				} else {
					prev.EndKey = cur.EndKey
				}
				distinct = false
			} else {
				// [a, nil] merge [b, c]
				r = append(r, cur)
			}
			continue
		}

		if c := prev.EndKey.Compare(cur.Key); c >= 0 {
			if cur.EndKey != nil {
				if prev.EndKey.Compare(cur.EndKey) < 0 {
					// [a, c] merge [b, d]
					if cur.Timestamp != prev.Timestamp {
						r = append(r, cur)
					} else {
						prev.EndKey = cur.EndKey
					}
					if c > 0 {
						distinct = false
					}
				} else {
					// [a, c] merge [b, c]
					if cur.Timestamp != prev.Timestamp {
						r = append(r, cur)
					}
					distinct = false
				}
			} else if c == 0 {
				// [a, b] merge [b, nil]
				if cur.Timestamp != prev.Timestamp {
					r = append(r, cur)
				}
				prev.EndKey = cur.Key.Next()
			} else {
				// [a, c] merge [b, nil]
				if cur.Timestamp != prev.Timestamp {
					r = append(r, cur)
				}
				distinct = false
			}
			continue
		}
		r = append(r, cur)
	}
	return r, distinct
}
