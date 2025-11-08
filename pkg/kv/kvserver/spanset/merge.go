// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanset

import "slices"

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

	// Sort first on the start key and second on the end key. Note that we're
	// relying on empty EndKey sorting before other EndKeys.
	slices.SortFunc(*latches, func(s1, s2 Span) int {
		if c := s1.Key.Compare(s2.Key); c != 0 {
			return c
		}
		return s1.EndKey.Compare(s2.EndKey)
	})

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
