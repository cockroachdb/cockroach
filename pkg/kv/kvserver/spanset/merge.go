// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanset

import "slices"

// mergeSpans sorts the given spans and merges ones with overlapping spans and
// equal access timestamps. The implementation is a modified roachpb.MergeSpans.
//
// The input spans are not safe for re-use.
func mergeSpans(latches []Span) []Span {
	if len(latches) == 0 {
		return latches
	}

	// Sort first on the start key and second on the end key. Note that we're
	// relying on empty EndKey sorting before other EndKeys.
	slices.SortFunc(latches, func(s1, s2 Span) int {
		if c := s1.Key.Compare(s2.Key); c != 0 {
			return c
		}
		return s1.EndKey.Compare(s2.EndKey)
	})

	// We build up the resulting slice of merged spans in place. This is safe
	// because "r" grows by at most 1 element on each iteration, staying abreast
	// or behind the iteration over "latches".
	r := latches[:1]

	for _, cur := range latches[1:] {
		prev := &r[len(r)-1]
		// Can only merge spans at the same timestamp.
		if cur.Timestamp != prev.Timestamp {
			r = append(r, cur)
		} else if len(prev.EndKey) == 0 { // prev is a point key
			if !cur.Key.Equal(prev.Key) { // cur.Key > prev.Key
				r = append(r, cur) // [a] does not overlap [b,any)
			} else { // [a] overlaps [a,any)
				prev.EndKey = cur.EndKey // cur.EndKey >= prev.EndKey
			}
		} else if c := cur.Key.Compare(prev.EndKey); c > 0 {
			r = append(r, cur) // [a,b) does not overlap [c,any)
		} else if c == 0 {
			if len(cur.EndKey) == 0 { // cur is a point key
				prev.EndKey = cur.Key.Next() // [a,b) + [b] = [a,b]
			} else if cur.EndKey.Compare(prev.EndKey) > 0 {
				prev.EndKey = cur.EndKey // [a,b) + [b,c) = [a,c)
			}
		} else if len(cur.EndKey) != 0 && cur.EndKey.Compare(prev.EndKey) > 0 {
			prev.EndKey = cur.EndKey
		}
	}
	return r
}
