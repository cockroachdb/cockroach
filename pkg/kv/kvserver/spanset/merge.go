// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanset

import "slices"

// mergeSpans sorts the given spans and merges ones with overlapping spans and
// equal access timestamps. The implementation is a modified roachpb.MergeSpans.
//
// The input and output spans represent the same subset of {keys}x{timestamps}
// space. The resulting spans are ordered by key. It is minimal (impossible to
// "merge" further without losing precision) only if there are no keys present
// under multiple timestamps.
//
// When there are keys present under multiple timestamps, the resulting merged
// set is not guaranteed to be minimal. For example, [{a-b}@10, b@20, {b-c}@10]
// is returned as is, even though we could merge it as [{a-c}@10, b@20]. This is
// due to an arbitrary choice of sorting it by key.
//
// TODO(pav-kv): we could sort by (timestamp, key), which makes it possible to
// merge spans with the same timestamp and guarantee minimality. After which, we
// can optionally sort by key again. This is still more expensive than one sort,
// so consider if the users are fine with the set sorted by (timestamp, key).
//
// The priority for this function is to be simple and free from assumptions
// about how to "resolve" the situation of having the same key under multiple
// timestamps. E.g. it does not assume that higher/lower timestamp "wins", or
// what the semantics of an empty timestamp are. Considerations like that are
// left to the consumer of this span set, e.g. the latch manager.
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
				r = append(r, cur) // [a] + [b,any) = [a], [b,any)
			} else { // [a] + [a,any) = [a,any)
				prev.EndKey = cur.EndKey // cur.EndKey >= prev.EndKey
			}
		} else if c := cur.Key.Compare(prev.EndKey); c > 0 {
			r = append(r, cur) // [a,b) + [c,any) = [a,b), [c,any)
		} else if c == 0 {
			if len(cur.EndKey) == 0 { // cur is a point key
				prev.EndKey = cur.Key.Next() // [a,b) + [b] = [a,b.Next())
			} else if cur.EndKey.Compare(prev.EndKey) > 0 {
				prev.EndKey = cur.EndKey // [a,b) + [b,c) = [a,c)
			}
		} else if len(cur.EndKey) != 0 && cur.EndKey.Compare(prev.EndKey) > 0 {
			prev.EndKey = cur.EndKey // [a,c) + [b,d) = [a,d)
		}
	}
	return r
}
