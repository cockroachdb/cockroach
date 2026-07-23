// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	"slices"
	"sort"
)

// MergeSpans sorts the incoming spans and merges overlapping spans. Returns
// true iff all of the spans are distinct. Note that even if it returns true,
// adjacent spans might have been merged (i.e. [a, b) is distinct from [b,c),
// but the two are still merged.
//
// The input spans are not safe for re-use.
func MergeSpans(spans []Span) ([]Span, bool) {
	if len(spans) == 0 {
		return spans, true
	}

	// Sort first on the start key and second on the end key. Note that we're
	// relying on empty EndKey sorting before other EndKeys.
	slices.SortFunc(spans, func(s1, s2 Span) int {
		if c := s1.Key.Compare(s2.Key); c != 0 {
			return c
		}
		return s1.EndKey.Compare(s2.EndKey)
	})

	// We build up the resulting slice of merged spans in place. This is safe
	// because "r" grows by at most 1 element on each iteration, staying abreast
	// or behind the iteration over "spans".
	r := spans[:1]
	distinct := true

	for _, cur := range spans[1:] {
		prev := &r[len(r)-1]
		if len(prev.EndKey) == 0 { // prev is a point key
			if !cur.Key.Equal(prev.Key) { // cur.Key > prev.Key
				r = append(r, cur) // [a] + [b,any) = [a], [b,any)
			} else {
				distinct = false         // [a] + [a,any) = [a,any)
				prev.EndKey = cur.EndKey // cur.EndKey >= prev.EndKey
			}
		} else if c := cur.Key.Compare(prev.EndKey); c > 0 {
			r = append(r, cur) // // [a,b) + [c,any) = [a,b), [c,any)
		} else if c == 0 {
			if len(cur.EndKey) == 0 { // cur is a point key
				prev.EndKey = cur.Key.Next() // [a,b) + [b] = [a,b.Next())
			} else if cur.EndKey.Compare(prev.EndKey) > 0 {
				prev.EndKey = cur.EndKey // [a,b) + [b,c) = [a,c)
			}
		} else {
			distinct = false // cur.Key is contained in prev
			if len(cur.EndKey) != 0 && cur.EndKey.Compare(prev.EndKey) > 0 {
				prev.EndKey = cur.EndKey // [a,c) + [b,d) = [a,d)
			}
		}
	}
	return r, distinct
}

// SubtractSpans subtracts the subspans covered by a set of non-overlapping
// spans from another set of non-overlapping spans.
//
// Specifically, it returns a non-overlapping set of spans that cover the spans
// covered by the minuend but not the subtrahend. For example, given a single
// minuend span [0, 10) and a subspan subtrahend [4,5) yields: [0, 4), [5, 10).
//
// The todo input is mutated during execution and is not safe for reuse after.
// The done input is left as-is and is safe for later reuse.
//
// Internally the minuend and subtrahend are labeled as "todo" and "done", i.e.
// conceptually it discusses them as a set of spans "to do" with a subset that
// has been "done" and need to be removed from the set "todo".
func SubtractSpans(todo, done Spans) Spans {
	if len(done) == 0 {
		return todo
	}
	sort.Sort(todo)
	sort.Sort(done)

	remaining := make(Spans, 0, len(todo))
	appendRemaining := func(s Span) {
		if len(remaining) > 0 && remaining[len(remaining)-1].EndKey.Equal(s.Key) {
			remaining[len(remaining)-1].EndKey = s.EndKey
		} else {
			remaining = append(remaining, s)
		}
	}

	var d int
	var t int
	for t < len(todo) && d < len(done) {
		tStart, tEnd := todo[t].Key, todo[t].EndKey
		dStart, dEnd := done[d].Key, done[d].EndKey
		if tStart.Equal(tEnd) {
			// We've shrunk the todo span to nothing: pop it off and move on.
			t++
			continue
		}
		if dStart.Compare(tEnd) >= 0 {
			// Done span starts after todo span: todo is kept in its entirety.
			appendRemaining(todo[t])
			t++
			continue
		}
		if dEnd.Compare(tStart) <= 0 {
			// Done span isn't in todo at all, so pop it off and move on.
			d++
			continue
		}

		// At this point, we know that the two spans overlap.
		endCmp := dEnd.Compare(tEnd)
		if dStart.Compare(tStart) <= 0 {
			// The done span starts at or before the todo span starts.
			if endCmp < 0 {
				// Covers strict prefix of todo: pop done and shrink remaining todo.
				todo[t].Key = dEnd
				d++
			} else if endCmp > 0 {
				// Covers all of todo and more: pop todo, keep consuming done.
				t++
			} else {
				// cmp == 0 means exactly matches: pop both.
				t++
				d++
			}
		} else {
			// The beginning of todo is uncovered: split it to remaining.
			appendRemaining(Span{Key: tStart, EndKey: dStart})

			if endCmp < 0 {
				// There is todo uncovered after done: Pop done, shrink and keep todo.
				todo[t].Key = dEnd
				d++
			} else if endCmp > 0 {
				// Done covers beyond todo: pop todo, keep consuming done.
				t++
			} else {
				// cmp == 0: covers to end, uncovered prefix already copied: pop both.
				t++
				d++
			}
		}
	}
	// Just append anything that's left.
	if t < len(todo) {
		remaining = append(remaining, todo[t:]...)
	}
	return remaining
}
