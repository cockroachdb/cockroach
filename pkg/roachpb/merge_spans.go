// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

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

// MergeSpans sorts the incoming spans and merges overlapping spans. Returns
// true iff all of the spans are distinct. Note that even if it returns true,
// adjacent spans might have been merged (i.e. [a, b) is distinct from [b,c),
// but the two are still merged.
//
// The input spans are not safe for re-use.
func MergeSpans(spans *[]Span) ([]Span, bool) {
	if len(*spans) == 0 {
		return *spans, true
	}

	sort.Sort((*sortedSpans)(spans))

	// We build up the resulting slice of merged spans in place. This is safe
	// because "r" grows by at most 1 element on each iteration, staying abreast
	// or behind the iteration over "spans".
	r := (*spans)[:1]
	distinct := true

	for _, cur := range (*spans)[1:] {
		prev := &r[len(r)-1]
		if len(cur.EndKey) == 0 && len(prev.EndKey) == 0 {
			if cur.Key.Compare(prev.Key) != 0 {
				// [a, nil] merge [b, nil]
				r = append(r, cur)
			} else {
				// [a, nil] merge [a, nil]
				distinct = false
			}
			continue
		}
		if len(prev.EndKey) == 0 {
			if cur.Key.Compare(prev.Key) == 0 {
				// [a, nil] merge [a, b]
				prev.EndKey = cur.EndKey
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
					prev.EndKey = cur.EndKey
					if c > 0 {
						distinct = false
					}
				} else {
					// [a, c] merge [b, c]
					distinct = false
				}
			} else if c == 0 {
				// [a, b] merge [b, nil]
				prev.EndKey = cur.Key.Next()
			} else {
				// [a, c] merge [b, nil]
				distinct = false
			}
			continue
		}
		r = append(r, cur)
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
