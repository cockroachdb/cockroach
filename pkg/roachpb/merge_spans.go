// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package roachpb

import "sort"

type sortedSpans []Span

func (s sortedSpans) Less(i, j int) bool {
	// Sort first on the start key and second on the end key. Note that we're
	// relying on EndKey = nil (and len(EndKey) == 0) sorting before other
	// EndKeys.
	c := s[i].Key.Compare(s[j].Key)
	if c != 0 {
		return c < 0
	}
	return s[i].EndKey.Compare(s[j].EndKey) < 0
}

func (s sortedSpans) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortedSpans) Len() int {
	return len(s)
}

// MergeSpans sorts the incoming spans and merges overlapping spans. Returns
// true iff all of the spans are distinct.
// The input spans are not safe for re-use.
func MergeSpans(spans []Span) ([]Span, bool) {
	if len(spans) == 0 {
		return spans, true
	}

	sort.Sort(sortedSpans(spans))

	// We build up the resulting slice of merged spans in place. This is safe
	// because "r" grows by at most 1 element on each iteration, staying abreast
	// or behind the iteration over "spans".
	r := spans[:1]
	distinct := true

	for _, cur := range spans[1:] {
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

// SubtractSpans subtracts one partial covering from another.
//
// Specifically, it returns a non-overlapping set of spans that cover the spans
// covered by the minuend but not the subtrahend. For example, given a single
// minuend span [0, 10) and a subspan subtrahend [4,5) yields: [0, 4), [5, 10).
//
// Both inputs are mutated during execution and are not safe for reuse after.
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

	var d int
	var t int
	for t < len(todo) && d < len(done) {
		// if we've shrunk the todo span to nothing, skip it.
		if todo[t].Key.Equal(todo[t].EndKey) {
			t++
			continue
		}
		// Done span is after todo span so it doesn't affect it.
		if todo[t].EndKey.Compare(done[d].Key) <= 0 {
			if len(remaining) > 0 && todo[t].Key.Equal(remaining[len(remaining)-1].EndKey) {
				remaining[len(remaining)-1].EndKey = todo[t].EndKey
			} else {
				remaining = append(remaining, todo[t])
			}
			t++
			continue
		}
		// we can't subtract a span that isn't in todo.
		if done[d].EndKey.Compare(todo[t].Key) <= 0 {
			d++
			continue
		}

		keyCmp := done[d].Key.Compare(todo[t].Key)
		endCmd := done[d].EndKey.Compare(todo[t].EndKey)

		// We can't subtract something that isn't in todo, so discard up to todo.
		if keyCmp < 0 {
			done[d].Key = todo[t].Key
			keyCmp = 0
		}

		if keyCmp == 0 {
			if endCmd < 0 {
				// Matches a strict prefix of an input span: shrink todo.
				todo[t].Key = done[d].EndKey
				d++
			} else if endCmd == 0 {
				// Exactly matches input span: skip it and done with both.
				t++
				d++
			} else if endCmd > 0 {
				// Matches all of todo and more: remove todo, update done to remainder.
				done[d].Key = todo[t].EndKey
				t++
			} else {
				panic("unreachable: does not end before, at or after")
			}
		} else if keyCmp > 0 {
			// The beginning of todo is uncovered: split it to remaining.
			before := Span{Key: todo[t].Key, EndKey: done[d].Key}

			if len(remaining) > 0 && before.Key.Equal(remaining[len(remaining)-1].EndKey) {
				remaining[len(remaining)-1].EndKey = before.EndKey
			} else {
				remaining = append(remaining, before)
			}

			if endCmd < 0 {
				// There is more uncovered after done: leave it in todo.
				todo[t].Key = done[d].EndKey
				d++
			} else if endCmd == 0 {
				// The uncovered prefix was already copied: done with both.
				t++
				d++
			} else if endCmd > 0 {
				// Done covers beyond todo: done with todo, shrink done.
				done[d].Key = todo[t].EndKey
				t++
			} else {
				panic("unreachable: does not end before, at or after")
			}
		} else {
			panic("unreachable: does not start before, at or after")
		}
	}

	// Just append anything that's left, but first see if it should merge with the
	// last added remainder span.
	if t < len(todo) && len(remaining) > 1 && todo[t].Key.Equal(remaining[len(remaining)-1].EndKey) {
		remaining[len(remaining)-1].EndKey = todo[t].EndKey
		t++
	}

	// Just append anything that's left.
	if t < len(todo) {
		remaining = append(remaining, todo[t:]...)
	}
	return remaining
}
