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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

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
