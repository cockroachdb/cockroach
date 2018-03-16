// Copyright 2018 The Cockroach Authors.
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

package constraint

import (
	"sort"
	"strings"
)

// Spans is a collection of spans. There are no general requirements on the
// contents of the spans in the structure; the caller has to make sure they make
// sense in the respective context.
type Spans struct {
	// firstSpan holds the first span and otherSpans hold any spans beyond the
	// first. These are separated in order to optimize for the common case of a
	// single-span constraint.
	firstSpan  Span
	otherSpans []Span
	numSpans   int32
	immutable  bool
}

// MakeSpans allocates enough space to support the given amount of spans without
// reallocation.
func MakeSpans(capacity int) Spans {
	if capacity <= 1 {
		return Spans{}
	}
	return Spans{otherSpans: make([]Span, 0, capacity-1)}
}

// SingleSpan creates Spans containing a single span.
func SingleSpan(sp *Span) Spans {
	return Spans{
		firstSpan: sp.Copy(),
		numSpans:  1,
	}
}

// Count returns the number of spans.
func (s *Spans) Count() int {
	return int(s.numSpans)
}

// Get returns the nth span.
func (s *Spans) Get(nth int) *Span {
	if nth == 0 && s.numSpans > 0 {
		return &s.firstSpan
	}
	return &s.otherSpans[nth-1]
}

// Append adds another span (at the end).
func (s *Spans) Append(sp *Span) {
	if s.immutable {
		panic("mutation disallowed")
	}
	if s.numSpans == 0 {
		s.firstSpan = sp.Copy()
	} else {
		s.otherSpans = append(s.otherSpans, sp.Copy())
	}
	s.numSpans++
}

// Truncate removes all but the first newLength spans.
func (s *Spans) Truncate(newLength int) {
	if s.immutable {
		panic("mutation disallowed")
	}
	if int32(newLength) > s.numSpans {
		panic("can't truncate to longer length")
	}
	if newLength == 0 {
		s.otherSpans = s.otherSpans[:0]
	} else {
		s.otherSpans = s.otherSpans[:newLength-1]
	}
	s.numSpans = int32(newLength)
}

func (s Spans) String() string {
	var b strings.Builder
	for i := 0; i < s.Count(); i++ {
		if i > 0 {
			b.WriteRune(' ')
		}
		b.WriteString(s.Get(i).String())
	}
	return b.String()
}

// makeImmutable causes panics in any future calls to methods that mutate either
// the Spans structure or any Span returned by Get.
func (s *Spans) makeImmutable() {
	s.immutable = true
	if s.numSpans > 0 {
		s.firstSpan.makeImmutable()
		for i := range s.otherSpans {
			s.otherSpans[i].makeImmutable()
		}
	}
}

// isSortedAndUnique returns true if the collection of spans is strictly ordered
// (see Span.Compare).
func (s *Spans) isSortedAndUnique(keyCtx KeyContext) bool {
	for i := 1; i < s.Count(); i++ {
		if s.Get(i-1).Compare(keyCtx, s.Get(i)) >= 0 {
			return false
		}
	}
	return true
}

// SortAndDedup sorts the spans (according to Span.Compare) and removes any
// duplicates.
func (s *Spans) SortAndDedup(keyCtx KeyContext) {
	// In many cases (e.g spans generated from normalized tuples), the spans are
	// already ordered. Check for that as a fast path.
	if s.isSortedAndUnique(keyCtx) {
		return
	}
	sort.Sort(&spanSorter{keyCtx: keyCtx, spans: s})
	n := 1
	for i := 1; i < s.Count(); i++ {
		curr := s.Get(i)
		if curr.Compare(keyCtx, s.Get(n-1)) != 0 {
			if n != i {
				*s.Get(n) = *curr
			}
			n++
		}
	}
	s.Truncate(n)
}

type spanSorter struct {
	keyCtx KeyContext
	spans  *Spans
}

var _ sort.Interface = &spanSorter{}

// Len is part of sort.Interface.
func (ss *spanSorter) Len() int {
	return ss.spans.Count()
}

// Less is part of sort.Interface.
func (ss *spanSorter) Less(i, j int) bool {
	return ss.spans.Get(i).Compare(ss.keyCtx, ss.spans.Get(j)) < 0
}

// Swap is part of sort.Interface.
func (ss *spanSorter) Swap(i, j int) {
	si := ss.spans.Get(i)
	sj := ss.spans.Get(j)
	*si, *sj = *sj, *si
}
