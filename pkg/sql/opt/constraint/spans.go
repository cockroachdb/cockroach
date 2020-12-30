// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package constraint

import (
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
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

// Alloc allocates enough space to support the given amount of spans without
// reallocation. Does nothing if the structure already contains spans.
func (s *Spans) Alloc(capacity int) {
	// We don't preallocate if the capacity is only 2: pre-allocating the slice to
	// size 1 is no better than allocating it on the first Append, but it's worse
	// if we end up not needing it.
	if capacity > 2 && s.numSpans == 0 {
		s.otherSpans = make([]Span, 0, capacity-1)
	}
}

// InitSingleSpan initializes the structure with a single span.
func (s *Spans) InitSingleSpan(sp *Span) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*s = Spans{
		firstSpan: *sp,
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
		panic(errors.AssertionFailedf("mutation disallowed"))
	}
	if s.numSpans == 0 {
		s.firstSpan = *sp
	} else {
		s.otherSpans = append(s.otherSpans, *sp)
	}
	s.numSpans++
}

// Truncate removes all but the first newLength spans.
func (s *Spans) Truncate(newLength int) {
	if s.immutable {
		panic(errors.AssertionFailedf("mutation disallowed"))
	}
	if int32(newLength) > s.numSpans {
		panic(errors.AssertionFailedf("can't truncate to longer length"))
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
}

// sortedAndMerged returns true if the collection of spans is strictly
// ordered and no spans overlap.
func (s *Spans) sortedAndMerged(keyCtx *KeyContext) bool {
	for i := 1; i < s.Count(); i++ {
		if !s.Get(i).StartsStrictlyAfter(keyCtx, s.Get(i-1)) {
			return false
		}
	}
	return true
}

// SortAndMerge sorts the spans and merges any overlapping spans.
func (s *Spans) SortAndMerge(keyCtx *KeyContext) {
	if s.sortedAndMerged(keyCtx) {
		return
	}
	sort.Sort(&spanSorter{keyCtx: *keyCtx, spans: s})

	// Merge overlapping spans. We maintain the last span and extend it with
	// whatever spans it overlaps with.
	n := 0
	currentSpan := *s.Get(0)
	for i := 1; i < s.Count(); i++ {
		sp := s.Get(i)
		if sp.StartsStrictlyAfter(keyCtx, &currentSpan) {
			// No overlap. "Output" the current span.
			*s.Get(n) = currentSpan
			n++
			currentSpan = *sp
		} else {
			// There is overlap; extend the current span to the right if necessary.
			if currentSpan.CompareEnds(keyCtx, sp) < 0 {
				currentSpan.end = sp.end
				currentSpan.endBoundary = sp.endBoundary
			}
		}
	}
	*s.Get(n) = currentSpan
	s.Truncate(n + 1)
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
	return ss.spans.Get(i).Compare(&ss.keyCtx, ss.spans.Get(j)) < 0
}

// Swap is part of sort.Interface.
func (ss *spanSorter) Swap(i, j int) {
	si := ss.spans.Get(i)
	sj := ss.spans.Get(j)
	*si, *sj = *sj, *si
}
