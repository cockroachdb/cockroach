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
	s.firstSpan = *sp
	s.otherSpans = nil
	s.numSpans = 1
	s.immutable = false
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

// KeyCount returns the number of distinct keys contained in the spans. See
// span.KeyCount for conditions under which this is possible. Example:
//
//    KeyCount('[/'ASIA' - /'ASIA'] [/'EUROPE'/1 - /'EUROPE'/4]') == 5
//
func (s *Spans) KeyCount(keyCtx *KeyContext) (int64, bool) {
	keyCount := int64(0)
	for i, cnt := 0, s.Count(); i < cnt; i++ {
		cnt, ok := s.Get(i).KeyCount(keyCtx)
		if !ok {
			return 0, false
		}
		keyCount += cnt
		if keyCount < 0 {
			// Overflow.
			return 0, false
		}
	}
	return keyCount, true
}

// ExtractSingleKeySpans returns a new Spans struct containing a span for each
// key in the given spans. Returns nil and false if one of the following is
// true:
//   1. The number of new spans exceeds the given limit value.
//   2. The spans don't all have the same key length (length as in
//      length(/3/'two'/1) == 3).
//   3. span.Split is unsuccessful for any of the spans.
//
// Example:
//
//    ExtractSingleKeySpans('[/'ASIA'/0 - /'ASIA'/0] [/'US'/1 - /'US'/4]')
//    =>
//    '[/'ASIA'/0 - /'ASIA'/0] [/'US'/1 - /'US'/1] [/'US'/2 - /'US'/2]
//     [/'US'/3 - /'US'/3] [/'US'/4 - /'US'/4]'
//
func (s *Spans) ExtractSingleKeySpans(keyCtx *KeyContext, limit int64) (*Spans, bool) {
	// Ensure that the number of keys in the spans does not exceed the limit.
	keyCount, ok := s.KeyCount(keyCtx)
	if !ok || keyCount > limit {
		return nil, false
	}

	// Ensure that the key lengths are consistent between spans.
	keyLength := -1
	for i, spanCnt := 0, s.Count(); i < spanCnt; i++ {
		span := s.Get(i)
		if keyLength == -1 {
			// Initialize keyLength.
			keyLength = span.StartKey().Length()
		}
		if span.StartKey().Length() != keyLength {
			// The spans don't all have the same key length.
			return nil, false
		}
	}

	// Construct the new single-key spans.
	newSpans := Spans{}
	for i, spanCnt := 0, s.Count(); i < spanCnt; i++ {
		splitSpans, ok := s.Get(i).Split(keyCtx, limit-int64(newSpans.Count()))
		if !ok {
			// The span could not be split into single key spans.
			return nil, false
		}
		for i, cnt := 0, splitSpans.Count(); i < cnt; i++ {
			newSpans.Append(splitSpans.Get(i))
		}
	}
	return &newSpans, true
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
