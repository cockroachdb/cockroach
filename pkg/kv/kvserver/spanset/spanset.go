// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanset

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// SpanAccess records the intended mode of access in a SpanSet.
type SpanAccess int

// Constants for SpanAccess. Higher-valued accesses imply lower-level ones.
const (
	SpanReadOnly SpanAccess = iota
	SpanReadWrite
	NumSpanAccess
)

// String returns a string representation of the SpanAccess.
func (a SpanAccess) String() string {
	switch a {
	case SpanReadOnly:
		return "read"
	case SpanReadWrite:
		return "write"
	default:
		panic("unreachable")
	}
}

// SafeValue implements the redact.SafeValue interface.
func (SpanAccess) SafeValue() {}

// SpanScope divides access types into local and global keys.
type SpanScope int

// Constants for span scopes.
const (
	SpanGlobal SpanScope = iota
	SpanLocal
	NumSpanScope
)

// String returns a string representation of the SpanScope.
func (a SpanScope) String() string {
	switch a {
	case SpanGlobal:
		return "global"
	case SpanLocal:
		return "local"
	default:
		panic("unreachable")
	}
}

// SafeValue implements the redact.SafeValue interface.
func (SpanScope) SafeValue() {}

// Span is used to represent a keyspan accessed by a request at a given
// timestamp. A zero timestamp indicates it's a non-MVCC access.
type Span struct {
	roachpb.Span
	Timestamp hlc.Timestamp
}

// TrickySpan represents a span that supports a special encoding where a nil
// start key with a non-nil end key represents the point span:
// [EndKey.Prev(), EndKey).
type TrickySpan roachpb.Span

// SpanSet tracks the set of key spans touched by a command, broken into MVCC
// and non-MVCC accesses. The set is divided into subsets for access type
// (read-only or read/write) and key scope (local or global; used to facilitate
// use by the separate local and global latches).
// The Span slice for a particular access and scope contains non-overlapping
// spans in increasing key order after calls to SortAndDedup.
type SpanSet struct {
	spans [NumSpanAccess][NumSpanScope][]Span
	// forbiddenSpansMatchers are functions that return an error if the given span
	// shouldn't be accessed (forbidden). This allows for complex pattern matching
	// like forbidding specific keys across all range IDs without enumerating them
	// explicitly.
	forbiddenSpansMatchers []func(TrickySpan) error
	allowUndeclared        bool
	allowForbidden         bool
}

var spanSetPool = sync.Pool{
	New: func() interface{} { return new(SpanSet) },
}

// New creates a new empty SpanSet.
func New() *SpanSet {
	return spanSetPool.Get().(*SpanSet)
}

// Release releases the SpanSet and its underlying slices. The receiver should
// not be used after being released.
func (s *SpanSet) Release() {
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			// Recycle slice if capacity below threshold.
			const maxRecycleCap = 8
			var recycle []Span
			if sl := s.spans[sa][ss]; cap(sl) <= maxRecycleCap {
				for i := range sl {
					sl[i] = Span{}
				}
				recycle = sl[:0]
			}
			s.spans[sa][ss] = recycle
		}
	}
	s.forbiddenSpansMatchers = nil
	s.allowForbidden = false
	s.allowUndeclared = false
	spanSetPool.Put(s)
}

// String prints a string representation of the SpanSet.
func (s *SpanSet) String() string {
	var buf strings.Builder
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			for _, cur := range s.GetSpans(sa, ss) {
				fmt.Fprintf(&buf, "%s %s: %s at %s\n",
					sa, ss, cur.Span.String(), cur.Timestamp.String())
			}
		}
	}
	return buf.String()
}

// Len returns the total number of spans tracked across all accesses and scopes.
func (s *SpanSet) Len() int {
	var count int
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			count += len(s.GetSpans(sa, ss))
		}
	}
	return count
}

// Empty returns whether the set contains any spans across all accesses and scopes.
func (s *SpanSet) Empty() bool {
	return s.Len() == 0
}

// Copy copies the SpanSet.
func (s *SpanSet) Copy() *SpanSet {
	n := New()
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			n.spans[sa][ss] = append(n.spans[sa][ss], s.spans[sa][ss]...)
		}
	}
	n.forbiddenSpansMatchers = append(n.forbiddenSpansMatchers, s.forbiddenSpansMatchers...)
	n.allowUndeclared = s.allowUndeclared
	n.allowForbidden = s.allowForbidden
	return n
}

// ShallowCopy performs a shallow SpanSet copy.
func (s *SpanSet) ShallowCopy() *SpanSet {
	n := New()
	*n = *s
	return n
}

// Iterate iterates over a SpanSet, calling the given function.
func (s *SpanSet) Iterate(f func(SpanAccess, SpanScope, Span)) {
	if s == nil {
		return
	}
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			for _, span := range s.spans[sa][ss] {
				f(sa, ss, span)
			}
		}
	}
}

// Reserve space for N additional spans.
func (s *SpanSet) Reserve(access SpanAccess, scope SpanScope, n int) {
	existing := s.spans[access][scope]
	if n <= cap(existing)-len(existing) {
		return
	}
	s.spans[access][scope] = make([]Span, len(existing), n+len(existing))
	copy(s.spans[access][scope], existing)
}

// AddNonMVCC adds a non-MVCC span to the span set. This should typically
// local keys.
func (s *SpanSet) AddNonMVCC(access SpanAccess, span roachpb.Span) {
	s.AddMVCC(access, span, hlc.Timestamp{})
}

// AddMVCC adds an MVCC span to the span set to be accessed at the given
// timestamp. This should typically be used for MVCC keys, user keys for e.g.
func (s *SpanSet) AddMVCC(access SpanAccess, span roachpb.Span, timestamp hlc.Timestamp) {
	scope := SpanGlobal
	if keys.IsLocal(span.Key) {
		scope = SpanLocal
		timestamp = hlc.Timestamp{}
	}

	s.spans[access][scope] = append(s.spans[access][scope], Span{Span: span, Timestamp: timestamp})
}

// AddForbiddenMatcher adds a forbidden span matcher. The matcher is a function
// that is called for each span access to check if it should be forbidden.
func (s *SpanSet) AddForbiddenMatcher(matcher func(TrickySpan) error) {
	s.forbiddenSpansMatchers = append(s.forbiddenSpansMatchers, matcher)
}

// Merge merges all spans in s2 into s. s2 is not modified.
func (s *SpanSet) Merge(s2 *SpanSet) {
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			s.spans[sa][ss] = append(s.spans[sa][ss], s2.spans[sa][ss]...)
		}
	}
	s.forbiddenSpansMatchers = append(s.forbiddenSpansMatchers, s2.forbiddenSpansMatchers...)
	// TODO(ibrahim): Figure out if the merged `allowUndeclared` and
	// `allowForbidden` should be true if it was true in either `s` or `s2`.
	s.allowUndeclared = s2.allowUndeclared
	s.allowForbidden = s2.allowForbidden
	s.SortAndDedup()
}

// SortAndDedup sorts the spans in the SpanSet by key and removes duplicate or
// overlapping / redundant spans. The SpanSet represents the same subset of the
// {keys}x{timestamps} space before and after this call, but is not guaranteed
// to have a minimal number of spans in a general case (see mergeSpans comment).
//
// TODO(pav-kv): does this make CheckAllowed falsely fail in some cases? Maybe
// it's fine: importantly, it should not falsely succeed.
func (s *SpanSet) SortAndDedup() {
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			s.spans[sa][ss] = mergeSpans(s.spans[sa][ss])
		}
	}
}

// GetSpans returns a slice of spans with the given parameters.
func (s *SpanSet) GetSpans(access SpanAccess, scope SpanScope) []Span {
	return s.spans[access][scope]
}

// Intersects returns true iff the span set denoted by `other` has any
// overlapping spans with `s`, and that those spans overlap in access type. Note
// that timestamps associated with the spans in the spanset are not considered,
// only the span boundaries are checked.
func (s *SpanSet) Intersects(other *SpanSet) bool {
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			otherSpans := other.GetSpans(sa, ss)
			for _, span := range otherSpans {
				// If access is allowed, we must have an overlap.
				if err := s.CheckAllowed(sa, TrickySpan(span.Span)); err == nil {
					return true
				}
			}
		}
	}
	return false
}

// AssertAllowed calls CheckAllowed and fatals if the access is not allowed.
// Timestamps associated with the spans in the spanset are not considered,
// only the span boundaries are checked.
func (s *SpanSet) AssertAllowed(access SpanAccess, span TrickySpan) {
	if err := s.CheckAllowed(access, span); err != nil {
		log.KvExec.Fatalf(context.TODO(), "%v", err)
	}
}

// CheckAllowed returns an error if the access is not allowed over the given
// keyspan based on the collection of spans in the spanset. Timestamps
// associated with the spans in the spanset are not considered, only the span
// boundaries are checked.
//
// If the provided span contains only an (exclusive) EndKey and has a nil
// (inclusive) Key then Key is considered to be the key previous to EndKey,
// i.e. [,b) will be considered [b.Prev(),b).
//
// TODO(irfansharif): This does not currently work for spans that straddle
// across multiple added spans. Specifically a spanset with spans [a-c) and
// [b-d) added under read only and read write access modes respectively would
// fail at checking if read only access over the span [a-d) was requested. This
// is also a problem if the added spans were read only and the spanset wasn't
// already SortAndDedup-ed.
func (s *SpanSet) CheckAllowed(access SpanAccess, span TrickySpan) error {
	return s.checkAllowed(access, span, func(_ SpanAccess, _ Span) bool {
		return true
	})
}

// CheckAllowedAt is like CheckAllowed, except it returns an error if the access
// is not allowed over the given keyspan at the given timestamp.
func (s *SpanSet) CheckAllowedAt(
	access SpanAccess, span TrickySpan, timestamp hlc.Timestamp,
) error {
	mvcc := !timestamp.IsEmpty()
	return s.checkAllowed(access, span, func(declAccess SpanAccess, declSpan Span) bool {
		declTimestamp := declSpan.Timestamp
		if declTimestamp.IsEmpty() {
			// When the span is declared as non-MVCC (i.e. with an empty
			// timestamp), it's equivalent to a read/write mutex where we
			// don't consider access timestamps.
			return true
		}

		switch declAccess {
		case SpanReadOnly:
			switch access {
			case SpanReadOnly:
				// Read spans acquired at a specific timestamp only allow reads
				// at that timestamp and below. Non-MVCC access is not allowed.
				return mvcc && timestamp.LessEq(declTimestamp)
			case SpanReadWrite:
				// NB: should not get here, see checkAllowed.
				panic("unexpected SpanReadWrite access")
			default:
				panic("unexpected span access")
			}
		case SpanReadWrite:
			switch access {
			case SpanReadOnly:
				// Write spans acquired at a specific timestamp allow reads at
				// any timestamp. Non-MVCC access is not allowed.
				return mvcc
			case SpanReadWrite:
				// Write spans acquired at a specific timestamp allow writes at
				// that timestamp of above. Non-MVCC access is not allowed.
				return mvcc && declTimestamp.LessEq(timestamp)
			default:
				panic("unexpected span access")
			}
		default:
			panic("unexpected span access")
		}
	})
}

func (s *SpanSet) checkAllowed(
	access SpanAccess, span TrickySpan, check func(SpanAccess, Span) bool,
) error {
	// Unless explicitly disabled, check if we access any forbidden spans.
	if !s.allowForbidden {
		// Check if the span is forbidden.
		for _, matcher := range s.forbiddenSpansMatchers {
			if err := matcher(span); err != nil {
				return errors.Errorf("cannot %s span %s: matches forbidden pattern",
					access, span)
			}
		}
	}

	// Unless explicitly disabled, check if we access any undeclared spans.
	if s.allowUndeclared {
		return nil
	}

	scope := SpanGlobal
	if (span.Key != nil && keys.IsLocal(span.Key)) ||
		(span.EndKey != nil && keys.IsLocal(span.EndKey)) {
		scope = SpanLocal
	}

	for ac := access; ac < NumSpanAccess; ac++ {
		for _, cur := range s.spans[ac][scope] {
			if Contains(cur.Span, span) && check(ac, cur) {
				return nil
			}
		}
	}

	return errors.Errorf("cannot %s undeclared span %s\ndeclared:\n%s\nstack:\n%s", access, span, s, debugutil.Stack())
}

// doesNormalSpanContainPointTrickySpan takes a normal span (s1), and takes a
// tricky span s2, where the Key is nil, which represents:
// [EndKey.Prev(), EndKey), and returns whether s1 contains s2 or not.
func doesNormalSpanContainPointTrickySpan(s1 roachpb.Span, s2 TrickySpan) bool {
	// The following is equivalent to:
	//   s1.Contains(roachpb.Span{Key: s2.EndKey.Prev()})

	if s1.EndKey == nil {
		return s1.Key.IsPrev(s2.EndKey)
	}

	return s1.Key.Compare(s2.EndKey) < 0 && s1.EndKey.Compare(s2.EndKey) >= 0
}

// Contains returns whether s1 contains s2, where s2 can be a TrickySpan.
func Contains(s1 roachpb.Span, s2 TrickySpan) bool {
	if s2.Key != nil {
		// The common case: s2 is a regular span with a non-nil start key.
		return s1.Contains(roachpb.Span(s2))
	}

	// s2 is a TrickySpan with nil Key and non-nil EndKey.
	return doesNormalSpanContainPointTrickySpan(s1, s2)
}

// Overlaps returns whether s1 overlaps s2, where s2 can be a TrickySpan.
func Overlaps(s1 roachpb.Span, s2 TrickySpan) bool {
	// The common case: both spans have non-nil start keys.
	if s2.Key != nil {
		return s1.Overlaps(roachpb.Span(s2))
	}

	// s2 is a TrickySpan with nil Key and non-nil EndKey. Since s2 is infinitely
	// small, s1 and s2 overlap IFF s1 contains s2.
	return doesNormalSpanContainPointTrickySpan(s1, s2)
}

// Validate returns an error if any spans that have been added to the set
// are invalid.
func (s *SpanSet) Validate() error {
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			for _, cur := range s.GetSpans(sa, ss) {
				if len(cur.EndKey) > 0 && cur.Key.Compare(cur.EndKey) >= 0 {
					return errors.Errorf("inverted span %s %s", cur.Key, cur.EndKey)
				}
			}
		}
	}

	return nil
}

// DisableUndeclaredAccessAssertions disables the assertions that prevent
// undeclared access to spans. This is generally set by requests that rely on
// other forms of synchronization for correctness (e.g. GCRequest).
func (s *SpanSet) DisableUndeclaredAccessAssertions() {
	s.allowUndeclared = true
}

// DisableForbiddenSpansAssertions disables forbidden spans assertions.
func (s *SpanSet) DisableForbiddenSpansAssertions() {
	s.allowForbidden = true
}
