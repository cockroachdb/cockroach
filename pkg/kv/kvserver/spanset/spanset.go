// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanset

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// SpanIsolation records the intended isolation level of access in a SpanSet.
type SpanIsolation int16

// Constants for SpanIsolation. Higher-valued accesses imply lower-level ones.
const (
	RequestIsolation SpanIsolation = iota
	TransactionIsolation
	NumSpanIsolation
)

// String returns a string representation of the SpanIsolation.
func (a SpanIsolation) String() string {
	switch a {
	case RequestIsolation:
		return "request"
	case TransactionIsolation:
		return "transaction"
	default:
		panic("unreachable")
	}
}

// SpanAccess records the intended mode of access in a SpanSet.
type SpanAccess int16

// Constants for SpanAccess. Higher-valued accesses imply lower-level ones.
// TODO(nvanbenschoten):
//   s/SpanReadOnly/ReadOnlyAccess/g
//   s/SpanReadWrite/ReadWriteAccess/g
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

// SpanScope divides access types into local and global keys.
type SpanScope int16

// Constants for span scopes.
// TODO(nvanbenschoten):
//   s/SpanGlobal/GlobalScope/g
//   s/SpanLocal/LocalScope/g
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

// SpanSet ...
type SpanSet [NumSpanIsolation][NumSpanAccess][NumSpanScope][]roachpb.Span

// Get returns a slice of spans with the given parameters.
func (s *SpanSet) Get(si SpanIsolation, sa SpanAccess, ss SpanScope) []roachpb.Span {
	return (*s)[si][sa][ss]
}

// GetRef returns a reference to a slice of spans with the given parameters.
func (s *SpanSet) GetRef(si SpanIsolation, sa SpanAccess, ss SpanScope) *[]roachpb.Span {
	return &(*s)[si][sa][ss]
}

// ForEach calls the provided function with each span.
func (s *SpanSet) ForEach(fn func(SpanIsolation, SpanAccess, SpanScope, roachpb.Span)) {
	for si := SpanIsolation(0); si < NumSpanIsolation; si++ {
		for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
			for ss := SpanScope(0); ss < NumSpanScope; ss++ {
				for _, span := range s.Get(si, sa, ss) {
					fn(si, sa, ss, span)
				}
			}
		}
	}
}

// Map applies a mapping function to each slice of spans sharing identical
// isolation, access, and scope levels.
func (s *SpanSet) Map(
	fn func(SpanIsolation, SpanAccess, SpanScope, []roachpb.Span) []roachpb.Span,
) {
	for si := SpanIsolation(0); si < NumSpanIsolation; si++ {
		for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
			for ss := SpanScope(0); ss < NumSpanScope; ss++ {
				*s.GetRef(si, sa, ss) = fn(si, sa, ss, s.Get(si, sa, ss))
			}
		}
	}
}

// String prints a string representation of the SpanSet.
func (s *SpanSet) String() string {
	var buf strings.Builder
	s.ForEach(func(si SpanIsolation, sa SpanAccess, ss SpanScope, span roachpb.Span) {
		fmt.Fprintf(&buf, "%s %s %s: %s\n", si, sa, ss, span)
	})
	return buf.String()
}

// Len returns the total number of spans tracked across all accesses and scopes.
func (s *SpanSet) Len() int {
	var count int
	s.ForEach(func(_ SpanIsolation, _ SpanAccess, _ SpanScope, _ roachpb.Span) {
		count++
	})
	return count
}

// Empty returns whether the set contains any spans across all isolation,
// access, and scope levels.
func (s *SpanSet) Empty() bool {
	return s.Len() == 0
}

// EmptyTransactional returns whether the set contains any spans across all
// access and scope levels with the transaction isolation.
func (s *SpanSet) EmptyTransactional() bool {
	var count int
	s.ForEach(func(si SpanIsolation, _ SpanAccess, _ SpanScope, _ roachpb.Span) {
		if si == TransactionIsolation {
			count++
		}
	})
	return count == 0
}

// BoundarySpan returns a span containing all the spans with the given params.
func (s *SpanSet) BoundarySpan(scope SpanScope) roachpb.Span {
	var boundary roachpb.Span
	s.ForEach(func(_ SpanIsolation, _ SpanAccess, _ SpanScope, span roachpb.Span) {
		if !boundary.Valid() {
			boundary = span
		} else {
			boundary = boundary.Combine(span)
		}
	})
	return boundary
}

// Validate returns an error if any spans that have been added to the set
// are invalid.
func (s *SpanSet) Validate() error {
	var err error
	s.ForEach(func(_ SpanIsolation, _ SpanAccess, _ SpanScope, span roachpb.Span) {
		if !span.Valid() && err == nil {
			err = errors.Errorf("invalid span %s", span)
		}
	})
	return err
}

// Reserve space for N additional spans.
func (s *SpanSet) Reserve(si SpanIsolation, sa SpanAccess, ss SpanScope, n int) {
	existing := s.Get(si, sa, ss)
	updated := make([]roachpb.Span, len(existing), n+cap(existing))
	copy(updated, existing)
	*s.GetRef(si, sa, ss) = updated
}

// Add adds a span to the span set with the specified isolation, and access
// levels.
func (s *SpanSet) Add(si SpanIsolation, sa SpanAccess, span roachpb.Span) {
	ss := SpanGlobal
	if keys.IsLocal(span.Key) {
		ss = SpanLocal
	}
	*s.GetRef(si, sa, ss) = append(s.Get(si, sa, ss), span)
}

// SortAndDedup sorts the spans in the SpanSet and removes any duplicates.
func (s *SpanSet) SortAndDedup() {
	s.Map(func(_ SpanIsolation, _ SpanAccess, _ SpanScope, spans []roachpb.Span) []roachpb.Span {
		spans, _ /* distinct */ = roachpb.MergeSpans(spans)
		return spans
	})
}

// Merge merges all spans in s2 into s. s2 is not modified.
func (s *SpanSet) Merge(s2 *SpanSet) {
	s.Map(func(si SpanIsolation, sa SpanAccess, ss SpanScope, spans []roachpb.Span) []roachpb.Span {
		return append(spans, s2.Get(si, sa, ss)...)
	})
	s.SortAndDedup()
}

// AssertAllowed calls CheckAllowed and fatals if the access is not allowed.
// Timestamps associated with the spans in the spanset are not considered,
// only the span boundaries are checked.
func (s *SpanSet) AssertAllowed(sa SpanAccess, span roachpb.Span) {
	if err := s.CheckAllowed(sa, span); err != nil {
		log.Fatalf(context.TODO(), "%v", err)
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
func (s *SpanSet) CheckAllowed(saCheck SpanAccess, spanCheck roachpb.Span) error {
	ssCheck := SpanGlobal
	if (spanCheck.Key != nil && keys.IsLocal(spanCheck.Key)) ||
		(spanCheck.EndKey != nil && keys.IsLocal(spanCheck.EndKey)) {
		ssCheck = SpanLocal
	}

	allowed := false
	s.ForEach(func(_ SpanIsolation, sa SpanAccess, ss SpanScope, span roachpb.Span) {
		if sa < saCheck {
			// Incompatible access (e.g. read access does not allow writes).
			return
		} else if ss != ssCheck {
			// Incompatible scope (e.g. local scope cannot contain global keys).
			return
		}
		if contains(span, spanCheck) {
			allowed = true
		}
	})
	if !allowed {
		return errors.Errorf("cannot %s undeclared span %s\ndeclared:\n%s\nstack:\n%s",
			saCheck, spanCheck, s, debug.Stack())
	}
	return nil
}

//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//

// TO DELETE.

// AddNonMVCC adds a non-MVCC span to the span set. This should typically
// local keys.
func (s *SpanSet) AddNonMVCC(sa SpanAccess, span roachpb.Span) {
	s.Add(RequestIsolation, sa, span)
}

// AddMVCC adds an MVCC span to the span set to be accessed at the given
// timestamp. This should typically be used for MVCC keys, user keys for e.g.
func (s *SpanSet) AddMVCC(sa SpanAccess, span roachpb.Span, _ hlc.Timestamp) {
	s.Add(TransactionIsolation, sa, span)
}

// CheckAllowedAt ...
func (s *SpanSet) CheckAllowedAt(saCheck SpanAccess, spanCheck roachpb.Span, _ hlc.Timestamp) error {
	return s.CheckAllowed(saCheck, spanCheck)
}

//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//

// // Span is used to represent a keyspan accessed by a request at a given
// // timestamp. A zero timestamp indicates it's a non-MVCC access.
// type Span struct {
// 	roachpb.Span
// 	Timestamp hlc.Timestamp
// }

// // SpanSet tracks the set of key spans touched by a command, broken into MVCC
// // and non-MVCC accesses. The set is divided into subsets for access type
// // (read-only or read/write) and key scope (local or global; used to facilitate
// // use by the separate local and global latches).
// // The Span slice for a particular access and scope contains non-overlapping
// // spans in increasing key order after calls to SortAndDedup.
// type SpanSetOld struct {
// 	spans [NumSpanAccess][NumSpanScope][]Span
// }

// // String prints a string representation of the SpanSet.
// func (s *SpanSetOld) String() string {
// 	var buf strings.Builder
// 	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
// 		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
// 			for _, cur := range s.GetSpans(sa, ss) {
// 				fmt.Fprintf(&buf, "%s %s: %s at %s\n",
// 					sa, ss, cur.Span.String(), cur.Timestamp.String())
// 			}
// 		}
// 	}
// 	return buf.String()
// }

// // Len returns the total number of spans tracked across all accesses and scopes.
// func (s *SpanSetOld) Len() int {
// 	var count int
// 	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
// 		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
// 			count += len(s.GetSpans(sa, ss))
// 		}
// 	}
// 	return count
// }

// // Empty returns whether the set contains any spans across all accesses and scopes.
// func (s *SpanSetOld) Empty() bool {
// 	return s.Len() == 0
// }

// // Reserve space for N additional spans.
// func (s *SpanSetOld) Reserve(access SpanAccess, scope SpanScope, n int) {
// 	existing := s.spans[access][scope]
// 	s.spans[access][scope] = make([]Span, len(existing), n+cap(existing))
// 	copy(s.spans[access][scope], existing)
// }

// // AddNonMVCC adds a non-MVCC span to the span set. This should typically
// // local keys.
// func (s *SpanSetOld) AddNonMVCC(access SpanAccess, span roachpb.Span) {
// 	s.AddMVCC(access, span, hlc.Timestamp{})
// }

// // AddMVCC adds an MVCC span to the span set to be accessed at the given
// // timestamp. This should typically be used for MVCC keys, user keys for e.g.
// func (s *SpanSetOld) AddMVCC(access SpanAccess, span roachpb.Span, timestamp hlc.Timestamp) {
// 	scope := SpanGlobal
// 	if keys.IsLocal(span.Key) {
// 		scope = SpanLocal
// 		timestamp = hlc.Timestamp{}
// 	}

// 	s.spans[access][scope] = append(s.spans[access][scope], Span{Span: span, Timestamp: timestamp})
// }

// // Merge merges all spans in s2 into s. s2 is not modified.
// func (s *SpanSetOld) Merge(s2 *SpanSet) {
// 	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
// 		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
// 			s.spans[sa][ss] = append(s.spans[sa][ss], s2.spans[sa][ss]...)
// 		}
// 	}
// 	s.SortAndDedup()
// }

// // SortAndDedup sorts the spans in the SpanSet and removes any duplicates.
// func (s *SpanSetOld) SortAndDedup() {
// 	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
// 		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
// 			s.spans[sa][ss], _ /* distinct */ = mergeSpans(s.spans[sa][ss])
// 		}
// 	}
// }

// // GetSpans returns a slice of spans with the given parameters.
// func (s *SpanSetOld) GetSpans(access SpanAccess, scope SpanScope) []Span {
// 	return s.spans[access][scope]
// }

// // BoundarySpan returns a span containing all the spans with the given params.
// func (s *SpanSetOld) BoundarySpan(scope SpanScope) roachpb.Span {
// 	var boundary roachpb.Span
// 	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
// 		for _, cur := range s.GetSpans(sa, scope) {
// 			if !boundary.Valid() {
// 				boundary = cur.Span
// 				continue
// 			}
// 			boundary = boundary.Combine(cur.Span)
// 		}
// 	}
// 	return boundary
// }

// // MaxProtectedTimestamp returns the maximum timestamp that is protected across
// // all MVCC spans in the SpanSet. ReadWrite spans are protected from their
// // declared timestamp forward, so they have no maximum protect timestamp.
// // However, ReadOnly are protected only up to their declared timestamp and
// // are not protected at later timestamps.
// func (s *SpanSetOld) MaxProtectedTimestamp() hlc.Timestamp {
// 	maxTS := hlc.MaxTimestamp
// 	for ss := SpanScope(0); ss < NumSpanScope; ss++ {
// 		for _, cur := range s.GetSpans(SpanReadOnly, ss) {
// 			curTS := cur.Timestamp
// 			if !curTS.IsEmpty() {
// 				maxTS.Backward(curTS)
// 			}
// 		}
// 	}
// 	return maxTS
// }

// // Intersects returns true iff the span set denoted by `other` has any
// // overlapping spans with `s`, and that those spans overlap in access type. Note
// // that timestamps associated with the spans in the spanset are not considered,
// // only the span boundaries are checked.
// func (s *SpanSetOld) Intersects(other *SpanSet) bool {
// 	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
// 		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
// 			otherSpans := other.GetSpans(sa, ss)
// 			for _, span := range otherSpans {
// 				// If access is allowed, we must have an overlap.
// 				if err := s.CheckAllowed(sa, span.Span); err == nil {
// 					return true
// 				}
// 			}
// 		}
// 	}
// 	return false
// }

// // AssertAllowed calls CheckAllowed and fatals if the access is not allowed.
// // Timestamps associated with the spans in the spanset are not considered,
// // only the span boundaries are checked.
// func (s *SpanSetOld) AssertAllowed(access SpanAccess, span roachpb.Span) {
// 	if err := s.CheckAllowed(access, span); err != nil {
// 		log.Fatalf(context.TODO(), "%v", err)
// 	}
// }

// // CheckAllowed returns an error if the access is not allowed over the given
// // keyspan based on the collection of spans in the spanset. Timestamps
// // associated with the spans in the spanset are not considered, only the span
// // boundaries are checked.
// //
// // If the provided span contains only an (exclusive) EndKey and has a nil
// // (inclusive) Key then Key is considered to be the key previous to EndKey,
// // i.e. [,b) will be considered [b.Prev(),b).
// //
// // TODO(irfansharif): This does not currently work for spans that straddle
// // across multiple added spans. Specifically a spanset with spans [a-c) and
// // [b-d) added under read only and read write access modes respectively would
// // fail at checking if read only access over the span [a-d) was requested. This
// // is also a problem if the added spans were read only and the spanset wasn't
// // already SortAndDedup-ed.
// func (s *SpanSetOld) CheckAllowed(access SpanAccess, span roachpb.Span) error {
// 	return s.checkAllowed(access, span, func(_ SpanAccess, _ Span) bool {
// 		return true
// 	})
// }

// // CheckAllowedAt is like CheckAllowed, except it returns an error if the access
// // is not allowed over the given keyspan at the given timestamp.
// func (s *SpanSetOld) CheckAllowedAt(
// 	access SpanAccess, span roachpb.Span, timestamp hlc.Timestamp,
// ) error {
// 	mvcc := !timestamp.IsEmpty()
// 	return s.checkAllowed(access, span, func(declAccess SpanAccess, declSpan Span) bool {
// 		declTimestamp := declSpan.Timestamp
// 		if declTimestamp.IsEmpty() {
// 			// When the span is declared as non-MVCC (i.e. with an empty
// 			// timestamp), it's equivalent to a read/write mutex where we
// 			// don't consider access timestamps.
// 			return true
// 		}

// 		switch declAccess {
// 		case SpanReadOnly:
// 			switch access {
// 			case SpanReadOnly:
// 				// Read spans acquired at a specific timestamp only allow reads
// 				// at that timestamp and below. Non-MVCC access is not allowed.
// 				return mvcc && timestamp.LessEq(declTimestamp)
// 			case SpanReadWrite:
// 				// NB: should not get here, see checkAllowed.
// 				panic("unexpected SpanReadWrite access")
// 			default:
// 				panic("unexpected span access")
// 			}
// 		case SpanReadWrite:
// 			switch access {
// 			case SpanReadOnly:
// 				// Write spans acquired at a specific timestamp allow reads at
// 				// any timestamp. Non-MVCC access is not allowed.
// 				return mvcc
// 			case SpanReadWrite:
// 				// Write spans acquired at a specific timestamp allow writes at
// 				// that timestamp of above. Non-MVCC access is not allowed.
// 				return mvcc && declTimestamp.LessEq(timestamp)
// 			default:
// 				panic("unexpected span access")
// 			}
// 		default:
// 			panic("unexpected span access")
// 		}
// 	})
// }

// func (s *SpanSetOld) checkAllowed(
// 	access SpanAccess, span roachpb.Span, check func(SpanAccess, Span) bool,
// ) error {
// 	scope := SpanGlobal
// 	if (span.Key != nil && keys.IsLocal(span.Key)) ||
// 		(span.EndKey != nil && keys.IsLocal(span.EndKey)) {
// 		scope = SpanLocal
// 	}

// 	for ac := access; ac < NumSpanAccess; ac++ {
// 		for _, cur := range s.spans[ac][scope] {
// 			if contains(cur.Span, span) && check(ac, cur) {
// 				return nil
// 			}
// 		}
// 	}

// 	return errors.Errorf("cannot %s undeclared span %s\ndeclared:\n%s\nstack:\n%s", access, span, s, debug.Stack())
// }

// // Validate returns an error if any spans that have been added to the set
// // are invalid.
// func (s *SpanSetOld) Validate() error {
// 	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
// 		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
// 			for _, cur := range s.GetSpans(sa, ss) {
// 				if len(cur.EndKey) > 0 && cur.Key.Compare(cur.EndKey) >= 0 {
// 					return errors.Errorf("inverted span %s %s", cur.Key, cur.EndKey)
// 				}
// 			}
// 		}
// 	}

// 	return nil
// }

// contains returns whether s1 contains s2. Unlike Span.Contains, this function
// supports spans with a nil start key and a non-nil end key (e.g. "[nil, c)").
// In this form, s2.Key (inclusive) is considered to be the previous key to
// s2.EndKey (exclusive).
func contains(s1, s2 roachpb.Span) bool {
	if s2.Key != nil {
		// The common case.
		return s1.Contains(s2)
	}

	// The following is equivalent to:
	//   s1.Contains(roachpb.Span{Key: s2.EndKey.Prev()})

	if s1.EndKey == nil {
		return s1.Key.IsPrev(s2.EndKey)
	}

	return s1.Key.Compare(s2.EndKey) < 0 && s1.EndKey.Compare(s2.EndKey) >= 0
}
