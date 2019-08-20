// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanlatch

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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

// SpanSet tracks the set of key spans touched by a command and at what
// timestamps. The set is divided into subsets for access type (read-only or
// read/write) and key scope (local or global; used to facilitate use by the
// separate local and global latches).
type SpanSet struct {
	latches [NumSpanAccess][NumSpanScope][]latch
}

// String prints a string representation of the SpanSet.
func (s *SpanSet) String() string {
	var buf strings.Builder
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			for _, sl := range s.GetLatches(sa, ss) {
				fmt.Fprintf(&buf, "%s %s: %s at %s\n", sa, ss, sl.span.String(), sl.ts.String())
			}
		}
	}
	return buf.String()
}

// Len returns the total number of span latches tracked across all accesses and scopes.
func (s *SpanSet) Len() int {
	var count int
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			count += len(s.GetLatches(sa, ss))
		}
	}
	return count
}

// Reserve space for N additional spans.
func (s *SpanSet) Reserve(access SpanAccess, scope SpanScope, n int) {
	existing := s.latches[access][scope]
	s.latches[access][scope] = make([]latch, len(existing), n+cap(existing))
	copy(s.latches[access][scope], existing)
}

// Add a span to the set without an associate timestamp. This should typically
// be used for non-MVCC access, for local keys for e.g.
func (s *SpanSet) Add(access SpanAccess, span roachpb.Span) {
	s.AddAt(access, span, hlc.Timestamp{})
}

// AddAt adds a span to the set to be accessed at the given timestamp.
func (s *SpanSet) AddAt(access SpanAccess, span roachpb.Span, timestamp hlc.Timestamp) {
	scope := SpanGlobal
	if keys.IsLocal(span.Key) {
		scope = SpanLocal
		// All local spans are treated non-MVCC.
		timestamp = hlc.Timestamp{}
	}

	s.latches[access][scope] = append(s.latches[access][scope], latch{span: span, ts: timestamp})
}

// SortAndDedup sorts the spans in the SpanSet and removes any duplicates.
func (s *SpanSet) SortAndDedup() {
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			s.latches[sa][ss], _ /* distinct */ = mergeSpanLatches(s.latches[sa][ss])
		}
	}
}

// GetLatches returns a slice of span latches with the given parameters.
func (s *SpanSet) GetLatches(access SpanAccess, scope SpanScope) []latch {
	return s.latches[access][scope]
}

// BoundarySpan returns a span containing all the spans with the given params.
func (s *SpanSet) BoundarySpan(scope SpanScope) roachpb.Span {
	var boundary roachpb.Span
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for _, sl := range s.latches[sa][scope] {
			if !boundary.Valid() {
				boundary = sl.span
				continue
			}
			boundary = boundary.Combine(sl.span)
		}
	}
	return boundary
}

// AssertAllowed calls CheckAllowed and fatals if the access is not allowed.
func (s *SpanSet) AssertAllowed(access SpanAccess, span roachpb.Span) {
	if err := s.CheckAllowed(access, span); err != nil {
		log.Fatal(context.TODO(), err)
	}
}

// CheckAllowed returns an error if the access is not allowed. This should be
// used for non-MVCC spans.
func (s *SpanSet) CheckAllowed(access SpanAccess, span roachpb.Span) error {
	return s.CheckAllowedAt(access, span, hlc.Timestamp{})
}

// CheckAllowedAt returns an error if the access is not allowed at the given timestamp.
func (s *SpanSet) CheckAllowedAt(access SpanAccess, span roachpb.Span, timestamp hlc.Timestamp) error {
	scope := SpanGlobal
	if keys.IsLocal(span.Key) {
		scope = SpanLocal
	}

	zerots := hlc.Timestamp{}
	for ac := access; ac < NumSpanAccess; ac++ {
		for _, sl := range s.latches[ac][scope] {
			if sl.span.Contains(span) {
				if sl.ts == zerots {
					// When the span latch is acquired as non-MVCC, it's equivalent to a sync.RWMutex
					// where we don't consider access timestamps.
					return nil
				}

				if access == SpanReadWrite {
					// Writes under a write span latch can only happen at the specific timestamp the latch was acquired.
					if timestamp == sl.ts {
						return nil
					}
				} else {
					// Read span latches acquired at a specific timestamp only allow reads at that timestamp and below.
					// Non-MVCC access is not allowed.
					if timestamp != zerots && (timestamp.Less(sl.ts) || timestamp == sl.ts) {
						return nil
					}
				}
			}
		}
	}

	if timestamp == zerots {
		return errors.Errorf("cannot %s undeclared span %s\ndeclared:\n%s", access, span, s)
	}
	return errors.Errorf("cannot %s undeclared span %s at %s\ndeclared:\n%s", access, span, timestamp.String(), s)
}

// Validate returns an error if any span latches that have been added to the set
// are invalid.
func (s *SpanSet) Validate() error {
	for sa := SpanAccess(0); sa < NumSpanAccess; sa++ {
		for ss := SpanScope(0); ss < NumSpanScope; ss++ {
			for _, sl := range s.GetLatches(sa, ss) {
				if len(sl.span.EndKey) > 0 && sl.span.Key.Compare(sl.span.EndKey) >= 0 {
					return errors.Errorf("inverted span %s %s", sl.span.Key, sl.span.EndKey)
				}
			}
		}
	}

	return nil
}
