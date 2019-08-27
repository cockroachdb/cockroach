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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// SpanAccess records the intended mode of access in SpanSet.
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

// SpanSet tracks the set of key spans touched by a command. The set
// is divided into subsets for access type (read-only or read/write)
// and key scope (local or global; used to facilitate use by the
// separate local and global latches).
type SpanSet struct {
	spans [NumSpanAccess][NumSpanScope][]roachpb.Span
}

// String prints a string representation of the span set.
func (ss *SpanSet) String() string {
	var buf strings.Builder
	for i := SpanAccess(0); i < NumSpanAccess; i++ {
		for j := SpanScope(0); j < NumSpanScope; j++ {
			for _, span := range ss.GetSpans(i, j) {
				fmt.Fprintf(&buf, "%s %s: %s\n", i, j, span)
			}
		}
	}
	return buf.String()
}

// Len returns the total number of spans tracked across all accesses and scopes.
func (ss *SpanSet) Len() int {
	var count int
	for i := SpanAccess(0); i < NumSpanAccess; i++ {
		for j := SpanScope(0); j < NumSpanScope; j++ {
			count += len(ss.GetSpans(i, j))
		}
	}
	return count
}

// Reserve space for N additional keys.
func (ss *SpanSet) Reserve(access SpanAccess, scope SpanScope, n int) {
	existing := ss.spans[access][scope]
	ss.spans[access][scope] = make([]roachpb.Span, len(existing), n+cap(existing))
	copy(ss.spans[access][scope], existing)
}

// Add a span to the set.
func (ss *SpanSet) Add(access SpanAccess, span roachpb.Span) {
	scope := SpanGlobal
	if keys.IsLocal(span.Key) {
		scope = SpanLocal
	}
	ss.spans[access][scope] = append(ss.spans[access][scope], span)
}

// SortAndDedup sorts the spans in the SpanSet and removes any duplicates.
func (ss *SpanSet) SortAndDedup() {
	for i := SpanAccess(0); i < NumSpanAccess; i++ {
		for j := SpanScope(0); j < NumSpanScope; j++ {
			ss.spans[i][j], _ /* distinct */ = roachpb.MergeSpans(ss.spans[i][j])
		}
	}
}

// GetSpans returns a slice of spans with the given parameters.
func (ss *SpanSet) GetSpans(access SpanAccess, scope SpanScope) []roachpb.Span {
	return ss.spans[access][scope]
}

// BoundarySpan returns a span containing all the spans with the given params.
func (ss *SpanSet) BoundarySpan(scope SpanScope) roachpb.Span {
	var boundary roachpb.Span
	for i := SpanAccess(0); i < NumSpanAccess; i++ {
		for _, span := range ss.spans[i][scope] {
			if !boundary.Valid() {
				boundary = span
				continue
			}
			boundary = boundary.Combine(span)
		}
	}
	return boundary
}

// AssertAllowed calls checkAllowed and fatals if the access is not allowed.
func (ss *SpanSet) AssertAllowed(access SpanAccess, span roachpb.Span) {
	if err := ss.CheckAllowed(access, span); err != nil {
		log.Fatal(context.TODO(), err)
	}
}

// CheckAllowed returns an error if the access is not allowed.
//
// TODO(irfansharif): This does not currently work for spans that straddle
// across multiple added spans. Specifically a spanset with spans [a-c) and
// [b-d) added under read only and read write access modes respectively would
// fail at checking if read only access over the span [a-d) was requested. This
// is also a problem if the added spans were read only and the spanset wasn't
// already SortAndDedup-ed.
func (ss *SpanSet) CheckAllowed(access SpanAccess, span roachpb.Span) error {
	scope := SpanGlobal
	if keys.IsLocal(span.Key) {
		scope = SpanLocal
	}
	for ac := access; ac < NumSpanAccess; ac++ {
		for _, s := range ss.spans[ac][scope] {
			if s.Contains(span) {
				return nil
			}
		}
	}

	return errors.Errorf("cannot %s undeclared span %s\ndeclared:\n%s", access, span, ss)
}

// Validate returns an error if any spans that have been added to the set
// are invalid.
func (ss *SpanSet) Validate() error {
	for _, accessSpans := range ss.spans {
		for _, spans := range accessSpans {
			for _, span := range spans {
				if len(span.EndKey) > 0 && span.Key.Compare(span.EndKey) >= 0 {
					return errors.Errorf("inverted span %s %s", span.Key, span.EndKey)
				}
			}
		}
	}
	return nil
}
