// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package spanset

import (
	"bytes"
	"context"
	"fmt"

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

// SpanScope divides access types into local and global keys.
type SpanScope int

// Constants for span scopes.
const (
	SpanGlobal SpanScope = iota
	SpanLocal
	NumSpanScope
)

// SpanSet tracks the set of key spans touched by a command. The set
// is divided into subsets for access type (read-only or read/write)
// and key scope (local or global; used to facilitate use by the
// separate local and global command queues).
type SpanSet struct {
	spans [NumSpanAccess][NumSpanScope][]roachpb.Span
}

// String prints a string representation of the span set.
func (ss *SpanSet) String() string {
	var buf bytes.Buffer
	for i := SpanAccess(0); i < NumSpanAccess; i++ {
		for j := SpanScope(0); j < NumSpanScope; j++ {
			for _, span := range ss.GetSpans(i, j) {
				fmt.Fprintf(&buf, "%d %d: %s\n", i, j, span)
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

// GetSpans returns a slice of spans with the given parameters.
func (ss *SpanSet) GetSpans(access SpanAccess, scope SpanScope) []roachpb.Span {
	return ss.spans[access][scope]
}

// AssertAllowed calls checkAllowed and fatals if the access is not allowed.
func (ss *SpanSet) AssertAllowed(access SpanAccess, span roachpb.Span) {
	if err := ss.CheckAllowed(access, span); err != nil {
		log.Fatal(context.TODO(), err)
	}
}

// CheckAllowed returns an error if the access is not allowed.
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
	action := "read"
	if access == SpanReadWrite {
		action = "write"
	}

	return errors.Errorf("cannot %s undeclared span %s\ndeclared:\n%s", action, span, ss)
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
