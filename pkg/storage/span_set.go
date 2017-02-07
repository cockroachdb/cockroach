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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// SpanAccess records the intended mode of access in SpanSet.
type SpanAccess int

// Constants for SpanAccess.
const (
	SpanReadOnly SpanAccess = iota
	SpanReadWrite
	numSpanAccess
)

type spanScope int

const (
	spanGlobal spanScope = iota
	spanLocal
	numSpanScope
)

// SpanSet tracks the set of key spans touched by a command. The set
// is divided into subsets for access type (read-only or read/write)
// and key scope (local or global; used to facilitate use by the
// separate local and global command queues).
type SpanSet struct {
	spans [numSpanAccess][numSpanScope][]roachpb.Span
}

// reserve space for N additional keys.
func (ss *SpanSet) reserve(access SpanAccess, scope spanScope, n int) {
	existing := ss.spans[access][scope]
	ss.spans[access][scope] = make([]roachpb.Span, len(existing), n+cap(existing))
	copy(ss.spans[access][scope], existing)
}

// Add a span to the set.
func (ss *SpanSet) Add(access SpanAccess, span roachpb.Span) {
	scope := spanGlobal
	if keys.IsLocal(span.Key) {
		scope = spanLocal
	}
	ss.spans[access][scope] = append(ss.spans[access][scope], span)
}

// getSpans returns a slice of spans with the given parameters.
func (ss *SpanSet) getSpans(access SpanAccess, scope spanScope) []roachpb.Span {
	return ss.spans[access][scope]
}
