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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Test that spans are properly classified as global or local and that
// GetSpans respects the scope argument.
func TestSpanSetGetSpansScope(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var ss SpanSet
	ss.Add(SpanReadOnly, roachpb.Span{Key: roachpb.Key("a")})
	ss.Add(SpanReadOnly, roachpb.Span{Key: keys.RangeLastGCKey(1)})
	ss.Add(SpanReadOnly, roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")})

	exp := []roachpb.Span{{Key: keys.RangeLastGCKey(1)}}
	if act := ss.GetSpans(SpanReadOnly, SpanLocal); !reflect.DeepEqual(act, exp) {
		t.Errorf("get local spans: got %v, expected %v", act, exp)
	}

	exp = []roachpb.Span{
		{Key: roachpb.Key("a")},
		{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
	}

	if act := ss.GetSpans(SpanReadOnly, SpanGlobal); !reflect.DeepEqual(act, exp) {
		t.Errorf("get global spans: got %v, expected %v", act, exp)
	}
}

// Test that CheckAllowed properly enforces boundaries.
func TestSpanSetCheckAllowedBoundaries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var ss SpanSet
	ss.Add(SpanReadOnly, roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")})
	ss.Add(SpanReadOnly, roachpb.Span{Key: roachpb.Key("g")})
	ss.Add(SpanReadOnly, roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("q")})

	allowed := []roachpb.Span{
		// Exactly as declared.
		{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")},
		{Key: roachpb.Key("g")},
		{Key: roachpb.Key("k"), EndKey: roachpb.Key("q")},

		// Points within the non-zero-length spans.
		{Key: roachpb.Key("c")},
		{Key: roachpb.Key("l")},

		// Sub-spans.
		{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
		{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
		{Key: roachpb.Key("l"), EndKey: roachpb.Key("m")},
	}
	for _, span := range allowed {
		if err := ss.CheckAllowed(SpanReadOnly, span); err != nil {
			t.Errorf("expected %s to be allowed, but got error: %s", span, err)
		}
	}

	disallowed := []roachpb.Span{
		// Points outside the declared spans, and on the endpoints.
		{Key: roachpb.Key("a")},
		{Key: roachpb.Key("d")},
		{Key: roachpb.Key("h")},
		{Key: roachpb.Key("v")},
		{Key: roachpb.Key("q")},

		// Spans outside the declared spans.
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
		{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")},
		{Key: roachpb.Key("q"), EndKey: roachpb.Key("z")},

		// Partial overlap.
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
		{Key: roachpb.Key("c"), EndKey: roachpb.Key("m")},
		{Key: roachpb.Key("g"), EndKey: roachpb.Key("k")},

		// Just past the end.
		{Key: roachpb.Key("b"), EndKey: roachpb.Key("d").Next()},
		{Key: roachpb.Key("g"), EndKey: roachpb.Key("g").Next()},
		{Key: roachpb.Key("k"), EndKey: roachpb.Key("q").Next()},
	}
	for _, span := range disallowed {
		if err := ss.CheckAllowed(SpanReadOnly, span); err == nil {
			t.Errorf("expected %s to be disallowed", span)
		}
	}
}

// Test that a span declared for write access also implies read
// access, but not vice-versa.
func TestSpanSetWriteImpliesRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var ss SpanSet
	roSpan := roachpb.Span{Key: roachpb.Key("read-only")}
	rwSpan := roachpb.Span{Key: roachpb.Key("read-write")}
	ss.Add(SpanReadOnly, roSpan)
	ss.Add(SpanReadWrite, rwSpan)

	if err := ss.CheckAllowed(SpanReadOnly, roSpan); err != nil {
		t.Errorf("expected to be allowed to read roSpan, error: %s", err)
	}
	if err := ss.CheckAllowed(SpanReadWrite, roSpan); err == nil {
		t.Errorf("expected not to be allowed to write roSpan")
	}
	if err := ss.CheckAllowed(SpanReadOnly, rwSpan); err != nil {
		t.Errorf("expected to be allowed to read rwSpan, error: %s", err)
	}
	if err := ss.CheckAllowed(SpanReadWrite, rwSpan); err != nil {
		t.Errorf("expected to be allowed to read rwSpan, error: %s", err)
	}
}
