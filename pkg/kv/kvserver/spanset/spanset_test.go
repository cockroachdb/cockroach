// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanset

import (
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// Test that spans are properly classified as global or local and that
// GetSpans respects the scope argument.
func TestSpanSetGetSpansScope(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var ss SpanSet
	ss.AddNonMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("a")})
	ss.AddNonMVCC(SpanReadOnly, roachpb.Span{Key: keys.RangeGCThresholdKey(1)})
	ss.AddNonMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")})

	exp := []Span{
		{Span: roachpb.Span{Key: keys.RangeGCThresholdKey(1)}},
	}
	if act := ss.GetSpans(SpanReadOnly, SpanLocal); !reflect.DeepEqual(act, exp) {
		t.Errorf("get local spans: got %v, expected %v", act, exp)
	}

	exp = []Span{
		{Span: roachpb.Span{Key: roachpb.Key("a")}},
		{Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
	}

	if act := ss.GetSpans(SpanReadOnly, SpanGlobal); !reflect.DeepEqual(act, exp) {
		t.Errorf("get global spans: got %v, expected %v", act, exp)
	}
}

func TestSpanSetCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ss := new(SpanSet)
	ss.AddMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("abc")}, hlc.Timestamp{WallTime: 123, Logical: 7})
	ss.AddNonMVCC(SpanReadWrite, roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")})

	c := ss.Copy()
	require.Equal(t, ss, c)

	// modifying element in ss should not modify copy
	ss.AddNonMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")})
	require.NotEqual(t, ss, c)
}

func TestSpanSetIterate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	spA := roachpb.Span{Key: roachpb.Key("a")}
	spRO := roachpb.Span{Key: roachpb.Key("r"), EndKey: roachpb.Key("o")}
	spRW := roachpb.Span{Key: roachpb.Key("r"), EndKey: roachpb.Key("w")}
	spLocal := roachpb.Span{Key: keys.RangeGCThresholdKey(1)}

	ss := new(SpanSet)
	ss.AddNonMVCC(SpanReadOnly, spLocal)
	ss.AddNonMVCC(SpanReadOnly, spRO)
	ss.AddNonMVCC(SpanReadOnly, spA)
	ss.AddNonMVCC(SpanReadWrite, spRW)

	type item struct {
		sa   SpanAccess
		ss   SpanScope
		span Span
	}
	expect := []item{
		{sa: SpanReadOnly, ss: SpanGlobal, span: Span{Span: spRO}},
		{sa: SpanReadOnly, ss: SpanGlobal, span: Span{Span: spA}},
		{sa: SpanReadOnly, ss: SpanLocal, span: Span{Span: spLocal}},
		{sa: SpanReadWrite, ss: SpanGlobal, span: Span{Span: spRW}},
	}
	items := []item{}
	ss.Iterate(func(sa SpanAccess, ss SpanScope, span Span) {
		items = append(items, item{sa: sa, ss: ss, span: span})
	})

	require.Equal(t, expect, items)
}

func TestSpanSetMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()

	spA := roachpb.Span{Key: roachpb.Key("a")}
	spBC := roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}
	spCE := roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}
	spBE := roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("e")}
	spLocal := roachpb.Span{Key: keys.RangeGCThresholdKey(1)}

	var ss SpanSet
	ss.AddNonMVCC(SpanReadOnly, spLocal)
	ss.AddNonMVCC(SpanReadOnly, spA)
	ss.AddNonMVCC(SpanReadWrite, spBC)
	require.Equal(t, []Span{{Span: spLocal}}, ss.GetSpans(SpanReadOnly, SpanLocal))
	require.Equal(t, []Span{{Span: spA}}, ss.GetSpans(SpanReadOnly, SpanGlobal))
	require.Equal(t, []Span{{Span: spBC}}, ss.GetSpans(SpanReadWrite, SpanGlobal))

	var ss2 SpanSet
	ss2.AddNonMVCC(SpanReadWrite, spCE)
	require.Nil(t, ss2.GetSpans(SpanReadOnly, SpanLocal))
	require.Nil(t, ss2.GetSpans(SpanReadOnly, SpanGlobal))
	require.Equal(t, []Span{{Span: spCE}}, ss2.GetSpans(SpanReadWrite, SpanGlobal))

	// Merge merges all spans. Notice the new spBE span.
	ss2.Merge(&ss)
	require.Equal(t, []Span{{Span: spLocal}}, ss2.GetSpans(SpanReadOnly, SpanLocal))
	require.Equal(t, []Span{{Span: spA}}, ss2.GetSpans(SpanReadOnly, SpanGlobal))
	require.Equal(t, []Span{{Span: spBE}}, ss2.GetSpans(SpanReadWrite, SpanGlobal))

	// The source set is not mutated on future changes to the merged set.
	ss2.AddNonMVCC(SpanReadOnly, spCE)
	require.Equal(t, []Span{{Span: spLocal}}, ss.GetSpans(SpanReadOnly, SpanLocal))
	require.Equal(t, []Span{{Span: spA}}, ss.GetSpans(SpanReadOnly, SpanGlobal))
	require.Equal(t, []Span{{Span: spBC}}, ss.GetSpans(SpanReadWrite, SpanGlobal))
	require.Equal(t, []Span{{Span: spLocal}}, ss2.GetSpans(SpanReadOnly, SpanLocal))
	require.Equal(t, []Span{{Span: spA}, {Span: spCE}}, ss2.GetSpans(SpanReadOnly, SpanGlobal))
	require.Equal(t, []Span{{Span: spBE}}, ss2.GetSpans(SpanReadWrite, SpanGlobal))
}

// Test that CheckAllowed properly enforces span boundaries.
func TestSpanSetCheckAllowedBoundaries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var bdGkq SpanSet
	bdGkq.AddNonMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")})
	bdGkq.AddNonMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("g")})
	bdGkq.AddNonMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("q")})

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
		if err := bdGkq.CheckAllowed(SpanReadOnly, span); err != nil {
			t.Errorf("expected %s to be allowed, but got error: %+v", span, err)
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
		if err := bdGkq.CheckAllowed(SpanReadOnly, span); err == nil {
			t.Errorf("expected %s to be disallowed", span)
		}
	}
}

// Test that CheckAllowedAt properly enforces timestamp control.
func TestSpanSetCheckAllowedAtTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var ss SpanSet
	ss.AddMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")}, hlc.Timestamp{WallTime: 2})
	ss.AddMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("g")}, hlc.Timestamp{WallTime: 2})
	ss.AddMVCC(SpanReadWrite, roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("o")}, hlc.Timestamp{WallTime: 2})
	ss.AddMVCC(SpanReadWrite, roachpb.Span{Key: roachpb.Key("s")}, hlc.Timestamp{WallTime: 2})
	ss.AddNonMVCC(SpanReadWrite, roachpb.Span{Key: keys.RangeGCThresholdKey(1)})

	var allowedRO = []struct {
		span roachpb.Span
		ts   hlc.Timestamp
	}{
		// Read access allowed for a subspan or included point at a timestamp
		// equal to or below associated timestamp.
		{roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")}, hlc.Timestamp{WallTime: 2}},
		{roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")}, hlc.Timestamp{WallTime: 1}},
		{roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("o")}, hlc.Timestamp{WallTime: 3}},
		{roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("o")}, hlc.Timestamp{WallTime: 2}},
		{roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("o")}, hlc.Timestamp{WallTime: 1}},
		{roachpb.Span{Key: roachpb.Key("g")}, hlc.Timestamp{WallTime: 2}},
		{roachpb.Span{Key: roachpb.Key("g")}, hlc.Timestamp{WallTime: 1}},
		{roachpb.Span{Key: roachpb.Key("s")}, hlc.Timestamp{WallTime: 3}},
		{roachpb.Span{Key: roachpb.Key("s")}, hlc.Timestamp{WallTime: 2}},
		{roachpb.Span{Key: roachpb.Key("s")}, hlc.Timestamp{WallTime: 1}},

		// Local keys.
		{roachpb.Span{Key: keys.RangeGCThresholdKey(1)}, hlc.Timestamp{}},
		{roachpb.Span{Key: keys.RangeGCThresholdKey(1)}, hlc.Timestamp{WallTime: 1}},
	}
	for _, tc := range allowedRO {
		if err := ss.CheckAllowedAt(SpanReadOnly, tc.span, tc.ts); err != nil {
			t.Errorf("expected %s at %s to be allowed, but got error: %+v", tc.span, tc.ts, err)
		}
	}

	var allowedRW = []struct {
		span roachpb.Span
		ts   hlc.Timestamp
	}{
		// Write access allowed for a subspan or included point at exactly the
		// declared timestamp.
		{roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("o")}, hlc.Timestamp{WallTime: 2}},
		{roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("o")}, hlc.Timestamp{WallTime: 3}},
		{roachpb.Span{Key: roachpb.Key("s")}, hlc.Timestamp{WallTime: 2}},
		{roachpb.Span{Key: roachpb.Key("s")}, hlc.Timestamp{WallTime: 3}},

		// Points within the non-zero-length span.
		{roachpb.Span{Key: roachpb.Key("n")}, hlc.Timestamp{WallTime: 2}},

		// Points within the non-zero-length span at a timestamp higher than what's
		// declared.
		{roachpb.Span{Key: roachpb.Key("n")}, hlc.Timestamp{WallTime: 3}},

		// Sub span at and above the declared timestamp.
		{roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("n")}, hlc.Timestamp{WallTime: 2}},
		{roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("n")}, hlc.Timestamp{WallTime: 3}},

		// Local keys.
		{roachpb.Span{Key: keys.RangeGCThresholdKey(1)}, hlc.Timestamp{}},
	}
	for _, tc := range allowedRW {
		if err := ss.CheckAllowedAt(SpanReadWrite, tc.span, tc.ts); err != nil {
			t.Errorf("expected %s at %s to be allowed, but got error: %+v", tc.span, tc.ts, err)
		}
	}

	readErr := "cannot read undeclared span"
	writeErr := "cannot write undeclared span"

	var disallowedRO = []struct {
		span roachpb.Span
		ts   hlc.Timestamp
	}{
		// Read access disallowed for subspan or included point at timestamp greater
		// than the associated timestamp.
		{roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")}, hlc.Timestamp{WallTime: 3}},
		{roachpb.Span{Key: roachpb.Key("g")}, hlc.Timestamp{WallTime: 3}},
	}
	for _, tc := range disallowedRO {
		if err := ss.CheckAllowedAt(SpanReadOnly, tc.span, tc.ts); !testutils.IsError(err, readErr) {
			t.Errorf("expected %s at %s to be disallowed", tc.span, tc.ts)
		}
	}

	var disallowedRW = []struct {
		span roachpb.Span
		ts   hlc.Timestamp
	}{
		// Write access disallowed for subspan or included point at timestamp
		// less than the associated timestamp.
		{roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("o")}, hlc.Timestamp{WallTime: 1}},
		{roachpb.Span{Key: roachpb.Key("s")}, hlc.Timestamp{WallTime: 1}},

		// Read only spans.
		{roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")}, hlc.Timestamp{WallTime: 2}},
		{roachpb.Span{Key: roachpb.Key("c")}, hlc.Timestamp{WallTime: 2}},

		// Points within the non-zero-length span at a timestamp lower than what's
		// declared.
		{roachpb.Span{Key: roachpb.Key("n")}, hlc.Timestamp{WallTime: 1}},

		// Sub span below the declared timestamp.
		{roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("n")}, hlc.Timestamp{WallTime: 1}},
	}
	for _, tc := range disallowedRW {
		if err := ss.CheckAllowedAt(SpanReadWrite, tc.span, tc.ts); !testutils.IsError(err, writeErr) {
			t.Errorf("expected %s at %s to be disallowed", tc.span, tc.ts)
		}
	}
}

func TestSpanSetCheckAllowedReversed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var bdGkq SpanSet
	bdGkq.AddNonMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")})
	bdGkq.AddNonMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("g")})
	bdGkq.AddNonMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("q")})

	allowed := []roachpb.Span{
		// Exactly as declared.
		{EndKey: roachpb.Key("d")},
		{EndKey: roachpb.Key("q")},
	}
	for _, span := range allowed {
		if err := bdGkq.CheckAllowed(SpanReadOnly, span); err != nil {
			t.Errorf("expected %s to be allowed, but got error: %+v", span, err)
		}
	}

	disallowed := []roachpb.Span{
		// Points outside the declared spans, and on the endpoints.
		{EndKey: roachpb.Key("b")},
		{EndKey: roachpb.Key("g")},
		{EndKey: roachpb.Key("k")},
	}
	for _, span := range disallowed {
		if err := bdGkq.CheckAllowed(SpanReadOnly, span); err == nil {
			t.Errorf("expected %s to be disallowed", span)
		}
	}
}

func TestSpanSetCheckAllowedAtReversed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := hlc.Timestamp{WallTime: 42}
	var bdGkq SpanSet
	bdGkq.AddMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")}, ts)
	bdGkq.AddMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("g")}, ts)
	bdGkq.AddMVCC(SpanReadOnly, roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("q")}, ts)

	allowed := []roachpb.Span{
		// Exactly as declared.
		{EndKey: roachpb.Key("d")},
		{EndKey: roachpb.Key("q")},
	}
	for _, span := range allowed {
		if err := bdGkq.CheckAllowedAt(SpanReadOnly, span, ts); err != nil {
			t.Errorf("expected %s to be allowed, but got error: %+v", span, err)
		}
	}

	disallowed := []roachpb.Span{
		// Points outside the declared spans, and on the endpoints.
		{EndKey: roachpb.Key("b")},
		{EndKey: roachpb.Key("g")},
		{EndKey: roachpb.Key("k")},
	}
	for _, span := range disallowed {
		if err := bdGkq.CheckAllowedAt(SpanReadOnly, span, ts); err == nil {
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
	ss.AddNonMVCC(SpanReadOnly, roSpan)
	ss.AddNonMVCC(SpanReadWrite, rwSpan)

	if err := ss.CheckAllowed(SpanReadOnly, roSpan); err != nil {
		t.Errorf("expected to be allowed to read roSpan, error: %+v", err)
	}
	if err := ss.CheckAllowed(SpanReadWrite, roSpan); err == nil {
		t.Errorf("expected not to be allowed to write roSpan")
	}
	if err := ss.CheckAllowed(SpanReadOnly, rwSpan); err != nil {
		t.Errorf("expected to be allowed to read rwSpan, error: %+v", err)
	}
	if err := ss.CheckAllowed(SpanReadWrite, rwSpan); err != nil {
		t.Errorf("expected to be allowed to read rwSpan, error: %+v", err)
	}
}

// makeSpanHelper accepts strings like: "a-d", and returns a span with
// startKey = a, and endKey = d. It also accepts `X` which represents nil. For
// example, "X-d" returns a span with a nil startKey, and endKey = d.
func makeSpanHelper(t *testing.T, s string) roachpb.Span {
	parts := strings.Split(s, "-")
	require.Len(t, parts, 2)

	var start roachpb.Key
	var end roachpb.Key

	if parts[0] != "X" {
		start = roachpb.Key(parts[0])
	}

	if parts[1] != "X" {
		end = roachpb.Key(parts[1])
	}

	return roachpb.Span{
		Key:    start,
		EndKey: end,
	}
}

// Test that Contains correctly determines if s1 contains s2, including
// support for spans with nil start/end keys.
func TestContains(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		s1       roachpb.Span
		s2       roachpb.Span
		expected bool
	}{
		// s1 equals s2.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "a-c"), expected: true},
		// s1 contains s2.
		{s1: makeSpanHelper(t, "a-d"), s2: makeSpanHelper(t, "b-c"), expected: true},
		// s1 contains point s2.
		{s1: makeSpanHelper(t, "a-d"), s2: makeSpanHelper(t, "a-X"), expected: true},
		// Point s1 contains point s2.
		{s1: makeSpanHelper(t, "a-X"), s2: makeSpanHelper(t, "a-X"), expected: true},
		// s1 contains point s2 with nil startKey.
		{s1: makeSpanHelper(t, "a-d"), s2: makeSpanHelper(t, "X-d"), expected: true},
		// Point s1 contains point s2 with nil startKey.
		{s1: makeSpanHelper(t, "a-X"), s2: roachpb.Span{EndKey: roachpb.Key("a").Next()}, expected: true},
		// s1 does not contain s2.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "d-f"), expected: false},
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "b-d"), expected: false},
		{s1: makeSpanHelper(t, "b-c"), s2: makeSpanHelper(t, "a-d"), expected: false},
		// s1 does not contain s2 point.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "d-X"), expected: false},
		// Point s1 does not contain s2.
		{s1: makeSpanHelper(t, "a-X"), s2: makeSpanHelper(t, "b-d"), expected: false},
		// Point s1 does not contain point s2.
		{s1: makeSpanHelper(t, "a-X"), s2: makeSpanHelper(t, "b-X"), expected: false},
		// s1 does not contain point s2 with nil startKey.
		{s1: makeSpanHelper(t, "a-d"), s2: makeSpanHelper(t, "X-e"), expected: false},
		// Point s1 does not contain point s2 with nil startKey.
		{s1: makeSpanHelper(t, "a-X"), s2: makeSpanHelper(t, "X-b"), expected: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, Contains(tc.s1, TrickySpan(tc.s2)))
		})
	}
}

// Test that Overlaps correctly determines if s1 overlaps s2, including
// support for spans with nil start/end keys.
func TestOverlaps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		s1       roachpb.Span
		s2       roachpb.Span
		expected bool
	}{
		// s1 equals s2.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "a-c"), expected: true},
		// s1 overlaps s2.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "b-d"), expected: true},
		// s1 contains s2.
		{s1: makeSpanHelper(t, "a-d"), s2: makeSpanHelper(t, "b-c"), expected: true},
		// s2 contains s1.
		{s1: makeSpanHelper(t, "b-c"), s2: makeSpanHelper(t, "a-d"), expected: true},
		// s1 overlaps point s2.
		{s1: makeSpanHelper(t, "a-d"), s2: makeSpanHelper(t, "a-X"), expected: true},
		// s1 overlaps point s2 with nil startKey.
		{s1: makeSpanHelper(t, "a-d"), s2: makeSpanHelper(t, "X-d"), expected: true},
		// Point s1 overlaps point s2 with nil startKey.
		{s1: makeSpanHelper(t, "a-X"), s2: roachpb.Span{EndKey: roachpb.Key("a").Next()}, expected: true},
		// Point s1 overlaps point s2.
		{s1: makeSpanHelper(t, "a-X"), s2: makeSpanHelper(t, "a-X"), expected: true},
		// s1 doesn't overlap s2.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "d-f"), expected: false},
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "c-d"), expected: false},
		// s1 doesn't overlap point s2.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "c-X"), expected: false},
		// s1 doesn't overlap point s2 with nil startKey.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "X-a"), expected: false},
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "X-d"), expected: false},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tc.expected, Overlaps(tc.s1, TrickySpan(tc.s2)))
		})
	}
}

// Test that PartiallyOverlaps correctly determines if s1 partially overlaps s2,
// including support for spans with nil start/end keys.
func TestPartiallyOverlaps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		s1       roachpb.Span
		s2       roachpb.Span
		expected bool
	}{
		// s1 equals s2.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "a-c"), expected: false},
		// s1 partially overlaps s2.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "b-d"), expected: true},
		{s1: makeSpanHelper(t, "b-d"), s2: makeSpanHelper(t, "a-c"), expected: true},
		// s1 contains s2.
		{s1: makeSpanHelper(t, "a-d"), s2: makeSpanHelper(t, "b-c"), expected: false},
		// s2 contains s1.
		{s1: makeSpanHelper(t, "b-c"), s2: makeSpanHelper(t, "a-d"), expected: false},
		// s1 contains point s2.
		{s1: makeSpanHelper(t, "a-d"), s2: makeSpanHelper(t, "a-X"), expected: false},
		// s1 contains point s2 with nil startKey.
		{s1: makeSpanHelper(t, "a-d"), s2: makeSpanHelper(t, "X-d"), expected: false},
		// Point s1 contains point s2 with nil startKey.
		{s1: makeSpanHelper(t, "a-X"), s2: roachpb.Span{EndKey: roachpb.Key("a").Next()}, expected: false},
		// Point s1 contains point s2.
		{s1: makeSpanHelper(t, "a-X"), s2: makeSpanHelper(t, "a-X"), expected: false},
		// s1 doesn't overlap s2.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "d-f"), expected: false},
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "c-d"), expected: false},
		// s1 doesn't overlap point s2.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "c-X"), expected: false},
		// s1 doesn't overlap point s2 with nil startKey.
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "X-a"), expected: false},
		{s1: makeSpanHelper(t, "a-c"), s2: makeSpanHelper(t, "X-d"), expected: false},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tc.expected, PartiallyOverlaps(tc.s1, TrickySpan(tc.s2)))
		})
	}
}
