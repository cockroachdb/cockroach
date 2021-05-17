// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// This file implements data structures used by index constraints generation.

package constraint

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

func TestSpanSet(t *testing.T) {
	testCases := []struct {
		start         Key
		startBoundary SpanBoundary
		end           Key
		endBoundary   SpanBoundary
		expected      string
	}{
		{ // 0
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			MakeKey(tree.NewDInt(5)), IncludeBoundary,
			"[/1 - /5]",
		},
		{ // 1
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), IncludeBoundary,
			MakeCompositeKey(tree.NewDString("mango"), tree.NewDInt(1)), ExcludeBoundary,
			"[/'cherry'/5 - /'mango'/1)",
		},
		{ // 2
			MakeCompositeKey(tree.NewDInt(5), tree.NewDInt(1)), ExcludeBoundary,
			MakeKey(tree.NewDInt(5)), IncludeBoundary,
			"(/5/1 - /5]",
		},
		{ // 3
			MakeKey(tree.NewDInt(5)), IncludeBoundary,
			MakeCompositeKey(tree.NewDInt(5), tree.NewDInt(1)), ExcludeBoundary,
			"[/5 - /5/1)",
		},
		{ // 4
			MakeKey(tree.DNull), IncludeBoundary,
			MakeCompositeKey(tree.NewDInt(5), tree.NewDInt(1)), ExcludeBoundary,
			"[/NULL - /5/1)",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			var sp Span
			sp.Init(tc.start, tc.startBoundary, tc.end, tc.endBoundary)
			if sp.String() != tc.expected {
				t.Errorf("expected: %s, actual: %s", tc.expected, sp.String())
			}
		})
	}

	testPanic := func(t *testing.T, fn func(), expected string) {
		t.Helper()
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("panic expected with message: %s", expected)
			} else if fmt.Sprint(r) != expected {
				t.Errorf("expected: %s, actual: %v", expected, r)
			}
		}()
		fn()
	}

	var sp Span
	// Create exclusive empty start boundary.
	testPanic(t, func() {
		sp.Init(EmptyKey, ExcludeBoundary, MakeKey(tree.DNull), IncludeBoundary)
	}, "an empty start boundary must be inclusive")

	// Create exclusive empty end boundary.
	testPanic(t, func() {
		sp.Init(MakeKey(tree.DNull), IncludeBoundary, EmptyKey, ExcludeBoundary)
	}, "an empty end boundary must be inclusive")
}

func TestSpanUnconstrained(t *testing.T) {
	// Test unconstrained span.
	unconstrained := Span{}
	if !unconstrained.IsUnconstrained() {
		t.Errorf("default span is not unconstrained")
	}

	if unconstrained.String() != "[ - ]" {
		t.Errorf("unexpected string value for unconstrained span: %s", unconstrained.String())
	}

	unconstrained.startBoundary = IncludeBoundary
	unconstrained.start = MakeKey(tree.DNull)
	if !unconstrained.IsUnconstrained() {
		t.Errorf("span beginning with NULL is not unconstrained")
	}

	// Test constrained span's IsUnconstrained method.
	var sp Span
	sp.Init(MakeKey(tree.NewDInt(5)), IncludeBoundary, MakeKey(tree.NewDInt(5)), IncludeBoundary)
	if sp.IsUnconstrained() {
		t.Errorf("IsUnconstrained should have returned false")
	}
}

func TestSpanSingleKey(t *testing.T) {
	testCases := []struct {
		start         Key
		startBoundary SpanBoundary
		end           Key
		endBoundary   SpanBoundary
		expected      bool
	}{
		{ // 0
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			true,
		},
		{ // 1
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			MakeKey(tree.NewDInt(2)), IncludeBoundary,
			false,
		},
		{ // 2
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			MakeKey(tree.NewDInt(1)), ExcludeBoundary,
			false,
		},
		{ // 3
			MakeKey(tree.NewDInt(1)), ExcludeBoundary,
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			false,
		},
		{ // 4
			EmptyKey, IncludeBoundary,
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			false,
		},
		{ // 5
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			EmptyKey, IncludeBoundary,
			false,
		},
		{ // 6
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			MakeKey(tree.DNull), IncludeBoundary,
			false,
		},
		{ // 7
			MakeKey(tree.NewDString("a")), IncludeBoundary,
			MakeKey(tree.NewDString("ab")), IncludeBoundary,
			false,
		},
		{ // 8
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(1)), IncludeBoundary,
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(1)), IncludeBoundary,
			true,
		},
		{ // 9
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(1)), IncludeBoundary,
			MakeCompositeKey(tree.NewDString("mango"), tree.NewDInt(1)), IncludeBoundary,
			false,
		},
		{ // 10
			MakeCompositeKey(tree.NewDString("cherry")), IncludeBoundary,
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(1)), IncludeBoundary,
			false,
		},
		{ // 11
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(1), tree.DNull), IncludeBoundary,
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(1), tree.DNull), IncludeBoundary,
			true,
		},
	}

	for i, tc := range testCases {
		st := cluster.MakeTestingClusterSettings()
		evalCtx := tree.MakeTestingEvalContext(st)

		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			var sp Span
			sp.Init(tc.start, tc.startBoundary, tc.end, tc.endBoundary)
			if sp.HasSingleKey(&evalCtx) != tc.expected {
				t.Errorf("expected: %v, actual: %v", tc.expected, !tc.expected)
			}
		})
	}
}

func TestSpanCompare(t *testing.T) {
	keyCtx := testKeyContext(1, 2)

	testComp := func(t *testing.T, left, right Span, expected int) {
		t.Helper()
		if actual := left.Compare(keyCtx, &right); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	one := MakeKey(tree.NewDInt(1))
	two := MakeKey(tree.NewDInt(2))
	oneone := MakeCompositeKey(tree.NewDInt(1), tree.NewDInt(1))
	twoone := MakeCompositeKey(tree.NewDInt(2), tree.NewDInt(1))

	var spans [17]Span

	// [ - /2)
	spans[0].Init(EmptyKey, IncludeBoundary, two, ExcludeBoundary)

	// [ - /2/1)
	spans[1].Init(EmptyKey, IncludeBoundary, twoone, ExcludeBoundary)

	// [ - /2/1]
	spans[2].Init(EmptyKey, IncludeBoundary, twoone, IncludeBoundary)

	// [ - /2]
	spans[3].Init(EmptyKey, IncludeBoundary, two, IncludeBoundary)

	// [ - ]
	spans[4] = Span{}

	// [/1 - /2/1)
	spans[5].Init(one, IncludeBoundary, twoone, ExcludeBoundary)

	// [/1 - /2/1]
	spans[6].Init(one, IncludeBoundary, twoone, IncludeBoundary)

	// [/1 - ]
	spans[7].Init(one, IncludeBoundary, EmptyKey, IncludeBoundary)

	// [/1/1 - /2)
	spans[8].Init(oneone, IncludeBoundary, two, ExcludeBoundary)

	// [/1/1 - /2]
	spans[9].Init(oneone, IncludeBoundary, two, IncludeBoundary)

	// [/1/1 - ]
	spans[10].Init(oneone, IncludeBoundary, EmptyKey, IncludeBoundary)

	// (/1/1 - /2)
	spans[11].Init(oneone, ExcludeBoundary, two, ExcludeBoundary)

	// (/1/1 - /2]
	spans[12].Init(oneone, ExcludeBoundary, two, IncludeBoundary)

	// (/1/1 - ]
	spans[13].Init(oneone, ExcludeBoundary, EmptyKey, IncludeBoundary)

	// (/1 - /2/1)
	spans[14].Init(one, ExcludeBoundary, twoone, ExcludeBoundary)

	// (/1 - /2/1]
	spans[15].Init(one, ExcludeBoundary, twoone, IncludeBoundary)

	// (/1 - ]
	spans[16].Init(one, ExcludeBoundary, EmptyKey, IncludeBoundary)

	for i := 0; i < len(spans)-1; i++ {
		testComp(t, spans[i], spans[i+1], -1)
		testComp(t, spans[i+1], spans[i], 1)
		testComp(t, spans[i], spans[i], 0)
		testComp(t, spans[i+1], spans[i+1], 0)
	}

	keyCtx = testKeyContext(-1, 2)

	// [ - /1)
	spans[0].Init(EmptyKey, IncludeBoundary, one, ExcludeBoundary)

	// [ - /1/1)
	spans[1].Init(EmptyKey, IncludeBoundary, oneone, ExcludeBoundary)

	// [ - /1/1]
	spans[2].Init(EmptyKey, IncludeBoundary, oneone, IncludeBoundary)

	// [ - /1]
	spans[3].Init(EmptyKey, IncludeBoundary, one, IncludeBoundary)

	// [ - ]
	spans[4] = Span{}

	// [/2 - /1/1)
	spans[5].Init(two, IncludeBoundary, oneone, ExcludeBoundary)

	// [/2 - /1/1]
	spans[6].Init(two, IncludeBoundary, oneone, IncludeBoundary)

	// [/2 - ]
	spans[7].Init(two, IncludeBoundary, EmptyKey, IncludeBoundary)

	// [/2/1 - /1)
	spans[8].Init(twoone, IncludeBoundary, one, ExcludeBoundary)

	// [/2/1 - /1]
	spans[9].Init(twoone, IncludeBoundary, one, IncludeBoundary)

	// [/2/1 - ]
	spans[10].Init(twoone, IncludeBoundary, EmptyKey, IncludeBoundary)

	// (/2/1 - /1)
	spans[11].Init(twoone, ExcludeBoundary, one, ExcludeBoundary)

	// (/2/1 - /1]
	spans[12].Init(twoone, ExcludeBoundary, one, IncludeBoundary)

	// (/2/1 - ]
	spans[13].Init(twoone, ExcludeBoundary, EmptyKey, IncludeBoundary)

	// (/2 - /1/1)
	spans[14].Init(two, ExcludeBoundary, oneone, ExcludeBoundary)

	// (/2 - /1/1]
	spans[15].Init(two, ExcludeBoundary, oneone, IncludeBoundary)

	// (/2 - ]
	spans[16].Init(two, ExcludeBoundary, EmptyKey, IncludeBoundary)

	for i := 0; i < len(spans)-1; i++ {
		testComp(t, spans[i], spans[i+1], -1)
		testComp(t, spans[i+1], spans[i], 1)
		testComp(t, spans[i], spans[i], 0)
		testComp(t, spans[i+1], spans[i+1], 0)
	}
}

func TestSpanCompareStarts(t *testing.T) {
	keyCtx := testKeyContext(1, 2)

	test := func(left, right Span, expected int) {
		t.Helper()
		if actual := left.CompareStarts(keyCtx, &right); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	one := MakeKey(tree.NewDInt(1))
	two := MakeKey(tree.NewDInt(2))
	five := MakeKey(tree.NewDInt(5))
	nine := MakeKey(tree.NewDInt(9))

	var onefive Span
	onefive.Init(one, IncludeBoundary, five, IncludeBoundary)
	var twonine Span
	twonine.Init(two, ExcludeBoundary, nine, ExcludeBoundary)

	// Same span.
	test(onefive, onefive, 0)

	// Different spans.
	test(onefive, twonine, -1)
	test(twonine, onefive, 1)
}

func TestSpanCompareEnds(t *testing.T) {
	keyCtx := testKeyContext(1, 2)

	test := func(left, right Span, expected int) {
		t.Helper()
		if actual := left.CompareEnds(keyCtx, &right); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	one := MakeKey(tree.NewDInt(1))
	two := MakeKey(tree.NewDInt(2))
	five := MakeKey(tree.NewDInt(5))
	nine := MakeKey(tree.NewDInt(9))

	var onefive Span
	onefive.Init(one, IncludeBoundary, five, IncludeBoundary)
	var twonine Span
	twonine.Init(two, ExcludeBoundary, nine, ExcludeBoundary)

	// Same span.
	test(onefive, onefive, 0)

	// Different spans.
	test(onefive, twonine, -1)
	test(twonine, onefive, 1)
}

func TestSpanStartsAfter(t *testing.T) {
	keyCtx := testKeyContext(1, 2)

	test := func(left, right Span, expected, expectedStrict bool) {
		t.Helper()
		if actual := left.StartsAfter(keyCtx, &right); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
		if actual := left.StartsStrictlyAfter(keyCtx, &right); actual != expectedStrict {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expectedStrict, actual)
		}
	}

	// Same span.
	var banana Span
	banana.Init(
		MakeCompositeKey(tree.DNull, tree.NewDInt(100)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), IncludeBoundary,
	)
	test(banana, banana, false, false)

	// Right span's start equal to left span's end.
	var cherry Span
	cherry.Init(
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), ExcludeBoundary,
		MakeKey(tree.NewDString("cherry")), ExcludeBoundary,
	)
	test(banana, cherry, false, false)
	test(cherry, banana, true, false)

	// Right span's start greater than left span's end, and inverse.
	var cherry2 Span
	cherry2.Init(
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(0)), IncludeBoundary,
		MakeKey(tree.NewDString("mango")), ExcludeBoundary,
	)
	test(cherry, cherry2, false, false)
	test(cherry2, cherry, true, true)
}

func TestSpanIntersect(t *testing.T) {
	keyCtx := testKeyContext(1, 2)
	testInt := func(left, right Span, expected string) {
		t.Helper()
		sp := left
		ok := sp.TryIntersectWith(keyCtx, &right)

		var actual string
		if ok {
			actual = sp.String()
		}

		if actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	// Same span.
	var banana Span
	banana.Init(
		MakeCompositeKey(tree.DNull, tree.NewDInt(100)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), IncludeBoundary,
	)
	testInt(banana, banana, "[/NULL/100 - /'banana'/50]")

	// One span immediately after the other.
	var grape Span
	grape.Init(
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("grape")), ExcludeBoundary,
	)
	testInt(banana, grape, "")
	testInt(grape, banana, "")

	// Partial overlap.
	var apple Span
	apple.Init(
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(300)), ExcludeBoundary,
	)
	testInt(banana, apple, "(/'apple'/200 - /'banana'/50]")
	testInt(apple, banana, "(/'apple'/200 - /'banana'/50]")

	// One span is subset of other.
	var mango Span
	mango.Init(
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("mango")), ExcludeBoundary,
	)
	testInt(apple, mango, "(/'apple'/200 - /'cherry'/300)")
	testInt(mango, apple, "(/'apple'/200 - /'cherry'/300)")
	testInt(Span{}, mango, "(/'apple'/200 - /'mango')")
	testInt(mango, Span{}, "(/'apple'/200 - /'mango')")

	// Spans are disjoint.
	var pear Span
	pear.Init(
		MakeCompositeKey(tree.NewDString("mango"), tree.NewDInt(0)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("pear"), tree.NewDInt(10)), IncludeBoundary,
	)
	testInt(mango, pear, "")
	testInt(pear, mango, "")

	// Ensure that if TryIntersectWith results in empty set, that it does not
	// update either span.
	mango2 := mango
	pear2 := pear
	mango2.TryIntersectWith(keyCtx, &pear2)
	if mango2.Compare(keyCtx, &mango) != 0 {
		t.Errorf("mango2 was incorrectly updated during TryIntersectWith")
	}
	if pear2.Compare(keyCtx, &pear) != 0 {
		t.Errorf("pear2 was incorrectly updated during TryIntersectWith")
	}

	// Partial overlap on second key.
	pear2.Init(
		MakeCompositeKey(tree.NewDString("pear"), tree.NewDInt(5), tree.DNull), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("raspberry"), tree.NewDInt(100)), IncludeBoundary,
	)
	testInt(pear, pear2, "(/'pear'/5/NULL - /'pear'/10]")
	testInt(pear2, pear, "(/'pear'/5/NULL - /'pear'/10]")

	// Unconstrained (uninitialized) span.
	testInt(banana, Span{}, "[/NULL/100 - /'banana'/50]")
	testInt(Span{}, banana, "[/NULL/100 - /'banana'/50]")
}

func TestSpanUnion(t *testing.T) {
	keyCtx := testKeyContext(1, 2)

	testUnion := func(left, right Span, expected string) {
		t.Helper()
		sp := left
		ok := sp.TryUnionWith(keyCtx, &right)

		var actual string
		if ok {
			actual = sp.String()
		}

		if actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	// Same span.
	var banana Span
	banana.Init(
		MakeCompositeKey(tree.DNull, tree.NewDInt(100)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), IncludeBoundary,
	)
	testUnion(banana, banana, "[/NULL/100 - /'banana'/50]")

	// Partial overlap.
	var apple Span
	apple.Init(
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(300)), ExcludeBoundary,
	)
	testUnion(banana, apple, "[/NULL/100 - /'cherry'/300)")
	testUnion(apple, banana, "[/NULL/100 - /'cherry'/300)")

	// One span is subset of other.
	var mango Span
	mango.Init(
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("mango")), ExcludeBoundary,
	)
	testUnion(apple, mango, "(/'apple'/200 - /'mango')")
	testUnion(mango, apple, "(/'apple'/200 - /'mango')")
	testUnion(Span{}, mango, "[ - ]")
	testUnion(mango, Span{}, "[ - ]")

	// Spans are disjoint.
	var pear Span
	pear.Init(
		MakeCompositeKey(tree.NewDString("mango"), tree.NewDInt(0)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("pear"), tree.NewDInt(10)), IncludeBoundary,
	)
	testUnion(mango, pear, "")
	testUnion(pear, mango, "")

	// Ensure that if TryUnionWith fails to merge, that it does not update
	// either span.
	mango2 := mango
	pear2 := pear
	mango2.TryUnionWith(keyCtx, &pear2)
	if mango2.Compare(keyCtx, &mango) != 0 {
		t.Errorf("mango2 was incorrectly updated during TryUnionWith")
	}
	if pear2.Compare(keyCtx, &pear) != 0 {
		t.Errorf("pear2 was incorrectly updated during TryUnionWith")
	}

	// Partial overlap on second key.
	pear2.Init(
		MakeCompositeKey(tree.NewDString("pear"), tree.NewDInt(5), tree.DNull), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("raspberry"), tree.NewDInt(100)), IncludeBoundary,
	)
	testUnion(pear, pear2, "[/'mango'/0 - /'raspberry'/100]")
	testUnion(pear, pear2, "[/'mango'/0 - /'raspberry'/100]")

	// Unconstrained (uninitialized) span.
	testUnion(banana, Span{}, "[ - ]")
	testUnion(Span{}, banana, "[ - ]")
}

func TestSpanPreferInclusive(t *testing.T) {
	keyCtx := testKeyContext(1, 2)

	testCases := []struct {
		start         Key
		startBoundary SpanBoundary
		end           Key
		endBoundary   SpanBoundary
		expected      string
	}{
		{ // 0
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			MakeKey(tree.NewDInt(5)), IncludeBoundary,
			"[/1 - /5]",
		},
		{ // 1
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			MakeKey(tree.NewDInt(5)), ExcludeBoundary,
			"[/1 - /4]",
		},
		{ // 2
			MakeKey(tree.NewDInt(1)), ExcludeBoundary,
			MakeKey(tree.NewDInt(5)), IncludeBoundary,
			"[/2 - /5]",
		},
		{ // 3
			MakeKey(tree.NewDInt(1)), ExcludeBoundary,
			MakeKey(tree.NewDInt(5)), ExcludeBoundary,
			"[/2 - /4]",
		},
		{ // 4
			MakeCompositeKey(tree.NewDInt(1), tree.NewDInt(math.MaxInt64)), ExcludeBoundary,
			MakeCompositeKey(tree.NewDInt(2), tree.NewDInt(math.MinInt64)), ExcludeBoundary,
			"(/1/9223372036854775807 - /2/-9223372036854775808)",
		},
		{ // 5
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), ExcludeBoundary,
			MakeCompositeKey(tree.NewDString("mango"), tree.NewDInt(1)), ExcludeBoundary,
			"[/'cherry'/6 - /'mango'/0]",
		},
		{ // 6
			MakeCompositeKey(tree.NewDInt(1), tree.NewDString("cherry")), ExcludeBoundary,
			MakeCompositeKey(tree.NewDInt(2), tree.NewDString("mango")), ExcludeBoundary,
			"[/1/e'cherry\\x00' - /2/'mango')",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			var sp Span
			sp.Init(tc.start, tc.startBoundary, tc.end, tc.endBoundary)
			sp.PreferInclusive(keyCtx)
			if sp.String() != tc.expected {
				t.Errorf("expected: %s, actual: %s", tc.expected, sp.String())
			}
		})
	}
}

func TestSpan_KeyCount(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	kcAscAsc := testKeyContext(1, 2)
	kcDescDesc := testKeyContext(-1, -2)
	enums := makeEnums(t)

	testCases := []struct {
		keyCtx   *KeyContext
		length   int
		span     Span
		expected string
	}{
		{ // 0
			// Single key span with DString datum type.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/US_WEST - /US_WEST]"),
			expected: "1",
		},
		{ // 1
			// Multiple key span with DInt datum type.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/-5 - /5]"),
			expected: "11",
		},
		{ // 2
			// Multiple key span with DOid datum type.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/-5 - /5]", types.OidFamily),
			expected: "11",
		},
		{ // 3
			// Multiple key span with DDate datum type.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/2000-1-1 - /2000-1-2]", types.DateFamily),
			expected: "2",
		},
		{ // 4
			// Single-key span with multiple-column key.
			keyCtx:   kcAscAsc,
			length:   2,
			span:     ParseSpan(&evalCtx, "[/US_WEST/item - /US_WEST/item]"),
			expected: "1",
		},
		{ // 5
			// Fails because the span is multiple-key and the type is not enumerable.
			keyCtx:   kcAscAsc,
			length:   2,
			span:     ParseSpan(&evalCtx, "[/US_WEST/item - /US_WEST/object]"),
			expected: "FAIL",
		},
		{ // 6
			// Descending multiple-key span.
			keyCtx:   kcDescDesc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/5 - /-5]"),
			expected: "11",
		},
		{ // 7
			// Descending multiple-key span with multiple-column keys.
			keyCtx:   kcDescDesc,
			length:   2,
			span:     ParseSpan(&evalCtx, "[/US_WEST/5 - /US_WEST/-5]"),
			expected: "11",
		},
		{ // 8
			// Fails because the keys can only differ in the last column.
			keyCtx:   kcAscAsc,
			length:   2,
			span:     ParseSpan(&evalCtx, "[/US_WEST/1 - /US_EAST/1]"),
			expected: "FAIL",
		},
		{ // 9
			// Fails because both keys must be at least as long as the given length.
			keyCtx:   kcAscAsc,
			length:   2,
			span:     ParseSpan(&evalCtx, "[/1/1 - /1]"),
			expected: "FAIL",
		},
		{ // 10
			// Fails because both keys must be at least as long as the given length.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/1 - ]"),
			expected: "FAIL",
		},
		{ // 11
			// Fails because the given prefix length must be larger than zero.
			keyCtx:   kcAscAsc,
			length:   0,
			span:     ParseSpan(&evalCtx, "[/1 - ]"),
			expected: "FAIL",
		},
		{ // 12
			// Case with postfix values beyond the given prefix length. Key count is
			// calculated only between the prefixes; postfixes are ignored.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/1/post - /5/fix]"),
			expected: "5",
		},
		{ // 13
			// Case with postfix for the start key, but not the end key.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/1/post - /5]"),
			expected: "5",
		},
		{ // 14
			// Case with postfix for the end key, but not the start key.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/1 - /5/fix]"),
			expected: "5",
		},
		{ // 15
			// Fails because of overflow.
			keyCtx: kcAscAsc,
			length: 1,
			span: Span{
				start:         MakeKey(tree.NewDInt(math.MinInt64)),
				end:           MakeKey(tree.NewDInt(math.MaxInt64)),
				startBoundary: IncludeBoundary,
				endBoundary:   IncludeBoundary,
			},
			expected: "FAIL",
		},
		{ // 16
			// Fails because of underflow.
			keyCtx: kcDescDesc,
			length: 1,
			span: Span{
				start:         MakeKey(tree.NewDInt(math.MaxInt64)),
				end:           MakeKey(tree.NewDInt(math.MinInt64)),
				startBoundary: IncludeBoundary,
				endBoundary:   IncludeBoundary,
			},
			expected: "FAIL",
		},
		{ // 17
			// Test enums.
			keyCtx: kcAscAsc,
			length: 1,
			span: Span{
				start:         MakeKey(enums[0]),
				end:           MakeKey(enums[1]),
				startBoundary: IncludeBoundary,
				endBoundary:   IncludeBoundary,
			},
			expected: "2",
		},
		{ // 18
			// Test enums.
			keyCtx: kcAscAsc,
			length: 1,
			span: Span{
				start:         MakeKey(enums[0]),
				end:           MakeKey(enums[2]),
				startBoundary: IncludeBoundary,
				endBoundary:   IncludeBoundary,
			},
			expected: "3",
		},
		{ // 19
			// Allow exclusive boundaries if the key is longer than the prefix.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "(/US_WEST/post - /US_WEST]"),
			expected: "1",
		},
		{ // 20
			// Allow exclusive boundaries if the key is longer than the prefix.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/1 - /2/fix)"),
			expected: "2",
		},
		{ // 21
			// Fails since the key is the same length as the prefix and the boundary
			// is exclusive.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "(/US_WEST - /US_WEST/fix]"),
			expected: "FAIL",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			toStr := func(cnt int64, ok bool) string {
				if !ok {
					return "FAIL"
				}
				return strconv.FormatInt(cnt, 10 /* base */)
			}

			if res := toStr(tc.span.KeyCount(tc.keyCtx, tc.length)); res != tc.expected {
				t.Errorf("expected: %s, actual: %s", tc.expected, res)
			}
		})
	}
}

func TestSpan_SplitSpan(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	kcAscAsc := testKeyContext(1, 2)
	kcDescDesc := testKeyContext(-1, -2)
	enums := makeEnums(t)

	testCases := []struct {
		keyCtx   *KeyContext
		length   int
		span     Span
		expected string
	}{
		{ // 0
			// Single-key span with multiple-column key.
			keyCtx:   kcAscAsc,
			length:   2,
			span:     ParseSpan(&evalCtx, "[/US_WEST/item - /US_WEST/item]"),
			expected: "[/'US_WEST'/'item' - /'US_WEST'/'item']",
		},
		{ // 1
			// Fails because the datum type is not enumerable.
			keyCtx:   kcAscAsc,
			length:   2,
			span:     ParseSpan(&evalCtx, "[/US_WEST/item - /US_WEST/object]"),
			expected: "FAIL",
		},
		{ // 2
			// Fails because only the last datums can differ, and only if they are
			// enumerable.
			keyCtx:   kcAscAsc,
			length:   2,
			span:     ParseSpan(&evalCtx, "[/US_EAST/item - /US_WEST/item]"),
			expected: "FAIL",
		},
		{ // 3
			// Ascending multiple-key span.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/-1 - /1]"),
			expected: "[/-1 - /-1] [/0 - /0] [/1 - /1]",
		},
		{ // 4
			// Descending multiple-key span.
			keyCtx:   kcDescDesc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/1 - /-1]"),
			expected: "[/1 - /1] [/0 - /0] [/-1 - /-1]",
		},
		{ // 5
			// Ascending multiple-key span with multiple-column keys.
			keyCtx: kcAscAsc,
			length: 2,
			span:   ParseSpan(&evalCtx, "[/US_WEST/-1 - /US_WEST/1]"),
			expected: "[/'US_WEST'/-1 - /'US_WEST'/-1] [/'US_WEST'/0 - /'US_WEST'/0] " +
				"[/'US_WEST'/1 - /'US_WEST'/1]",
		},
		{ // 6
			// Fails because the keys are different lengths.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[ - /'US_WEST']"),
			expected: "FAIL",
		},
		{ // 7
			// Single span with 10 keys (equal to maxKeyCount).
			keyCtx: kcAscAsc,
			length: 1,
			span: ParseSpan(
				&evalCtx,
				"[/0 - /9]",
			),
			expected: "[/0 - /0] [/1 - /1] [/2 - /2] [/3 - /3] [/4 - /4] [/5 - /5] [/6 - /6] [/7 - /7] " +
				"[/8 - /8] [/9 - /9]",
		},
		{ // 8
			// Postfix values beyond the given prefix length. Postfixes are applied to
			// the start key of the first Span, and the end key of the last Span.
			keyCtx: kcAscAsc,
			length: 1,
			span:   ParseSpan(&evalCtx, "[/-1/post - /5/fix]"),
			expected: "[/-1/'post' - /-1] [/0 - /0] [/1 - /1] [/2 - /2] " +
				"[/3 - /3] [/4 - /4] [/5 - /5/'fix']",
		},
		{ // 9
			// Postfix for start key, but not end key.
			keyCtx: kcAscAsc,
			length: 1,
			span:   ParseSpan(&evalCtx, "[/-1/post/fix - /5]"),
			expected: "[/-1/'post'/'fix' - /-1] [/0 - /0] [/1 - /1] [/2 - /2] " +
				"[/3 - /3] [/4 - /4] [/5 - /5]",
		},
		{ // 10
			// Postfix for end key, but not start key.
			keyCtx: kcAscAsc,
			length: 1,
			span:   ParseSpan(&evalCtx, "[/-1 - /5/post/fix]"),
			expected: "[/-1 - /-1] [/0 - /0] [/1 - /1] [/2 - /2] [/3 - /3] " +
				"[/4 - /4] [/5 - /5/'post'/'fix']",
		},
		{ // 11
			// Fails because prefix length is zero.
			keyCtx:   kcAscAsc,
			length:   0,
			span:     ParseSpan(&evalCtx, "[/-1 - /5]"),
			expected: "FAIL",
		},
		{ // 12
			// Fails because the end key is not as long as the given prefix length.
			keyCtx:   kcAscAsc,
			length:   2,
			span:     ParseSpan(&evalCtx, "[/-1/1 - /5]"),
			expected: "FAIL",
		},
		{ // 13
			// Test enums.
			keyCtx: kcAscAsc,
			length: 1,
			span: Span{
				start:         MakeKey(enums[0]),
				end:           MakeKey(enums[1]),
				startBoundary: IncludeBoundary,
				endBoundary:   IncludeBoundary,
			},
			expected: "[/'hello' - /'hello'] [/'hey' - /'hey']",
		},
		{ // 14
			// Test enums.
			keyCtx: kcAscAsc,
			length: 1,
			span: Span{
				start:         MakeKey(enums[0]),
				end:           MakeKey(enums[2]),
				startBoundary: IncludeBoundary,
				endBoundary:   IncludeBoundary,
			},
			expected: "[/'hello' - /'hello'] [/'hey' - /'hey'] [/'hi' - /'hi']",
		},
		{ // 15
			// Allow exclusive boundaries if the key is longer than the prefix.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "(/1/post - /2]"),
			expected: "(/1/'post' - /1] [/2 - /2]",
		},
		{ // 16
			// Allow exclusive boundaries if the key is longer than the prefix.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "[/1 - /2/fix)"),
			expected: "[/1 - /1] [/2 - /2/'fix')",
		},
		{ // 17
			// Fails since the key is the same length as the prefix and the boundary
			// is exclusive.
			keyCtx:   kcAscAsc,
			length:   1,
			span:     ParseSpan(&evalCtx, "(/US_WEST - /US_WEST/fix]"),
			expected: "FAIL",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			toStr := func(spans *Spans, ok bool) string {
				if !ok {
					return "FAIL"
				}
				return spans.String()
			}

			if res := toStr(tc.span.Split(tc.keyCtx, tc.length)); res != tc.expected {
				t.Errorf("expected: %s, actual: %s", tc.expected, res)
			}
		})
	}
}

func makeEnums(t *testing.T) tree.Datums {
	t.Helper()
	enumMembers := []string{"hello", "hey", "hi"}
	enumType := types.MakeEnum(typedesc.TypeIDToOID(500), typedesc.TypeIDToOID(100500))
	enumType.TypeMeta = types.UserDefinedTypeMetadata{
		Name: &types.UserDefinedTypeName{
			Schema: "test",
			Name:   "greeting",
		},
		EnumData: &types.EnumMetadata{
			LogicalRepresentations: enumMembers,
			PhysicalRepresentations: [][]byte{
				encoding.EncodeUntaggedIntValue(nil, 0),
				encoding.EncodeUntaggedIntValue(nil, 1),
				encoding.EncodeUntaggedIntValue(nil, 2),
			},
			IsMemberReadOnly: make([]bool, len(enumMembers)),
		},
	}
	enumHello, err := tree.MakeDEnumFromLogicalRepresentation(enumType, enumMembers[0])
	if err != nil {
		t.Fatal(err)
	}
	enumHey, err := tree.MakeDEnumFromLogicalRepresentation(enumType, enumMembers[1])
	if err != nil {
		t.Fatal(err)
	}
	enumHi, err := tree.MakeDEnumFromLogicalRepresentation(enumType, enumMembers[2])
	if err != nil {
		t.Fatal(err)
	}
	return tree.Datums{enumHello, enumHey, enumHi}
}
