// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// This file implements data structures used by index constraints generation.

package constraint

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	}

	keyCtx := testKeyContext()

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			var sp Span
			sp.Set(keyCtx, tc.start, tc.startBoundary, tc.end, tc.endBoundary)
			if sp.String() != tc.expected {
				t.Errorf("expected: %s, actual: %s", tc.expected, sp.String())
			}
		})
	}

	testPanic := func(t *testing.T, fn func(), expected string) {
		t.Helper()
		defer func() {
			msg := recover()
			if msg == nil {
				t.Errorf("panic expected with message: %s", expected)
			} else if msg != expected {
				t.Errorf("expected: %s, actual: %s", expected, msg)
			}
		}()
		fn()
	}

	var sp Span
	// Try to create unconstrained span.
	testPanic(t, func() {
		sp.Set(keyCtx, EmptyKey, IncludeBoundary, EmptyKey, IncludeBoundary)
	}, "unconstrained span should never be used")

	// Create exclusive empty start boundary.
	testPanic(t, func() {
		sp.Set(keyCtx, EmptyKey, ExcludeBoundary, MakeKey(tree.DNull), IncludeBoundary)
	}, "an empty start boundary must be inclusive")

	// Create exclusive empty end boundary.
	testPanic(t, func() {
		sp.Set(keyCtx, MakeKey(tree.DNull), IncludeBoundary, EmptyKey, ExcludeBoundary)
	}, "an empty end boundary must be inclusive")

	// Try to create zero-length spans.
	testPanic(t, func() {
		sp.Set(
			keyCtx,
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			MakeKey(tree.NewDInt(1)), ExcludeBoundary,
		)
	}, "span cannot be empty")

	testPanic(t, func() {
		sp.Set(
			keyCtx,
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), ExcludeBoundary,
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), IncludeBoundary,
		)
	}, "span cannot be empty")

	// Try to create spans where start boundary > end boundary.
	testPanic(t, func() {
		sp.Set(keyCtx, MakeKey(tree.NewDInt(0)), IncludeBoundary, MakeKey(tree.DNull), ExcludeBoundary)
	}, "span cannot be empty")

	testPanic(t, func() {
		sp.Set(
			keyCtx,
			MakeCompositeKey(tree.NewDString("mango"), tree.NewDInt(1)), IncludeBoundary,
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), IncludeBoundary,
		)
	}, "span cannot be empty")
}

func TestSpanUnconstrained(t *testing.T) {
	keyCtx := testKeyContext()

	// Test unconstrained span.
	unconstrained := Span{}
	if !unconstrained.IsUnconstrained() {
		t.Errorf("default span is not unconstrained")
	}

	if unconstrained.String() != "[ - ]" {
		t.Errorf("unexpected string value for unconstrained span: %s", unconstrained.String())
	}

	// Test constrained span's IsUnconstrained method.
	var sp Span
	sp.Set(
		keyCtx,
		MakeKey(tree.NewDInt(5)), IncludeBoundary,
		MakeKey(tree.NewDInt(5)), IncludeBoundary,
	)
	if sp.IsUnconstrained() {
		t.Errorf("IsUnconstrained should have returned false")
	}
}

func TestSpanCompare(t *testing.T) {
	keyCtx := testKeyContext()

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
	spans[0].Set(keyCtx, EmptyKey, IncludeBoundary, two, ExcludeBoundary)

	// [ - /2/1)
	spans[1].Set(keyCtx, EmptyKey, IncludeBoundary, twoone, ExcludeBoundary)

	// [ - /2/1]
	spans[2].Set(keyCtx, EmptyKey, IncludeBoundary, twoone, IncludeBoundary)

	// [ - /2]
	spans[3].Set(keyCtx, EmptyKey, IncludeBoundary, two, IncludeBoundary)

	// [ - ]
	spans[4] = Span{}

	// [/1 - /2/1)
	spans[5].Set(keyCtx, one, IncludeBoundary, twoone, ExcludeBoundary)

	// [/1 - /2/1]
	spans[6].Set(keyCtx, one, IncludeBoundary, twoone, IncludeBoundary)

	// [/1 - ]
	spans[7].Set(keyCtx, one, IncludeBoundary, EmptyKey, IncludeBoundary)

	// [/1/1 - /2)
	spans[8].Set(keyCtx, oneone, IncludeBoundary, two, ExcludeBoundary)

	// [/1/1 - /2]
	spans[9].Set(keyCtx, oneone, IncludeBoundary, two, IncludeBoundary)

	// [/1/1 - ]
	spans[10].Set(keyCtx, oneone, IncludeBoundary, EmptyKey, IncludeBoundary)

	// (/1/1 - /2)
	spans[11].Set(keyCtx, oneone, ExcludeBoundary, two, ExcludeBoundary)

	// (/1/1 - /2]
	spans[12].Set(keyCtx, oneone, ExcludeBoundary, two, IncludeBoundary)

	// (/1/1 - ]
	spans[13].Set(keyCtx, oneone, ExcludeBoundary, EmptyKey, IncludeBoundary)

	// (/1 - /2/1)
	spans[14].Set(keyCtx, one, ExcludeBoundary, twoone, ExcludeBoundary)

	// (/1 - /2/1]
	spans[15].Set(keyCtx, one, ExcludeBoundary, twoone, IncludeBoundary)

	// (/1 - ]
	spans[16].Set(keyCtx, one, ExcludeBoundary, EmptyKey, IncludeBoundary)

	for i := 0; i < len(spans)-1; i++ {
		testComp(t, spans[i], spans[i+1], -1)
		testComp(t, spans[i+1], spans[i], 1)
		testComp(t, spans[i], spans[i], 0)
		testComp(t, spans[i+1], spans[i+1], 0)
	}

	keyCtx.Columns.firstCol = -keyCtx.Columns.firstCol

	// [ - /1)
	spans[0].Set(keyCtx, EmptyKey, IncludeBoundary, one, ExcludeBoundary)

	// [ - /1/1)
	spans[1].Set(keyCtx, EmptyKey, IncludeBoundary, oneone, ExcludeBoundary)

	// [ - /1/1]
	spans[2].Set(keyCtx, EmptyKey, IncludeBoundary, oneone, IncludeBoundary)

	// [ - /1]
	spans[3].Set(keyCtx, EmptyKey, IncludeBoundary, one, IncludeBoundary)

	// [ - ]
	spans[4] = Span{}

	// [/2 - /1/1)
	spans[5].Set(keyCtx, two, IncludeBoundary, oneone, ExcludeBoundary)

	// [/2 - /1/1]
	spans[6].Set(keyCtx, two, IncludeBoundary, oneone, IncludeBoundary)

	// [/2 - ]
	spans[7].Set(keyCtx, two, IncludeBoundary, EmptyKey, IncludeBoundary)

	// [/2/1 - /1)
	spans[8].Set(keyCtx, twoone, IncludeBoundary, one, ExcludeBoundary)

	// [/2/1 - /1]
	spans[9].Set(keyCtx, twoone, IncludeBoundary, one, IncludeBoundary)

	// [/2/1 - ]
	spans[10].Set(keyCtx, twoone, IncludeBoundary, EmptyKey, IncludeBoundary)

	// (/2/1 - /1)
	spans[11].Set(keyCtx, twoone, ExcludeBoundary, one, ExcludeBoundary)

	// (/2/1 - /1]
	spans[12].Set(keyCtx, twoone, ExcludeBoundary, one, IncludeBoundary)

	// (/2/1 - ]
	spans[13].Set(keyCtx, twoone, ExcludeBoundary, EmptyKey, IncludeBoundary)

	// (/2 - /1/1)
	spans[14].Set(keyCtx, two, ExcludeBoundary, oneone, ExcludeBoundary)

	// (/2 - /1/1]
	spans[15].Set(keyCtx, two, ExcludeBoundary, oneone, IncludeBoundary)

	// (/2 - ]
	spans[16].Set(keyCtx, two, ExcludeBoundary, EmptyKey, IncludeBoundary)

	for i := 0; i < len(spans)-1; i++ {
		testComp(t, spans[i], spans[i+1], -1)
		testComp(t, spans[i+1], spans[i], 1)
		testComp(t, spans[i], spans[i], 0)
		testComp(t, spans[i+1], spans[i+1], 0)
	}
}

func TestSpanCompareStarts(t *testing.T) {
	keyCtx := testKeyContext()

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
	onefive.Set(keyCtx, one, IncludeBoundary, five, IncludeBoundary)
	var twonine Span
	twonine.Set(keyCtx, two, ExcludeBoundary, nine, ExcludeBoundary)

	// Same span.
	test(onefive, onefive, 0)

	// Different spans.
	test(onefive, twonine, -1)
	test(twonine, onefive, 1)
}

func TestSpanCompareEnds(t *testing.T) {
	keyCtx := testKeyContext()

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
	onefive.Set(keyCtx, one, IncludeBoundary, five, IncludeBoundary)
	var twonine Span
	twonine.Set(keyCtx, two, ExcludeBoundary, nine, ExcludeBoundary)

	// Same span.
	test(onefive, onefive, 0)

	// Different spans.
	test(onefive, twonine, -1)
	test(twonine, onefive, 1)
}

func TestSpanStartsAfter(t *testing.T) {
	keyCtx := testKeyContext()

	test := func(left, right Span, expected bool) {
		t.Helper()
		if actual := left.StartsAfter(keyCtx, &right); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	// Same span.
	var banana Span
	banana.Set(
		keyCtx,
		MakeCompositeKey(tree.DNull, tree.NewDInt(100)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), IncludeBoundary,
	)
	test(banana, banana, false)

	// Right span's start equal to left span's end.
	var cherry Span
	cherry.Set(
		keyCtx,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), ExcludeBoundary,
		MakeKey(tree.NewDString("cherry")), ExcludeBoundary,
	)
	test(banana, cherry, false)
	test(cherry, banana, true)

	// Right span's start greater than left span's end, and inverse.
	var cherry2 Span
	cherry2.Set(
		keyCtx,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(0)), IncludeBoundary,
		MakeKey(tree.NewDString("mango")), ExcludeBoundary,
	)
	test(cherry, cherry2, false)
	test(cherry2, cherry, true)
}

func TestSpanIntersect(t *testing.T) {
	keyCtx := testKeyContext()
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
	banana.Set(
		keyCtx,
		MakeCompositeKey(tree.DNull, tree.NewDInt(100)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), IncludeBoundary,
	)
	testInt(banana, banana, "[/NULL/100 - /'banana'/50]")

	// One span immediately after the other.
	var grape Span
	grape.Set(
		keyCtx,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("grape")), ExcludeBoundary,
	)
	testInt(banana, grape, "")
	testInt(grape, banana, "")

	// Partial overlap.
	var apple Span
	apple.Set(
		keyCtx,
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(300)), ExcludeBoundary,
	)
	testInt(banana, apple, "(/'apple'/200 - /'banana'/50]")
	testInt(apple, banana, "(/'apple'/200 - /'banana'/50]")

	// One span is subset of other.
	var mango Span
	mango.Set(
		keyCtx,
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("mango")), ExcludeBoundary,
	)
	testInt(apple, mango, "(/'apple'/200 - /'cherry'/300)")
	testInt(mango, apple, "(/'apple'/200 - /'cherry'/300)")
	testInt(Span{}, mango, "(/'apple'/200 - /'mango')")
	testInt(mango, Span{}, "(/'apple'/200 - /'mango')")

	// Spans are disjoint.
	var pear Span
	pear.Set(
		keyCtx,
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
	pear2.Set(
		keyCtx,
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
	keyCtx := testKeyContext()

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
	banana.Set(
		keyCtx,
		MakeCompositeKey(tree.DNull, tree.NewDInt(100)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), IncludeBoundary,
	)
	testUnion(banana, banana, "[/NULL/100 - /'banana'/50]")

	// Partial overlap.
	var apple Span
	apple.Set(
		keyCtx,
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(300)), ExcludeBoundary,
	)
	testUnion(banana, apple, "[/NULL/100 - /'cherry'/300)")
	testUnion(apple, banana, "[/NULL/100 - /'cherry'/300)")

	// One span is subset of other.
	var mango Span
	mango.Set(
		keyCtx,
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("mango")), ExcludeBoundary,
	)
	testUnion(apple, mango, "(/'apple'/200 - /'mango')")
	testUnion(mango, apple, "(/'apple'/200 - /'mango')")
	testUnion(Span{}, mango, "[ - ]")
	testUnion(mango, Span{}, "[ - ]")

	// Spans are disjoint.
	var pear Span
	pear.Set(
		keyCtx,
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
	pear2.Set(
		keyCtx,
		MakeCompositeKey(tree.NewDString("pear"), tree.NewDInt(5), tree.DNull), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("raspberry"), tree.NewDInt(100)), IncludeBoundary,
	)
	testUnion(pear, pear2, "[/'mango'/0 - /'raspberry'/100]")
	testUnion(pear, pear2, "[/'mango'/0 - /'raspberry'/100]")

	// Unconstrained (uninitialized) span.
	testUnion(banana, Span{}, "[ - ]")
	testUnion(Span{}, banana, "[ - ]")
}
