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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestSpanSet(t *testing.T) {
	test := func(t *testing.T, sp Span, expected string) {
		t.Helper()
		if sp.String() != expected {
			t.Errorf("expected: %s, actual: %s", expected, sp.String())
		}
	}

	testInvalid := func(t *testing.T, ok bool) {
		t.Helper()
		if ok {
			t.Errorf("expected TrySet to return false, but returned true")
		}
	}

	// Test string representations of built-in spans.
	test(t, EmptySpan, "[Ø - Ø)")
	test(t, FullSpan, "[Ø - ∞]")

	evalCtx := tree.MakeTestingEvalContext()

	var sp Span
	_ = sp.TrySet(
		&evalCtx,
		MakeKey(tree.NewDInt(1)), IncludeBoundary,
		MakeKey(tree.NewDInt(5)), IncludeBoundary,
	)
	test(t, sp, "[/1 - /5]")

	_ = sp.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("mango"), tree.NewDInt(1)), ExcludeBoundary,
	)
	test(t, sp, "[/'cherry'/5 - /'mango'/1)")

	_ = sp.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDInt(5), tree.NewDInt(1)), ExcludeBoundary,
		MakeKey(tree.NewDInt(5)), IncludeBoundary,
	)
	test(t, sp, "(/5/1 - /5]")

	_ = sp.TrySet(
		&evalCtx,
		MakeKey(tree.NewDInt(5)), IncludeBoundary,
		MakeCompositeKey(tree.NewDInt(5), tree.NewDInt(1)), ExcludeBoundary,
	)
	test(t, sp, "[/5 - /5/1)")

	// Create full span.
	_ = sp.TrySet(&evalCtx, EmptyKey, IncludeBoundary, EmptyKey, IncludeBoundary)
	test(t, sp, "[Ø - ∞]")

	// Try to create zero-length spans (should return false).
	ok := sp.TrySet(&evalCtx, EmptyKey, IncludeBoundary, EmptyKey, ExcludeBoundary)
	testInvalid(t, ok)

	ok = sp.TrySet(&evalCtx, EmptyKey, ExcludeBoundary, EmptyKey, IncludeBoundary)
	testInvalid(t, ok)

	ok = sp.TrySet(&evalCtx, EmptyKey, ExcludeBoundary, EmptyKey, ExcludeBoundary)
	testInvalid(t, ok)

	ok = sp.TrySet(
		&evalCtx,
		MakeKey(tree.NewDInt(1)), IncludeBoundary,
		MakeKey(tree.NewDInt(1)), ExcludeBoundary,
	)
	testInvalid(t, ok)

	ok = sp.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), IncludeBoundary,
	)
	testInvalid(t, ok)

	// Try to create spans where start boundary < end boundary (should return
	// false).
	ok = sp.TrySet(&evalCtx, EmptyKey, ExcludeBoundary, MakeKey(tree.DNull), IncludeBoundary)
	testInvalid(t, ok)

	ok = sp.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("mango"), tree.NewDInt(1)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), IncludeBoundary,
	)
	testInvalid(t, ok)

	// Ensure that failed set does not update the span.
	sp = Span{}
	ok = sp.TrySet(
		&evalCtx,
		MakeKey(tree.NewDInt(1)), ExcludeBoundary,
		MakeKey(tree.NewDInt(1)), ExcludeBoundary,
	)
	testInvalid(t, ok)
	if !sp.IsEmpty() {
		t.Errorf("TrySet modified the span even though it failed")
	}
}

func TestSpanIsEmpty(t *testing.T) {
	test := func(t *testing.T, sp Span, expected bool) {
		t.Helper()
		if actual := sp.IsEmpty(); actual != expected {
			t.Errorf("span: %s, expected: %v, actual: %v", sp.String(), expected, actual)
		}
	}

	evalCtx := tree.MakeTestingEvalContext()

	test(t, EmptySpan, true)
	test(t, FullSpan, false)

	var sp Span
	_ = sp.TrySet(
		&evalCtx,
		MakeKey(tree.NewDInt(1)), IncludeBoundary,
		MakeKey(tree.NewDInt(2)), ExcludeBoundary,
	)
	test(t, sp, false)
}

func TestSpanIsFull(t *testing.T) {
	test := func(t *testing.T, sp Span, expected bool) {
		t.Helper()
		if actual := sp.IsFull(); actual != expected {
			t.Errorf("span: %s, expected: %v, actual: %v", sp.String(), expected, actual)
		}
	}

	evalCtx := tree.MakeTestingEvalContext()

	test(t, EmptySpan, false)
	test(t, FullSpan, true)

	var sp Span
	_ = sp.TrySet(
		&evalCtx,
		MakeKey(tree.NewDInt(1)), IncludeBoundary,
		MakeKey(tree.NewDInt(2)), ExcludeBoundary,
	)
	test(t, sp, false)
}

func TestSpanCompare(t *testing.T) {
	testComp := func(t *testing.T, evalCtx *tree.EvalContext, left, right Span, expected int) {
		t.Helper()
		if actual := left.Compare(evalCtx, &right); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	evalCtx := tree.MakeTestingEvalContext()

	testComp(t, &evalCtx, EmptySpan, EmptySpan, 0)
	testComp(t, &evalCtx, FullSpan, FullSpan, 0)
	testComp(t, &evalCtx, EmptySpan, FullSpan, -1)
	testComp(t, &evalCtx, FullSpan, EmptySpan, 1)

	one := MakeKey(tree.NewDInt(1))
	two := MakeKey(tree.NewDInt(2))
	nine := MakeKey(tree.NewDInt(9))
	onetwo := MakeCompositeKey(tree.NewDInt(1), tree.NewDInt(2))

	var left, right Span

	// [/1 - /1) vs [/1 - /1/2)
	_ = left.TrySet(&evalCtx, one, IncludeBoundary, one, ExcludeBoundary)
	_ = right.TrySet(&evalCtx, one, IncludeBoundary, onetwo, ExcludeBoundary)
	testComp(t, &evalCtx, left, right, -1)
	testComp(t, &evalCtx, right, left, 1)
	testComp(t, &evalCtx, left, left, 0)
	testComp(t, &evalCtx, right, right, 0)

	// [/1 - /1/2) vs [/1 - /1/2]
	left = right
	_ = right.TrySet(&evalCtx, one, IncludeBoundary, onetwo, IncludeBoundary)
	testComp(t, &evalCtx, left, right, -1)
	testComp(t, &evalCtx, right, left, 1)

	// [/1 - /1/2] vs [/1 - /1]
	left = right
	_ = right.TrySet(&evalCtx, one, IncludeBoundary, one, IncludeBoundary)
	testComp(t, &evalCtx, left, right, -1)
	testComp(t, &evalCtx, right, left, 1)

	// [/1 - /1] vs [/1 - /2)
	left = right
	_ = right.TrySet(&evalCtx, one, IncludeBoundary, two, ExcludeBoundary)
	testComp(t, &evalCtx, left, right, -1)
	testComp(t, &evalCtx, right, left, 1)

	// [/1 - /2) vs [/1/2 - /2)
	left = right
	_ = right.TrySet(&evalCtx, onetwo, IncludeBoundary, two, ExcludeBoundary)
	testComp(t, &evalCtx, left, right, -1)
	testComp(t, &evalCtx, right, left, 1)
	testComp(t, &evalCtx, left, left, 0)
	testComp(t, &evalCtx, right, right, 0)

	// [/1/2 - /2) vs (/1/2 - /2)
	left = right
	_ = right.TrySet(&evalCtx, onetwo, ExcludeBoundary, two, ExcludeBoundary)
	testComp(t, &evalCtx, left, right, -1)
	testComp(t, &evalCtx, right, left, 1)

	// (/1/2 - /2) vs (/1 - /2)
	left = right
	_ = right.TrySet(&evalCtx, one, ExcludeBoundary, two, ExcludeBoundary)
	testComp(t, &evalCtx, left, right, -1)
	testComp(t, &evalCtx, right, left, 1)

	// (/1 - /2) vs (/1 - /9)
	left = right
	_ = right.TrySet(&evalCtx, one, ExcludeBoundary, nine, ExcludeBoundary)
	testComp(t, &evalCtx, left, right, -1)
	testComp(t, &evalCtx, right, left, 1)

	// (/1 - /9) vs [/2 - /9]
	left = right
	_ = right.TrySet(&evalCtx, two, IncludeBoundary, nine, IncludeBoundary)
	testComp(t, &evalCtx, left, right, -1)
	testComp(t, &evalCtx, right, left, 1)
}

func TestSpanCompareStarts(t *testing.T) {
	test := func(t *testing.T, evalCtx *tree.EvalContext, left, right Span, expected int) {
		t.Helper()
		if actual := left.CompareStarts(evalCtx, &right); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	evalCtx := tree.MakeTestingEvalContext()

	test(t, &evalCtx, EmptySpan, EmptySpan, 0)
	test(t, &evalCtx, FullSpan, EmptySpan, 0)
	test(t, &evalCtx, EmptySpan, FullSpan, 0)
	test(t, &evalCtx, FullSpan, FullSpan, 0)

	one := MakeKey(tree.NewDInt(1))
	two := MakeKey(tree.NewDInt(2))
	five := MakeKey(tree.NewDInt(5))
	nine := MakeKey(tree.NewDInt(9))

	var onefive Span
	_ = onefive.TrySet(&evalCtx, one, IncludeBoundary, five, IncludeBoundary)
	var twonine Span
	_ = twonine.TrySet(&evalCtx, two, ExcludeBoundary, nine, ExcludeBoundary)

	// Same span.
	test(t, &evalCtx, onefive, onefive, 0)

	// Different spans.
	test(t, &evalCtx, onefive, twonine, -1)
	test(t, &evalCtx, twonine, onefive, 1)
}

func TestSpanCompareEnds(t *testing.T) {
	test := func(t *testing.T, evalCtx *tree.EvalContext, left, right Span, expected int) {
		t.Helper()
		if actual := left.CompareEnds(evalCtx, &right); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	evalCtx := tree.MakeTestingEvalContext()

	test(t, &evalCtx, EmptySpan, EmptySpan, 0)
	test(t, &evalCtx, FullSpan, EmptySpan, 1)
	test(t, &evalCtx, EmptySpan, FullSpan, -1)
	test(t, &evalCtx, FullSpan, FullSpan, 0)

	one := MakeKey(tree.NewDInt(1))
	two := MakeKey(tree.NewDInt(2))
	five := MakeKey(tree.NewDInt(5))
	nine := MakeKey(tree.NewDInt(9))

	var onefive Span
	_ = onefive.TrySet(&evalCtx, one, IncludeBoundary, five, IncludeBoundary)
	var twonine Span
	_ = twonine.TrySet(&evalCtx, two, ExcludeBoundary, nine, ExcludeBoundary)

	// Same span.
	test(t, &evalCtx, onefive, onefive, 0)

	// Different spans.
	test(t, &evalCtx, onefive, twonine, -1)
	test(t, &evalCtx, twonine, onefive, 1)
}

func TestSpanStartsAfter(t *testing.T) {
	test := func(t *testing.T, evalCtx *tree.EvalContext, left, right Span, expected bool) {
		t.Helper()
		if actual := left.StartsAfter(evalCtx, &right); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	evalCtx := tree.MakeTestingEvalContext()

	test(t, &evalCtx, EmptySpan, EmptySpan, true)
	test(t, &evalCtx, FullSpan, EmptySpan, true)
	test(t, &evalCtx, EmptySpan, FullSpan, false)
	test(t, &evalCtx, FullSpan, FullSpan, false)

	// Same span.
	var banana Span
	_ = banana.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.DNull, tree.NewDInt(100)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), IncludeBoundary,
	)
	test(t, &evalCtx, banana, banana, false)

	// Right span's start equal to left span's end.
	var cherry Span
	_ = cherry.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), ExcludeBoundary,
		MakeKey(tree.NewDString("cherry")), ExcludeBoundary,
	)
	test(t, &evalCtx, banana, cherry, false)
	test(t, &evalCtx, cherry, banana, true)

	// Right span's start greater than left span's end, and inverse.
	var cherry2 Span
	_ = cherry2.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(0)), IncludeBoundary,
		MakeKey(tree.NewDString("mango")), ExcludeBoundary,
	)
	test(t, &evalCtx, cherry, cherry2, false)
	test(t, &evalCtx, cherry2, cherry, true)
}

func TestSpanIntersect(t *testing.T) {
	testInt := func(t *testing.T, evalCtx *tree.EvalContext, left, right Span, expected string) {
		t.Helper()
		sp := left
		ok := sp.TryIntersectWith(evalCtx, &right)

		var actual string
		if ok {
			actual = sp.String()
		}

		if actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	evalCtx := tree.MakeTestingEvalContext()

	testInt(t, &evalCtx, EmptySpan, EmptySpan, "[Ø - Ø)")
	testInt(t, &evalCtx, FullSpan, EmptySpan, "[Ø - Ø)")
	testInt(t, &evalCtx, EmptySpan, FullSpan, "")

	// Same span.
	var banana Span
	_ = banana.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.DNull, tree.NewDInt(100)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), IncludeBoundary,
	)
	testInt(t, &evalCtx, banana, banana, "[/NULL/100 - /'banana'/50]")

	// One span immediately after the other.
	var grape Span
	_ = grape.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("grape")), ExcludeBoundary,
	)
	testInt(t, &evalCtx, banana, grape, "")
	testInt(t, &evalCtx, grape, banana, "")

	// Partial overlap.
	var apple Span
	_ = apple.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(300)), ExcludeBoundary,
	)
	testInt(t, &evalCtx, banana, apple, "(/'apple'/200 - /'banana'/50]")
	testInt(t, &evalCtx, apple, banana, "(/'apple'/200 - /'banana'/50]")

	// One span is subset of other.
	var mango Span
	_ = mango.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("mango")), ExcludeBoundary,
	)
	testInt(t, &evalCtx, apple, mango, "(/'apple'/200 - /'cherry'/300)")
	testInt(t, &evalCtx, mango, apple, "(/'apple'/200 - /'cherry'/300)")
	testInt(t, &evalCtx, FullSpan, mango, "(/'apple'/200 - /'mango')")
	testInt(t, &evalCtx, mango, FullSpan, "(/'apple'/200 - /'mango')")

	// Spans are disjoint.
	var pear Span
	_ = pear.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("mango"), tree.NewDInt(0)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("pear"), tree.NewDInt(10)), IncludeBoundary,
	)
	testInt(t, &evalCtx, mango, pear, "")
	testInt(t, &evalCtx, pear, mango, "")

	// Ensure that if TryIntersectWith results in empty set, that it does not
	// update either span.
	mango2 := mango
	pear2 := pear
	_ = mango2.TryIntersectWith(&evalCtx, &pear2)
	if mango2.Compare(&evalCtx, &mango) != 0 {
		t.Errorf("mango2 was incorrectly updated during TryIntersectWith")
	}
	if pear2.Compare(&evalCtx, &pear) != 0 {
		t.Errorf("pear2 was incorrectly updated during TryIntersectWith")
	}

	// Partial overlap on second key.
	_ = pear2.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("pear"), tree.NewDInt(5), tree.DNull), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("raspberry"), tree.NewDInt(100)), IncludeBoundary,
	)
	testInt(t, &evalCtx, pear, pear2, "(/'pear'/5/NULL - /'pear'/10]")
	testInt(t, &evalCtx, pear, pear2, "(/'pear'/5/NULL - /'pear'/10]")
}

func TestSpanUnion(t *testing.T) {
	testUnion := func(t *testing.T, evalCtx *tree.EvalContext, left, right Span, expected string) {
		t.Helper()
		sp := left
		ok := sp.TryUnionWith(evalCtx, &right)

		var actual string
		if ok {
			actual = sp.String()
		}

		if actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	evalCtx := tree.MakeTestingEvalContext()

	testUnion(t, &evalCtx, EmptySpan, EmptySpan, "[Ø - Ø)")
	testUnion(t, &evalCtx, FullSpan, EmptySpan, "[Ø - ∞]")
	testUnion(t, &evalCtx, EmptySpan, FullSpan, "[Ø - ∞]")

	// Same span.
	var banana Span
	_ = banana.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.DNull, tree.NewDInt(100)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), IncludeBoundary,
	)
	testUnion(t, &evalCtx, banana, banana, "[/NULL/100 - /'banana'/50]")

	// Partial overlap.
	var apple Span
	_ = apple.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(300)), ExcludeBoundary,
	)
	testUnion(t, &evalCtx, banana, apple, "[/NULL/100 - /'cherry'/300)")
	testUnion(t, &evalCtx, apple, banana, "[/NULL/100 - /'cherry'/300)")

	// One span is subset of other.
	var mango Span
	_ = mango.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("mango")), ExcludeBoundary,
	)
	testUnion(t, &evalCtx, apple, mango, "(/'apple'/200 - /'mango')")
	testUnion(t, &evalCtx, mango, apple, "(/'apple'/200 - /'mango')")
	testUnion(t, &evalCtx, FullSpan, mango, "[Ø - ∞]")
	testUnion(t, &evalCtx, mango, FullSpan, "[Ø - ∞]")

	// Spans are disjoint.
	var pear Span
	_ = pear.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("mango"), tree.NewDInt(0)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("pear"), tree.NewDInt(10)), IncludeBoundary,
	)
	testUnion(t, &evalCtx, mango, pear, "")
	testUnion(t, &evalCtx, pear, mango, "")

	// Ensure that if TryUnionWith fails to merge, that it does not update
	// either span.
	mango2 := mango
	pear2 := pear
	_ = mango2.TryUnionWith(&evalCtx, &pear2)
	if mango2.Compare(&evalCtx, &mango) != 0 {
		t.Errorf("mango2 was incorrectly updated during TryUnionWith")
	}
	if pear2.Compare(&evalCtx, &pear) != 0 {
		t.Errorf("pear2 was incorrectly updated during TryUnionWith")
	}

	// Partial overlap on second key.
	_ = pear2.TrySet(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("pear"), tree.NewDInt(5), tree.DNull), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("raspberry"), tree.NewDInt(100)), IncludeBoundary,
	)
	testUnion(t, &evalCtx, pear, pear2, "[/'mango'/0 - /'raspberry'/100]")
	testUnion(t, &evalCtx, pear, pear2, "[/'mango'/0 - /'raspberry'/100]")
}
