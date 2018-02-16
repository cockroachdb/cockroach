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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestSpanSet(t *testing.T) {
	test := func(t *testing.T, sp Span, expected string) {
		t.Helper()
		if sp.String() != expected {
			t.Errorf("expected: %s, actual: %s", expected, sp.String())
		}
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

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	var sp Span
	sp.Set(
		&evalCtx,
		MakeKey(tree.NewDInt(1)), IncludeBoundary,
		MakeKey(tree.NewDInt(5)), IncludeBoundary,
	)
	test(t, sp, "[/1 - /5]")

	sp.Set(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("mango"), tree.NewDInt(1)), ExcludeBoundary,
	)
	test(t, sp, "[/'cherry'/5 - /'mango'/1)")

	sp.Set(
		&evalCtx,
		MakeCompositeKey(tree.NewDInt(5), tree.NewDInt(1)), ExcludeBoundary,
		MakeKey(tree.NewDInt(5)), IncludeBoundary,
	)
	test(t, sp, "(/5/1 - /5]")

	sp.Set(
		&evalCtx,
		MakeKey(tree.NewDInt(5)), IncludeBoundary,
		MakeCompositeKey(tree.NewDInt(5), tree.NewDInt(1)), ExcludeBoundary,
	)
	test(t, sp, "[/5 - /5/1)")

	// Try to create unconstrained span.
	testPanic(t, func() {
		sp.Set(&evalCtx, EmptyKey, IncludeBoundary, EmptyKey, IncludeBoundary)
	}, "unconstrained span should never be used")

	// Create exclusive empty start boundary.
	testPanic(t, func() {
		sp.Set(&evalCtx, EmptyKey, ExcludeBoundary, MakeKey(tree.DNull), IncludeBoundary)
	}, "an empty start boundary must be inclusive")

	// Create exclusive empty end boundary.
	testPanic(t, func() {
		sp.Set(&evalCtx, MakeKey(tree.DNull), IncludeBoundary, EmptyKey, ExcludeBoundary)
	}, "an empty end boundary must be inclusive")

	// Try to create zero-length spans.
	testPanic(t, func() {
		sp.Set(
			&evalCtx,
			MakeKey(tree.NewDInt(1)), IncludeBoundary,
			MakeKey(tree.NewDInt(1)), ExcludeBoundary,
		)
	}, "span cannot be empty")

	testPanic(t, func() {
		sp.Set(
			&evalCtx,
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), ExcludeBoundary,
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), IncludeBoundary,
		)
	}, "span cannot be empty")

	// Try to create spans where start boundary > end boundary.
	testPanic(t, func() {
		sp.Set(&evalCtx, MakeKey(tree.NewDInt(0)), IncludeBoundary, MakeKey(tree.DNull), ExcludeBoundary)
	}, "span cannot be empty")

	testPanic(t, func() {
		sp.Set(
			&evalCtx,
			MakeCompositeKey(tree.NewDString("mango"), tree.NewDInt(1)), IncludeBoundary,
			MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(5)), IncludeBoundary,
		)
	}, "span cannot be empty")
}

func TestSpanUnconstrained(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

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
		&evalCtx,
		MakeKey(tree.NewDInt(5)), IncludeBoundary,
		MakeKey(tree.NewDInt(5)), IncludeBoundary,
	)
	if sp.IsUnconstrained() {
		t.Errorf("IsUnconstrained should have returned false")
	}
}

func TestSpanCompare(t *testing.T) {
	testComp := func(t *testing.T, evalCtx *tree.EvalContext, left, right Span, expected int) {
		t.Helper()
		if actual := left.Compare(evalCtx, &right); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	one := MakeKey(tree.NewDInt(1))
	two := MakeKey(tree.NewDInt(2))
	oneone := MakeCompositeKey(tree.NewDInt(1), tree.NewDInt(1))
	twoone := MakeCompositeKey(tree.NewDInt(2), tree.NewDInt(1))

	var spans [17]Span

	// [ - /2)
	spans[0].Set(&evalCtx, EmptyKey, IncludeBoundary, two, ExcludeBoundary)

	// [ - /2/1)
	spans[1].Set(&evalCtx, EmptyKey, IncludeBoundary, twoone, ExcludeBoundary)

	// [ - /2/1]
	spans[2].Set(&evalCtx, EmptyKey, IncludeBoundary, twoone, IncludeBoundary)

	// [ - /2]
	spans[3].Set(&evalCtx, EmptyKey, IncludeBoundary, two, IncludeBoundary)

	// [ - ]
	spans[4] = Span{}

	// [/1 - /2/1)
	spans[5].Set(&evalCtx, one, IncludeBoundary, twoone, ExcludeBoundary)

	// [/1 - /2/1]
	spans[6].Set(&evalCtx, one, IncludeBoundary, twoone, IncludeBoundary)

	// [/1 - ]
	spans[7].Set(&evalCtx, one, IncludeBoundary, EmptyKey, IncludeBoundary)

	// [/1/1 - /2)
	spans[8].Set(&evalCtx, oneone, IncludeBoundary, two, ExcludeBoundary)

	// [/1/1 - /2]
	spans[9].Set(&evalCtx, oneone, IncludeBoundary, two, IncludeBoundary)

	// [/1/1 - ]
	spans[10].Set(&evalCtx, oneone, IncludeBoundary, EmptyKey, IncludeBoundary)

	// (/1/1 - /2)
	spans[11].Set(&evalCtx, oneone, ExcludeBoundary, two, ExcludeBoundary)

	// (/1/1 - /2]
	spans[12].Set(&evalCtx, oneone, ExcludeBoundary, two, IncludeBoundary)

	// (/1/1 - ]
	spans[13].Set(&evalCtx, oneone, ExcludeBoundary, EmptyKey, IncludeBoundary)

	// (/1 - /2/1)
	spans[14].Set(&evalCtx, one, ExcludeBoundary, twoone, ExcludeBoundary)

	// (/1 - /2/1]
	spans[15].Set(&evalCtx, one, ExcludeBoundary, twoone, IncludeBoundary)

	// (/1 - ]
	spans[16].Set(&evalCtx, one, ExcludeBoundary, EmptyKey, IncludeBoundary)

	for i := 0; i < len(spans)-1; i++ {
		testComp(t, &evalCtx, spans[i], spans[i+1], -1)
		testComp(t, &evalCtx, spans[i+1], spans[i], 1)
		testComp(t, &evalCtx, spans[i], spans[i], 0)
		testComp(t, &evalCtx, spans[i+1], spans[i+1], 0)
	}
}

func TestSpanCompareStarts(t *testing.T) {
	test := func(t *testing.T, evalCtx *tree.EvalContext, left, right Span, expected int) {
		t.Helper()
		if actual := left.CompareStarts(evalCtx, &right); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	one := MakeKey(tree.NewDInt(1))
	two := MakeKey(tree.NewDInt(2))
	five := MakeKey(tree.NewDInt(5))
	nine := MakeKey(tree.NewDInt(9))

	var onefive Span
	onefive.Set(&evalCtx, one, IncludeBoundary, five, IncludeBoundary)
	var twonine Span
	twonine.Set(&evalCtx, two, ExcludeBoundary, nine, ExcludeBoundary)

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

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	one := MakeKey(tree.NewDInt(1))
	two := MakeKey(tree.NewDInt(2))
	five := MakeKey(tree.NewDInt(5))
	nine := MakeKey(tree.NewDInt(9))

	var onefive Span
	onefive.Set(&evalCtx, one, IncludeBoundary, five, IncludeBoundary)
	var twonine Span
	twonine.Set(&evalCtx, two, ExcludeBoundary, nine, ExcludeBoundary)

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

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	// Same span.
	var banana Span
	banana.Set(
		&evalCtx,
		MakeCompositeKey(tree.DNull, tree.NewDInt(100)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), IncludeBoundary,
	)
	test(t, &evalCtx, banana, banana, false)

	// Right span's start equal to left span's end.
	var cherry Span
	cherry.Set(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), ExcludeBoundary,
		MakeKey(tree.NewDString("cherry")), ExcludeBoundary,
	)
	test(t, &evalCtx, banana, cherry, false)
	test(t, &evalCtx, cherry, banana, true)

	// Right span's start greater than left span's end, and inverse.
	var cherry2 Span
	cherry2.Set(
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

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	// Same span.
	var banana Span
	banana.Set(
		&evalCtx,
		MakeCompositeKey(tree.DNull, tree.NewDInt(100)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), IncludeBoundary,
	)
	testInt(t, &evalCtx, banana, banana, "[/NULL/100 - /'banana'/50]")

	// One span immediately after the other.
	var grape Span
	grape.Set(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("grape")), ExcludeBoundary,
	)
	testInt(t, &evalCtx, banana, grape, "")
	testInt(t, &evalCtx, grape, banana, "")

	// Partial overlap.
	var apple Span
	apple.Set(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(300)), ExcludeBoundary,
	)
	testInt(t, &evalCtx, banana, apple, "(/'apple'/200 - /'banana'/50]")
	testInt(t, &evalCtx, apple, banana, "(/'apple'/200 - /'banana'/50]")

	// One span is subset of other.
	var mango Span
	mango.Set(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("mango")), ExcludeBoundary,
	)
	testInt(t, &evalCtx, apple, mango, "(/'apple'/200 - /'cherry'/300)")
	testInt(t, &evalCtx, mango, apple, "(/'apple'/200 - /'cherry'/300)")
	testInt(t, &evalCtx, Span{}, mango, "(/'apple'/200 - /'mango')")
	testInt(t, &evalCtx, mango, Span{}, "(/'apple'/200 - /'mango')")

	// Spans are disjoint.
	var pear Span
	pear.Set(
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
	mango2.TryIntersectWith(&evalCtx, &pear2)
	if mango2.Compare(&evalCtx, &mango) != 0 {
		t.Errorf("mango2 was incorrectly updated during TryIntersectWith")
	}
	if pear2.Compare(&evalCtx, &pear) != 0 {
		t.Errorf("pear2 was incorrectly updated during TryIntersectWith")
	}

	// Partial overlap on second key.
	pear2.Set(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("pear"), tree.NewDInt(5), tree.DNull), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("raspberry"), tree.NewDInt(100)), IncludeBoundary,
	)
	testInt(t, &evalCtx, pear, pear2, "(/'pear'/5/NULL - /'pear'/10]")
	testInt(t, &evalCtx, pear2, pear, "(/'pear'/5/NULL - /'pear'/10]")

	// Unconstrained (uninitialized) span.
	testInt(t, &evalCtx, banana, Span{}, "[/NULL/100 - /'banana'/50]")
	testInt(t, &evalCtx, Span{}, banana, "[/NULL/100 - /'banana'/50]")
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

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	// Same span.
	var banana Span
	banana.Set(
		&evalCtx,
		MakeCompositeKey(tree.DNull, tree.NewDInt(100)), IncludeBoundary,
		MakeCompositeKey(tree.NewDString("banana"), tree.NewDInt(50)), IncludeBoundary,
	)
	testUnion(t, &evalCtx, banana, banana, "[/NULL/100 - /'banana'/50]")

	// Partial overlap.
	var apple Span
	apple.Set(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("cherry"), tree.NewDInt(300)), ExcludeBoundary,
	)
	testUnion(t, &evalCtx, banana, apple, "[/NULL/100 - /'cherry'/300)")
	testUnion(t, &evalCtx, apple, banana, "[/NULL/100 - /'cherry'/300)")

	// One span is subset of other.
	var mango Span
	mango.Set(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("apple"), tree.NewDInt(200)), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("mango")), ExcludeBoundary,
	)
	testUnion(t, &evalCtx, apple, mango, "(/'apple'/200 - /'mango')")
	testUnion(t, &evalCtx, mango, apple, "(/'apple'/200 - /'mango')")
	testUnion(t, &evalCtx, Span{}, mango, "[ - ]")
	testUnion(t, &evalCtx, mango, Span{}, "[ - ]")

	// Spans are disjoint.
	var pear Span
	pear.Set(
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
	mango2.TryUnionWith(&evalCtx, &pear2)
	if mango2.Compare(&evalCtx, &mango) != 0 {
		t.Errorf("mango2 was incorrectly updated during TryUnionWith")
	}
	if pear2.Compare(&evalCtx, &pear) != 0 {
		t.Errorf("pear2 was incorrectly updated during TryUnionWith")
	}

	// Partial overlap on second key.
	pear2.Set(
		&evalCtx,
		MakeCompositeKey(tree.NewDString("pear"), tree.NewDInt(5), tree.DNull), ExcludeBoundary,
		MakeCompositeKey(tree.NewDString("raspberry"), tree.NewDInt(100)), IncludeBoundary,
	)
	testUnion(t, &evalCtx, pear, pear2, "[/'mango'/0 - /'raspberry'/100]")
	testUnion(t, &evalCtx, pear, pear2, "[/'mango'/0 - /'raspberry'/100]")

	// Unconstrained (uninitialized) span.
	testUnion(t, &evalCtx, banana, Span{}, "[ - ]")
	testUnion(t, &evalCtx, Span{}, banana, "[ - ]")
}
