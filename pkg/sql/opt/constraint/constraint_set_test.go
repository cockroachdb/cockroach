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

package constraint

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestConstraintSetIntersect(t *testing.T) {
	kc1 := testKeyContext(1)
	kc2 := testKeyContext(2)
	kc12 := testKeyContext(1, 2)
	evalCtx := kc1.EvalCtx

	test := func(cs *Set, expected string) {
		t.Helper()
		if cs.String() != expected {
			t.Errorf("\nexpected:\n%vactual:\n%v", expected, cs.String())
		}
	}

	data := newSpanTestData(kc12)

	// Simple AND case.
	// @1 > 20
	gt20 := testSet(kc1, &data.spGt20)
	test(gt20, "/1: (/20 - ]\n")

	// @1 <= 40
	le40 := testSet(kc1, &data.spLe40)
	test(le40, "/1: [ - /40]\n")

	// @1 > 20 AND @1 <= 40
	range2040 := gt20.Intersect(evalCtx, le40)
	test(range2040, "/1: (/20 - /40]\n")
	range2040 = le40.Intersect(evalCtx, gt20)
	test(range2040, "/1: (/20 - /40]\n")

	// Include constraint on multiple columns.
	// (@1, @2) >= (10, 15)
	gt1015 := testSet(kc12, &data.spGe1015)
	test(gt1015, "/1/2: [/10/15 - ]\n")

	// (@1, @2) >= (10, 15) AND @1 <= 40
	multi2 := le40.Intersect(evalCtx, gt1015)
	test(multi2, ""+
		"/1: [ - /40]\n"+
		"/1/2: [/10/15 - ]\n")

	multi2 = gt1015.Intersect(evalCtx, le40)
	test(multi2, ""+
		"/1: [ - /40]\n"+
		"/1/2: [/10/15 - ]\n")

	// (@1, @2) >= (10, 15) AND @1 <= 40 AND @2 < 80
	lt80 := testSet(kc2, &data.spLt80)
	multi3 := lt80.Intersect(evalCtx, multi2)
	test(multi3, ""+
		"/1: [ - /40]\n"+
		"/1/2: [/10/15 - ]\n"+
		"/2: [ - /80)\n")

	multi3 = multi2.Intersect(evalCtx, lt80)
	test(multi3, ""+
		"/1: [ - /40]\n"+
		"/1/2: [/10/15 - ]\n"+
		"/2: [ - /80)\n")

	// Mismatched number of constraints in each set.
	eq10 := testSet(kc1, &data.spEq10)
	mismatched := eq10.Intersect(evalCtx, multi3)
	test(mismatched, ""+
		"/1: [/10 - /10]\n"+
		"/1/2: [/10/15 - ]\n"+
		"/2: [ - /80)\n")

	mismatched = multi3.Intersect(evalCtx, eq10)
	test(mismatched, ""+
		"/1: [/10 - /10]\n"+
		"/1/2: [/10/15 - ]\n"+
		"/2: [ - /80)\n")

	// Multiple intersecting constraints on different columns.
	diffCols := eq10.Intersect(evalCtx, testSet(kc2, &data.spGt20))
	res := diffCols.Intersect(evalCtx, multi3)
	test(res, ""+
		"/1: [/10 - /10]\n"+
		"/1/2: [/10/15 - ]\n"+
		"/2: (/20 - /80)\n")

	res = multi3.Intersect(evalCtx, diffCols)
	test(res, ""+
		"/1: [/10 - /10]\n"+
		"/1/2: [/10/15 - ]\n"+
		"/2: (/20 - /80)\n")

	// Intersection results in Contradiction.
	res = eq10.Intersect(evalCtx, gt20)
	test(res, "contradiction\n")
	res = gt20.Intersect(evalCtx, eq10)
	test(res, "contradiction\n")

	// Intersect with Unconstrained (identity op).
	res = range2040.Intersect(evalCtx, Unconstrained)
	test(res, "/1: (/20 - /40]\n")
	res = Unconstrained.Intersect(evalCtx, range2040)
	test(res, "/1: (/20 - /40]\n")

	// Intersect with Contradiction (always contradiction).
	res = eq10.Intersect(evalCtx, Contradiction)
	test(res, "contradiction\n")
	res = Contradiction.Intersect(evalCtx, eq10)
	test(res, "contradiction\n")
}

func TestConstraintSetUnion(t *testing.T) {
	kc1 := testKeyContext(1)
	kc2 := testKeyContext(2)
	kc12 := testKeyContext(1, 2)
	evalCtx := kc1.EvalCtx
	data := newSpanTestData(kc12)

	test := func(cs *Set, expected string) {
		t.Helper()
		if cs.String() != expected {
			t.Errorf("\nexpected:\n%vactual:\n%v", expected, cs.String())
		}
	}

	// Simple OR case.
	// @1 > 20
	gt20 := testSet(kc1, &data.spGt20)
	test(gt20, "/1: (/20 - ]\n")

	// @1 = 10
	eq10 := testSet(kc1, &data.spEq10)
	test(eq10, "/1: [/10 - /10]\n")

	// @1 > 20 OR @1 = 10
	gt20eq10 := gt20.Union(evalCtx, eq10)
	test(gt20eq10, "/1: [/10 - /10] (/20 - ]\n")
	gt20eq10 = eq10.Union(evalCtx, gt20)
	test(gt20eq10, "/1: [/10 - /10] (/20 - ]\n")

	// Combine constraints that result in full span and unconstrained result.
	// @1 > 20 OR @1 = 10 OR @1 <= 40
	le40 := testSet(kc1, &data.spLe40)
	res := gt20eq10.Union(evalCtx, le40)
	test(res, "unconstrained\n")
	res = le40.Union(evalCtx, gt20eq10)
	test(res, "unconstrained\n")

	// Include constraint on multiple columns and union with itself.
	// (@1, @2) >= (10, 15)
	gt1015 := testSet(kc12, &data.spGe1015)
	res = gt1015.Union(evalCtx, gt1015)
	test(res, "/1/2: [/10/15 - ]\n")

	// Union incompatible constraints (both are discarded).
	// (@1, @2) >= (10, 15) OR @2 < 80
	lt80 := testSet(kc2, &data.spLt80)
	res = gt1015.Union(evalCtx, lt80)
	test(res, "unconstrained\n")
	res = lt80.Union(evalCtx, gt1015)
	test(res, "unconstrained\n")

	// Union two sets with multiple and differing numbers of constraints.
	// ((@1, @2) >= (10, 15) AND @2 < 80 AND @1 > 20) OR (@1 = 10 AND @2 = 80)
	multi3 := gt1015.Intersect(evalCtx, lt80)
	multi3 = multi3.Intersect(evalCtx, gt20)

	eq80 := testSet(kc2, &data.spEq80)
	multi2 := eq10.Intersect(evalCtx, eq80)

	res = multi3.Union(evalCtx, multi2)
	test(res, ""+
		"/1: [/10 - /10] (/20 - ]\n"+
		"/2: [ - /80]\n")
	res = multi2.Union(evalCtx, multi3)
	test(res, ""+
		"/1: [/10 - /10] (/20 - ]\n"+
		"/2: [ - /80]\n")

	// Do same as previous, but in different order so that discarded constraint
	// is at end of list rather than beginning.
	// (@1 > 20 AND @2 < 80 AND (@1, @2) >= (10, 15)) OR (@1 = 10 AND @2 = 80)
	multi3 = gt20.Intersect(evalCtx, lt80)
	multi3 = multi3.Intersect(evalCtx, gt1015)

	res = multi3.Union(evalCtx, multi2)
	test(res, ""+
		"/1: [/10 - /10] (/20 - ]\n"+
		"/2: [ - /80]\n")
	res = multi2.Union(evalCtx, multi3)
	test(res, ""+
		"/1: [/10 - /10] (/20 - ]\n"+
		"/2: [ - /80]\n")

	// Union with Unconstrained (always unconstrained).
	res = gt20.Union(evalCtx, Unconstrained)
	test(res, "unconstrained\n")
	res = Unconstrained.Union(evalCtx, gt20)
	test(res, "unconstrained\n")

	// Union with Contradiction (identity op).
	res = eq10.Union(evalCtx, Contradiction)
	test(res, "/1: [/10 - /10]\n")
	res = Contradiction.Union(evalCtx, eq10)
	test(res, "/1: [/10 - /10]\n")
}

type spanTestData struct {
	spEq10   Span // [/10 - /10]
	spGt20   Span // (/20 - ]
	spLe40   Span // [ - /40]
	spLt80   Span // [ - /80)
	spEq80   Span // [/80 - /80]
	spGe1015 Span // [/10/15 - ]
}

func newSpanTestData(keyCtx KeyContext) *spanTestData {
	data := &spanTestData{}

	key10 := MakeKey(tree.NewDInt(10))
	key15 := MakeKey(tree.NewDInt(15))
	key20 := MakeKey(tree.NewDInt(20))
	key40 := MakeKey(tree.NewDInt(40))
	key80 := MakeKey(tree.NewDInt(80))

	// [/10 - /10]
	data.spEq10.Set(keyCtx, key10, IncludeBoundary, key10, IncludeBoundary)

	// (/20 - ]
	data.spGt20.Set(keyCtx, key20, ExcludeBoundary, EmptyKey, IncludeBoundary)

	// [ - /40]
	data.spLe40.Set(keyCtx, EmptyKey, IncludeBoundary, key40, IncludeBoundary)

	// [ - /80)
	data.spLt80.Set(keyCtx, EmptyKey, IncludeBoundary, key80, ExcludeBoundary)

	// [/80 - /80]
	data.spEq80.Set(keyCtx, key80, IncludeBoundary, key80, IncludeBoundary)

	// [/10/15 - ]
	key1015 := key10.Concat(key15)
	data.spGe1015.Set(keyCtx, key1015, IncludeBoundary, EmptyKey, IncludeBoundary)

	return data
}

func testSet(keyCtx KeyContext, sp *Span) *Set {
	var c Constraint
	c.Init(keyCtx, SingleSpan(sp))
	return SingleConstraint(c)
}
