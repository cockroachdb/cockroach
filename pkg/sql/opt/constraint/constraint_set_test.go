// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package constraint

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
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
			t.Errorf("\nexpected:\n%v\nactual:\n%v", expected, cs.String())
		}
	}

	data := newSpanTestData()

	// Simple AND case.
	// @1 > 20
	var c Constraint
	c.InitSingleSpan(kc1, &data.spGt20)
	gt20 := SingleConstraint(&c)
	test(gt20, "/1: (/20 - ]")

	// @1 <= 40
	le40 := SingleSpanConstraint(kc1, &data.spLe40)
	test(le40, "/1: [ - /40]")

	// @1 > 20 AND @1 <= 40
	range2040 := gt20.Intersect(evalCtx, le40)
	test(range2040, "/1: (/20 - /40]")
	range2040 = le40.Intersect(evalCtx, gt20)
	test(range2040, "/1: (/20 - /40]")

	// Include constraint on multiple columns.
	// (@1, @2) >= (10, 15)
	gt1015 := SingleSpanConstraint(kc12, &data.spGe1015)
	test(gt1015, "/1/2: [/10/15 - ]")

	// (@1, @2) >= (10, 15) AND @1 <= 40
	multi2 := le40.Intersect(evalCtx, gt1015)
	test(multi2, ""+
		"/1: [ - /40]; "+
		"/1/2: [/10/15 - ]")

	multi2 = gt1015.Intersect(evalCtx, le40)
	test(multi2, ""+
		"/1: [ - /40]; "+
		"/1/2: [/10/15 - ]")

	// (@1, @2) >= (10, 15) AND @1 <= 40 AND @2 < 80
	lt80 := SingleSpanConstraint(kc2, &data.spLt80)
	multi3 := lt80.Intersect(evalCtx, multi2)
	test(multi3, ""+
		"/1: [ - /40]; "+
		"/1/2: [/10/15 - ]; "+
		"/2: [ - /80)")

	multi3 = multi2.Intersect(evalCtx, lt80)
	test(multi3, ""+
		"/1: [ - /40]; "+
		"/1/2: [/10/15 - ]; "+
		"/2: [ - /80)")

	// Mismatched number of constraints in each set.
	eq10 := SingleSpanConstraint(kc1, &data.spEq10)
	mismatched := eq10.Intersect(evalCtx, multi3)
	test(mismatched, ""+
		"/1: [/10 - /10]; "+
		"/1/2: [/10/15 - ]; "+
		"/2: [ - /80)")

	mismatched = multi3.Intersect(evalCtx, eq10)
	test(mismatched, ""+
		"/1: [/10 - /10]; "+
		"/1/2: [/10/15 - ]; "+
		"/2: [ - /80)")

	// Multiple intersecting constraints on different columns.
	diffCols := eq10.Intersect(evalCtx, SingleSpanConstraint(kc2, &data.spGt20))
	res := diffCols.Intersect(evalCtx, multi3)
	test(res, ""+
		"/1: [/10 - /10]; "+
		"/1/2: [/10/15 - ]; "+
		"/2: (/20 - /80)")

	res = multi3.Intersect(evalCtx, diffCols)
	test(res, ""+
		"/1: [/10 - /10]; "+
		"/1/2: [/10/15 - ]; "+
		"/2: (/20 - /80)")

	// Intersection results in Contradiction.
	res = eq10.Intersect(evalCtx, gt20)
	test(res, "contradiction")
	res = gt20.Intersect(evalCtx, eq10)
	test(res, "contradiction")

	// Intersect with Unconstrained (identity op).
	res = range2040.Intersect(evalCtx, Unconstrained)
	test(res, "/1: (/20 - /40]")
	res = Unconstrained.Intersect(evalCtx, range2040)
	test(res, "/1: (/20 - /40]")

	// Intersect with Contradiction (always contradiction).
	res = eq10.Intersect(evalCtx, Contradiction)
	test(res, "contradiction")
	res = Contradiction.Intersect(evalCtx, eq10)
	test(res, "contradiction")
}

func TestConstraintSetUnion(t *testing.T) {
	kc1 := testKeyContext(1)
	kc2 := testKeyContext(2)
	kc12 := testKeyContext(1, 2)
	evalCtx := kc1.EvalCtx
	data := newSpanTestData()

	test := func(cs *Set, expected string) {
		t.Helper()
		if cs.String() != expected {
			t.Errorf("\nexpected:\n%vactual:\n%v", expected, cs.String())
		}
	}

	// Simple OR case.
	// @1 > 20
	gt20 := SingleSpanConstraint(kc1, &data.spGt20)
	test(gt20, "/1: (/20 - ]")

	// @1 = 10
	eq10 := SingleSpanConstraint(kc1, &data.spEq10)
	test(eq10, "/1: [/10 - /10]")

	// @1 > 20 OR @1 = 10
	gt20eq10 := gt20.Union(evalCtx, eq10)
	test(gt20eq10, "/1: [/10 - /10] (/20 - ]")
	gt20eq10 = eq10.Union(evalCtx, gt20)
	test(gt20eq10, "/1: [/10 - /10] (/20 - ]")

	// Combine constraints that result in full span and unconstrained result.
	// @1 > 20 OR @1 = 10 OR @1 <= 40
	le40 := SingleSpanConstraint(kc1, &data.spLe40)
	res := gt20eq10.Union(evalCtx, le40)
	test(res, "unconstrained")
	res = le40.Union(evalCtx, gt20eq10)
	test(res, "unconstrained")

	// Include constraint on multiple columns and union with itself.
	// (@1, @2) >= (10, 15)
	gt1015 := SingleSpanConstraint(kc12, &data.spGe1015)
	res = gt1015.Union(evalCtx, gt1015)
	test(res, "/1/2: [/10/15 - ]")

	// Union incompatible constraints (both are discarded).
	// (@1, @2) >= (10, 15) OR @2 < 80
	lt80 := SingleSpanConstraint(kc2, &data.spLt80)
	res = gt1015.Union(evalCtx, lt80)
	test(res, "unconstrained")
	res = lt80.Union(evalCtx, gt1015)
	test(res, "unconstrained")

	// Union two sets with multiple and differing numbers of constraints.
	// ((@1, @2) >= (10, 15) AND @2 < 80 AND @1 > 20) OR (@1 = 10 AND @2 = 80)
	multi3 := gt1015.Intersect(evalCtx, lt80)
	multi3 = multi3.Intersect(evalCtx, gt20)

	eq80 := SingleSpanConstraint(kc2, &data.spEq80)
	multi2 := eq10.Intersect(evalCtx, eq80)

	res = multi3.Union(evalCtx, multi2)
	test(res, ""+
		"/1: [/10 - /10] (/20 - ]; "+
		"/2: [ - /80]")
	res = multi2.Union(evalCtx, multi3)
	test(res, ""+
		"/1: [/10 - /10] (/20 - ]; "+
		"/2: [ - /80]")

	// Do same as previous, but in different order so that discarded constraint
	// is at end of list rather than beginning.
	// (@1 > 20 AND @2 < 80 AND (@1, @2) >= (10, 15)) OR (@1 = 10 AND @2 = 80)
	multi3 = gt20.Intersect(evalCtx, lt80)
	multi3 = multi3.Intersect(evalCtx, gt1015)

	res = multi3.Union(evalCtx, multi2)
	test(res, ""+
		"/1: [/10 - /10] (/20 - ]; "+
		"/2: [ - /80]")
	res = multi2.Union(evalCtx, multi3)
	test(res, ""+
		"/1: [/10 - /10] (/20 - ]; "+
		"/2: [ - /80]")

	// Union with Unconstrained (always unconstrained).
	res = gt20.Union(evalCtx, Unconstrained)
	test(res, "unconstrained")
	res = Unconstrained.Union(evalCtx, gt20)
	test(res, "unconstrained")

	// Union with Contradiction (identity op).
	res = eq10.Union(evalCtx, Contradiction)
	test(res, "/1: [/10 - /10]")
	res = Contradiction.Union(evalCtx, eq10)
	test(res, "/1: [/10 - /10]")
}

func TestExtractCols(t *testing.T) {
	type testCase struct {
		constraints []string
		expected    opt.ColSet
	}

	cols := opt.MakeColSet

	cases := []testCase{
		{
			[]string{
				`/1: [/10 - /10]`,
				`/2: [/8 - /8]`,
				`/-3: [/13 - /7]`,
			},
			cols(1, 2, 3),
		},
		{
			[]string{
				`/1/2: [/10/4 - /10/5] [/12/4 - /12/5]`,
				`/2: [/4 - /4]`,
			},
			cols(1, 2),
		},
		{
			[]string{
				`/1/2/3: [/10/4 - /10/5] [/12/4 - /12/5]`,
				`/4: [/4 - /4]`,
			},
			cols(1, 2, 3, 4),
		},
	}

	evalCtx := tree.NewTestingEvalContext(nil)
	for _, tc := range cases {
		cs := Unconstrained
		for _, constraint := range tc.constraints {
			constraint := ParseConstraint(evalCtx, constraint)
			cs = cs.Intersect(evalCtx, SingleConstraint(&constraint))
		}
		cols := cs.ExtractCols()
		if !tc.expected.Equals(cols) {
			t.Errorf("expected constant columns from %s to be %s, was %s", cs, tc.expected, cols)
		}
	}
}

func TestExtractConstColsForSet(t *testing.T) {
	type vals map[opt.ColumnID]string
	type testCase struct {
		constraints []string
		expected    vals
	}

	cases := []testCase{
		{[]string{`/1: [/10 - /10]`}, vals{1: "10"}},
		{[]string{`/-1: [/10 - /10]`}, vals{1: "10"}},
		{[]string{`/1: [/10 - /11]`}, vals{}},
		{[]string{`/1: [/10 - ]`}, vals{}},
		{[]string{`/1/2: [/10/2 - /10/4]`}, vals{1: "10"}},
		{[]string{`/1/-2: [/10/4 - /10/2]`}, vals{1: "10"}},
		{[]string{`/1/2: [/10/2 - /10/2]`}, vals{1: "10", 2: "2"}},
		{[]string{`/1/2: [/10/2 - /12/2]`}, vals{}},
		{[]string{`/1/2: [/9/2 - /9/2] [/10/2 - /12/2]`}, vals{}},
		{[]string{`/1: [/10 - /10] [/12 - /12]`}, vals{}},
		{
			[]string{
				`/1: [/10 - /10]`,
				`/2: [/8 - /8]`,
				`/-3: [/13 - /7]`,
			},
			vals{1: "10", 2: "8"},
		},
		{
			[]string{
				`/1/2: [/10/4 - /10/5] [/12/4 - /12/5]`,
				`/2: [/4 - /4]`,
			},
			vals{2: "4"},
		},
		{[]string{`/1: [/10 - /11)`}, vals{}},
		{
			[]string{
				`/2/1: [/900/4 - /900/4] [/1000/4 - /1000/4] [/1100/4 - /1100/4] [/1400/4 - /1400/4] [/1500/4 - /1500/4]`,
			},
			vals{1: "4"},
		},
		{
			[]string{
				`/1: [/2 - /3]`,
				`/2/1: [/10/3 - /11/1]`,
			},
			vals{},
		},
	}

	evalCtx := tree.NewTestingEvalContext(nil)
	for _, tc := range cases {
		cs := Unconstrained
		for _, constraint := range tc.constraints {
			constraint := ParseConstraint(evalCtx, constraint)
			cs = cs.Intersect(evalCtx, SingleConstraint(&constraint))
		}
		cols := cs.ExtractConstCols(evalCtx)
		var expCols opt.ColSet
		for col := range tc.expected {
			expCols.Add(col)
		}
		if !expCols.Equals(cols) {
			t.Errorf("%s: expected constant columns be %s, was %s", cs, expCols, cols)
		}
		// Ensure that no value is returned for the columns that are not constant.
		cs.ExtractCols().ForEach(func(col opt.ColumnID) {
			if !cols.Contains(col) {
				val := cs.ExtractValueForConstCol(evalCtx, col)
				if val != nil {
					t.Errorf("%s: const value should not have been found for column %d", cs, col)
				}
			}
		})
		// Ensure that the expected value is returned for the columns that are constant.
		cols.ForEach(func(col opt.ColumnID) {
			val := cs.ExtractValueForConstCol(evalCtx, col)
			if val == nil {
				t.Errorf("%s: no const value for column %d", cs, col)
				return
			}
			if actual, expected := val.String(), tc.expected[col]; actual != expected {
				t.Errorf("%s: expected value %s for column %d, got %s", cs, expected, col, actual)
			}
		})
	}
}

func TestHasSingleColumnConstValues(t *testing.T) {
	type testCase struct {
		constraints []string
		col         opt.ColumnID
		vals        []int
	}
	cases := []testCase{
		{[]string{`/1: [/10 - /10]`}, 1, []int{10}},
		{[]string{`/-1: [/10 - /10]`}, 1, []int{10}},
		{[]string{`/1: [/10 - /11]`}, 0, nil},
		{[]string{`/1: [/10 - /10] [/11 - /11]`}, 1, []int{10, 11}},
		{[]string{`/1: [/10 - /10] [/11 - /11] [/12 - /12]`}, 1, []int{10, 11, 12}},
		{[]string{`/1: [/10 - /10] [/11 - /11] [/12 - /13]`}, 0, nil},
		{[]string{`/1/2: [/10/2 - /10/4]`}, 0, nil},
		{[]string{`/1/2: [/10/2 - /10/2]`}, 0, nil},
		{
			[]string{
				`/1: [/10 - /10]`,
				`/2: [/8 - /8]`,
			},
			0, nil,
		},
		{
			[]string{
				`/1: [/10 - /10]`,
				`/1/2: [/10/8 - /10/8]`,
			},
			0, nil,
		},
	}
	evalCtx := tree.NewTestingEvalContext(nil)
	for _, tc := range cases {
		cs := Unconstrained
		for _, constraint := range tc.constraints {
			constraint := ParseConstraint(evalCtx, constraint)
			cs = cs.Intersect(evalCtx, SingleConstraint(&constraint))
		}
		col, vals, _ := cs.HasSingleColumnConstValues(evalCtx)
		var intVals []int
		for _, val := range vals {
			intVals = append(intVals, int(*val.(*tree.DInt)))
		}
		if tc.col != col || !reflect.DeepEqual(tc.vals, intVals) {
			t.Errorf("%s: expected %d,%d got %d,%d", cs, tc.col, tc.vals, col, intVals)
		}
	}
}

type spanTestData struct {
	spEq10   Span // [/10 - /10]
	spGt20   Span // (/20 - ]
	spLe40   Span // [ - /40]
	spLt80   Span // [ - /80)
	spEq80   Span // [/80 - /80]
	spGe1015 Span // [/10/15 - ]
}

func newSpanTestData() *spanTestData {
	data := &spanTestData{}

	key10 := MakeKey(tree.NewDInt(10))
	key15 := MakeKey(tree.NewDInt(15))
	key20 := MakeKey(tree.NewDInt(20))
	key40 := MakeKey(tree.NewDInt(40))
	key80 := MakeKey(tree.NewDInt(80))

	// [/10 - /10]
	data.spEq10.Init(key10, IncludeBoundary, key10, IncludeBoundary)

	// (/20 - ]
	data.spGt20.Init(key20, ExcludeBoundary, EmptyKey, IncludeBoundary)

	// [ - /40]
	data.spLe40.Init(EmptyKey, IncludeBoundary, key40, IncludeBoundary)

	// [ - /80)
	data.spLt80.Init(EmptyKey, IncludeBoundary, key80, ExcludeBoundary)

	// [/80 - /80]
	data.spEq80.Init(key80, IncludeBoundary, key80, IncludeBoundary)

	// [/10/15 - ]
	key1015 := key10.Concat(key15)
	data.spGe1015.Init(key1015, IncludeBoundary, EmptyKey, IncludeBoundary)

	return data
}
