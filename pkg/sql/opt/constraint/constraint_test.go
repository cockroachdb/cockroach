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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestConstraintUnion(t *testing.T) {
	test := func(t *testing.T, evalCtx *tree.EvalContext, left, right *Constraint, expected string) {
		t.Helper()
		clone := *left
		clone.UnionWith(evalCtx, right)

		if actual := clone.String(); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	data := newConstraintTestData(&evalCtx)

	// Union constraint with itself.
	test(t, &evalCtx, &data.c1to10, &data.c1to10, "/1: [/1 - /10]")

	// Merge first spans in each constraint.
	test(t, &evalCtx, &data.c1to10, &data.c5to25, "/1: [/1 - /25)")
	test(t, &evalCtx, &data.c5to25, &data.c1to10, "/1: [/1 - /25)")

	// Disjoint spans in each constraint.
	test(t, &evalCtx, &data.c1to10, &data.c40to50, "/1: [/1 - /10] [/40 - /50]")
	test(t, &evalCtx, &data.c40to50, &data.c1to10, "/1: [/1 - /10] [/40 - /50]")

	// Adjacent disjoint spans in each constraint.
	test(t, &evalCtx, &data.c20to30, &data.c30to40, "/1: [/20 - /40]")
	test(t, &evalCtx, &data.c30to40, &data.c20to30, "/1: [/20 - /40]")

	// Merge multiple spans down to single span.
	var left, right Constraint
	left = data.c1to10
	left.UnionWith(&evalCtx, &data.c20to30)
	left.UnionWith(&evalCtx, &data.c40to50)

	right = data.c5to25
	right.UnionWith(&evalCtx, &data.c30to40)

	test(t, &evalCtx, &left, &right, "/1: [/1 - /50]")
	test(t, &evalCtx, &right, &left, "/1: [/1 - /50]")

	// Multiple disjoint spans on each side.
	left = data.c1to10
	left.UnionWith(&evalCtx, &data.c20to30)

	right = data.c40to50
	right.UnionWith(&evalCtx, &data.c60to70)

	test(t, &evalCtx, &left, &right, "/1: [/1 - /10] [/20 - /30) [/40 - /50] (/60 - /70)")
	test(t, &evalCtx, &right, &left, "/1: [/1 - /10] [/20 - /30) [/40 - /50] (/60 - /70)")

	// Multiple spans that yield the unconstrained span.
	left = data.cLt10
	right = data.c5to25
	right.UnionWith(&evalCtx, &data.cGt20)

	test(t, &evalCtx, &left, &right, "/1: unconstrained")
	test(t, &evalCtx, &right, &left, "/1: unconstrained")

	if left.String() != "/1: [ - /10)" {
		t.Errorf("tryUnionWith failed, but still modified one of the spans: %v", left.String())
	}
	if right.String() != "/1: (/5 - ]" {
		t.Errorf("tryUnionWith failed, but still modified one of the spans: %v", right.String())
	}

	// Multiple columns.
	expected := "/1/2: [/'cherry'/true - /'strawberry']"
	test(t, &evalCtx, &data.cherryRaspberry, &data.mangoStrawberry, expected)
	test(t, &evalCtx, &data.mangoStrawberry, &data.cherryRaspberry, expected)
}

func TestConstraintIntersect(t *testing.T) {
	test := func(t *testing.T, evalCtx *tree.EvalContext, left, right *Constraint, expected string) {
		t.Helper()
		clone := *left
		clone.IntersectWith(evalCtx, right)
		if actual := clone.String(); actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	data := newConstraintTestData(&evalCtx)

	// Intersect constraint with itself.
	test(t, &evalCtx, &data.c1to10, &data.c1to10, "/1: [/1 - /10]")

	// Intersect first spans in each constraint.
	test(t, &evalCtx, &data.c1to10, &data.c5to25, "/1: (/5 - /10]")
	test(t, &evalCtx, &data.c5to25, &data.c1to10, "/1: (/5 - /10]")

	// Disjoint spans in each constraint.
	test(t, &evalCtx, &data.c1to10, &data.c40to50, "/1: contradiction")
	test(t, &evalCtx, &data.c40to50, &data.c1to10, "/1: contradiction")

	// Intersect multiple spans.
	var left, right Constraint
	left = data.c1to10
	left.UnionWith(&evalCtx, &data.c20to30)
	left.UnionWith(&evalCtx, &data.c40to50)

	right = data.c5to25
	right.UnionWith(&evalCtx, &data.c30to40)

	test(t, &evalCtx, &right, &left, "/1: (/5 - /10] [/20 - /25) [/40 - /40]")
	test(t, &evalCtx, &left, &right, "/1: (/5 - /10] [/20 - /25) [/40 - /40]")

	// Intersect multiple disjoint spans.
	left = data.c1to10
	left.UnionWith(&evalCtx, &data.c20to30)

	right = data.c40to50
	right.UnionWith(&evalCtx, &data.c60to70)

	test(t, &evalCtx, &left, &right, "/1: contradiction")
	test(t, &evalCtx, &right, &left, "/1: contradiction")

	if left.String() != "/1: [/1 - /10] [/20 - /30)" {
		t.Errorf("tryIntersectWith failed, but still modified one of the spans: %v", left.String())
	}
	if right.String() != "/1: [/40 - /50] (/60 - /70)" {
		t.Errorf("tryIntersectWith failed, but still modified one of the spans: %v", right.String())
	}

	// Multiple columns.
	expected := "/1/2: [/'mango'/false - /'raspberry'/false)"
	test(t, &evalCtx, &data.cherryRaspberry, &data.mangoStrawberry, expected)
	test(t, &evalCtx, &data.mangoStrawberry, &data.cherryRaspberry, expected)
}

func TestConstraintSubsetOf(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	data := newConstraintTestData(&evalCtx)

	test := func(left, right Constraint, expected bool) {
		t.Helper()
		if actual := left.SubsetOf(&evalCtx, &right); actual != expected {
			format := "left: %s, right: %s, expected: %t, actual: %t"
			t.Errorf(format, left.String(), right.String(), expected, actual)
		}
	}

	var left, right Constraint
	left = data.c1to10
	test(left, left, true)

	test(left, data.cLt10, false)
	test(data.cLt10, left, false)

	right = data.c1to10
	right.UnionWith(&evalCtx, &data.c5to25)
	test(left, right, true)
	test(right, left, false)

	left.UnionWith(&evalCtx, &data.c30to40)
	test(left, right, false)
	test(right, left, false)

	right.UnionWith(&evalCtx, &data.c30to40)
	test(left, right, true)
	test(right, left, false)

	right.UnionWith(&evalCtx, &data.c20to30)
	test(left, right, true)
	test(right, left, false)

	right.UnionWith(&evalCtx, &data.c40to50)
	test(left, right, true)
	test(right, left, false)

	right = data.c1to10
	right.UnionWith(&evalCtx, &data.c20to30)
	right.UnionWith(&evalCtx, &data.c30to40)
	right.UnionWith(&evalCtx, &data.c60to70)

	test(data.c1to10, right, true)
	test(data.c20to30, right, true)
	test(data.c30to40, right, true)
	test(data.c40to50, right, false)
	test(data.c60to70, right, true)

	// Generate a contradiction and verify it is a subset of anything else.
	right = data.c1to10
	right.IntersectWith(&evalCtx, &data.c20to30)
	test(data.c1to10, right, false)
	test(right, data.c1to10, true)
	test(right, right, true)
}

func TestCutFirstColumn(t *testing.T) {
	kc := testKeyContext(1, 2, 3)
	evalCtx := kc.EvalCtx

	gen := func(
		leftVals []int, leftBoundary SpanBoundary, rightVals []int, rightBoundary SpanBoundary,
	) *Constraint {
		leftDatums := make([]tree.Datum, len(leftVals))
		for i, v := range leftVals {
			leftDatums[i] = tree.NewDInt(tree.DInt(v))
		}
		rightDatums := make([]tree.Datum, len(rightVals))
		for i, v := range rightVals {
			rightDatums[i] = tree.NewDInt(tree.DInt(v))
		}
		var sp Span
		sp.Set(
			kc,
			MakeCompositeKey(leftDatums...), leftBoundary,
			MakeCompositeKey(rightDatums...), rightBoundary,
		)
		var c Constraint
		c.Init(kc, SingleSpan(&sp))
		return &c
	}

	test := func(c *Constraint, level int, expected string) {
		t.Helper()
		cCopy := *c
		for i := 0; i < level; i++ {
			cCopy.CutFirstColumn(evalCtx)
		}
		if actual := cCopy.String(); actual != expected {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	}

	c := gen([]int{1, 2, 3}, IncludeBoundary, []int{1, 2, 4}, IncludeBoundary)
	test(c, 1, "/2/3: [/2/3 - /2/4]")
	test(c, 2, "/3: [/3 - /4]")

	c.UnionWith(evalCtx, gen([]int{1, 1, 5}, IncludeBoundary, []int{1, 1, 6}, IncludeBoundary))
	test(c, 1, "/2/3: [/1/5 - /1/6] [/2/3 - /2/4]")
	test(c, 2, "/3: [/3 - /4] [/5 - /6]")

	c.UnionWith(evalCtx, gen([]int{1, 2, 8}, IncludeBoundary, []int{1, 4, 9}, IncludeBoundary))
	test(c, 1, "/2/3: [/1/5 - /1/6] [/2/3 - /2/4] [/2/8 - /4/9]")
	test(c, 2, "/3: unconstrained")

	c.UnionWith(evalCtx, gen([]int{2, 1}, IncludeBoundary, []int{4, 2}, IncludeBoundary))
	test(c, 1, "/2/3: unconstrained")
	test(c, 2, "/3: unconstrained")
}

func TestConsolidateSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		s string
		// expected value
		e string
	}{
		{
			s: "[/1 - /2] [/3 - /5] [/7 - /9]",
			e: "[/1 - /5] [/7 - /9]",
		},
		{
			s: "[/1 - /2] (/3 - /5] [/7 - /9]",
			e: "[/1 - /2] (/3 - /5] [/7 - /9]",
		},
		{
			s: "[/1 - /2) [/3 - /5] [/7 - /9]",
			e: "[/1 - /2) [/3 - /5] [/7 - /9]",
		},
		{
			s: "[/1 - /2) (/3 - /5] [/7 - /9]",
			e: "[/1 - /2) (/3 - /5] [/7 - /9]",
		},
		{
			s: "[/1/1 - /1/3] [/1/4 - /2]",
			e: "[/1/1 - /2]",
		},
		{
			s: "[/1/1/5 - /1/1/3] [/1/1/2 - /1/1/1]",
			e: "[/1/1/5 - /1/1/1]",
		},
		{
			s: "[/1/1/5 - /1/1/3] [/1/2/2 - /1/2/1]",
			e: "[/1/1/5 - /1/1/3] [/1/2/2 - /1/2/1]",
		},
		{
			s: "[/1 - /2] [/3 - /4] [/5 - /6] [/8 - /9] [/10 - /11] [/12 - /13] [/15 - /16]",
			e: "[/1 - /6] [/8 - /13] [/15 - /16]",
		},
	}

	kc := testKeyContext(1, 2, -3)
	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			spans := parseSpans(kc, tc.s)
			var c Constraint
			c.Init(kc, spans)
			c.ConsolidateSpans(kc.EvalCtx)
			if res := c.Spans.String(); res != tc.e {
				t.Errorf("expected  %s  got  %s", tc.e, res)
			}
		})
	}
}

func TestExactPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		s string
		// expected value
		e int
	}{
		{
			s: "",
			e: 0,
		},
		{
			s: "[/1 - /1]",
			e: 1,
		},
		{
			s: "[/1 - /2]",
			e: 0,
		},
		{
			s: "[/1 - /2]",
			e: 0,
		},
		{
			s: "[/1/2/3 - /1/2/3]",
			e: 3,
		},
		{
			s: "[/1/2/3 - /1/2/3] [/1/2/5 - /1/2/8]",
			e: 2,
		},
		{
			s: "[/1/2/3 - /1/2/3] [/1/2/5 - /1/3/8]",
			e: 1,
		},
		{
			s: "[/1/2/3 - /1/2/3] [/3 - /4]",
			e: 0,
		},
		{
			s: "[/1/2/1 - /1/2/1] [/1/3/1 - /1/4/1]",
			e: 1,
		},
	}

	kc := testKeyContext(1, 2, 3)
	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			spans := parseSpans(kc, tc.s)
			var c Constraint
			c.Init(kc, spans)
			if res := c.ExactPrefix(kc.EvalCtx); res != tc.e {
				t.Errorf("expected  %d  got  %d", tc.e, res)
			}
		})
	}
}

type constraintTestData struct {
	cLt10           Constraint // [ - /10)
	cGt20           Constraint // (/20 - ]
	c1to10          Constraint // [/1 - /10]
	c5to25          Constraint // (/5 - /25)
	c20to30         Constraint // [/20 - /30)
	c30to40         Constraint // [/30 - /40]
	c40to50         Constraint // [/40 - /50]
	c60to70         Constraint // (/60 - /70)
	cherryRaspberry Constraint // [/'cherry'/true - /'raspberry'/false)
	mangoStrawberry Constraint // [/'mango'/false - /'strawberry']
}

func newConstraintTestData(evalCtx *tree.EvalContext) *constraintTestData {
	data := &constraintTestData{}

	key1 := MakeKey(tree.NewDInt(1))
	key5 := MakeKey(tree.NewDInt(5))
	key10 := MakeKey(tree.NewDInt(10))
	key20 := MakeKey(tree.NewDInt(20))
	key25 := MakeKey(tree.NewDInt(25))
	key30 := MakeKey(tree.NewDInt(30))
	key40 := MakeKey(tree.NewDInt(40))
	key50 := MakeKey(tree.NewDInt(50))
	key60 := MakeKey(tree.NewDInt(60))
	key70 := MakeKey(tree.NewDInt(70))

	kc12 := testKeyContext(1, 2)
	kc1 := testKeyContext(1)

	cherry := MakeCompositeKey(tree.NewDString("cherry"), tree.DBoolTrue)
	mango := MakeCompositeKey(tree.NewDString("mango"), tree.DBoolFalse)
	raspberry := MakeCompositeKey(tree.NewDString("raspberry"), tree.DBoolFalse)
	strawberry := MakeKey(tree.NewDString("strawberry"))

	var span Span

	// [ - /10)
	span.Set(kc1, EmptyKey, IncludeBoundary, key10, ExcludeBoundary)
	data.cLt10.Init(kc1, SingleSpan(&span))

	// (/20 - ]
	span.Set(kc1, key20, ExcludeBoundary, EmptyKey, IncludeBoundary)
	data.cGt20.Init(kc1, SingleSpan(&span))

	// [/1 - /10]
	span.Set(kc1, key1, IncludeBoundary, key10, IncludeBoundary)
	data.c1to10.Init(kc1, SingleSpan(&span))

	// (/5 - /25)
	span.Set(kc1, key5, ExcludeBoundary, key25, ExcludeBoundary)
	data.c5to25.Init(kc1, SingleSpan(&span))

	// [/20 - /30)
	span.Set(kc1, key20, IncludeBoundary, key30, ExcludeBoundary)
	data.c20to30.Init(kc1, SingleSpan(&span))

	// [/30 - /40]
	span.Set(kc1, key30, IncludeBoundary, key40, IncludeBoundary)
	data.c30to40.Init(kc1, SingleSpan(&span))

	// [/40 - /50]
	span.Set(kc1, key40, IncludeBoundary, key50, IncludeBoundary)
	data.c40to50.Init(kc1, SingleSpan(&span))

	// (/60 - /70)
	span.Set(kc1, key60, ExcludeBoundary, key70, ExcludeBoundary)
	data.c60to70.Init(kc1, SingleSpan(&span))

	// [/'cherry'/true - /'raspberry'/false)
	span.Set(kc12, cherry, IncludeBoundary, raspberry, ExcludeBoundary)
	data.cherryRaspberry.Init(kc12, SingleSpan(&span))

	// [/'mango'/false - /'strawberry']
	span.Set(kc12, mango, IncludeBoundary, strawberry, IncludeBoundary)
	data.mangoStrawberry.Init(kc12, SingleSpan(&span))

	return data
}

// parseSpans parses a list of spans with integer values like:
//   "[/1 - /2], [/5 - /6]
func parseSpans(keyCtx *KeyContext, str string) Spans {
	if str == "" {
		return Spans{}
	}
	s := strings.Split(str, " ")
	// Each span has three pieces.
	if len(s)%3 != 0 {
		panic(str)
	}
	var result Spans
	for i := 0; i < len(s)/3; i++ {
		sp := parseSpan(keyCtx, strings.Join(s[i*3:i*3+3], " "))
		result.Append(&sp)
	}
	return result
}

// parses a span with integer column values in the format of Span.String,
// e.g: [/1 - /2].
func parseSpan(keyCtx *KeyContext, str string) Span {
	if len(str) < len("[ - ]") {
		panic(str)
	}
	var startBoundary, endBoundary SpanBoundary
	switch str[0] {
	case '[':
		startBoundary = IncludeBoundary
	case '(':
		startBoundary = ExcludeBoundary
	default:
		panic(str)
	}
	switch str[len(str)-1] {
	case ']':
		endBoundary = IncludeBoundary
	case ')':
		endBoundary = ExcludeBoundary
	default:
		panic(str)
	}
	sepIdx := strings.Index(str, " - ")
	if sepIdx == -1 {
		panic(str)
	}
	startVal := str[1:sepIdx]
	endVal := str[sepIdx+len(" - ") : len(str)-1]

	parseVals := func(str string) tree.Datums {
		if str == "" {
			return nil
		}
		if str[0] != '/' {
			panic(str)
		}
		var res tree.Datums
		for i := 1; i < len(str); {
			length := strings.Index(str[i:], "/")
			if length == -1 {
				length = len(str) - i
			}
			val, err := strconv.Atoi(str[i : i+length])
			if err != nil {
				panic(err)
			}
			res = append(res, tree.NewDInt(tree.DInt(val)))
			i += length + 1
		}
		return res
	}
	var sp Span
	startVals := parseVals(startVal)
	endVals := parseVals(endVal)
	sp.Set(
		keyCtx,
		MakeCompositeKey(startVals...), startBoundary,
		MakeCompositeKey(endVals...), endBoundary,
	)
	return sp
}
