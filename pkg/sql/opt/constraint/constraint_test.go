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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partition"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
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

func TestConstraintContains(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	testData := []struct {
		a        string
		b        string
		expected bool
	}{
		// Positive tests.
		{
			a:        "/1: contradiction",
			b:        "/1: contradiction",
			expected: true,
		},
		{
			a:        "/1: unconstrained",
			b:        "/1: unconstrained",
			expected: true,
		},
		{
			a:        "/1: unconstrained",
			b:        "/1: contradiction",
			expected: true,
		},
		{
			a:        "/1: unconstrained",
			b:        "/1: [/1 - /1]",
			expected: true,
		},
		{
			"/1: [/1 - /1]",
			"/1: contradiction",
			true,
		},
		{
			"/1: [/1 - /1]",
			"/1: [/1 - /1]",
			true,
		},
		{
			a:        "/1: [/1 - /5]",
			b:        "/1: [/1 - /4]",
			expected: true,
		},
		{
			a:        "/1: [/1 - /5]",
			b:        "/1: [/2 - /5]",
			expected: true,
		},
		{
			a:        "/1: [/1 - /5]",
			b:        "/1: [/2 - /4]",
			expected: true,
		},
		{
			a:        "/1: [/0 - /1] [/3 - /3]",
			b:        "/1: [/3 - /3]",
			expected: true,
		},
		{
			a:        "/1: [/0 - /1] [/3 - /6]",
			b:        "/1: [/4 - /5]",
			expected: true,
		},
		{
			a:        "/1: [/1 - /100]",
			b:        "/1: [/2 - /2] [/4 - /5] [/20 - /30]",
			expected: true,
		},
		{
			a:        "/1: [/0 - /0] [/1 - /100] [/150 - /200]",
			b:        "/1: [/2 - /2] [/4 - /5] [/20 - /30]",
			expected: true,
		},
		// Negative tests.
		{
			a:        "/1: contradiction",
			b:        "/1: unconstrained",
			expected: false,
		},
		{
			a:        "/1: contradiction",
			b:        "/1: [/1 - /1]",
			expected: false,
		},
		{
			a:        "/1: [/1 - /1]",
			b:        "/1: [/2 - /2]",
			expected: false,
		},
		{
			a:        "/1: [/1 - /2]",
			b:        "/1: [/0 - /2]",
			expected: false,
		},
		{
			a:        "/1: [/1 - /2]",
			b:        "/1: [/1 - /3]",
			expected: false,
		},
		{
			a:        "/1: [/0 - /0] [/1 - /1]",
			b:        "/1: [/0 - /0] [/2 - /2]",
			expected: false,
		},
		{
			a:        "/1: [/0 - /0] [/2 - /3]",
			b:        "/1: [/0 - /0] [/1 - /3]",
			expected: false,
		},
		{
			a:        "/1: [/0 - /0] [/1 - /2]",
			b:        "/1: [/0 - /0] [/1 - /3]",
			expected: false,
		},
		{
			a:        "/1: [/0 - /1] [/3 - /3]",
			b:        "/1: [/2 - /2]",
			expected: false,
		},
		{
			a:        "/1: [/1 - /100]",
			b:        "/1: [/2 - /2] [/4 - /5] [/90 - /110]",
			expected: false,
		},
		{
			a:        "/1: [/0 - /0] [/1 - /100] [/120 - /120]",
			b:        "/1: [/2 - /2] [/4 - /5] [/90 - /110]",
			expected: false,
		},
	}

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ac := ParseConstraint(&evalCtx, tc.a)
			bc := ParseConstraint(&evalCtx, tc.b)

			res := ac.Contains(&evalCtx, &bc)
			if res == tc.expected {
				return
			}
			if tc.expected {
				t.Errorf("%s should contain %s", ac, bc)
			} else {
				t.Errorf("%s should not contain %s", ac, bc)
			}
		})
	}
}

func TestConstraintContainsSpan(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	// Each test case has a bunch of spans that are expected to be contained, and
	// a bunch of spans that are expected not to be contained.
	testData := []struct {
		constraint        string
		containedSpans    string
		notContainedSpans string
	}{
		{
			constraint:        "/1: [/1 - /3]",
			containedSpans:    "[/1 - /1] (/1 - /2) (/1 - /3) [/2 - /3] [/1 - /3]",
			notContainedSpans: "[/0 - /1] (/0 - /1] (/0 - /2] (/0 - /3) [/1 - /4) [/2 - /5]",
		},
		{
			constraint: "/1/2: [ - /2] [/4 - /4] [/5/3 - /7) [/9 - /9/20]",
			containedSpans: "[ - /1] [ - /2) [ - /2] [/1 - /2] [/2 - /2] [/4 - /4] " +
				"[/5/3 - /5/3/1] [/6 - /6] [/5/5 - /7) [/9/10 - /9/15] [/9/19 - /9/20]",
			notContainedSpans: "[ - /3] [/1 - /3] [/3 - /4] [/3 - /6] [/5/3 - /7] [/6 - /8] " +
				"[/9/20 - /9/21] [/8 - /9]",
		},
		{
			constraint:        "/1/-2: [/1/5 - /1/2] [/3/5 - /5/2] [/7 - ]",
			containedSpans:    "[/1/5 - /1/2] [/1/4 - /1/3] [/1/4 - /1/2] [/4 - /5) [/4/6 - /5/3] [/7/1 - ]",
			notContainedSpans: "[/1/5 - /1/1] [/1/3 - /1/1] [/3/6 - /3/5] [/4 - /5] [/4 - /5/1] [/6/10 - ]",
		},
	}

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			c := ParseConstraint(&evalCtx, tc.constraint)

			spans := parseSpans(&evalCtx, tc.containedSpans)
			for i := 0; i < spans.Count(); i++ {
				if sp := spans.Get(i); !c.ContainsSpan(&evalCtx, sp) {
					t.Errorf("%s should contain span %s", c, sp)
				}
			}
			spans = parseSpans(&evalCtx, tc.notContainedSpans)
			for i := 0; i < spans.Count(); i++ {
				if sp := spans.Get(i); c.ContainsSpan(&evalCtx, sp) {
					t.Errorf("%s should not contain span %s", c, sp)
				}
			}
		})
	}
}

func TestConstraintIntersectsSpan(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	// Each test case has a bunch of spans that are expected to intersect the
	// constraint, and a bunch of spans that are expected not to.
	testData := []struct {
		constraint           string
		intersectingSpans    string
		notIntersectingSpans string
	}{
		{
			constraint:           "/1: [/1 - /3]",
			intersectingSpans:    "[/0 - /1] (/0 - /2] [/2 - /2] (/0 - /3) [/1 - /1] (/1 - /2) [/2 - /3] [/1 - /3] [/2 - /5]",
			notIntersectingSpans: "[/0 - /1) (/3 - /5) [/7 - /7] [/7 - /10]",
		},
		{
			constraint: "/1/2: [ - /2] [/4 - /4] [/5/3 - /7) [/9 - /9/20]",
			intersectingSpans: "[ - /1] [ - /2] [ - /3] [/1 - /2] [/2 - /2] [/3 - /4] [/4 - /4] " +
				"[/5/3 - /5/3/1] [/5/3 - /7] [/6 - /8] [/5/5 - /7) [/9/19 - /9/20] [/9/20 - /9/21] [/8 - /9]",
			notIntersectingSpans: "[/3 - /3] (/2 - /4) [/5 - /5/3) (/9/20 - /10]",
		},
		{
			constraint:           "/1/-2: [/1/5 - /1/2] [/3/5 - /5/2] [/7 - ]",
			intersectingSpans:    "[/1/5 - /1/2] [/1/4 - /1/3] [/1/4 - /1/2] [/4 - /5) [/4 - /5/1] [/4/6 - /5/3] [/6/10 - ]",
			notIntersectingSpans: "[ - /1/6] (/1/2 - /3/5) [/6 - /7) (/6/3 - /6/9]",
		},
	}

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			c := ParseConstraint(&evalCtx, tc.constraint)

			spans := parseSpans(&evalCtx, tc.intersectingSpans)
			for i := 0; i < spans.Count(); i++ {
				if sp := spans.Get(i); !c.IntersectsSpan(&evalCtx, sp) {
					t.Errorf("%s should intersect span %s", c, sp)
				}
			}
			spans = parseSpans(&evalCtx, tc.notIntersectingSpans)
			for i := 0; i < spans.Count(); i++ {
				if sp := spans.Get(i); c.IntersectsSpan(&evalCtx, sp) {
					t.Errorf("%s should not intersect span %s", c, sp)
				}
			}
		})
	}
}

func TestConstraintCombine(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	testData := []struct {
		a, b, e string
	}{
		{
			a: "/1/2: [ - /2] [/4 - /4] [/5/30 - /7] [/9 - /9/20]",
			b: "/2: [/10 - /10] [/20 - /20] [/30 - /30] [/40 - /40]",
			e: "/1/2: [ - /2/40] [/4/10 - /4/10] [/4/20 - /4/20] [/4/30 - /4/30] [/4/40 - /4/40] " +
				"[/5/30 - /7/40] [/9/10 - /9/20]",
		},
		{
			a: "/1/2/3: [ - /1/10] [/2 - /3/20] [/4/30 - /5] [/6/10 - /6/10]",
			b: "/3: [/50 - /50] [/60 - /70]",
			e: "/1/2/3: [ - /1/10/70] [/2 - /3/20/70] [/4/30/50 - /5] [/6/10/50 - /6/10/50] " +
				"[/6/10/60 - /6/10/70]",
		},
		{
			a: "/1/2/3/4: [ - /10] [/15 - /15] [/20 - /20/10] [/30 - /40) [/80 - ]",
			b: "/2/3/4: [/20 - /20/10] [/30 - /30] [/40 - /40]",
			e: "/1/2/3/4: [ - /10/40] [/15/20 - /15/20/10] [/15/30 - /15/30] [/15/40 - /15/40] " +
				"[/30/20 - /40) [/80/20 - ]",
		},
		{
			a: "/1/2/3/4: [ - /10/40] [/15/20 - /15/20/10] [/15/30 - /15/30] [/15/40 - /15/40] " +
				"[/30/20 - /40) [/80/20 - ]",
			b: "/4: [/20/10 - /30] [/40 - /40]",
			e: "/1/2/3/4: [ - /10/40] [/15/20 - /15/20/10/40] [/15/30 - /15/30] [/15/40 - /15/40] " +
				"[/30/20 - /40) [/80/20 - ]",
		},
		{
			a: "/1/2: [/1 - /1/6]",
			b: "/2: [/8 - /8]",
			e: "/1/2: contradiction",
		},
	}

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			a := ParseConstraint(&evalCtx, tc.a)
			b := ParseConstraint(&evalCtx, tc.b)
			a.Combine(&evalCtx, &b)
			if res := a.String(); res != tc.e {
				t.Errorf("expected\n  %s; got\n  %s", tc.e, res)
			}
		})
	}
}

func TestConsolidateSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

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
		{
			// Test that consolidating two spans preserves the correct type of ending
			// boundary (#38878).
			s: "[/1 - /2] [/3 - /5)",
			e: "[/1 - /5)",
		},
	}

	kc := testKeyContext(1, 2, -3)
	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			spans := parseSpans(&evalCtx, tc.s)
			var c Constraint
			c.Init(kc, &spans)
			c.ConsolidateSpans(kc.EvalCtx, nil)
			if res := c.Spans.String(); res != tc.e {
				t.Errorf("expected  %s  got  %s", tc.e, res)
			}
		})
	}
}

func TestConsolidateLocalAndRemoteSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	testData := []struct {
		spanInputs string
		// expected value
		expected string

		// Partition Spans
		// The start key of each span defines a different PARTITION BY LIST value.
		// The end key is ignored.
		partitionSpans string

		// Partition Localities, one for each partitionSpan above
		// true == local, false == remote
		// There must be the same number of entries as partition spans.
		localities []bool
	}{
		{
			partitionSpans: "[/1 - /1] [/3 - /3] [/7 - /9]",
			localities:     []bool{true, false, false},
			spanInputs:     "[/1/2 - /1/3] [/1/4 - /6] [/7 - /9]",
			expected:       "[/1/2 - /1/3] [/1/4 - /9]",
		},
		// TODO(msirek):  This result is expected to change if span-based
		//                partition locality checking is enabled, in which case
		//                the span [/3 - /4] would not get consolidated.
		{
			partitionSpans: "[/3 - /3] [/4 - /4]",
			localities:     []bool{true, true},
			spanInputs:     "[/1 - /2] [/3 - /4] [/7 - /9]",
			expected:       "[/1 - /4] [/7 - /9]",
		},
		{
			partitionSpans: "[/1/2 - /1/2] [/1/4 - /1/4] [/1 - /1]",
			localities:     []bool{true, true, false},
			spanInputs:     "[/1/2 - /1/4] [/1/5 - /5]",
			expected:       "[/1/2 - /5]",
		},
		// TODO(msirek):  This result is expected to change when span-based
		//                partition locality checking is enabled.
		{
			partitionSpans: "[/1/2 - /1/2] [/1/3 - /1/3] [/1/4 - /1/4] [/1 - /1]",
			localities:     []bool{true, true, true, false},
			spanInputs:     "[/1/2 - /1/4] [/1/5 - /5]",
			expected:       "[/1/2 - /5]",
		},
		// TODO(msirek):  This result is expected to change when span-based
		//                partition locality checking is enabled.
		{
			partitionSpans: "[/1/2 - /1/2] [/1/3 - /1/3] [/1/4 - /1/4] [/1/2/3 - /1/2/3]",
			localities:     []bool{true, true, true, false},
			spanInputs:     "[/1/2 - /1/4] [/1/5 - /5]",
			expected:       "[/1/2 - /5]",
		},
		{
			partitionSpans: "[/1/2/3 - /1/2/3] [/9 - /9]",
			localities:     []bool{true, false},
			spanInputs:     "[/1/2/1 - /1/2/2] [/1/2/3 - /1/2/3] [/1/2/4 - /9]",
			expected:       "[/1/2/1 - /1/2/2] [/1/2/3 - /1/2/3] [/1/2/4 - /9]",
		},
	}

	kc := testKeyContext(1, 2, 3)
	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			// Read the partitionSpans and localities entries to make an index that
			// only has the partitions and ps (PrefixSorter) elements populated.
			partitionSpans := parseSpans(&evalCtx, tc.partitionSpans)
			partitions := make([]testcat.Partition, partitionSpans.Count())
			localPartitions := util.FastIntSet{}
			for j := 0; j < partitionSpans.Count(); j++ {
				span := partitionSpans.Get(j)
				spanDatums := make([]tree.Datums, 1)
				datumSlice := make(tree.Datums, span.StartKey().Length())
				for k := 0; k < span.StartKey().Length(); k++ {
					datumSlice[k] = span.StartKey().Value(k)
				}
				spanDatums[0] = datumSlice
				partitions[j] = testcat.Partition{}
				partitions[j].SetDatums(spanDatums)

				if tc.localities[j] {
					localPartitions.Add(j)
				}
			}

			// Make the index
			index := &testcat.Index{}
			index.SetPartitions(partitions)
			// Make the PrefixSorter.
			ps := partition.GetSortedPrefixes(index, localPartitions, &evalCtx)

			// Run the test.
			spans := parseSpans(&evalCtx, tc.spanInputs)
			var c Constraint
			c.Init(kc, &spans)
			c.ConsolidateSpans(kc.EvalCtx, ps)
			if res := c.Spans.String(); res != tc.expected {
				t.Errorf("expected  %s  got  %s", tc.expected, res)
			}
		})
	}

}

func TestExactPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	testData := []struct {
		s string
		// expected value
		e int
	}{
		{
			s: "contradiction",
			e: 0,
		},
		{
			s: "unconstrained",
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
			s: "[/1/2/3 - /1/2/3] [/1/3/3 - /1/3/3]",
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
			spans := parseSpans(&evalCtx, tc.s)
			var c Constraint
			c.Init(kc, &spans)
			if res := c.ExactPrefix(kc.EvalCtx); res != tc.e {
				t.Errorf("expected  %d  got  %d", tc.e, res)
			}
		})
	}
}

func TestPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	testData := []struct {
		s string
		// expected value
		e int
	}{
		{
			s: "contradiction",
			e: 0,
		},
		{
			s: "unconstrained",
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
			s: "[/1/2/3 - /1/2/3] [/1/3/3 - /1/3/3]",
			e: 3,
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
			spans := parseSpans(&evalCtx, tc.s)
			var c Constraint
			c.Init(kc, &spans)
			if res := c.Prefix(kc.EvalCtx); res != tc.e {
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
	span.Init(EmptyKey, IncludeBoundary, key10, ExcludeBoundary)
	data.cLt10.InitSingleSpan(kc1, &span)

	// (/20 - ]
	span.Init(key20, ExcludeBoundary, EmptyKey, IncludeBoundary)
	data.cGt20.InitSingleSpan(kc1, &span)

	// [/1 - /10]
	span.Init(key1, IncludeBoundary, key10, IncludeBoundary)
	data.c1to10.InitSingleSpan(kc1, &span)

	// (/5 - /25)
	span.Init(key5, ExcludeBoundary, key25, ExcludeBoundary)
	data.c5to25.InitSingleSpan(kc1, &span)

	// [/20 - /30)
	span.Init(key20, IncludeBoundary, key30, ExcludeBoundary)
	data.c20to30.InitSingleSpan(kc1, &span)

	// [/30 - /40]
	span.Init(key30, IncludeBoundary, key40, IncludeBoundary)
	data.c30to40.InitSingleSpan(kc1, &span)

	// [/40 - /50]
	span.Init(key40, IncludeBoundary, key50, IncludeBoundary)
	data.c40to50.InitSingleSpan(kc1, &span)

	// (/60 - /70)
	span.Init(key60, ExcludeBoundary, key70, ExcludeBoundary)
	data.c60to70.InitSingleSpan(kc1, &span)

	// [/'cherry'/true - /'raspberry'/false)
	span.Init(cherry, IncludeBoundary, raspberry, ExcludeBoundary)
	data.cherryRaspberry.InitSingleSpan(kc12, &span)

	// [/'mango'/false - /'strawberry']
	span.Init(mango, IncludeBoundary, strawberry, IncludeBoundary)
	data.mangoStrawberry.InitSingleSpan(kc12, &span)

	return data
}

func TestExtractConstCols(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	testData := []struct {
		c string
		// expected value
		e []opt.ColumnID
	}{
		{ // 0
			c: "/1: contradiction",
			e: []opt.ColumnID{},
		},
		{ // 1
			c: "/1: [ - /2]",
			e: []opt.ColumnID{},
		},
		{ // 2
			c: "/1: [/3 - /4]",
			e: []opt.ColumnID{},
		},
		{ // 3
			c: "/1: [/4 - /4]",
			e: []opt.ColumnID{1},
		},
		{ // 4
			c: "/-1: [ - /2]",
			e: []opt.ColumnID{},
		},
		{ // 5
			c: "/-1: [/4 - /4]",
			e: []opt.ColumnID{1},
		},
		{ // 6
			c: "/1/2/3: [/1/1/NULL - /1/1/2] [/3/3/3 - /3/3/4]",
			e: []opt.ColumnID{},
		},
		{ // 7
			c: "/1/2/3: [/1/1/1 - /1/1/1] [/4/1 - /4/1]",
			e: []opt.ColumnID{2},
		},
		{ // 8
			c: "/1/2/3: [/1/1/1 - /1/1/2] [/1/1/3 - /1/3/4]",
			e: []opt.ColumnID{1},
		},
		{ // 9
			c: "/1/2/3/4: [/1/1/1/1 - /1/1/2/1] [/3/1/3/1 - /3/1/4/1]",
			e: []opt.ColumnID{2},
		},
		{ // 10
			c: "/1/2/3/4: [/1/1/2/1 - /1/1/2/1] [/3/1/3/1 - /3/1/3/1]",
			e: []opt.ColumnID{2, 4},
		},
		{ // 11
			c: "/1/-2/-3: [/1/1/2 - /1/1] [/3/3/4 - /3/3/3]",
			e: []opt.ColumnID{},
		},
		{ // 12
			c: "/1/2/3: [/1/1/1 - /1/1/2] [/1/3/3 - /1/3/4] [/1/4 - /1/4/1]",
			e: []opt.ColumnID{1},
		},
		{ // 13
			c: "/1/2/3: [/1/1/NULL - /1/1/NULL] [/3/3/NULL - /3/3/NULL]",
			e: []opt.ColumnID{3},
		},
		{ // 14
			c: "/1/2/3: [/1/1/1 - /1/1/1] [/4/1 - /5/1]",
			e: []opt.ColumnID{},
		},
	}

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			c := ParseConstraint(&evalCtx, tc.c)
			cols := c.ExtractConstCols(&evalCtx)
			if exp := opt.MakeColSet(tc.e...); !cols.Equals(exp) {
				t.Errorf("expected %s; got %s", exp, cols)
			}
		})
	}
}

func TestExtractNotNullCols(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	testData := []struct {
		c string
		// expected value
		e []opt.ColumnID
	}{
		{ // 0
			c: "/1: [/2 - ]",
			e: []opt.ColumnID{1},
		},
		{ // 1
			c: "/1: [ - /2]",
			e: []opt.ColumnID{},
		},
		{ // 2
			c: "/1: [/NULL - /4]",
			e: []opt.ColumnID{},
		},
		{ // 3
			c: "/1: (/NULL - /4]",
			e: []opt.ColumnID{1},
		},
		{ // 4
			c: "/-1: [ - /2]",
			e: []opt.ColumnID{1},
		},
		{ // 5
			c: "/-1: [/2 - ]",
			e: []opt.ColumnID{},
		},
		{ // 6
			c: "/-1: [/4 - /NULL]",
			e: []opt.ColumnID{},
		},
		{ // 7
			c: "/-1: [/4 - /NULL)",
			e: []opt.ColumnID{1},
		},
		{ // 8
			c: "/1/2/3: [/1/1/1 - /1/1/2] [/3/3/3 - /3/3/4]",
			e: []opt.ColumnID{1, 2, 3},
		},
		{ // 9
			c: "/1/2/3/4: [/1/1/1/1 - /1/1/2/1] [/3/3/3/1 - /3/3/4/1]",
			e: []opt.ColumnID{1, 2, 3},
		},
		{ // 10
			c: "/1/2/3: [/1/1 - /1/1/2] [/3/3/3 - /3/3/4]",
			e: []opt.ColumnID{1, 2},
		},
		{ // 11
			c: "/1/-2/-3: [/1/1/2 - /1/1] [/3/3/4 - /3/3/3]",
			e: []opt.ColumnID{1, 2},
		},
		{ // 12
			c: "/1/2/3: [/1/1/1 - /1/1/2] [/3/3/3 - /3/3/4] [/4/4/1 - /5]",
			e: []opt.ColumnID{1},
		},
		{ // 13
			c: "/1/2/3: [/1/1/NULL - /1/1/2] [/3/3/3 - /3/3/4]",
			e: []opt.ColumnID{1, 2},
		},
		{ // 14
			c: "/1/2/3: [/1/1/1 - /1/1/1] [/2/NULL/2 - /2/NULL/3]",
			e: []opt.ColumnID{1, 3},
		},
	}

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			c := ParseConstraint(&evalCtx, tc.c)
			cols := c.ExtractNotNullCols(&evalCtx)
			if exp := opt.MakeColSet(tc.e...); !cols.Equals(exp) {
				t.Errorf("expected %s; got %s", exp, cols)
			}
		})
	}
}

func TestCollectFirstColumnValues(t *testing.T) {
	test := func(t *testing.T, evalCtx *tree.EvalContext, testConstraint *Constraint, expected string, hasNull bool) {
		t.Helper()

		var values tree.Datums
		var hasNullValue, ok bool
		values, hasNullValue, ok = testConstraint.CollectFirstColumnValues(evalCtx)
		if !ok {
			if expected != "()" {
				format := "Failed to collect values from constraint: %s  Expected values: %v"
				t.Errorf(format, testConstraint.String(), expected)
			}
		}
		actual := values.String()

		if hasNull != hasNullValue {
			format := "Collect values hasNull mismatch for constraint: %s. expected: %t, actual: %t"
			t.Errorf(format, testConstraint.String(), hasNull, hasNullValue)
		}

		if actual != expected {
			format := "testConstraint: %s, expected: %v, actual: %v"
			t.Errorf(format, testConstraint.String(), expected, actual)
		}
	}
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	testData := []struct {
		spans          string
		expectedValues string
		hasNull        bool
	}{
		{ // 0
			spans:          "/1: [/2 - ]",
			expectedValues: "()",
			hasNull:        false,
		},
		{ // 1
			spans:          "/1: [/2 - /2]",
			expectedValues: "(2)",
			hasNull:        false,
		},
		{ // 2
			spans:          "/1: [/NULL - /4]",
			expectedValues: "()",
			hasNull:        false,
		},
		{ // 3
			spans:          "/1: [/1 - /4]",
			expectedValues: "(1, 2, 3, 4)",
			hasNull:        false,
		},
		{ // 4
			spans:          "/1: [/1 - /4] [/5 - /5] [/7 - /8]",
			expectedValues: "(1, 2, 3, 4, 5, 7, 8)",
			hasNull:        false,
		},
		{ // 5
			spans:          "/1: (/1 - /4]",
			expectedValues: "()",
			hasNull:        false,
		},
		{ // 6
			spans:          "/1: [/NULL - /NULL]",
			expectedValues: "(NULL)",
			hasNull:        true,
		},
		{ // 7
			spans:          "/1/2: [/NULL/1 - /NULL/3]",
			expectedValues: "(NULL)",
			hasNull:        true,
		},
		{ // 8
			spans:          "/1/2: [/1/NULL - /2/NULL]",
			expectedValues: "(1, 2)",
			hasNull:        false,
		},
		{ // 9
			spans:          "/1/2/3/4: [/1/1/1/1 - /1/1/2/1] [/3/3/3/1 - /3/3/4/1]",
			expectedValues: "(1, 3)",
			hasNull:        false,
		},
		{ // 10
			spans:          "/1/2/3: [/1/1/NULL - /1/1/2] [/3/3/3 - /3/3/4]",
			expectedValues: "(1, 3)",
			hasNull:        false,
		},
		{ // 11
			spans:          "/1/2/3: [/1/1 - /1/1] [/1/3 - /1/3]",
			expectedValues: "(1)",
			hasNull:        false,
		},
	}

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			c := ParseConstraint(&evalCtx, tc.spans)
			test(t, &evalCtx, &c, tc.expectedValues, tc.hasNull)
		})
	}
}
