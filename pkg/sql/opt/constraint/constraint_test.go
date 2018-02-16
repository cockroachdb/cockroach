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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestConstraintUnion(t *testing.T) {
	test := func(t *testing.T, evalCtx *tree.EvalContext, left, right *Constraint, expected string) {
		t.Helper()
		clone := *left
		ok := clone.tryUnionWith(evalCtx, right)

		var actual string
		if ok {
			actual = clone.String()
		}

		if actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, clone.String())
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

	// Tight disjoint spans in each constraint.
	test(t, &evalCtx, &data.c1to10, &data.c20to30, "/1: [/1 - /10] [/20 - /30)")
	test(t, &evalCtx, &data.c20to30, &data.c1to10, "/1: [/1 - /10] [/20 - /30)")

	// Merge multiple spans down to single span.
	var left, right Constraint
	left = data.c1to10
	_ = left.tryUnionWith(&evalCtx, &data.c20to30)
	_ = left.tryUnionWith(&evalCtx, &data.c40to50)

	right = data.c5to25
	_ = right.tryUnionWith(&evalCtx, &data.c30to40)

	test(t, &evalCtx, &left, &right, "/1: [/1 - /50]")
	test(t, &evalCtx, &right, &left, "/1: [/1 - /50]")

	// Multiple disjoint spans on each side.
	left = data.c1to10
	_ = left.tryUnionWith(&evalCtx, &data.c20to30)

	right = data.c40to50
	_ = right.tryUnionWith(&evalCtx, &data.c60to70)

	test(t, &evalCtx, &left, &right, "/1: [/1 - /10] [/20 - /30) [/40 - /50] (/60 - /70)")
	test(t, &evalCtx, &right, &left, "/1: [/1 - /10] [/20 - /30) [/40 - /50] (/60 - /70)")

	// Multiple spans that yield the unconstrained span (should return ok=false
	// and not modify either span).
	left = data.cLt10
	right = data.c5to25
	_ = right.tryUnionWith(&evalCtx, &data.cGt20)

	test(t, &evalCtx, &left, &right, "")
	test(t, &evalCtx, &right, &left, "")

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
		ok := clone.tryIntersectWith(evalCtx, right)

		var actual string
		if ok {
			actual = clone.String()
		}

		if actual != expected {
			format := "left: %s, right: %s, expected: %v, actual: %v"
			t.Errorf(format, left.String(), right.String(), expected, clone.String())
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
	test(t, &evalCtx, &data.c1to10, &data.c40to50, "")
	test(t, &evalCtx, &data.c40to50, &data.c1to10, "")

	// Intersect multiple spans.
	var left, right Constraint
	left = data.c1to10
	_ = left.tryUnionWith(&evalCtx, &data.c20to30)
	_ = left.tryUnionWith(&evalCtx, &data.c40to50)

	right = data.c5to25
	_ = right.tryUnionWith(&evalCtx, &data.c30to40)

	test(t, &evalCtx, &right, &left, "/1: (/5 - /10] [/20 - /25) [/40 - /40]")
	test(t, &evalCtx, &left, &right, "/1: (/5 - /10] [/20 - /25) [/40 - /40]")

	// Intersect multiple disjoint spans (should return ok=false and not
	// modify either span).
	left = data.c1to10
	_ = left.tryUnionWith(&evalCtx, &data.c20to30)

	right = data.c40to50
	_ = right.tryUnionWith(&evalCtx, &data.c60to70)

	test(t, &evalCtx, &left, &right, "")
	test(t, &evalCtx, &right, &left, "")

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

type constraintTestData struct {
	cLt10           Constraint // [Ø - /10)
	cGt20           Constraint // (/20 - ∞]
	c1to10          Constraint // [/1 - /10]
	c5to25          Constraint // (/5 - /25)
	c20to30         Constraint // [/20 - /30)
	c30to40         Constraint // [/30 - /40]
	c40to50         Constraint // [/40 - /50]
	c60to70         Constraint // (/60 - /70)
	cherryRaspberry Constraint // [/'cherry'/true - /'raspberry'/false)
	mangoStrawberry Constraint // [/'mango'/true - /'strawberry']
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

	cherry := MakeCompositeKey(tree.NewDString("cherry"), tree.DBoolTrue)
	mango := MakeCompositeKey(tree.NewDString("mango"), tree.DBoolFalse)
	raspberry := MakeCompositeKey(tree.NewDString("raspberry"), tree.DBoolFalse)
	strawberry := MakeKey(tree.NewDString("strawberry"))

	var span Span

	// [Ø - /10)
	span.Set(evalCtx, EmptyKey, IncludeBoundary, key10, ExcludeBoundary)
	data.cLt10.init(1, &span)

	// (/20 - ∞]
	span.Set(evalCtx, key20, ExcludeBoundary, EmptyKey, IncludeBoundary)
	data.cGt20.init(1, &span)

	// [/1 - /10]
	span.Set(evalCtx, key1, IncludeBoundary, key10, IncludeBoundary)
	data.c1to10.init(1, &span)

	// (/5 - /25)
	span.Set(evalCtx, key5, ExcludeBoundary, key25, ExcludeBoundary)
	data.c5to25.init(1, &span)

	// [/20 - /30)
	span.Set(evalCtx, key20, IncludeBoundary, key30, ExcludeBoundary)
	data.c20to30.init(1, &span)

	// [/30 - /40]
	span.Set(evalCtx, key30, IncludeBoundary, key40, IncludeBoundary)
	data.c30to40.init(1, &span)

	// [/40 - /50]
	span.Set(evalCtx, key40, IncludeBoundary, key50, IncludeBoundary)
	data.c40to50.init(1, &span)

	// (/60 - /70)
	span.Set(evalCtx, key60, ExcludeBoundary, key70, ExcludeBoundary)
	data.c60to70.init(1, &span)

	// [/'cherry'/true - /'raspberry'/false)
	span.Set(evalCtx, cherry, IncludeBoundary, raspberry, ExcludeBoundary)
	data.cherryRaspberry.initComposite([]opt.ColumnIndex{1, 2}, &span)

	// [/'mango'/true - /'strawberry']
	span.Set(evalCtx, mango, IncludeBoundary, strawberry, IncludeBoundary)
	data.mangoStrawberry.initComposite([]opt.ColumnIndex{1, 2}, &span)

	return data
}
