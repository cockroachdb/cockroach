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

package xform

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
)

func TestTyping(t *testing.T) {
	datadriven.RunTest(t, "testdata/typing", func(d *datadriven.TestData) string {
		// Only compile command supported.
		if d.Cmd != "build" {
			t.FailNow()
		}

		stmt, err := parser.ParseOne(d.Input)
		if err != nil {
			d.Fatalf(t, "%v", err)
		}

		o := NewOptimizer(createTypingCatalog(), OptimizeNone)
		b := optbuilder.New(context.Background(), o.Factory(), stmt)
		root, props, err := b.Build()
		if err != nil {
			d.Fatalf(t, "%v", err)
		}

		ev := o.Optimize(root, props)
		return ev.String()
	})
}

func TestTypingTrueFalse(t *testing.T) {
	o := NewOptimizer(createTypingCatalog(), OptimizeNone)
	f := o.Factory()

	// (True)
	ev := o.Optimize(f.ConstructTrue(), &opt.PhysicalProps{})
	testTyping(t, ev, types.Bool)

	// (False)
	ev = o.Optimize(f.ConstructFalse(), &opt.PhysicalProps{})
	testTyping(t, ev, types.Bool)
}

func TestTypingJson(t *testing.T) {
	o := NewOptimizer(createTypingCatalog(), OptimizeNone)
	f := o.Factory()

	// (Const <json>)
	json, _ := tree.ParseDJSON("[1, 2]")
	jsonGroup := f.ConstructConst(f.InternPrivate(json))

	// (Const <int>)
	intGroup := f.ConstructConst(f.InternPrivate(tree.NewDInt(1)))

	// (Const <string-array>)
	arrGroup := f.ConstructConst(f.InternPrivate(tree.NewDArray(types.String)))

	// (FetchVal (Const <json>) (Const <int>))
	fetchValGroup := f.ConstructFetchVal(jsonGroup, intGroup)
	ev := o.Optimize(fetchValGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.JSON)

	// (FetchValPath (Const <json>) (Const <string-array>))
	fetchValPathGroup := f.ConstructFetchValPath(jsonGroup, arrGroup)
	ev = o.Optimize(fetchValPathGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.JSON)

	// (FetchText (Const <json>) (Const <int>))
	fetchTextGroup := f.ConstructFetchText(jsonGroup, intGroup)
	ev = o.Optimize(fetchTextGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.String)

	// (FetchTextPath (Const <json>) (Const <string-array>))
	fetchTextPathGroup := f.ConstructFetchTextPath(jsonGroup, arrGroup)
	ev = o.Optimize(fetchTextPathGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.String)
}

func TestTypingFunction(t *testing.T) {
	o := NewOptimizer(createTypingCatalog(), 0 /* maxSteps */)
	f := o.Factory()

	// Function with fixed return type.
	// (Function (Const "length") (Const "text"))
	nameGroup := f.ConstructConst(f.InternPrivate(tree.NewDString("length")))
	arg1Group := f.ConstructConst(f.InternPrivate(tree.NewDString("text")))
	argsList := f.InternList([]opt.GroupID{arg1Group})
	funcGroup := f.ConstructFunction(nameGroup, argsList)
	ev := o.Optimize(funcGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.Int)

	// Function with return type dependent on arg types.
	// (Function (Const "div") (Const 1.0) (Const 2.0))
	nameGroup = f.ConstructConst(f.InternPrivate(tree.NewDString("div")))
	arg1Group = f.ConstructConst(f.InternPrivate(tree.NewDFloat(1.0)))
	arg2Group := f.ConstructConst(f.InternPrivate(tree.NewDFloat(2.0)))
	argsList = f.InternList([]opt.GroupID{arg1Group, arg2Group})
	funcGroup = f.ConstructFunction(nameGroup, argsList)
	ev = o.Optimize(funcGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.Float)

	// Function with same arguments in multiple overloads.
	// (Function (Const "now::timestamp"))
	nameGroup = f.ConstructConst(f.InternPrivate(tree.NewDString("now:::timestamp")))
	funcGroup = f.ConstructFunction(nameGroup, opt.EmptyList)
	ev = o.Optimize(funcGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.Timestamp)

	// Function with same arguments in multiple overloads (different overload).
	// (Function (Const "now::timestamptz"))
	nameGroup = f.ConstructConst(f.InternPrivate(tree.NewDString("now:::timestamptz")))
	funcGroup = f.ConstructFunction(nameGroup, opt.EmptyList)
	ev = o.Optimize(funcGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.TimestampTZ)

	// Variadic function.
	// (Function (Const "greatest") (Const 1) (Const 2))
	nameGroup = f.ConstructConst(f.InternPrivate(tree.NewDString("greatest")))
	arg1Group = f.ConstructConst(f.InternPrivate(tree.NewDInt(1)))
	arg2Group = f.ConstructConst(f.InternPrivate(tree.NewDInt(2)))
	argsList = f.InternList([]opt.GroupID{arg1Group, arg2Group})
	funcGroup = f.ConstructFunction(nameGroup, argsList)
	ev = o.Optimize(funcGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.Int)
}

// This "test" is special, in that it is verifying that we don't add functions
// in the future that break assumptions in the optimizer's typing code.
func TestTypingAssumptions(t *testing.T) {
	// Assumption: It is possible to infer the return type from the types of
	//             function arguments for all but a small set of well-known
	//             functions.
	for name, builtin := range builtins.Builtins {
		// Skip past known ambiguous functions.
		if opt.DisambiguateFunction(name, types.Any) != name {
			continue
		}
		if opt.DisambiguateFunction(strings.ToLower(name), types.Any) != name {
			continue
		}

		seen := make([]tree.TypeList, 0)
		for _, overload := range builtin {
			for _, sig := range seen {
				if sig.Match(overload.Types.Types()) {
					t.Errorf("%s function has ambiguous arguments", name)
				}
			}
			seen = append(seen, overload.Types)
		}
	}
}

func createTypingCatalog() *testutils.TestCatalog {
	cat := testutils.NewTestCatalog()

	// CREATE TABLE a (x INT PRIMARY KEY, y INT)
	a := &testutils.TestTable{Name: "a"}
	x := &testutils.TestColumn{Name: "x", Type: types.Int}
	y := &testutils.TestColumn{Name: "y", Type: types.Int, Nullable: true}
	a.Columns = append(a.Columns, x, y)
	cat.AddTable(a)

	// CREATE TABLE b (x NVARCHAR PRIMARY KEY, z DECIMAL NOT NULL)
	b := &testutils.TestTable{Name: "b"}
	x = &testutils.TestColumn{Name: "x", Type: types.String}
	y = &testutils.TestColumn{Name: "z", Type: types.Decimal}
	b.Columns = append(b.Columns, x, y)
	cat.AddTable(b)

	return cat
}

func testTyping(t *testing.T, ev ExprView, expected types.T) {
	t.Helper()

	actual := ev.Logical().Scalar.Type

	if !actual.Equivalent(expected) {
		t.Fatalf("\nexpected: %s\nactual  : %s", expected, actual)
	}
}
