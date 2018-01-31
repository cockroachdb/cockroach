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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/build"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/datadriven"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
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

		o := NewOptimizer(createTypingCatalog(), 0 /* maxSteps */)
		b := build.NewBuilder(context.Background(), o.Factory(), stmt)
		root, props, err := b.Build()
		if err != nil {
			d.Fatalf(t, "%v", err)
		}

		ev := o.Optimize(root, props)
		return ev.String()
	})
}

func TestTypingTrueFalse(t *testing.T) {
	o := NewOptimizer(createTypingCatalog(), 0 /* maxSteps */)
	f := o.Factory()

	// (True)
	ev := o.Optimize(f.ConstructTrue(), &opt.PhysicalProps{})
	testTyping(t, ev, types.Bool)

	// (False)
	ev = o.Optimize(f.ConstructFalse(), &opt.PhysicalProps{})
	testTyping(t, ev, types.Bool)
}

func TestTypingJson(t *testing.T) {
	o := NewOptimizer(createTypingCatalog(), 0 /* maxSteps */)
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
