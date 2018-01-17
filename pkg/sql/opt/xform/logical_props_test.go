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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

func TestLogicalScanProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := newTestFactory()
	a := f.constructTableA()

	// (Scan a)
	scanGroup := f.ConstructScan(f.InternPrivate(a))

	expected := "columns: a.x:1 a.y:null:2\n"
	testLogicalProps(t, f.mem, scanGroup, expected)
}

func TestLogicalSelectProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := newTestFactory()
	a := f.constructTableA()

	// (Select (Scan a) (True))
	scanGroup := f.ConstructScan(f.InternPrivate(a))
	filterGroup := f.ConstructTrue()
	selGroup := f.ConstructSelect(scanGroup, filterGroup)

	expected := "columns: a.x:1 a.y:null:2\n"
	testLogicalProps(t, f.mem, selGroup, expected)
}

func TestLogicalProjectProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := newTestFactory()
	b := f.constructTableB()

	// (Project (Scan b) (Projections [(Variable b.y) (False)]))
	scanGroup := f.ConstructScan(f.InternPrivate(b))

	col1 := f.Metadata().TableColumn(b, 1 /* ord */)
	varGroup := f.ConstructVariable(f.InternPrivate(col1))
	col2 := f.Metadata().AddColumn("false")
	items := f.StoreList([]GroupID{varGroup, f.ConstructFalse()})
	cols := util.MakeFastIntSet(int(col1), int(col2))
	projectionsGroup := f.ConstructProjections(items, f.InternPrivate(&cols))

	projectGroup := f.ConstructProject(scanGroup, projectionsGroup)

	expected := "columns: b.z:2 false:null:3\n"
	testLogicalProps(t, f.mem, projectGroup, expected)

	// Make sure that not null columns are subset of output columns.
	notNullCols := f.mem.lookupGroup(projectGroup).logical.Relational.NotNullCols
	if notNullCols.String() != "(2)" {
		t.Fatalf("unexpected not null cols: %s", notNullCols.String())
	}
}

func TestLogicalJoinProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := newTestFactory()
	a := f.constructTableA()
	b := f.constructTableB()

	joinFunc := func(op Operator, expected string) {
		t.Helper()

		// (Join (Scan a) (Scan b) (True))
		leftGroup := f.ConstructScan(f.InternPrivate(a))
		rightGroup := f.ConstructScan(f.InternPrivate(b))
		onGroup := f.ConstructTrue()
		children := []GroupID{leftGroup, rightGroup, onGroup}
		joinGroup := f.DynamicConstruct(op, children, 0 /* private */)

		testLogicalProps(t, f.mem, joinGroup, expected)
	}

	joinFunc(InnerJoinOp, "columns: a.x:1 a.y:null:2 b.x:3 b.z:4\n")
	joinFunc(InnerJoinApplyOp, "columns: a.x:1 a.y:null:2 b.x:3 b.z:4\n")
	joinFunc(LeftJoinOp, "columns: a.x:1 a.y:null:2 b.x:null:3 b.z:null:4\n")
	joinFunc(LeftJoinApplyOp, "columns: a.x:1 a.y:null:2 b.x:null:3 b.z:null:4\n")
	joinFunc(RightJoinOp, "columns: a.x:null:1 a.y:null:2 b.x:3 b.z:4\n")
	joinFunc(RightJoinApplyOp, "columns: a.x:null:1 a.y:null:2 b.x:3 b.z:4\n")
	joinFunc(FullJoinOp, "columns: a.x:null:1 a.y:null:2 b.x:null:3 b.z:null:4\n")
	joinFunc(FullJoinApplyOp, "columns: a.x:null:1 a.y:null:2 b.x:null:3 b.z:null:4\n")
	joinFunc(SemiJoinOp, "columns: a.x:1 a.y:null:2\n")
	joinFunc(SemiJoinApplyOp, "columns: a.x:1 a.y:null:2\n")
	joinFunc(AntiJoinOp, "columns: a.x:1 a.y:null:2\n")
	joinFunc(AntiJoinApplyOp, "columns: a.x:1 a.y:null:2\n")
}

func TestLogicalGroupByProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := newTestFactory()
	a := f.constructTableA()

	// (GroupBy (Scan a) (Projections [(Variable a.y)]) (Projections [(False)]))
	scanGroup := f.ConstructScan(f.InternPrivate(a))

	col1 := f.Metadata().TableColumn(a, 1 /* ord */)
	varGroup := f.ConstructVariable(f.InternPrivate(col1))
	items1 := f.StoreList([]GroupID{varGroup})
	cols1 := util.MakeFastIntSet(int(col1))
	groupingsGroup := f.ConstructProjections(items1, f.InternPrivate(&cols1))

	col2 := f.Metadata().AddColumn("false")
	items2 := f.StoreList([]GroupID{f.ConstructFalse()})
	cols2 := util.MakeFastIntSet(int(col2))
	aggsGroup := f.ConstructProjections(items2, f.InternPrivate(&cols2))

	groupByGroup := f.ConstructGroupBy(scanGroup, groupingsGroup, aggsGroup)

	expected := "columns: a.y:null:2 false:null:3\n"
	testLogicalProps(t, f.mem, groupByGroup, expected)
}

func TestLogicalSetProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := newTestFactory()
	a := f.constructTableA()
	b := f.constructTableB()

	// (Union (Scan b) (Scan a))
	leftGroup := f.ConstructScan(f.InternPrivate(b))
	rightGroup := f.ConstructScan(f.InternPrivate(a))

	a0 := f.Metadata().TableColumn(a, 0 /* ord */)
	a1 := f.Metadata().TableColumn(a, 1 /* ord */)
	b0 := f.Metadata().TableColumn(b, 0 /* ord */)
	b1 := f.Metadata().TableColumn(b, 1 /* ord */)
	colMap := f.InternPrivate(&ColMap{b0: a1, b1: a0})

	unionGroup := f.ConstructUnion(leftGroup, rightGroup, colMap)

	expected := "columns: b.x:null:3 b.z:4\n"
	testLogicalProps(t, f.mem, unionGroup, expected)
}

func TestLogicalValuesProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := newTestFactory()
	a := f.constructTableA()

	// (Values)
	rows := f.StoreList(nil)
	a0 := f.Metadata().TableColumn(a, 0 /* ord */)
	a1 := f.Metadata().TableColumn(a, 1 /* ord */)
	cols := util.MakeFastIntSet(int(a0), int(a1))
	valuesGroup := f.ConstructValues(rows, f.InternPrivate(&cols))

	expected := "columns: a.x:null:1 a.y:null:2\n"
	testLogicalProps(t, f.mem, valuesGroup, expected)
}

func testLogicalProps(t *testing.T, mem *memo, group GroupID, expected string) {
	t.Helper()

	tp := treeprinter.New()
	mem.lookupGroup(group).logical.format(mem, tp)
	actual := tp.String()

	if actual != expected {
		t.Fatalf("\nexpected: %sactual  : %s", expected, actual)
	}
}
