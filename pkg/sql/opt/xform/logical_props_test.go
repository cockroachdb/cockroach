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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

func TestLogicalProps(t *testing.T) {
	runDataDrivenTest(t, "testdata/logprops/*")
}

// Test joins that cannot yet be tested using SQL syntax + optimizer.
func TestLogicalJoinProps(t *testing.T) {
	cat := createLogPropsCatalog(t)
	f := newFactory(cat, 0)
	a := f.Metadata().AddTable(cat.Table("a"))
	b := f.Metadata().AddTable(cat.Table("b"))

	joinFunc := func(op opt.Operator, expected string) {
		t.Helper()

		// (Join (Scan a) (Scan b) (True))
		leftGroup := f.ConstructScan(f.InternPrivate(a))
		rightGroup := f.ConstructScan(f.InternPrivate(b))
		onGroup := f.ConstructTrue()
		children := []opt.GroupID{leftGroup, rightGroup, onGroup}
		joinGroup := f.DynamicConstruct(op, children, 0)

		testLogicalProps(t, f, joinGroup, expected)
	}

	joinFunc(opt.InnerJoinApplyOp, "columns: a.x:int:1 a.y:int:null:2 b.x:int:3 b.z:int:4\n")
	joinFunc(opt.LeftJoinApplyOp, "columns: a.x:int:1 a.y:int:null:2 b.x:int:null:3 b.z:int:null:4\n")
	joinFunc(opt.RightJoinApplyOp, "columns: a.x:int:null:1 a.y:int:null:2 b.x:int:3 b.z:int:4\n")
	joinFunc(opt.FullJoinApplyOp, "columns: a.x:int:null:1 a.y:int:null:2 b.x:int:null:3 b.z:int:null:4\n")
	joinFunc(opt.SemiJoinOp, "columns: a.x:int:1 a.y:int:null:2\n")
	joinFunc(opt.SemiJoinApplyOp, "columns: a.x:int:1 a.y:int:null:2\n")
	joinFunc(opt.AntiJoinOp, "columns: a.x:int:1 a.y:int:null:2\n")
	joinFunc(opt.AntiJoinApplyOp, "columns: a.x:int:1 a.y:int:null:2\n")
}

func TestLogicalSetProps(t *testing.T) {
	cat := createLogPropsCatalog(t)
	f := newFactory(cat, 0)
	a := f.Metadata().AddTable(cat.Table("a"))
	b := f.Metadata().AddTable(cat.Table("b"))

	// (Union (Scan b) (Scan a))
	leftGroup := f.ConstructScan(f.InternPrivate(b))
	rightGroup := f.ConstructScan(f.InternPrivate(a))

	a0 := f.Metadata().TableColumn(a, 0)
	a1 := f.Metadata().TableColumn(a, 1)
	b0 := f.Metadata().TableColumn(b, 0)
	b1 := f.Metadata().TableColumn(b, 1)
	colMap := &opt.ColMap{}
	colMap.Set(int(b0), int(a1))
	colMap.Set(int(b1), int(a0))

	unionGroup := f.ConstructUnion(leftGroup, rightGroup, f.InternPrivate(colMap))

	expected := "columns: b.x:int:null:3 b.z:int:4\n"
	testLogicalProps(t, f, unionGroup, expected)
}

func TestLogicalValuesProps(t *testing.T) {
	cat := createLogPropsCatalog(t)
	f := newFactory(cat, 0)
	a := f.Metadata().AddTable(cat.Table("a"))

	// (Values)
	rows := f.InternList(nil)
	a0 := f.Metadata().TableColumn(a, 0)
	a1 := f.Metadata().TableColumn(a, 1)
	cols := opt.ColList{a0, a1}
	valuesGroup := f.ConstructValues(rows, f.InternPrivate(&cols))

	expected := "columns: a.x:int:null:1 a.y:int:null:2\n"
	testLogicalProps(t, f, valuesGroup, expected)
}

func testLogicalProps(t *testing.T, f *factory, group opt.GroupID, expected string) {
	t.Helper()

	tp := treeprinter.New()
	f.mem.lookupGroup(group).logical.format(f.Metadata(), tp)
	actual := tp.String()

	if actual != expected {
		t.Fatalf("\nexpected: %s\nactual  : %s", expected, actual)
	}
}

func createLogPropsCatalog(t *testing.T) *testutils.TestCatalog {
	cat := testutils.NewTestCatalog()
	testutils.ExecuteTestDDL(t, "CREATE TABLE a (x INT PRIMARY KEY, y INT)", cat)
	testutils.ExecuteTestDDL(t, "CREATE TABLE b (x INT PRIMARY KEY, z INT NOT NULL)", cat)
	return cat
}
