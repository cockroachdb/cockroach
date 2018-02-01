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
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/build"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/datadriven"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

func TestLogicalProps(t *testing.T) {
	datadriven.RunTest(t, "testdata/logical_props", func(d *datadriven.TestData) string {
		// Only props command supported.
		if d.Cmd != "props" {
			t.FailNow()
		}

		stmt, err := parser.ParseOne(d.Input)
		if err != nil {
			d.Fatalf(t, "%v", err)
		}

		f := newFactory(createCatalog(), 0)
		b := build.NewBuilder(context.Background(), f, stmt)
		root, _, err := b.Build()
		if err != nil {
			d.Fatalf(t, "%v", err)
		}

		return formatProps(f, root)
	})
}

func TestLogicalJoinProps(t *testing.T) {
	cat := createCatalog()
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

	joinFunc(opt.InnerJoinOp, "columns: a.x:1 a.y:null:2 b.x:3 b.z:4\n")
	joinFunc(opt.InnerJoinApplyOp, "columns: a.x:1 a.y:null:2 b.x:3 b.z:4\n")
	joinFunc(opt.LeftJoinOp, "columns: a.x:1 a.y:null:2 b.x:null:3 b.z:null:4\n")
	joinFunc(opt.LeftJoinApplyOp, "columns: a.x:1 a.y:null:2 b.x:null:3 b.z:null:4\n")
	joinFunc(opt.RightJoinOp, "columns: a.x:null:1 a.y:null:2 b.x:3 b.z:4\n")
	joinFunc(opt.RightJoinApplyOp, "columns: a.x:null:1 a.y:null:2 b.x:3 b.z:4\n")
	joinFunc(opt.FullJoinOp, "columns: a.x:null:1 a.y:null:2 b.x:null:3 b.z:null:4\n")
	joinFunc(opt.FullJoinApplyOp, "columns: a.x:null:1 a.y:null:2 b.x:null:3 b.z:null:4\n")
	joinFunc(opt.SemiJoinOp, "columns: a.x:1 a.y:null:2\n")
	joinFunc(opt.SemiJoinApplyOp, "columns: a.x:1 a.y:null:2\n")
	joinFunc(opt.AntiJoinOp, "columns: a.x:1 a.y:null:2\n")
	joinFunc(opt.AntiJoinApplyOp, "columns: a.x:1 a.y:null:2\n")
}

func TestLogicalGroupByProps(t *testing.T) {
	cat := createCatalog()
	f := newFactory(cat, 0)
	a := f.Metadata().AddTable(cat.Table("a"))

	// (GroupBy (Scan a) (Projections [(Variable a.y)]) (Projections [(False)]))
	scanGroup := f.ConstructScan(f.InternPrivate(a))

	col1 := f.Metadata().TableColumn(a, 1)
	varGroup := f.ConstructVariable(f.InternPrivate(col1))
	items1 := f.StoreList([]opt.GroupID{varGroup})
	cols1 := util.MakeFastIntSet(int(col1))
	groupingsGroup := f.ConstructProjections(items1, f.InternPrivate(&cols1))

	col2 := f.Metadata().AddColumn("false")
	items2 := f.StoreList([]opt.GroupID{f.ConstructFalse()})
	cols2 := util.MakeFastIntSet(int(col2))
	aggsGroup := f.ConstructProjections(items2, f.InternPrivate(&cols2))

	groupByGroup := f.ConstructGroupBy(scanGroup, groupingsGroup, aggsGroup)

	expected := "columns: a.y:null:2 false:null:3\n"
	testLogicalProps(t, f, groupByGroup, expected)
}

func TestLogicalSetProps(t *testing.T) {
	cat := createCatalog()
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

	expected := "columns: b.x:null:3 b.z:4\n"
	testLogicalProps(t, f, unionGroup, expected)
}

func TestLogicalValuesProps(t *testing.T) {
	cat := createCatalog()
	f := newFactory(cat, 0)
	a := f.Metadata().AddTable(cat.Table("a"))

	// (Values)
	rows := f.StoreList(nil)
	a0 := f.Metadata().TableColumn(a, 0)
	a1 := f.Metadata().TableColumn(a, 1)
	cols := util.MakeFastIntSet(int(a0), int(a1))
	valuesGroup := f.ConstructValues(rows, f.InternPrivate(&cols))

	expected := "columns: a.x:null:1 a.y:null:2\n"
	testLogicalProps(t, f, valuesGroup, expected)
}

func testLogicalProps(t *testing.T, f *factory, group opt.GroupID, expected string) {
	t.Helper()

	tp := treeprinter.New()
	f.mem.lookupGroup(group).logical.format(f.mem, tp)
	actual := tp.String()

	if actual != expected {
		t.Fatalf("\nexpected: %s\nactual  : %s", expected, actual)
	}
}

func formatProps(f *factory, group opt.GroupID) string {
	tp := treeprinter.New()
	md := f.Metadata()
	logical := f.mem.lookupGroup(group).logical

	if logical.Relational != nil {
		nd := tp.Child("relational")
		nd.Child(fmt.Sprintf("columns:%s", formatCols(md, logical.Relational.OutputCols)))
		nd.Child(fmt.Sprintf("not null:%s", formatCols(md, logical.Relational.NotNullCols)))
	}

	return tp.String()
}

func formatCols(md *opt.Metadata, cols opt.ColSet) string {
	var buf bytes.Buffer
	cols.ForEach(func(i int) {
		colIndex := opt.ColumnIndex(i)
		label := md.ColumnLabel(colIndex)
		fmt.Fprintf(&buf, " %s:%d", label, colIndex)
	})
	return buf.String()
}

func createCatalog() *testutils.TestCatalog {
	cat := testutils.NewTestCatalog()

	// CREATE TABLE a (x INT PRIMARY KEY, y INT)
	a := &testutils.TestTable{Name: "a"}
	x := &testutils.TestColumn{Name: "x"}
	y := &testutils.TestColumn{Name: "y", Nullable: true}
	a.Columns = append(a.Columns, x, y)
	cat.AddTable(a)

	// CREATE TABLE b (x INT, z INT NOT NULL, FOREIGN KEY (x) REFERENCES a (x))
	b := &testutils.TestTable{Name: "b"}
	x = &testutils.TestColumn{Name: "x"}
	y = &testutils.TestColumn{Name: "z"}
	b.Columns = append(b.Columns, x, y)
	cat.AddTable(b)

	return cat
}
