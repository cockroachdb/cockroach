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

package norm

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// TestFactoryProjectColsFromBoth unit tests the Factory.projectColsFromBoth
// method. This method is used in several decorrelation rules, and is difficult
// to fully test with data-driven rules tests.
func TestFactoryProjectColsFromBoth(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var f Factory
	f.Init(&evalCtx)
	pb := projectionsBuilder{f: &f}

	cat := createFiltersCatalog(t)
	a := f.Metadata().AddTable(cat.Table(tree.NewTableName("t", "a")))
	ax := a.ColumnID(0)
	ay := a.ColumnID(1)
	aCols := util.MakeFastIntSet(int(ax), int(ay))

	a2 := f.Metadata().AddTable(cat.Table(tree.NewTableName("t", "a")))
	a2x := a2.ColumnID(0)
	a2y := a2.ColumnID(1)
	a2Cols := util.MakeFastIntSet(int(a2x), int(a2y))

	scan := f.ConstructScan(f.InternScanOpDef(&memo.ScanOpDef{Table: a, Cols: aCols}))
	scan2 := f.ConstructScan(f.InternScanOpDef(&memo.ScanOpDef{Table: a2, Cols: a2Cols}))

	// Construct Projections with two passthrough columns.
	pb.addPassthroughCols(f.funcs.OutputCols(scan))
	passthroughProj := pb.buildProjections()

	// Construct Projections with one passthrough and two synthesized columns.
	plus := f.ConstructPlus(
		f.ConstructVariable(f.InternColumnID(ay)),
		f.ConstructConst(f.InternDatum(tree.NewDInt(1))),
	)
	plusACol := f.Metadata().AddColumn("plusA", types.Int)
	plusBCol := f.Metadata().AddColumn("plusB", types.Int)
	pb.addPassthroughCol(ax)
	pb.addSynthesized(plus, plusACol)
	pb.addSynthesized(plus, plusBCol)
	synthProj := pb.buildProjections()

	// Construct Projections with two partially overlapping synthesized columns.
	plusCCol := f.Metadata().AddColumn("plusC", types.Int)
	pb.addSynthesized(plus, plusBCol)
	pb.addSynthesized(plus, plusCCol)
	synth2Proj := pb.buildProjections()

	testCases := []struct {
		left     memo.GroupID
		right    memo.GroupID
		expected string
	}{
		{left: scan, right: scan, expected: "(1,2)"},
		{left: scan, right: scan2, expected: "(1-4)"},
		{left: scan2, right: scan, expected: "(1-4)"},

		{left: passthroughProj, right: passthroughProj, expected: "(1,2)"},
		{left: passthroughProj, right: scan, expected: "(1,2)"},
		{left: scan, right: passthroughProj, expected: "(1,2)"},
		{left: passthroughProj, right: scan2, expected: "(1-4)"},
		{left: scan2, right: passthroughProj, expected: "(1-4)"},

		{left: synthProj, right: synthProj, expected: "(1,5,6)"},
		{left: synthProj, right: passthroughProj, expected: "(1,2,5,6)"},
		{left: passthroughProj, right: synthProj, expected: "(1,2,5,6)"},
		{left: synthProj, right: scan, expected: "(1,2,5,6)"},
		{left: scan, right: synthProj, expected: "(1,2,5,6)"},
		{left: synthProj, right: scan2, expected: "(1,3-6)"},
		{left: scan2, right: synthProj, expected: "(1,3-6)"},

		{left: synthProj, right: synth2Proj, expected: "(1,5-7)"},
	}

	for _, tc := range testCases {
		combined := f.funcs.ProjectColsFromBoth(tc.left, tc.right)
		f.Memo().CheckExpr(memo.MakeNormExprID(combined))
		actual := f.funcs.OutputCols(combined).String()
		if actual != tc.expected {
			t.Errorf("expected: %s, actual: %s", tc.expected, actual)
		}

		def := f.funcs.ExtractProjectionsOpDef(f.Memo().NormExpr(combined).AsProjections().Def())
		expectedCount := def.PassthroughCols.Len() + len(def.SynthesizedCols)
		actualCount := f.funcs.OutputCols(combined).Len()
		if actualCount != expectedCount {
			t.Errorf("expected column count: %d, actual column count: %d", expectedCount, actualCount)
		}
	}
}

// TestSimplifyFilters tests factory.simplifyFilters. It's hard to fully test
// using SQL, as And operator rules simplify the expression before the Filters
// operator is created.
func TestSimplifyFilters(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var f Factory
	f.Init(&evalCtx)

	cat := createFiltersCatalog(t)
	a := f.Metadata().AddTable(cat.Table(tree.NewTableName("t", "a")))
	ax := a.ColumnID(0)

	variable := f.ConstructVariable(f.InternColumnID(ax))
	constant := f.ConstructConst(f.InternDatum(tree.NewDInt(1)))
	eq := f.ConstructEq(variable, constant)

	// Filters expression evaluates to False if any operand is False.
	conditions := []memo.GroupID{eq, f.ConstructFalse(), eq}
	result := f.ConstructFilters(f.InternList(conditions))
	ev := memo.MakeNormExprView(f.Memo(), result)
	if ev.Operator() != opt.FalseOp {
		t.Fatalf("result should have been False")
	}

	// Filters expression evaluates to False if any operand is Null.
	conditions = []memo.GroupID{f.ConstructNull(f.InternType(types.Unknown)), eq, eq}
	result = f.ConstructFilters(f.InternList(conditions))
	ev = memo.MakeNormExprView(f.Memo(), result)
	if ev.Operator() != opt.FalseOp {
		t.Fatalf("result should have been False")
	}

	// Filters operator skips True operands.
	conditions = []memo.GroupID{eq, f.ConstructTrue(), eq, f.ConstructTrue()}
	result = f.ConstructFilters(f.InternList(conditions))
	ev = memo.MakeNormExprView(f.Memo(), result)
	if ev.Operator() != opt.FiltersOp || ev.ChildCount() != 2 {
		t.Fatalf("filters result should have filtered True operators")
	}

	// Filters operator flattens nested And operands.
	conditions = []memo.GroupID{eq, eq}
	and := f.ConstructAnd(f.InternList(conditions))
	conditions = []memo.GroupID{and, eq, and}
	result = f.ConstructFilters(f.InternList(conditions))
	ev = memo.MakeNormExprView(f.Memo(), result)
	if ev.Operator() != opt.FiltersOp || ev.ChildCount() != 5 {
		t.Fatalf("result should have flattened And operators")
	}
}

func createFiltersCatalog(t *testing.T) *testcat.Catalog {
	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE a (x INT PRIMARY KEY, y INT)"); err != nil {
		t.Fatal(err)
	}
	return cat
}
