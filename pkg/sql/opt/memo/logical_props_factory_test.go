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

package memo_test

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

func TestLogicalPropsFactory(t *testing.T) {
	runDataDrivenTest(t, "testdata/logprops/", memo.ExprFmtHideCost)
}

// Test joins that cannot yet be tested using SQL syntax + optimizer.
func TestLogicalJoinProps(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	f := norm.NewFactory(&evalCtx)

	// Disable all rules so that the expected operators are constructed.
	f.DisableOptimizations()

	cat := createLogPropsCatalog(t)
	a := f.Metadata().AddTable(cat.Table("a"))
	b := f.Metadata().AddTable(cat.Table("b"))

	joinFunc := func(op opt.Operator, expected string) {
		t.Helper()

		// (Join (Scan a) (Scan b) (True))
		leftGroup := f.ConstructScan(f.InternScanOpDef(constructScanOpDef(f.Metadata(), a)))
		rightGroup := f.ConstructScan(f.InternScanOpDef(constructScanOpDef(f.Metadata(), b)))
		onGroup := f.ConstructTrue()
		operands := norm.DynamicOperands{
			norm.DynamicID(leftGroup),
			norm.DynamicID(rightGroup),
			norm.DynamicID(onGroup),
		}
		joinGroup := f.DynamicConstruct(op, operands)

		ev := memo.MakeNormExprView(f.Memo(), joinGroup)
		testLogicalProps(t, f.Metadata(), ev, expected)
	}

	joinFunc(opt.InnerJoinApplyOp, "a.x:1(int!null) a.y:2(int) b.x:3(int!null) b.z:4(int!null)\n")
	joinFunc(opt.LeftJoinApplyOp, "a.x:1(int!null) a.y:2(int) b.x:3(int) b.z:4(int)\n")
	joinFunc(opt.RightJoinApplyOp, "a.x:1(int) a.y:2(int) b.x:3(int!null) b.z:4(int!null)\n")
	joinFunc(opt.FullJoinApplyOp, "a.x:1(int) a.y:2(int) b.x:3(int) b.z:4(int)\n")
	joinFunc(opt.SemiJoinOp, "a.x:1(int!null) a.y:2(int)\n")
	joinFunc(opt.SemiJoinApplyOp, "a.x:1(int!null) a.y:2(int)\n")
	joinFunc(opt.AntiJoinOp, "a.x:1(int!null) a.y:2(int)\n")
	joinFunc(opt.AntiJoinApplyOp, "a.x:1(int!null) a.y:2(int)\n")
}

func constructScanOpDef(md *opt.Metadata, tabID opt.TableID) *memo.ScanOpDef {
	def := memo.ScanOpDef{Table: tabID}
	for i := 0; i < md.Table(tabID).ColumnCount(); i++ {
		def.Cols.Add(int(md.TableColumn(tabID, i)))
	}
	return &def
}

func testLogicalProps(t *testing.T, md *opt.Metadata, ev memo.ExprView, expected string) {
	t.Helper()

	logical := ev.Logical()
	if logical.Relational == nil {
		panic("only relational properties are supported")
	}

	tp := treeprinter.New()
	logical.FormatColSet(tp, md, "", logical.Relational.OutputCols)
	actual := strings.Trim(tp.String(), " ")

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
