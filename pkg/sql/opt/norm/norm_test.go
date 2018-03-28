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

package norm_test

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
)

// TestNormRules runs data-driven testcases of the form:
//   <command>
//   <SQL statement>
//   ----
//   <expected results>
//
// The supported commands are:
//
//  - exec-ddl
//
//    Runs a SQL DDL statement to build the test catalog. Only a small number
//    of DDL statements are supported, and those not fully.
//
//  - build
//
//    Builds an expression tree from a SQL query and outputs it without any
//    optimizations applied to it.
//
//  - opt
//
//    Builds an expression tree from a SQL query, fully optimizes it using the
//    memo, and then outputs the lowest cost tree.
//
//  - optsteps
//
//    Outputs the lowest cost tree for each step in optimization using the
//    standard unified diff format. Used for debugging the optimizer.
//
//  - memo
//
//    Builds an expression tree from a SQL query, fully optimizes it using the
//    memo, and then outputs the memo containing the forest of trees.
//
// Rules files can be run separately like this:
//   make test PKG=./pkg/sql/opt/norm TESTS="TestRules/bool"
//   make test PKG=./pkg/sql/opt/norm TESTS="TestRules/comp"
//   ...
func TestNormRules(t *testing.T) {
	const testdataGlob = "testdata/*"
	const fmtFlags = memo.ExprFmtHideStats | memo.ExprFmtHideCost

	for _, path := range testutils.GetTestFiles(t, testdataGlob) {
		catalog := testutils.NewTestCatalog()
		t.Run(filepath.Base(path), func(t *testing.T) {
			datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
				tester := testutils.NewOptTester(catalog, d.Input)
				switch d.Cmd {
				case "exec-ddl":
					return testutils.ExecuteTestDDL(t, d.Input, catalog)

				case "build":
					ev, err := tester.OptBuild()
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					return ev.FormatString(fmtFlags)

				case "opt":
					ev, err := tester.Optimize()
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					return ev.FormatString(fmtFlags)

				case "optsteps":
					result, err := tester.OptSteps(fmtFlags, testing.Verbose())
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					return result

				case "memo":
					result, err := tester.Memo()
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					return result

				default:
					d.Fatalf(t, "unsupported command: %s", d.Cmd)
					return ""
				}
			})
		})
	}
}

// Test the FoldNullInEmpty rule. Can't create empty tuple on right side of
// IN/NOT IN in SQL, so do it here.
func TestRuleFoldNullInEmpty(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	f := norm.NewFactory(&evalCtx)

	null := f.ConstructNull(f.InternType(types.Unknown))
	empty := f.ConstructTuple(memo.EmptyList)
	in := f.ConstructIn(null, empty)
	ev := memo.MakeNormExprView(f.Memo(), in)
	if ev.Operator() != opt.FalseOp {
		t.Errorf("expected NULL IN () to fold to False")
	}

	notIn := f.ConstructNotIn(null, empty)
	ev = memo.MakeNormExprView(f.Memo(), notIn)
	if ev.Operator() != opt.TrueOp {
		t.Errorf("expected NULL NOT IN () to fold to True")
	}
}

// Ensure that every binary commutative operator overload can have its operands
// switched. Patterns like CommuteConst rely on this being possible.
func TestRuleBinaryAssumption(t *testing.T) {
	fn := func(op opt.Operator) {
		for _, overload := range tree.BinOps[opt.BinaryOpReverseMap[op]] {
			binOp := overload.(tree.BinOp)
			if !memo.BinaryOverloadExists(op, binOp.RightType, binOp.LeftType) {
				t.Errorf("could not find inverse for overload: %+v", op)
			}
		}
	}

	// Only include commutative binary operators.
	fn(opt.PlusOp)
	fn(opt.MultOp)
	fn(opt.BitandOp)
	fn(opt.BitorOp)
	fn(opt.BitxorOp)
}

func TestDynamicListID(t *testing.T) {
	listID := memo.ListID{Offset: 1, Length: 2}
	dynID := norm.MakeDynamicListID(listID)
	roundtripID := dynID.ListID()
	if listID != roundtripID {
		t.Errorf("invalid ListID/DynamicID conversions")
	}
}

// TestSimplifyFilters tests factory.simplifyFilters. It's hard to fully test
// using SQL, as And operator rules simplify the expression before the Filters
// operator is created.
func TestSimplifyFilters(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	f := norm.NewFactory(&evalCtx)

	cat := createFiltersCatalog(t)
	a := f.Metadata().AddTable(cat.Table("a"))
	ax := f.Metadata().TableColumn(a, 0)

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

func createFiltersCatalog(t *testing.T) *testutils.TestCatalog {
	cat := testutils.NewTestCatalog()
	testutils.ExecuteTestDDL(t, "CREATE TABLE a (x INT PRIMARY KEY, y INT)", cat)
	return cat
}
