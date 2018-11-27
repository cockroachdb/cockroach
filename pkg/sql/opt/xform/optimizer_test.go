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

package xform_test

import (
	"context"
	"flag"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
)

func TestDetachMemo(t *testing.T) {
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := parser.ParseOne("SELECT * FROM abc WHERE c=$1")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	semaCtx := tree.MakeSemaContext(false /* privileged */)
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	var o xform.Optimizer
	o.Init(&evalCtx)
	err = optbuilder.New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt).Build()
	if err != nil {
		t.Fatal(err)
	}

	before := o.DetachMemo()

	if !o.Memo().IsEmpty() {
		t.Error("memo expression should be reinitialized by DetachMemo")
	}

	semaCtx.Placeholders.Clear()
	o.Init(&evalCtx)

	stmt2, err := parser.ParseOne("SELECT a=$1 FROM abc")
	if err != nil {
		t.Fatal(err)
	}

	err = optbuilder.New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt2).Build()
	if err != nil {
		t.Fatal(err)
	}

	after := o.Memo()
	if after == before {
		t.Error("after memo cannot be the same as the detached memo")
	}

	if !strings.Contains(after.RootExpr().String(), "variable: a [type=int]") {
		t.Error("after memo did not contain expected operator")
	}

	if after.RootExpr().(memo.RelExpr).Memo() != after {
		t.Error("after memo expression does not reference the after memo")
	}

	if before == o.Memo() {
		t.Error("detached memo should not be reused")
	}

	if before.RootExpr().(memo.RelExpr).Memo() != before {
		t.Error("detached memo expression does not reference the detached memo")
	}

	if !strings.Contains(before.RootExpr().String(), "variable: c [type=string]") {
		t.Error("detached memo did not contain expected operator")
	}
}

// TestCoster files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestCoster/sort"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestCoster/scan"
//   ...
func TestCoster(t *testing.T) {
	runDataDrivenTest(
		t, "testdata/coster/",
		memo.ExprFmtHideRuleProps|memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars,
	)
}

// TestPhysicalProps files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestPhysicalPropsFactory/ordering"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestPhysicalPropsFactory/presentation"
//   ...
func TestPhysicalProps(t *testing.T) {
	runDataDrivenTest(t, "testdata/physprops/", memo.ExprFmtHideAll)
}

// TestRuleProps files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRuleProps/orderings"
//   ...
func TestRuleProps(t *testing.T) {
	datadriven.Walk(t, "testdata/ruleprops", func(t *testing.T, path string) {
		catalog := testcat.New()
		datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
			tester := testutils.NewOptTester(catalog, d.Input)
			tester.Flags.ExprFormat = memo.ExprFmtHideStats | memo.ExprFmtHideCost |
				memo.ExprFmtHideQualifications | memo.ExprFmtHideScalars
			return tester.RunCommand(t, d)
		})
	})
}

// TestRules files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRules/scan"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRules/select"
//   ...
func TestRules(t *testing.T) {
	runDataDrivenTest(
		t,
		"testdata/rules/",
		memo.ExprFmtHideStats|memo.ExprFmtHideCost|memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars,
	)
}

var externalTestData = flag.String(
	"d", "testdata/external/", "test files directory for TestExternal",
)

// TestExternal contains test cases from external customers and external
// benchmarks (like TPCH), so that changes in their query plans can be monitored
// over time.
//
// TestExternal files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestExternal/tpch"
//   ...
//
// Test files from another location can be run using the -d flag:
//   make test PKG=./pkg/sql/opt/xform TESTS=TestExternal TESTFLAGS='-d /some-dir'
//
func TestExternal(t *testing.T) {
	runDataDrivenTest(
		t,
		*externalTestData,
		memo.ExprFmtHideStats|memo.ExprFmtHideCost|memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars,
	)
}

// runDataDrivenTest runs data-driven testcases of the form
//   <command>
//   <SQL statement>
//   ----
//   <expected results>
//
// See OptTester.Handle for supported commands.
func runDataDrivenTest(t *testing.T, path string, fmtFlags memo.ExprFmtFlags) {
	datadriven.Walk(t, path, func(t *testing.T, path string) {
		catalog := testcat.New()
		datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
			tester := testutils.NewOptTester(catalog, d.Input)
			tester.Flags.ExprFormat = fmtFlags
			return tester.RunCommand(t, d)
		})
	})
}
