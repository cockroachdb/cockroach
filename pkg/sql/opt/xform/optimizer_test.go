// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform_test

import (
	"flag"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/opttester"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestDetachMemo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	catalog := testcat.New()
	if _, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))"); err != nil {
		t.Fatal(err)
	}

	var o xform.Optimizer
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	testutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM abc WHERE c=$1")

	before := o.DetachMemo()

	if !o.Memo().IsEmpty() {
		t.Error("memo expression should be reinitialized by DetachMemo")
	}

	testutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT a=$1 FROM abc")

	after := o.Memo()
	if after == before {
		t.Error("after memo cannot be the same as the detached memo")
	}

	if !strings.Contains(after.RootExpr().String(), "variable: a:1 [type=int]") {
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

	if !strings.Contains(before.RootExpr().String(), "variable: c:3 [type=string]") {
		t.Error("detached memo did not contain expected operator")
	}
}

// TestDetachMemoRace reproduces the condition in #34904: a detached memo still
// aliases table annotations in the metadata. The problematic annotation is a
// statistics object. Construction of new expression can trigger calculation of
// new statistics.
func TestDetachMemoRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	catalog := testcat.New()

	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT, b INT, c INT, d INT)")
	if err != nil {
		t.Fatal(err)
	}
	var o xform.Optimizer
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	testutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM abc WHERE a = $1")
	mem := o.DetachMemo()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		col := opt.ColumnID(i + 1)
		wg.Add(1)
		go func() {
			var o xform.Optimizer
			evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
			o.Init(&evalCtx, catalog)
			f := o.Factory()
			var replaceFn norm.ReplaceFunc
			replaceFn = func(e opt.Expr) opt.Expr {
				if sel, ok := e.(*memo.SelectExpr); ok {
					return f.ConstructSelect(
						f.CopyAndReplaceDefault(sel.Input, replaceFn).(memo.RelExpr),
						memo.FiltersExpr{f.ConstructFiltersItem(
							f.ConstructEq(
								f.ConstructVariable(col),
								f.ConstructConst(tree.NewDInt(10), types.Int),
							),
						)},
					)
				}
				return f.CopyAndReplaceDefault(e, replaceFn)
			}
			// Rewrite the filter to use a different column, which will trigger creation
			// of new table statistics. If the statistics object is aliased, this will
			// be racy.
			f.CopyAndReplace(mem.RootExpr().(memo.RelExpr), mem.RootProps(), replaceFn)
			wg.Done()
		}()
	}
	wg.Wait()
}

// TestCoster files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestCoster/sort"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestCoster/scan"
//   ...
func TestCoster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runDataDrivenTest(
		t, "testdata/coster/",
		memo.ExprFmtHideRuleProps|memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|
			memo.ExprFmtHideTypes,
	)
}

// TestPhysicalProps files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestPhysicalPropsFactory/ordering"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestPhysicalPropsFactory/presentation"
//   ...
func TestPhysicalProps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runDataDrivenTest(
		t, "testdata/physprops/",
		memo.ExprFmtHideConstraints|
			memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideStats|
			memo.ExprFmtHideCost|
			memo.ExprFmtHideQualifications|
			memo.ExprFmtHideScalars|
			memo.ExprFmtHideTypes,
	)
}

// TestRuleProps files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRuleProps/orderings"
//   ...
func TestRuleProps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runDataDrivenTest(
		t,
		"testdata/ruleprops/",
		memo.ExprFmtHideStats|memo.ExprFmtHideCost|memo.ExprFmtHideQualifications|
			memo.ExprFmtHideScalars|memo.ExprFmtHideTypes,
	)
}

// TestRules files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRules/scan"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRules/select"
//   ...
func TestRules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runDataDrivenTest(
		t,
		"testdata/rules/",
		memo.ExprFmtHideStats|memo.ExprFmtHideCost|memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|memo.ExprFmtHideTypes,
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
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runDataDrivenTest(
		t,
		*externalTestData,
		memo.ExprFmtHideStats|memo.ExprFmtHideCost|memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|memo.ExprFmtHideTypes,
	)
}

func TestPlaceholderFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runDataDrivenTest(
		t,
		"testdata/placeholder-fast-path",
		memo.ExprFmtHideCost|memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|memo.ExprFmtHideTypes,
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
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			tester := opttester.New(catalog, d.Input)
			tester.Flags.ExprFormat = fmtFlags
			return tester.RunCommand(t, d)
		})
	})
}
