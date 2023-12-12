// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package xform_test

import (
	"context"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	tu "github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
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
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	testutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM abc WHERE c=$1")

	before := o.DetachMemo(context.Background())

	if !o.Memo().IsEmpty() {
		t.Error("memo expression should be reinitialized by DetachMemo")
	}

	testutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT a=$1 FROM abc")

	after := o.Memo()
	if after == before {
		t.Error("after memo cannot be the same as the detached memo")
	}

	if !strings.Contains(after.String(), "variable: a:1 [type=int]") {
		t.Error("after memo did not contain expected operator")
	}

	if before == o.Memo() {
		t.Error("detached memo should not be reused")
	}

	if !strings.Contains(before.String(), "variable: c:3 [type=string]") {
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
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	testutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM abc WHERE a = $1")
	mem := o.DetachMemo(context.Background())

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		col := opt.ColumnID(i + 1)
		wg.Add(1)
		go func() {
			var o xform.Optimizer
			evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
			o.Init(context.Background(), &evalCtx, catalog)
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
			f.CopyAndReplace(mem, mem.RootExpr().(memo.RelExpr), mem.RootProps(), replaceFn)
			wg.Done()
		}()
	}
	wg.Wait()
}

// TestCoster files can be run separately like this:
//
//	./dev test pkg/sql/opt/xform -f TestCoster/sort
//	./dev test pkg/sql/opt/xform -f TestCoster/scan
//	...
func TestCoster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runDataDrivenTest(
		t, tu.TestDataPath(t, "coster", ""),
		memo.ExprFmtHideRuleProps|memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|
			memo.ExprFmtHideTypes|memo.ExprFmtHideNotVisibleIndexInfo|memo.ExprFmtHideFastPathChecks,
	)
}

// TestPhysicalProps files can be run separately like this:
//
//	./dev test pkg/sql/opt/xform -f TestPhysicalPropsFactory/ordering
//	./dev test pkg/sql/opt/xform -f TestPhysicalPropsFactory/presentation
//	...
func TestPhysicalProps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runDataDrivenTest(
		t, tu.TestDataPath(t, "physprops", ""),
		memo.ExprFmtHideConstraints|
			memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideStats|
			memo.ExprFmtHideCost|
			memo.ExprFmtHideQualifications|
			memo.ExprFmtHideScalars|
			memo.ExprFmtHideTypes|
			memo.ExprFmtHideNotVisibleIndexInfo|
			memo.ExprFmtHideFastPathChecks,
	)
}

// TestRuleProps files can be run separately like this:
//
//	./dev test pkg/sql/opt/xform -f TestRuleProps/orderings
//	...
func TestRuleProps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runDataDrivenTest(
		t,
		tu.TestDataPath(t, "ruleprops"),
		memo.ExprFmtHideStats|memo.ExprFmtHideCost|memo.ExprFmtHideQualifications|
			memo.ExprFmtHideScalars|memo.ExprFmtHideTypes|memo.ExprFmtHideNotVisibleIndexInfo|memo.ExprFmtHideFastPathChecks,
	)
}

// TestRules files can be run separately like this:
//
//	./dev test pkg/sql/opt/xform -f TestRules/scan
//	./dev test pkg/sql/opt/xform -f TestRules/select
//	...
func TestRules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runDataDrivenTest(
		t,
		tu.TestDataPath(t, "rules"),
		memo.ExprFmtHideStats|memo.ExprFmtHideCost|memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|memo.ExprFmtHideTypes|
			memo.ExprFmtHideNotVisibleIndexInfo|memo.ExprFmtHideFastPathChecks,
	)
}

var externalTestData = flag.String(
	"d", "testdata/external", "test files directory for TestExternal",
)

// TestExternal contains test cases from external customers and external
// benchmarks (like TPCH), so that changes in their query plans can be monitored
// over time.
//
// TestExternal files can be run separately like this:
//
//	./dev test pkg/sql/opt/xform -f TestExternal/tpch
//	...
//
// Test files from another location can be run using the -d flag:
//
//	./dev test pkg/sql/opt/xform -f TestExternal --rewrite \
//	  --test-args="-d=/some-dir" -- --sandbox_writable_path="/some-dir"
func TestExternal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runDataDrivenTest(
		t,
		*externalTestData,
		memo.ExprFmtHideStats|memo.ExprFmtHideCost|memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|memo.ExprFmtHideTypes|
			memo.ExprFmtHideNotVisibleIndexInfo|memo.ExprFmtHideFastPathChecks,
	)
}

func TestPlaceholderFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runDataDrivenTest(
		t,
		tu.TestDataPath(t, "placeholder-fast-path"),
		memo.ExprFmtHideCost|memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|memo.ExprFmtHideTypes|
			memo.ExprFmtHideNotVisibleIndexInfo|memo.ExprFmtHideFastPathChecks,
	)
}

// runDataDrivenTest runs data-driven testcases of the form
//
//	<command>
//	<SQL statement>
//	----
//	<expected results>
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
