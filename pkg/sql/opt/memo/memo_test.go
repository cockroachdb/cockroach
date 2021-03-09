// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	opttestutils "github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/opttester"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/datadriven"
)

func TestMemo(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps | memo.ExprFmtHideQualifications |
		memo.ExprFmtHideStats
	runDataDrivenTest(t, "testdata/memo", flags)
}

func TestFormat(t *testing.T) {
	runDataDrivenTest(t, "testdata/format", memo.ExprFmtShowAll)
}

func TestLogicalProps(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideQualifications | memo.ExprFmtHideStats
	runDataDrivenTest(t, "testdata/logprops/", flags)
}

func TestStats(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps | memo.ExprFmtHideQualifications |
		memo.ExprFmtHideScalars
	runDataDrivenTest(t, "testdata/stats/", flags)
}

func TestStatsQuality(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps | memo.ExprFmtHideQualifications |
		memo.ExprFmtHideScalars
	runDataDrivenTest(t, "testdata/stats_quality/", flags)
}

func TestCompositeSensitive(t *testing.T) {
	datadriven.RunTest(t, "testdata/composite_sensitive", func(t *testing.T, d *datadriven.TestData) string {
		semaCtx := tree.MakeSemaContext()
		evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

		var f norm.Factory
		f.Init(&evalCtx, nil /* catalog */)
		md := f.Metadata()

		if d.Cmd != "composite-sensitive" {
			d.Fatalf(t, "unsupported command: %s\n", d.Cmd)
		}
		var sv opttestutils.ScalarVars

		for _, arg := range d.CmdArgs {
			key, vals := arg.Key, arg.Vals
			switch key {
			case "vars":
				err := sv.Init(md, vals)
				if err != nil {
					d.Fatalf(t, "%v", err)
				}

			default:
				d.Fatalf(t, "unknown argument: %s\n", key)
			}
		}

		expr, err := parser.ParseExpr(d.Input)
		if err != nil {
			d.Fatalf(t, "error parsing: %v", err)
		}

		b := optbuilder.NewScalar(context.Background(), &semaCtx, &evalCtx, &f)
		if err := b.Build(expr); err != nil {
			d.Fatalf(t, "error building: %v", err)
		}
		return fmt.Sprintf("%v", memo.CanBeCompositeSensitive(md, f.Memo().RootExpr()))
	})
}

func TestMemoInit(t *testing.T) {
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM abc WHERE $1=10")

	o.Init(&evalCtx, catalog)
	if !o.Memo().IsEmpty() {
		t.Fatal("memo should be empty")
	}
	if o.Memo().MemoryEstimate() != 0 {
		t.Fatal("memory estimate should be 0")
	}
	if o.Memo().RootExpr() != nil {
		t.Fatal("root expression should be nil")
	}
	if o.Memo().RootProps() != nil {
		t.Fatal("root props should be nil")
	}
}

func TestMemoIsStale(t *testing.T) {
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE VIEW abcview AS SELECT a, b, c FROM abc")
	if err != nil {
		t.Fatal(err)
	}

	// Revoke access to the underlying table. The user should retain indirect
	// access via the view.
	catalog.Table(tree.NewTableNameWithSchema("t", tree.PublicSchemaName, "abc")).Revoked = true

	// Initialize context with starting values.
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData.Database = "t"

	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT a, b+1 FROM abcview WHERE c='foo'")
	o.Memo().Metadata().AddSchema(catalog.Schema())

	ctx := context.Background()
	stale := func() {
		t.Helper()
		if isStale, err := o.Memo().IsStale(ctx, &evalCtx, catalog); err != nil {
			t.Fatal(err)
		} else if !isStale {
			t.Errorf("memo should be stale")
		}

		// If we did not initialize the Memo's copy of a SessionData setting, the
		// tests as written still pass if the default value is 0. To detect this, we
		// create a new memo with the changed setting and verify it's not stale.
		var o2 xform.Optimizer
		opttestutils.BuildQuery(t, &o2, catalog, &evalCtx, "SELECT a, b+1 FROM abcview WHERE c='foo'")

		if isStale, err := o2.Memo().IsStale(ctx, &evalCtx, catalog); err != nil {
			t.Fatal(err)
		} else if isStale {
			t.Errorf("memo should not be stale")
		}
	}

	notStale := func() {
		t.Helper()
		if isStale, err := o.Memo().IsStale(ctx, &evalCtx, catalog); err != nil {
			t.Fatal(err)
		} else if isStale {
			t.Errorf("memo should not be stale")
		}
	}

	notStale()

	// Stale reorder joins limit.
	evalCtx.SessionData.ReorderJoinsLimit = 4
	stale()
	evalCtx.SessionData.ReorderJoinsLimit = 0
	notStale()

	// Stale zig zag join enable.
	evalCtx.SessionData.ZigzagJoinEnabled = true
	stale()
	evalCtx.SessionData.ZigzagJoinEnabled = false
	notStale()

	// Stale optimizer histogram usage enable.
	evalCtx.SessionData.OptimizerUseHistograms = true
	stale()
	evalCtx.SessionData.OptimizerUseHistograms = false
	notStale()

	// Stale optimizer multi-col stats usage enable.
	evalCtx.SessionData.OptimizerUseMultiColStats = true
	stale()
	evalCtx.SessionData.OptimizerUseMultiColStats = false
	notStale()

	// Stale locality optimized search enable.
	evalCtx.SessionData.LocalityOptimizedSearch = true
	stale()
	evalCtx.SessionData.LocalityOptimizedSearch = false
	notStale()

	// Stale safe updates.
	evalCtx.SessionData.SafeUpdates = true
	stale()
	evalCtx.SessionData.SafeUpdates = false
	notStale()

	// Stale prefer lookup joins for FKs.
	evalCtx.SessionData.PreferLookupJoinsForFKs = true
	stale()
	evalCtx.SessionData.PreferLookupJoinsForFKs = false
	notStale()

	// Stale data sources and schema. Create new catalog so that data sources are
	// recreated and can be modified independently.
	catalog = testcat.New()
	_, err = catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE VIEW abcview AS SELECT a, b, c FROM abc")
	if err != nil {
		t.Fatal(err)
	}

	// User no longer has access to view.
	catalog.View(tree.NewTableNameWithSchema("t", tree.PublicSchemaName, "abcview")).Revoked = true
	_, err = o.Memo().IsStale(ctx, &evalCtx, catalog)
	if exp := "user does not have privilege"; !testutils.IsError(err, exp) {
		t.Fatalf("expected %q error, but got %+v", exp, err)
	}
	catalog.View(tree.NewTableNameWithSchema("t", tree.PublicSchemaName, "abcview")).Revoked = false
	notStale()

	// Table ID changes.
	catalog.Table(tree.NewTableNameWithSchema("t", tree.PublicSchemaName, "abc")).TabID = 1
	stale()
	catalog.Table(tree.NewTableNameWithSchema("t", tree.PublicSchemaName, "abc")).TabID = 53
	notStale()

	// Table Version changes.
	catalog.Table(tree.NewTableNameWithSchema("t", tree.PublicSchemaName, "abc")).TabVersion = 1
	stale()
	catalog.Table(tree.NewTableNameWithSchema("t", tree.PublicSchemaName, "abc")).TabVersion = 0
	notStale()
}

// TestStatsAvailable tests that the statisticsBuilder correctly identifies
// for each expression whether statistics were available on the base table.
// This test is here (instead of statistics_builder_test.go) to avoid import
// cycles.
func TestStatsAvailable(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	catalog := testcat.New()
	if _, err := catalog.ExecuteDDL(
		"CREATE TABLE t (a INT, b INT)",
	); err != nil {
		t.Fatal(err)
	}

	var o xform.Optimizer

	testNotAvailable := func(expr memo.RelExpr) {
		traverseExpr(expr, func(e memo.RelExpr) {
			if e.Relational().Stats.Available {
				t.Fatal("stats should not be available")
			}
		})
	}

	// Stats should not be available for any expression.
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM t WHERE a=1")
	testNotAvailable(o.Memo().RootExpr().(memo.RelExpr))

	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT sum(a), b FROM t GROUP BY b")
	testNotAvailable(o.Memo().RootExpr().(memo.RelExpr))

	opttestutils.BuildQuery(t, &o, catalog, &evalCtx,
		"SELECT * FROM t AS t1, t AS t2 WHERE t1.a = t2.a AND t1.b = 5",
	)
	testNotAvailable(o.Memo().RootExpr().(memo.RelExpr))

	if _, err := catalog.ExecuteDDL(
		`ALTER TABLE t INJECT STATISTICS '[
		{
			"columns": ["a"],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"row_count": 1000,
			"distinct_count": 500
		},
		{
			"columns": ["b"],
			"created_at": "2018-01-01 1:30:00.00000+00:00",
			"row_count": 1000,
			"distinct_count": 500
		}
	]'`); err != nil {
		t.Fatal(err)
	}

	testAvailable := func(expr memo.RelExpr) {
		traverseExpr(expr, func(e memo.RelExpr) {
			if !e.Relational().Stats.Available {
				t.Fatal("stats should be available")
			}
		})
	}

	// Stats should be available for all expressions.
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM t WHERE a=1")
	testAvailable(o.Memo().RootExpr().(memo.RelExpr))

	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT sum(a), b FROM t GROUP BY b")
	testAvailable(o.Memo().RootExpr().(memo.RelExpr))

	opttestutils.BuildQuery(t, &o, catalog, &evalCtx,
		"SELECT * FROM t AS t1, t AS t2 WHERE t1.a = t2.a AND t1.b = 5",
	)
	testAvailable(o.Memo().RootExpr().(memo.RelExpr))
}

// traverseExpr is a helper function to recursively traverse a relational
// expression and apply a function to the root as well as each relational
// child.
func traverseExpr(expr memo.RelExpr, f func(memo.RelExpr)) {
	f(expr)
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		if child, ok := expr.Child(i).(memo.RelExpr); ok {
			traverseExpr(child, f)
		}
	}
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
