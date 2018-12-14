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
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
)

func TestMemo(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps | memo.ExprFmtHideQualifications |
		memo.ExprFmtHideStats
	runDataDrivenTest(t, "testdata/memo", flags)
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

func TestMemoInit(t *testing.T) {
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := parser.ParseOne("SELECT * FROM abc WHERE $1=10")
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

	o.Init(&evalCtx)
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
	catalog.Table(tree.NewTableName("t", "abc")).Revoked = true

	ctx := context.Background()
	semaCtx := tree.MakeSemaContext(false /* privileged */)
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	// Initialize context with starting values.
	searchPath := []string{"path1", "path2"}
	evalCtx.SessionData.Database = "t"
	evalCtx.SessionData.SearchPath = sessiondata.MakeSearchPath(searchPath)

	stmt, err := parser.ParseOne("SELECT a, b+1 FROM abcview WHERE c='foo'")
	if err != nil {
		t.Fatal(err)
	}

	var o xform.Optimizer
	o.Init(&evalCtx)
	err = optbuilder.New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt).Build()
	if err != nil {
		t.Fatal(err)
	}
	if o.Memo().IsStale(ctx, &evalCtx, catalog) {
		t.Errorf("memo should not be stale")
	}

	// Stale current database.
	evalCtx.SessionData.Database = "newdb"
	if !o.Memo().IsStale(ctx, &evalCtx, catalog) {
		t.Errorf("expected stale current database")
	}
	evalCtx.SessionData.Database = "t"

	// Stale search path.
	evalCtx.SessionData.SearchPath = sessiondata.SearchPath{}
	if !o.Memo().IsStale(ctx, &evalCtx, catalog) {
		t.Errorf("expected stale search path")
	}
	evalCtx.SessionData.SearchPath = sessiondata.MakeSearchPath([]string{"path1", "path2"})
	if o.Memo().IsStale(ctx, &evalCtx, catalog) {
		t.Errorf("memo should not be stale")
	}
	evalCtx.SessionData.SearchPath = sessiondata.MakeSearchPath(searchPath)

	// Stale location.
	evalCtx.SessionData.DataConversion.Location = time.FixedZone("PST", -8*60*60)
	if !o.Memo().IsStale(ctx, &evalCtx, catalog) {
		t.Errorf("expected stale location")
	}
	evalCtx.SessionData.DataConversion.Location = time.UTC

	// Stale schema.
	_, err = catalog.ExecuteDDL("DROP TABLE abc")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING)")
	if err != nil {
		t.Fatal(err)
	}
	if !o.Memo().IsStale(ctx, &evalCtx, catalog) {
		t.Errorf("expected stale schema")
	}
	catalog = testcat.New()
	_, _ = catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	_, _ = catalog.ExecuteDDL("CREATE VIEW abcview AS SELECT a, b, c FROM abc")

	// User no longer has access to view.
	catalog.View(tree.NewTableName("t", "abcview")).Revoked = true
	if !o.Memo().IsStale(ctx, &evalCtx, catalog) {
		t.Errorf("expected user not to have SELECT privilege on view")
	}
	catalog.View(tree.NewTableName("t", "abcview")).Revoked = false

	// Ensure that memo is not stale after restoring to original state.
	if o.Memo().IsStale(ctx, &evalCtx, catalog) {
		t.Errorf("memo should not be stale")
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
		datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
			tester := testutils.NewOptTester(catalog, d.Input)
			tester.Flags.ExprFormat = fmtFlags
			return tester.RunCommand(t, d)
		})
	})
}
