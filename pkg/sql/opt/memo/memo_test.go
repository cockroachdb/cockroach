// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package memo_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	opttestutils "github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/opttester"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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

func TestMemoInit(t *testing.T) {
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM abc WHERE $1=10")

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

	// Initialize context with starting values.
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData.Database = "t"

	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT a, b+1 FROM abcview WHERE c='foo'")
	o.Memo().Metadata().AddSchemaDependency(catalog.Schema().Name(), catalog.Schema(), privilege.CREATE)
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

	// Stale location.
	evalCtx.SessionData.DataConversion.Location = time.FixedZone("PST", -8*60*60)
	stale()
	evalCtx.SessionData.DataConversion.Location = time.UTC
	notStale()

	// Stale bytes encode format.
	evalCtx.SessionData.DataConversion.BytesEncodeFormat = sessiondata.BytesEncodeBase64
	stale()
	evalCtx.SessionData.DataConversion.BytesEncodeFormat = sessiondata.BytesEncodeHex
	notStale()

	// Stale extra float digits.
	evalCtx.SessionData.DataConversion.ExtraFloatDigits = 2
	stale()
	evalCtx.SessionData.DataConversion.ExtraFloatDigits = 0
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

	// Stale safe updates.
	evalCtx.SessionData.SafeUpdates = true
	stale()
	evalCtx.SessionData.SafeUpdates = false
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
	catalog.View(tree.NewTableName("t", "abcview")).Revoked = true
	_, err = o.Memo().IsStale(ctx, &evalCtx, catalog)
	if exp := "user does not have privilege"; !testutils.IsError(err, exp) {
		t.Fatalf("expected %q error, but got %+v", exp, err)
	}
	catalog.View(tree.NewTableName("t", "abcview")).Revoked = false
	notStale()

	// Table ID changes.
	catalog.Table(tree.NewTableName("t", "abc")).TabID = 1
	stale()
	catalog.Table(tree.NewTableName("t", "abc")).TabID = 53
	notStale()

	// Table Version changes.
	catalog.Table(tree.NewTableName("t", "abc")).TabVersion = 1
	stale()
	catalog.Table(tree.NewTableName("t", "abc")).TabVersion = 0
	notStale()

	// Schema ID changes.
	catalog.Schema().SchemaID = 2
	stale()
	catalog.Schema().SchemaID = 1
	notStale()

	// User no longer has access to schema.
	catalog.Schema().Revoked = true
	if isStale, err := o.Memo().IsStale(ctx, &evalCtx, catalog); err == nil || !isStale {
		t.Errorf("expected user not to have CREATE privilege on schema")
	}
	catalog.Schema().Revoked = false
	notStale()
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
			tester := opttester.New(catalog, d.Input)
			tester.Flags.ExprFormat = fmtFlags
			return tester.RunCommand(t, d)
		})
	})
}
