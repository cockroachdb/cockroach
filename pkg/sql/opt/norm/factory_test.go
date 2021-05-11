// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// TestSimplifyFilters tests factory.SimplifyFilters. It's hard to fully test
// using SQL, as And operator rules simplify the expression before the Filters
// operator is created.
func TestSimplifyFilters(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE a (x INT PRIMARY KEY, y INT)"); err != nil {
		t.Fatal(err)
	}

	var f norm.Factory
	f.Init(&evalCtx, cat)

	tn := tree.NewTableNameWithSchema("t", tree.PublicSchemaName, "a")
	a := f.Metadata().AddTable(cat.Table(tn), tn)
	ax := a.ColumnID(0)

	variable := f.ConstructVariable(ax)
	constant := f.ConstructConst(tree.NewDInt(1), types.Int)
	eq := f.ConstructEq(variable, constant)

	// Filters expression evaluates to False if any operand is False.
	vals := f.ConstructValues(memo.ScalarListWithEmptyTuple, &memo.ValuesPrivate{
		Cols: opt.ColList{},
		ID:   f.Metadata().NextUniqueID(),
	})
	filters := memo.FiltersExpr{
		f.ConstructFiltersItem(eq),
		f.ConstructFiltersItem(memo.FalseSingleton),
		f.ConstructFiltersItem(eq),
	}
	sel := f.ConstructSelect(vals, filters)
	if sel.Relational().Cardinality.Max != 0 {
		t.Fatalf("result should have been collapsed to zero cardinality rowset")
	}

	// Filters operator skips True operands.
	filters = memo.FiltersExpr{f.ConstructFiltersItem(eq), f.ConstructFiltersItem(memo.TrueSingleton)}
	sel = f.ConstructSelect(vals, filters)
	if len(sel.(*memo.SelectExpr).Filters) != 1 {
		t.Fatalf("filters result should have filtered True operator")
	}
}

// Test CopyAndReplace on an already optimized join. Before CopyAndReplace is
// called, the join has a placeholder that causes the optimizer to use a merge
// join. After CopyAndReplace substitutes a constant for the placeholder, the
// optimizer switches to a lookup join. A similar pattern is used by the
// ApplyJoin execution operator which replaces variables with constants in an
// already optimized tree. The CopyAndReplace code must take care to copy over
// the normalized tree rather than the optimized tree by using the FirstExpr
// method.
func TestCopyAndReplace(t *testing.T) {
	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE ab (a INT PRIMARY KEY, b INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.ExecuteDDL("CREATE TABLE cde (c INT PRIMARY KEY, d INT, e INT, INDEX(d))"); err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	var o xform.Optimizer
	testutils.BuildQuery(t, &o, cat, &evalCtx, "SELECT * FROM cde INNER JOIN ab ON a=c AND d=$1")

	if e, err := o.Optimize(); err != nil {
		t.Fatal(err)
	} else if e.Op() != opt.MergeJoinOp {
		t.Errorf("expected optimizer to choose merge-join, not %v", e.Op())
	}

	m := o.Factory().DetachMemo()

	o.Init(&evalCtx, cat)
	var replaceFn norm.ReplaceFunc
	replaceFn = func(e opt.Expr) opt.Expr {
		if e.Op() == opt.PlaceholderOp {
			return o.Factory().ConstructConstVal(tree.NewDInt(1), types.Int)
		}
		return o.Factory().CopyAndReplaceDefault(e, replaceFn)
	}
	o.Factory().CopyAndReplace(m.RootExpr().(memo.RelExpr), m.RootProps(), replaceFn)

	if e, err := o.Optimize(); err != nil {
		t.Fatal(err)
	} else if e.Op() != opt.LookupJoinOp {
		t.Errorf("expected optimizer to choose lookup-join, not %v", e.Op())
	}
}

// Test that CopyAndReplace works on expressions using WithScan.
func TestCopyAndReplaceWithScan(t *testing.T) {
	cat := testcat.New()
	for _, ddl := range []string{
		"CREATE TABLE ab (a INT PRIMARY KEY, b INT)",
		"CREATE TABLE parent (p INT PRIMARY KEY)",
		"CREATE TABLE child (c INT PRIMARY KEY, p INT REFERENCES parent(p))",
	} {
		if _, err := cat.ExecuteDDL(ddl); err != nil {
			t.Fatal(err)
		}
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	for _, query := range []string{
		"WITH cte AS (SELECT * FROM ab) SELECT * FROM cte, cte AS cte2 WHERE cte.a = cte2.b",
		"INSERT INTO child VALUES (1,1), (2,2)",
		"UPSERT INTO child SELECT a, b FROM ab",
		"UPDATE child SET p=p+1 WHERE c > 1",
		"UPDATE parent SET p=p+1 WHERE p > 1",
		"DELETE FROM parent WHERE p < 10",
		"WITH RECURSIVE cte(x) AS (VALUES (1) UNION ALL SELECT x+1 FROM cte WHERE x < 10) SELECT * FROM cte",
	} {
		t.Run(query, func(t *testing.T) {
			var o xform.Optimizer
			testutils.BuildQuery(t, &o, cat, &evalCtx, query)

			m := o.Factory().DetachMemo()

			o.Init(&evalCtx, cat)
			var replaceFn norm.ReplaceFunc
			replaceFn = func(e opt.Expr) opt.Expr {
				return o.Factory().CopyAndReplaceDefault(e, replaceFn)
			}
			o.Factory().CopyAndReplace(m.RootExpr().(memo.RelExpr), m.RootProps(), replaceFn)

			if _, err := o.Optimize(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
