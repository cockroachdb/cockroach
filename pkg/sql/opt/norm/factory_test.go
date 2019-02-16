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
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// TestSimplifyFilters tests factory.SimplifyFilters. It's hard to fully test
// using SQL, as And operator rules simplify the expression before the Filters
// operator is created.
func TestSimplifyFilters(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var f norm.Factory
	f.Init(&evalCtx)

	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE a (x INT PRIMARY KEY, y INT)"); err != nil {
		t.Fatal(err)
	}
	a := f.Metadata().AddTable(cat.Table(tree.NewTableName("t", "a")))
	ax := a.ColumnID(0)

	variable := f.ConstructVariable(ax)
	constant := f.ConstructConst(tree.NewDInt(1))
	eq := f.ConstructEq(variable, constant)

	// Filters expression evaluates to False if any operand is False.
	vals := f.ConstructValues(memo.ScalarListWithEmptyTuple, opt.ColList{})
	filters := memo.FiltersExpr{{Condition: eq}, {Condition: &memo.FalseFilter}, {Condition: eq}}
	sel := f.ConstructSelect(vals, filters)
	if sel.Relational().Cardinality.Max == 0 {
		t.Fatalf("result should have been collapsed to zero cardinality rowset")
	}

	// Filters operator skips True operands.
	filters = memo.FiltersExpr{{Condition: eq}, {Condition: memo.TrueSingleton}}
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
	semaCtx := tree.MakeSemaContext()

	var o xform.Optimizer
	o.Init(&evalCtx)

	stmt, err := parser.ParseOne("SELECT * FROM ab INNER JOIN cde ON a=c AND d=$1")
	if err != nil {
		t.Fatal(err)
	}
	if err := semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
		t.Fatal(err)
	}
	b := optbuilder.New(context.Background(), &semaCtx, &evalCtx, cat, o.Factory(), stmt.AST)
	if err := b.Build(); err != nil {
		t.Fatal(err)
	}

	if e := o.Optimize(); e.Op() != opt.MergeJoinOp {
		t.Errorf("expected optimizer to choose merge-join, not %v", e.Op())
	}

	m := o.Factory().DetachMemo()

	o.Init(&evalCtx)
	o.Factory().CopyAndReplace(m.RootExpr().(memo.RelExpr), m.RootProps(), func(e opt.Expr) opt.Expr {
		if e.Op() == opt.PlaceholderOp {
			return o.Factory().ConstructConstVal(tree.NewDInt(1), types.Int)
		}
		return e
	})

	if e := o.Optimize(); e.Op() != opt.LookupJoinOp {
		t.Errorf("expected optimizer to choose lookup-join, not %v", e.Op())
	}
}
