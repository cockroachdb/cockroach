// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type testVarContainer []tree.Datum

var _ eval.IndexedVarContainer = testVarContainer{}

func (d testVarContainer) IndexedVarEval(
	ctx context.Context, idx int, e tree.ExprEvaluator,
) (tree.Datum, error) {
	return d[idx].Eval(ctx, e)
}

func (d testVarContainer) IndexedVarResolvedType(idx int) *types.T {
	return d[idx].ResolvedType()
}

func (d testVarContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("var%d", idx))
	return &n
}

func TestIndexedVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	c := make(testVarContainer, 4)
	c[0] = tree.NewDInt(3)
	c[1] = tree.NewDInt(5)
	c[2] = tree.NewDInt(6)
	c[3] = tree.NewDInt(0)

	h := tree.MakeIndexedVarHelper(c, 4)

	// We use only the first three variables.
	v0 := h.IndexedVar(0)
	v1 := h.IndexedVar(1)
	v2 := h.IndexedVar(2)

	if !h.IndexedVarUsed(0) || !h.IndexedVarUsed(1) || !h.IndexedVarUsed(2) || h.IndexedVarUsed(3) {
		t.Errorf("invalid IndexedVarUsed results %t %t %t %t (expected false false false true)",
			h.IndexedVarUsed(0), h.IndexedVarUsed(1), h.IndexedVarUsed(2), h.IndexedVarUsed(3))
	}

	binary := func(op treebin.BinaryOperator, left, right tree.Expr) tree.Expr {
		return &tree.BinaryExpr{Operator: op, Left: left, Right: right}
	}
	expr := binary(treebin.MakeBinaryOperator(treebin.Plus), v0, binary(treebin.MakeBinaryOperator(treebin.Mult), v1, v2))

	// Verify the expression evaluates correctly.
	ctx := context.Background()
	semaContext := tree.MakeSemaContext()
	semaContext.IVarContainer = c
	typedExpr, err := expr.TypeCheck(ctx, &semaContext, types.Any)
	if err != nil {
		t.Fatal(err)
	}

	str := typedExpr.String()
	expectedStr := "var0 + (var1 * var2)"
	if str != expectedStr {
		t.Errorf("invalid expression string '%s', expected '%s'", str, expectedStr)
	}

	// Test formatting using the indexed var format interceptor.
	f := tree.NewFmtCtx(
		tree.FmtSimple,
		tree.FmtIndexedVarFormat(
			func(ctx *tree.FmtCtx, idx int) {
				ctx.Printf("customVar%d", idx)
			}),
	)
	f.FormatNode(typedExpr)
	str = f.CloseAndGetString()

	expectedStr = "customVar0 + (customVar1 * customVar2)"
	if str != expectedStr {
		t.Errorf("invalid expression string '%s', expected '%s'", str, expectedStr)
	}

	typ := typedExpr.ResolvedType()
	if !typ.Equivalent(types.Int) {
		t.Errorf("invalid expression type %s", typ)
	}
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	evalCtx.IVarContainer = c
	d, err := eval.Expr(ctx, evalCtx, typedExpr)
	if err != nil {
		t.Fatal(err)
	}
	if d.Compare(evalCtx, tree.NewDInt(3+5*6)) != 0 {
		t.Errorf("invalid result %s (expected %d)", d, 3+5*6)
	}
}
