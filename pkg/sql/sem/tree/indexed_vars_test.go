// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

type testVarContainerSlot struct {
	d    tree.Datum
	used bool
}

type testVarContainer []testVarContainerSlot

var _ eval.IndexedVarContainer = testVarContainer{}

func (d testVarContainer) IndexedVarEval(
	ctx context.Context, idx int, e tree.ExprEvaluator,
) (tree.Datum, error) {
	return d[idx].d.Eval(ctx, e)
}

func (d testVarContainer) IndexedVarResolvedType(idx int) *types.T {
	d[idx].used = true
	return d[idx].d.ResolvedType()
}

func (d testVarContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("var%d", idx))
	return &n
}

func TestIndexedVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	c := make(testVarContainer, 4)
	c[0].d = tree.NewDInt(3)
	c[1].d = tree.NewDInt(5)
	c[2].d = tree.NewDInt(6)
	c[3].d = tree.NewDInt(0)

	h := tree.MakeIndexedVarHelper(c, 4)

	// We use only the first three variables.
	v0 := h.IndexedVar(0)
	v1 := h.IndexedVar(1)
	v2 := h.IndexedVar(2)

	if !c[0].used || !c[1].used || !c[2].used || c[3].used {
		t.Errorf("invalid IndexedVarUsed results %t %t %t %t (expected false false false true)",
			c[0].used, c[1].used, c[2].used, c[3].used)
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
