// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type testVarContainer []Datum

func (d testVarContainer) IndexedVarEval(idx int, ctx *EvalContext) (Datum, error) {
	return d[idx].Eval(ctx)
}

func (d testVarContainer) IndexedVarResolvedType(idx int) *types.T {
	return d[idx].ResolvedType()
}

func (d testVarContainer) IndexedVarNodeFormatter(idx int) NodeFormatter {
	n := Name(fmt.Sprintf("var%d", idx))
	return &n
}

func TestIndexedVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	c := make(testVarContainer, 4)
	c[0] = NewDInt(3)
	c[1] = NewDInt(5)
	c[2] = NewDInt(6)
	c[3] = NewDInt(0)

	h := MakeIndexedVarHelper(c, 4)

	// We use only the first three variables.
	v0 := h.IndexedVar(0)
	v1 := h.IndexedVar(1)
	v2 := h.IndexedVar(2)

	if !h.IndexedVarUsed(0) || !h.IndexedVarUsed(1) || !h.IndexedVarUsed(2) || h.IndexedVarUsed(3) {
		t.Errorf("invalid IndexedVarUsed results %t %t %t %t (expected false false false true)",
			h.IndexedVarUsed(0), h.IndexedVarUsed(1), h.IndexedVarUsed(2), h.IndexedVarUsed(3))
	}

	binary := func(op BinaryOperator, left, right Expr) Expr {
		return &BinaryExpr{Operator: op, Left: left, Right: right}
	}
	expr := binary(MakeBinaryOperator(Plus), v0, binary(MakeBinaryOperator(Mult), v1, v2))

	// Verify the expression evaluates correctly.
	ctx := context.Background()
	semaContext := MakeSemaContext()
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
	f := NewFmtCtx(
		FmtSimple,
		FmtIndexedVarFormat(
			func(ctx *FmtCtx, idx int) {
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
	evalCtx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	evalCtx.IVarContainer = c
	d, err := typedExpr.Eval(evalCtx)
	if err != nil {
		t.Fatal(err)
	}
	if d.Compare(evalCtx, NewDInt(3+5*6)) != 0 {
		t.Errorf("invalid result %s (expected %d)", d, 3+5*6)
	}
}
