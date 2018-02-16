// Copyright 2016 The Cockroach Authors.
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

package tree

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type testVarContainer []Datum

func (d testVarContainer) IndexedVarEval(idx int, ctx *EvalContext) (Datum, error) {
	return d[idx].Eval(ctx)
}

func (d testVarContainer) IndexedVarResolvedType(idx int) types.T {
	return d[idx].ResolvedType()
}

func (d testVarContainer) IndexedVarNodeFormatter(idx int) NodeFormatter {
	n := Name(fmt.Sprintf("var%d", idx))
	return &n
}

func TestIndexedVars(t *testing.T) {
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
	expr := binary(Plus, v0, binary(Mult, v1, v2))

	// Verify the expression evaluates correctly.
	semaContext := &SemaContext{IVarContainer: c}
	typedExpr, err := expr.TypeCheck(semaContext, types.Any)
	if err != nil {
		t.Fatal(err)
	}

	str := typedExpr.String()
	expectedStr := "var0 + (var1 * var2)"
	if str != expectedStr {
		t.Errorf("invalid expression string '%s', expected '%s'", str, expectedStr)
	}

	// Test formatting using the indexed var format interceptor.
	f := NewFmtCtxWithBuf(FmtSimple)
	f.WithIndexedVarFormat(
		func(ctx *FmtCtx, idx int) {
			ctx.Printf("customVar%d", idx)
		},
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
