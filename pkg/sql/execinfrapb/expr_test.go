// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfrapb

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type testVarContainer struct{}

var _ tree.IndexedVarContainer = &testVarContainer{}

func (d *testVarContainer) IndexedVarResolvedType(idx int) *types.T {
	return types.Int
}

func TestProcessExpression(t *testing.T) {
	defer leaktest.AfterTest(t)()

	e := Expression{Expr: "@1 * (@2 + @3) + @1"}

	var c testVarContainer
	h := tree.MakeIndexedVarHelper(&c, 4)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext()
	expr, err := processExpression(context.Background(), e, &evalCtx, &semaCtx, &h)
	if err != nil {
		t.Fatal(err)
	}

	str := expr.String()
	expectedStr := "(@1 * (@2 + @3)) + @1"
	if str != expectedStr {
		t.Errorf("invalid expression string '%s', expected '%s'", str, expectedStr)
	}

	// Verify the expression is fully typed.
	typ := expr.ResolvedType()
	if !typ.Equivalent(types.Int) {
		t.Errorf("invalid expression type %s", typ)
	}

	// We can process a new expression with the same tree.IndexedVarHelper.
	e = Expression{Expr: "@4 - @1"}
	expr, err = processExpression(context.Background(), e, &evalCtx, &semaCtx, &h)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that the new expression can be formatted correctly.
	str = expr.String()
	expectedStr = "@4 - @1"
	if str != expectedStr {
		t.Errorf("invalid expression string '%s', expected '%s'", str, expectedStr)
	}

	// Verify the expression is fully typed.
	typ = expr.ResolvedType()
	if !typ.Equivalent(types.Int) {
		t.Errorf("invalid expression type %s", typ)
	}
}

// Test that processExpression evaluates constant exprs into datums.
func TestProcessExpressionConstantEval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	e := Expression{Expr: "ARRAY[1:::INT,2:::INT]"}

	h := tree.MakeIndexedVarHelper(nil, 0)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext()
	expr, err := processExpression(context.Background(), e, &evalCtx, &semaCtx, &h)
	if err != nil {
		t.Fatal(err)
	}

	expected := &tree.DArray{
		ParamTyp:    types.Int,
		Array:       tree.Datums{tree.NewDInt(1), tree.NewDInt(2)},
		HasNonNulls: true,
	}
	if !reflect.DeepEqual(expr, expected) {
		t.Errorf("invalid expr '%v', expected '%v'", expr, expected)
	}
}
