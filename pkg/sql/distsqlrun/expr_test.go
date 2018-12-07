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

package distsqlrun

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type testVarContainer struct{}

func (d testVarContainer) IndexedVarResolvedType(idx int) types.T {
	return types.Int
}

func (d testVarContainer) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return nil, nil
}

func (d testVarContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("var%d", idx))
	return &n
}

func TestProcessExpression(t *testing.T) {
	defer leaktest.AfterTest(t)()

	e := distsqlpb.Expression{Expr: "@1 * (@2 + @3) + @1"}

	h := tree.MakeIndexedVarHelper(testVarContainer{}, 4)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext(false)
	expr, err := processExpression(e, &evalCtx, &semaCtx, &h)
	if err != nil {
		t.Fatal(err)
	}

	if !h.IndexedVarUsed(0) || !h.IndexedVarUsed(1) || !h.IndexedVarUsed(2) || h.IndexedVarUsed(3) {
		t.Errorf("invalid IndexedVarUsed results %t %t %t %t (expected false false false true)",
			h.IndexedVarUsed(0), h.IndexedVarUsed(1), h.IndexedVarUsed(2), h.IndexedVarUsed(3))
	}

	str := expr.String()
	expectedStr := "(var0 * (var1 + var2)) + var0"
	if str != expectedStr {
		t.Errorf("invalid expression string '%s', expected '%s'", str, expectedStr)
	}
}

// Test that processExpression evaluates constant exprs into datums.
func TestProcessExpressionConstantEval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	e := distsqlpb.Expression{Expr: "ARRAY[1:::INT,2:::INT]"}

	h := tree.MakeIndexedVarHelper(nil, 0)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext(false)
	expr, err := processExpression(e, &evalCtx, &semaCtx, &h)
	if err != nil {
		t.Fatal(err)
	}

	expected := &tree.DArray{
		ParamTyp: types.Int,
		Array:    tree.Datums{tree.NewDInt(1), tree.NewDInt(2)},
	}
	if !reflect.DeepEqual(expr, expected) {
		t.Errorf("invalid expr '%v', expected '%v'", expr, expected)
	}
}
