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

package tree_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestConstantEvalArrayComparison(t *testing.T) {
	defer tree.MockNameTypes(map[string]types.T{"a": types.TArray{Typ: types.Int}})()

	expr, err := parser.ParseExpr("a = ARRAY[1:::INT,2:::INT]")
	if err != nil {
		t.Fatal(err)
	}

	semaCtx := tree.MakeSemaContext(true /* privileged */)
	typedExpr, err := expr.TypeCheck(&semaCtx, types.Any)
	if err != nil {
		t.Fatal(err)
	}

	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer ctx.Mon.Stop(context.Background())
	defer ctx.ActiveMemAcc.Close(context.Background())
	c := tree.MakeConstantEvalVisitor(ctx)
	expr, _ = tree.WalkExpr(&c, typedExpr)
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}

	left := tree.ColumnItem{
		ColumnName: "a",
	}
	right := tree.DArray{
		ParamTyp: types.Int,
		Array:    tree.Datums{tree.NewDInt(1), tree.NewDInt(2)},
	}
	expected := tree.NewTypedComparisonExpr(tree.EQ, &left, &right)
	if !reflect.DeepEqual(expr, expected) {
		t.Errorf("invalid expr '%v', expected '%v'", expr, expected)
	}
}
