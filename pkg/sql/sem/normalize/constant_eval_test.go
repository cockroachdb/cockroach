// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package normalize_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/normalize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestConstantEvalArrayComparison(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer tree.MockNameTypes(map[string]*types.T{"a": types.MakeArray(types.Int)})()

	expr, err := parser.ParseExpr("a = ARRAY[1:::INT,2:::INT]")
	if err != nil {
		t.Fatal(err)
	}

	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	typedExpr, err := expr.TypeCheck(context.Background(), &semaCtx, types.AnyElement)
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	c := normalize.MakeConstantEvalVisitor(context.Background(), evalCtx)
	expr, _ = tree.WalkExpr(&c, typedExpr)
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}

	left := tree.ColumnItem{
		ColumnName: "a",
	}
	right := tree.DArray{
		ParamTyp:    types.Int,
		Array:       tree.Datums{tree.NewDInt(1), tree.NewDInt(2)},
		HasNonNulls: true,
	}
	expected := tree.NewTypedComparisonExpr(treecmp.MakeComparisonOperator(treecmp.EQ), &left, &right)
	if !reflect.DeepEqual(expr, expected) {
		t.Errorf("invalid expr '%v', expected '%v'", expr, expected)
	}
}
