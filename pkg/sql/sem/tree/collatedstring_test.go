// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestCastToCollatedString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	cases := []struct {
		typ      *types.T
		contents string
	}{
		{types.MakeCollatedString(types.String, "de"), "test"},
		{types.MakeCollatedString(types.String, "en"), "test"},
		{types.MakeCollatedString(types.MakeString(5), "en"), "test"},
		{types.MakeCollatedString(types.MakeString(4), "en"), "test"},
		{types.MakeCollatedString(types.MakeString(3), "en"), "tes"},
	}
	ctx := context.Background()
	for _, cas := range cases {
		t.Run("", func(t *testing.T) {
			expr := &tree.CastExpr{
				Expr:       tree.NewDString("test"),
				Type:       cas.typ,
				SyntaxMode: tree.CastShort,
			}
			semaCtx := tree.MakeSemaContext(nil /* resolver */)
			typedexpr, err := expr.TypeCheck(ctx, &semaCtx, types.AnyElement)
			if err != nil {
				t.Fatal(err)
			}
			evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			val, err := eval.Expr(ctx, evalCtx, typedexpr)
			if err != nil {
				t.Fatal(err)
			}
			switch v := val.(type) {
			case *tree.DCollatedString:
				if v.Locale != cas.typ.Locale() {
					t.Errorf("expected locale %q but got %q", cas.typ.Locale(), v.Locale)
				}
				if v.Contents != cas.contents {
					t.Errorf("expected contents %q but got %q", cas.contents, v.Contents)
				}
			default:
				t.Errorf("expected type *DCollatedString but got %T", v)
			}
		})
	}
}
