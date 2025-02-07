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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestEvalComparisonExprCaching(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testExprs := []struct {
		op          treecmp.ComparisonOperatorSymbol
		left, right string
		cacheCount  int
	}{
		// Comparisons.
		{treecmp.EQ, `0`, `1`, 0},
		{treecmp.LT, `0`, `1`, 0},
		// LIKE and NOT LIKE
		{treecmp.Like, `TEST`, `T%T`, 1},
		{treecmp.NotLike, `TEST`, `%E%T`, 1},
		// ILIKE and NOT ILIKE
		{treecmp.ILike, `TEST`, `T%T`, 1},
		{treecmp.NotILike, `TEST`, `%E%T`, 1},
		// SIMILAR TO and NOT SIMILAR TO
		{treecmp.SimilarTo, `abc`, `(b|c)%`, 1},
		{treecmp.NotSimilarTo, `abc`, `%(b|d)%`, 1},
		// ~, !~, ~*, and !~*
		{treecmp.RegMatch, `abc`, `(b|c).`, 1},
		{treecmp.NotRegMatch, `abc`, `(b|c).`, 1},
		{treecmp.RegIMatch, `abc`, `(b|c).`, 1},
		{treecmp.NotRegIMatch, `abc`, `(b|c).`, 1},
	}
	for _, d := range testExprs {
		expr := &tree.ComparisonExpr{
			Operator: treecmp.MakeComparisonOperator(d.op),
			Left:     tree.NewDString(d.left),
			Right:    tree.NewDString(d.right),
		}
		ctx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		defer ctx.Stop(context.Background())
		ctx.ReCache = tree.NewRegexpCache(8)
		typedExpr, err := tree.TypeCheck(context.Background(), expr, nil, types.AnyElement)
		if err != nil {
			t.Fatalf("%v: %v", d, err)
		}
		if _, err := eval.Expr(context.Background(), ctx, typedExpr); err != nil {
			t.Fatalf("%v: %v", d, err)
		}
		if typedExpr.(*tree.ComparisonExpr).Op.EvalOp == nil {
			t.Errorf("%s: expected the comparison function to be looked up and memoized, but it wasn't", expr)
		}
		if count := ctx.ReCache.Len(); count != d.cacheCount {
			t.Errorf("%s: expected regular expression cache to contain %d compiled patterns, but found %d", expr, d.cacheCount, count)
		}
	}
}
