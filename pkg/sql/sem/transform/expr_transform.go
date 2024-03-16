// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package transform

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/normalize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ExprTransformContext supports the methods that test expression
// properties and can normalize expressions, defined below.  This
// should be used in planner instance to avoid re-allocation of these
// visitors between uses.
type ExprTransformContext struct {
	normalizeVisitor normalize.Visitor
}

// NormalizeExpr is a wrapper around EvalContex.Expr which
// avoids allocation of a normalizeVisitor. See var_expr.go for
// details.
func (t *ExprTransformContext) NormalizeExpr(
	ctx context.Context, evalCtx *eval.Context, typedExpr tree.TypedExpr,
) (tree.TypedExpr, error) {
	if evalCtx.SkipNormalize {
		return typedExpr, nil
	}
	t.normalizeVisitor = normalize.MakeNormalizeVisitor(ctx, evalCtx)
	expr, _ := tree.WalkExpr(&t.normalizeVisitor, typedExpr)
	if err := t.normalizeVisitor.Err(); err != nil {
		return nil, err
	}
	return expr.(tree.TypedExpr), nil
}
