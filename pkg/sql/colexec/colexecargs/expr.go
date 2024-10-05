// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecargs

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var exprHelperPool = sync.Pool{
	New: func() interface{} {
		return &ExprHelper{}
	},
}

// NewExprHelper returns a new ExprHelper.
func NewExprHelper() *ExprHelper {
	return exprHelperPool.Get().(*ExprHelper)
}

// ExprHelper is a utility struct that helps with expression handling in the
// vectorized engine.
type ExprHelper struct {
	SemaCtx *tree.SemaContext
}

// ProcessExpr processes the given expression and returns a well-typed
// expression. Note that SemaCtx must be already set on h.
//
// evalCtx will not be mutated.
func (h *ExprHelper) ProcessExpr(
	ctx context.Context, expr execinfrapb.Expression, evalCtx *eval.Context, typs []*types.T,
) (tree.TypedExpr, error) {
	if expr.LocalExpr != nil {
		return expr.LocalExpr, nil
	}
	return execinfrapb.DeserializeExpr(ctx, expr, typs, h.SemaCtx, evalCtx)
}
