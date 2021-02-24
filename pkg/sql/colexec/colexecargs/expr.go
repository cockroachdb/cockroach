// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecargs

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
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
	helper execinfrapb.ExprHelper
}

// ProcessExpr processes the given expression and returns a well-typed
// expression.
func (h *ExprHelper) ProcessExpr(
	expr execinfrapb.Expression,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	typs []*types.T,
) (tree.TypedExpr, error) {
	if expr.LocalExpr != nil {
		return expr.LocalExpr, nil
	}
	h.helper.Types = typs
	tempVars := tree.MakeIndexedVarHelper(&h.helper, len(typs))
	return execinfrapb.DeserializeExpr(expr.Expr, semaCtx, evalCtx, &tempVars)
}

// Remove unused warning.
var _ = findIVarsInRange

// findIVarsInRange searches Expr for presence of tree.IndexedVars with indices
// in range [start, end). It returns a slice containing all such indices.
func findIVarsInRange(expr execinfrapb.Expression, start int, end int) ([]uint32, error) {
	res := make([]uint32, 0)
	if start >= end {
		return res, nil
	}
	var exprToWalk tree.Expr
	if expr.LocalExpr != nil {
		exprToWalk = expr.LocalExpr
	} else {
		e, err := parser.ParseExpr(expr.Expr)
		if err != nil {
			return nil, err
		}
		exprToWalk = e
	}
	visitor := ivarExpressionVisitor{ivarSeen: make([]bool, end)}
	_, _ = tree.WalkExpr(visitor, exprToWalk)
	for i := start; i < end; i++ {
		if visitor.ivarSeen[i] {
			res = append(res, uint32(i))
		}
	}
	return res, nil
}

type ivarExpressionVisitor struct {
	ivarSeen []bool
}

var _ tree.Visitor = &ivarExpressionVisitor{}

// VisitPre is a part of tree.Visitor interface.
func (i ivarExpressionVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	switch e := expr.(type) {
	case *tree.IndexedVar:
		if e.Idx < len(i.ivarSeen) {
			i.ivarSeen[e.Idx] = true
		}
		return false, expr
	default:
		return true, expr
	}
}

// VisitPost is a part of tree.Visitor interface.
func (i ivarExpressionVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }
