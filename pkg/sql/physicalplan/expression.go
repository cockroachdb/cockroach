// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file contains helper code to populate execinfrapb.Expressions during
// planning.

package physicalplan

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// ExprContext is an interface containing objects necessary for creating
// execinfrapb.Expressions.
type ExprContext interface {
	// EvalContext returns the eval.Context for planning.
	EvalContext() *eval.Context

	// IsLocal returns true if the current plan is local.
	IsLocal() bool
}

// fakeExprContext is a fake implementation of ExprContext that always behaves
// as if it were part of a non-local query.
type fakeExprContext struct{}

var _ ExprContext = fakeExprContext{}

func (fakeExprContext) EvalContext() *eval.Context {
	return &eval.Context{}
}

func (fakeExprContext) IsLocal() bool {
	return false
}

// MakeExpression creates a execinfrapb.Expression.
//
// The execinfrapb.Expression uses the placeholder syntax (@1, @2, @3..) to
// refer to columns.
//
// The expr uses IndexedVars to refer to columns. The caller can optionally
// remap these columns by passing an indexVarMap: an IndexedVar with index i
// becomes column indexVarMap[i].
//
// ctx can be nil in which case a fakeExprCtx will be used.
func MakeExpression(
	ctx context.Context, expr tree.TypedExpr, exprContext ExprContext, indexVarMap []int,
) (execinfrapb.Expression, error) {
	if expr == nil {
		return execinfrapb.Expression{}, nil
	}
	if exprContext == nil {
		exprContext = &fakeExprContext{}
	}

	// Always replace the subqueries with their results (they must have been
	// executed before the main query).
	evalCtx := exprContext.EvalContext()
	subqueryVisitor := &evalAndReplaceSubqueryVisitor{
		evalCtx: evalCtx,
	}
	outExpr, _ := tree.WalkExpr(subqueryVisitor, expr)
	if subqueryVisitor.err != nil {
		return execinfrapb.Expression{}, subqueryVisitor.err
	}
	expr = outExpr.(tree.TypedExpr)

	if indexVarMap != nil {
		// Remap our indexed vars.
		expr = RemapIVarsInTypedExpr(expr, indexVarMap)
	}
	expression := execinfrapb.Expression{LocalExpr: expr}
	if exprContext.IsLocal() {
		return expression, nil
	}

	// Since the plan is not fully local, serialize the expression.
	fmtCtx := execinfrapb.ExprFmtCtxBase(ctx, evalCtx)
	fmtCtx.FormatNode(expr)
	if log.V(1) {
		log.Infof(ctx, "Expr %s:\n%s", fmtCtx.String(), tree.ExprDebugString(expr))
	}
	expression.Expr = fmtCtx.CloseAndGetString()
	return expression, nil
}

type evalAndReplaceSubqueryVisitor struct {
	evalCtx *eval.Context
	err     error
}

var _ tree.Visitor = &evalAndReplaceSubqueryVisitor{}

func (e *evalAndReplaceSubqueryVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	switch expr := expr.(type) {
	case *tree.Subquery:
		val, err := e.evalCtx.Planner.EvalSubquery(expr)
		if err != nil {
			e.err = err
			return false, expr
		}
		newExpr := tree.Expr(val)
		typ := expr.ResolvedType()
		if _, isTuple := val.(*tree.DTuple); !isTuple && typ.Family() != types.UnknownFamily && typ.Family() != types.TupleFamily {
			newExpr = tree.NewTypedCastExpr(val, typ)
		}
		return false, newExpr
	default:
		return true, expr
	}
}

func (evalAndReplaceSubqueryVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// RemapIVarsInTypedExpr remaps tree.IndexedVars in expr using indexVarMap.
// Note that a new expression is returned.
func RemapIVarsInTypedExpr(expr tree.TypedExpr, indexVarMap []int) tree.TypedExpr {
	v := &ivarRemapper{indexVarMap: indexVarMap}
	newExpr, _ := tree.WalkExpr(v, expr)
	return newExpr.(tree.TypedExpr)
}

type ivarRemapper struct {
	indexVarMap []int
}

var _ tree.Visitor = &ivarRemapper{}

func (v *ivarRemapper) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if ivar, ok := expr.(*tree.IndexedVar); ok {
		newIvar := *ivar
		newIvar.Idx = v.indexVarMap[ivar.Idx]
		return false, &newIvar
	}
	return true, expr
}

func (*ivarRemapper) VisitPost(expr tree.Expr) tree.Expr { return expr }
