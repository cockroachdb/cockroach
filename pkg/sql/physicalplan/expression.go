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
//
// Note: If making multiple calls to MakeExpression, consider using ExprFactory
// as it can be more efficient.
func MakeExpression(
	ctx context.Context, expr tree.TypedExpr, exprContext ExprContext, indexVarMap []int,
) (execinfrapb.Expression, error) {
	var ef ExprFactory
	ef.Init(ctx, exprContext, indexVarMap)
	return ef.Make(expr)
}

// ExprFactory creates execinfrapb.Expressions. It should be used instead of
// MakeExpression when making multiple expressions because it will reuse
// allocated objects between calls to ExprFactory.Make.
type ExprFactory struct {
	ctx             context.Context
	eCtx            ExprContext
	indexVarMap     []int
	indexedVarsHint int

	subqueryVisitor *evalAndReplaceSubqueryVisitor
	remapper        *ivarRemapper
}

// Init initializes the ExprFactory.
//
// exprContext can be nil in which case a fakeExprCtx will be used.
//
// The factory can optionally remap columns in expression by passing an
// indexVarMap: an IndexedVar with index i becomes column indexVarMap[i].
func (ef *ExprFactory) Init(ctx context.Context, exprContext ExprContext, indexVarMap []int) {
	*ef = ExprFactory{
		ctx:         ctx,
		eCtx:        exprContext,
		indexVarMap: indexVarMap,
	}
}

// IndexedVarsHint can be used to provide the ExprFactory with an estimate of how
// many indexed vars exist across all expressions that will be built with the
// factory. If the indexed vars need to be remapped, this estimate will be used
// to batch allocations of indexed vars.
//
// NOTE: This function must be called before any calls to Make to ensure that
// the hint is used.
func (ef *ExprFactory) IndexedVarsHint(hint int) {
	ef.indexedVarsHint = hint
}

// Make creates a execinfrapb.Expression. Init must be called on the ExprFactory
// before Make.
//
// The execinfrapb.Expression uses the placeholder syntax (@1, @2, @3..) to
// refer to columns.
//
// If the ExprFactory was initialized with an indexVarMap, then the columns in
// the expression are remapped: an IndexedVar with index i becomes column
// indexVarMap[i].
func (ef *ExprFactory) Make(expr tree.TypedExpr) (execinfrapb.Expression, error) {
	if expr == nil {
		return execinfrapb.Expression{}, nil
	}
	if ef.eCtx == nil {
		ef.eCtx = &fakeExprContext{}
	}

	// Always replace the subqueries with their results (they must have been
	// executed before the main query).
	evalCtx := ef.eCtx.EvalContext()
	if ef.subqueryVisitor == nil {
		// Lazily allocate the subquery visitor.
		ef.subqueryVisitor = &evalAndReplaceSubqueryVisitor{evalCtx: evalCtx}
	}
	outExpr, _ := tree.WalkExpr(ef.subqueryVisitor, expr)
	if ef.subqueryVisitor.err != nil {
		return execinfrapb.Expression{}, ef.subqueryVisitor.err
	}
	expr = outExpr.(tree.TypedExpr)

	expr = ef.maybeRemapIVarsInTypedExpr(expr)
	expression := execinfrapb.Expression{LocalExpr: expr}
	if ef.eCtx.IsLocal() {
		return expression, nil
	}

	// Since the plan is not fully local, serialize the expression.
	fmtCtx := execinfrapb.ExprFmtCtxBase(ef.ctx, evalCtx)
	fmtCtx.FormatNode(expr)
	if log.V(1) {
		log.Infof(ef.ctx, "Expr %s:\n%s", fmtCtx.String(), tree.ExprDebugString(expr))
	}
	expression.Expr = fmtCtx.CloseAndGetString()
	return expression, nil
}

// maybeRemapIVarsInTypedExpr remaps tree.IndexedVars in expr using indexVarMap
// and the new expression is returned. If indexVarMap is nil, no remapping
// occurs and the expression is returned as-is.
func (ef *ExprFactory) maybeRemapIVarsInTypedExpr(expr tree.TypedExpr) tree.TypedExpr {
	if ef.indexVarMap == nil {
		return expr
	}
	if ef.remapper == nil {
		// Lazily allocate the IndexVar remapper visitor.
		ef.remapper = &ivarRemapper{indexVarMap: ef.indexVarMap}
		if ef.indexedVarsHint > 0 {
			// If there is a hint for the number of indexed vars that will be
			// needed, allocate them ahead of time.
			ef.remapper.indexVarAlloc = make([]tree.IndexedVar, ef.indexedVarsHint)
		}
	}
	newExpr, _ := tree.WalkExpr(ef.remapper, expr)
	return newExpr.(tree.TypedExpr)
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

type ivarRemapper struct {
	indexVarMap   []int
	indexVarAlloc []tree.IndexedVar
}

var _ tree.Visitor = &ivarRemapper{}

func (v *ivarRemapper) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if ivar, ok := expr.(*tree.IndexedVar); ok {
		newIvar := v.allocIndexedVar()
		*newIvar = *ivar
		newIvar.Idx = v.indexVarMap[ivar.Idx]
		return false, newIvar
	}
	return true, expr
}

func (*ivarRemapper) VisitPost(expr tree.Expr) tree.Expr { return expr }

func (v *ivarRemapper) allocIndexedVar() *tree.IndexedVar {
	if len(v.indexVarAlloc) == 0 {
		// Allocate indexed vars in small batches.
		v.indexVarAlloc = make([]tree.IndexedVar, 4)
	}
	iv := &v.indexVarAlloc[0]
	v.indexVarAlloc = v.indexVarAlloc[1:]
	return iv
}
