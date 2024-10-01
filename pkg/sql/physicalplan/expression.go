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

	replacer *iVarAndSubqueryReplacer
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

	// Replace subqueries with their results (they must have been executed
	// before the main query) and remap IndexedVars.
	evalCtx := ef.eCtx.EvalContext()
	if ef.replacer == nil {
		// Lazily allocate the replacer.
		ef.replacer = newIVarAndSubqueryReplacer(evalCtx, ef.indexVarMap, ef.indexedVarsHint)
	}
	newExpr, _ := tree.WalkExpr(ef.replacer, expr)
	if ef.replacer.err != nil {
		return execinfrapb.Expression{}, ef.replacer.err
	}
	expr = newExpr.(tree.TypedExpr)

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

// iVarAndSubqueryReplacer is a tree.Visitor that replaces subqueries with their
// results (they must have been executed before the main query) and remaps
// tree.IndexedVars in expr using indexVarMap, if it is non-nil.
type iVarAndSubqueryReplacer struct {
	evalCtx       *eval.Context
	indexVarMap   []int
	indexVarAlloc []tree.IndexedVar
	err           error
}

var _ tree.Visitor = &iVarAndSubqueryReplacer{}

func newIVarAndSubqueryReplacer(
	evalCtx *eval.Context, indexVarMap []int, indexVarHint int,
) *iVarAndSubqueryReplacer {
	r := &iVarAndSubqueryReplacer{
		evalCtx:     evalCtx,
		indexVarMap: indexVarMap,
	}
	if indexVarHint > 0 {
		// If there is a hint for the number of indexed vars that will be
		// needed, allocate them ahead of time.
		r.indexVarAlloc = make([]tree.IndexedVar, indexVarHint)
	}
	return r
}

func (r *iVarAndSubqueryReplacer) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	switch t := expr.(type) {
	case *tree.Subquery:
		val, err := r.evalCtx.Planner.EvalSubquery(t)
		if err != nil {
			r.err = err
			return false, expr
		}
		newExpr := tree.Expr(val)
		typ := t.ResolvedType()
		if _, isTuple := val.(*tree.DTuple); !isTuple && typ.Family() != types.UnknownFamily && typ.Family() != types.TupleFamily {
			newExpr = tree.NewTypedCastExpr(val, typ)
		}
		return false, newExpr

	case *tree.IndexedVar:
		if r.indexVarMap == nil {
			// No-op if there is no index var map.
			return false, expr
		}
		if t.Idx == r.indexVarMap[t.Idx] {
			// Avoid the identical remapping since it's redundant.
			return false, expr
		}
		newIvar := r.allocIndexedVar()
		*newIvar = *t
		newIvar.Idx = r.indexVarMap[t.Idx]
		return false, newIvar

	default:
		return true, expr
	}
}

func (*iVarAndSubqueryReplacer) VisitPost(expr tree.Expr) tree.Expr { return expr }

func (r *iVarAndSubqueryReplacer) allocIndexedVar() *tree.IndexedVar {
	if len(r.indexVarAlloc) == 0 {
		// Allocate indexed vars in small batches.
		r.indexVarAlloc = make([]tree.IndexedVar, 4)
	}
	iv := &r.indexVarAlloc[0]
	r.indexVarAlloc = r.indexVarAlloc[1:]
	return iv
}
