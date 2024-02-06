// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfrapb

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/normalize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// DeserializeExpr deserializes expr, binds the indexed variables to the
// provided IndexedVarHelper, and evaluates any constants in the expression.
//
// evalCtx will not be mutated.
func DeserializeExpr(
	ctx context.Context,
	expr string,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	vars *tree.IndexedVarHelper,
) (tree.TypedExpr, error) {
	if expr == "" {
		return nil, nil
	}

	deserializedExpr, err := processExpression(ctx, Expression{Expr: expr}, evalCtx, semaCtx, vars)
	if err != nil {
		return deserializedExpr, err
	}
	var t transform.ExprTransformContext
	if t.AggregateInExpr(ctx, deserializedExpr, evalCtx.SessionData().SearchPath) {
		return nil, errors.Errorf("expression '%s' has aggregate", deserializedExpr)
	}
	return deserializedExpr, nil
}

// DeserializeExprWithTypes is similar to Deserialize. It can be used when the
// types of indexed vars are available, but a tree.IndexedVarHelper is not.
func DeserializeExprWithTypes(
	ctx context.Context,
	expr string,
	typs []*types.T,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
) (tree.TypedExpr, error) {
	if expr == "" {
		return nil, nil
	}
	var eh exprHelper
	eh.types = typs
	tempVars := tree.MakeIndexedVarHelper(&eh, len(typs))
	return DeserializeExpr(ctx, expr, semaCtx, evalCtx, &tempVars)
}

// RunFilter runs a filter expression and returns whether the filter passes.
func RunFilter(ctx context.Context, filter tree.TypedExpr, evalCtx *eval.Context) (bool, error) {
	if filter == nil {
		return true, nil
	}

	d, err := eval.Expr(ctx, evalCtx, filter)
	if err != nil {
		return false, err
	}

	return d == tree.DBoolTrue, nil
}

// processExpression parses the string expression inside an Expression,
// and associates ordinal references (@1, @2, etc) with the given helper.
//
// evalCtx will not be mutated.
func processExpression(
	ctx context.Context,
	exprSpec Expression,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	h *tree.IndexedVarHelper,
) (tree.TypedExpr, error) {
	if exprSpec.Expr == "" {
		return nil, nil
	}
	expr, err := parser.ParseExprWithInt(
		exprSpec.Expr,
		parser.NakedIntTypeFromDefaultIntSize(evalCtx.SessionData().DefaultIntSize),
	)
	if err != nil {
		return nil, err
	}

	// Bind IndexedVars to h.
	expr = h.Rebind(expr)

	semaCtx.IVarContainer = h.Container()
	// Convert to a fully typed expression.
	typedExpr, err := tree.TypeCheck(ctx, expr, semaCtx, types.Any)
	if err != nil {
		// Type checking must succeed by now.
		return nil, errors.NewAssertionErrorWithWrappedErrf(err, "%s", expr)
	}

	// Pre-evaluate constant expressions. This is necessary to avoid repeatedly
	// re-evaluating constant values every time the expression is applied.
	//
	// TODO(solon): It would be preferable to enhance our expression serialization
	// format so this wouldn't be necessary.
	c := normalize.MakeConstantEvalVisitor(ctx, evalCtx)
	expr, _ = tree.WalkExpr(&c, typedExpr)
	if err := c.Err(); err != nil {
		return nil, err
	}

	return expr.(tree.TypedExpr), nil
}

// ExprHelper implements the common logic around evaluating an expression that
// depends on a set of values.
type ExprHelper struct {
	exprHelper
	expr tree.TypedExpr
}

// ExprHelper implements tree.IndexedVarContainer.
var _ eval.IndexedVarContainer = &ExprHelper{}

// Init initializes the ExprHelper.
func (eh *ExprHelper) Init(
	ctx context.Context,
	expr Expression,
	types []*types.T,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
) (err error) {
	if err = eh.init(ctx, types, semaCtx, evalCtx); err != nil {
		return err
	}
	if eh.expr, err = eh.prepareExpr(ctx, expr); err != nil {
		return err
	}
	return nil
}

// Expr returns the typed expression within the helper.
func (eh *ExprHelper) Expr() tree.TypedExpr {
	return eh.expr
}

// Eval - given a row - evaluates the wrapped expression and returns the
// resulting datum. For example, given a row (1, 2, 3, 4, 5):
//
//	'@2' would return '2'
//	'@2 + @5' would return '7'
//	'@1' would return '1'
//	'@2 + 10' would return '12'
func (eh *ExprHelper) Eval(ctx context.Context, row rowenc.EncDatumRow) (tree.Datum, error) {
	return eh.eval(ctx, eh.expr, row)
}

// EvalFilter is used for filter expressions; it evaluates the expression and
// returns whether the filter passes.
func (eh *ExprHelper) EvalFilter(ctx context.Context, row rowenc.EncDatumRow) (bool, error) {
	return eh.evalFilter(ctx, eh.expr, row)
}

// MultiExprHelper is similar to ExprHelper. It is preferred in cases where
// there are multiple expressions to evaluate.
type MultiExprHelper struct {
	h     exprHelper
	exprs []tree.TypedExpr
}

// Init initializes the MultiExprHelper.
func (eh *MultiExprHelper) Init(
	ctx context.Context,
	numExprs int,
	types []*types.T,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
) (err error) {
	if err = eh.h.init(ctx, types, semaCtx, evalCtx); err != nil {
		return err
	}
	// Reuse the exprs slice if there is enough capacity.
	if cap(eh.exprs) >= numExprs {
		eh.exprs = eh.exprs[:numExprs]
	} else {
		eh.exprs = make([]tree.TypedExpr, numExprs)
	}
	return nil
}

// Expr returns the i-th typed expression within the helper. If AddExpr was not
// called for the given index, then Expr will return nil.
func (eh *MultiExprHelper) Expr(i int) tree.TypedExpr {
	return eh.exprs[i]
}

// ExprCount returns the number of expressions that the helper can store. Note
// that if AddExpr was not called for every index, some expressions will not
// have been set.
func (eh *MultiExprHelper) ExprCount() int {
	if eh == nil {
		return 0
	}
	return len(eh.exprs)
}

// AddExpr sets the given expression as the i-th expression in the helper.
func (eh *MultiExprHelper) AddExpr(ctx context.Context, expr Expression, i int) (err error) {
	eh.exprs[i], err = eh.h.prepareExpr(ctx, expr)
	return err
}

// SetRow sets the internal row of the expression helper.
// TODO(mgartner): This is only used in projectSetProcessor and ideally it could
// be removed.
func (eh *MultiExprHelper) SetRow(row rowenc.EncDatumRow) {
	eh.h.row = row
}

// EvalExpr evaluates the i-th expression with the given row and returns the
// resulting datum.
func (eh *MultiExprHelper) EvalExpr(
	ctx context.Context, i int, row rowenc.EncDatumRow,
) (tree.Datum, error) {
	return eh.h.eval(ctx, eh.exprs[i], row)
}

// IVarContainer returns the tree.IndexedVarContainer of the expression helper.
func (eh *MultiExprHelper) IVarContainer() tree.IndexedVarContainer {
	return &eh.h
}

// Reset clears all references in the expression helper.
func (eh *MultiExprHelper) Reset() {
	if eh == nil {
		return
	}
	eh.h = exprHelper{}
	for i := range eh.exprs {
		eh.exprs[i] = nil
	}
	eh.exprs = eh.exprs[:0]
}

// exprHelper is a base implementation of an expressoin helper used by both
// ExprHelper and MultiExprHelper.
type exprHelper struct {
	evalCtx    *eval.Context
	semaCtx    *tree.SemaContext
	datumAlloc *tree.DatumAlloc

	// vars is used to generate IndexedVars that are "backed" by the values in
	// row.
	vars tree.IndexedVarHelper

	types []*types.T
	row   rowenc.EncDatumRow
}

// exprHelper implements tree.IndexedVarContainer.
var _ eval.IndexedVarContainer = &exprHelper{}

// IndexedVarResolvedType is part of the tree.IndexedVarContainer interface.
func (eh *exprHelper) IndexedVarResolvedType(idx int) *types.T {
	return eh.types[idx]
}

// IndexedVarEval is part of the eval.IndexedVarContainer interface.
func (eh *exprHelper) IndexedVarEval(
	ctx context.Context, idx int, e tree.ExprEvaluator,
) (tree.Datum, error) {
	err := eh.row[idx].EnsureDecoded(eh.types[idx], eh.datumAlloc)
	if err != nil {
		return nil, err
	}
	return eh.row[idx].Datum.Eval(ctx, e)
}

// init initializes the exprHelper.
func (eh *exprHelper) init(
	ctx context.Context, types []*types.T, semaCtx *tree.SemaContext, evalCtx *eval.Context,
) error {
	eh.evalCtx = evalCtx
	eh.semaCtx = semaCtx
	eh.types = types
	eh.vars = tree.MakeIndexedVarHelper(eh, len(types))
	eh.datumAlloc = &tree.DatumAlloc{}
	if semaCtx.TypeResolver != nil {
		for _, t := range types {
			if err := typedesc.EnsureTypeIsHydrated(ctx, t, semaCtx.TypeResolver.(catalog.TypeDescriptorResolver)); err != nil {
				return err
			}
		}
	}
	return nil
}

// prepareExpr converts the given Expression into a tree.TypedExpr and returns
// it.
func (eh *exprHelper) prepareExpr(ctx context.Context, expr Expression) (tree.TypedExpr, error) {
	if expr.Empty() {
		return nil, nil
	}
	if expr.LocalExpr != nil {
		eh.vars.RebindTyped(expr.LocalExpr)
		return expr.LocalExpr, nil
	}
	return DeserializeExpr(ctx, expr.Expr, eh.semaCtx, eh.evalCtx, &eh.vars)
}

// evalFilter is used for filter expressions; it evaluates the expression and
// returns whether the filter passes.
func (eh *exprHelper) evalFilter(
	ctx context.Context, expr tree.TypedExpr, row rowenc.EncDatumRow,
) (bool, error) {
	eh.row = row
	eh.evalCtx.PushIVarContainer(eh)
	pass, err := RunFilter(ctx, expr, eh.evalCtx)
	eh.evalCtx.PopIVarContainer()
	return pass, err
}

// eval - given an expression and a row - evaluates the wrapped expression and
// returns the resulting datum. For example, given a row (1, 2, 3, 4, 5):
//
//	'@2' would return '2'
//	'@2 + @5' would return '7'
//	'@1' would return '1'
//	'@2 + 10' would return '12'
func (eh *exprHelper) eval(
	ctx context.Context, expr tree.TypedExpr, row rowenc.EncDatumRow,
) (tree.Datum, error) {
	eh.row = row
	eh.evalCtx.PushIVarContainer(eh)
	d, err := eval.Expr(ctx, eh.evalCtx, expr)
	eh.evalCtx.PopIVarContainer()
	return d, err
}
