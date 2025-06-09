// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execexpr

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/normalize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// DeserializeExpr deserializes expr and binds the indexed variables to the
// provided IndexedVarHelper.
func DeserializeExpr(
	ctx context.Context,
	expr execinfrapb.Expression,
	typs []*types.T,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
) (tree.TypedExpr, error) {
	if expr.Expr == "" {
		return nil, nil
	}
	eh := helper{evalCtx: evalCtx, semaCtx: semaCtx, types: typs}
	return eh.deserializeExpr(ctx, expr)
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

// Helper implements the common logic around evaluating an expression that
// depends on a set of values.
type Helper struct {
	helper
	expr tree.TypedExpr
}

// Helper implements tree.IndexedVarContainer.
var _ eval.IndexedVarContainer = &Helper{}

// Init initializes the Helper.
func (eh *Helper) Init(
	ctx context.Context,
	expr execinfrapb.Expression,
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
func (eh *Helper) Expr() tree.TypedExpr {
	return eh.expr
}

// Eval - given a row - evaluates the wrapped expression and returns the
// resulting datum. For example, given a row (1, 2, 3, 4, 5):
//
//	'@2' would return '2'
//	'@2 + @5' would return '7'
//	'@1' would return '1'
//	'@2 + 10' would return '12'
func (eh *Helper) Eval(ctx context.Context, row rowenc.EncDatumRow) (tree.Datum, error) {
	return eh.eval(ctx, eh.expr, row)
}

// EvalFilter is used for filter expressions; it evaluates the expression and
// returns whether the filter passes.
func (eh *Helper) EvalFilter(ctx context.Context, row rowenc.EncDatumRow) (bool, error) {
	return eh.evalFilter(ctx, eh.expr, row)
}

// MultiHelper is similar to Helper. It is preferred in cases where
// there are multiple expressions to evaluate.
type MultiHelper struct {
	h     helper
	exprs []tree.TypedExpr
}

// Init initializes the MultiHelper.
func (eh *MultiHelper) Init(
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
func (eh *MultiHelper) Expr(i int) tree.TypedExpr {
	return eh.exprs[i]
}

// ExprCount returns the number of expressions that the helper can store. Note
// that if AddExpr was not called for every index, some expressions will not
// have been set.
func (eh *MultiHelper) ExprCount() int {
	if eh == nil {
		return 0
	}
	return len(eh.exprs)
}

// AddExpr sets the given expression as the i-th expression in the helper.
func (eh *MultiHelper) AddExpr(
	ctx context.Context, expr execinfrapb.Expression, i int,
) (err error) {
	eh.exprs[i], err = eh.h.prepareExpr(ctx, expr)
	return err
}

// SetRow sets the internal row of the expression helper.
// TODO(mgartner): This is only used in projectSetProcessor and ideally it could
// be removed.
func (eh *MultiHelper) SetRow(row rowenc.EncDatumRow) {
	eh.h.row = row
}

// EvalExpr evaluates the i-th expression with the given row and returns the
// resulting datum.
func (eh *MultiHelper) EvalExpr(
	ctx context.Context, i int, row rowenc.EncDatumRow,
) (tree.Datum, error) {
	return eh.h.eval(ctx, eh.exprs[i], row)
}

// IVarContainer returns the tree.IndexedVarContainer of the expression helper.
func (eh *MultiHelper) IVarContainer() tree.IndexedVarContainer {
	return &eh.h
}

// Reset clears all references in the expression helper.
func (eh *MultiHelper) Reset() {
	if eh == nil {
		return
	}
	eh.h = helper{}
	for i := range eh.exprs {
		eh.exprs[i] = nil
	}
	eh.exprs = eh.exprs[:0]
}

// helper is a base implementation of an expression helper used by both
// Helper and MultiHelper.
type helper struct {
	evalCtx    *eval.Context
	semaCtx    *tree.SemaContext
	datumAlloc *tree.DatumAlloc

	types []*types.T
	row   rowenc.EncDatumRow
}

// helper implements tree.IndexedVarContainer.
var _ eval.IndexedVarContainer = &helper{}

// IndexedVarResolvedType is part of the tree.IndexedVarContainer interface.
func (eh *helper) IndexedVarResolvedType(idx int) *types.T {
	return eh.types[idx]
}

// IndexedVarEval is part of the eval.IndexedVarContainer interface.
func (eh *helper) IndexedVarEval(idx int) (tree.Datum, error) {
	err := eh.row[idx].EnsureDecoded(eh.types[idx], eh.datumAlloc)
	if err != nil {
		return nil, err
	}
	return eh.row[idx].Datum, nil
}

// init initializes the helper.
func (eh *helper) init(
	ctx context.Context, types []*types.T, semaCtx *tree.SemaContext, evalCtx *eval.Context,
) error {
	*eh = helper{
		evalCtx:    evalCtx,
		semaCtx:    semaCtx,
		types:      types,
		datumAlloc: &tree.DatumAlloc{},
	}
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
func (eh *helper) prepareExpr(
	ctx context.Context, expr execinfrapb.Expression,
) (tree.TypedExpr, error) {
	if expr.Empty() {
		return nil, nil
	}
	if expr.LocalExpr != nil {
		return expr.LocalExpr, nil
	}
	return eh.deserializeExpr(ctx, expr)
}

// deserializeExpr converts the given expression's string representation into a
// tree.TypedExpr and returns it.
func (eh *helper) deserializeExpr(
	ctx context.Context, e execinfrapb.Expression,
) (tree.TypedExpr, error) {
	if e.Expr == "" {
		return nil, nil
	}

	expr, err := parser.ParseExpr(e.Expr)
	if err != nil {
		return nil, err
	}

	// Set the iVarContainer for semaCtx so that indexed vars can be
	// type-checked.
	originalIVarContainer := eh.semaCtx.IVarContainer
	eh.semaCtx.IVarContainer = eh
	defer func() { eh.semaCtx.IVarContainer = originalIVarContainer }()

	// Type-check the expression.
	typedExpr, err := tree.TypeCheck(ctx, expr, eh.semaCtx, types.AnyElement)
	if err != nil {
		// Type checking must succeed.
		return nil, errors.NewAssertionErrorWithWrappedErrf(err, "%s", expr)
	}

	// Pre-evaluate constant expressions. This is necessary to avoid repeatedly
	// re-evaluating constant values every time the expression is applied.
	//
	// TODO(solon): It would be preferable to enhance our expression
	// serialization format so this wouldn't be necessary.
	c := normalize.MakeConstantEvalVisitor(ctx, eh.evalCtx)
	expr, _ = tree.WalkExpr(&c, typedExpr)
	if err := c.Err(); err != nil {
		return nil, err
	}

	return expr.(tree.TypedExpr), nil
}

// evalFilter is used for filter expressions; it evaluates the expression and
// returns whether the filter passes.
func (eh *helper) evalFilter(
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
func (eh *helper) eval(
	ctx context.Context, expr tree.TypedExpr, row rowenc.EncDatumRow,
) (tree.Datum, error) {
	eh.row = row
	eh.evalCtx.PushIVarContainer(eh)
	d, err := eval.Expr(ctx, eh.evalCtx, expr)
	eh.evalCtx.PopIVarContainer()
	return d, err
}
