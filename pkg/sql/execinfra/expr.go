// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// ivarBinder is a tree.Visitor that binds ordinal references
// (IndexedVars represented by @1, @2, ...) to an IndexedVarContainer.
type ivarBinder struct {
	h   *tree.IndexedVarHelper
	err error
}

func (v *ivarBinder) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}
	if ivar, ok := expr.(*tree.IndexedVar); ok {
		newVar, err := v.h.BindIfUnbound(ivar)
		if err != nil {
			v.err = err
			return false, expr
		}
		return false, newVar
	}
	return true, expr
}

func (*ivarBinder) VisitPost(expr tree.Expr) tree.Expr { return expr }

// processExpression parses the string expression inside an Expression,
// and associates ordinal references (@1, @2, etc) with the given helper.
func processExpression(
	exprSpec execinfrapb.Expression,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	h *tree.IndexedVarHelper,
) (tree.TypedExpr, error) {
	if exprSpec.Expr == "" {
		return nil, nil
	}
	expr, err := parser.ParseExpr(exprSpec.Expr)
	if err != nil {
		return nil, err
	}

	// Bind IndexedVars to our eh.Vars.
	v := ivarBinder{h: h, err: nil}
	expr, _ = tree.WalkExpr(&v, expr)
	if v.err != nil {
		return nil, v.err
	}

	semaCtx.IVarContainer = h.Container()
	// Convert to a fully typed expression.
	typedExpr, err := tree.TypeCheck(expr, semaCtx, types.Any)
	if err != nil {
		// Type checking must succeed by now.
		return nil, errors.NewAssertionErrorWithWrappedErrf(err, "%s", expr)
	}

	// Pre-evaluate constant expressions. This is necessary to avoid repeatedly
	// re-evaluating constant values every time the expression is applied.
	//
	// TODO(solon): It would be preferable to enhance our expression serialization
	// format so this wouldn't be necessary.
	c := tree.MakeConstantEvalVisitor(evalCtx)
	expr, _ = tree.WalkExpr(&c, typedExpr)
	if err := c.Err(); err != nil {
		return nil, err
	}

	return expr.(tree.TypedExpr), nil
}

// ExprHelper implements the common logic around evaluating an expression that
// depends on a set of values.
type ExprHelper struct {
	_ util.NoCopy

	Expr tree.TypedExpr
	// Vars is used to generate IndexedVars that are "backed" by the values in
	// `Row`.
	Vars tree.IndexedVarHelper

	evalCtx *tree.EvalContext

	Types      []types.T
	Row        sqlbase.EncDatumRow
	datumAlloc sqlbase.DatumAlloc
}

func (eh *ExprHelper) String() string {
	if eh.Expr == nil {
		return "none"
	}
	return eh.Expr.String()
}

// ExprHelper implements tree.IndexedVarContainer.
var _ tree.IndexedVarContainer = &ExprHelper{}

// IndexedVarResolvedType is part of the tree.IndexedVarContainer interface.
func (eh *ExprHelper) IndexedVarResolvedType(idx int) *types.T {
	return &eh.Types[idx]
}

// IndexedVarEval is part of the tree.IndexedVarContainer interface.
func (eh *ExprHelper) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	err := eh.Row[idx].EnsureDecoded(&eh.Types[idx], &eh.datumAlloc)
	if err != nil {
		return nil, err
	}
	return eh.Row[idx].Datum.Eval(ctx)
}

// IndexedVarNodeFormatter is part of the parser.IndexedVarContainer interface.
func (eh *ExprHelper) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("$%d", idx))
	return &n
}

// Init initializes the ExprHelper.
func (eh *ExprHelper) Init(
	expr execinfrapb.Expression, types []types.T, evalCtx *tree.EvalContext,
) error {
	if expr.Empty() {
		return nil
	}
	eh.evalCtx = evalCtx
	eh.Types = types
	eh.Vars = tree.MakeIndexedVarHelper(eh, len(types))

	if expr.LocalExpr != nil {
		eh.Expr = expr.LocalExpr
		// Bind IndexedVars to our eh.Vars.
		eh.Vars.Rebind(eh.Expr, true /* alsoReset */, false /* normalizeToNonNil */)
		return nil
	}
	var err error
	semaContext := tree.MakeSemaContext()
	eh.Expr, err = processExpression(expr, evalCtx, &semaContext, &eh.Vars)
	if err != nil {
		return err
	}
	var t transform.ExprTransformContext
	if t.AggregateInExpr(eh.Expr, evalCtx.SessionData.SearchPath) {
		return errors.Errorf("expression '%s' has aggregate", eh.Expr)
	}
	return nil
}

// EvalFilter is used for filter expressions; it evaluates the expression and
// returns whether the filter passes.
func (eh *ExprHelper) EvalFilter(row sqlbase.EncDatumRow) (bool, error) {
	eh.Row = row
	eh.evalCtx.PushIVarContainer(eh)
	pass, err := sqlbase.RunFilter(eh.Expr, eh.evalCtx)
	eh.evalCtx.PopIVarContainer()
	return pass, err
}

// Eval - given a row - evaluates the wrapped expression and returns the
// resulting datum. For example, given a row (1, 2, 3, 4, 5):
//  '@2' would return '2'
//  '@2 + @5' would return '7'
//  '@1' would return '1'
//  '@2 + 10' would return '12'
func (eh *ExprHelper) Eval(row sqlbase.EncDatumRow) (tree.Datum, error) {
	eh.Row = row

	eh.evalCtx.PushIVarContainer(eh)
	d, err := eh.Expr.Eval(eh.evalCtx)
	eh.evalCtx.PopIVarContainer()
	return d, err
}
