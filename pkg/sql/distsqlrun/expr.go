// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package distsqlrun

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
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
	exprSpec distsqlpb.Expression,
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

	// Bind IndexedVars to our eh.vars.
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

// exprHelper implements the common logic around evaluating an expression that
// depends on a set of values.
type exprHelper struct {
	_ util.NoCopy

	expr tree.TypedExpr
	// vars is used to generate IndexedVars that are "backed" by the values in
	// `row`.
	vars tree.IndexedVarHelper

	evalCtx *tree.EvalContext

	types      []types.T
	row        sqlbase.EncDatumRow
	datumAlloc sqlbase.DatumAlloc
}

func (eh *exprHelper) String() string {
	if eh.expr == nil {
		return "none"
	}
	return eh.expr.String()
}

// exprHelper implements tree.IndexedVarContainer.
var _ tree.IndexedVarContainer = &exprHelper{}

// IndexedVarResolvedType is part of the tree.IndexedVarContainer interface.
func (eh *exprHelper) IndexedVarResolvedType(idx int) *types.T {
	return &eh.types[idx]
}

// IndexedVarEval is part of the tree.IndexedVarContainer interface.
func (eh *exprHelper) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	err := eh.row[idx].EnsureDecoded(&eh.types[idx], &eh.datumAlloc)
	if err != nil {
		return nil, err
	}
	return eh.row[idx].Datum.Eval(ctx)
}

// IndexedVarNodeFormatter is part of the parser.IndexedVarContainer interface.
func (eh *exprHelper) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("$%d", idx))
	return &n
}

func (eh *exprHelper) init(
	expr distsqlpb.Expression, types []types.T, evalCtx *tree.EvalContext,
) error {
	if expr.Empty() {
		return nil
	}
	eh.evalCtx = evalCtx
	eh.types = types
	eh.vars = tree.MakeIndexedVarHelper(eh, len(types))

	if expr.LocalExpr != nil {
		eh.expr = expr.LocalExpr
		// Bind IndexedVars to our eh.vars.
		eh.vars.Rebind(eh.expr, true /* alsoReset */, false /* normalizeToNonNil */)
		return nil
	}
	var err error
	semaContext := tree.MakeSemaContext()
	eh.expr, err = processExpression(expr, evalCtx, &semaContext, &eh.vars)
	if err != nil {
		return err
	}
	var t transform.ExprTransformContext
	if t.AggregateInExpr(eh.expr, evalCtx.SessionData.SearchPath) {
		return errors.Errorf("expression '%s' has aggregate", eh.expr)
	}
	return nil
}

// evalFilter is used for filter expressions; it evaluates the expression and
// returns whether the filter passes.
func (eh *exprHelper) evalFilter(row sqlbase.EncDatumRow) (bool, error) {
	eh.row = row
	eh.evalCtx.PushIVarContainer(eh)
	pass, err := sqlbase.RunFilter(eh.expr, eh.evalCtx)
	eh.evalCtx.PopIVarContainer()
	return pass, err
}

// Given a row, eval evaluates the wrapped expression and returns the
// resulting datum. For example, given a row (1, 2, 3, 4, 5):
//  '@2' would return '2'
//  '@2 + @5' would return '7'
//  '@1' would return '1'
//  '@2 + 10' would return '12'
func (eh *exprHelper) eval(row sqlbase.EncDatumRow) (tree.Datum, error) {
	eh.row = row

	eh.evalCtx.PushIVarContainer(eh)
	d, err := eh.expr.Eval(eh.evalCtx)
	eh.evalCtx.PopIVarContainer()
	return d, err
}
