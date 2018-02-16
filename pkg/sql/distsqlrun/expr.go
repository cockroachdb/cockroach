// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/pkg/errors"
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
func processExpression(exprSpec Expression, h *tree.IndexedVarHelper) (tree.TypedExpr, error) {
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

	// Convert to a fully typed expression.
	typedExpr, err := tree.TypeCheck(expr, &tree.SemaContext{IVarContainer: h.Container()}, types.Any)
	if err != nil {
		return nil, errors.Wrap(err, expr.String())
	}

	return typedExpr, nil
}

// exprHelper implements the common logic around evaluating an expression that
// depends on a set of values.
type exprHelper struct {
	noCopy util.NoCopy

	expr tree.TypedExpr
	// vars is used to generate IndexedVars that are "backed" by the values in
	// `row`.
	vars tree.IndexedVarHelper

	evalCtx *tree.EvalContext

	types      []sqlbase.ColumnType
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
func (eh *exprHelper) IndexedVarResolvedType(idx int) types.T {
	return eh.types[idx].ToDatumType()
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
	expr Expression, types []sqlbase.ColumnType, evalCtx *tree.EvalContext,
) error {
	eh.evalCtx = evalCtx
	if expr.Expr == "" {
		return nil
	}
	eh.types = types
	eh.vars = tree.MakeIndexedVarHelper(eh, len(types))
	var err error
	eh.expr, err = processExpression(expr, &eh.vars)
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
