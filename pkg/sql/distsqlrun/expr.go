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
//
// Author: Radu Berinde (radu@cockroachlabs.com)
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsqlrun

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/pkg/errors"
)

// ivarBinder is a parser.Visitor that binds ordinal references
// (IndexedVars represented by @1, @2, ...) to an IndexedVarContainer.
type ivarBinder struct {
	h   *parser.IndexedVarHelper
	err error
}

func (v *ivarBinder) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if v.err != nil {
		return false, expr
	}
	if ivar, ok := expr.(*parser.IndexedVar); ok {
		if err := v.h.BindIfUnbound(ivar); err != nil {
			v.err = err
		}
		return false, expr
	}
	return true, expr
}

func (*ivarBinder) VisitPost(expr parser.Expr) parser.Expr { return expr }

// processExpression parses the string expression inside an Expression,
// and associates ordinal references (@1, @2, etc) with the given helper.
func processExpression(exprSpec Expression, h *parser.IndexedVarHelper) (parser.TypedExpr, error) {
	if exprSpec.Expr == "" {
		return nil, nil
	}
	expr, err := parser.ParseExpr(exprSpec.Expr)
	if err != nil {
		return nil, err
	}

	// Bind IndexedVars to our eh.vars.
	v := ivarBinder{h: h, err: nil}
	parser.WalkExprConst(&v, expr)
	if v.err != nil {
		return nil, v.err
	}

	// Convert to a fully typed expression.
	typedExpr, err := parser.TypeCheck(expr, nil, parser.TypeAny)
	if err != nil {
		return nil, err
	}

	return typedExpr, nil
}

// exprHelper implements the common logic around evaluating an expression that
// depends on a set of values.
type exprHelper struct {
	noCopy util.NoCopy

	expr parser.TypedExpr
	// vars is used to generate IndexedVars that are "backed" by the values in
	// `row`.
	vars parser.IndexedVarHelper

	evalCtx *parser.EvalContext

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

// exprHelper implements parser.IndexedVarContainer.
var _ parser.IndexedVarContainer = &exprHelper{}

// IndexedVarResolvedType is part of the parser.IndexedVarContainer interface.
func (eh *exprHelper) IndexedVarResolvedType(idx int) parser.Type {
	return eh.types[idx].ToDatumType()
}

// IndexedVarEval is part of the parser.IndexedVarContainer interface.
func (eh *exprHelper) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	err := eh.row[idx].EnsureDecoded(&eh.datumAlloc)
	if err != nil {
		return nil, err
	}
	return eh.row[idx].Datum.Eval(ctx)
}

// IndexedVarString is part of the parser.IndexedVarContainer interface.
func (eh *exprHelper) IndexedVarFormat(buf *bytes.Buffer, _ parser.FmtFlags, idx int) {
	fmt.Fprintf(buf, "$%d", idx)
}

func (eh *exprHelper) init(
	expr Expression, types []sqlbase.ColumnType, evalCtx *parser.EvalContext,
) error {
	if expr.Expr == "" {
		return nil
	}
	eh.types = types
	eh.evalCtx = evalCtx
	eh.vars = parser.MakeIndexedVarHelper(eh, len(types))
	var err error
	eh.expr, err = processExpression(expr, &eh.vars)
	if err != nil {
		return err
	}
	var p parser.Parser
	if p.AggregateInExpr(eh.expr, evalCtx.SearchPath) {
		return errors.Errorf("expression '%s' has aggregate", eh.expr)
	}
	return nil
}

// evalFilter is used for filter expressions; it evaluates the expression and
// returns whether the filter passes.
func (eh *exprHelper) evalFilter(row sqlbase.EncDatumRow) (bool, error) {
	eh.row = row
	return sqlbase.RunFilter(eh.expr, eh.evalCtx)
}

// Given a row, eval evaluates the wrapped expression and returns the
// resulting datum. For example, given a row (1, 2, 3, 4, 5):
//  '@2' would return '2'
//  '@2 + @5' would return '7'
//  '@1' would return '1'
//  '@2 + 10' would return '12'
func (eh *exprHelper) eval(row sqlbase.EncDatumRow) (parser.Datum, error) {
	eh.row = row

	return eh.expr.Eval(eh.evalCtx)
}
