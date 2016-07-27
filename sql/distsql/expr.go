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

package distsql

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/pkg/errors"
)

// valArgsConvert is a parser.Visitor that converts Placeholders ($0, $1, etc.) to
// IndexedVars.
type valArgsConvert struct {
	h   *parser.IndexedVarHelper
	err error
}

func (v *valArgsConvert) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if val, ok := expr.(parser.Placeholder); ok {
		idx, err := strconv.Atoi(val.Name)
		if err != nil || idx < 0 || idx >= v.h.NumVars() {
			v.err = errors.Errorf("invalid variable index %s", val.Name)
			return false, expr
		}

		return false, v.h.IndexedVar(idx)
	}
	return true, expr
}

func (*valArgsConvert) VisitPost(expr parser.Expr) parser.Expr { return expr }

// processExpression parses the string expression inside an Expression,
// interpreting $0, $1, etc as indexed variables.
func processExpression(exprSpec Expression, h *parser.IndexedVarHelper) (parser.TypedExpr, error) {
	if exprSpec.Expr == "" {
		return nil, nil
	}
	expr, err := parser.ParseExprTraditional(exprSpec.Expr)
	if err != nil {
		return nil, err
	}

	// Convert Placeholders to IndexedVars
	v := valArgsConvert{h: h, err: nil}
	expr, _ = parser.WalkExpr(&v, expr)
	if v.err != nil {
		return nil, v.err
	}

	// Convert to a fully typed expression.
	typedExpr, err := parser.TypeCheck(expr, nil, parser.NoTypePreference)
	if err != nil {
		return nil, err
	}

	return typedExpr, nil
}

// exprHelper implements the common logic around evaluating an expression that
// depends on a set of values.
type exprHelper struct {
	expr parser.TypedExpr
	// vars is used to generate IndexedVars that are "backed" by
	// the values in `row`.
	vars parser.IndexedVarHelper

	evalCtx *parser.EvalContext

	types      []sqlbase.ColumnType_Kind
	row        sqlbase.EncDatumRow
	datumAlloc sqlbase.DatumAlloc
}

func (eh exprHelper) String() string {
	if eh.expr == nil {
		return "none"
	}
	return eh.expr.String()
}

// exprHelper implements parser.IndexedVarContainer.
var _ parser.IndexedVarContainer = &exprHelper{}

// IndexedVarReturnType is part of the parser.IndexedVarContainer interface.
func (eh *exprHelper) IndexedVarReturnType(idx int) parser.Datum {
	return eh.types[idx].ToDatumType()
}

// IndexedVarEval is part of the parser.IndexedVarContainer interface.
func (eh *exprHelper) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	err := eh.row[idx].Decode(&eh.datumAlloc)
	if err != nil {
		return nil, err
	}
	return eh.row[idx].Datum.Eval(ctx)
}

// IndexedVarString is part of the parser.IndexedVarContainer interface.
func (eh *exprHelper) IndexedVarString(idx int) string {
	return fmt.Sprintf("$%d", idx)
}

func (eh *exprHelper) init(
	expr Expression, types []sqlbase.ColumnType_Kind, evalCtx *parser.EvalContext,
) error {
	if expr.Expr == "" {
		return nil
	}
	eh.types = types
	eh.evalCtx = evalCtx
	eh.vars = parser.MakeIndexedVarHelper(eh, len(types))
	var err error
	eh.expr, err = processExpression(expr, &eh.vars)
	return err
}

// evalFilter is used for filter expressions; it evaluates the expression and
// returns whether the filter passes.
func (eh *exprHelper) evalFilter(row sqlbase.EncDatumRow) (bool, error) {
	eh.row = row
	return sqlbase.RunFilter(eh.expr, eh.evalCtx)
}
