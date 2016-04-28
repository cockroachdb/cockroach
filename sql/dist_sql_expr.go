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

package sql

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/sql/parser"
)

// valArgsConvert is a parser.Visitor that converts ValArgs ($0, $1, etc.) to
// IndexedVars.
type valArgsConvert struct {
	h   *parser.IndexedVarHelper
	err error
}

func (v *valArgsConvert) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if val, ok := expr.(parser.ValArg); ok {
		idx, err := strconv.Atoi(val.Name)
		if err != nil || idx < 0 || idx >= v.h.NumVars() {
			v.err = fmt.Errorf("invalid variable index %s", val.Name)
			return false, expr
		}

		return false, v.h.IndexedVar(idx)
	}
	return true, expr
}

func (*valArgsConvert) VisitPost(expr parser.Expr) parser.Expr { return expr }

// processSQLExpression parses the string expression inside a SQLExpression,
// interpreting $0, $1, etc as indexed variables.
func processSQLExpression(exprSpec SQLExpression, h *parser.IndexedVarHelper) (parser.Expr, error) {
	expr, err := parser.ParseExprTraditional(exprSpec.Expr)
	if err != nil {
		return nil, err
	}

	v := valArgsConvert{h: h, err: nil}
	expr, _ = parser.WalkExpr(&v, expr)
	if v.err != nil {
		return nil, v.err
	}
	return expr, nil
}
