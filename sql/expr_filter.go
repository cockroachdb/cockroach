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
//
// This file implements routines for manipulating filtering expressions.

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
)

// varConvertFunc is a callback that is used when splitting filtering expressions (see splitFilter).
//
// The purpose of splitting filters is to derive a "restricted" filter expression that can be used at
// an earlier stage where not all variables are available. For example, we may have an expression
// that refers to variables in two tables and we want to derive an expression that we can use with a
// single table (to reject some rows early); or, we could have an expression that refers to a table
// but we want to derive an expression that we can use only with the columns available in an index.
//
// varConvertFunc serves two purposes:
//
//  - identifies which variables can be in the "restricted" filtering expression we are trying to
//    derive, via the "ok" return value.
//
//  - optionally, it converts variables in the derived expression to a new type. If no conversion is
//    necessary, the same variable can be returned.
//
// For example, to split a filter and get an expression that only refers to qvalues for column 0, we
// could use:
//
//    func(expr parser.VariableExpr) (bool, parser.VariableExpr) {
//       q := expr.(*qvalue)
//       if q.colRef.colIdx == 0 {
//          return true, q
//       } else {
//          return false, nil
//       }
//    }
type varConvertFunc func(expr parser.VariableExpr) (ok bool, newExpr parser.VariableExpr)

type varConvertVisitor struct {
	// If justCheck is true, the visitor only checks that all VariableExpr in the expression can be
	// in the restricted filter (i.e. conv returns ok = true).
	// If justCheck is false, all VariableExpr are converted. This mode can only be run if all
	// variables can be in the restricted filter.
	justCheck   bool
	checkFailed bool
	conv        varConvertFunc
}

var _ parser.Visitor = &varConvertVisitor{}

// Visit is invoked for each Expr node. It can return a new expression for the
// node, or it can stop processing by returning a nil Visitor.
func (v *varConvertVisitor) Visit(expr parser.Expr, pre bool) (parser.Visitor, parser.Expr) {
	if !pre || (v.justCheck && v.checkFailed) {
		return nil, expr
	}

	if varExpr, ok := expr.(parser.VariableExpr); ok {
		ok, converted := v.conv(varExpr)
		if !ok {
			// variable not in the "restricted" set of variables.
			if !v.justCheck {
				panic(fmt.Sprintf("exprConvertVars called with unchecked variable %s", varExpr))
			}
			v.checkFailed = true
			return nil, expr
		}
		if !v.justCheck {
			return v, converted
		}
	}

	return v, expr
}

// Checks if the given expression only has vars that are known to the conversion function.
func exprCheckVars(expr parser.Expr, conv varConvertFunc) bool {
	v := varConvertVisitor{justCheck: true, conv: conv}
	parser.WalkExpr(&v, expr)
	return !v.checkFailed
}

// Convert the variables in the given expression; the expression must only contain
// variables known to the conversion function (exprCheckVars should be used first).
func exprConvertVars(expr parser.Expr, conv varConvertFunc) parser.Expr {
	v := varConvertVisitor{justCheck: false, conv: conv}
	return parser.WalkExpr(&v, expr)
}

func makeAnd(left parser.Expr, right parser.Expr) parser.Expr {
	if left == parser.DBool(false) || right == parser.DBool(false) {
		return parser.DBool(false)
	}
	if left == parser.DBool(true) {
		return right
	}
	if right == parser.DBool(true) {
		return left
	}
	return &parser.AndExpr{Left: left, Right: right}
}

func makeOr(left parser.Expr, right parser.Expr) parser.Expr {
	if left == parser.DBool(true) || right == parser.DBool(true) {
		return parser.DBool(true)
	}
	if left == parser.DBool(false) {
		return right
	}
	if right == parser.DBool(false) {
		return left
	}
	return &parser.OrExpr{Left: left, Right: right}
}

func makeNot(expr parser.Expr) parser.Expr {
	if expr == parser.DBool(true) {
		return parser.DBool(false)
	}
	if expr == parser.DBool(false) {
		return parser.DBool(true)
	}
	return &parser.NotExpr{Expr: expr}
}

// splitBoolExpr splits a boolean expression E into two boolean expressions RES and REM such that:
//
//  - RES only has variables known to the conversion function (it is the "restricted" to a
//    particular set of variables)
//
//  - If weaker is true, for any setting of variables x:
//       E(x) = (RES(x) AND REM(x))
//    This implies RES(x) <= E(x), i.e. RES is "weaker"
//
//  - If weaker is false:
//       E(x) = (RES(x) OR REM(x))
//    This implies RES(x) => E(x), i.e. RES is "stronger"
//
// Note: the original expression is modified in-place and should not be used again.
func splitBoolExpr(expr parser.Expr, conv varConvertFunc, weaker bool) (restricted, remainder parser.Expr) {
	// If the expression only contains "convertible" vars, the split is trivial.
	if exprCheckVars(expr, conv) {
		// An "empty" filter is always true in the weaker (normal) case (where the filter is
		// equivalent to RES AND REM) and always false in the stronger (inverted) case (where the
		// filter is equivalent to RES OR REM).
		return exprConvertVars(expr, conv), parser.DBool(weaker)
	}

	switch t := expr.(type) {
	case *parser.AndExpr:
		if weaker {
			// In the weaker (normal) case, we have
			//   E = (leftRes AND leftRem) AND (rightRes AND rightRem)
			// We can just rearrange:
			//   E = (leftRes AND rightRes) AND (leftRem AND rightRem)
			leftRes, leftRem := splitBoolExpr(t.Left, conv, weaker)
			rightRes, rightRem := splitBoolExpr(t.Right, conv, weaker)
			return makeAnd(leftRes, rightRes), makeAnd(leftRem, rightRem)
		}

		// In the stronger (inverted) case, we have
		//   E = (leftRes OR leftRem) AND (rightRes OR rightRem)
		// We can't do more than:
		//   E = (leftRes AND rightRes) OR E
		exprCopy := expr.DeepCopy()
		leftRes, _ := splitBoolExpr(t.Left, conv, weaker)
		rightRes, _ := splitBoolExpr(t.Right, conv, weaker)
		return makeAnd(leftRes, rightRes), exprCopy

	case *parser.OrExpr:
		if !weaker {
			// In the stronger (inverted) case, we have
			//   E = (leftRes OR leftRem) OR (rightRes AND rightRem)
			// We can just rearrange:
			//   E = (leftRes OR rightRes) OR (leftRem AND rightRem)
			leftRes, leftRem := splitBoolExpr(t.Left, conv, weaker)
			rightRes, rightRem := splitBoolExpr(t.Right, conv, weaker)
			return makeOr(leftRes, rightRes), makeOr(leftRem, rightRem)
		}

		// In the weaker (normal) case, we have
		//   E = (leftRes AND leftRem) OR (rightRes AND rightRem)
		// We can't do more than:
		//   E = (leftRes OR rightRes) OR E
		exprCopy := expr.DeepCopy()
		leftRes, _ := splitBoolExpr(t.Left, conv, weaker)
		rightRes, _ := splitBoolExpr(t.Right, conv, weaker)
		return makeOr(leftRes, rightRes), exprCopy

	case *parser.ParenExpr:
		return splitBoolExpr(t.Expr, conv, weaker)

	case *parser.NotExpr:
		exprRes, exprRem := splitBoolExpr(t.Expr, conv, !weaker)
		return makeNot(exprRes), makeNot(exprRem)

	default:
		// We can't split off anything. See the comment above on "empty" filters.
		return parser.DBool(weaker), expr
	}
}

// splitFilter splits a boolean expression E into two boolean expressions RES and REM such that:
//
//  - RES contains only variables known to the conversion function (it is "restricted" to a
//    particular set of variables). These variables are also converted as returned by conv.
//
//  - the original expression is equivalent to the conjunction (AND) between the RES and REM
//    expressions.
//
// Splitting allows us to do filtering at various layers, where one layer only knows the values of
// some variables. Instead of evaluating E in an upper layer, we evaluate RES in a lower layer
// and then evaluate REM in the upper layer (on results that passed the RES filter).
//
// Notes:
//  - the implementation is best-effort (it tries to get as much of the expression into RES as
//    possible, and make REM as small as possible).
//  - the original expression is modified in-place and should not be used again.
func splitFilter(expr parser.Expr, conv varConvertFunc) (restricted, remainder parser.Expr) {
	if expr == nil {
		return nil, nil
	}
	restricted, remainder = splitBoolExpr(expr, conv, true)
	if restricted == parser.DBool(true) {
		restricted = nil
	}
	if remainder == parser.DBool(true) {
		remainder = nil
	}
	return restricted, remainder
}
