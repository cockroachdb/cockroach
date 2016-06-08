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

func (v *varConvertVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if v.justCheck && v.checkFailed {
		return false, expr
	}

	if varExpr, ok := expr.(parser.VariableExpr); ok {
		// Ignore sub-queries
		if _, isSubquery := expr.(*subquery); isSubquery {
			return false, expr
		}

		ok, converted := v.conv(varExpr)
		if !ok {
			// variable not in the "restricted" set of variables.
			if !v.justCheck {
				panic(fmt.Sprintf("exprConvertVars called with unchecked variable %s", varExpr))
			}
			v.checkFailed = true
			return false, expr
		}

		if v.justCheck {
			return false, expr
		}
		return false, converted
	}

	return true, expr
}

func (v *varConvertVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

// Checks if the given expression only has vars that are known to the conversion function.
func exprCheckVars(expr parser.Expr, conv varConvertFunc) bool {
	v := varConvertVisitor{justCheck: true, conv: conv}
	parser.WalkExprConst(&v, expr)
	return !v.checkFailed
}

// Convert the variables in the given expression; the expression must only contain
// variables known to the conversion function (exprCheckVars should be used first).
func exprConvertVars(expr parser.TypedExpr, conv varConvertFunc) parser.TypedExpr {
	v := varConvertVisitor{justCheck: false, conv: conv}
	ret, _ := parser.WalkExpr(&v, expr)
	return ret.(parser.TypedExpr)
}

func makeAnd(left parser.TypedExpr, right parser.TypedExpr) parser.TypedExpr {
	if left == parser.DBoolFalse || right == parser.DBoolFalse {
		return parser.DBoolFalse
	}
	if left == parser.DBoolTrue {
		return right
	}
	if right == parser.DBoolTrue {
		return left
	}
	return parser.NewTypedAndExpr(left, right)
}

func makeOr(left parser.TypedExpr, right parser.TypedExpr) parser.TypedExpr {
	if left == parser.DBoolTrue || right == parser.DBoolTrue {
		return parser.DBoolTrue
	}
	if left == parser.DBoolFalse {
		return right
	}
	if right == parser.DBoolFalse {
		return left
	}
	return parser.NewTypedOrExpr(left, right)
}

func makeNot(expr parser.TypedExpr) parser.TypedExpr {
	if expr == parser.DBoolTrue {
		return parser.DBoolFalse
	}
	if expr == parser.DBoolFalse {
		return parser.DBoolTrue
	}
	return parser.NewTypedNotExpr(expr)
}

// splitBoolExpr splits a boolean expression E into two boolean expressions RES and REM such that:
//
//  - RES only has variables known to the conversion function (it is "restricted" to a particular
//    set of variables)
//
//  - If weaker is true, for any setting of variables x:
//       E(x) = (RES(x) AND REM(x))
//    This implies RES(x) <= E(x), i.e. RES is "weaker"
//
//  - If weaker is false:
//       E(x) = (RES(x) OR REM(x))
//    This implies RES(x) => E(x), i.e. RES is "stronger"
func splitBoolExpr(expr parser.TypedExpr, conv varConvertFunc, weaker bool) (restricted, remainder parser.TypedExpr) {
	// If the expression only contains "restricted" vars, the split is trivial.
	if exprCheckVars(expr, conv) {
		// An "empty" filter is always true in the weaker (normal) case (where the filter is
		// equivalent to RES AND REM) and always false in the stronger (inverted) case (where the
		// filter is equivalent to RES OR REM).
		return exprConvertVars(expr, conv), parser.MakeDBool(parser.DBool(weaker))
	}

	switch t := expr.(type) {
	case *parser.AndExpr:
		if weaker {
			// In the weaker (normal) case, we have
			//   E = (leftRes AND leftRem) AND (rightRes AND rightRem)
			// We can just rearrange:
			//   E = (leftRes AND rightRes) AND (leftRem AND rightRem)
			leftRes, leftRem := splitBoolExpr(t.TypedLeft(), conv, weaker)
			rightRes, rightRem := splitBoolExpr(t.TypedRight(), conv, weaker)
			return makeAnd(leftRes, rightRes), makeAnd(leftRem, rightRem)
		}

		// In the stronger (inverted) case, we have
		//   E = (leftRes OR leftRem) AND (rightRes OR rightRem)
		// We can't do more than:
		//   E = (leftRes AND rightRes) OR E
		leftRes, _ := splitBoolExpr(t.TypedLeft(), conv, weaker)
		rightRes, _ := splitBoolExpr(t.TypedRight(), conv, weaker)
		return makeAnd(leftRes, rightRes), expr

	case *parser.OrExpr:
		if !weaker {
			// In the stronger (inverted) case, we have
			//   E = (leftRes OR leftRem) OR (rightRes AND rightRem)
			// We can just rearrange:
			//   E = (leftRes OR rightRes) OR (leftRem AND rightRem)
			leftRes, leftRem := splitBoolExpr(t.TypedLeft(), conv, weaker)
			rightRes, rightRem := splitBoolExpr(t.TypedRight(), conv, weaker)
			return makeOr(leftRes, rightRes), makeOr(leftRem, rightRem)
		}

		// In the weaker (normal) case, we have
		//   E = (leftRes AND leftRem) OR (rightRes AND rightRem)
		// We can't do more than:
		//   E = (leftRes OR rightRes) OR E
		leftRes, _ := splitBoolExpr(t.TypedLeft(), conv, weaker)
		rightRes, _ := splitBoolExpr(t.TypedRight(), conv, weaker)
		return makeOr(leftRes, rightRes), expr

	case *parser.ParenExpr:
		return splitBoolExpr(t.TypedInnerExpr(), conv, weaker)

	case *parser.NotExpr:
		exprRes, exprRem := splitBoolExpr(t.TypedInnerExpr(), conv, !weaker)
		return makeNot(exprRes), makeNot(exprRem)

	default:
		// We can't split off anything (we already handled the case when expr contains only
		// restricted vars above).
		// For why we return DBool(weaker), see the comment above on "empty" filters.
		return parser.MakeDBool(parser.DBool(weaker)), expr
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
func splitFilter(
	expr parser.TypedExpr, conv varConvertFunc,
) (restricted, remainder parser.TypedExpr) {
	if expr == nil {
		return nil, nil
	}
	restricted, remainder = splitBoolExpr(expr, conv, true)
	if restricted == parser.DBoolTrue {
		restricted = nil
	}
	if remainder == parser.DBoolTrue {
		remainder = nil
	}
	return restricted, remainder
}
