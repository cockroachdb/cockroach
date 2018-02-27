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
// This file implements routines for manipulating filtering expressions.

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
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
// For example, to split a filter and get an expression that only refers to IndexedVars for column 0, we
// could use:
//
//    func(expr tree.VariableExpr) (bool, tree.VariableExpr) {
//       q := expr.(*IndexedVar)
//       if q.colRef.colIdx == 0 {
//          return true, q
//       } else {
//          return false, nil
//       }
//    }
type varConvertFunc func(expr tree.VariableExpr) (ok bool, newExpr tree.Expr)

type varConvertVisitor struct {
	// If justCheck is true, the visitor only checks that all VariableExpr in the expression can be
	// in the restricted filter (i.e. conv returns ok = true).
	// If justCheck is false, all VariableExpr are converted. This mode can only be run if all
	// variables can be in the restricted filter.
	justCheck   bool
	checkFailed bool
	conv        varConvertFunc
}

var _ tree.Visitor = &varConvertVisitor{}

func (v *varConvertVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.justCheck && v.checkFailed {
		return false, expr
	}

	if varExpr, ok := expr.(tree.VariableExpr); ok {
		// Ignore sub-queries and placeholders
		switch expr.(type) {
		case *tree.Subquery, *tree.Placeholder:
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

func (v *varConvertVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// Checks if the given expression only has vars that are known to the conversion function.
func exprCheckVars(expr tree.Expr, conv varConvertFunc) bool {
	v := varConvertVisitor{justCheck: true, conv: conv}
	tree.WalkExprConst(&v, expr)
	return !v.checkFailed
}

// Convert the variables in the given expression; the expression must only contain
// variables known to the conversion function (exprCheckVars should be used first).
func exprConvertVars(expr tree.TypedExpr, conv varConvertFunc) tree.TypedExpr {
	if expr == nil {
		return expr
	}
	v := varConvertVisitor{justCheck: false, conv: conv}
	ret, _ := tree.WalkExpr(&v, expr)
	return ret.(tree.TypedExpr)
}

func makeAnd(left tree.TypedExpr, right tree.TypedExpr) tree.TypedExpr {
	if left == tree.DBoolFalse || right == tree.DBoolFalse {
		return tree.DBoolFalse
	}
	if left == tree.DBoolTrue {
		return right
	}
	if right == tree.DBoolTrue {
		return left
	}
	return tree.NewTypedAndExpr(left, right)
}

func makeOr(left tree.TypedExpr, right tree.TypedExpr) tree.TypedExpr {
	if left == tree.DBoolTrue || right == tree.DBoolTrue {
		return tree.DBoolTrue
	}
	if left == tree.DBoolFalse {
		return right
	}
	if right == tree.DBoolFalse {
		return left
	}
	return tree.NewTypedOrExpr(left, right)
}

func makeNot(expr tree.TypedExpr) tree.TypedExpr {
	if expr == tree.DBoolTrue {
		return tree.DBoolFalse
	}
	if expr == tree.DBoolFalse {
		return tree.DBoolTrue
	}
	return tree.NewTypedNotExpr(expr)
}

// If the given expression is a tuple inequality, extract the longest prefix of
// a tuple comparison that contains only variables known to the conversion
// function.
//
// If weaker is true (the "normal" case), we want a weaker inequality (i.e. an
// inequality that is implied by `t`). For example:
//   (a, b, x) > (1, 2, 3)
// and only a, b are convertible. The result is:
//   (a, b) >= (1, 2)
//
// If weaker is false, we want a STRONGER condition (i.e. one that implies `t`;
// this is used when this condition is under a NOT node); in this case
//   (a, b, x) >= (1, 2, 3)
// becomes
//   (a, b) > (1, 2)
// because (a, b) > (1, 2) implies (a, b, x) >= (1, 2, 3) for any x.
//
// See splitBoolExpr for more information on weaker.
//
// Note that equality is not handled; instead of special handling here, we
// should normalize tuple equalities to conjunctions (#20473).
//
// The function returns the new, converted ComparisonExpr, or nil if
// this is not such an expression or we don't have a useful prefix.
func tryTruncateTupleComparison(
	t *tree.ComparisonExpr, conv varConvertFunc, weaker bool,
) tree.TypedExpr {
	switch t.Operator {
	case tree.EQ, tree.NE:
		return tryTruncateTupleEqOrNe(t, conv, weaker)
	case tree.LT, tree.GT, tree.LE, tree.GE:
	default:
		return nil
	}
	left, leftOk := t.Left.(*tree.Tuple)
	right, rightOk := t.Right.(*tree.DTuple)
	// TODO(radu): we should also support Tuple on both sides.
	if !leftOk || !rightOk {
		return nil
	}
	if len(left.Exprs) != len(right.D) {
		panic(fmt.Sprintf("invalid tuple comparison %s", t))
	}
	// Find the longest prefix for which both sides are convertible.
	prefix := 0
	for ; prefix < len(left.Exprs); prefix++ {
		if !exprCheckVars(left.Exprs[prefix], conv) {
			break
		}
	}

	if prefix == 0 {
		// Not even the first tuple members are useful.
		return nil
	}
	newOp := *t

	if prefix == 1 {
		// Simplify to a non-tuple comparison.
		newOp.Left = left.Exprs[0]
		newOp.Right = right.D[0]
	} else {
		// Preserve a prefix of the tuples.
		newOp.Left = left.Truncate(prefix)
		t := &tree.DTuple{
			D: right.D[:prefix],
		}
		if right.Sorted() {
			t.SetSorted()
		}
		newOp.Right = t
	}
	// Adjust the operator as necessary.
	if weaker {
		switch newOp.Operator {
		case tree.LT:
			// (a, b, c) < (1, 2, 3) becomes (a, b) <= (1, 2).
			newOp.Operator = tree.LE
		case tree.GT:
			// (a, b, c) > (1, 2, 3) becomes (a, b) >= (1, 2).
			newOp.Operator = tree.GE
		}
	} else {
		// We are under a NOT and have to return a stronger condition.
		switch newOp.Operator {
		case tree.LE:
			// (a, b, c) <= (1, 2, 3) becomes (a, b) < (1, 2).
			newOp.Operator = tree.LT
		case tree.GE:
			// (a, b, c) > (1, 2, 3) becomes (a, b) >= (1, 2).
			newOp.Operator = tree.GT
		}
	}
	return exprConvertVars(&newOp, conv)
}

// If the given expression is a tuple EQ or NE, extract a comparison that
// contains only variables known to the conversion function. See
// tryTruncateTupleComparison for information on "weaker".
func tryTruncateTupleEqOrNe(
	t *tree.ComparisonExpr, conv varConvertFunc, weaker bool,
) tree.TypedExpr {
	// In the "normal" case where we want a weaker expression:
	//  - we can convert (a, b, x) = (1, 2, 3) to (a, b) = (1, 2).
	//  - we can't convert (a, b, x) != (1, 2, 3).
	//
	// In the "inverted" case where we want a stronger expression:
	//  - we can't convert (a, b, x) = (1, 2, 3).
	//  - we can convert (a, b, x) != (1, 2, 3) to (a, b) != (1, 2).
	if (t.Operator == tree.EQ) != weaker {
		return nil
	}
	left, leftOk := t.Left.(*tree.Tuple)
	right, rightOk := t.Right.(*tree.DTuple)
	// TODO(radu): we should also support Tuple on both sides.
	if !leftOk || !rightOk {
		return nil
	}
	if len(left.Exprs) != len(right.D) {
		panic(fmt.Sprintf("invalid tuple comparison %s", t))
	}
	var convertible util.FastIntSet
	for i, e := range left.Exprs {
		if exprCheckVars(e, conv) {
			convertible.Add(i)
		}
	}
	if convertible.Empty() {
		return nil
	}

	newOp := *t
	if convertible.Len() == 1 {
		// Simplify to a non-tuple comparison.
		idx, _ := convertible.Next(0)
		newOp.Left = left.Exprs[idx]
		newOp.Right = right.D[idx]
	} else {
		newOp.Left = left.Project(convertible)
		dTuple := &tree.DTuple{
			D: make(tree.Datums, 0, convertible.Len()),
		}
		if right.Sorted() {
			dTuple.SetSorted()
		}
		for i, ok := convertible.Next(0); ok; i, ok = convertible.Next(i + 1) {
			dTuple.D = append(dTuple.D, right.D[i])
		}
		newOp.Right = dTuple
	}
	return exprConvertVars(&newOp, conv)
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
func splitBoolExpr(
	expr tree.TypedExpr, conv varConvertFunc, weaker bool,
) (restricted, remainder tree.TypedExpr) {
	// If the expression only contains "restricted" vars, the split is trivial.
	if exprCheckVars(expr, conv) {
		// An "empty" filter is always true in the weaker (normal) case (where the filter is
		// equivalent to RES AND REM) and always false in the stronger (inverted) case (where the
		// filter is equivalent to RES OR REM).
		return exprConvertVars(expr, conv), tree.MakeDBool(tree.DBool(weaker))
	}

	switch t := expr.(type) {
	case *tree.AndExpr:
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

	case *tree.OrExpr:
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

	case *tree.ParenExpr:
		return splitBoolExpr(t.TypedInnerExpr(), conv, weaker)

	case *tree.NotExpr:
		exprRes, exprRem := splitBoolExpr(t.TypedInnerExpr(), conv, !weaker)
		return makeNot(exprRes), makeNot(exprRem)

	case *tree.ComparisonExpr:
		if truncated := tryTruncateTupleComparison(t, conv, weaker); truncated != nil {
			return truncated, expr
		}

	default:
	}

	// We can't split off anything (we already handled the case when expr contains only
	// restricted vars above).
	// For why we return DBool(weaker), see the comment above on "empty" filters.
	return tree.MakeDBool(tree.DBool(weaker)), expr
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
func splitFilter(expr tree.TypedExpr, conv varConvertFunc) (restricted, remainder tree.TypedExpr) {
	if expr == nil {
		return nil, nil
	}
	restricted, remainder = splitBoolExpr(expr, conv, true)
	if restricted == tree.DBoolTrue {
		restricted = nil
	}
	if remainder == tree.DBoolTrue {
		remainder = nil
	}
	return restricted, remainder
}

// extractNotNullConstraintsFromNotNullTuple deduces which IndexedVars must be
// not NULL given an expr that must be not NULL and, if it is a tuple, all its
// members must be not NULL.
func extractNotNullConstraintsFromNotNullTuple(expr tree.TypedExpr) util.FastIntSet {
	result := extractNotNullConstraintsFromNotNullExpr(expr)
	if t, ok := expr.(*tree.Tuple); ok {
		for _, e := range t.Exprs {
			result.UnionWith(extractNotNullConstraintsFromNotNullExpr(e.(tree.TypedExpr)))
		}
	}
	return result
}

// extractNotNullConstraintsFromNotNullExpr deduces which IndexedVars must be
// not NULL for expr to be not NULL. It is best-effort so there may be false
// negatives (but no false positives).
func extractNotNullConstraintsFromNotNullExpr(expr tree.TypedExpr) util.FastIntSet {
	switch t := expr.(type) {
	case *tree.IndexedVar:
		var result util.FastIntSet
		result.Add(t.Idx)
		return result

	case *tree.BinaryExpr:
		// For all binary operations, both operands must be not NULL.
		left := extractNotNullConstraintsFromNotNullExpr(t.TypedLeft())
		right := extractNotNullConstraintsFromNotNullExpr(t.TypedRight())
		left.UnionWith(right)
		return left

	case *tree.ComparisonExpr:
		if (t.Operator == tree.IsNotDistinctFrom || t.Operator == tree.IsDistinctFrom) && t.Right == tree.DNull {
			return util.FastIntSet{}
		}
		if t.Operator == tree.In || t.Operator == tree.NotIn {
			return extractNotNullConstraintsFromNotNullTuple(t.TypedLeft())
		}
		// For all other comparison operations, both operands must be not NULL.
		result := extractNotNullConstraintsFromNotNullExpr(t.TypedLeft())
		result.UnionWith(extractNotNullConstraintsFromNotNullExpr(t.TypedRight()))
		// For tuple equality, all tuple members must be not NULL.
		if t.Operator == tree.EQ {
			result.UnionWith(extractNotNullConstraintsFromNotNullTuple(t.TypedLeft()))
			result.UnionWith(extractNotNullConstraintsFromNotNullTuple(t.TypedRight()))
		}
		return result

	case *tree.ParenExpr:
		return extractNotNullConstraintsFromNotNullExpr(t.TypedInnerExpr())

	// TODO(radu): handle other expressions.

	default:
		return util.FastIntSet{}
	}
}

// extractNotNullConstraints deduces which IndexedVars must be not-NULL for a
// filter condition to pass. For example a filter "x > 5" implies that x is not
// NULL; a filter "x > y" implies that both x and y are not NULL. It is
// best-effort so there may be false negatives (but no false positives).
func extractNotNullConstraints(filter tree.TypedExpr) util.FastIntSet {
	if typ := filter.ResolvedType(); typ == types.Unknown {
		return util.FastIntSet{}
	} else if typ != types.Bool {
		panic(fmt.Sprintf("non-bool filter expression: %s (type: %s)", filter, filter.ResolvedType()))
	}
	switch t := filter.(type) {
	case *tree.AndExpr:
		left := extractNotNullConstraints(t.TypedLeft())
		right := extractNotNullConstraints(t.TypedRight())
		left.UnionWith(right)
		return left

	case *tree.OrExpr:
		left := extractNotNullConstraints(t.TypedLeft())
		right := extractNotNullConstraints(t.TypedRight())
		left.IntersectionWith(right)
		return left

	case *tree.ParenExpr:
		return extractNotNullConstraints(t.TypedInnerExpr())

	case *tree.NotExpr:
		// For the inner expression to be FALSE, it must not be NULL.
		return extractNotNullConstraintsFromNotNullExpr(t.TypedInnerExpr())

	case *tree.ComparisonExpr:
		result := extractNotNullConstraintsFromNotNullExpr(filter)
		if t.Operator == tree.IsDistinctFrom && t.Right == tree.DNull {
			result.UnionWith(extractNotNullConstraintsFromNotNullExpr(t.TypedLeft()))
		}
		return result

	default:
		// For any expression to be TRUE, it must not be NULL.
		return extractNotNullConstraintsFromNotNullExpr(filter)
	}
}
