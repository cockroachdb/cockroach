// Copyright 2018 The Cockroach Authors.
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

package memo

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// This file contains various helper functions that extract useful information
// from expressions.

// CanExtractConstTuple returns true if the expression is a TupleOp with
// constant values (a nested tuple of constant values is considered constant).
func CanExtractConstTuple(e opt.Expr) bool {
	return e.Op() == opt.TupleOp && CanExtractConstDatum(e)
}

// CanExtractConstDatum returns true if a constant datum can be created from the
// given expression (tuples and arrays of constant values are considered
// constant values). If CanExtractConstDatum returns true, then
// ExtractConstDatum is guaranteed to work as well.
func CanExtractConstDatum(e opt.Expr) bool {
	if opt.IsConstValueOp(e) {
		return true
	}

	if tup, ok := e.(*TupleExpr); ok {
		for _, elem := range tup.Elems {
			if !CanExtractConstDatum(elem) {
				return false
			}
		}
		return true
	}

	if arr, ok := e.(*ArrayExpr); ok {
		for _, elem := range arr.Elems {
			if !CanExtractConstDatum(elem) {
				return false
			}
		}
		return true
	}

	return false
}

// ExtractConstDatum returns the Datum that represents the value of an
// expression with a constant value. An expression with a constant value is:
//  - one that has a ConstValue tag, or
//  - a tuple or array where all children are constant values.
func ExtractConstDatum(e opt.Expr) tree.Datum {
	switch t := e.(type) {
	case *NullExpr:
		return tree.DNull

	case *TrueExpr:
		return tree.DBoolTrue

	case *FalseExpr:
		return tree.DBoolFalse

	case *ConstExpr:
		return t.Value

	case *TupleExpr:
		datums := make(tree.Datums, len(t.Elems))
		for i := range datums {
			datums[i] = ExtractConstDatum(t.Elems[i])
		}
		typ := t.Typ.(types.TTuple)
		return tree.NewDTuple(typ, datums...)

	case *ArrayExpr:
		elementType := t.Typ.(types.TArray).Typ
		a := tree.NewDArray(elementType)
		a.Array = make(tree.Datums, len(t.Elems))
		for i := range a.Array {
			a.Array[i] = ExtractConstDatum(t.Elems[i])
			if a.Array[i] == tree.DNull {
				a.HasNulls = true
			}
		}
		return a
	}
	panic(fmt.Sprintf("non-const expression: %+v", e))
}

// ExtractAggSingleInputColumn returns the input ColumnID of an aggregate
// operator that has a single input.
func ExtractAggSingleInputColumn(e opt.ScalarExpr) opt.ColumnID {
	if !opt.IsAggregateOp(e) {
		panic("not an Aggregate")
	}
	return ExtractVarFromAggInput(e.Child(0).(opt.ScalarExpr)).Col
}

// ExtractAggInputColumns returns the input columns of an aggregate (which can
// be empty).
func ExtractAggInputColumns(e opt.ScalarExpr) opt.ColSet {
	if !opt.IsAggregateOp(e) {
		panic("not an Aggregate")
	}
	var res opt.ColSet
	for i, n := 0, e.ChildCount(); i < n; i++ {
		res.Add(int(ExtractVarFromAggInput(e.Child(i).(opt.ScalarExpr)).Col))
	}
	return res
}

// ExtractVarFromAggInput is given an argument to an Aggregate and returns the
// inner Variable expression, stripping out modifiers like AggDistinct.
func ExtractVarFromAggInput(arg opt.ScalarExpr) *VariableExpr {
	if distinct, ok := arg.(*AggDistinctExpr); ok {
		arg = distinct.Input
	}
	if variable, ok := arg.(*VariableExpr); ok {
		return variable
	}
	panic("aggregate input not a Variable")
}

// ExtractJoinEqualityColumns returns pairs of columns (one from the left side,
// one from the right side) which are constrained to be equal in a join (and
// have equivalent types).
func ExtractJoinEqualityColumns(
	leftCols, rightCols opt.ColSet, on FiltersExpr,
) (leftEq opt.ColList, rightEq opt.ColList) {
	for i := range on {
		condition := on[i].Condition
		ok, left, right := isJoinEquality(leftCols, rightCols, condition)
		if !ok {
			continue
		}
		// Don't allow any column to show up twice.
		// TODO(radu): need to figure out the right thing to do in cases
		// like: left.a = right.a AND left.a = right.b
		duplicate := false
		for i := range leftEq {
			if leftEq[i] == left || rightEq[i] == right {
				duplicate = true
				break
			}
		}
		if !duplicate {
			leftEq = append(leftEq, left)
			rightEq = append(rightEq, right)
		}
	}
	return leftEq, rightEq
}

func isVarEquality(condition opt.ScalarExpr) (leftVar, rightVar *VariableExpr, ok bool) {
	if eq, ok := condition.(*EqExpr); ok {
		if leftVar, ok := eq.Left.(*VariableExpr); ok {
			if rightVar, ok := eq.Right.(*VariableExpr); ok {
				return leftVar, rightVar, true
			}
		}
	}
	return nil, nil, false
}

func isJoinEquality(
	leftCols, rightCols opt.ColSet, condition opt.ScalarExpr,
) (ok bool, left, right opt.ColumnID) {
	lvar, rvar, ok := isVarEquality(condition)
	if !ok {
		return false, 0, 0
	}

	// Don't allow mixed types (see #22519).
	if !lvar.DataType().Equivalent(rvar.DataType()) {
		return false, 0, 0
	}

	if leftCols.Contains(int(lvar.Col)) && rightCols.Contains(int(rvar.Col)) {
		return true, lvar.Col, rvar.Col
	}
	if leftCols.Contains(int(rvar.Col)) && rightCols.Contains(int(lvar.Col)) {
		return true, rvar.Col, lvar.Col
	}

	return false, 0, 0
}

// ExtractRemainingJoinFilters calculates the remaining ON condition after
// removing equalities that are handled separately. The given function
// determines if an equality is redundant. The result is empty if there are no
// remaining conditions.
func ExtractRemainingJoinFilters(on FiltersExpr, leftEq, rightEq opt.ColList) FiltersExpr {
	var newFilters FiltersExpr
	for i := range on {
		leftVar, rightVar, ok := isVarEquality(on[i].Condition)
		if ok {
			a, b := leftVar.Col, rightVar.Col
			found := false
			for j := range leftEq {
				if (a == leftEq[j] && b == rightEq[j]) ||
					(a == rightEq[j] && b == leftEq[j]) {
					found = true
					break
				}
			}
			if found {
				// Skip this condition.
				continue
			}
		}
		if newFilters == nil {
			newFilters = make(FiltersExpr, 0, len(on)-i)
		}
		newFilters = append(newFilters, on[i])
	}
	return newFilters
}

// ExtractFixedColumns returns columns in filter expression that have been
// constrained to fixed values.
func ExtractFixedColumns(
	on FiltersExpr, mem *Memo, evalCtx *tree.EvalContext,
) (fixedCols opt.ColSet) {
	for i := range on {
		if on[i].Op() != opt.FiltersItemOp {
			continue
		}
		scalar := on[i]
		scalarProps := scalar.ScalarProps(mem)
		if scalarProps.Constraints != nil && !scalarProps.Constraints.IsUnconstrained() {
			fixedCols.UnionWith(getConstrainedCols(scalarProps.Constraints, evalCtx))
		}
	}
	return fixedCols
}

// getConstrainedCols returns constrained prefixes in the set of constraints
// passed in.
func getConstrainedCols(
	constraints *constraint.Set, evalCtx *tree.EvalContext,
) (cols opt.ColSet) {
	for i := 0; i < constraints.Length(); i++ {
		constraint := constraints.Constraint(i)
		cols.UnionWith(constraint.ExtractConstCols(evalCtx))
	}
	return cols
}

// ExtractValueFromFilter iterates through filter expr and builds a map of constant
// cols to values.
func ExtractValuesFromFilter(
	on FiltersExpr, cols opt.ColSet,
) map[int]tree.Datum {
	vals := make(map[int]tree.Datum)
	for i := range on {
		if on[i].Op() != opt.FiltersItemOp {
			continue
		}

		ok, col, val := extractFixedEquality(on[i].Condition)
		if !ok || !cols.Contains(col) {
			continue
		}
		vals[col] = val
	}
	return vals
}

// extractFixedEquality extracts a column that's being equated to a constant
// value if possible.
func extractFixedEquality(
	condition opt.ScalarExpr,
) (bool, int, tree.Datum) {
	if eq, ok := condition.(*EqExpr); ok {
		if leftVar, ok := eq.Left.(*VariableExpr); ok {
			if CanExtractConstDatum(eq.Right) {
				return true, int(leftVar.Col), ExtractConstDatum(eq.Right)
			}
		}
		if rightVar, ok := eq.Right.(*VariableExpr); ok {
			if CanExtractConstDatum(eq.Left) {
				return true, int(rightVar.Col), ExtractConstDatum(eq.Left)
			}
		}
	}

	return false, 0, nil
}
