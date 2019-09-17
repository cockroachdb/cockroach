// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
		return tree.NewDTuple(t.Typ, datums...)

	case *ArrayExpr:
		elementType := t.Typ.ArrayContents()
		a := tree.NewDArray(elementType)
		a.Array = make(tree.Datums, len(t.Elems))
		for i := range a.Array {
			a.Array[i] = ExtractConstDatum(t.Elems[i])
			if a.Array[i] == tree.DNull {
				a.HasNulls = true
			} else {
				a.HasNonNulls = true
			}
		}
		return a
	}
	panic(errors.AssertionFailedf("non-const expression: %+v", e))
}

// ExtractAggSingleInputColumn returns the input ColumnID of an aggregate
// operator that has a single input.
func ExtractAggSingleInputColumn(e opt.ScalarExpr) opt.ColumnID {
	if !opt.IsAggregateOp(e) {
		panic(errors.AssertionFailedf("not an Aggregate"))
	}
	return ExtractVarFromAggInput(e.Child(0).(opt.ScalarExpr)).Col
}

// ExtractAggInputColumns returns the set of columns the aggregate depends on.
func ExtractAggInputColumns(e opt.ScalarExpr) opt.ColSet {
	if !opt.IsAggregateOp(e) {
		panic(errors.AssertionFailedf("not an Aggregate"))
	}

	if e.ChildCount() == 0 {
		return opt.ColSet{}
	}

	arg := e.Child(0)
	var res opt.ColSet
	if filter, ok := arg.(*AggFilterExpr); ok {
		res.Add(filter.Filter.(*VariableExpr).Col)
		arg = filter.Input
	}
	if distinct, ok := arg.(*AggDistinctExpr); ok {
		arg = distinct.Input
	}
	if variable, ok := arg.(*VariableExpr); ok {
		res.Add(variable.Col)
		return res
	}
	panic(errors.AssertionFailedf("unhandled aggregate input %T", log.Safe(arg)))
}

// ExtractVarFromAggInput is given an argument to an Aggregate and returns the
// inner Variable expression, stripping out modifiers like AggDistinct.
func ExtractVarFromAggInput(arg opt.ScalarExpr) *VariableExpr {
	if filter, ok := arg.(*AggFilterExpr); ok {
		arg = filter.Input
	}
	if distinct, ok := arg.(*AggDistinctExpr); ok {
		arg = distinct.Input
	}
	if variable, ok := arg.(*VariableExpr); ok {
		return variable
	}
	panic(errors.AssertionFailedf("aggregate input not a Variable"))
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

// ExtractJoinEqualityFilters returns the filters containing pairs of columns
// (one from the left side, one from the right side) which are constrained to
// be equal in a join (and have equivalent types).
func ExtractJoinEqualityFilters(leftCols, rightCols opt.ColSet, on FiltersExpr) FiltersExpr {
	// We want to avoid allocating a new slice unless strictly necessary.
	var newFilters FiltersExpr
	for i := range on {
		condition := on[i].Condition
		ok, _, _ := isJoinEquality(leftCols, rightCols, condition)
		if ok {
			if newFilters != nil {
				newFilters = append(newFilters, on[i])
			}
		} else {
			if newFilters == nil {
				newFilters = make(FiltersExpr, i, len(on)-1)
				copy(newFilters, on[:i])
			}
		}
	}
	if newFilters != nil {
		return newFilters
	}
	return on
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

	if leftCols.Contains(lvar.Col) && rightCols.Contains(rvar.Col) {
		return true, lvar.Col, rvar.Col
	}
	if leftCols.Contains(rvar.Col) && rightCols.Contains(lvar.Col) {
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

// ExtractConstColumns returns columns in the filters expression that have been
// constrained to fixed values.
func ExtractConstColumns(
	on FiltersExpr, mem *Memo, evalCtx *tree.EvalContext,
) (fixedCols opt.ColSet) {
	for i := range on {
		scalar := on[i]
		scalarProps := scalar.ScalarProps(mem)
		if scalarProps.Constraints != nil && !scalarProps.Constraints.IsUnconstrained() {
			fixedCols.UnionWith(scalarProps.Constraints.ExtractConstCols(evalCtx))
		}
	}
	return fixedCols
}

// ExtractValuesFromFilter returns a map of constant columns, to the values
// they're constrained to.
func ExtractValuesFromFilter(on FiltersExpr, cols opt.ColSet) map[opt.ColumnID]tree.Datum {
	vals := make(map[opt.ColumnID]tree.Datum)
	for i := range on {
		ok, col, val := extractConstEquality(on[i].Condition)
		if !ok || !cols.Contains(col) {
			continue
		}
		vals[col] = val
	}
	return vals
}

// ExtractConstantFilter returns a map of columns to the filters that constrain
// value to a constant.
func ExtractConstantFilter(on FiltersExpr, cols opt.ColSet) map[opt.ColumnID]FiltersItem {
	vals := make(map[opt.ColumnID]FiltersItem)
	for i := range on {
		ok, col, _ := extractConstEquality(on[i].Condition)
		if !ok || !cols.Contains(col) {
			continue
		}
		vals[col] = on[i]
	}
	return vals
}

// extractConstEquality extracts a column that's being equated to a constant
// value if possible.
func extractConstEquality(condition opt.ScalarExpr) (bool, opt.ColumnID, tree.Datum) {
	// TODO(justin): this is error-prone because this logic is different from the
	// constraint logic. Extract these values directly from the constraints.
	switch condition.(type) {
	case *EqExpr, *IsExpr:
		// Only check the left side - the variable is always on the left side
		// due to the CommuteVar norm rule.
		if leftVar, ok := condition.Child(0).(*VariableExpr); ok {
			if CanExtractConstDatum(condition.Child(1)) {
				return true, leftVar.Col, ExtractConstDatum(condition.Child(1))
			}
		}
	}

	return false, 0, nil
}
