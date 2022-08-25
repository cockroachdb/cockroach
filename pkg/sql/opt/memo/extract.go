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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
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

// ExtractAggFunc digs down into the given aggregate expression and returns the
// aggregate function, skipping past any AggFilter or AggDistinct operators.
func ExtractAggFunc(e opt.ScalarExpr) opt.ScalarExpr {
	if filter, ok := e.(*AggFilterExpr); ok {
		e = filter.Input
	}

	if distinct, ok := e.(*AggDistinctExpr); ok {
		e = distinct.Input
	}

	if !opt.IsAggregateOp(e) {
		panic(errors.AssertionFailedf("not an Aggregate"))
	}

	return e
}

// ExtractAggInputColumns returns the set of columns the aggregate depends on.
func ExtractAggInputColumns(e opt.ScalarExpr) opt.ColSet {
	var res opt.ColSet
	if filter, ok := e.(*AggFilterExpr); ok {
		res.Add(filter.Filter.(*VariableExpr).Col)
		e = filter.Input
	}

	if distinct, ok := e.(*AggDistinctExpr); ok {
		e = distinct.Input
	}

	if !opt.IsAggregateOp(e) {
		panic(errors.AssertionFailedf("not an Aggregate"))
	}

	for i, n := 0, e.ChildCount(); i < n; i++ {
		if variable, ok := e.Child(i).(*VariableExpr); ok {
			res.Add(variable.Col)
		}
	}

	return res
}

// ExtractAggFirstVar is given an aggregate expression and returns the Variable
// expression for the first argument, skipping past modifiers like AggDistinct.
func ExtractAggFirstVar(e opt.ScalarExpr) *VariableExpr {
	e = ExtractAggFunc(e)
	if e.ChildCount() == 0 {
		panic(errors.AssertionFailedf("aggregate does not have any arguments"))
	}

	if variable, ok := e.Child(0).(*VariableExpr); ok {
		return variable
	}

	panic(errors.AssertionFailedf("first aggregate input is not a Variable"))
}

// TODO(mgartner): Document this.
type JoinConditionInfo struct {
	LeftCols     opt.ColList
	RightCols    opt.ColList
	FilterOrds   []int
	FilterOrdSet util.FastIntSet
}

// TODO(mgartner): Document this.
type JoinConditionExtractFlags uint8

// TODO(mgartner): Document this.
const (
	ExtractJoinOkOnly   JoinConditionExtractFlags = 0
	ExtractJoinLeftCols JoinConditionExtractFlags = 1 << (iota - 1)
	ExtractJoinRightCols
	ExtractJoinFilterOrds
	ExtractJoinFilterOrdSet
	JoinConditionExtractAll JoinConditionExtractFlags = (1 << iota) - 1
)

// HasFlags tests whether the given requests are all set.
func (r JoinConditionExtractFlags) HasFlags(subset JoinConditionExtractFlags) bool {
	return r&subset == subset
}

// TODO(mgartner): Document this.
func ExtractJoinConditionInfo(
	leftCols, rightCols opt.ColSet, on FiltersExpr, inequality bool, req JoinConditionExtractFlags,
) (info JoinConditionInfo, ok bool) {
	// Track the seen columns in seenCols. This is not used if only the ok bool
	// is requested.
	var seenCols opt.ColSet
	for i := range on {
		condition := on[i].Condition
		localOk, left, right := ExtractJoinCondition(leftCols, rightCols, condition, inequality)
		if localOk {
			ok = true
		}

		// Return or continue to the next filter if only the ok bool is
		// requested.
		if req == ExtractJoinOkOnly {
			if ok {
				return JoinConditionInfo{}, true
			} else {
				continue
			}
		}

		// Otherwise, collect the requested information about the join
		// condition.
		if seenCols.Contains(left) || seenCols.Contains(right) {
			// Don't allow any column to show up twice.
			// TODO(radu): need to figure out the right thing to do in cases
			//  like: left.a = right.a AND left.a = right.b
			continue
		}
		seenCols.Add(left)
		seenCols.Add(right)
		if req.HasFlags(ExtractJoinLeftCols) {
			info.LeftCols = append(info.LeftCols, left)
		}
		if req.HasFlags(ExtractJoinRightCols) {
			info.RightCols = append(info.RightCols, right)
		}
		if req.HasFlags(ExtractJoinFilterOrdSet) {
			info.FilterOrds = append(info.FilterOrds, i)
		}
		if req.HasFlags(ExtractJoinFilterOrds) {
			info.FilterOrdSet.Add(i)
		}
	}
	return info, ok
}

func isVarEqualityOrInequality(
	condition opt.ScalarExpr, inequality bool,
) (leftVar, rightVar *VariableExpr, ok bool) {
	switch condition.Op() {
	case opt.EqOp:
		if inequality {
			return nil, nil, false
		}
	case opt.LtOp, opt.LeOp, opt.GtOp, opt.GeOp:
		if !inequality {
			return nil, nil, false
		}
	default:
		return nil, nil, false
	}
	leftVar, leftOk := condition.Child(0).(*VariableExpr)
	rightVar, rightOk := condition.Child(1).(*VariableExpr)
	return leftVar, rightVar, leftOk && rightOk
}

// ExtractJoinEquality restricts ExtractJoinCondition to only allow equalities.
func ExtractJoinEquality(
	leftCols, rightCols opt.ColSet, condition opt.ScalarExpr,
) (ok bool, left, right opt.ColumnID) {
	return ExtractJoinCondition(leftCols, rightCols, condition, false /* inequality */)
}

// ExtractJoinCondition returns true if the given condition is a simple equality
// or inequality condition with two variables (e.g. a=b), where one of the
// variables (returned as "left") is in the set of leftCols and the other
// (returned as "right") is in the set of rightCols. inequality is used to
// indicate whether the condition should be an inequality (e.g. < or >).
func ExtractJoinCondition(
	leftCols, rightCols opt.ColSet, condition opt.ScalarExpr, inequality bool,
) (ok bool, left, right opt.ColumnID) {
	lvar, rvar, ok := isVarEqualityOrInequality(condition, inequality)
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
// remaining conditions. Panics if leftEq and rightEq are not the same length.
func ExtractRemainingJoinFilters(on FiltersExpr, leftEq, rightEq opt.ColList) FiltersExpr {
	if len(leftEq) != len(rightEq) {
		panic(errors.AssertionFailedf("leftEq and rightEq have different lengths"))
	}
	if len(leftEq) == 0 {
		return on
	}
	var newFilters FiltersExpr
	for i := range on {
		leftVar, rightVar, ok := isVarEqualityOrInequality(on[i].Condition, false /* inequality */)
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
func ExtractConstColumns(on FiltersExpr, evalCtx *eval.Context) (fixedCols opt.ColSet) {
	for i := range on {
		scalar := on[i]
		scalarProps := scalar.ScalarProps()
		if scalarProps.Constraints != nil && !scalarProps.Constraints.IsUnconstrained() {
			fixedCols.UnionWith(scalarProps.Constraints.ExtractConstCols(evalCtx))
		}
	}
	return fixedCols
}

// ExtractValueForConstColumn returns the constant value of a column returned by
// ExtractConstColumns.
func ExtractValueForConstColumn(
	on FiltersExpr, evalCtx *eval.Context, col opt.ColumnID,
) tree.Datum {
	for i := range on {
		scalar := on[i]
		scalarProps := scalar.ScalarProps()
		if scalarProps.Constraints != nil {
			if val := scalarProps.Constraints.ExtractValueForConstCol(evalCtx, col); val != nil {
				return val
			}
		}
	}
	return nil
}
