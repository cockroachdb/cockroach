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
