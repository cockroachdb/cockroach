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

package tree

import "fmt"

// This file implements comparisons between tuples.
//
// This implements the comparators for three separate relations.
//
// - a total ordering for the purpose of sorting and searching
//   values in indexes. In that relation, NULL sorts at the
//   same location as itself and before other values.
//
//   Functions: TotalOrderLess(), TotalOrderCompare().
//
// - the logical SQL scalar partial ordering, where non-NULL
//   values can be compared to each others but NULL comparisons
//   themselves produce a NULL result.
//
//   Function: scalarCompare()
//
// - the IS [NOT] DISTINCT relation, in which every value can
//   be compared to every other, NULLs are distinct from every
//   non-NULL value but not distinct from each other.
//
//   Function: Distinct().
//
// Due to the way the SQL language semantics are constructed, it is
// the case Distinct() returns true if and only if
// TotalOrderCompare() returns nonzero.  However, one should be
// careful when using this methods to properly convey *intent* to the
// reader of the code:
//
// - the functions related to the total order for sorting should only
//   be used in contexts that are about sorting values.
//
// - Distinct() and scalarCompare() should be used everywhere else.
//
// Besides, separating Distinct() from TotalOrderCompare() enables
// later performance optimizations of the former by specializing the
// code. This is currently done for e.g. EncDatums.

// TotalOrderLess returns true if and only if a sorts before b. NULLs
// are considered to sort first.
func TotalOrderLess(ctx *EvalContext, a, b Datum) bool {
	return doCompare(ctx, true, a, b) < 0
}

// TotalOrderCompare returns -1 if a sorts before b, +1 if a sorts
// after b, and 0 if a and be are considered equal for the purpose of
// sorting.
// This function is only suitable for index span computations and
// should not be used to test equality. Consider Distinct()
// and scalarCompare() instead.
func TotalOrderCompare(ctx *EvalContext, a, b Datum) int {
	return doCompare(ctx, true, a, b)
}

// Distinct returns true if and only if a and b are distinct
// from each other. NULLs are considered to not be distinct from each
// other but are distinct from every other value.
func Distinct(ctx *EvalContext, a, b Datum) bool {
	return doCompare(ctx, true, a, b) != 0
}

// Distinct checks to see if two slices of datums are distinct
// from each other. Any change in value is considered distinct,
// however, a NULL value is NOT considered disctinct from another NULL
// value.
func (d Datums) Distinct(evalCtx *EvalContext, other Datums) bool {
	if len(d) != len(other) {
		return true
	}
	for i, val := range d {
		if Distinct(evalCtx, val, other[i]) {
			return true
		}
	}
	return false
}

// scalarCompare returns a SQL value for the given comparison.
//
// It properly returns NULL if the comparison is not "Distinct" and
// requires comparing NULL values against anything else.
//
// The operator op and two operands must have already undergone
// normalization via foldComparisonExpr: the only supported operators
// here are EQ, LT, LE and IsNotDistinctFrom.
func scalarCompare(ctx *EvalContext, a, b Datum, op ComparisonOperator) Datum {
	if op == IsNotDistinctFrom {
		return MakeDBool(DBool(!Distinct(ctx, a, b)))
	}

	cmp := doCompare(ctx, false, a, b)
	if cmp == -2 {
		return DNull
	}

	switch op {
	case EQ:
		return MakeDBool(cmp == 0)
	case LT:
		return MakeDBool(cmp < 0)
	case LE:
		return MakeDBool(cmp <= 0)
	default:
		panic(fmt.Sprintf("unexpected ComparisonOperator in boolFromCmp: %v", op))
	}
}

// doCompare is the main function.
func doCompare(ctx *EvalContext, orderedNULLs bool, a, b Datum) int {
	if a == DNull || b == DNull {
		if orderedNULLs {
			if b != DNull {
				return -1
			}
			if a != DNull {
				return 1
			}
			return 0
		}
		return -2
	}
	// TODO(knz): this should not use internalCompare any more.
	return a.internalCompare(ctx, b)
}
