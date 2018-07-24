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
//   Functions: IsLowerOrdered(), TotalOrderComparison().
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
//   Function: IsDistinct().
//
// Due to the way the SQL language semantics are constructed, it is
// the case IsDistinct() returns true if and only if
// TotalOrderComparison() returns nonzero.  However, one should be
// careful when using this methods to properly convey *intent* to the
// reader of the code:
//
// - the functions related to the total order for sorting should only
//   be used in contexts that are about sorting values.
//
// - IsDistinct() and scalarCompare() should be used everywhere else.
//

// IsLowerOrdered returns true if and only if a sorts before b. NULLs
// are considered to sort first.
func IsLowerOrdered(ctx *EvalContext, a, b Datum) bool {
	return comparex(ctx, true, a, b) < 0
}

// TotalOrderComparison returns -1 if a sorts before b, +1 if a sorts
// after b, and 0 if a and be are considered equal for the purpose of
// sorting.
// This function is only suitable for index span computations and
// should not be used to test equality. Consider IsDistinct()
// and scalarCompare() instead.
func TotalOrderComparison(ctx *EvalContext, a, b Datum) int {
	return comparex(ctx, true, a, b)
}

// IsDistinct returns true if and only if a and b are distinct from
// each other. NULLs are considered to not be distinct from each other
// but are distinct from every other value.
func IsDistinct(ctx *EvalContext, a, b Datum) bool {
	return comparex(ctx, true, a, b) != 0
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
		return MakeDBool(DBool(!IsDistinct(ctx, a, b)))
	}

	cmp := comparex(ctx, false, a, b)
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

// comparex is the main function.
func comparex(ctx *EvalContext, orderedNULLs bool, a, b Datum) int {
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
	// FIXME: this should not use TotallyBrokenCompare any more.
	return a.TotallyBrokenCompare(ctx, b)
}
