// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/errors"
)

// ComparisonExprWithSubOperator evaluates a comparison expression that has
// sub-operator (which are ANY, SOME, and ALL).
func ComparisonExprWithSubOperator(
	ctx context.Context, evalCtx *Context, expr *tree.ComparisonExpr, left, right tree.Datum,
) (tree.Datum, error) {
	var datums tree.Datums
	// Right is either a tuple or an array of Datums.
	if !expr.Op.CalledOnNullInput && right == tree.DNull {
		return tree.DNull, nil
	} else if tuple, ok := tree.AsDTuple(right); ok {
		datums = tuple.D
	} else if array, ok := tree.AsDArray(right); ok {
		datums = array.Array
	} else {
		return nil, errors.AssertionFailedf("unhandled right expression %s", right)
	}
	return evalDatumsCmp(ctx, evalCtx, expr.Operator, expr.SubOperator, expr.Op, left, datums)
}

func evalComparison(
	ctx context.Context, evalCtx *Context, op treecmp.ComparisonOperator, left, right tree.Datum,
) (tree.Datum, error) {
	if left == tree.DNull || right == tree.DNull {
		return tree.DNull, nil
	}
	ltype := left.ResolvedType()
	rtype := right.ResolvedType()
	if fn, ok := tree.CmpOps[op.Symbol].LookupImpl(ltype, rtype); ok {
		return BinaryOp(ctx, evalCtx, fn.EvalOp, left, right)
	}
	return nil, pgerror.Newf(
		pgcode.UndefinedFunction, "unsupported comparison operator: <%s> %s <%s>", ltype, op, rtype)
}

// evalDatumsCmp evaluates Datums (slice of Datum) using the provided
// sub-operator type (ANY/SOME, ALL) and its CmpOp with the left Datum.
// It returns the result of the ANY/SOME/ALL predicate.
//
// A NULL result is returned if there exists a NULL element and:
//
//	ANY/SOME: no comparisons evaluate to true
//	ALL: no comparisons evaluate to false
//
// For example, given 1 < ANY (SELECT * FROM generate_series(1,3))
// (right is a DTuple), evalTupleCmp would be called with:
//
//	evalDatumsCmp(ctx, LT, Any, CmpOp(LT, leftType, rightParamType), leftDatum, rightTuple.D).
//
// Similarly, given 1 < ANY (ARRAY[1, 2, 3]) (right is a DArray),
// evalArrayCmp would be called with:
//
//	evalDatumsCmp(ctx, LT, Any, CmpOp(LT, leftType, rightParamType), leftDatum, rightArray.Array).
func evalDatumsCmp(
	ctx context.Context,
	evalCtx *Context,
	op, subOp treecmp.ComparisonOperator,
	fn *tree.CmpOp,
	left tree.Datum,
	right tree.Datums,
) (tree.Datum, error) {
	all := op.Symbol == treecmp.All
	any := !all
	sawNull := false
	for _, elem := range right {
		if left == tree.DNull || elem == tree.DNull {
			sawNull = true
			continue
		}

		_, newLeft, newRight, _, not := tree.FoldComparisonExprWithDatums(subOp, left, elem)
		d, err := BinaryOp(ctx, evalCtx, fn.EvalOp, newLeft, newRight)
		if err != nil {
			return nil, err
		}
		if d == tree.DNull {
			sawNull = true
			continue
		}

		b := d.(*tree.DBool)
		res := *b != tree.DBool(not)
		if any && res {
			return tree.DBoolTrue, nil
		} else if all && !res {
			return tree.DBoolFalse, nil
		}
	}

	if sawNull {
		// If the right-hand array contains any null elements and no [false,true]
		// comparison result is obtained, the result of [ALL,ANY] will be null.
		return tree.DNull, nil
	}

	if all {
		// ALL are true && !sawNull
		return tree.DBoolTrue, nil
	}
	// ANY is false && !sawNull
	return tree.DBoolFalse, nil
}

func boolFromCmp(cmp int, op treecmp.ComparisonOperator) *tree.DBool {
	switch op.Symbol {
	case treecmp.EQ, treecmp.IsNotDistinctFrom:
		return tree.MakeDBool(cmp == 0)
	case treecmp.LT:
		return tree.MakeDBool(cmp < 0)
	case treecmp.LE:
		return tree.MakeDBool(cmp <= 0)
	default:
		panic(errors.AssertionFailedf("unexpected ComparisonOperator in boolFromCmp: %v", errors.Safe(op)))
	}
}

func cmpOpTupleFn(
	ctx context.Context,
	cmpCtx tree.CompareContext,
	left, right tree.DTuple,
	op treecmp.ComparisonOperator,
) (tree.Datum, error) {
	cmp := 0
	sawNull := false
	for i, leftElem := range left.D {
		rightElem := right.D[i]
		// Like with cmpOpScalarFn, check for values that need to be handled
		// differently than when ordering Datums.
		if leftElem == tree.DNull || rightElem == tree.DNull {
			switch op.Symbol {
			case treecmp.EQ:
				// If either Datum is NULL and the op is EQ, we continue the
				// comparison and the result is only NULL if the other (non-NULL)
				// elements are equal. This is because NULL is thought of as "unknown",
				// so a NULL equality comparison does not prevent the equality from
				// being proven false, but does prevent it from being proven true.
				sawNull = true

			case treecmp.IsNotDistinctFrom:
				// For IS NOT DISTINCT FROM, NULLs are "equal".
				if leftElem != tree.DNull || rightElem != tree.DNull {
					return tree.DBoolFalse, nil
				}

			default:
				// If either Datum is NULL and the op is not EQ or IS NOT DISTINCT FROM,
				// we short-circuit the evaluation and the result of the comparison is
				// NULL. This is because NULL is thought of as "unknown" and tuple
				// inequality is defined lexicographically, so once a NULL comparison is
				// seen, the result of the entire tuple comparison is unknown.
				return tree.DNull, nil
			}
		} else {
			var err error
			cmp, err = leftElem.Compare(ctx, cmpCtx, rightElem)
			if err != nil {
				return tree.DNull, err
			}
			if cmp != 0 {
				break
			}
		}
	}
	b := boolFromCmp(cmp, op)
	if b == tree.DBoolTrue && sawNull {
		// The op is EQ and all non-NULL elements are equal, but we saw at least
		// one NULL element. Since NULL comparisons are treated as unknown, the
		// result of the comparison becomes unknown (NULL).
		return tree.DNull, nil
	}
	return b, nil
}
