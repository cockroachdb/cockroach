// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// CommuteInequality swaps the operands of an inequality comparison expression,
// changing the operator to compensate:
//
//	5 < x
//
// to:
//
//	x > 5
func (c *CustomFuncs) CommuteInequality(
	op opt.Operator, left, right opt.ScalarExpr,
) opt.ScalarExpr {
	op = opt.CommuteEqualityOrInequalityOp(op)
	return c.f.DynamicConstruct(op, right, left).(opt.ScalarExpr)
}

// FoldBinaryCheckOverflow attempts to evaluate a binary expression with
// constant inputs. The only operations supported are plus and minus. It returns
// a constant expression as if all the following criteria are met:
//
//  1. The right datum is an integer, float, decimal, or interval. This
//     restriction can be lifted for any type that we can construct a zero value
//     of. The zero value of the right type is required in order to check for
//     overflow/underflow (see #4).
//  2. An overload function for the given operator and input types exists and
//     has an appropriate volatility.
//  3. The result type of the overload is equivalent to the type of left. This
//     is required in order to check for overflow/underflow (see #4).
//  4. The evaluation causes no error.
//  5. The evaluation does not overflow or underflow.
//
// If any of these conditions are not met, it returns ok=false.
func (c *CustomFuncs) FoldBinaryCheckOverflow(
	op opt.Operator, left, right opt.ScalarExpr,
) (_ opt.ScalarExpr, ok bool) {
	var zeroDatumForRightType tree.Datum
	switch right.DataType().Family() {
	case types.IntFamily, types.FloatFamily, types.DecimalFamily:
		zeroDatumForRightType = tree.DZero
	case types.IntervalFamily:
		zeroDatumForRightType = tree.DZeroInterval
	default:
		// Any other type families of right are not supported.
		return nil, false
	}

	o, ok := memo.FindBinaryOverload(op, left.DataType(), right.DataType())
	if !ok || !c.CanFoldOperator(o.Volatility) {
		return nil, false
	}
	if !o.ReturnType.Equivalent(left.DataType()) {
		// We can only check for overflow or underflow when the result type
		// matches the type of left.
		return nil, false
	}

	lDatum, rDatum := memo.ExtractConstDatum(left), memo.ExtractConstDatum(right)
	result, err := eval.BinaryOp(c.f.evalCtx, o.EvalOp, lDatum, rDatum)
	if err != nil {
		return nil, false
	}

	cmpResLeft, err := result.CompareError(c.f.evalCtx, lDatum)
	if err != nil {
		return nil, false
	}

	cmpRightZero, err := rDatum.CompareError(c.f.evalCtx, zeroDatumForRightType)
	if err != nil {
		return nil, false
	}

	// If the operator is + and right is <0, check for underflow.
	if op == opt.PlusOp && cmpRightZero < 0 && cmpResLeft > 0 {
		return nil, false
	}
	// If the operator is + and right is >=0, check for overflow.
	if op == opt.PlusOp && cmpRightZero >= 0 && cmpResLeft < 0 {
		return nil, false
	}
	// If the operator is - and right is <0, check for overflow.
	if op == opt.MinusOp && cmpRightZero < 0 && cmpResLeft < 0 {
		return nil, false
	}
	// If the operator is - and right is >=0, check for underflow.
	if op == opt.MinusOp && cmpRightZero >= 0 && cmpResLeft > 0 {
		return nil, false
	}
	// The operation did not overflow or underflow.
	return c.f.ConstructConstVal(result, o.ReturnType), true
}

// NormalizeTupleEquality remaps the elements of two tuples compared for
// equality, like this:
//
//	(a, b, c) = (x, y, z)
//
// into this:
//
//	(a = x) AND (b = y) AND (c = z)
func (c *CustomFuncs) NormalizeTupleEquality(left, right memo.ScalarListExpr) opt.ScalarExpr {
	if len(left) != len(right) {
		panic(errors.AssertionFailedf("tuple length mismatch"))
	}
	if len(left) == 0 {
		// () = (), which is always true.
		return memo.TrueSingleton
	}

	var result opt.ScalarExpr
	for i := range left {
		eq := c.f.ConstructEq(left[i], right[i])
		if result == nil {
			result = eq
		} else {
			result = c.f.ConstructAnd(result, eq)
		}
	}
	return result
}

// FirstScalarListExpr returns the first ScalarExpr in the given list.
func (c *CustomFuncs) FirstScalarListExpr(list memo.ScalarListExpr) opt.ScalarExpr {
	return list[0]
}

// SecondScalarListExpr returns the second ScalarExpr in the given list.
func (c *CustomFuncs) SecondScalarListExpr(list memo.ScalarListExpr) opt.ScalarExpr {
	return list[1]
}

// MakeTimeZoneFunction constructs a new timezone() function with the given zone
// and timestamp as arguments. The type of the function result is TIMESTAMPTZ if
// ts is of type TIMESTAMP, or TIMESTAMP if is of type TIMESTAMPTZ.
func (c *CustomFuncs) MakeTimeZoneFunction(zone opt.ScalarExpr, ts opt.ScalarExpr) opt.ScalarExpr {
	argType := types.TimestampTZ
	resultType := types.Timestamp
	if ts.DataType().Family() == types.TimestampFamily {
		argType, resultType = resultType, argType
	}

	args := make(memo.ScalarListExpr, 2)
	args[0] = zone
	args[1] = ts

	props, overload := findTimeZoneFunction(argType)
	return c.f.ConstructFunction(args, &memo.FunctionPrivate{
		Name:       "timezone",
		Typ:        resultType,
		Properties: props,
		Overload:   overload,
	})
}

// findTimeZoneFunction returns the function properties and overload of the
// timezone() function with a second argument that matches the given input type.
// If no overload is found, findTimeZoneFunction panics.
func findTimeZoneFunction(typ *types.T) (*tree.FunctionProperties, *tree.Overload) {
	props, overloads := builtinsregistry.GetBuiltinProperties("timezone")
	for o := range overloads {
		overload := &overloads[o]
		if overload.Types.MatchAt(typ, 1) {
			return props, overload
		}
	}
	panic(errors.AssertionFailedf("could not find overload for timezone"))
}

// STDistanceUseSpheroid returns true if the use_spheroid argument of
// st_distance is not explicitly false. use_spheroid is the third argument of
// st_distance for the geography overload and it is true by default. The
// geometry overload does not have a use_spheroid argument, so if either of the
// first two arguments are geometries, it returns false.
func (c *CustomFuncs) STDistanceUseSpheroid(args memo.ScalarListExpr) bool {
	if len(args) < 2 {
		panic(errors.AssertionFailedf("expected st_distance to have at least two arguments"))
	}
	if args[0].DataType().Family() == types.GeometryFamily ||
		args[1].DataType().Family() == types.GeometryFamily {
		return false
	}
	const useSpheroidIdx = 2
	if len(args) <= useSpheroidIdx {
		// The use_spheroid argument is true by default, so return true if it
		// was not provided.
		return true
	}
	return args[useSpheroidIdx].Op() != opt.FalseOp
}

// MakeIntersectionFunction returns an ST_Intersects function for the given
// arguments.
func (c *CustomFuncs) MakeIntersectionFunction(args memo.ScalarListExpr) opt.ScalarExpr {
	const name = "st_intersects"
	const useSpheroidIdx = 2
	resultType := types.Bool

	// We discard the use_spheroid argument, if present, because st_intersects
	// does not have an overload with a use_spheroid argument. It always uses
	// sphere-based calculation and never spheroid-based calculation. It is safe
	// to discard use_spheroid here because it is guaranteed to be false in the
	// match pattern of FoldEqZeroSTDistance by STDistanceUseSpheroid.
	args = args[:useSpheroidIdx]

	props, overload, ok := memo.FindFunction(&args, name)
	if !ok {
		panic(errors.AssertionFailedf("could not find overload for %s", name))
	}
	return c.f.ConstructFunction(
		args,
		&memo.FunctionPrivate{
			Name:       name,
			Typ:        resultType,
			Properties: props,
			Overload:   overload,
		},
	)
}

// MakeSTDWithinLeft returns an ST_DWithin function that replaces an expression
// of the following form: ST_Distance(a,b) <= x. Note that the ST_Distance
// function is on the left side of the inequality.
func (c *CustomFuncs) MakeSTDWithinLeft(
	op opt.Operator, args memo.ScalarListExpr, bound opt.ScalarExpr,
) opt.ScalarExpr {
	const fnName = "st_dwithin"
	const fnIsLeftArg = true
	return c.makeSTDWithin(op, args, bound, fnName, fnIsLeftArg)
}

// MakeSTDWithinRight returns an ST_DWithin function that replaces an expression
// of the following form: x <= ST_Distance(a,b). Note that the ST_Distance
// function is on the right side of the inequality.
func (c *CustomFuncs) MakeSTDWithinRight(
	op opt.Operator, args memo.ScalarListExpr, bound opt.ScalarExpr,
) opt.ScalarExpr {
	const fnName = "st_dwithin"
	const fnIsLeftArg = false
	return c.makeSTDWithin(op, args, bound, fnName, fnIsLeftArg)
}

// MakeSTDFullyWithinLeft returns an ST_DFullyWithin function that replaces an
// expression of the following form: ST_MaxDistance(a,b) <= x. Note that the
// ST_MaxDistance function is on the left side of the inequality.
func (c *CustomFuncs) MakeSTDFullyWithinLeft(
	op opt.Operator, args memo.ScalarListExpr, bound opt.ScalarExpr,
) opt.ScalarExpr {
	const fnName = "st_dfullywithin"
	const fnIsLeftArg = true
	return c.makeSTDWithin(op, args, bound, fnName, fnIsLeftArg)
}

// MakeSTDFullyWithinRight returns an ST_DFullyWithin function that replaces an
// expression of the following form: x <= ST_MaxDistance(a,b). Note that the
// ST_MaxDistance function is on the right side of the inequality.
func (c *CustomFuncs) MakeSTDFullyWithinRight(
	op opt.Operator, args memo.ScalarListExpr, bound opt.ScalarExpr,
) opt.ScalarExpr {
	const fnName = "st_dfullywithin"
	const fnIsLeftArg = false
	return c.makeSTDWithin(op, args, bound, fnName, fnIsLeftArg)
}

// makeSTDWithin returns an ST_DWithin (or ST_DFullyWithin) function that
// replaces an expression of the following form: ST_Distance(a,b) <= x. The
// ST_Distance function can be on either side of the inequality, and the
// inequality can be one of the following: '<', '<=', '>', '>='. This
// replacement allows early-exit behavior, and may enable use of an inverted
// index scan.
func (c *CustomFuncs) makeSTDWithin(
	op opt.Operator, args memo.ScalarListExpr, bound opt.ScalarExpr, fnName string, fnIsLeftArg bool,
) opt.ScalarExpr {
	var not bool
	var name string
	incName := fnName
	exName := fnName + "exclusive"
	switch op {
	case opt.GeOp:
		if fnIsLeftArg {
			// Matched expression: ST_Distance(a,b) >= x.
			not = true
			name = exName
		} else {
			// Matched expression: x >= ST_Distance(a,b).
			not = false
			name = incName
		}

	case opt.GtOp:
		if fnIsLeftArg {
			// Matched expression: ST_Distance(a,b) > x.
			not = true
			name = incName
		} else {
			// Matched expression: x > ST_Distance(a,b).
			not = false
			name = exName
		}

	case opt.LeOp:
		if fnIsLeftArg {
			// Matched expression: ST_Distance(a,b) <= x.
			not = false
			name = incName
		} else {
			// Matched expression: x <= ST_Distance(a,b).
			not = true
			name = exName
		}

	case opt.LtOp:
		if fnIsLeftArg {
			// Matched expression: ST_Distance(a,b) < x.
			not = false
			name = exName
		} else {
			// Matched expression: x < ST_Distance(a,b).
			not = true
			name = incName
		}
	}

	newArgs := make(memo.ScalarListExpr, len(args)+1)
	const distanceIdx, useSpheroidIdx = 2, 3
	copy(newArgs, args[:distanceIdx])

	// The distance parameter must be type float.
	newArgs[distanceIdx] = c.f.ConstructCast(bound, types.Float)

	// Add the use_spheroid parameter if it exists.
	if len(newArgs) > useSpheroidIdx {
		newArgs[useSpheroidIdx] = args[useSpheroidIdx-1]
	}

	props, overload, ok := memo.FindFunction(&newArgs, name)
	if !ok {
		panic(errors.AssertionFailedf("could not find overload for %s", name))
	}
	within := c.f.ConstructFunction(newArgs, &memo.FunctionPrivate{
		Name:       name,
		Typ:        types.Bool,
		Properties: props,
		Overload:   overload,
	})
	if not {
		// ST_DWithin and ST_DWithinExclusive are equivalent to ST_Distance <= x and
		// ST_Distance < x respectively. The comparison operator in the matched
		// expression (if ST_Distance is normalized to be on the left) is either '>'
		// or '>='. Therefore, we have to take the opposite of within.
		within = c.f.ConstructNot(within)
	}
	return within
}
