// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// ArithmeticErrorsOnOverflow returns true if addition or subtraction with the
// given types will cause an error when the value overflows or underflows.
func (c *CustomFuncs) ArithmeticErrorsOnOverflow(left, right *types.T) bool {
	switch left.Family() {
	case types.TimestampFamily, types.TimestampTZFamily:
		return right.Family() == types.IntervalFamily
	case types.IntFamily, types.FloatFamily, types.DecimalFamily:
	default:
		return false
	}
	switch right.Family() {
	case types.IntFamily, types.FloatFamily, types.DecimalFamily:
	default:
		return false
	}
	return true
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

// CollapseRepeatedLikePatternWildcards returns a new pattern string datum with
// repeated "%" wildcards collapsed. It returns ok=false if the pattern is not a
// constant string or there are no repeated characters to collapse.
func (c *CustomFuncs) CollapseRepeatedLikePatternWildcards(
	pattern opt.ScalarExpr,
) (_ opt.ScalarExpr, ok bool) {
	patternConst, ok := pattern.(*memo.ConstExpr)
	if !ok {
		return nil, false
	}
	switch t := patternConst.Value.(type) {
	case *tree.DString:
		orig := string(*t)
		collapsed := eval.CollapseLikeWildcards(orig)
		if orig != collapsed {
			return c.f.ConstructConstVal(tree.NewDString(collapsed), types.String), true
		}
	case *tree.DCollatedString:
		orig := t.Contents
		collapsed := eval.CollapseLikeWildcards(orig)
		if orig != collapsed {
			d, err := tree.NewDCollatedString(collapsed, t.Locale, &c.f.evalCtx.CollationEnv)
			if err != nil {
				panic(err)
			}
			return c.f.ConstructConstVal(d, patternConst.Typ), true
		}
	}
	return nil, false
}

// MakeLevenshteinLessEqualFunction constructs a new levenshtein_less_equal()
// function with the three given arguments.
func (c *CustomFuncs) MakeLevenshteinLessEqualFunction(
	arg1, arg2, maxD opt.ScalarExpr,
) opt.ScalarExpr {
	args := memo.ScalarListExpr{arg1, arg2, maxD}
	props, overload := findLevenshteinLessEqualFunction()
	return c.f.ConstructFunction(args, &memo.FunctionPrivate{
		Name:       "levenshtein_less_equal",
		Typ:        types.Int,
		Properties: props,
		Overload:   overload,
	})
}

// findLevenshteinLessEqualFunction returns the function properties and overload
// of the levenshtein_less_equal function with the signature (string, string,
// int).
func findLevenshteinLessEqualFunction() (*tree.FunctionProperties, *tree.Overload) {
	props, overloads := builtinsregistry.GetBuiltinProperties("levenshtein_less_equal")
	for o := range overloads {
		overload := &overloads[o]
		if overload.Types.Match([]*types.T{types.String, types.String, types.Int}) {
			return props, overload
		}
	}
	panic(errors.AssertionFailedf("could not find overload for levenshtein_less_equal"))
}
