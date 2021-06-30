// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"unsafe"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

func initAggregateBuiltins() {
	// Add all aggregates to the Builtins map after a few sanity checks.
	for k, v := range aggregates {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}

		if v.props.Class != tree.AggregateClass {
			panic(errors.AssertionFailedf("%s: aggregate functions should be marked with the tree.AggregateClass "+
				"function class, found %v", k, v))
		}
		for _, a := range v.overloads {
			if a.AggregateFunc == nil {
				panic(errors.AssertionFailedf("%s: aggregate functions should have tree.AggregateFunc constructors, "+
					"found %v", k, a))
			}
			if a.WindowFunc == nil {
				panic(errors.AssertionFailedf("%s: aggregate functions should have tree.WindowFunc constructors, "+
					"found %v", k, a))
			}
		}

		builtins[k] = v
	}
}

func aggProps() tree.FunctionProperties {
	return tree.FunctionProperties{Class: tree.AggregateClass}
}

func aggPropsNullableArgs() tree.FunctionProperties {
	f := aggProps()
	f.NullableArgs = true
	return f
}

// allMaxMinAggregateTypes contains extra types that aren't in
// types.Scalar that the max/min aggregate functions are defined on.
var allMaxMinAggregateTypes = append(
	[]*types.T{types.AnyCollatedString, types.AnyEnum},
	types.Scalar...,
)

// aggregates are a special class of builtin functions that are wrapped
// at execution in a bucketing layer to combine (aggregate) the result
// of the function being run over many rows.
//
// See `aggregateFuncHolder` in the sql package.
//
// In particular they must not be simplified during normalization
// (and thus must be marked as impure), even when they are given a
// constant argument (e.g. SUM(1)). This is because aggregate
// functions must return NULL when they are no rows in the source
// table, so their evaluation must always be delayed until query
// execution.
//
// Some aggregate functions must handle nullable arguments, since normalizing
// an aggregate function call to NULL in the presence of a NULL argument may
// not be correct. There are two cases where an aggregate function must handle
// nullable arguments:
// 1) the aggregate function does not skip NULLs (e.g., ARRAY_AGG); and
// 2) the aggregate function does not return NULL when it aggregates no rows
//		(e.g., COUNT).
//
// For use in other packages, see AllAggregateBuiltinNames and
// GetBuiltinProperties().
// These functions are also identified with Class == tree.AggregateClass.
// The properties are reachable via tree.FunctionDefinition.
var aggregates = map[string]builtinDefinition{
	"array_agg": setProps(aggPropsNullableArgs(),
		arrayBuiltin(func(t *types.T) tree.Overload {
			return makeAggOverloadWithReturnType(
				[]*types.T{t},
				func(args []tree.TypedExpr) *types.T {
					if len(args) == 0 {
						return types.MakeArray(t)
					}
					// Whenever possible, use the expression's type, so we can properly
					// handle aliased types that don't explicitly have overloads.
					return types.MakeArray(args[0].ResolvedType())
				},
				newArrayAggregate,
				"Aggregates the selected values into an array.",
				tree.VolatilityImmutable,
			)
		})),

	"avg": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntAvgAggregate,
			"Calculates the average of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatAvgAggregate,
			"Calculates the average of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalAvgAggregate,
			"Calculates the average of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Interval}, types.Interval, newIntervalAvgAggregate,
			"Calculates the average of the selected values.", tree.VolatilityImmutable),
	),

	"bit_and": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Int, newIntBitAndAggregate,
			"Calculates the bitwise AND of all non-null input values, or null if none.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.VarBit}, types.VarBit, newBitBitAndAggregate,
			"Calculates the bitwise AND of all non-null input values, or null if none.", tree.VolatilityImmutable),
	),

	"bit_or": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Int, newIntBitOrAggregate,
			"Calculates the bitwise OR of all non-null input values, or null if none.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.VarBit}, types.VarBit, newBitBitOrAggregate,
			"Calculates the bitwise OR of all non-null input values, or null if none.", tree.VolatilityImmutable),
	),

	"bool_and": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Bool}, types.Bool, newBoolAndAggregate,
			"Calculates the boolean value of `AND`ing all selected values.", tree.VolatilityImmutable),
	),

	"bool_or": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Bool}, types.Bool, newBoolOrAggregate,
			"Calculates the boolean value of `OR`ing all selected values.", tree.VolatilityImmutable),
	),

	"concat_agg": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.String}, types.String, newStringConcatAggregate,
			"Concatenates all selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Bytes}, types.Bytes, newBytesConcatAggregate,
			"Concatenates all selected values.", tree.VolatilityImmutable),
		// TODO(eisen): support collated strings when the type system properly
		// supports parametric types.
	),

	"corr": makeRegressionAggregateBuiltin(
		newCorrAggregate,
		"Calculates the correlation coefficient of the selected values.",
	),

	"covar_pop": makeRegressionAggregateBuiltin(
		newCovarPopAggregate,
		"Calculates the population covariance of the selected values.",
	),

	"covar_samp": makeRegressionAggregateBuiltin(
		newCovarSampAggregate,
		"Calculates the sample covariance of the selected values.",
	),

	"regr_avgx": makeRegressionAggregateBuiltin(
		newRegressionAvgXAggregate, "Calculates the average of the independent variable (sum(X)/N).",
	),

	"regr_avgy": makeRegressionAggregateBuiltin(
		newRegressionAvgYAggregate, "Calculates the average of the dependent variable (sum(Y)/N).",
	),

	"regr_intercept": makeRegressionAggregateBuiltin(
		newRegressionInterceptAggregate, "Calculates y-intercept of the least-squares-fit linear equation determined by the (X, Y) pairs.",
	),

	"regr_r2": makeRegressionAggregateBuiltin(
		newRegressionR2Aggregate, "Calculates square of the correlation coefficient.",
	),

	"regr_slope": makeRegressionAggregateBuiltin(
		newRegressionSlopeAggregate, "Calculates slope of the least-squares-fit linear equation determined by the (X, Y) pairs.",
	),

	"regr_sxx": makeRegressionAggregateBuiltin(
		newRegressionSXXAggregate, "Calculates sum of squares of the independent variable.",
	),

	"regr_sxy": makeRegressionAggregateBuiltin(
		newRegressionSXYAggregate, "Calculates sum of products of independent times dependent variable.",
	),

	"regr_syy": makeRegressionAggregateBuiltin(
		newRegressionSYYAggregate, "Calculates sum of squares of the dependent variable.",
	),

	"regr_count": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Float, types.Float}, types.Int, newRegressionCountAggregate,
			"Calculates number of input rows in which both expressions are nonnull.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Int, types.Int}, types.Int, newRegressionCountAggregate,
			"Calculates number of input rows in which both expressions are nonnull.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal, types.Decimal}, types.Int, newRegressionCountAggregate,
			"Calculates number of input rows in which both expressions are nonnull.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Float, types.Int}, types.Int, newRegressionCountAggregate,
			"Calculates number of input rows in which both expressions are nonnull.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Float, types.Decimal}, types.Int, newRegressionCountAggregate,
			"Calculates number of input rows in which both expressions are nonnull.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Int, types.Float}, types.Int, newRegressionCountAggregate,
			"Calculates number of input rows in which both expressions are nonnull.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Int, types.Decimal}, types.Int, newRegressionCountAggregate,
			"Calculates number of input rows in which both expressions are nonnull.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal, types.Float}, types.Int, newRegressionCountAggregate,
			"Calculates number of input rows in which both expressions are nonnull.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal, types.Int}, types.Int, newRegressionCountAggregate,
			"Calculates number of input rows in which both expressions are nonnull.", tree.VolatilityImmutable),
	),

	"count": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.Any}, types.Int, newCountAggregate,
			"Calculates the number of selected elements.", tree.VolatilityImmutable),
	),

	"count_rows": makeBuiltin(aggProps(),
		tree.Overload{
			Types:         tree.ArgTypes{},
			ReturnType:    tree.FixedReturnType(types.Int),
			AggregateFunc: newCountRowsAggregate,
			WindowFunc: func(params []*types.T, evalCtx *tree.EvalContext) tree.WindowFunc {
				return newFramableAggregateWindow(
					newCountRowsAggregate(params, evalCtx, nil /* arguments */),
					func(evalCtx *tree.EvalContext, arguments tree.Datums) tree.AggregateFunc {
						return newCountRowsAggregate(params, evalCtx, arguments)
					},
				)
			},
			Info:       "Calculates the number of rows.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"every": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Bool}, types.Bool, newBoolAndAggregate,
			"Calculates the boolean value of `AND`ing all selected values.", tree.VolatilityImmutable),
	),

	"max": collectOverloads(aggProps(), allMaxMinAggregateTypes,
		func(t *types.T) tree.Overload {
			info := "Identifies the maximum selected value."
			vol := tree.VolatilityImmutable
			return makeAggOverloadWithReturnType(
				[]*types.T{t}, tree.IdentityReturnType(0), newMaxAggregate, info, vol,
			)
		}),

	"min": collectOverloads(aggProps(), allMaxMinAggregateTypes,
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			vol := tree.VolatilityImmutable
			return makeAggOverloadWithReturnType(
				[]*types.T{t}, tree.IdentityReturnType(0), newMinAggregate, info, vol,
			)
		}),

	"string_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.String, types.String}, types.String, newStringConcatAggregate,
			"Concatenates all selected values using the provided delimiter.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Bytes, types.Bytes}, types.Bytes, newBytesConcatAggregate,
			"Concatenates all selected values using the provided delimiter.", tree.VolatilityImmutable),
	),

	"sum_int": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Int, newSmallIntSumAggregate,
			"Calculates the sum of the selected values.", tree.VolatilityImmutable),
	),

	"sum": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntSumAggregate,
			"Calculates the sum of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatSumAggregate,
			"Calculates the sum of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalSumAggregate,
			"Calculates the sum of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Interval}, types.Interval, newIntervalSumAggregate,
			"Calculates the sum of the selected values.", tree.VolatilityImmutable),
	),

	"sqrdiff": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values.", tree.VolatilityImmutable),
	),

	// final_(variance|stddev) computes the global (variance|standard deviation)
	// from an arbitrary collection of local sums of squared difference from the mean.
	// Adapted from https://www.johndcook.com/blog/skewness_kurtosis and
	// https://github.com/cockroachdb/cockroach/pull/17728.

	// TODO(knz): The 3-argument final_variance and final_stddev are
	// only defined for internal use by distributed aggregations. They
	// are marked as "private" so as to not trigger panics from issue
	// #10495.

	// The input signature is: SQDIFF, SUM, COUNT
	"final_variance": makePrivate(makeBuiltin(aggProps(),
		makeAggOverload(
			[]*types.T{types.Decimal, types.Decimal, types.Int},
			types.Decimal,
			newDecimalFinalVarianceAggregate,
			"Calculates the variance from the selected locally-computed squared difference values.",
			tree.VolatilityImmutable,
		),
		makeAggOverload(
			[]*types.T{types.Float, types.Float, types.Int},
			types.Float,
			newFloatFinalVarianceAggregate,
			"Calculates the variance from the selected locally-computed squared difference values.",
			tree.VolatilityImmutable,
		),
	)),

	"final_stddev": makePrivate(makeBuiltin(aggProps(),
		makeAggOverload(
			[]*types.T{types.Decimal, types.Decimal, types.Int},
			types.Decimal,
			newDecimalFinalStdDevAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
			tree.VolatilityImmutable,
		),
		makeAggOverload(
			[]*types.T{types.Float, types.Float, types.Int},
			types.Float,
			newFloatFinalStdDevAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
			tree.VolatilityImmutable,
		),
	)),

	// variance is a historical alias for var_samp.
	"variance": makeVarianceBuiltin(),
	"var_samp": makeVarianceBuiltin(),
	"var_pop": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntVarPopAggregate,
			"Calculates the population variance of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalVarPopAggregate,
			"Calculates the population variance of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatVarPopAggregate,
			"Calculates the population variance of the selected values.", tree.VolatilityImmutable),
	),

	// stddev is a historical alias for stddev_samp.
	"stddev":      makeStdDevBuiltin(),
	"stddev_samp": makeStdDevBuiltin(),
	"stddev_pop": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntStdDevPopAggregate,
			"Calculates the population standard deviation of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalStdDevPopAggregate,
			"Calculates the population standard deviation of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatStdDevPopAggregate,
			"Calculates the population standard deviation of the selected values.", tree.VolatilityImmutable),
	),

	"xor_agg": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Bytes}, types.Bytes, newBytesXorAggregate,
			"Calculates the bitwise XOR of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Int}, types.Int, newIntXorAggregate,
			"Calculates the bitwise XOR of the selected values.", tree.VolatilityImmutable),
	),

	"json_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.Any}, types.Jsonb, newJSONAggregate,
			"Aggregates values as a JSON or JSONB array.", tree.VolatilityStable),
	),

	"jsonb_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.Any}, types.Jsonb, newJSONAggregate,
			"Aggregates values as a JSON or JSONB array.", tree.VolatilityStable),
	),

	"json_object_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.String, types.Any}, types.Jsonb, newJSONObjectAggregate,
			"Aggregates values as a JSON or JSONB object.", tree.VolatilityStable),
	),
	"jsonb_object_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.String, types.Any}, types.Jsonb, newJSONObjectAggregate,
			"Aggregates values as a JSON or JSONB object.", tree.VolatilityStable),
	),

	"st_makeline": makeBuiltin(
		tree.FunctionProperties{
			Class:                   tree.AggregateClass,
			NullableArgs:            true,
			AvailableOnPublicSchema: true,
		},
		makeAggOverload(
			[]*types.T{types.Geometry},
			types.Geometry,
			func(
				params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
			) tree.AggregateFunc {
				return &stMakeLineAgg{
					acc: evalCtx.Mon.MakeBoundAccount(),
				}
			},
			infoBuilder{
				info: "Forms a LineString from Point, MultiPoint or LineStrings. Other shapes will be ignored.",
			}.String(),
			tree.VolatilityImmutable,
		),
	),
	"st_extent": makeBuiltin(
		tree.FunctionProperties{
			Class:                   tree.AggregateClass,
			NullableArgs:            true,
			AvailableOnPublicSchema: true,
		},
		makeAggOverload(
			[]*types.T{types.Geometry},
			types.Box2D,
			func(
				params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
			) tree.AggregateFunc {
				return &stExtentAgg{}
			},
			infoBuilder{
				info: "Forms a Box2D that encapsulates all provided geometries.",
			}.String(),
			tree.VolatilityImmutable,
		),
	),
	"st_union":      makeSTUnionBuiltin(),
	"st_memunion":   makeSTUnionBuiltin(),
	"st_collect":    makeSTCollectBuiltin(),
	"st_memcollect": makeSTCollectBuiltin(),

	AnyNotNull: makePrivate(makeBuiltin(aggProps(),
		makeAggOverloadWithReturnType(
			[]*types.T{types.Any},
			tree.IdentityReturnType(0),
			newAnyNotNullAggregate,
			"Returns an arbitrary not-NULL value, or NULL if none exists.",
			tree.VolatilityImmutable,
		))),

	// Ordered-set aggregations.
	"percentile_disc": makeBuiltin(aggProps(),
		makeAggOverloadWithReturnType(
			[]*types.T{types.Float},
			func(args []tree.TypedExpr) *types.T { return tree.UnknownReturnType },
			builtinMustNotRun,
			"Discrete percentile: returns the first input value whose position in the ordering equals or "+
				"exceeds the specified fraction.",
			tree.VolatilityImmutable),
		makeAggOverloadWithReturnType(
			[]*types.T{types.FloatArray},
			func(args []tree.TypedExpr) *types.T { return tree.UnknownReturnType },
			builtinMustNotRun,
			"Discrete percentile: returns input values whose position in the ordering equals or "+
				"exceeds the specified fractions.",
			tree.VolatilityImmutable),
	),
	"percentile_disc_impl": makePrivate(collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			return makeAggOverload([]*types.T{types.Float, t}, t, newPercentileDiscAggregate,
				"Implementation of percentile_disc.",
				tree.VolatilityImmutable)
		},
		func(t *types.T) tree.Overload {
			return makeAggOverload([]*types.T{types.FloatArray, t}, types.MakeArray(t), newPercentileDiscAggregate,
				"Implementation of percentile_disc.",
				tree.VolatilityImmutable)
		},
	)),
	"percentile_cont": makeBuiltin(aggProps(),
		makeAggOverload(
			[]*types.T{types.Float},
			types.Float,
			builtinMustNotRun,
			"Continuous percentile: returns a float corresponding to the specified fraction in the ordering, "+
				"interpolating between adjacent input floats if needed.",
			tree.VolatilityImmutable),
		makeAggOverload(
			[]*types.T{types.Float},
			types.Interval,
			builtinMustNotRun,
			"Continuous percentile: returns an interval corresponding to the specified fraction in the ordering, "+
				"interpolating between adjacent input intervals if needed.",
			tree.VolatilityImmutable),
		makeAggOverload(
			[]*types.T{types.FloatArray},
			types.MakeArray(types.Float),
			builtinMustNotRun,
			"Continuous percentile: returns floats corresponding to the specified fractions in the ordering, "+
				"interpolating between adjacent input floats if needed.",
			tree.VolatilityImmutable),
		makeAggOverload(
			[]*types.T{types.FloatArray},
			types.MakeArray(types.Interval),
			builtinMustNotRun,
			"Continuous percentile: returns intervals corresponding to the specified fractions in the ordering, "+
				"interpolating between adjacent input intervals if needed.",
			tree.VolatilityImmutable),
	),
	"percentile_cont_impl": makePrivate(makeBuiltin(aggProps(),
		makeAggOverload(
			[]*types.T{types.Float, types.Float},
			types.Float,
			newPercentileContAggregate,
			"Implementation of percentile_cont.",
			tree.VolatilityImmutable),
		makeAggOverload(
			[]*types.T{types.Float, types.Interval},
			types.Interval,
			newPercentileContAggregate,
			"Implementation of percentile_cont.",
			tree.VolatilityImmutable),
		makeAggOverload(
			[]*types.T{types.FloatArray, types.Float},
			types.MakeArray(types.Float),
			newPercentileContAggregate,
			"Implementation of percentile_cont.",
			tree.VolatilityImmutable),
		makeAggOverload(
			[]*types.T{types.FloatArray, types.Interval},
			types.MakeArray(types.Interval),
			newPercentileContAggregate,
			"Implementation of percentile_cont.",
			tree.VolatilityImmutable),
	)),
}

// AnyNotNull is the name of the aggregate returned by NewAnyNotNullAggregate.
const AnyNotNull = "any_not_null"

func makePrivate(b builtinDefinition) builtinDefinition {
	b.props.Private = true
	return b
}

func makeAggOverload(
	in []*types.T,
	ret *types.T,
	f func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc,
	info string,
	volatility tree.Volatility,
) tree.Overload {
	return makeAggOverloadWithReturnType(
		in,
		tree.FixedReturnType(ret),
		f,
		info,
		volatility,
	)
}

func makeAggOverloadWithReturnType(
	in []*types.T,
	retType tree.ReturnTyper,
	f func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc,
	info string,
	volatility tree.Volatility,
) tree.Overload {
	argTypes := make(tree.ArgTypes, len(in))
	for i, typ := range in {
		argTypes[i].Name = fmt.Sprintf("arg%d", i+1)
		argTypes[i].Typ = typ
	}

	return tree.Overload{
		// See the comment about aggregate functions in the definitions
		// of the Builtins array above.
		Types:         argTypes,
		ReturnType:    retType,
		AggregateFunc: f,
		WindowFunc: func(params []*types.T, evalCtx *tree.EvalContext) tree.WindowFunc {
			aggWindowFunc := f(params, evalCtx, nil /* arguments */)
			switch w := aggWindowFunc.(type) {
			case *minAggregate:
				min := &slidingWindowFunc{}
				min.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return -a.Compare(evalCtx, b)
				})
				return min
			case *maxAggregate:
				max := &slidingWindowFunc{}
				max.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return a.Compare(evalCtx, b)
				})
				return max
			case *intSumAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *decimalSumAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *floatSumAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *intervalSumAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *avgAggregate:
				// w.agg is a sum aggregate.
				return &avgWindowFunc{sum: newSlidingWindowSumFunc(w.agg)}
			}

			return newFramableAggregateWindow(
				aggWindowFunc,
				func(evalCtx *tree.EvalContext, arguments tree.Datums) tree.AggregateFunc {
					return f(params, evalCtx, arguments)
				},
			)
		},
		Info:       info,
		Volatility: volatility,
	}
}

func makeStdDevBuiltin() builtinDefinition {
	return makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntStdDevAggregate,
			"Calculates the standard deviation of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalStdDevAggregate,
			"Calculates the standard deviation of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatStdDevAggregate,
			"Calculates the standard deviation of the selected values.", tree.VolatilityImmutable),
	)
}

func makeSTCollectBuiltin() builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{
			Class:                   tree.AggregateClass,
			NullableArgs:            true,
			AvailableOnPublicSchema: true,
		},
		makeAggOverload(
			[]*types.T{types.Geometry},
			types.Geometry,
			newSTCollectAgg,
			infoBuilder{
				info: "Collects geometries into a GeometryCollection or multi-type as appropriate.",
			}.String(),
			tree.VolatilityImmutable,
		),
	)
}

func makeSTUnionBuiltin() builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{
			Class:                   tree.AggregateClass,
			NullableArgs:            true,
			AvailableOnPublicSchema: true,
		},
		makeAggOverload(
			[]*types.T{types.Geometry},
			types.Geometry,
			func(
				params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
			) tree.AggregateFunc {
				return &stUnionAgg{
					acc: evalCtx.Mon.MakeBoundAccount(),
				}
			},
			infoBuilder{
				info: "Applies a spatial union to the geometries provided.",
			}.String(),
			tree.VolatilityImmutable,
		),
	)
}

func makeRegressionAggregateBuiltin(
	aggregateFunc func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc, info string,
) builtinDefinition {
	return makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Float, types.Float}, types.Float, aggregateFunc,
			info, tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Int, types.Int}, types.Float, aggregateFunc,
			info, tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal, types.Decimal}, types.Float, aggregateFunc,
			info, tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Float, types.Int}, types.Float, aggregateFunc,
			info, tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Float, types.Decimal}, types.Float, aggregateFunc,
			info, tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Int, types.Float}, types.Float, aggregateFunc,
			info, tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Int, types.Decimal}, types.Float, aggregateFunc,
			info, tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal, types.Float}, types.Float, aggregateFunc,
			info, tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal, types.Int}, types.Float, aggregateFunc,
			info, tree.VolatilityImmutable),
	)
}

type stMakeLineAgg struct {
	flatCoords []float64
	layout     geom.Layout
	acc        mon.BoundAccount
}

// Add implements the AggregateFunc interface.
func (agg *stMakeLineAgg) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	if firstArg == tree.DNull {
		return nil
	}
	geomArg := tree.MustBeDGeometry(firstArg)

	g, err := geomArg.AsGeomT()
	if err != nil {
		return err
	}

	if len(agg.flatCoords) == 0 {
		agg.layout = g.Layout()
	} else if agg.layout != g.Layout() {
		return errors.Newf(
			"mixed dimensionality not allowed (adding dimension %s to dimension %s)",
			g.Layout(),
			agg.layout,
		)
	}
	switch g.(type) {
	case *geom.Point, *geom.LineString, *geom.MultiPoint:
		if err := agg.acc.Grow(ctx, int64(len(g.FlatCoords())*8)); err != nil {
			return err
		}
		agg.flatCoords = append(agg.flatCoords, g.FlatCoords()...)
	}
	return nil
}

// Result implements the AggregateFunc interface.
func (agg *stMakeLineAgg) Result() (tree.Datum, error) {
	if len(agg.flatCoords) == 0 {
		return tree.DNull, nil
	}
	g, err := geo.MakeGeometryFromGeomT(geom.NewLineStringFlat(agg.layout, agg.flatCoords))
	if err != nil {
		return nil, err
	}
	return tree.NewDGeometry(g), nil
}

// Reset implements the AggregateFunc interface.
func (agg *stMakeLineAgg) Reset(ctx context.Context) {
	agg.flatCoords = agg.flatCoords[:0]
	agg.acc.Empty(ctx)
}

// Close implements the AggregateFunc interface.
func (agg *stMakeLineAgg) Close(ctx context.Context) {
	agg.acc.Close(ctx)
}

// Size implements the AggregateFunc interface.
func (agg *stMakeLineAgg) Size() int64 {
	return sizeOfSTMakeLineAggregate
}

type stUnionAgg struct {
	srid geopb.SRID
	// TODO(#geo): store the current union object in C memory, to avoid the EWKB round trips.
	ewkb geopb.EWKB
	acc  mon.BoundAccount
	set  bool
}

// Add implements the AggregateFunc interface.
func (agg *stUnionAgg) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	if firstArg == tree.DNull {
		return nil
	}
	geomArg := tree.MustBeDGeometry(firstArg)
	if !agg.set {
		agg.ewkb = geomArg.EWKB()
		agg.set = true
		agg.srid = geomArg.SRID()
		return nil
	}
	if agg.srid != geomArg.SRID() {
		c, err := geo.ParseGeometryFromEWKB(agg.ewkb)
		if err != nil {
			return err
		}
		return geo.NewMismatchingSRIDsError(geomArg.Geometry.SpatialObject(), c.SpatialObject())
	}
	if err := agg.acc.Grow(ctx, int64(len(geomArg.EWKB()))); err != nil {
		return err
	}
	var err error
	// TODO(#geo):We are allocating a slice for the result each time we
	// call geos.Union in cStringToSafeGoBytes.
	// We could change geos.Union to accept the existing slice.
	agg.ewkb, err = geos.Union(agg.ewkb, geomArg.EWKB())
	if err != nil {
		return err
	}
	return agg.acc.ResizeTo(ctx, int64(len(agg.ewkb)))
}

// Result implements the AggregateFunc interface.
func (agg *stUnionAgg) Result() (tree.Datum, error) {
	if !agg.set {
		return tree.DNull, nil
	}
	g, err := geo.ParseGeometryFromEWKB(agg.ewkb)
	if err != nil {
		return nil, err
	}
	return tree.NewDGeometry(g), nil
}

// Reset implements the AggregateFunc interface.
func (agg *stUnionAgg) Reset(ctx context.Context) {
	agg.ewkb = nil
	agg.set = false
	agg.acc.Empty(ctx)
}

// Close implements the AggregateFunc interface.
func (agg *stUnionAgg) Close(ctx context.Context) {
	agg.acc.Close(ctx)
}

// Size implements the AggregateFunc interface.
func (agg *stUnionAgg) Size() int64 {
	return sizeOfSTUnionAggregate
}

type stCollectAgg struct {
	acc  mon.BoundAccount
	coll geom.T
}

func newSTCollectAgg(_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &stCollectAgg{
		acc: evalCtx.Mon.MakeBoundAccount(),
	}
}

// Add implements the AggregateFunc interface.
func (agg *stCollectAgg) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	if firstArg == tree.DNull {
		return nil
	}
	if err := agg.acc.Grow(ctx, int64(firstArg.Size())); err != nil {
		return err
	}
	geomArg := tree.MustBeDGeometry(firstArg)
	t, err := geomArg.AsGeomT()
	if err != nil {
		return err
	}
	if agg.coll != nil && agg.coll.SRID() != t.SRID() {
		c, err := geo.MakeGeometryFromGeomT(agg.coll)
		if err != nil {
			return err
		}
		return geo.NewMismatchingSRIDsError(geomArg.Geometry.SpatialObject(), c.SpatialObject())
	}

	// Fast path for geometry collections
	if gc, ok := agg.coll.(*geom.GeometryCollection); ok {
		return gc.Push(t)
	}

	// Try to append to a multitype, if possible.
	switch t := t.(type) {
	case *geom.Point:
		if agg.coll == nil {
			agg.coll = geom.NewMultiPoint(t.Layout()).SetSRID(t.SRID())
		}
		if multi, ok := agg.coll.(*geom.MultiPoint); ok {
			return multi.Push(t)
		}
	case *geom.LineString:
		if agg.coll == nil {
			agg.coll = geom.NewMultiLineString(t.Layout()).SetSRID(t.SRID())
		}
		if multi, ok := agg.coll.(*geom.MultiLineString); ok {
			return multi.Push(t)
		}
	case *geom.Polygon:
		if agg.coll == nil {
			agg.coll = geom.NewMultiPolygon(t.Layout()).SetSRID(t.SRID())
		}
		if multi, ok := agg.coll.(*geom.MultiPolygon); ok {
			return multi.Push(t)
		}
	}

	// At this point, agg.coll is either a multitype incompatible with t, or nil.
	var gc *geom.GeometryCollection
	if agg.coll != nil {
		// Converting the multitype to a collection temporarily doubles the memory usage.
		usedMem := agg.acc.Used()
		if err := agg.acc.Grow(ctx, usedMem); err != nil {
			return err
		}
		gc, err = agg.multiToCollection(agg.coll)
		if err != nil {
			return err
		}
		agg.coll = nil
		agg.acc.Shrink(ctx, usedMem)
	} else {
		gc = geom.NewGeometryCollection().SetSRID(t.SRID())
	}
	agg.coll = gc
	return gc.Push(t)
}

func (agg *stCollectAgg) multiToCollection(multi geom.T) (*geom.GeometryCollection, error) {
	gc := geom.NewGeometryCollection().SetSRID(multi.SRID())
	switch t := multi.(type) {
	case *geom.MultiPoint:
		for i := 0; i < t.NumPoints(); i++ {
			if err := gc.Push(t.Point(i)); err != nil {
				return nil, err
			}
		}
	case *geom.MultiLineString:
		for i := 0; i < t.NumLineStrings(); i++ {
			if err := gc.Push(t.LineString(i)); err != nil {
				return nil, err
			}
		}
	case *geom.MultiPolygon:
		for i := 0; i < t.NumPolygons(); i++ {
			if err := gc.Push(t.Polygon(i)); err != nil {
				return nil, err
			}
		}
	default:
		return nil, errors.AssertionFailedf("unexpected geometry type: %T", t)
	}
	return gc, nil
}

// Result implements the AggregateFunc interface.
func (agg *stCollectAgg) Result() (tree.Datum, error) {
	if agg.coll == nil {
		return tree.DNull, nil
	}
	g, err := geo.MakeGeometryFromGeomT(agg.coll)
	if err != nil {
		return nil, err
	}
	return tree.NewDGeometry(g), nil
}

// Reset implements the AggregateFunc interface.
func (agg *stCollectAgg) Reset(ctx context.Context) {
	agg.coll = nil
	agg.acc.Empty(ctx)
}

// Close implements the AggregateFunc interface.
func (agg *stCollectAgg) Close(ctx context.Context) {
	agg.acc.Close(ctx)
}

// Size implements the AggregateFunc interface.
func (agg *stCollectAgg) Size() int64 {
	return sizeOfSTCollectAggregate
}

type stExtentAgg struct {
	bbox *geo.CartesianBoundingBox
}

// Add implements the AggregateFunc interface.
func (agg *stExtentAgg) Add(_ context.Context, firstArg tree.Datum, otherArgs ...tree.Datum) error {
	if firstArg == tree.DNull {
		return nil
	}
	geomArg := tree.MustBeDGeometry(firstArg)
	if geomArg.Empty() {
		return nil
	}
	b := geomArg.CartesianBoundingBox()
	agg.bbox = agg.bbox.WithPoint(b.LoX, b.LoY).WithPoint(b.HiX, b.HiY)
	return nil
}

// Result implements the AggregateFunc interface.
func (agg *stExtentAgg) Result() (tree.Datum, error) {
	if agg.bbox == nil {
		return tree.DNull, nil
	}
	return tree.NewDBox2D(*agg.bbox), nil
}

// Reset implements the AggregateFunc interface.
func (agg *stExtentAgg) Reset(context.Context) {
	agg.bbox = nil
}

// Close implements the AggregateFunc interface.
func (agg *stExtentAgg) Close(context.Context) {}

// Size implements the AggregateFunc interface.
func (agg *stExtentAgg) Size() int64 {
	return sizeOfSTExtentAggregate
}

func makeVarianceBuiltin() builtinDefinition {
	return makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntVarianceAggregate,
			"Calculates the variance of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalVarianceAggregate,
			"Calculates the variance of the selected values.", tree.VolatilityImmutable),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatVarianceAggregate,
			"Calculates the variance of the selected values.", tree.VolatilityImmutable),
	)
}

// builtinMustNotRun panics and indicates that a builtin cannot be run.
func builtinMustNotRun(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	panic("builtin must be overridden and cannot be run directly")
}

var _ tree.AggregateFunc = &arrayAggregate{}
var _ tree.AggregateFunc = &avgAggregate{}
var _ tree.AggregateFunc = &corrAggregate{}
var _ tree.AggregateFunc = &countAggregate{}
var _ tree.AggregateFunc = &countRowsAggregate{}
var _ tree.AggregateFunc = &maxAggregate{}
var _ tree.AggregateFunc = &minAggregate{}
var _ tree.AggregateFunc = &smallIntSumAggregate{}
var _ tree.AggregateFunc = &intSumAggregate{}
var _ tree.AggregateFunc = &decimalSumAggregate{}
var _ tree.AggregateFunc = &floatSumAggregate{}
var _ tree.AggregateFunc = &intervalSumAggregate{}
var _ tree.AggregateFunc = &intSqrDiffAggregate{}
var _ tree.AggregateFunc = &floatSqrDiffAggregate{}
var _ tree.AggregateFunc = &decimalSqrDiffAggregate{}
var _ tree.AggregateFunc = &floatSumSqrDiffsAggregate{}
var _ tree.AggregateFunc = &decimalSumSqrDiffsAggregate{}
var _ tree.AggregateFunc = &floatVarianceAggregate{}
var _ tree.AggregateFunc = &decimalVarianceAggregate{}
var _ tree.AggregateFunc = &floatStdDevAggregate{}
var _ tree.AggregateFunc = &decimalStdDevAggregate{}
var _ tree.AggregateFunc = &anyNotNullAggregate{}
var _ tree.AggregateFunc = &concatAggregate{}
var _ tree.AggregateFunc = &boolAndAggregate{}
var _ tree.AggregateFunc = &boolOrAggregate{}
var _ tree.AggregateFunc = &bytesXorAggregate{}
var _ tree.AggregateFunc = &intXorAggregate{}
var _ tree.AggregateFunc = &jsonAggregate{}
var _ tree.AggregateFunc = &intBitAndAggregate{}
var _ tree.AggregateFunc = &bitBitAndAggregate{}
var _ tree.AggregateFunc = &intBitOrAggregate{}
var _ tree.AggregateFunc = &bitBitOrAggregate{}
var _ tree.AggregateFunc = &percentileDiscAggregate{}
var _ tree.AggregateFunc = &percentileContAggregate{}
var _ tree.AggregateFunc = &stMakeLineAgg{}
var _ tree.AggregateFunc = &stUnionAgg{}
var _ tree.AggregateFunc = &stExtentAgg{}
var _ tree.AggregateFunc = &covarPopAggregate{}
var _ tree.AggregateFunc = &covarSampAggregate{}
var _ tree.AggregateFunc = &regressionInterceptAggregate{}
var _ tree.AggregateFunc = &regressionR2Aggregate{}
var _ tree.AggregateFunc = &regressionSlopeAggregate{}
var _ tree.AggregateFunc = &regressionSXXAggregate{}
var _ tree.AggregateFunc = &regressionSXYAggregate{}
var _ tree.AggregateFunc = &regressionSYYAggregate{}
var _ tree.AggregateFunc = &regressionCountAggregate{}
var _ tree.AggregateFunc = &regressionAvgXAggregate{}
var _ tree.AggregateFunc = &regressionAvgYAggregate{}

const sizeOfArrayAggregate = int64(unsafe.Sizeof(arrayAggregate{}))
const sizeOfAvgAggregate = int64(unsafe.Sizeof(avgAggregate{}))
const sizeOfRegressionAccumulatorBase = int64(unsafe.Sizeof(regressionAccumulatorBase{}))
const sizeOfCountAggregate = int64(unsafe.Sizeof(countAggregate{}))
const sizeOfRegressionCountAggregate = int64(unsafe.Sizeof(regressionCountAggregate{}))
const sizeOfCountRowsAggregate = int64(unsafe.Sizeof(countRowsAggregate{}))
const sizeOfMaxAggregate = int64(unsafe.Sizeof(maxAggregate{}))
const sizeOfMinAggregate = int64(unsafe.Sizeof(minAggregate{}))
const sizeOfSmallIntSumAggregate = int64(unsafe.Sizeof(smallIntSumAggregate{}))
const sizeOfIntSumAggregate = int64(unsafe.Sizeof(intSumAggregate{}))
const sizeOfDecimalSumAggregate = int64(unsafe.Sizeof(decimalSumAggregate{}))
const sizeOfFloatSumAggregate = int64(unsafe.Sizeof(floatSumAggregate{}))
const sizeOfIntervalSumAggregate = int64(unsafe.Sizeof(intervalSumAggregate{}))
const sizeOfIntSqrDiffAggregate = int64(unsafe.Sizeof(intSqrDiffAggregate{}))
const sizeOfFloatSqrDiffAggregate = int64(unsafe.Sizeof(floatSqrDiffAggregate{}))
const sizeOfDecimalSqrDiffAggregate = int64(unsafe.Sizeof(decimalSqrDiffAggregate{}))
const sizeOfFloatSumSqrDiffsAggregate = int64(unsafe.Sizeof(floatSumSqrDiffsAggregate{}))
const sizeOfDecimalSumSqrDiffsAggregate = int64(unsafe.Sizeof(decimalSumSqrDiffsAggregate{}))
const sizeOfFloatVarianceAggregate = int64(unsafe.Sizeof(floatVarianceAggregate{}))
const sizeOfDecimalVarianceAggregate = int64(unsafe.Sizeof(decimalVarianceAggregate{}))
const sizeOfFloatVarPopAggregate = int64(unsafe.Sizeof(floatVarPopAggregate{}))
const sizeOfDecimalVarPopAggregate = int64(unsafe.Sizeof(decimalVarPopAggregate{}))
const sizeOfFloatStdDevAggregate = int64(unsafe.Sizeof(floatStdDevAggregate{}))
const sizeOfDecimalStdDevAggregate = int64(unsafe.Sizeof(decimalStdDevAggregate{}))
const sizeOfAnyNotNullAggregate = int64(unsafe.Sizeof(anyNotNullAggregate{}))
const sizeOfConcatAggregate = int64(unsafe.Sizeof(concatAggregate{}))
const sizeOfBoolAndAggregate = int64(unsafe.Sizeof(boolAndAggregate{}))
const sizeOfBoolOrAggregate = int64(unsafe.Sizeof(boolOrAggregate{}))
const sizeOfBytesXorAggregate = int64(unsafe.Sizeof(bytesXorAggregate{}))
const sizeOfIntXorAggregate = int64(unsafe.Sizeof(intXorAggregate{}))
const sizeOfJSONAggregate = int64(unsafe.Sizeof(jsonAggregate{}))
const sizeOfJSONObjectAggregate = int64(unsafe.Sizeof(jsonObjectAggregate{}))
const sizeOfIntBitAndAggregate = int64(unsafe.Sizeof(intBitAndAggregate{}))
const sizeOfBitBitAndAggregate = int64(unsafe.Sizeof(bitBitAndAggregate{}))
const sizeOfIntBitOrAggregate = int64(unsafe.Sizeof(intBitOrAggregate{}))
const sizeOfBitBitOrAggregate = int64(unsafe.Sizeof(bitBitOrAggregate{}))
const sizeOfPercentileDiscAggregate = int64(unsafe.Sizeof(percentileDiscAggregate{}))
const sizeOfPercentileContAggregate = int64(unsafe.Sizeof(percentileContAggregate{}))
const sizeOfSTMakeLineAggregate = int64(unsafe.Sizeof(stMakeLineAgg{}))
const sizeOfSTUnionAggregate = int64(unsafe.Sizeof(stUnionAgg{}))
const sizeOfSTCollectAggregate = int64(unsafe.Sizeof(stCollectAgg{}))
const sizeOfSTExtentAggregate = int64(unsafe.Sizeof(stExtentAgg{}))

// singleDatumAggregateBase is a utility struct that helps aggregate builtins
// that store a single datum internally track their memory usage related to
// that single datum.
// It will reuse tree.EvalCtx.SingleDatumAggMemAccount when non-nil and will
// *not* close that account upon its closure; if it is nil, then a new memory
// account will be created specifically for this struct and that account will
// be closed upon this struct's closure.
type singleDatumAggregateBase struct {
	mode singleDatumAggregateBaseMode
	acc  *mon.BoundAccount
	// accountedFor indicates how much memory (in bytes) have been registered
	// with acc.
	accountedFor int64
}

// singleDatumAggregateBaseMode indicates the mode in which
// singleDatumAggregateBase operates with regards to resetting and closing
// behaviors.
type singleDatumAggregateBaseMode int

const (
	// sharedSingleDatumAggregateBaseMode is a mode in which the memory account
	// will be grown and shrunk according the corresponding aggregate builtin's
	// memory usage, but the account will never be cleared or closed. In this
	// mode, singleDatumAggregateBaseMode is *not* responsible for closing the
	// memory account.
	sharedSingleDatumAggregateBaseMode singleDatumAggregateBaseMode = iota
	// nonSharedSingleDatumAggregateBaseMode is a mode in which the memory
	// account is "owned" by singleDatumAggregateBase, so the account can be
	// cleared and closed by it. In fact, singleDatumAggregateBase is
	// responsible for the account's closure.
	nonSharedSingleDatumAggregateBaseMode
)

// makeSingleDatumAggregateBase makes a new singleDatumAggregateBase. If
// evalCtx has non-nil SingleDatumAggMemAccount field, then that memory account
// will be used by the new struct which will operate in "shared" mode
func makeSingleDatumAggregateBase(evalCtx *tree.EvalContext) singleDatumAggregateBase {
	if evalCtx.SingleDatumAggMemAccount == nil {
		newAcc := evalCtx.Mon.MakeBoundAccount()
		return singleDatumAggregateBase{
			mode: nonSharedSingleDatumAggregateBaseMode,
			acc:  &newAcc,
		}
	}
	return singleDatumAggregateBase{
		mode: sharedSingleDatumAggregateBaseMode,
		acc:  evalCtx.SingleDatumAggMemAccount,
	}
}

// updateMemoryUsage updates the memory account to reflect the new memory
// usage. If any memory has been previously registered with this struct, then
// the account is updated only by the delta between previous and new usages,
// otherwise, it is grown by newUsage.
func (b *singleDatumAggregateBase) updateMemoryUsage(ctx context.Context, newUsage int64) error {
	if err := b.acc.Grow(ctx, newUsage-b.accountedFor); err != nil {
		return err
	}
	b.accountedFor = newUsage
	return nil
}

func (b *singleDatumAggregateBase) reset(ctx context.Context) {
	switch b.mode {
	case sharedSingleDatumAggregateBaseMode:
		b.acc.Shrink(ctx, b.accountedFor)
		b.accountedFor = 0
	case nonSharedSingleDatumAggregateBaseMode:
		b.acc.Clear(ctx)
	default:
		panic(errors.Errorf("unexpected singleDatumAggregateBaseMode: %d", b.mode))
	}
}

func (b *singleDatumAggregateBase) close(ctx context.Context) {
	switch b.mode {
	case sharedSingleDatumAggregateBaseMode:
		b.acc.Shrink(ctx, b.accountedFor)
		b.accountedFor = 0
	case nonSharedSingleDatumAggregateBaseMode:
		b.acc.Close(ctx)
	default:
		panic(errors.Errorf("unexpected singleDatumAggregateBaseMode: %d", b.mode))
	}
}

// See NewAnyNotNullAggregate.
type anyNotNullAggregate struct {
	singleDatumAggregateBase

	val tree.Datum
}

// NewAnyNotNullAggregate returns an aggregate function that returns an
// arbitrary not-NULL value passed to Add (or NULL if no such value). This is
// particularly useful for "passing through" values for columns which we know
// are constant within any aggregation group (for example, the grouping columns
// themselves).
//
// Note that NULL values do not affect the result of the aggregation; this is
// important in a few different contexts:
//
//  - in distributed multi-stage aggregations, we can have a local stage with
//    multiple (parallel) instances feeding into a final stage. If some of the
//    instances see no rows, they emit a NULL into the final stage which needs
//    to be ignored.
//
//  - for query optimization, when moving aggregations across left joins (which
//    add NULL values).
func NewAnyNotNullAggregate(evalCtx *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &anyNotNullAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		val:                      tree.DNull,
	}
}

func newAnyNotNullAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, datums tree.Datums,
) tree.AggregateFunc {
	return NewAnyNotNullAggregate(evalCtx, datums)
}

// Add sets the value to the passed datum.
func (a *anyNotNullAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if a.val == tree.DNull && datum != tree.DNull {
		a.val = datum
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the value most recently passed to Add.
func (a *anyNotNullAggregate) Result() (tree.Datum, error) {
	return a.val, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *anyNotNullAggregate) Reset(ctx context.Context) {
	a.val = tree.DNull
	a.reset(ctx)
}

// Close is no-op in aggregates using constant space.
func (a *anyNotNullAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *anyNotNullAggregate) Size() int64 {
	return sizeOfAnyNotNullAggregate
}

type arrayAggregate struct {
	arr *tree.DArray
	// Note that we do not embed singleDatumAggregateBase struct to help with
	// memory accounting because arrayAggregate stores multiple datums inside
	// of arr.
	acc mon.BoundAccount
}

func newArrayAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &arrayAggregate{
		arr: tree.NewDArray(params[0]),
		acc: evalCtx.Mon.MakeBoundAccount(),
	}
}

// Add accumulates the passed datum into the array.
func (a *arrayAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if err := a.acc.Grow(ctx, int64(datum.Size())); err != nil {
		return err
	}
	return a.arr.Append(datum)
}

// Result returns a copy of the array of all datums passed to Add.
func (a *arrayAggregate) Result() (tree.Datum, error) {
	if len(a.arr.Array) > 0 {
		arrCopy := *a.arr
		return &arrCopy, nil
	}
	return tree.DNull, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *arrayAggregate) Reset(ctx context.Context) {
	a.arr = tree.NewDArray(a.arr.ParamTyp)
	a.acc.Empty(ctx)
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *arrayAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *arrayAggregate) Size() int64 {
	return sizeOfArrayAggregate
}

type avgAggregate struct {
	agg   tree.AggregateFunc
	count int
}

func newIntAvgAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &avgAggregate{agg: newIntSumAggregate(params, evalCtx, arguments)}
}
func newFloatAvgAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &avgAggregate{agg: newFloatSumAggregate(params, evalCtx, arguments)}
}
func newDecimalAvgAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &avgAggregate{agg: newDecimalSumAggregate(params, evalCtx, arguments)}
}
func newIntervalAvgAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &avgAggregate{agg: newIntervalSumAggregate(params, evalCtx, arguments)}
}

// Add accumulates the passed datum into the average.
func (a *avgAggregate) Add(ctx context.Context, datum tree.Datum, other ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if err := a.agg.Add(ctx, datum); err != nil {
		return err
	}
	a.count++
	return nil
}

// Result returns the average of all datums passed to Add.
func (a *avgAggregate) Result() (tree.Datum, error) {
	sum, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if sum == tree.DNull {
		return sum, nil
	}
	switch t := sum.(type) {
	case *tree.DFloat:
		return tree.NewDFloat(*t / tree.DFloat(a.count)), nil
	case *tree.DDecimal:
		count := apd.New(int64(a.count), 0)
		_, err := tree.DecimalCtx.Quo(&t.Decimal, &t.Decimal, count)
		return t, err
	case *tree.DInterval:
		return &tree.DInterval{Duration: t.Duration.Div(int64(a.count))}, nil
	default:
		return nil, errors.AssertionFailedf("unexpected SUM result type: %s", t)
	}
}

// Reset implements tree.AggregateFunc interface.
func (a *avgAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
	a.count = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *avgAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *avgAggregate) Size() int64 {
	return sizeOfAvgAggregate
}

type concatAggregate struct {
	singleDatumAggregateBase

	forBytes   bool
	sawNonNull bool
	delimiter  string // used for non window functions
	result     bytes.Buffer
}

func newBytesConcatAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	concatAgg := &concatAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		forBytes:                 true,
	}
	if len(arguments) == 1 && arguments[0] != tree.DNull {
		concatAgg.delimiter = string(tree.MustBeDBytes(arguments[0]))
	} else if len(arguments) > 1 {
		panic(errors.AssertionFailedf("too many arguments passed in, expected < 2, got %d", len(arguments)))
	}
	return concatAgg
}

func newStringConcatAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	concatAgg := &concatAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
	}
	if len(arguments) == 1 && arguments[0] != tree.DNull {
		concatAgg.delimiter = string(tree.MustBeDString(arguments[0]))
	} else if len(arguments) > 1 {
		panic(errors.AssertionFailedf("too many arguments passed in, expected < 2, got %d", len(arguments)))
	}
	return concatAgg
}

func (a *concatAggregate) Add(ctx context.Context, datum tree.Datum, others ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if !a.sawNonNull {
		a.sawNonNull = true
	} else {
		delimiter := a.delimiter
		// If this is called as part of a window function, the delimiter is passed in
		// via the first element in others.
		if len(others) == 1 && others[0] != tree.DNull {
			if a.forBytes {
				delimiter = string(tree.MustBeDBytes(others[0]))
			} else {
				delimiter = string(tree.MustBeDString(others[0]))
			}
		} else if len(others) > 1 {
			panic(errors.AssertionFailedf("too many other datums passed in, expected < 2, got %d", len(others)))
		}
		if len(delimiter) > 0 {
			a.result.WriteString(delimiter)
		}
	}
	var arg string
	if a.forBytes {
		arg = string(tree.MustBeDBytes(datum))
	} else {
		arg = string(tree.MustBeDString(datum))
	}
	a.result.WriteString(arg)
	if err := a.updateMemoryUsage(ctx, int64(a.result.Cap())); err != nil {
		return err
	}
	return nil
}

func (a *concatAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	if a.forBytes {
		res := tree.DBytes(a.result.String())
		return &res, nil
	}
	res := tree.DString(a.result.String())
	return &res, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *concatAggregate) Reset(ctx context.Context) {
	a.sawNonNull = false
	a.result.Reset()
	// Note that a.result.Reset() does *not* release already allocated memory,
	// so we do not reset singleDatumAggregateBase.
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *concatAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *concatAggregate) Size() int64 {
	return sizeOfConcatAggregate
}

type intBitAndAggregate struct {
	sawNonNull bool
	result     int64
}

func newIntBitAndAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &intBitAndAggregate{}
}

// Add inserts one value into the running bitwise AND.
func (a *intBitAndAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if !a.sawNonNull {
		// This is the first non-null datum, so we simply store
		// the provided value for the aggregation.
		a.result = int64(tree.MustBeDInt(datum))
		a.sawNonNull = true
		return nil
	}
	// This is not the first non-null datum, so we actually AND it with the
	// aggregate so far.
	a.result = a.result & int64(tree.MustBeDInt(datum))
	return nil
}

// Result returns the bitwise AND.
func (a *intBitAndAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDInt(tree.DInt(a.result)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *intBitAndAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *intBitAndAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *intBitAndAggregate) Size() int64 {
	return sizeOfIntBitAndAggregate
}

type bitBitAndAggregate struct {
	sawNonNull bool
	result     bitarray.BitArray
}

func newBitBitAndAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &bitBitAndAggregate{}
}

// Add inserts one value into the running bitwise AND.
func (a *bitBitAndAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	bits := &tree.MustBeDBitArray(datum).BitArray
	if !a.sawNonNull {
		// This is the first non-null datum, so we simply store
		// the provided value for the aggregation.
		a.result = *bits
		a.sawNonNull = true
		return nil
	}
	// If the length of the current bit array is different from that of the
	// stored value, we return an error.
	if a.result.BitLen() != bits.BitLen() {
		return tree.NewCannotMixBitArraySizesError("AND")
	}
	// This is not the first non-null datum, so we actually AND it with the
	// aggregate so far.
	a.result = bitarray.And(a.result, *bits)
	return nil
}

// Result returns the bitwise AND.
func (a *bitBitAndAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return &tree.DBitArray{BitArray: a.result}, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *bitBitAndAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result = bitarray.BitArray{}
}

// Close is part of the tree.AggregateFunc interface.
func (a *bitBitAndAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *bitBitAndAggregate) Size() int64 {
	return sizeOfBitBitAndAggregate
}

type intBitOrAggregate struct {
	sawNonNull bool
	result     int64
}

func newIntBitOrAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &intBitOrAggregate{}
}

// Add inserts one value into the running bitwise OR.
func (a *intBitOrAggregate) Add(
	_ context.Context, datum tree.Datum, otherArgs ...tree.Datum,
) error {
	if datum == tree.DNull {
		return nil
	}
	if !a.sawNonNull {
		// This is the first non-null datum, so we simply store
		// the provided value for the aggregation.
		a.result = int64(tree.MustBeDInt(datum))
		a.sawNonNull = true
		return nil
	}
	// This is not the first non-null datum, so we actually OR it with the
	// aggregate so far.
	a.result = a.result | int64(tree.MustBeDInt(datum))
	return nil
}

// Result returns the bitwise OR.
func (a *intBitOrAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDInt(tree.DInt(a.result)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *intBitOrAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *intBitOrAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *intBitOrAggregate) Size() int64 {
	return sizeOfIntBitOrAggregate
}

type bitBitOrAggregate struct {
	sawNonNull bool
	result     bitarray.BitArray
}

func newBitBitOrAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &bitBitOrAggregate{}
}

// Add inserts one value into the running bitwise OR.
func (a *bitBitOrAggregate) Add(
	_ context.Context, datum tree.Datum, otherArgs ...tree.Datum,
) error {
	if datum == tree.DNull {
		return nil
	}
	bits := &tree.MustBeDBitArray(datum).BitArray
	if !a.sawNonNull {
		// This is the first non-null datum, so we simply store
		// the provided value for the aggregation.
		a.result = *bits
		a.sawNonNull = true
		return nil
	}
	// If the length of the current bit array is different from that of the
	// stored value, we return an error.
	if a.result.BitLen() != bits.BitLen() {
		return tree.NewCannotMixBitArraySizesError("OR")
	}
	// This is not the first non-null datum, so we actually OR it with the
	// aggregate so far.
	a.result = bitarray.Or(a.result, *bits)
	return nil
}

// Result returns the bitwise OR.
func (a *bitBitOrAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return &tree.DBitArray{BitArray: a.result}, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *bitBitOrAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result = bitarray.BitArray{}
}

// Close is part of the tree.AggregateFunc interface.
func (a *bitBitOrAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *bitBitOrAggregate) Size() int64 {
	return sizeOfBitBitOrAggregate
}

type boolAndAggregate struct {
	sawNonNull bool
	result     bool
}

func newBoolAndAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &boolAndAggregate{}
}

func (a *boolAndAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if !a.sawNonNull {
		a.sawNonNull = true
		a.result = true
	}
	a.result = a.result && bool(*datum.(*tree.DBool))
	return nil
}

func (a *boolAndAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.MakeDBool(tree.DBool(a.result)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *boolAndAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *boolAndAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *boolAndAggregate) Size() int64 {
	return sizeOfBoolAndAggregate
}

type boolOrAggregate struct {
	sawNonNull bool
	result     bool
}

func newBoolOrAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &boolOrAggregate{}
}

func (a *boolOrAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	a.sawNonNull = true
	a.result = a.result || bool(*datum.(*tree.DBool))
	return nil
}

func (a *boolOrAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.MakeDBool(tree.DBool(a.result)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *boolOrAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *boolOrAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *boolOrAggregate) Size() int64 {
	return sizeOfBoolOrAggregate
}

// regressionAccumulatorBase is a base struct for the aggregate functions
// for statistics. It represents a transition datatype for these functions.
// Ported from Postgresql (see https://github.com/postgres/postgres/blob/bc1fbc960bf5efbb692f4d1bf91bf9bc6390425a/src/backend/utils/adt/float.c#L3277).
//
// The Youngs-Cramer algorithm is used to reduce rounding errors
// in the aggregate final functions.
//
// Note that Y is the first argument to all these aggregates!
//
// It might seem attractive to optimize this by having multiple accumulator
// functions that only calculate the sums actually needed.  But on most
// modern machines, a couple of extra floating-point multiplies will be
// insignificant compared to the other per-tuple overhead, so I've chosen
// to minimize code space instead.
type regressionAccumulatorBase struct {
	n   float64
	sx  float64
	sxx float64
	sy  float64
	syy float64
	sxy float64
}

// Add implements tree.AggregateFunc interface.
func (a *regressionAccumulatorBase) Add(
	_ context.Context, datumY tree.Datum, otherArgs ...tree.Datum,
) error {
	if datumY == tree.DNull {
		return nil
	}

	datumX := otherArgs[0]
	if datumX == tree.DNull {
		return nil
	}

	x, err := a.float64Val(datumX)
	if err != nil {
		return err
	}

	y, err := a.float64Val(datumY)
	if err != nil {
		return err
	}

	return a.add(y, x)
}

// Reset implements tree.AggregateFunc interface.
func (a *regressionAccumulatorBase) Reset(context.Context) {
	*a = regressionAccumulatorBase{}
}

// Close implements tree.AggregateFunc interface.
func (a *regressionAccumulatorBase) Close(context.Context) {}

// Size implements tree.AggregateFunc interface.
func (a *regressionAccumulatorBase) Size() int64 {
	return sizeOfRegressionAccumulatorBase
}

func (a *regressionAccumulatorBase) add(y float64, x float64) error {
	n := a.n
	sx := a.sx
	sxx := a.sxx
	sy := a.sy
	syy := a.syy
	sxy := a.sxy

	// Use the Youngs-Cramer algorithm to incorporate the new values into the
	// transition values.
	n++
	sx += x
	sy += y

	if a.n > 0 {
		tmpX := x*n - sx
		tmpY := y*n - sy
		scale := 1.0 / (n * a.n)
		sxx += tmpX * tmpX * scale
		syy += tmpY * tmpY * scale
		sxy += tmpX * tmpY * scale

		// Overflow check.  We only report an overflow error when finite
		// inputs lead to infinite results.  Note also that sxx, syy and Sxy
		// should be NaN if any of the relevant inputs are infinite, so we
		// intentionally prevent them from becoming infinite.
		if math.IsInf(sx, 0) || math.IsInf(sxx, 0) || math.IsInf(sy, 0) || math.IsInf(syy, 0) || math.IsInf(sxy, 0) {
			if ((math.IsInf(sx, 0) || math.IsInf(sxx, 0)) &&
				!math.IsInf(a.sx, 0) && !math.IsInf(x, 0)) ||
				((math.IsInf(sy, 0) || math.IsInf(syy, 0)) &&
					!math.IsInf(a.sy, 0) && !math.IsInf(y, 0)) ||
				(math.IsInf(sxy, 0) &&
					!math.IsInf(a.sx, 0) && !math.IsInf(x, 0) &&
					!math.IsInf(a.sy, 0) && !math.IsInf(y, 0)) {
				return tree.ErrFloatOutOfRange
			}

			if math.IsInf(sxx, 0) {
				sxx = math.NaN()
			}
			if math.IsInf(syy, 0) {
				syy = math.NaN()
			}
			if math.IsInf(sxy, 0) {
				sxy = math.NaN()
			}
		}
	} else {
		// At the first input, we normally can leave Sxx et al as 0.  However,
		// if the first input is Inf or NaN, we'd better force the dependent
		// sums to NaN; otherwise we will falsely report variance zero when
		// there are no more inputs.
		if math.IsNaN(x) || math.IsInf(x, 0) {
			sxx = math.NaN()
			sxy = math.NaN()
		}
		if math.IsNaN(y) || math.IsInf(y, 0) {
			syy = math.NaN()
			sxy = math.NaN()
		}
	}

	a.n = n
	a.sx = sx
	a.sy = sy
	a.sxx = sxx
	a.syy = syy
	a.sxy = sxy

	return nil
}

func (a *regressionAccumulatorBase) float64Val(datum tree.Datum) (float64, error) {
	switch val := datum.(type) {
	case *tree.DFloat:
		return float64(*val), nil
	case *tree.DInt:
		return float64(*val), nil
	case *tree.DDecimal:
		return val.Decimal.Float64()
	default:
		return 0, fmt.Errorf("invalid type %T (%v)", val, val)
	}
}

// corrAggregate represents SQL:2003 correlation coefficient.
type corrAggregate struct {
	regressionAccumulatorBase
}

func newCorrAggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &corrAggregate{}
}

// Result implements tree.AggregateFunc interface.
func (a *corrAggregate) Result() (tree.Datum, error) {
	if a.n < 1 {
		return tree.DNull, nil
	}

	if a.sxx == 0 || a.syy == 0 {
		return tree.DNull, nil
	}
	return tree.NewDFloat(tree.DFloat(a.sxy / math.Sqrt(a.sxx*a.syy))), nil
}

// covarPopAggregate represents population covariance.
type covarPopAggregate struct {
	regressionAccumulatorBase
}

func newCovarPopAggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &covarPopAggregate{}
}

// Result implements tree.AggregateFunc interface.
func (a *covarPopAggregate) Result() (tree.Datum, error) {
	if a.n < 1 {
		return tree.DNull, nil
	}

	return tree.NewDFloat(tree.DFloat(a.sxy / a.n)), nil
}

// covarSampAggregate represents sample covariance.
type covarSampAggregate struct {
	regressionAccumulatorBase
}

func newCovarSampAggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &covarSampAggregate{}
}

// Result implements tree.AggregateFunc interface.
func (a *covarSampAggregate) Result() (tree.Datum, error) {
	if a.n < 2 {
		return tree.DNull, nil
	}

	return tree.NewDFloat(tree.DFloat(a.sxy / (a.n - 1))), nil
}

// regressionAvgXAggregate represents SQL:2003 average of the independent
// variable (sum(X)/N).
type regressionAvgXAggregate struct {
	regressionAccumulatorBase
}

func newRegressionAvgXAggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &regressionAvgXAggregate{}
}

// Result implements tree.AggregateFunc interface.
func (a *regressionAvgXAggregate) Result() (tree.Datum, error) {
	if a.n < 1 {
		return tree.DNull, nil
	}

	return tree.NewDFloat(tree.DFloat(a.sx / a.n)), nil
}

// regressionAvgYAggregate represents SQL:2003 average of the dependent
// variable (sum(Y)/N).
type regressionAvgYAggregate struct {
	regressionAccumulatorBase
}

func newRegressionAvgYAggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &regressionAvgYAggregate{}
}

// Result implements tree.AggregateFunc interface.
func (a *regressionAvgYAggregate) Result() (tree.Datum, error) {
	if a.n < 1 {
		return tree.DNull, nil
	}

	return tree.NewDFloat(tree.DFloat(a.sy / a.n)), nil
}

// regressionInterceptAggregate represents y-intercept.
type regressionInterceptAggregate struct {
	regressionAccumulatorBase
}

func newRegressionInterceptAggregate(
	[]*types.T, *tree.EvalContext, tree.Datums,
) tree.AggregateFunc {
	return &regressionInterceptAggregate{}
}

// Result implements tree.AggregateFunc interface.
func (a *regressionInterceptAggregate) Result() (tree.Datum, error) {
	if a.n < 1 {
		return tree.DNull, nil
	}
	if a.sxx == 0 {
		return tree.DNull, nil
	}

	return tree.NewDFloat(tree.DFloat((a.sy - a.sx*a.sxy/a.sxx) / a.n)), nil
}

// regressionR2Aggregate represents square of the correlation coefficient.
type regressionR2Aggregate struct {
	regressionAccumulatorBase
}

func newRegressionR2Aggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &regressionR2Aggregate{}
}

// Result implements tree.AggregateFunc interface.
func (a *regressionR2Aggregate) Result() (tree.Datum, error) {
	if a.n < 1 {
		return tree.DNull, nil
	}
	if a.sxx == 0 {
		return tree.DNull, nil
	}
	if a.syy == 0 {
		return tree.NewDFloat(tree.DFloat(1.0)), nil
	}

	return tree.NewDFloat(tree.DFloat((a.sxy * a.sxy) / (a.sxx * a.syy))), nil
}

// regressionSlopeAggregate represents slope of the least-squares-fit linear
// equation determined by the (X, Y) pairs.
type regressionSlopeAggregate struct {
	regressionAccumulatorBase
}

func newRegressionSlopeAggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &regressionSlopeAggregate{}
}

// Result implements tree.AggregateFunc interface.
func (a *regressionSlopeAggregate) Result() (tree.Datum, error) {
	if a.n < 1 {
		return tree.DNull, nil
	}
	if a.sxx == 0 {
		return tree.DNull, nil
	}

	return tree.NewDFloat(tree.DFloat(a.sxy / a.sxx)), nil
}

// regressionSXXAggregate represents sum of squares of the independent variable.
type regressionSXXAggregate struct {
	regressionAccumulatorBase
}

func newRegressionSXXAggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &regressionSXXAggregate{}
}

// Result implements tree.AggregateFunc interface.
func (a *regressionSXXAggregate) Result() (tree.Datum, error) {
	if a.n < 1 {
		return tree.DNull, nil
	}

	return tree.NewDFloat(tree.DFloat(a.sxx)), nil
}

// regressionSXYAggregate represents sum of products of independent
// times dependent variable.
type regressionSXYAggregate struct {
	regressionAccumulatorBase
}

func newRegressionSXYAggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &regressionSXYAggregate{}
}

// Result implements tree.AggregateFunc interface.
func (a *regressionSXYAggregate) Result() (tree.Datum, error) {
	if a.n < 1 {
		return tree.DNull, nil
	}

	return tree.NewDFloat(tree.DFloat(a.sxy)), nil
}

// regressionSYYAggregate represents sum of squares of the dependent variable.
type regressionSYYAggregate struct {
	regressionAccumulatorBase
}

func newRegressionSYYAggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &regressionSYYAggregate{}
}

// Result implements tree.AggregateFunc interface.
func (a *regressionSYYAggregate) Result() (tree.Datum, error) {
	if a.n < 1 {
		return tree.DNull, nil
	}

	return tree.NewDFloat(tree.DFloat(a.syy)), nil
}

// regressionCountAggregate calculates number of input rows in which both
// expressions are nonnull.
type regressionCountAggregate struct {
	count int
}

func newRegressionCountAggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &regressionCountAggregate{}
}

func (a *regressionCountAggregate) Add(
	_ context.Context, datumY tree.Datum, otherArgs ...tree.Datum,
) error {
	if datumY == tree.DNull {
		return nil
	}

	datumX := otherArgs[0]
	if datumX == tree.DNull {
		return nil
	}

	a.count++
	return nil
}

// Result implements tree.AggregateFunc interface.
func (a *regressionCountAggregate) Result() (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(a.count)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *regressionCountAggregate) Reset(context.Context) {
	a.count = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *regressionCountAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *regressionCountAggregate) Size() int64 {
	return sizeOfRegressionCountAggregate
}

type countAggregate struct {
	count int
}

func newCountAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &countAggregate{}
}

func (a *countAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	a.count++
	return nil
}

func (a *countAggregate) Result() (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(a.count)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *countAggregate) Reset(context.Context) {
	a.count = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *countAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *countAggregate) Size() int64 {
	return sizeOfCountAggregate
}

type countRowsAggregate struct {
	count int
}

func newCountRowsAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &countRowsAggregate{}
}

func (a *countRowsAggregate) Add(_ context.Context, _ tree.Datum, _ ...tree.Datum) error {
	a.count++
	return nil
}

func (a *countRowsAggregate) Result() (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(a.count)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *countRowsAggregate) Reset(context.Context) {
	a.count = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *countRowsAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *countRowsAggregate) Size() int64 {
	return sizeOfCountRowsAggregate
}

// maxAggregate keeps track of the largest value passed to Add.
type maxAggregate struct {
	singleDatumAggregateBase

	max               tree.Datum
	evalCtx           *tree.EvalContext
	variableDatumSize bool
}

func newMaxAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "max" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	return &maxAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
	}
}

// Add sets the max to the larger of the current max or the passed datum.
func (a *maxAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if a.max == nil {
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
		a.max = datum
		return nil
	}
	c := a.max.Compare(a.evalCtx, datum)
	if c < 0 {
		a.max = datum
		if a.variableDatumSize {
			if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Result returns the largest value passed to Add.
func (a *maxAggregate) Result() (tree.Datum, error) {
	if a.max == nil {
		return tree.DNull, nil
	}
	return a.max, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *maxAggregate) Reset(ctx context.Context) {
	a.max = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *maxAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *maxAggregate) Size() int64 {
	return sizeOfMaxAggregate
}

// minAggregate keeps track of the smallest value passed to Add.
type minAggregate struct {
	singleDatumAggregateBase

	min               tree.Datum
	evalCtx           *tree.EvalContext
	variableDatumSize bool
}

func newMinAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "min" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	return &minAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
	}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *minAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if a.min == nil {
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
		a.min = datum
		return nil
	}
	c := a.min.Compare(a.evalCtx, datum)
	if c > 0 {
		a.min = datum
		if a.variableDatumSize {
			if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *minAggregate) Result() (tree.Datum, error) {
	if a.min == nil {
		return tree.DNull, nil
	}
	return a.min, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *minAggregate) Reset(ctx context.Context) {
	a.min = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *minAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *minAggregate) Size() int64 {
	return sizeOfMinAggregate
}

type smallIntSumAggregate struct {
	sum         int64
	seenNonNull bool
}

func newSmallIntSumAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &smallIntSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *smallIntSumAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}

	var ok bool
	a.sum, ok = arith.AddWithOverflow(a.sum, int64(tree.MustBeDInt(datum)))
	if !ok {
		return tree.ErrIntOutOfRange
	}
	a.seenNonNull = true
	return nil
}

// Result returns the sum.
func (a *smallIntSumAggregate) Result() (tree.Datum, error) {
	if !a.seenNonNull {
		return tree.DNull, nil
	}
	return tree.NewDInt(tree.DInt(a.sum)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *smallIntSumAggregate) Reset(context.Context) {
	a.sum = 0
	a.seenNonNull = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *smallIntSumAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *smallIntSumAggregate) Size() int64 {
	return sizeOfSmallIntSumAggregate
}

type intSumAggregate struct {
	singleDatumAggregateBase

	// Either the `intSum` and `decSum` fields contains the
	// result. Which one is used is determined by the `large` field
	// below.
	intSum      int64
	decSum      apd.Decimal
	tmpDec      apd.Decimal
	large       bool
	seenNonNull bool
}

func newIntSumAggregate(_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &intSumAggregate{singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx)}
}

// Add adds the value of the passed datum to the sum.
func (a *intSumAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}

	t := int64(tree.MustBeDInt(datum))
	if t != 0 {
		// The sum can be computed using a single int64 as long as the
		// result of the addition does not overflow.  However since Go
		// does not provide checked addition, we have to check for the
		// overflow explicitly.
		if !a.large {
			r, ok := arith.AddWithOverflow(a.intSum, t)
			if ok {
				a.intSum = r
			} else {
				// And overflow was detected; go to large integers, but keep the
				// sum computed so far.
				a.large = true
				a.decSum.SetInt64(a.intSum)
			}
		}

		if a.large {
			a.tmpDec.SetInt64(t)
			_, err := tree.ExactCtx.Add(&a.decSum, &a.decSum, &a.tmpDec)
			if err != nil {
				return err
			}
			if err := a.updateMemoryUsage(ctx, int64(tree.SizeOfDecimal(&a.decSum))); err != nil {
				return err
			}
		}
	}
	a.seenNonNull = true
	return nil
}

// Result returns the sum.
func (a *intSumAggregate) Result() (tree.Datum, error) {
	if !a.seenNonNull {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{}
	if a.large {
		dd.Set(&a.decSum)
	} else {
		dd.SetInt64(a.intSum)
	}
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *intSumAggregate) Reset(context.Context) {
	// We choose not to reset apd.Decimal's since they will be set to
	// appropriate values when overflow occurs - we simply force the aggregate
	// to use Go types (at least, at first). That's why we also not reset the
	// singleDatumAggregateBase.
	a.seenNonNull = false
	a.intSum = 0
	a.large = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *intSumAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *intSumAggregate) Size() int64 {
	return sizeOfIntSumAggregate
}

type decimalSumAggregate struct {
	singleDatumAggregateBase

	sum        apd.Decimal
	sawNonNull bool
}

func newDecimalSumAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalSumAggregate{singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx)}
}

// Add adds the value of the passed datum to the sum.
func (a *decimalSumAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := datum.(*tree.DDecimal)
	_, err := tree.ExactCtx.Add(&a.sum, &a.sum, &t.Decimal)
	if err != nil {
		return err
	}

	if err := a.updateMemoryUsage(ctx, int64(tree.SizeOfDecimal(&a.sum))); err != nil {
		return err
	}

	a.sawNonNull = true
	return nil
}

// Result returns the sum.
func (a *decimalSumAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{}
	dd.Set(&a.sum)
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *decimalSumAggregate) Reset(ctx context.Context) {
	a.sum.SetInt64(0)
	a.sawNonNull = false
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalSumAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *decimalSumAggregate) Size() int64 {
	return sizeOfDecimalSumAggregate
}

type floatSumAggregate struct {
	sum        float64
	sawNonNull bool
}

func newFloatSumAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &floatSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *floatSumAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := datum.(*tree.DFloat)
	a.sum += float64(*t)
	a.sawNonNull = true
	return nil
}

// Result returns the sum.
func (a *floatSumAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDFloat(tree.DFloat(a.sum)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *floatSumAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.sum = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *floatSumAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *floatSumAggregate) Size() int64 {
	return sizeOfFloatSumAggregate
}

type intervalSumAggregate struct {
	sum        duration.Duration
	sawNonNull bool
}

func newIntervalSumAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &intervalSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *intervalSumAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := datum.(*tree.DInterval).Duration
	a.sum = a.sum.Add(t)
	a.sawNonNull = true
	return nil
}

// Result returns the sum.
func (a *intervalSumAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return &tree.DInterval{Duration: a.sum}, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *intervalSumAggregate) Reset(context.Context) {
	a.sum = a.sum.Sub(a.sum)
	a.sawNonNull = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *intervalSumAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *intervalSumAggregate) Size() int64 {
	return sizeOfIntervalSumAggregate
}

// Read-only constants used for square difference computations.
var (
	decimalOne = apd.New(1, 0)
	decimalTwo = apd.New(2, 0)
)

type intSqrDiffAggregate struct {
	agg decimalSqrDiff
	// Used for passing int64s as *apd.Decimal values.
	tmpDec tree.DDecimal
}

func newIntSqrDiff(evalCtx *tree.EvalContext) decimalSqrDiff {
	return &intSqrDiffAggregate{agg: newDecimalSqrDiff(evalCtx)}
}

func newIntSqrDiffAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return newIntSqrDiff(evalCtx)
}

// Count is part of the decimalSqrDiff interface.
func (a *intSqrDiffAggregate) Count() *apd.Decimal {
	return a.agg.Count()
}

// Tmp is part of the decimalSqrDiff interface.
func (a *intSqrDiffAggregate) Tmp() *apd.Decimal {
	return a.agg.Tmp()
}

func (a *intSqrDiffAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}

	a.tmpDec.SetInt64(int64(tree.MustBeDInt(datum)))
	return a.agg.Add(ctx, &a.tmpDec)
}

func (a *intSqrDiffAggregate) Result() (tree.Datum, error) {
	return a.agg.Result()
}

// Reset implements tree.AggregateFunc interface.
func (a *intSqrDiffAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *intSqrDiffAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *intSqrDiffAggregate) Size() int64 {
	return sizeOfIntSqrDiffAggregate
}

type floatSqrDiffAggregate struct {
	count   int64
	mean    float64
	sqrDiff float64
}

func newFloatSqrDiff() floatSqrDiff {
	return &floatSqrDiffAggregate{}
}

func newFloatSqrDiffAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return newFloatSqrDiff()
}

// Count is part of the floatSqrDiff interface.
func (a *floatSqrDiffAggregate) Count() int64 {
	return a.count
}

func (a *floatSqrDiffAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	f := float64(*datum.(*tree.DFloat))

	// Uses the Knuth/Welford method for accurately computing squared difference online in a
	// single pass. Refer to squared difference calculations
	// in http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.count++
	delta := f - a.mean
	// We are converting an int64 number (with 63-bit precision)
	// to a float64 (with 52-bit precision), thus in the worst cases,
	// we may lose up to 11 bits of precision. This was deemed acceptable
	// considering that we are losing 11 bits on a 52+-bit operation and
	// that users dealing with floating points should be aware
	// of floating-point imprecision.
	a.mean += delta / float64(a.count)
	a.sqrDiff += delta * (f - a.mean)
	return nil
}

func (a *floatSqrDiffAggregate) Result() (tree.Datum, error) {
	if a.count < 1 {
		return tree.DNull, nil
	}
	return tree.NewDFloat(tree.DFloat(a.sqrDiff)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *floatSqrDiffAggregate) Reset(context.Context) {
	a.count = 0
	a.mean = 0
	a.sqrDiff = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *floatSqrDiffAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *floatSqrDiffAggregate) Size() int64 {
	return sizeOfFloatSqrDiffAggregate
}

type decimalSqrDiffAggregate struct {
	singleDatumAggregateBase

	// Variables used across iterations.
	ed      *apd.ErrDecimal
	count   apd.Decimal
	mean    apd.Decimal
	sqrDiff apd.Decimal

	// Variables used as scratch space within iterations.
	delta apd.Decimal
	tmp   apd.Decimal
}

func newDecimalSqrDiff(evalCtx *tree.EvalContext) decimalSqrDiff {
	ed := apd.MakeErrDecimal(tree.IntermediateCtx)
	return &decimalSqrDiffAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		ed:                       &ed,
	}
}

func newDecimalSqrDiffAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return newDecimalSqrDiff(evalCtx)
}

// Count is part of the decimalSqrDiff interface.
func (a *decimalSqrDiffAggregate) Count() *apd.Decimal {
	return &a.count
}

// Tmp is part of the decimalSqrDiff interface.
func (a *decimalSqrDiffAggregate) Tmp() *apd.Decimal {
	return &a.tmp
}

func (a *decimalSqrDiffAggregate) Add(
	ctx context.Context, datum tree.Datum, _ ...tree.Datum,
) error {
	if datum == tree.DNull {
		return nil
	}
	d := &datum.(*tree.DDecimal).Decimal

	// Uses the Knuth/Welford method for accurately computing squared difference online in a
	// single pass. Refer to squared difference calculations
	// in http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.ed.Add(&a.count, &a.count, decimalOne)
	a.ed.Sub(&a.delta, d, &a.mean)
	a.ed.Quo(&a.tmp, &a.delta, &a.count)
	a.ed.Add(&a.mean, &a.mean, &a.tmp)
	a.ed.Sub(&a.tmp, d, &a.mean)
	a.ed.Add(&a.sqrDiff, &a.sqrDiff, a.ed.Mul(&a.delta, &a.delta, &a.tmp))

	size := int64(tree.SizeOfDecimal(&a.count) +
		tree.SizeOfDecimal(&a.mean) +
		tree.SizeOfDecimal(&a.sqrDiff) +
		tree.SizeOfDecimal(&a.delta) +
		tree.SizeOfDecimal(&a.tmp))
	if err := a.updateMemoryUsage(ctx, size); err != nil {
		return err
	}

	return a.ed.Err()
}

func (a *decimalSqrDiffAggregate) Result() (tree.Datum, error) {
	if a.count.Cmp(decimalOne) < 0 {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{}
	dd.Set(&a.sqrDiff)
	// Remove trailing zeros. Depending on the order in which the input
	// is processed, some number of trailing zeros could be added to the
	// output. Remove them so that the results are the same regardless of order.
	dd.Decimal.Reduce(&dd.Decimal)
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *decimalSqrDiffAggregate) Reset(ctx context.Context) {
	a.count.SetInt64(0)
	a.mean.SetInt64(0)
	a.sqrDiff.SetInt64(0)
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalSqrDiffAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *decimalSqrDiffAggregate) Size() int64 {
	return sizeOfDecimalSqrDiffAggregate
}

type floatSumSqrDiffsAggregate struct {
	count   int64
	mean    float64
	sqrDiff float64
}

func newFloatSumSqrDiffs() floatSqrDiff {
	return &floatSumSqrDiffsAggregate{}
}

func (a *floatSumSqrDiffsAggregate) Count() int64 {
	return a.count
}

// The signature for the datums is:
//   SQRDIFF (float), SUM (float), COUNT(int)
func (a *floatSumSqrDiffsAggregate) Add(
	_ context.Context, sqrDiffD tree.Datum, otherArgs ...tree.Datum,
) error {
	sumD := otherArgs[0]
	countD := otherArgs[1]
	if sqrDiffD == tree.DNull || sumD == tree.DNull || countD == tree.DNull {
		return nil
	}

	sqrDiff := float64(*sqrDiffD.(*tree.DFloat))
	sum := float64(*sumD.(*tree.DFloat))
	count := int64(*countD.(*tree.DInt))

	mean := sum / float64(count)
	delta := mean - a.mean

	// Compute the sum of Knuth/Welford sum of squared differences from the
	// mean in a single pass. Adapted from sum of RunningStats in
	// https://www.johndcook.com/blog/skewness_kurtosis and our
	// implementation of NumericStats
	// https://github.com/cockroachdb/cockroach/pull/17728.
	totalCount, ok := arith.AddWithOverflow(a.count, count)
	if !ok {
		return pgerror.Newf(pgcode.NumericValueOutOfRange,
			"number of values in aggregate exceed max count of %d", math.MaxInt64,
		)
	}
	// We are converting an int64 number (with 63-bit precision)
	// to a float64 (with 52-bit precision), thus in the worst cases,
	// we may lose up to 11 bits of precision. This was deemed acceptable
	// considering that we are losing 11 bits on a 52+-bit operation and
	// that users dealing with floating points should be aware
	// of floating-point imprecision.
	a.sqrDiff += sqrDiff + delta*delta*float64(count)*float64(a.count)/float64(totalCount)
	a.count = totalCount
	a.mean += delta * float64(count) / float64(a.count)
	return nil
}

func (a *floatSumSqrDiffsAggregate) Result() (tree.Datum, error) {
	if a.count < 1 {
		return tree.DNull, nil
	}
	return tree.NewDFloat(tree.DFloat(a.sqrDiff)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *floatSumSqrDiffsAggregate) Reset(context.Context) {
	a.count = 0
	a.mean = 0
	a.sqrDiff = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *floatSumSqrDiffsAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *floatSumSqrDiffsAggregate) Size() int64 {
	return sizeOfFloatSumSqrDiffsAggregate
}

type decimalSumSqrDiffsAggregate struct {
	singleDatumAggregateBase

	// Variables used across iterations.
	ed      *apd.ErrDecimal
	count   apd.Decimal
	mean    apd.Decimal
	sqrDiff apd.Decimal

	// Variables used as scratch space within iterations.
	tmpCount apd.Decimal
	tmpMean  apd.Decimal
	delta    apd.Decimal
	tmp      apd.Decimal
}

func newDecimalSumSqrDiffs(evalCtx *tree.EvalContext) decimalSqrDiff {
	ed := apd.MakeErrDecimal(tree.IntermediateCtx)
	return &decimalSumSqrDiffsAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		ed:                       &ed,
	}
}

// Count is part of the decimalSqrDiff interface.
func (a *decimalSumSqrDiffsAggregate) Count() *apd.Decimal {
	return &a.count
}

// Tmp is part of the decimalSumSqrDiffs interface.
func (a *decimalSumSqrDiffsAggregate) Tmp() *apd.Decimal {
	return &a.tmp
}

func (a *decimalSumSqrDiffsAggregate) Add(
	ctx context.Context, sqrDiffD tree.Datum, otherArgs ...tree.Datum,
) error {
	sumD := otherArgs[0]
	countD := otherArgs[1]
	if sqrDiffD == tree.DNull || sumD == tree.DNull || countD == tree.DNull {
		return nil
	}
	sqrDiff := &sqrDiffD.(*tree.DDecimal).Decimal
	sum := &sumD.(*tree.DDecimal).Decimal
	a.tmpCount.SetInt64(int64(*countD.(*tree.DInt)))

	a.ed.Quo(&a.tmpMean, sum, &a.tmpCount)
	a.ed.Sub(&a.delta, &a.tmpMean, &a.mean)

	// Compute the sum of Knuth/Welford sum of squared differences from the
	// mean in a single pass. Adapted from sum of RunningStats in
	// https://www.johndcook.com/blog/skewness_kurtosis and our
	// implementation of NumericStats
	// https://github.com/cockroachdb/cockroach/pull/17728.

	// This is logically equivalent to
	//   sqrDiff + delta * delta * tmpCount * a.count / (tmpCount + a.count)
	// where the expression is computed from RIGHT to LEFT.
	a.ed.Add(&a.tmp, &a.tmpCount, &a.count)
	a.ed.Quo(&a.tmp, &a.count, &a.tmp)
	a.ed.Mul(&a.tmp, &a.tmpCount, &a.tmp)
	a.ed.Mul(&a.tmp, &a.delta, &a.tmp)
	a.ed.Mul(&a.tmp, &a.delta, &a.tmp)
	a.ed.Add(&a.tmp, sqrDiff, &a.tmp)
	// Update running squared difference.
	a.ed.Add(&a.sqrDiff, &a.sqrDiff, &a.tmp)

	// Update total count.
	a.ed.Add(&a.count, &a.count, &a.tmpCount)

	// This is logically equivalent to
	//   delta * tmpCount / a.count
	// where the expression is computed from LEFT to RIGHT.
	// Note `a.count` is now the total count (includes tmpCount).
	a.ed.Mul(&a.tmp, &a.delta, &a.tmpCount)
	a.ed.Quo(&a.tmp, &a.tmp, &a.count)
	// Update running mean.
	a.ed.Add(&a.mean, &a.mean, &a.tmp)

	size := int64(tree.SizeOfDecimal(&a.count) +
		tree.SizeOfDecimal(&a.mean) +
		tree.SizeOfDecimal(&a.sqrDiff) +
		tree.SizeOfDecimal(&a.tmpCount) +
		tree.SizeOfDecimal(&a.tmpMean) +
		tree.SizeOfDecimal(&a.delta) +
		tree.SizeOfDecimal(&a.tmp))
	if err := a.updateMemoryUsage(ctx, size); err != nil {
		return err
	}

	return a.ed.Err()
}

func (a *decimalSumSqrDiffsAggregate) Result() (tree.Datum, error) {
	if a.count.Cmp(decimalOne) < 0 {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{Decimal: a.sqrDiff}
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *decimalSumSqrDiffsAggregate) Reset(ctx context.Context) {
	a.count.SetInt64(0)
	a.mean.SetInt64(0)
	a.sqrDiff.SetInt64(0)
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalSumSqrDiffsAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *decimalSumSqrDiffsAggregate) Size() int64 {
	return sizeOfDecimalSumSqrDiffsAggregate
}

type floatSqrDiff interface {
	tree.AggregateFunc
	Count() int64
}

type decimalSqrDiff interface {
	tree.AggregateFunc
	Count() *apd.Decimal
	Tmp() *apd.Decimal
}

type floatVarianceAggregate struct {
	agg floatSqrDiff
}

type decimalVarianceAggregate struct {
	agg decimalSqrDiff
}

// Both Variance and FinalVariance aggregators have the same codepath for
// their tree.AggregateFunc interface.
// The key difference is that Variance employs SqrDiffAggregate which
// has one input: VALUE; whereas FinalVariance employs SumSqrDiffsAggregate
// which takes in three inputs: (local) SQRDIFF, SUM, COUNT.
// FinalVariance is used for local/final aggregation in distsql.
func newIntVarianceAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalVarianceAggregate{agg: newIntSqrDiff(evalCtx)}
}

func newFloatVarianceAggregate(
	_ []*types.T, _ *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &floatVarianceAggregate{agg: newFloatSqrDiff()}
}

func newDecimalVarianceAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalVarianceAggregate{agg: newDecimalSqrDiff(evalCtx)}
}

func newFloatFinalVarianceAggregate(
	_ []*types.T, _ *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &floatVarianceAggregate{agg: newFloatSumSqrDiffs()}
}

func newDecimalFinalVarianceAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalVarianceAggregate{agg: newDecimalSumSqrDiffs(evalCtx)}
}

// Add is part of the tree.AggregateFunc interface.
//  Variance: VALUE(float)
//  FinalVariance: SQRDIFF(float), SUM(float), COUNT(int)
func (a *floatVarianceAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Add is part of the tree.AggregateFunc interface.
//  Variance: VALUE(int|decimal)
//  FinalVariance: SQRDIFF(decimal), SUM(decimal), COUNT(int)
func (a *decimalVarianceAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Result calculates the variance from the member square difference aggregator.
func (a *floatVarianceAggregate) Result() (tree.Datum, error) {
	if a.agg.Count() < 2 {
		return tree.DNull, nil
	}
	sqrDiff, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	return tree.NewDFloat(tree.DFloat(float64(*sqrDiff.(*tree.DFloat)) / (float64(a.agg.Count()) - 1))), nil
}

// Result calculates the variance from the member square difference aggregator.
func (a *decimalVarianceAggregate) Result() (tree.Datum, error) {
	if a.agg.Count().Cmp(decimalTwo) < 0 {
		return tree.DNull, nil
	}
	sqrDiff, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if _, err = tree.IntermediateCtx.Sub(a.agg.Tmp(), a.agg.Count(), decimalOne); err != nil {
		return nil, err
	}
	dd := &tree.DDecimal{}
	if _, err = tree.DecimalCtx.Quo(&dd.Decimal, &sqrDiff.(*tree.DDecimal).Decimal, a.agg.Tmp()); err != nil {
		return nil, err
	}
	// Remove trailing zeros. Depending on the order in which the input is
	// processed, some number of trailing zeros could be added to the
	// output. Remove them so that the results are the same regardless of
	// order.
	dd.Decimal.Reduce(&dd.Decimal)
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *floatVarianceAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *floatVarianceAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *floatVarianceAggregate) Size() int64 {
	return sizeOfFloatVarianceAggregate
}

// Reset implements tree.AggregateFunc interface.
func (a *decimalVarianceAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalVarianceAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *decimalVarianceAggregate) Size() int64 {
	return sizeOfDecimalVarianceAggregate
}

type floatVarPopAggregate struct {
	agg floatSqrDiff
}

type decimalVarPopAggregate struct {
	agg decimalSqrDiff
}

func newIntVarPopAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalVarPopAggregate{agg: newIntSqrDiff(evalCtx)}
}

func newFloatVarPopAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &floatVarPopAggregate{agg: newFloatSqrDiff()}
}

func newDecimalVarPopAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalVarPopAggregate{agg: newDecimalSqrDiff(evalCtx)}
}

// Add is part of the tree.AggregateFunc interface.
//  Population Variance: VALUE(float)
func (a *floatVarPopAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Add is part of the tree.AggregateFunc interface.
//  Population Variance: VALUE(int|decimal)
func (a *decimalVarPopAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Result calculates the population variance from the member square difference aggregator.
func (a *floatVarPopAggregate) Result() (tree.Datum, error) {
	if a.agg.Count() < 1 {
		return tree.DNull, nil
	}
	sqrDiff, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	return tree.NewDFloat(tree.DFloat(float64(*sqrDiff.(*tree.DFloat)) / (float64(a.agg.Count())))), nil
}

// Result calculates the population variance from the member square difference aggregator.
func (a *decimalVarPopAggregate) Result() (tree.Datum, error) {
	if a.agg.Count().Cmp(decimalOne) < 0 {
		return tree.DNull, nil
	}
	sqrDiff, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	dd := &tree.DDecimal{}
	if _, err = tree.DecimalCtx.Quo(&dd.Decimal, &sqrDiff.(*tree.DDecimal).Decimal, a.agg.Count()); err != nil {
		return nil, err
	}
	// Remove trailing zeros. Depending on the order in which the input is
	// processed, some number of trailing zeros could be added to the
	// output. Remove them so that the results are the same regardless of
	// order.
	dd.Decimal.Reduce(&dd.Decimal)
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *floatVarPopAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *floatVarPopAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *floatVarPopAggregate) Size() int64 {
	return sizeOfFloatVarPopAggregate
}

// Reset implements tree.AggregateFunc interface.
func (a *decimalVarPopAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalVarPopAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *decimalVarPopAggregate) Size() int64 {
	return sizeOfDecimalVarPopAggregate
}

type floatStdDevAggregate struct {
	agg tree.AggregateFunc
}

type decimalStdDevAggregate struct {
	agg tree.AggregateFunc
}

// Both StdDev and FinalStdDev aggregators have the same codepath for
// their tree.AggregateFunc interface.
// The key difference is that StdDev employs SqrDiffAggregate which
// has one input: VALUE; whereas FinalStdDev employs SumSqrDiffsAggregate
// which takes in three inputs: (local) SQRDIFF, SUM, COUNT.
// FinalStdDev is used for local/final aggregation in distsql.
func newIntStdDevAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &decimalStdDevAggregate{agg: newIntVarianceAggregate(params, evalCtx, arguments)}
}

func newFloatStdDevAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &floatStdDevAggregate{agg: newFloatVarianceAggregate(params, evalCtx, arguments)}
}

func newDecimalStdDevAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &decimalStdDevAggregate{agg: newDecimalVarianceAggregate(params, evalCtx, arguments)}
}

func newFloatFinalStdDevAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &floatStdDevAggregate{agg: newFloatFinalVarianceAggregate(params, evalCtx, arguments)}
}

func newDecimalFinalStdDevAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &decimalStdDevAggregate{agg: newDecimalFinalVarianceAggregate(params, evalCtx, arguments)}
}

func newIntStdDevPopAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &decimalStdDevAggregate{agg: newIntVarPopAggregate(params, evalCtx, arguments)}
}

func newFloatStdDevPopAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &floatStdDevAggregate{agg: newFloatVarPopAggregate(params, evalCtx, arguments)}
}

func newDecimalStdDevPopAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &decimalStdDevAggregate{agg: newDecimalVarPopAggregate(params, evalCtx, arguments)}
}

// Add implements the tree.AggregateFunc interface.
// The signature of the datums is:
//  StdDev: VALUE(float)
//  FinalStdDev: SQRDIFF(float), SUM(float), COUNT(int)
func (a *floatStdDevAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Add is part of the tree.AggregateFunc interface.
// The signature of the datums is:
//  StdDev: VALUE(int|decimal)
//  FinalStdDev: SQRDIFF(decimal), SUM(decimal), COUNT(int)
func (a *decimalStdDevAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Result computes the square root of the variance aggregator.
func (a *floatStdDevAggregate) Result() (tree.Datum, error) {
	variance, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if variance == tree.DNull {
		return variance, nil
	}
	return tree.NewDFloat(tree.DFloat(math.Sqrt(float64(*variance.(*tree.DFloat))))), nil
}

// Result computes the square root of the variance aggregator.
func (a *decimalStdDevAggregate) Result() (tree.Datum, error) {
	// TODO(richardwu): both decimalVarianceAggregate and
	// finalDecimalVarianceAggregate return a decimal result with
	// default tree.DecimalCtx precision. We want to be able to specify that the
	// varianceAggregate use tree.IntermediateCtx (with the extra precision)
	// since it is returning an intermediate value for stdDevAggregate (of
	// which we take the Sqrt).
	variance, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if variance == tree.DNull {
		return variance, nil
	}
	varianceDec := variance.(*tree.DDecimal)
	_, err = tree.DecimalCtx.Sqrt(&varianceDec.Decimal, &varianceDec.Decimal)
	return varianceDec, err
}

// Reset implements tree.AggregateFunc interface.
func (a *floatStdDevAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *floatStdDevAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *floatStdDevAggregate) Size() int64 {
	return sizeOfFloatStdDevAggregate
}

// Reset implements tree.AggregateFunc interface.
func (a *decimalStdDevAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalStdDevAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *decimalStdDevAggregate) Size() int64 {
	return sizeOfDecimalStdDevAggregate
}

type bytesXorAggregate struct {
	singleDatumAggregateBase

	sum        []byte
	sawNonNull bool
}

func newBytesXorAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &bytesXorAggregate{singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx)}
}

// Add inserts one value into the running xor.
func (a *bytesXorAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := []byte(*datum.(*tree.DBytes))
	if !a.sawNonNull {
		if err := a.updateMemoryUsage(ctx, int64(len(t))); err != nil {
			return err
		}
		a.sum = append([]byte(nil), t...)
	} else if len(a.sum) != len(t) {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"arguments to xor must all be the same length %d vs %d", len(a.sum), len(t),
		)
	} else {
		for i := range t {
			a.sum[i] = a.sum[i] ^ t[i]
		}
	}
	a.sawNonNull = true
	return nil
}

// Result returns the xor.
func (a *bytesXorAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDBytes(tree.DBytes(a.sum)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *bytesXorAggregate) Reset(ctx context.Context) {
	a.sum = nil
	a.sawNonNull = false
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *bytesXorAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *bytesXorAggregate) Size() int64 {
	return sizeOfBytesXorAggregate
}

type intXorAggregate struct {
	sum        int64
	sawNonNull bool
}

func newIntXorAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &intXorAggregate{}
}

// Add inserts one value into the running xor.
func (a *intXorAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	x := int64(*datum.(*tree.DInt))
	a.sum = a.sum ^ x
	a.sawNonNull = true
	return nil
}

// Result returns the xor.
func (a *intXorAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDInt(tree.DInt(a.sum)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *intXorAggregate) Reset(context.Context) {
	a.sum = 0
	a.sawNonNull = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *intXorAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *intXorAggregate) Size() int64 {
	return sizeOfIntXorAggregate
}

type jsonAggregate struct {
	singleDatumAggregateBase

	evalCtx    *tree.EvalContext
	builder    *json.ArrayBuilderWithCounter
	sawNonNull bool
}

func newJSONAggregate(_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &jsonAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		builder:                  json.NewArrayBuilderWithCounter(),
		sawNonNull:               false,
	}
}

// Add accumulates the transformed json into the JSON array.
func (a *jsonAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	j, err := tree.AsJSON(
		datum,
		a.evalCtx.SessionData.DataConversionConfig,
		a.evalCtx.GetLocation(),
	)
	if err != nil {
		return err
	}
	a.builder.Add(j)
	if err = a.updateMemoryUsage(ctx, int64(a.builder.Size())); err != nil {
		return err
	}
	a.sawNonNull = true
	return nil
}

// Result returns an DJSON from the array of JSON.
func (a *jsonAggregate) Result() (tree.Datum, error) {
	if a.sawNonNull {
		return tree.NewDJSON(a.builder.Build()), nil
	}
	return tree.DNull, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *jsonAggregate) Reset(ctx context.Context) {
	a.builder = json.NewArrayBuilderWithCounter()
	a.sawNonNull = false
	a.reset(ctx)
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *jsonAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *jsonAggregate) Size() int64 {
	return sizeOfJSONAggregate
}

// validateInputFractions validates that the inputs are expected and returns an
// array containing either a single fraction or multiple fractions.
func validateInputFractions(datum tree.Datum) ([]float64, bool, error) {
	fractions := make([]float64, 0)
	singleInput := false

	validate := func(fraction float64) error {
		if fraction < 0 || fraction > 1.0 {
			return pgerror.Newf(pgcode.NumericValueOutOfRange,
				"percentile value %f is not between 0 and 1", fraction)
		}
		return nil
	}

	if datum.ResolvedType().Identical(types.Float) {
		fraction := float64(tree.MustBeDFloat(datum))
		singleInput = true
		if err := validate(fraction); err != nil {
			return nil, false, err
		}
		fractions = append(fractions, fraction)
	} else if datum.ResolvedType().Equivalent(types.FloatArray) {
		fractionsDatum := tree.MustBeDArray(datum)
		for _, f := range fractionsDatum.Array {
			fraction := float64(tree.MustBeDFloat(f))
			if err := validate(fraction); err != nil {
				return nil, false, err
			}
			fractions = append(fractions, fraction)
		}
	} else {
		panic(errors.AssertionFailedf("unexpected input type, %s", datum.ResolvedType()))
	}
	return fractions, singleInput, nil
}

type percentileDiscAggregate struct {
	arr *tree.DArray
	// Note that we do not embed singleDatumAggregateBase struct to help with
	// memory accounting because percentileDiscAggregate stores multiple datums
	// inside of arr.
	acc mon.BoundAccount
	// We need singleInput to differentiate whether the input was a single
	// fraction, or an array of fractions.
	singleInput bool
	fractions   []float64
}

func newPercentileDiscAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &percentileDiscAggregate{
		arr: tree.NewDArray(params[1]),
		acc: evalCtx.Mon.MakeBoundAccount(),
	}
}

// Add stores the input percentile and all the values to calculate the discrete percentile.
func (a *percentileDiscAggregate) Add(
	ctx context.Context, datum tree.Datum, others ...tree.Datum,
) error {
	if len(a.fractions) == 0 && datum != tree.DNull {
		fractions, singleInput, err := validateInputFractions(datum)
		if err != nil {
			return err
		}
		a.fractions = fractions
		a.singleInput = singleInput
	}

	if len(others) == 1 && others[0] != tree.DNull {
		if err := a.acc.Grow(ctx, int64(others[0].Size())); err != nil {
			return err
		}
		return a.arr.Append(others[0])
	} else if len(others) != 1 {
		panic(errors.AssertionFailedf("unexpected number of other datums passed in, expected 1, got %d", len(others)))
	}
	return nil
}

// Result finds the discrete percentile.
func (a *percentileDiscAggregate) Result() (tree.Datum, error) {
	// Return null if there are no values.
	if a.arr.Len() == 0 {
		return tree.DNull, nil
	}

	if len(a.fractions) > 0 {
		res := tree.NewDArray(a.arr.ParamTyp)
		for _, fraction := range a.fractions {
			// If zero fraction is specified then give the first index, otherwise account
			// for row index which uses zero-based indexing.
			if fraction == 0.0 {
				if err := res.Append(a.arr.Array[0]); err != nil {
					return nil, err
				}
				continue
			}
			// Use math.Ceil since we want the first value whose position equals or
			// exceeds the specified fraction.
			rowIndex := int(math.Ceil(fraction*float64(a.arr.Len()))) - 1
			if err := res.Append(a.arr.Array[rowIndex]); err != nil {
				return nil, err
			}
		}

		if a.singleInput {
			return res.Array[0], nil
		}
		return res, nil
	}
	panic("input must either be a single fraction, or an array of fractions")
}

// Reset implements tree.AggregateFunc interface.
func (a *percentileDiscAggregate) Reset(ctx context.Context) {
	a.arr = tree.NewDArray(a.arr.ParamTyp)
	a.acc.Empty(ctx)
	a.singleInput = false
	a.fractions = a.fractions[:0]
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *percentileDiscAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *percentileDiscAggregate) Size() int64 {
	return sizeOfPercentileDiscAggregate
}

type percentileContAggregate struct {
	arr *tree.DArray
	// Note that we do not embed singleDatumAggregateBase struct to help with
	// memory accounting because percentileContAggregate stores multiple datums
	// inside of arr.
	acc mon.BoundAccount
	// We need singleInput to differentiate whether the input was a single
	// fraction, or an array of fractions.
	singleInput bool
	fractions   []float64
}

func newPercentileContAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &percentileContAggregate{
		arr: tree.NewDArray(params[1]),
		acc: evalCtx.Mon.MakeBoundAccount(),
	}
}

// Add stores the input percentile and all the values to calculate the continuous percentile.
func (a *percentileContAggregate) Add(
	ctx context.Context, datum tree.Datum, others ...tree.Datum,
) error {
	if len(a.fractions) == 0 && datum != tree.DNull {
		fractions, singleInput, err := validateInputFractions(datum)
		if err != nil {
			return err
		}
		a.fractions = fractions
		a.singleInput = singleInput
	}

	if len(others) == 1 && others[0] != tree.DNull {
		if err := a.acc.Grow(ctx, int64(others[0].Size())); err != nil {
			return err
		}
		return a.arr.Append(others[0])
	} else if len(others) != 1 {
		panic(errors.AssertionFailedf("unexpected number of other datums passed in, expected 1, got %d", len(others)))
	}
	return nil
}

// Result finds the continuous percentile.
func (a *percentileContAggregate) Result() (tree.Datum, error) {
	// Return null if there are no values.
	if a.arr.Len() == 0 {
		return tree.DNull, nil
	}
	// The following is the formula for calculating the continuous percentile:
	// RN = (1 + (fraction * (frameSize - 1))
	// CRN = Ceil(RN)
	// FRN = Floor(RN)
	// If (CRN = FRN = RN) then the result is:
	//   (value at row RN)
	// Otherwise the result is:
	//   (CRN - RN) * (value at row FRN) +
	//   (RN - FRN) * (value at row CRN)
	// Adapted from:
	//   https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions110.htm
	// If the input specified was a single fraction, then return a single value.
	if len(a.fractions) > 0 {
		res := tree.NewDArray(a.arr.ParamTyp)
		for _, fraction := range a.fractions {
			rowNumber := 1.0 + (fraction * (float64(a.arr.Len()) - 1.0))
			ceilRowNumber := int(math.Ceil(rowNumber))
			floorRowNumber := int(math.Floor(rowNumber))

			if a.arr.ParamTyp.Identical(types.Float) {
				var target float64
				if rowNumber == float64(ceilRowNumber) && rowNumber == float64(floorRowNumber) {
					target = float64(tree.MustBeDFloat(a.arr.Array[int(rowNumber)-1]))
				} else {
					floorValue := float64(tree.MustBeDFloat(a.arr.Array[floorRowNumber-1]))
					ceilValue := float64(tree.MustBeDFloat(a.arr.Array[ceilRowNumber-1]))
					target = (float64(ceilRowNumber)-rowNumber)*floorValue +
						(rowNumber-float64(floorRowNumber))*ceilValue
				}
				if err := res.Append(tree.NewDFloat(tree.DFloat(target))); err != nil {
					return nil, err
				}
			} else if a.arr.ParamTyp.Family() == types.IntervalFamily {
				var target *tree.DInterval
				if rowNumber == float64(ceilRowNumber) && rowNumber == float64(floorRowNumber) {
					target = tree.MustBeDInterval(a.arr.Array[int(rowNumber)-1])
				} else {
					floorValue := tree.MustBeDInterval(a.arr.Array[floorRowNumber-1]).AsFloat64()
					ceilValue := tree.MustBeDInterval(a.arr.Array[ceilRowNumber-1]).AsFloat64()
					targetDuration := duration.FromFloat64(
						(float64(ceilRowNumber)-rowNumber)*floorValue +
							(rowNumber-float64(floorRowNumber))*ceilValue)
					target = tree.NewDInterval(targetDuration, types.DefaultIntervalTypeMetadata)
				}
				if err := res.Append(target); err != nil {
					return nil, err
				}
			} else {
				panic(errors.AssertionFailedf("argument type must be float or interval, got %s", a.arr.ParamTyp.String()))
			}
		}
		if a.singleInput {
			return res.Array[0], nil
		}
		return res, nil
	}
	panic("input must either be a single fraction, or an array of fractions")
}

// Reset implements tree.AggregateFunc interface.
func (a *percentileContAggregate) Reset(ctx context.Context) {
	a.arr = tree.NewDArray(a.arr.ParamTyp)
	a.acc.Empty(ctx)
	a.singleInput = false
	a.fractions = a.fractions[:0]
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *percentileContAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *percentileContAggregate) Size() int64 {
	return sizeOfPercentileContAggregate
}

type jsonObjectAggregate struct {
	singleDatumAggregateBase

	evalCtx    *tree.EvalContext
	builder    *json.ObjectBuilderWithCounter
	sawNonNull bool
}

func newJSONObjectAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &jsonObjectAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		builder:                  json.NewObjectBuilderWithCounter(),
		sawNonNull:               false,
	}
}

// Add accumulates the transformed json into the JSON object.
func (a *jsonObjectAggregate) Add(
	ctx context.Context, datum tree.Datum, others ...tree.Datum,
) error {
	if len(others) != 1 {
		return errors.Errorf("wrong number of arguments, expected key/value pair")
	}

	// If the key datum is NULL, return an error.
	if datum == tree.DNull {
		return pgerror.New(pgcode.InvalidParameterValue,
			"field name must not be null")
	}

	key, err := asJSONBuildObjectKey(
		datum,
		a.evalCtx.SessionData.DataConversionConfig,
		a.evalCtx.GetLocation(),
	)
	if err != nil {
		return err
	}
	val, err := tree.AsJSON(
		others[0],
		a.evalCtx.SessionData.DataConversionConfig,
		a.evalCtx.GetLocation(),
	)
	if err != nil {
		return err
	}
	a.builder.Add(key, val)
	if err = a.updateMemoryUsage(ctx, int64(a.builder.Size())); err != nil {
		return err
	}
	a.sawNonNull = true
	return nil
}

// Result returns a DJSON from the array of JSON.
func (a *jsonObjectAggregate) Result() (tree.Datum, error) {
	if a.sawNonNull {
		return tree.NewDJSON(a.builder.Build()), nil
	}
	return tree.DNull, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *jsonObjectAggregate) Reset(ctx context.Context) {
	a.builder = json.NewObjectBuilderWithCounter()
	a.sawNonNull = false
	a.reset(ctx)
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *jsonObjectAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *jsonObjectAggregate) Size() int64 {
	return sizeOfJSONObjectAggregate
}
