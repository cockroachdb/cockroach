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
	"time"
	"unsafe"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

func initAggregateBuiltins() {
	// Add all aggregates to the Builtins map after a few sanity checks.
	for k, v := range aggregates {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}

		if !v.props.Impure {
			panic(fmt.Sprintf("%s: aggregate functions should all be impure, found %v", k, v))
		}
		if v.props.Class != tree.AggregateClass {
			panic(fmt.Sprintf("%s: aggregate functions should be marked with the tree.AggregateClass "+
				"function class, found %v", k, v))
		}
		for _, a := range v.overloads {
			if a.AggregateFunc == nil {
				panic(fmt.Sprintf("%s: aggregate functions should have tree.AggregateFunc constructors, "+
					"found %v", k, a))
			}
			if a.WindowFunc == nil {
				panic(fmt.Sprintf("%s: aggregate functions should have tree.WindowFunc constructors, "+
					"found %v", k, a))
			}
		}

		// The aggregate functions are considered "row dependent". This is
		// because each aggregate function application receives the set of
		// grouped rows as implicit parameter. It may have a different
		// value in every group, so it cannot be considered constant in
		// the context of a data source.
		v.props.NeedsRepeatedEvaluation = true

		builtins[k] = v
	}
}

func aggProps() tree.FunctionProperties {
	return tree.FunctionProperties{Class: tree.AggregateClass, Impure: true}
}

func aggPropsNullableArgs() tree.FunctionProperties {
	f := aggProps()
	f.NullableArgs = true
	return f
}

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
			)
		})),

	"avg": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntAvgAggregate,
			"Calculates the average of the selected values."),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatAvgAggregate,
			"Calculates the average of the selected values."),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalAvgAggregate,
			"Calculates the average of the selected values."),
		makeAggOverload([]*types.T{types.Interval}, types.Interval, newIntervalAvgAggregate,
			"Calculates the average of the selected values."),
	),

	"bit_and": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Int, newBitAndAggregate,
			"Calculates the bitwise AND of all non-null input values, or null if none."),
	),

	"bit_or": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Int, newBitOrAggregate,
			"Calculates the bitwise OR of all non-null input values, or null if none."),
	),

	"bool_and": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Bool}, types.Bool, newBoolAndAggregate,
			"Calculates the boolean value of `AND`ing all selected values."),
	),

	"bool_or": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Bool}, types.Bool, newBoolOrAggregate,
			"Calculates the boolean value of `OR`ing all selected values."),
	),

	"concat_agg": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.String}, types.String, newStringConcatAggregate,
			"Concatenates all selected values."),
		makeAggOverload([]*types.T{types.Bytes}, types.Bytes, newBytesConcatAggregate,
			"Concatenates all selected values."),
		// TODO(eisen): support collated strings when the type system properly
		// supports parametric types.
	),

	"corr": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Float, types.Float}, types.Float, newCorrAggregate,
			"Calculates the correlation coefficient of the selected values."),
		makeAggOverload([]*types.T{types.Int, types.Int}, types.Float, newCorrAggregate,
			"Calculates the correlation coefficient of the selected values."),
		makeAggOverload([]*types.T{types.Float, types.Int}, types.Float, newCorrAggregate,
			"Calculates the correlation coefficient of the selected values."),
		makeAggOverload([]*types.T{types.Int, types.Float}, types.Float, newCorrAggregate,
			"Calculates the correlation coefficient of the selected values."),
	),

	"count": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.Any}, types.Int, newCountAggregate,
			"Calculates the number of selected elements."),
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
			Info: "Calculates the number of rows.",
		},
	),

	"max": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			return makeAggOverload([]*types.T{t}, t, newMaxAggregate,
				"Identifies the maximum selected value.")
		}),

	"min": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			return makeAggOverload([]*types.T{t}, t, newMinAggregate,
				"Identifies the minimum selected value.")
		}),

	"string_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.String, types.String}, types.String, newStringConcatAggregate,
			"Concatenates all selected values using the provided delimiter."),
		makeAggOverload([]*types.T{types.Bytes, types.Bytes}, types.Bytes, newBytesConcatAggregate,
			"Concatenates all selected values using the provided delimiter."),
	),

	"sum_int": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Int, newSmallIntSumAggregate,
			"Calculates the sum of the selected values."),
	),

	"sum": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggOverload([]*types.T{types.Interval}, types.Interval, newIntervalSumAggregate,
			"Calculates the sum of the selected values."),
	),

	"sqrdiff": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values."),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values."),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values."),
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
		),
		makeAggOverload(
			[]*types.T{types.Float, types.Float, types.Int},
			types.Float,
			newFloatFinalVarianceAggregate,
			"Calculates the variance from the selected locally-computed squared difference values.",
		),
	)),

	"final_stddev": makePrivate(makeBuiltin(aggProps(),
		makeAggOverload(
			[]*types.T{types.Decimal,
				types.Decimal, types.Int},
			types.Decimal,
			newDecimalFinalStdDevAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]*types.T{types.Float, types.Float, types.Int},
			types.Float,
			newFloatFinalStdDevAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
	)),

	"variance": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntVarianceAggregate,
			"Calculates the variance of the selected values."),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalVarianceAggregate,
			"Calculates the variance of the selected values."),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatVarianceAggregate,
			"Calculates the variance of the selected values."),
	),

	"stddev": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
	),

	"xor_agg": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Bytes}, types.Bytes, newBytesXorAggregate,
			"Calculates the bitwise XOR of the selected values."),
		makeAggOverload([]*types.T{types.Int}, types.Int, newIntXorAggregate,
			"Calculates the bitwise XOR of the selected values."),
	),

	"json_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.Any}, types.Jsonb, newJSONAggregate,
			"Aggregates values as a JSON or JSONB array."),
	),

	"jsonb_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.Any}, types.Jsonb, newJSONAggregate,
			"Aggregates values as a JSON or JSONB array."),
	),

	"json_object_agg":  makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Class: tree.AggregateClass, Impure: true}),
	"jsonb_object_agg": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Class: tree.AggregateClass, Impure: true}),

	AnyNotNull: makePrivate(makeBuiltin(aggProps(),
		makeAggOverloadWithReturnType(
			[]*types.T{types.Any},
			tree.IdentityReturnType(0),
			newAnyNotNullAggregate,
			"Returns an arbitrary not-NULL value, or NULL if none exists.",
		))),
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
) tree.Overload {
	return makeAggOverloadWithReturnType(
		in,
		tree.FixedReturnType(ret),
		f,
		info,
	)
}

func makeAggOverloadWithReturnType(
	in []*types.T,
	retType tree.ReturnTyper,
	f func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc,
	info string,
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
			case *MinAggregate:
				min := &slidingWindowFunc{}
				min.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return -a.Compare(evalCtx, b)
				})
				return min
			case *MaxAggregate:
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
		Info: info,
	}
}

var _ tree.AggregateFunc = &arrayAggregate{}
var _ tree.AggregateFunc = &avgAggregate{}
var _ tree.AggregateFunc = &corrAggregate{}
var _ tree.AggregateFunc = &countAggregate{}
var _ tree.AggregateFunc = &countRowsAggregate{}
var _ tree.AggregateFunc = &MaxAggregate{}
var _ tree.AggregateFunc = &MinAggregate{}
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
var _ tree.AggregateFunc = &bitAndAggregate{}
var _ tree.AggregateFunc = &bitOrAggregate{}

const sizeOfArrayAggregate = int64(unsafe.Sizeof(arrayAggregate{}))
const sizeOfAvgAggregate = int64(unsafe.Sizeof(avgAggregate{}))
const sizeOfCorrAggregate = int64(unsafe.Sizeof(corrAggregate{}))
const sizeOfCountAggregate = int64(unsafe.Sizeof(countAggregate{}))
const sizeOfCountRowsAggregate = int64(unsafe.Sizeof(countRowsAggregate{}))
const sizeOfMaxAggregate = int64(unsafe.Sizeof(MaxAggregate{}))
const sizeOfMinAggregate = int64(unsafe.Sizeof(MinAggregate{}))
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
const sizeOfFloatStdDevAggregate = int64(unsafe.Sizeof(floatStdDevAggregate{}))
const sizeOfDecimalStdDevAggregate = int64(unsafe.Sizeof(decimalStdDevAggregate{}))
const sizeOfAnyNotNullAggregate = int64(unsafe.Sizeof(anyNotNullAggregate{}))
const sizeOfConcatAggregate = int64(unsafe.Sizeof(concatAggregate{}))
const sizeOfBoolAndAggregate = int64(unsafe.Sizeof(boolAndAggregate{}))
const sizeOfBoolOrAggregate = int64(unsafe.Sizeof(boolOrAggregate{}))
const sizeOfBytesXorAggregate = int64(unsafe.Sizeof(bytesXorAggregate{}))
const sizeOfIntXorAggregate = int64(unsafe.Sizeof(intXorAggregate{}))
const sizeOfJSONAggregate = int64(unsafe.Sizeof(jsonAggregate{}))
const sizeOfBitAndAggregate = int64(unsafe.Sizeof(bitAndAggregate{}))
const sizeOfBitOrAggregate = int64(unsafe.Sizeof(bitOrAggregate{}))

// See NewAnyNotNullAggregate.
type anyNotNullAggregate struct {
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
func NewAnyNotNullAggregate(*tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &anyNotNullAggregate{val: tree.DNull}
}

func newAnyNotNullAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &anyNotNullAggregate{val: tree.DNull}
}

// Add sets the value to the passed datum.
func (a *anyNotNullAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if a.val == tree.DNull && datum != tree.DNull {
		a.val = datum
	}
	return nil
}

// Result returns the value most recently passed to Add.
func (a *anyNotNullAggregate) Result() (tree.Datum, error) {
	return a.val, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *anyNotNullAggregate) Reset(context.Context) {}

// Close is no-op in aggregates using constant space.
func (a *anyNotNullAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *anyNotNullAggregate) Size() int64 {
	return sizeOfAnyNotNullAggregate
}

type arrayAggregate struct {
	arr *tree.DArray
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
	forBytes   bool
	sawNonNull bool
	delimiter  string // used for non window functions
	result     bytes.Buffer
	acc        mon.BoundAccount
}

func newBytesConcatAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	concatAgg := &concatAggregate{
		forBytes: true,
		acc:      evalCtx.Mon.MakeBoundAccount(),
	}
	if len(arguments) == 1 && arguments[0] != tree.DNull {
		concatAgg.delimiter = string(tree.MustBeDBytes(arguments[0]))
	} else if len(arguments) > 1 {
		panic(fmt.Sprintf("too many arguments passed in, expected < 2, got %d", len(arguments)))
	}
	return concatAgg
}

func newStringConcatAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	concatAgg := &concatAggregate{
		acc: evalCtx.Mon.MakeBoundAccount(),
	}
	if len(arguments) == 1 && arguments[0] != tree.DNull {
		concatAgg.delimiter = string(tree.MustBeDString(arguments[0]))
	} else if len(arguments) > 1 {
		panic(fmt.Sprintf("too many arguments passed in, expected < 2, got %d", len(arguments)))
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
			panic(fmt.Sprintf("too many other datums passed in, expected < 2, got %d", len(others)))
		}
		if len(delimiter) > 0 {
			if err := a.acc.Grow(ctx, int64(len(delimiter))); err != nil {
				return err
			}
			a.result.WriteString(delimiter)
		}
	}
	var arg string
	if a.forBytes {
		arg = string(tree.MustBeDBytes(datum))
	} else {
		arg = string(tree.MustBeDString(datum))
	}
	if err := a.acc.Grow(ctx, int64(len(arg))); err != nil {
		return err
	}
	a.result.WriteString(arg)
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
func (a *concatAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result.Reset()
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *concatAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *concatAggregate) Size() int64 {
	return sizeOfConcatAggregate
}

type bitAndAggregate struct {
	sawNonNull bool
	result     int64
}

func newBitAndAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &bitAndAggregate{}
}

// Add inserts one value into the running bitwise AND.
func (a *bitAndAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
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
func (a *bitAndAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDInt(tree.DInt(a.result)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *bitAndAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *bitAndAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *bitAndAggregate) Size() int64 {
	return sizeOfBitAndAggregate
}

type bitOrAggregate struct {
	sawNonNull bool
	result     int64
}

func newBitOrAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &bitOrAggregate{}
}

// Add inserts one value into the running bitwise OR.
func (a *bitOrAggregate) Add(_ context.Context, datum tree.Datum, otherArgs ...tree.Datum) error {
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
func (a *bitOrAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDInt(tree.DInt(a.result)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *bitOrAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *bitOrAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *bitOrAggregate) Size() int64 {
	return sizeOfBitOrAggregate
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

// corrAggregate represents SQL:2003 correlation coefficient.
//
// n   be count of rows.
// sx  be the sum of the column of values of <independent variable expression>
// sx2 be the sum of the squares of values in the <independent variable expression> column
// sy  be the sum of the column of values of <dependent variable expression>
// sy2 be the sum of the squares of values in the <dependent variable expression> column
// sxy be the sum of the row-wise products of the value in the <independent variable expression>
//     column times the value in the <dependent variable expression> column.
//
// result:
//   1) If n*sx2 equals sx*sx, then the result is the null value.
//   2) If n*sy2 equals sy*sy, then the result is the null value.
//   3) Otherwise, the resut is SQRT(POWER(n*sxy-sx*sy,2) / ((n*sx2-sx*sx)*(n*sy2-sy*sy))).
//      If the exponent of the approximate mathematical result of the operation is not within
//      the implementation-defined exponent range for the result data type, then the result
//      is the null value.
type corrAggregate struct {
	n   int
	sx  float64
	sx2 float64
	sy  float64
	sy2 float64
	sxy float64
}

func newCorrAggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &corrAggregate{}
}

// Add implements tree.AggregateFunc interface.
func (a *corrAggregate) Add(_ context.Context, datumY tree.Datum, otherArgs ...tree.Datum) error {
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

	a.n++
	a.sx += x
	a.sy += y
	a.sx2 += x * x
	a.sy2 += y * y
	a.sxy += x * y

	if math.IsInf(a.sx, 0) ||
		math.IsInf(a.sx2, 0) ||
		math.IsInf(a.sy, 0) ||
		math.IsInf(a.sy2, 0) ||
		math.IsInf(a.sxy, 0) {
		return tree.ErrFloatOutOfRange
	}

	return nil
}

// Result implements tree.AggregateFunc interface.
func (a *corrAggregate) Result() (tree.Datum, error) {
	if a.n < 1 {
		return tree.DNull, nil
	}

	if a.sx2 == 0 || a.sy2 == 0 {
		return tree.DNull, nil
	}

	floatN := float64(a.n)

	numeratorX := floatN*a.sx2 - a.sx*a.sx
	if math.IsInf(numeratorX, 0) {
		return tree.DNull, pgerror.New(pgcode.NumericValueOutOfRange, "float out of range")
	}

	numeratorY := floatN*a.sy2 - a.sy*a.sy
	if math.IsInf(numeratorY, 0) {
		return tree.DNull, pgerror.New(pgcode.NumericValueOutOfRange, "float out of range")
	}

	numeratorXY := floatN*a.sxy - a.sx*a.sy
	if math.IsInf(numeratorXY, 0) {
		return tree.DNull, pgerror.New(pgcode.NumericValueOutOfRange, "float out of range")
	}

	if numeratorX <= 0 || numeratorY <= 0 {
		return tree.DNull, nil
	}

	return tree.NewDFloat(tree.DFloat(numeratorXY / math.Sqrt(numeratorX*numeratorY))), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *corrAggregate) Reset(context.Context) {
	a.n = 0
	a.sx = 0
	a.sx2 = 0
	a.sy = 0
	a.sy2 = 0
	a.sxy = 0
}

// Close implements tree.AggregateFunc interface.
func (a *corrAggregate) Close(context.Context) {}

// Size implements tree.AggregateFunc interface.
func (a *corrAggregate) Size() int64 {
	return sizeOfCorrAggregate
}

func (a *corrAggregate) float64Val(datum tree.Datum) (float64, error) {
	switch val := datum.(type) {
	case *tree.DFloat:
		return float64(*val), nil
	case *tree.DInt:
		return float64(*val), nil
	default:
		return 0, fmt.Errorf("invalid type %v", val)
	}
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

// MaxAggregate keeps track of the largest value passed to Add.
type MaxAggregate struct {
	max     tree.Datum
	evalCtx *tree.EvalContext

	acc               mon.BoundAccount
	datumSize         uintptr
	variableDatumSize bool
}

func newMaxAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	sz, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a fixed size, it will be included in the size
	// reported by Size(). Otherwise it will be accounted for in Add(). This
	// avoids doing unnecessary memory accounting work for fixed-size datums.
	if variable {
		sz = 0
	}
	return &MaxAggregate{
		evalCtx:           evalCtx,
		acc:               evalCtx.Mon.MakeBoundAccount(),
		datumSize:         sz,
		variableDatumSize: variable,
	}
}

// Add sets the max to the larger of the current max or the passed datum.
func (a *MaxAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if a.max == nil {
		if err := a.acc.ResizeTo(ctx, int64(datum.Size())); err != nil {
			return err
		}
		a.max = datum
		return nil
	}
	c := a.max.Compare(a.evalCtx, datum)
	if c < 0 {
		a.max = datum
		if a.variableDatumSize {
			if err := a.acc.ResizeTo(ctx, int64(datum.Size())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Result returns the largest value passed to Add.
func (a *MaxAggregate) Result() (tree.Datum, error) {
	if a.max == nil {
		return tree.DNull, nil
	}
	return a.max, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *MaxAggregate) Reset(ctx context.Context) {
	a.max = nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *MaxAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *MaxAggregate) Size() int64 {
	return sizeOfMaxAggregate + int64(a.datumSize)
}

// MinAggregate keeps track of the smallest value passed to Add.
type MinAggregate struct {
	min     tree.Datum
	evalCtx *tree.EvalContext

	acc               mon.BoundAccount
	datumSize         uintptr
	variableDatumSize bool
}

func newMinAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	sz, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a fixed size, it will be included in the size
	// reported by Size(). Otherwise it will be accounted for in Add(). This
	// avoids doing unnecessary memory accounting work for fixed-size datums.
	if variable {
		// Datum size will be accounted for in the Add method.
		sz = 0
	}
	return &MinAggregate{
		evalCtx:           evalCtx,
		acc:               evalCtx.Mon.MakeBoundAccount(),
		datumSize:         sz,
		variableDatumSize: variable,
	}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *MinAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if a.min == nil {
		if err := a.acc.ResizeTo(ctx, int64(datum.Size())); err != nil {
			return err
		}
		a.min = datum
		return nil
	}
	c := a.min.Compare(a.evalCtx, datum)
	if c > 0 {
		a.min = datum
		if a.variableDatumSize {
			if err := a.acc.ResizeTo(ctx, int64(datum.Size())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *MinAggregate) Result() (tree.Datum, error) {
	if a.min == nil {
		return tree.DNull, nil
	}
	return a.min, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *MinAggregate) Reset(context.Context) {
	a.min = nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *MinAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *MinAggregate) Size() int64 {
	return sizeOfMinAggregate + int64(a.datumSize)
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
	// Either the `intSum` and `decSum` fields contains the
	// result. Which one is used is determined by the `large` field
	// below.
	intSum      int64
	decSum      apd.Decimal
	tmpDec      apd.Decimal
	large       bool
	seenNonNull bool
	acc         mon.BoundAccount
}

func newIntSumAggregate(_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &intSumAggregate{acc: evalCtx.Mon.MakeBoundAccount()}
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
				a.decSum.SetFinite(a.intSum, 0)
			}
		}

		if a.large {
			a.tmpDec.SetFinite(t, 0)
			_, err := tree.ExactCtx.Add(&a.decSum, &a.decSum, &a.tmpDec)
			if err != nil {
				return err
			}
			if err := a.acc.ResizeTo(ctx, int64(tree.SizeOfDecimal(a.decSum))); err != nil {
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
		dd.SetFinite(a.intSum, 0)
	}
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *intSumAggregate) Reset(context.Context) {
	// We choose not to reset apd.Decimal's since they will be set to appropriate
	// values when overflow occurs - we simply force the aggregate to use Go
	// types (at least, at first).
	a.seenNonNull = false
	a.intSum = 0
	a.large = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *intSumAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *intSumAggregate) Size() int64 {
	return sizeOfIntSumAggregate
}

type decimalSumAggregate struct {
	sum        apd.Decimal
	sawNonNull bool
	acc        mon.BoundAccount
}

func newDecimalSumAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalSumAggregate{acc: evalCtx.Mon.MakeBoundAccount()}
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

	if err := a.acc.ResizeTo(ctx, int64(tree.SizeOfDecimal(a.sum))); err != nil {
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
	a.sum.SetFinite(0, 0)
	a.sawNonNull = false
	a.acc.Empty(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalSumAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
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

	a.tmpDec.SetFinite(int64(tree.MustBeDInt(datum)), 0)
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
	// Variables used across iterations.
	ed      *apd.ErrDecimal
	count   apd.Decimal
	mean    apd.Decimal
	sqrDiff apd.Decimal

	// Variables used as scratch space within iterations.
	delta apd.Decimal
	tmp   apd.Decimal

	acc mon.BoundAccount
}

func newDecimalSqrDiff(evalCtx *tree.EvalContext) decimalSqrDiff {
	ed := apd.MakeErrDecimal(tree.IntermediateCtx)
	return &decimalSqrDiffAggregate{
		ed:  &ed,
		acc: evalCtx.Mon.MakeBoundAccount(),
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

	size := tree.SizeOfDecimal(a.count) +
		tree.SizeOfDecimal(a.mean) +
		tree.SizeOfDecimal(a.sqrDiff) +
		tree.SizeOfDecimal(a.delta) +
		tree.SizeOfDecimal(a.tmp)
	if err := a.acc.ResizeTo(ctx, int64(size)); err != nil {
		return err
	}

	return a.ed.Err()
}

func (a *decimalSqrDiffAggregate) Result() (tree.Datum, error) {
	if a.count.Cmp(decimalOne) < 0 {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{Decimal: a.sqrDiff}
	// Remove trailing zeros. Depending on the order in which the input
	// is processed, some number of trailing zeros could be added to the
	// output. Remove them so that the results are the same regardless of order.
	dd.Decimal.Reduce(&dd.Decimal)
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *decimalSqrDiffAggregate) Reset(context.Context) {
	a.count.SetFinite(0, 0)
	a.mean.SetFinite(0, 0)
	a.sqrDiff.SetFinite(0, 0)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalSqrDiffAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
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

	acc mon.BoundAccount
}

func newDecimalSumSqrDiffs(evalCtx *tree.EvalContext) decimalSqrDiff {
	ed := apd.MakeErrDecimal(tree.IntermediateCtx)
	return &decimalSumSqrDiffsAggregate{
		ed:  &ed,
		acc: evalCtx.Mon.MakeBoundAccount(),
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

	size := tree.SizeOfDecimal(a.count) +
		tree.SizeOfDecimal(a.mean) +
		tree.SizeOfDecimal(a.sqrDiff) +
		tree.SizeOfDecimal(a.tmpCount) +
		tree.SizeOfDecimal(a.tmpMean) +
		tree.SizeOfDecimal(a.delta) +
		tree.SizeOfDecimal(a.tmp)
	if err := a.acc.ResizeTo(ctx, int64(size)); err != nil {
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
func (a *decimalSumSqrDiffsAggregate) Reset(context.Context) {
	a.count.SetFinite(0, 0)
	a.mean.SetFinite(0, 0)
	a.sqrDiff.SetFinite(0, 0)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalSumSqrDiffsAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
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
	sum        []byte
	sawNonNull bool
}

func newBytesXorAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &bytesXorAggregate{}
}

// Add inserts one value into the running xor.
func (a *bytesXorAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := []byte(*datum.(*tree.DBytes))
	if !a.sawNonNull {
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
func (a *bytesXorAggregate) Reset(context.Context) {
	a.sum = nil
	a.sawNonNull = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *bytesXorAggregate) Close(context.Context) {}

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
	loc        *time.Location
	builder    *json.ArrayBuilderWithCounter
	acc        mon.BoundAccount
	sawNonNull bool
}

func newJSONAggregate(_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &jsonAggregate{
		loc:        evalCtx.GetLocation(),
		builder:    json.NewArrayBuilderWithCounter(),
		acc:        evalCtx.Mon.MakeBoundAccount(),
		sawNonNull: false,
	}
}

// Add accumulates the transformed json into the JSON array.
func (a *jsonAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	j, err := tree.AsJSON(datum, a.loc)
	if err != nil {
		return err
	}
	oldSize := a.builder.Size()
	a.builder.Add(j)
	if err = a.acc.Grow(ctx, int64(a.builder.Size()-oldSize)); err != nil {
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
	a.acc.Empty(ctx)
	a.sawNonNull = false
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *jsonAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *jsonAggregate) Size() int64 {
	return sizeOfJSONAggregate
}
