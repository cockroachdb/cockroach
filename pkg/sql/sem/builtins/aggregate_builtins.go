// Copyright 2015 The Cockroach Authors.
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

package builtins

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

func initAggregateBuiltins() {
	// Add all aggregates to the Builtins map after a few sanity checks.
	for k, v := range Aggregates {
		for i, a := range v {
			if !a.Impure {
				panic(fmt.Sprintf("aggregate functions should all be impure, found %v", a))
			}
			if a.Class != tree.AggregateClass {
				panic(fmt.Sprintf("aggregate functions should be marked with the tree.AggregateClass "+
					"function class, found %v", a))
			}
			if a.AggregateFunc == nil {
				panic(fmt.Sprintf("aggregate functions should have tree.AggregateFunc constructors, "+
					"found %v", a))
			}
			if a.WindowFunc == nil {
				panic(fmt.Sprintf("aggregate functions should have tree.WindowFunc constructors, "+
					"found %v", a))
			}

			// The aggregate functions are considered "row dependent". This is
			// because each aggregate function application receives the set of
			// grouped rows as implicit parameter. It may have a different
			// value in every group, so it cannot be considered constant in
			// the context of a data source.
			v[i].NeedsRepeatedEvaluation = true
		}

		Builtins[k] = v
	}
}

// Aggregates are a special class of builtin functions that are wrapped
// at execution in a bucketing layer to combine (aggregate) the result
// of the function being run over many rows.
// See `aggregateFuncHolder` in the sql package.
// In particular they must not be simplified during normalization
// (and thus must be marked as impure), even when they are given a
// constant argument (e.g. SUM(1)). This is because aggregate
// functions must return NULL when they are no rows in the source
// table, so their evaluation must always be delayed until query
// execution.
// Some aggregate functions must handle nullable arguments, since normalizing
// an aggregate function call to NULL in the presence of a NULL argument may
// not be correct. There are two cases where an aggregate function must handle
// nullable arguments:
// 1) the aggregate function does not skip NULLs (e.g., ARRAY_AGG); and
// 2) the aggregate function does not return NULL when it aggregates no rows
//		(e.g., COUNT).
// Exported for use in documentation.
var Aggregates = map[string][]tree.Builtin{
	"array_agg": arrayBuiltin(func(t types.T) tree.Builtin {
		return makeAggBuiltinWithReturnType(
			[]types.T{t},
			func(args []tree.TypedExpr) types.T {
				if len(args) == 0 {
					return types.TArray{Typ: t}
				}
				// Whenever possible, use the expression's type, so we can properly
				// handle aliased types that don't explicitly have overloads.
				return types.TArray{Typ: args[0].ResolvedType()}
			},
			newArrayAggregate,
			"Aggregates the selected values into an array.",
			true /* NullableArgs */)
	}),

	"avg": {
		makeAggBuiltin([]types.T{types.Int}, types.Decimal, newIntAvgAggregate,
			"Calculates the average of the selected values."),
		makeAggBuiltin([]types.T{types.Float}, types.Float, newFloatAvgAggregate,
			"Calculates the average of the selected values."),
		makeAggBuiltin([]types.T{types.Decimal}, types.Decimal, newDecimalAvgAggregate,
			"Calculates the average of the selected values."),
	},

	"bool_and": {
		makeAggBuiltin([]types.T{types.Bool}, types.Bool, newBoolAndAggregate,
			"Calculates the boolean value of `AND`ing all selected values."),
	},

	"bool_or": {
		makeAggBuiltin([]types.T{types.Bool}, types.Bool, newBoolOrAggregate,
			"Calculates the boolean value of `OR`ing all selected values."),
	},

	"concat_agg": {
		// TODO(knz): When CockroachDB supports STRING_AGG, CONCAT_AGG(X)
		// should be substituted to STRING_AGG(X, '') and executed as
		// such (no need for a separate implementation).
		makeAggBuiltin([]types.T{types.String}, types.String, newStringConcatAggregate,
			"Concatenates all selected values."),
		makeAggBuiltin([]types.T{types.Bytes}, types.Bytes, newBytesConcatAggregate,
			"Concatenates all selected values."),
		// TODO(eisen): support collated strings when the type system properly
		// supports parametric types.
	},

	"count": {
		makeAggBuiltinWithNullableArgs([]types.T{types.Any}, types.Int, newCountAggregate,
			"Calculates the number of selected elements."),
	},

	"count_rows": {
		{
			Impure:        true,
			Class:         tree.AggregateClass,
			Types:         tree.ArgTypes{},
			ReturnType:    tree.FixedReturnType(types.Int),
			AggregateFunc: newCountRowsAggregate,
			WindowFunc: func(params []types.T, evalCtx *tree.EvalContext) tree.WindowFunc {
				return newAggregateWindow(newCountRowsAggregate(params, evalCtx))
			},
			Info: "Calculates the number of rows.",
		},
	},

	"max": collectBuiltins(func(t types.T) tree.Builtin {
		return makeAggBuiltin([]types.T{t}, t, newMaxAggregate,
			"Identifies the maximum selected value.")
	}, types.AnyNonArray...),
	"min": collectBuiltins(func(t types.T) tree.Builtin {
		return makeAggBuiltin([]types.T{t}, t, newMinAggregate,
			"Identifies the minimum selected value.")
	}, types.AnyNonArray...),

	"sum_int": {
		makeAggBuiltin([]types.T{types.Int}, types.Int, newSmallIntSumAggregate,
			"Calculates the sum of the selected values."),
	},

	"sum": {
		makeAggBuiltin([]types.T{types.Int}, types.Decimal, newIntSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggBuiltin([]types.T{types.Float}, types.Float, newFloatSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggBuiltin([]types.T{types.Decimal}, types.Decimal, newDecimalSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggBuiltin([]types.T{types.Interval}, types.Interval, newIntervalSumAggregate,
			"Calculates the sum of the selected values."),
	},

	"sqrdiff": {
		makeAggBuiltin([]types.T{types.Int}, types.Decimal, newIntSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values."),
		makeAggBuiltin([]types.T{types.Decimal}, types.Decimal, newDecimalSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values."),
		makeAggBuiltin([]types.T{types.Float}, types.Float, newFloatSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values."),
	},

	// final_(variance|stddev) computes the global (variance|standard deviation)
	// from an arbitrary collection of local sums of squared difference from the mean.
	// Adapted from https://www.johndcook.com/blog/skewness_kurtosis and
	// https://github.com/cockroachdb/cockroach/pull/17728.

	// The input signature is: SQDIFF, SUM, COUNT
	"final_variance": {
		makeAggBuiltin([]types.T{types.Decimal, types.Decimal, types.Int}, types.Decimal, newDecimalFinalVarianceAggregate,
			"Calculates the variance from the selected locally-computed squared difference values."),
		makeAggBuiltin([]types.T{types.Float, types.Float, types.Int}, types.Float, newFloatFinalVarianceAggregate,
			"Calculates the variance from the selected locally-computed squared difference values."),
	},

	"final_stddev": {
		makeAggBuiltin([]types.T{types.Decimal, types.Decimal, types.Int}, types.Decimal, newDecimalFinalStdDevAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values."),
		makeAggBuiltin([]types.T{types.Float, types.Float, types.Int}, types.Float, newFloatFinalStdDevAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values."),
	},

	"variance": {
		makeAggBuiltin([]types.T{types.Int}, types.Decimal, newIntVarianceAggregate,
			"Calculates the variance of the selected values."),
		makeAggBuiltin([]types.T{types.Decimal}, types.Decimal, newDecimalVarianceAggregate,
			"Calculates the variance of the selected values."),
		makeAggBuiltin([]types.T{types.Float}, types.Float, newFloatVarianceAggregate,
			"Calculates the variance of the selected values."),
	},

	"stddev": {
		makeAggBuiltin([]types.T{types.Int}, types.Decimal, newIntStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
		makeAggBuiltin([]types.T{types.Decimal}, types.Decimal, newDecimalStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
		makeAggBuiltin([]types.T{types.Float}, types.Float, newFloatStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
	},

	"xor_agg": {
		makeAggBuiltin([]types.T{types.Bytes}, types.Bytes, newBytesXorAggregate,
			"Calculates the bitwise XOR of the selected values."),
		makeAggBuiltin([]types.T{types.Int}, types.Int, newIntXorAggregate,
			"Calculates the bitwise XOR of the selected values."),
	},

	"json_agg": {
		makeAggBuiltinWithNullableArgs([]types.T{types.Any}, types.JSON, newJSONAggregate,
			"aggregates values as a JSON or JSONB array"),
	},

	"jsonb_agg": {
		makeAggBuiltinWithNullableArgs([]types.T{types.Any}, types.JSON, newJSONAggregate,
			"aggregates values as a JSON or JSONB array"),
	},
}

func makeAggBuiltin(
	in []types.T, ret types.T, f func([]types.T, *tree.EvalContext) tree.AggregateFunc, info string,
) tree.Builtin {
	return makeAggBuiltinWithReturnType(
		in, tree.FixedReturnType(ret), f, info, false /* nullableArgs */)
}

func makeAggBuiltinWithNullableArgs(
	in []types.T, ret types.T, f func([]types.T, *tree.EvalContext) tree.AggregateFunc, info string,
) tree.Builtin {
	return makeAggBuiltinWithReturnType(
		in, tree.FixedReturnType(ret), f, info, true /* nullableArgs */)
}

func makeAggBuiltinWithReturnType(
	in []types.T,
	retType tree.ReturnTyper,
	f func([]types.T, *tree.EvalContext) tree.AggregateFunc,
	info string,
	nullableArgs bool,
) tree.Builtin {
	argTypes := make(tree.ArgTypes, len(in))
	for i, typ := range in {
		argTypes[i].Name = fmt.Sprintf("arg%d", i+1)
		argTypes[i].Typ = typ
	}

	return tree.Builtin{
		// See the comment about aggregate functions in the definitions
		// of the Builtins array above.
		Impure:        true,
		Class:         tree.AggregateClass,
		Types:         argTypes,
		ReturnType:    retType,
		AggregateFunc: f,
		WindowFunc: func(params []types.T, evalCtx *tree.EvalContext) tree.WindowFunc {
			return newAggregateWindow(f(params, evalCtx))
		},
		Info:         info,
		NullableArgs: nullableArgs,
	}
}

var _ tree.AggregateFunc = &arrayAggregate{}
var _ tree.AggregateFunc = &avgAggregate{}
var _ tree.AggregateFunc = &countAggregate{}
var _ tree.AggregateFunc = &MaxAggregate{}
var _ tree.AggregateFunc = &MinAggregate{}
var _ tree.AggregateFunc = &intSumAggregate{}
var _ tree.AggregateFunc = &decimalSumAggregate{}
var _ tree.AggregateFunc = &floatSumAggregate{}
var _ tree.AggregateFunc = &intSqrDiffAggregate{}
var _ tree.AggregateFunc = &floatSqrDiffAggregate{}
var _ tree.AggregateFunc = &decimalSqrDiffAggregate{}
var _ tree.AggregateFunc = &floatSumSqrDiffsAggregate{}
var _ tree.AggregateFunc = &decimalSumSqrDiffsAggregate{}
var _ tree.AggregateFunc = &floatVarianceAggregate{}
var _ tree.AggregateFunc = &decimalVarianceAggregate{}
var _ tree.AggregateFunc = &floatStdDevAggregate{}
var _ tree.AggregateFunc = &decimalStdDevAggregate{}
var _ tree.AggregateFunc = &identAggregate{}
var _ tree.AggregateFunc = &concatAggregate{}
var _ tree.AggregateFunc = &bytesXorAggregate{}
var _ tree.AggregateFunc = &intXorAggregate{}

// In order to render the unaggregated (i.e. grouped) fields, during aggregation,
// the values for those fields have to be stored for each bucket.
// The `identAggregate` provides an "aggregate" function that actually
// just returns the last value passed to `add`, unchanged. For accumulating
// and rendering though it behaves like the other aggregate functions,
// allowing both those steps to avoid special-casing grouped vs aggregated fields.
type identAggregate struct {
	val tree.Datum
}

// NewIdentAggregate returns an identAggregate (see comment on struct).
func NewIdentAggregate(*tree.EvalContext) tree.AggregateFunc {
	return &identAggregate{}
}

// Add sets the value to the passed datum.
func (a *identAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	// If we see at least one non-NULL value, ignore any NULLs.
	// This is used in distributed multi-stage aggregations, where a local stage
	// with multiple (parallel) instances feeds into a final stage. If some of the
	// instances see no rows, they emit a NULL; the final IDENT aggregator needs
	// to ignore these.
	// TODO(radu): this - along with other hacks like special-handling of the nil
	// result in (*aggregateGroupHolder).Eval - illustrates why IDENT as an
	// aggregator is not a sound. We should remove this concept and handle GROUP
	// BY columns separately in the groupNode and the aggregator processor
	// (#12525).
	if a.val == nil || datum != tree.DNull {
		a.val = datum
	}
	return nil
}

// Result returns the value most recently passed to Add.
func (a *identAggregate) Result() (tree.Datum, error) {
	// It is significant that identAggregate returns nil, and not tree.DNull,
	// if no result was known via Add(). See
	// sql.(*aggregateFuncHolder).Eval() for details.
	return a.val, nil
}

// Close is no-op in aggregates using constant space.
func (a *identAggregate) Close(context.Context) {}

type arrayAggregate struct {
	arr *tree.DArray
	acc mon.BoundAccount
}

func newArrayAggregate(params []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
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

// Result returns an array of all datums passed to Add.
func (a *arrayAggregate) Result() (tree.Datum, error) {
	if len(a.arr.Array) > 0 {
		return a.arr, nil
	}
	return tree.DNull, nil
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *arrayAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

type avgAggregate struct {
	agg   tree.AggregateFunc
	count int
}

func newIntAvgAggregate(params []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &avgAggregate{agg: newIntSumAggregate(params, evalCtx)}
}
func newFloatAvgAggregate(params []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &avgAggregate{agg: newFloatSumAggregate(params, evalCtx)}
}
func newDecimalAvgAggregate(params []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &avgAggregate{agg: newDecimalSumAggregate(params, evalCtx)}
}

// Add accumulates the passed datum into the average.
func (a *avgAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
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
	default:
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected SUM result type: %s", t)
	}
}

// Close is part of the tree.AggregateFunc interface.
func (a *avgAggregate) Close(context.Context) {}

type concatAggregate struct {
	forBytes   bool
	sawNonNull bool
	result     bytes.Buffer
	acc        mon.BoundAccount
}

func newBytesConcatAggregate(_ []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &concatAggregate{
		forBytes: true,
		acc:      evalCtx.Mon.MakeBoundAccount(),
	}
}
func newStringConcatAggregate(_ []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &concatAggregate{acc: evalCtx.Mon.MakeBoundAccount()}
}

func (a *concatAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	a.sawNonNull = true
	var arg string
	if a.forBytes {
		arg = string(*datum.(*tree.DBytes))
	} else {
		arg = string(tree.MustBeDString(datum))
	}
	if err := a.acc.Grow(ctx, int64(datum.Size())); err != nil {
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

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *concatAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

type boolAndAggregate struct {
	sawNonNull bool
	result     bool
}

func newBoolAndAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
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

// Close is part of the tree.AggregateFunc interface.
func (a *boolAndAggregate) Close(context.Context) {}

type boolOrAggregate struct {
	sawNonNull bool
	result     bool
}

func newBoolOrAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
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

// Close is part of the tree.AggregateFunc interface.
func (a *boolOrAggregate) Close(context.Context) {}

type countAggregate struct {
	count int
}

func newCountAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
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

// Close is part of the tree.AggregateFunc interface.
func (a *countAggregate) Close(context.Context) {}

type countRowsAggregate struct {
	count int
}

func newCountRowsAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
	return &countRowsAggregate{}
}

func (a *countRowsAggregate) Add(_ context.Context, _ tree.Datum, _ ...tree.Datum) error {
	a.count++
	return nil
}

func (a *countRowsAggregate) Result() (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(a.count)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *countRowsAggregate) Close(context.Context) {}

// MaxAggregate keeps track of the largest value passed to Add.
type MaxAggregate struct {
	max     tree.Datum
	evalCtx *tree.EvalContext
}

func newMaxAggregate(_ []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &MaxAggregate{evalCtx: evalCtx}
}

// Add sets the max to the larger of the current max or the passed datum.
func (a *MaxAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if a.max == nil {
		a.max = datum
		return nil
	}
	c := a.max.Compare(a.evalCtx, datum)
	if c < 0 {
		a.max = datum
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

// Close is part of the tree.AggregateFunc interface.
func (a *MaxAggregate) Close(context.Context) {}

// MinAggregate keeps track of the smallest value passed to Add.
type MinAggregate struct {
	min     tree.Datum
	evalCtx *tree.EvalContext
}

func newMinAggregate(_ []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &MinAggregate{evalCtx: evalCtx}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *MinAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if a.min == nil {
		a.min = datum
		return nil
	}
	c := a.min.Compare(a.evalCtx, datum)
	if c > 0 {
		a.min = datum
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

// Close is part of the tree.AggregateFunc interface.
func (a *MinAggregate) Close(context.Context) {}

type smallIntSumAggregate struct {
	sum         int64
	seenNonNull bool
}

func newSmallIntSumAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
	return &smallIntSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *smallIntSumAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}

	a.sum += int64(tree.MustBeDInt(datum))
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

// Close is part of the tree.AggregateFunc interface.
func (a *smallIntSumAggregate) Close(context.Context) {}

type intSumAggregate struct {
	// Either the `intSum` and `decSum` fields contains the
	// result. Which one is used is determined by the `large` field
	// below.
	intSum      int64
	decSum      tree.DDecimal
	tmpDec      apd.Decimal
	large       bool
	seenNonNull bool
}

func newIntSumAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
	return &intSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *intSumAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
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
			r, ok := tree.AddWithOverflow(a.intSum, t)
			if ok {
				a.intSum = r
			} else {
				// And overflow was detected; go to large integers, but keep the
				// sum computed so far.
				a.large = true
				a.decSum.SetCoefficient(a.intSum)
			}
		}

		if a.large {
			a.tmpDec.SetCoefficient(t)
			_, err := tree.ExactCtx.Add(&a.decSum.Decimal, &a.decSum.Decimal, &a.tmpDec)
			if err != nil {
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
		dd.Set(&a.decSum.Decimal)
	} else {
		dd.SetCoefficient(a.intSum)
	}
	return dd, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *intSumAggregate) Close(context.Context) {}

type decimalSumAggregate struct {
	sum        apd.Decimal
	sawNonNull bool
}

func newDecimalSumAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
	return &decimalSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *decimalSumAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := datum.(*tree.DDecimal)
	_, err := tree.ExactCtx.Add(&a.sum, &a.sum, &t.Decimal)
	if err != nil {
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

// Close is part of the tree.AggregateFunc interface.
func (a *decimalSumAggregate) Close(context.Context) {}

type floatSumAggregate struct {
	sum        float64
	sawNonNull bool
}

func newFloatSumAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
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

// Close is part of the tree.AggregateFunc interface.
func (a *floatSumAggregate) Close(context.Context) {}

type intervalSumAggregate struct {
	sum        duration.Duration
	sawNonNull bool
}

func newIntervalSumAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
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

// Close is part of the tree.AggregateFunc interface.
func (a *intervalSumAggregate) Close(context.Context) {}

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

func newIntSqrDiff() decimalSqrDiff {
	return &intSqrDiffAggregate{agg: newDecimalSqrDiff()}
}

func newIntSqrDiffAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
	return newIntSqrDiff()
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

	a.tmpDec.SetCoefficient(int64(tree.MustBeDInt(datum)))
	return a.agg.Add(ctx, &a.tmpDec)
}

func (a *intSqrDiffAggregate) Result() (tree.Datum, error) {
	return a.agg.Result()
}

// Close is part of the tree.AggregateFunc interface.
func (a *intSqrDiffAggregate) Close(context.Context) {}

type floatSqrDiffAggregate struct {
	count   int64
	mean    float64
	sqrDiff float64
}

func newFloatSqrDiff() floatSqrDiff {
	return &floatSqrDiffAggregate{}
}

func newFloatSqrDiffAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
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

// Close is part of the tree.AggregateFunc interface.
func (a *floatSqrDiffAggregate) Close(context.Context) {}

type decimalSqrDiffAggregate struct {
	// Variables used across iterations.
	ed      *apd.ErrDecimal
	count   apd.Decimal
	mean    apd.Decimal
	sqrDiff apd.Decimal

	// Variables used as scratch space within iterations.
	delta apd.Decimal
	tmp   apd.Decimal
}

func newDecimalSqrDiff() decimalSqrDiff {
	ed := apd.MakeErrDecimal(tree.IntermediateCtx)
	return &decimalSqrDiffAggregate{ed: &ed}
}

func newDecimalSqrDiffAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
	return newDecimalSqrDiff()
}

// Count is part of the decimalSqrDiff interface.
func (a *decimalSqrDiffAggregate) Count() *apd.Decimal {
	return &a.count
}

// Tmp is part of the decimalSqrDiff interface.
func (a *decimalSqrDiffAggregate) Tmp() *apd.Decimal {
	return &a.tmp
}

func (a *decimalSqrDiffAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
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

// Close is part of the tree.AggregateFunc interface.
func (a *decimalSqrDiffAggregate) Close(context.Context) {}

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
	totalCount, ok := tree.AddWithOverflow(a.count, count)
	if !ok {
		return pgerror.NewErrorf(pgerror.CodeNumericValueOutOfRangeError, "number of values in aggregate exceed max count of %d", math.MaxInt64)
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

// Close is part of the tree.AggregateFunc interface.
func (a *floatSumSqrDiffsAggregate) Close(context.Context) {}

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
}

func newDecimalSumSqrDiffs() decimalSqrDiff {
	ed := apd.MakeErrDecimal(tree.IntermediateCtx)
	return &decimalSumSqrDiffsAggregate{ed: &ed}
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
	_ context.Context, sqrDiffD tree.Datum, otherArgs ...tree.Datum,
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

	return a.ed.Err()
}

func (a *decimalSumSqrDiffsAggregate) Result() (tree.Datum, error) {
	if a.count.Cmp(decimalOne) < 0 {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{Decimal: a.sqrDiff}
	return dd, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalSumSqrDiffsAggregate) Close(context.Context) {}

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
func newIntVarianceAggregate(params []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &decimalVarianceAggregate{agg: newIntSqrDiff()}
}

func newFloatVarianceAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
	return &floatVarianceAggregate{agg: newFloatSqrDiff()}
}

func newDecimalVarianceAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
	return &decimalVarianceAggregate{agg: newDecimalSqrDiff()}
}

func newFloatFinalVarianceAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
	return &floatVarianceAggregate{agg: newFloatSumSqrDiffs()}
}

func newDecimalFinalVarianceAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
	return &decimalVarianceAggregate{agg: newDecimalSumSqrDiffs()}
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

// Close is part of the tree.AggregateFunc interface.
func (a *floatVarianceAggregate) Close(context.Context) {}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalVarianceAggregate) Close(context.Context) {}

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
func newIntStdDevAggregate(params []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &decimalStdDevAggregate{agg: newIntVarianceAggregate(params, evalCtx)}
}

func newFloatStdDevAggregate(params []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &floatStdDevAggregate{agg: newFloatVarianceAggregate(params, evalCtx)}
}

func newDecimalStdDevAggregate(params []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &decimalStdDevAggregate{agg: newDecimalVarianceAggregate(params, evalCtx)}
}

func newFloatFinalStdDevAggregate(params []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &floatStdDevAggregate{agg: newFloatFinalVarianceAggregate(params, evalCtx)}
}

func newDecimalFinalStdDevAggregate(
	params []types.T, evalCtx *tree.EvalContext,
) tree.AggregateFunc {
	return &decimalStdDevAggregate{agg: newDecimalFinalVarianceAggregate(params, evalCtx)}
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

// Close is part of the tree.AggregateFunc interface.
func (a *floatStdDevAggregate) Close(context.Context) {}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalStdDevAggregate) Close(context.Context) {}

type bytesXorAggregate struct {
	sum        []byte
	sawNonNull bool
}

func newBytesXorAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
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
		return pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "arguments to xor must all be the same length %d vs %d", len(a.sum), len(t))
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

// Close is part of the tree.AggregateFunc interface.
func (a *bytesXorAggregate) Close(context.Context) {}

type intXorAggregate struct {
	sum        int64
	sawNonNull bool
}

func newIntXorAggregate(_ []types.T, _ *tree.EvalContext) tree.AggregateFunc {
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

// Close is part of the tree.AggregateFunc interface.
func (a *intXorAggregate) Close(context.Context) {}

type jsonAggregate struct {
	builder    *json.ArrayBuilderWithCounter
	acc        mon.BoundAccount
	sawNonNull bool
}

func newJSONAggregate(params []types.T, evalCtx *tree.EvalContext) tree.AggregateFunc {
	return &jsonAggregate{
		builder:    json.NewArrayBuilderWithCounter(),
		acc:        evalCtx.Mon.MakeBoundAccount(),
		sawNonNull: false,
	}
}

// Add accumulates the transformed json into the JSON array.
func (a *jsonAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	j, err := asJSON(datum)
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

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *jsonAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}
