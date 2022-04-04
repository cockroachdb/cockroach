// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// AggregateConstructor is a function that creates an aggregate function.
type AggregateConstructor func(*tree.EvalContext, tree.Datums) tree.AggregateFunc

// GetAggregateInfo returns the aggregate constructor and the return type for
// the given aggregate function when applied on the given type.
func GetAggregateInfo(
	fn execinfrapb.AggregatorSpec_Func, inputTypes ...*types.T,
) (aggregateConstructor AggregateConstructor, returnType *types.T, err error) {
	if fn == execinfrapb.AnyNotNull {
		// The ANY_NOT_NULL builtin does not have a fixed return type;
		// handle it separately.
		if len(inputTypes) != 1 {
			return nil, nil, errors.Errorf("any_not_null aggregate needs 1 input")
		}
		return builtins.NewAnyNotNullAggregate, inputTypes[0], nil
	}

	props, builtins := builtins.GetBuiltinProperties(strings.ToLower(fn.String()))
	for _, b := range builtins {
		typs := b.Types.Types()
		if len(typs) != len(inputTypes) {
			continue
		}
		match := true
		for i, t := range typs {
			if !inputTypes[i].Equivalent(t) {
				if props.NullableArgs && inputTypes[i].IsAmbiguous() {
					continue
				}
				match = false
				break
			}
		}
		if match {
			// Found!
			constructAgg := func(evalCtx *tree.EvalContext, arguments tree.Datums) tree.AggregateFunc {
				return b.AggregateFunc(inputTypes, evalCtx, arguments)
			}
			colTyp := b.InferReturnTypeFromInputArgTypes(inputTypes)
			return constructAgg, colTyp, nil
		}
	}
	return nil, nil, errors.Errorf(
		"no builtin aggregate for %s on %+v", fn, inputTypes,
	)
}

// GetAggregateConstructor processes the specification of a single aggregate
// function.
//
// evalCtx will not be mutated.
func GetAggregateConstructor(
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	aggInfo *execinfrapb.AggregatorSpec_Aggregation,
	inputTypes []*types.T,
) (constructor AggregateConstructor, arguments tree.Datums, outputType *types.T, err error) {
	argTypes := make([]*types.T, len(aggInfo.ColIdx)+len(aggInfo.Arguments))
	for j, c := range aggInfo.ColIdx {
		if c >= uint32(len(inputTypes)) {
			err = errors.Errorf("ColIdx out of range (%d)", aggInfo.ColIdx)
			return
		}
		argTypes[j] = inputTypes[c]
	}
	arguments = make(tree.Datums, len(aggInfo.Arguments))
	var d tree.Datum
	for j, argument := range aggInfo.Arguments {
		h := execinfrapb.ExprHelper{}
		// Pass nil types and row - there are no variables in these expressions.
		if err = h.Init(argument, nil /* types */, semaCtx, evalCtx); err != nil {
			err = errors.Wrapf(err, "%s", argument)
			return
		}
		d, err = h.Eval(nil /* row */)
		if err != nil {
			err = errors.Wrapf(err, "%s", argument)
			return
		}
		argTypes[len(aggInfo.ColIdx)+j] = d.ResolvedType()
		arguments[j] = d
	}
	constructor, outputType, err = GetAggregateInfo(aggInfo.Func, argTypes...)
	return
}

// GetWindowFunctionInfo returns windowFunc constructor and the return type
// when given fn is applied to given inputTypes.
func GetWindowFunctionInfo(
	fn execinfrapb.WindowerSpec_Func, inputTypes ...*types.T,
) (windowConstructor func(*tree.EvalContext) tree.WindowFunc, returnType *types.T, err error) {
	if fn.AggregateFunc != nil && *fn.AggregateFunc == execinfrapb.AnyNotNull {
		// The ANY_NOT_NULL builtin does not have a fixed return type;
		// handle it separately.
		if len(inputTypes) != 1 {
			return nil, nil, errors.Errorf("any_not_null aggregate needs 1 input")
		}
		return builtins.NewAggregateWindowFunc(builtins.NewAnyNotNullAggregate), inputTypes[0], nil
	}

	var funcStr string
	if fn.AggregateFunc != nil {
		funcStr = fn.AggregateFunc.String()
	} else if fn.WindowFunc != nil {
		funcStr = fn.WindowFunc.String()
	} else {
		return nil, nil, errors.Errorf(
			"function is neither an aggregate nor a window function",
		)
	}
	props, builtins := builtins.GetBuiltinProperties(strings.ToLower(funcStr))
	for _, b := range builtins {
		typs := b.Types.Types()
		if len(typs) != len(inputTypes) {
			continue
		}
		match := true
		for i, t := range typs {
			if !inputTypes[i].Equivalent(t) {
				if props.NullableArgs && inputTypes[i].IsAmbiguous() {
					continue
				}
				match = false
				break
			}
		}
		if match {
			// Found!
			constructAgg := func(evalCtx *tree.EvalContext) tree.WindowFunc {
				return b.WindowFunc(inputTypes, evalCtx)
			}
			colTyp := b.InferReturnTypeFromInputArgTypes(inputTypes)
			return constructAgg, colTyp, nil
		}
	}
	return nil, nil, errors.Errorf(
		"no builtin aggregate/window function for %s on %v", funcStr, inputTypes,
	)
}
