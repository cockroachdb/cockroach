// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execagg

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// AggregateConstructor is a function that creates an aggregate function.
type AggregateConstructor func(*eval.Context, tree.Datums) eval.AggregateFunc

// getAggregateInfo returns the aggregate constructor and the return type for
// the given aggregate function when applied on the given types.
//
// inputTypes cannot be mutated if aggregateConstructor will be used.
func getAggregateInfo(
	fn execinfrapb.AggregatorSpec_Func, paramTypes []*types.T,
) (aggregateConstructor AggregateConstructor, returnType *types.T, err error) {
	if fn == execinfrapb.AnyNotNull {
		// The ANY_NOT_NULL builtin does not have a fixed return type;
		// handle it separately.
		if len(paramTypes) != 1 {
			return nil, nil, errors.Errorf("any_not_null aggregate needs 1 input")
		}
		return builtins.NewAnyNotNullAggregate, paramTypes[0], nil
	}

	_, builtins := builtinsregistry.GetBuiltinProperties(strings.ToLower(fn.String()))
	for _, b := range builtins {
		typs := b.Types.Types()
		if len(typs) != len(paramTypes) {
			continue
		}
		match := true
		for i, t := range typs {
			if !paramTypes[i].Equivalent(t) {
				if b.CalledOnNullInput && paramTypes[i].IsAmbiguous() {
					continue
				}
				match = false
				break
			}
		}
		if match {
			// Found!
			constructAgg := func(evalCtx *eval.Context, arguments tree.Datums) eval.AggregateFunc {
				return b.AggregateFunc.(eval.AggregateOverload)(paramTypes, evalCtx, arguments)
			}
			colTyp := b.InferReturnTypeFromInputArgTypes(paramTypes)
			return constructAgg, colTyp, nil
		}
	}
	return nil, nil, errors.Errorf(
		"no builtin aggregate for %s on %+v", fn, paramTypes,
	)
}

// GetAggregateConstructor processes the specification of a single aggregate
// function.
//
// evalCtx will not be mutated.
func GetAggregateConstructor(
	ctx context.Context,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	aggInfo *execinfrapb.AggregatorSpec_Aggregation,
	inputTypes []*types.T,
	pAlloc *ParamTypesAllocator,
) (constructor AggregateConstructor, arguments tree.Datums, outputType *types.T, err error) {
	paramTypes, err := pAlloc.alloc(len(aggInfo.ColIdx) + len(aggInfo.Arguments))
	if err != nil {
		return nil, nil, nil, err
	}
	for j, c := range aggInfo.ColIdx {
		if c >= uint32(len(inputTypes)) {
			err = errors.Errorf("ColIdx out of range (%d)", aggInfo.ColIdx)
			return
		}
		paramTypes[j] = inputTypes[c]
	}
	arguments = make(tree.Datums, len(aggInfo.Arguments))
	var d tree.Datum
	for j, argument := range aggInfo.Arguments {
		h := execinfrapb.ExprHelper{}
		// Pass nil types and row - there are no variables in these expressions.
		if err = h.Init(ctx, argument, nil /* types */, semaCtx, evalCtx); err != nil {
			err = errors.Wrapf(err, "%s", argument)
			return
		}
		d, err = h.Eval(ctx, nil /* row */)
		if err != nil {
			err = errors.Wrapf(err, "%s", argument)
			return
		}
		paramTypes[len(aggInfo.ColIdx)+j] = d.ResolvedType()
		arguments[j] = d
	}
	constructor, outputType, err = getAggregateInfo(aggInfo.Func, paramTypes)
	return
}

// ParamTypesAllocator is a helper struct for batching allocations of aggregate
// function parameter types.
type ParamTypesAllocator struct {
	typs []*types.T
}

// MakeParamTypesAllocator creates a new ParamTypesAllocator with enough space
// to allocate slices of parameter types for all the given aggregate functions.
func MakeParamTypesAllocator(
	aggregations []execinfrapb.AggregatorSpec_Aggregation,
) ParamTypesAllocator {
	numParamTypes := 0
	for _, aggFn := range aggregations {
		numParamTypes += len(aggFn.ColIdx)
		numParamTypes += len(aggFn.Arguments)
	}
	return ParamTypesAllocator{
		typs: make([]*types.T, numParamTypes),
	}
}

// alloc returns a slice of types.T of length n. It returns a slice of the
// allocator's type slice and returns an error if the allocator's type slice is
// not long enough.
func (p *ParamTypesAllocator) alloc(n int) (_ []*types.T, err error) {
	if len(p.typs) < n {
		return nil, errors.AssertionFailedf("not enough types; requested %d, have %d", n, len(p.typs))
	}
	res := p.typs[:n:n]
	p.typs = p.typs[n:]
	return res, nil
}

// GetAggregateOutputType returns the output type for the given aggregate
// function when applied on the given types.
//
// inputTypes argument can be mutated by the caller.
func GetAggregateOutputType(
	fn execinfrapb.AggregatorSpec_Func, paramTypes []*types.T,
) (outputType *types.T, err error) {
	_, outputType, err = getAggregateInfo(fn, paramTypes)
	return outputType, err
}

// GetWindowFunctionInfo returns windowFunc constructor and the return type
// when given fn is applied to given inputTypes.
func GetWindowFunctionInfo(
	fn execinfrapb.WindowerSpec_Func, inputTypes ...*types.T,
) (windowConstructor func(*eval.Context) eval.WindowFunc, returnType *types.T, err error) {
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
	_, builtins := builtinsregistry.GetBuiltinProperties(strings.ToLower(funcStr))
	for _, b := range builtins {
		typs := b.Types.Types()
		if len(typs) != len(inputTypes) {
			continue
		}
		match := true
		for i, t := range typs {
			if !inputTypes[i].Equivalent(t) {
				if b.CalledOnNullInput && inputTypes[i].IsAmbiguous() {
					continue
				}
				match = false
				break
			}
		}
		if match {
			// Found!
			constructAgg := func(evalCtx *eval.Context) eval.WindowFunc {
				return b.WindowFunc.(eval.WindowOverload)(inputTypes, evalCtx)
			}
			colTyp := b.InferReturnTypeFromInputArgTypes(inputTypes)
			return constructAgg, colTyp, nil
		}
	}
	return nil, nil, errors.Errorf(
		"no builtin aggregate/window function for %s on %v", funcStr, inputTypes,
	)
}

// NeedHashAggregator returns whether the given aggregator spec requires hash
// aggregation.
func NeedHashAggregator(aggSpec *execinfrapb.AggregatorSpec) (bool, error) {
	var groupCols, orderedCols intsets.Fast
	for _, col := range aggSpec.OrderedGroupCols {
		orderedCols.Add(int(col))
	}
	for _, col := range aggSpec.GroupCols {
		if !orderedCols.Contains(int(col)) {
			return true, nil
		}
		groupCols.Add(int(col))
	}
	if !orderedCols.SubsetOf(groupCols) {
		return false, errors.AssertionFailedf("ordered cols must be a subset of grouping cols")
	}
	return false, nil
}
