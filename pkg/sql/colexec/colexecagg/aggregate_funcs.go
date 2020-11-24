// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

// IsAggOptimized returns whether aggFn has an optimized implementation.
func IsAggOptimized(aggFn execinfrapb.AggregatorSpec_Func) bool {
	switch aggFn {
	case execinfrapb.AggregatorSpec_ANY_NOT_NULL,
		execinfrapb.AggregatorSpec_AVG,
		execinfrapb.AggregatorSpec_SUM,
		execinfrapb.AggregatorSpec_SUM_INT,
		execinfrapb.AggregatorSpec_CONCAT_AGG,
		execinfrapb.AggregatorSpec_COUNT_ROWS,
		execinfrapb.AggregatorSpec_COUNT,
		execinfrapb.AggregatorSpec_MIN,
		execinfrapb.AggregatorSpec_MAX,
		execinfrapb.AggregatorSpec_BOOL_AND,
		execinfrapb.AggregatorSpec_BOOL_OR:
		return true
	default:
		return false
	}
}

// AggregateFunc is an aggregate function that performs computation on a batch
// when Compute(batch) is called and writes the output to the Vec passed in
// in Init. The AggregateFunc performs an aggregation per group and outputs the
// aggregation once the start of the new group is reached. If the end of the
// group is not reached before the batch is finished, the AggregateFunc will
// store a carry value that it will use next time Compute is called. Note that
// this carry value is stored at the output index. Therefore if any memory
// modification of the output vector is made, the caller *MUST* copy the value
// at the current index inclusive for a correct aggregation.
type AggregateFunc interface {
	// Init sets the groups for the aggregation and the output vector. Each index
	// in groups corresponds to a column value in the input batch. true represents
	// the start of a new group. Note that the very first group in the whole
	// input should *not* be marked as a start of a new group.
	Init(groups []bool, vec coldata.Vec)

	// Reset resets the aggregate function for another run. Primarily used for
	// benchmarks.
	Reset()

	// CurrentOutputIndex returns the current index in the output vector that the
	// aggregate function is writing to. All indices < the index returned are
	// finished aggregations for previous groups. A negative index may be returned
	// to signify an aggregate function that has not yet performed any
	// computation.
	CurrentOutputIndex() int
	// SetOutputIndex sets the output index to write to. The value for the current
	// index is carried over.
	SetOutputIndex(idx int)

	// Compute computes the aggregation on the input batch.
	// Note: the implementations should be careful to account for their memory
	// usage.
	// Note: inputLen is assumed to be greater than zero.
	Compute(vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int)

	// Flush flushes the result of aggregation on the last group. It should be
	// called once after input batches have been Compute()'d. outputIdx is only
	// used in case of hash aggregation - for ordered aggregation the aggregate
	// function itself should maintain the output index to write to.
	// The caller *must* ensure that the memory accounting is done on the
	// output vector of the aggregate function.
	// Note: the implementations are free to not account for the memory used
	// for the result of aggregation of the last group.
	Flush(outputIdx int)

	// HandleEmptyInputScalar populates the output for a case of an empty input
	// when the aggregate function is in scalar context. The output must always
	// be a single value (either null or zero, depending on the function).
	HandleEmptyInputScalar()
}

type orderedAggregateFuncBase struct {
	groups []bool
	// curIdx tracks the current output index of this function.
	curIdx int
	// nulls is the nulls vector of the output vector of this function.
	nulls *coldata.Nulls
}

func (o *orderedAggregateFuncBase) Init(groups []bool, vec coldata.Vec) {
	o.groups = groups
	o.nulls = vec.Nulls()
}

func (o *orderedAggregateFuncBase) Reset() {
	o.curIdx = 0
	o.nulls.UnsetNulls()
}

func (o *orderedAggregateFuncBase) CurrentOutputIndex() int {
	return o.curIdx
}

func (o *orderedAggregateFuncBase) SetOutputIndex(idx int) {
	o.curIdx = idx
}

func (o *orderedAggregateFuncBase) HandleEmptyInputScalar() {
	// Most aggregate functions return a single NULL value on an empty input
	// in the scalar context (the exceptions are COUNT aggregates which need
	// to overwrite this method).
	o.nulls.SetNull(0)
}

type hashAggregateFuncBase struct {
	// nulls is the nulls vector of the output vector of this function.
	nulls *coldata.Nulls
}

func (h *hashAggregateFuncBase) Init(_ []bool, vec coldata.Vec) {
	h.nulls = vec.Nulls()
}

func (h *hashAggregateFuncBase) Reset() {
	h.nulls.UnsetNulls()
}

func (h *hashAggregateFuncBase) CurrentOutputIndex() int {
	colexecerror.InternalError(errors.AssertionFailedf("CurrentOutputIndex called with hash aggregation"))
	// This code is unreachable, but the compiler cannot infer that.
	return 0
}

func (h *hashAggregateFuncBase) SetOutputIndex(int) {
	colexecerror.InternalError(errors.AssertionFailedf("SetOutputIndex called with hash aggregation"))
}

func (h *hashAggregateFuncBase) HandleEmptyInputScalar() {
	colexecerror.InternalError(errors.AssertionFailedf("HandleEmptyInputScalar called with hash aggregation"))
}

// aggregateFuncAlloc is an aggregate function allocator that pools allocations
// of the structs of the same statically-typed aggregate function.
type aggregateFuncAlloc interface {
	// newAggFunc returns the aggregate function from the pool with all
	// necessary fields initialized.
	newAggFunc() AggregateFunc
}

// AggregateFuncsAlloc is a utility struct that pools allocations of multiple
// aggregate functions simultaneously (i.e. it supports a "schema of aggregate
// functions"). It will resolve the aggregate functions in its constructor to
// instantiate aggregateFuncAlloc objects and will use those to populate slices
// of new aggregation functions when requested.
type AggregateFuncsAlloc struct {
	allocator *colmem.Allocator
	// allocSize determines the number of objects allocated when the previous
	// allocations have been used up.
	allocSize int64
	// returnFuncs is the pool for the slice to be returned in
	// makeAggregateFuncs.
	returnFuncs []AggregateFunc
	// aggFuncAllocs are all necessary aggregate function allocators. Note that
	// a separate aggregateFuncAlloc will be created for each aggFn from the
	// schema (even if there are "duplicates" - exactly the same functions - in
	// the function schema).
	aggFuncAllocs []aggregateFuncAlloc
}

// NewAggregateFuncsAlloc returns a new AggregateFuncsAlloc.
func NewAggregateFuncsAlloc(
	allocator *colmem.Allocator,
	inputTypes []*types.T,
	spec *execinfrapb.AggregatorSpec,
	evalCtx *tree.EvalContext,
	constructors []execinfrapb.AggregateConstructor,
	constArguments []tree.Datums,
	outputTypes []*types.T,
	allocSize int64,
	isHashAgg bool,
) (*AggregateFuncsAlloc, *colconv.VecToDatumConverter, colexecbase.Closers, error) {
	funcAllocs := make([]aggregateFuncAlloc, len(spec.Aggregations))
	var toClose colexecbase.Closers
	var vecIdxsToConvert []int
	for _, aggFn := range spec.Aggregations {
		if !IsAggOptimized(aggFn.Func) {
			for _, vecIdx := range aggFn.ColIdx {
				found := false
				for i := range vecIdxsToConvert {
					if vecIdxsToConvert[i] == int(vecIdx) {
						found = true
						break
					}
				}
				if !found {
					vecIdxsToConvert = append(vecIdxsToConvert, int(vecIdx))
				}
			}
		}
	}
	inputArgsConverter := colconv.NewVecToDatumConverter(len(inputTypes), vecIdxsToConvert)
	for i, aggFn := range spec.Aggregations {
		var err error
		switch aggFn.Func {
		case execinfrapb.AggregatorSpec_ANY_NOT_NULL:
			if isHashAgg {
				funcAllocs[i], err = newAnyNotNullHashAggAlloc(allocator, inputTypes[aggFn.ColIdx[0]], allocSize)
			} else {
				funcAllocs[i], err = newAnyNotNullOrderedAggAlloc(allocator, inputTypes[aggFn.ColIdx[0]], allocSize)
			}
		case execinfrapb.AggregatorSpec_AVG:
			if isHashAgg {
				funcAllocs[i], err = newAvgHashAggAlloc(allocator, inputTypes[aggFn.ColIdx[0]], allocSize)
			} else {
				funcAllocs[i], err = newAvgOrderedAggAlloc(allocator, inputTypes[aggFn.ColIdx[0]], allocSize)
			}
		case execinfrapb.AggregatorSpec_SUM:
			if isHashAgg {
				funcAllocs[i], err = newSumHashAggAlloc(allocator, inputTypes[aggFn.ColIdx[0]], allocSize)
			} else {
				funcAllocs[i], err = newSumOrderedAggAlloc(allocator, inputTypes[aggFn.ColIdx[0]], allocSize)
			}
		case execinfrapb.AggregatorSpec_SUM_INT:
			if isHashAgg {
				funcAllocs[i], err = newSumIntHashAggAlloc(allocator, inputTypes[aggFn.ColIdx[0]], allocSize)
			} else {
				funcAllocs[i], err = newSumIntOrderedAggAlloc(allocator, inputTypes[aggFn.ColIdx[0]], allocSize)
			}
		case execinfrapb.AggregatorSpec_CONCAT_AGG:
			if isHashAgg {
				funcAllocs[i] = newConcatHashAggAlloc(allocator, allocSize)
			} else {
				funcAllocs[i] = newConcatOrderedAggAlloc(allocator, allocSize)
			}
		case execinfrapb.AggregatorSpec_COUNT_ROWS:
			if isHashAgg {
				funcAllocs[i] = newCountRowsHashAggAlloc(allocator, allocSize)
			} else {
				funcAllocs[i] = newCountRowsOrderedAggAlloc(allocator, allocSize)
			}
		case execinfrapb.AggregatorSpec_COUNT:
			if isHashAgg {
				funcAllocs[i] = newCountHashAggAlloc(allocator, allocSize)
			} else {
				funcAllocs[i] = newCountOrderedAggAlloc(allocator, allocSize)
			}
		case execinfrapb.AggregatorSpec_MIN:
			if isHashAgg {
				funcAllocs[i] = newMinHashAggAlloc(allocator, inputTypes[aggFn.ColIdx[0]], allocSize)
			} else {
				funcAllocs[i] = newMinOrderedAggAlloc(allocator, inputTypes[aggFn.ColIdx[0]], allocSize)
			}
		case execinfrapb.AggregatorSpec_MAX:
			if isHashAgg {
				funcAllocs[i] = newMaxHashAggAlloc(allocator, inputTypes[aggFn.ColIdx[0]], allocSize)
			} else {
				funcAllocs[i] = newMaxOrderedAggAlloc(allocator, inputTypes[aggFn.ColIdx[0]], allocSize)
			}
		case execinfrapb.AggregatorSpec_BOOL_AND:
			if isHashAgg {
				funcAllocs[i] = newBoolAndHashAggAlloc(allocator, allocSize)
			} else {
				funcAllocs[i] = newBoolAndOrderedAggAlloc(allocator, allocSize)
			}
		case execinfrapb.AggregatorSpec_BOOL_OR:
			if isHashAgg {
				funcAllocs[i] = newBoolOrHashAggAlloc(allocator, allocSize)
			} else {
				funcAllocs[i] = newBoolOrOrderedAggAlloc(allocator, allocSize)
			}
		// NOTE: if you're adding an implementation of a new aggregate
		// function, make sure to account for the memory under that struct in
		// its constructor.
		default:
			if isHashAgg {
				funcAllocs[i] = newDefaultHashAggAlloc(
					allocator, constructors[i], evalCtx, inputArgsConverter,
					len(aggFn.ColIdx), constArguments[i], outputTypes[i], allocSize,
				)
			} else {
				funcAllocs[i] = newDefaultOrderedAggAlloc(
					allocator, constructors[i], evalCtx, inputArgsConverter,
					len(aggFn.ColIdx), constArguments[i], outputTypes[i], allocSize,
				)
			}
			toClose = append(toClose, funcAllocs[i].(colexecbase.Closer))
		}

		if err != nil {
			return nil, nil, nil, err
		}
	}
	return &AggregateFuncsAlloc{
		allocator:     allocator,
		allocSize:     allocSize,
		aggFuncAllocs: funcAllocs,
	}, inputArgsConverter, toClose, nil
}

// sizeOfAggregateFunc is the size of some AggregateFunc implementation.
// countHashAgg was chosen arbitrarily, but it's important that we use a
// pointer to the aggregate function struct.
const sizeOfAggregateFunc = int64(unsafe.Sizeof(&countHashAgg{}))
const aggregateFuncSliceOverhead = int64(unsafe.Sizeof([]AggregateFunc{}))

// MakeAggregateFuncs returns a slice of aggregate function according to the
// initialized schema.
func (a *AggregateFuncsAlloc) MakeAggregateFuncs() []AggregateFunc {
	if len(a.returnFuncs) == 0 {
		// We have exhausted the previously allocated pools of objects, so we
		// need to allocate a new slice for a.returnFuncs, and we need it to be
		// of 'allocSize x number of funcs in schema' length. Every
		// aggFuncAlloc will allocate allocSize of objects on the newAggFunc
		// call below.
		a.allocator.AdjustMemoryUsage(aggregateFuncSliceOverhead + sizeOfAggregateFunc*int64(len(a.aggFuncAllocs))*a.allocSize)
		a.returnFuncs = make([]AggregateFunc, len(a.aggFuncAllocs)*int(a.allocSize))
	}
	funcs := a.returnFuncs[:len(a.aggFuncAllocs)]
	a.returnFuncs = a.returnFuncs[len(a.aggFuncAllocs):]
	for i, alloc := range a.aggFuncAllocs {
		funcs[i] = alloc.newAggFunc()
	}
	return funcs
}

type aggAllocBase struct {
	allocator *colmem.Allocator
	allocSize int64
}

// ProcessAggregations processes all aggregate functions specified in
// aggregations.
func ProcessAggregations(
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	aggregations []execinfrapb.AggregatorSpec_Aggregation,
	inputTypes []*types.T,
) (
	constructors []execinfrapb.AggregateConstructor,
	constArguments []tree.Datums,
	outputTypes []*types.T,
	err error,
) {
	constructors = make([]execinfrapb.AggregateConstructor, len(aggregations))
	constArguments = make([]tree.Datums, len(aggregations))
	outputTypes = make([]*types.T, len(aggregations))
	for i, aggFn := range aggregations {
		constructors[i], constArguments[i], outputTypes[i], err = execinfrapb.GetAggregateConstructor(
			evalCtx, semaCtx, &aggFn, inputTypes,
		)
		if err != nil {
			return
		}
	}
	return
}

var (
	zeroDecimalValue  apd.Decimal
	zeroFloat64Value  float64
	zeroInt64Value    int64
	zeroIntervalValue duration.Duration
	zeroBytesValue    []byte
)
