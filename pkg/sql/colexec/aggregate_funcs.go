// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// SupportedAggFns contains all aggregate functions supported by the vectorized
// engine.
var SupportedAggFns = []execinfrapb.AggregatorSpec_Func{
	execinfrapb.AggregatorSpec_ANY_NOT_NULL,
	execinfrapb.AggregatorSpec_AVG,
	execinfrapb.AggregatorSpec_SUM,
	execinfrapb.AggregatorSpec_SUM_INT,
	execinfrapb.AggregatorSpec_COUNT_ROWS,
	execinfrapb.AggregatorSpec_COUNT,
	execinfrapb.AggregatorSpec_MIN,
	execinfrapb.AggregatorSpec_MAX,
	execinfrapb.AggregatorSpec_BOOL_AND,
	execinfrapb.AggregatorSpec_BOOL_OR,
}

// aggregateFunc is an aggregate function that performs computation on a batch
// when Compute(batch) is called and writes the output to the Vec passed in
// in Init. The aggregateFunc performs an aggregation per group and outputs the
// aggregation once the end of the group is reached. If the end of the group is
// not reached before the batch is finished, the aggregateFunc will store a
// carry value that it will use next time Compute is called. Note that this
// carry value is stored at the output index. Therefore if any memory
// modification of the output vector is made, the caller *MUST* copy the value
// at the current index inclusive for a correct aggregation.
type aggregateFunc interface {
	// Init sets the groups for the aggregation and the output vector. Each index
	// in groups corresponds to a column value in the input batch. true represents
	// the first value of a new group.
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
	Compute(batch coldata.Batch, inputIdxs []uint32)

	// Flush flushes the result of aggregation on the last group. It should be
	// called once after input batches have been Compute()'d.
	// Note: the implementations are free to not account for the memory used
	// for the result of aggregation of the last group.
	Flush()

	// HandleEmptyInputScalar populates the output for a case of an empty input
	// when the aggregate function is in scalar context. The output must always
	// be a single value (either null or zero, depending on the function).
	// TODO(yuzefovich): we can pull scratch field of aggregates into a shared
	// aggregator and implement this method once on the shared base.
	HandleEmptyInputScalar()
}

// aggregateFuncAlloc is an aggregate function allocator that pools allocations
// of the structs of the same statically-typed aggregate function.
type aggregateFuncAlloc interface {
	// newAggFunc returns the aggregate function from the pool with all
	// necessary fields initialized.
	newAggFunc() aggregateFunc
}

// aggregateFuncsAlloc is a utility struct that pools allocations of multiple
// aggregate functions simultaneously (i.e. it supports a "schema of aggregate
// functions"). It will resolve the aggregate functions in its constructor to
// instantiate aggregateFuncAlloc objects and will use those to populate slices
// of new aggregation functions when requested.
type aggregateFuncsAlloc struct {
	allocator *colmem.Allocator
	// allocSize determines the number of objects allocated when the previous
	// allocations have been used up.
	allocSize int64
	// returnFuncs is the pool for the slice to be returned in
	// makeAggregateFuncs.
	returnFuncs []aggregateFunc
	// aggFuncAllocs are all necessary aggregate function allocators. Note that
	// a separate aggregateFuncAlloc will be created for each aggFn from the
	// schema (even if there are "duplicates" - exactly the same functions - in
	// the function schema).
	aggFuncAllocs []aggregateFuncAlloc
}

func newAggregateFuncsAlloc(
	allocator *colmem.Allocator,
	aggTyps [][]*types.T,
	aggFns []execinfrapb.AggregatorSpec_Func,
	allocSize int64,
	isHashAgg bool,
) (*aggregateFuncsAlloc, error) {
	funcAllocs := make([]aggregateFuncAlloc, len(aggFns))
	for i := range aggFns {
		var err error
		switch aggFns[i] {
		case execinfrapb.AggregatorSpec_ANY_NOT_NULL:
			if isHashAgg {
				funcAllocs[i], err = newAnyNotNullHashAggAlloc(allocator, aggTyps[i][0], allocSize)
			} else {
				funcAllocs[i], err = newAnyNotNullOrderedAggAlloc(allocator, aggTyps[i][0], allocSize)
			}
		case execinfrapb.AggregatorSpec_AVG:
			funcAllocs[i], err = newAvgAggAlloc(allocator, aggTyps[i][0], allocSize)
		case execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_SUM_INT:
			funcAllocs[i], err = newSumAggAlloc(allocator, aggTyps[i][0], allocSize)
		case execinfrapb.AggregatorSpec_COUNT_ROWS:
			funcAllocs[i] = newCountRowsAggAlloc(allocator, allocSize)
		case execinfrapb.AggregatorSpec_COUNT:
			funcAllocs[i] = newCountAggAlloc(allocator, allocSize)
		case execinfrapb.AggregatorSpec_MIN:
			funcAllocs[i], err = newMinAggAlloc(allocator, aggTyps[i][0], allocSize)
		case execinfrapb.AggregatorSpec_MAX:
			funcAllocs[i], err = newMaxAggAlloc(allocator, aggTyps[i][0], allocSize)
		case execinfrapb.AggregatorSpec_BOOL_AND:
			funcAllocs[i] = newBoolAndAggAlloc(allocator, allocSize)
		case execinfrapb.AggregatorSpec_BOOL_OR:
			funcAllocs[i] = newBoolOrAggAlloc(allocator, allocSize)
		// NOTE: if you're adding an implementation of a new aggregate
		// function, make sure to account for the memory under that struct in
		// its constructor.
		default:
			return nil, errors.Errorf("unsupported columnar aggregate function %s", aggFns[i].String())
		}

		if err != nil {
			return nil, err
		}
	}
	return &aggregateFuncsAlloc{
		allocator:     allocator,
		allocSize:     allocSize,
		aggFuncAllocs: funcAllocs,
	}, nil
}

// sizeOfAggregateFunc is the size of some aggregateFunc implementation.
// countAgg was chosen arbitrarily, but it's important that we use a pointer to
// the aggregate function struct.
const sizeOfAggregateFunc = int64(unsafe.Sizeof(&countAgg{}))

func (a *aggregateFuncsAlloc) makeAggregateFuncs() []aggregateFunc {
	if len(a.returnFuncs) == 0 {
		// We have exhausted the previously allocated pools of objects, so we
		// need to allocate a new slice for a.returnFuncs, and we need it to be
		// of 'allocSize x number of funcs in schema' length. Every
		// aggFuncAlloc will allocate allocSize of objects on the newAggFunc
		// call below.
		a.allocator.AdjustMemoryUsage(sizeOfAggregateFunc * a.allocSize)
		a.returnFuncs = make([]aggregateFunc, len(a.aggFuncAllocs)*int(a.allocSize))
	}
	funcs := a.returnFuncs[:len(a.aggFuncAllocs)]
	a.returnFuncs = a.returnFuncs[len(a.aggFuncAllocs):]
	for i, alloc := range a.aggFuncAllocs {
		funcs[i] = alloc.newAggFunc()
	}
	return funcs
}

func makeAggregateFuncsOutputTypes(
	aggTyps [][]*types.T, aggFns []execinfrapb.AggregatorSpec_Func,
) ([]*types.T, error) {
	outTyps := make([]*types.T, len(aggFns))

	for i := range aggFns {
		// Set the output type of the aggregate.
		switch aggFns[i] {
		case execinfrapb.AggregatorSpec_COUNT_ROWS, execinfrapb.AggregatorSpec_COUNT:
			// TODO(jordan): this is a somewhat of a hack. The aggregate functions
			// should come with their own output types, somehow.
			outTyps[i] = types.Int
		case
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_AVG,
			execinfrapb.AggregatorSpec_SUM,
			execinfrapb.AggregatorSpec_SUM_INT,
			execinfrapb.AggregatorSpec_MIN,
			execinfrapb.AggregatorSpec_MAX,
			execinfrapb.AggregatorSpec_BOOL_AND,
			execinfrapb.AggregatorSpec_BOOL_OR:
			// Output types are the input types for now.
			outTyps[i] = aggTyps[i][0]
		default:
			return nil, errors.Errorf("unsupported columnar aggregate function %s", aggFns[i].String())
		}
	}

	return outTyps, nil
}

// extractAggTypes returns a nested array representing the input types
// corresponding to each aggregation function.
func extractAggTypes(aggCols [][]uint32, typs []*types.T) [][]*types.T {
	aggTyps := make([][]*types.T, len(aggCols))

	for aggIdx := range aggCols {
		aggTyps[aggIdx] = make([]*types.T, len(aggCols[aggIdx]))
		for i, colIdx := range aggCols[aggIdx] {
			aggTyps[aggIdx][i] = typs[colIdx]
		}
	}

	return aggTyps
}

// isAggregateSupported returns whether the aggregate function that operates on
// columns of types 'inputTypes' (which can be empty in case of COUNT_ROWS) is
// supported.
func isAggregateSupported(
	allocator *colmem.Allocator, aggFn execinfrapb.AggregatorSpec_Func, inputTypes []*types.T,
) (bool, error) {
	switch aggFn {
	case execinfrapb.AggregatorSpec_SUM:
		switch inputTypes[0].Family() {
		case types.IntFamily:
			// TODO(alfonso): plan ordinary SUM on integer types by casting to DECIMAL
			// at the end, mod issues with overflow. Perhaps to avoid the overflow
			// issues, at first, we could plan SUM for all types besides Int64.
			return false, errors.Newf("sum on int cols not supported (use sum_int)")
		}
	case execinfrapb.AggregatorSpec_SUM_INT:
		// TODO(yuzefovich): support this case through vectorize.
		if inputTypes[0].Width() != 64 {
			return false, errors.Newf("sum_int is only supported on Int64 through vectorized")
		}
	}

	// We're only interested in resolving the aggregate functions and will not
	// be actually creating them with the alloc, so we use 0 as the allocation
	// size.
	_, err := newAggregateFuncsAlloc(
		allocator,
		[][]*types.T{inputTypes},
		[]execinfrapb.AggregatorSpec_Func{aggFn},
		0,     /* allocSize */
		false, /* isHashAgg */
	)
	if err != nil {
		return false, err
	}
	outputTypes, err := makeAggregateFuncsOutputTypes(
		[][]*types.T{inputTypes},
		[]execinfrapb.AggregatorSpec_Func{aggFn},
	)
	if err != nil {
		return false, err
	}
	_, retType, err := execinfrapb.GetAggregateInfo(aggFn, inputTypes...)
	if err != nil {
		return false, err
	}
	// The columnar aggregates will return the same physical output type as their
	// input. However, our current builtin resolution might say that the return
	// type is the canonical for the family (for example, MAX on INT4 is said to
	// return INT8), so we explicitly check whether the type the columnar
	// aggregate returns and the type the planning code will expect it to return
	// are the same. If they are not, we fallback to row-by-row engine.
	if !retType.Identical(outputTypes[0]) {
		// TODO(yuzefovich): support this case through vectorize. Probably it needs
		// to be done at the same time as #38845.
		return false, errors.Newf("aggregates with different input and output types are not supported")
	}
	return true, nil
}
