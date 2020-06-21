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
// aggregation once the start of the new group is reached. If the end of the
// group is not reached before the batch is finished, the aggregateFunc will
// store a carry value that it will use next time Compute is called. Note that
// this carry value is stored at the output index. Therefore if any memory
// modification of the output vector is made, the caller *MUST* copy the value
// at the current index inclusive for a correct aggregation.
type aggregateFunc interface {
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
			if isHashAgg {
				funcAllocs[i], err = newAvgHashAggAlloc(allocator, aggTyps[i][0], allocSize)
			} else {
				funcAllocs[i], err = newAvgOrderedAggAlloc(allocator, aggTyps[i][0], allocSize)
			}
		case execinfrapb.AggregatorSpec_SUM:
			if isHashAgg {
				funcAllocs[i], err = newSumHashAggAlloc(allocator, aggTyps[i][0], allocSize)
			} else {
				funcAllocs[i], err = newSumOrderedAggAlloc(allocator, aggTyps[i][0], allocSize)
			}
		case execinfrapb.AggregatorSpec_SUM_INT:
			if isHashAgg {
				funcAllocs[i], err = newSumIntHashAggAlloc(allocator, aggTyps[i][0], allocSize)
			} else {
				funcAllocs[i], err = newSumIntOrderedAggAlloc(allocator, aggTyps[i][0], allocSize)
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
				funcAllocs[i] = newMinHashAggAlloc(allocator, aggTyps[i][0], allocSize)
			} else {
				funcAllocs[i] = newMinOrderedAggAlloc(allocator, aggTyps[i][0], allocSize)
			}
		case execinfrapb.AggregatorSpec_MAX:
			if isHashAgg {
				funcAllocs[i] = newMaxHashAggAlloc(allocator, aggTyps[i][0], allocSize)
			} else {
				funcAllocs[i] = newMaxOrderedAggAlloc(allocator, aggTyps[i][0], allocSize)
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
			return nil, errors.AssertionFailedf("didn't find aggregateFuncAlloc for %s", aggFns[i].String())
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
const sizeOfAggregateFunc = int64(unsafe.Sizeof(&countHashAgg{}))

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

// MakeAggregateFuncsOutputTypes produces the output types for a given set of
// aggregates.
func MakeAggregateFuncsOutputTypes(
	aggTyps [][]*types.T, aggFns []execinfrapb.AggregatorSpec_Func,
) ([]*types.T, error) {
	var err error
	outTyps := make([]*types.T, len(aggFns))
	for i, aggFn := range aggFns {
		_, outTyps[i], err = execinfrapb.GetAggregateInfo(aggFn, aggTyps[i]...)
		if err != nil {
			return nil, err
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

type aggAllocBase struct {
	allocator *colmem.Allocator
	allocSize int64
}
