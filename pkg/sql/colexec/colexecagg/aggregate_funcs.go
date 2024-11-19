// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecagg

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execagg"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

// IsAggOptimized returns whether aggFn has an optimized implementation.
func IsAggOptimized(aggFn execinfrapb.AggregatorSpec_Func) bool {
	switch aggFn {
	case execinfrapb.AnyNotNull,
		execinfrapb.Avg,
		execinfrapb.BoolAnd,
		execinfrapb.BoolOr,
		execinfrapb.ConcatAgg,
		execinfrapb.Count,
		execinfrapb.Max,
		execinfrapb.Min,
		execinfrapb.Sum,
		execinfrapb.SumInt,
		execinfrapb.CountRows:
		return true
	default:
		return false
	}
}

// We will be sharing aggregateFuncAllocs between different columns, and in
// order to quickly determine whether a particular aggregate overload has
// already been created, we'll operate on a stack-allocated array of
// numOverloadsTotal pointers, in which all overloads for a single aggregate
// type will be contiguous. In other words, our allocs array will have
//   - [0:anyNotNullNumOverloads] corresponding to all anyNotNull aggregates,
//   - [anyNotNullNumOverloads:anyNotNullNumOverloads+avgNumOverloads] to all avg
//     aggregates, and so on.
const (
	anyNotNullFirstOverload = 0
	avgFirstOverload        = anyNotNullFirstOverload + anyNotNullNumOverloads
	boolAndFirstOverload    = avgFirstOverload + avgNumOverloads
	boolOrFirstOverload     = boolAndFirstOverload + 1
	concatFirstOverload     = boolOrFirstOverload + 1
	countFirstOverload      = concatFirstOverload + 1
	maxFirstOverload        = countFirstOverload + 1
	minFirstOverload        = maxFirstOverload + minMaxNumOverloads
	sumFirstOverload        = minFirstOverload + minMaxNumOverloads
	sumIntFirstOverload     = sumFirstOverload + sumNumOverloads
	countRowsFirstOverload  = sumIntFirstOverload + sumIntNumOverloads
	numOverloadsTotal       = countRowsFirstOverload + 1
)

// AggregateFunc is an aggregate function that performs computation on a batch
// when Compute(batch) is called and writes the output to the Vec passed in
// in SetOutput. The AggregateFunc performs an aggregation per group and outputs
// the aggregation once the start of the new group is reached. If the end of the
// group is not reached before the batch is finished, the AggregateFunc will
// store a carry value itself that it will use next time Compute is called to
// continue the aggregation of the last group.
type AggregateFunc interface {
	// Init sets the groups for the aggregation. Each index in groups
	// corresponds to a column value in the input batch. true represents the
	// start of a new group (the first group must also have 'true' set for the
	// very first tuple).
	Init(groups []bool)

	// SetOutput sets the output vector to write the results of aggregation
	// into. If the output vector changes, it is up to the caller to make sure
	// that results already written to the old vector are propagated further.
	SetOutput(vec *coldata.Vec)

	// CurrentOutputIndex returns the current index in the output vector that
	// the aggregate function is writing to. All indices < the index returned
	// are finished aggregations for previous groups.
	CurrentOutputIndex() int

	// SetOutputIndex sets the output index to write to.
	SetOutputIndex(idx int)

	// Compute computes the aggregation on the input batch.
	// Note: the implementations should be careful to account for their memory
	// usage.
	// Note: endIdx is assumed to be greater than zero.
	Compute(vecs []*coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int)

	// Flush flushes the result of aggregation on the last group. It should be
	// called once after input batches have been Compute()'d. outputIdx is only
	// used in case of hash aggregation - for ordered aggregation the aggregate
	// function itself should maintain the output index to write to.
	// The caller *must* ensure that the memory accounting is done on the
	// output vector of the aggregate function.
	Flush(outputIdx int)

	// HandleEmptyInputScalar populates the output for a case of an empty input
	// when the aggregate function is in scalar context. The output must always
	// be a single value (either null or zero, depending on the function).
	HandleEmptyInputScalar()

	// Reset resets the aggregate function which allows for reusing the same
	// instance for computation without the need to create a new instance.
	Reset()
}

type orderedAggregateFuncBase struct {
	groups []bool
	// curIdx tracks the current output index of this function.
	curIdx    int
	allocator *colmem.Allocator
	// vec is the output vector of this function.
	vec *coldata.Vec
	// nulls is the nulls vector of the output vector of this function.
	nulls *coldata.Nulls
	// isFirstGroup tracks whether the new group (indicated by 'true' in
	// 'groups') is actually the first group in the whole input.
	isFirstGroup bool
}

func (o *orderedAggregateFuncBase) Init(groups []bool) {
	o.groups = groups
	o.Reset()
}

func (o *orderedAggregateFuncBase) SetOutput(vec *coldata.Vec) {
	o.vec = vec
	o.nulls = vec.Nulls()
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

func (o *orderedAggregateFuncBase) Reset() {
	o.curIdx = 0
	o.isFirstGroup = true
}

type unorderedAggregateFuncBase struct {
	allocator *colmem.Allocator
	// vec is the output vector of this function.
	vec *coldata.Vec
	// nulls is the nulls vector of the output vector of this function.
	nulls *coldata.Nulls
}

func (h *unorderedAggregateFuncBase) Init(_ []bool) {}

func (h *unorderedAggregateFuncBase) SetOutput(vec *coldata.Vec) {
	h.vec = vec
	h.nulls = vec.Nulls()
}

func (h *unorderedAggregateFuncBase) CurrentOutputIndex() int {
	colexecerror.InternalError(errors.AssertionFailedf("CurrentOutputIndex called with unordered aggregation"))
	// This code is unreachable, but the compiler cannot infer that.
	return 0
}

func (h *unorderedAggregateFuncBase) SetOutputIndex(int) {
	colexecerror.InternalError(errors.AssertionFailedf("SetOutputIndex called with unordered aggregation"))
}

func (h *unorderedAggregateFuncBase) HandleEmptyInputScalar() {
	colexecerror.InternalError(errors.AssertionFailedf("HandleEmptyInputScalar called with unordered aggregation"))
}

// aggregateFuncAlloc is an aggregate function allocator that pools allocations
// of the structs of the same statically-typed aggregate function.
type aggregateFuncAlloc interface {
	// newAggFunc returns the aggregate function from the pool with all
	// necessary fields initialized.
	newAggFunc() AggregateFunc
	// increaseAllocSize increments allocSize of this allocator by delta.
	increaseAllocSize(delta int64)
}

// AggregateFuncsAlloc is a utility struct that pools allocations of multiple
// aggregate functions simultaneously (i.e. it supports a "schema of aggregate
// functions"). It will resolve the aggregate functions in its constructor to
// instantiate aggregateFuncAlloc objects and will use those to populate slices
// of new aggregation functions when requested.
type AggregateFuncsAlloc struct {
	allocator *colmem.Allocator
	// allocSize determines the number of objects allocated when the previous
	// allocations have been used up. This number will grow exponentially until
	// it reaches maxAllocSize.
	allocSize int64
	// maxAllocSize determines the maximum allocSize value.
	maxAllocSize int64
	// returnFuncs is the pool for the slice to be returned in
	// makeAggregateFuncs.
	returnFuncs []AggregateFunc
	// aggFuncAllocs are all necessary aggregate function allocators. Note that
	// any aggregateFuncAlloc might be shared between multiple aggFns - in such
	// cases the same reference to aggregateFuncAlloc will be stored multiple
	// times in aggFuncAllocs.
	aggFuncAllocs []aggregateFuncAlloc
}

// AggKind represents the context in which an aggregate function is executed -
// in a grouping or a window context. If grouping, the strategy can be either
// "Hash" or "Ordered".
type AggKind uint8

const (
	// HashAggKind indicates the hash strategy of executing an aggregate function
	// in a grouping context.
	HashAggKind AggKind = iota
	// OrderedAggKind indicates the ordered strategy of executing an aggregate
	// function in a grouping context.
	OrderedAggKind
	// WindowAggKind indicates that an aggregate function is being executed as a
	// window function.
	WindowAggKind
)

// NewAggregateFuncsAlloc returns a new AggregateFuncsAlloc.
func NewAggregateFuncsAlloc(
	ctx context.Context,
	args *NewAggregatorArgs,
	aggregations []execinfrapb.AggregatorSpec_Aggregation,
	initialAllocSize int64,
	maxAllocSize int64,
	aggKind AggKind,
) (*AggregateFuncsAlloc, *colconv.VecToDatumConverter, colexecop.Closers, error) {
	if initialAllocSize > maxAllocSize {
		return nil, nil, nil, errors.AssertionFailedf(
			"initialAllocSize %d must be no greater than maxAllocSize %d", initialAllocSize, maxAllocSize,
		)
	}
	allocSize := initialAllocSize
	funcAllocs := make([]aggregateFuncAlloc, len(aggregations))
	var toClose colexecop.Closers
	var vecIdxsToConvert []int
	for _, aggFn := range aggregations {
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
	var inputArgsConverter *colconv.VecToDatumConverter
	if len(vecIdxsToConvert) > 0 {
		// Only create the converter if we actually need to convert some vectors
		// for the default aggregate functions.
		inputArgsConverter = colconv.NewVecToDatumConverter(len(args.InputTypes), vecIdxsToConvert, false /* willRelease */)
	}
	// allocs tracks all aggregateFuncAllocs for optimized aggregate functions
	// that we have created so far. Since we don't have any concurrency during
	// allocation of aggregate functions, if we happen to have multiple
	// aggregates with the same input types, then those can share the alloc
	// struct.
	//
	// See comment above anyNotNullFirstOverload constant for how this single
	// slice is divided between all optimized aggregate functions.
	var allocs [numOverloadsTotal]aggregateFuncAlloc
	for i, aggFn := range aggregations {
		// firstOverloadIndex will remain numOverloadsTotal if non-optimized
		// aggregate function is created.
		firstOverloadIndex := numOverloadsTotal
		var offset int
		// freshAllocator will be set to true whenever a new allocator is
		// created.
		var freshAllocator bool
		var err error
		switch aggFn.Func {
		case execinfrapb.AnyNotNull:
			firstOverloadIndex = anyNotNullFirstOverload
			inputType := args.InputTypes[aggFn.ColIdx[0]]
			offset = anyNotNullOverloadOffset(inputType)
			if allocs[firstOverloadIndex+offset] == nil {
				freshAllocator = true
				switch aggKind {
				case HashAggKind:
					allocs[firstOverloadIndex+offset], err = newAnyNotNullHashAggAlloc(args.Allocator, inputType, allocSize)
				case OrderedAggKind:
					allocs[firstOverloadIndex+offset], err = newAnyNotNullOrderedAggAlloc(args.Allocator, inputType, allocSize)
				case WindowAggKind:
					colexecerror.InternalError(errors.AssertionFailedf("anyNotNull window aggregate not supported"))
				default:
					colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
				}
			}
		case execinfrapb.Avg:
			firstOverloadIndex = avgFirstOverload
			inputType := args.InputTypes[aggFn.ColIdx[0]]
			offset = avgOverloadOffset(inputType)
			if allocs[firstOverloadIndex+offset] == nil {
				freshAllocator = true
				switch aggKind {
				case HashAggKind:
					allocs[firstOverloadIndex+offset], err = newAvgHashAggAlloc(args.Allocator, inputType, allocSize)
				case OrderedAggKind:
					allocs[firstOverloadIndex+offset], err = newAvgOrderedAggAlloc(args.Allocator, inputType, allocSize)
				case WindowAggKind:
					allocs[firstOverloadIndex+offset], err = newAvgWindowAggAlloc(args.Allocator, inputType, allocSize)
				default:
					colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
				}
			}
		case execinfrapb.BoolAnd:
			firstOverloadIndex = boolAndFirstOverload
			if allocs[firstOverloadIndex+offset] == nil {
				freshAllocator = true
				switch aggKind {
				case HashAggKind:
					allocs[firstOverloadIndex+offset] = newBoolAndHashAggAlloc(args.Allocator, allocSize)
				case OrderedAggKind:
					allocs[firstOverloadIndex+offset] = newBoolAndOrderedAggAlloc(args.Allocator, allocSize)
				case WindowAggKind:
					allocs[firstOverloadIndex+offset] = newBoolAndWindowAggAlloc(args.Allocator, allocSize)
				default:
					colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
				}
			}
		case execinfrapb.BoolOr:
			firstOverloadIndex = boolOrFirstOverload
			if allocs[firstOverloadIndex+offset] == nil {
				freshAllocator = true
				switch aggKind {
				case HashAggKind:
					allocs[firstOverloadIndex+offset] = newBoolOrHashAggAlloc(args.Allocator, allocSize)
				case OrderedAggKind:
					allocs[firstOverloadIndex+offset] = newBoolOrOrderedAggAlloc(args.Allocator, allocSize)
				case WindowAggKind:
					allocs[firstOverloadIndex+offset] = newBoolOrWindowAggAlloc(args.Allocator, allocSize)
				default:
					colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
				}
			}
		case execinfrapb.ConcatAgg:
			firstOverloadIndex = concatFirstOverload
			if allocs[firstOverloadIndex+offset] == nil {
				freshAllocator = true
				switch aggKind {
				case HashAggKind:
					allocs[firstOverloadIndex+offset] = newConcatHashAggAlloc(args.Allocator, allocSize)
				case OrderedAggKind:
					allocs[firstOverloadIndex+offset] = newConcatOrderedAggAlloc(args.Allocator, allocSize)
				case WindowAggKind:
					allocs[firstOverloadIndex+offset] = newConcatWindowAggAlloc(args.Allocator, allocSize)
				default:
					colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
				}
			}
		case execinfrapb.Count:
			firstOverloadIndex = countFirstOverload
			if allocs[firstOverloadIndex+offset] == nil {
				freshAllocator = true
				switch aggKind {
				case HashAggKind:
					allocs[firstOverloadIndex+offset] = newCountHashAggAlloc(args.Allocator, allocSize)
				case OrderedAggKind:
					allocs[firstOverloadIndex+offset] = newCountOrderedAggAlloc(args.Allocator, allocSize)
				case WindowAggKind:
					allocs[firstOverloadIndex+offset] = newCountWindowAggAlloc(args.Allocator, allocSize)
				default:
					colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
				}
			}
		case execinfrapb.Max:
			firstOverloadIndex = maxFirstOverload
			inputType := args.InputTypes[aggFn.ColIdx[0]]
			offset = minMaxOverloadOffset(inputType)
			if allocs[firstOverloadIndex+offset] == nil {
				freshAllocator = true
				switch aggKind {
				case HashAggKind:
					allocs[firstOverloadIndex+offset] = newMaxHashAggAlloc(args.Allocator, inputType, allocSize)
				case OrderedAggKind:
					allocs[firstOverloadIndex+offset] = newMaxOrderedAggAlloc(args.Allocator, inputType, allocSize)
				case WindowAggKind:
					allocs[firstOverloadIndex+offset] = newMaxWindowAggAlloc(args.Allocator, inputType, allocSize)
				default:
					colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
				}
			}
		case execinfrapb.Min:
			firstOverloadIndex = minFirstOverload
			inputType := args.InputTypes[aggFn.ColIdx[0]]
			offset = minMaxOverloadOffset(inputType)
			if allocs[firstOverloadIndex+offset] == nil {
				freshAllocator = true
				switch aggKind {
				case HashAggKind:
					allocs[firstOverloadIndex+offset] = newMinHashAggAlloc(args.Allocator, inputType, allocSize)
				case OrderedAggKind:
					allocs[firstOverloadIndex+offset] = newMinOrderedAggAlloc(args.Allocator, inputType, allocSize)
				case WindowAggKind:
					allocs[firstOverloadIndex+offset] = newMinWindowAggAlloc(args.Allocator, inputType, allocSize)
				default:
					colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
				}
			}
		case execinfrapb.Sum:
			firstOverloadIndex = sumFirstOverload
			inputType := args.InputTypes[aggFn.ColIdx[0]]
			offset = sumOverloadOffset(inputType)
			if allocs[firstOverloadIndex+offset] == nil {
				freshAllocator = true
				switch aggKind {
				case HashAggKind:
					allocs[firstOverloadIndex+offset], err = newSumHashAggAlloc(args.Allocator, inputType, allocSize)
				case OrderedAggKind:
					allocs[firstOverloadIndex+offset], err = newSumOrderedAggAlloc(args.Allocator, inputType, allocSize)
				case WindowAggKind:
					allocs[firstOverloadIndex+offset], err = newSumWindowAggAlloc(args.Allocator, inputType, allocSize)
				default:
					colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
				}
			}
		case execinfrapb.SumInt:
			firstOverloadIndex = sumIntFirstOverload
			inputType := args.InputTypes[aggFn.ColIdx[0]]
			offset = sumIntOverloadOffset(inputType)
			if allocs[firstOverloadIndex+offset] == nil {
				freshAllocator = true
				switch aggKind {
				case HashAggKind:
					allocs[firstOverloadIndex+offset], err = newSumIntHashAggAlloc(args.Allocator, inputType, allocSize)
				case OrderedAggKind:
					allocs[firstOverloadIndex+offset], err = newSumIntOrderedAggAlloc(args.Allocator, inputType, allocSize)
				case WindowAggKind:
					allocs[firstOverloadIndex+offset], err = newSumIntWindowAggAlloc(args.Allocator, inputType, allocSize)
				default:
					colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
				}
			}
		case execinfrapb.CountRows:
			firstOverloadIndex = countRowsFirstOverload
			if allocs[firstOverloadIndex+offset] == nil {
				freshAllocator = true
				switch aggKind {
				case HashAggKind:
					allocs[firstOverloadIndex+offset] = newCountRowsHashAggAlloc(args.Allocator, allocSize)
				case OrderedAggKind:
					allocs[firstOverloadIndex+offset] = newCountRowsOrderedAggAlloc(args.Allocator, allocSize)
				case WindowAggKind:
					allocs[firstOverloadIndex+offset] = newCountRowsWindowAggAlloc(args.Allocator, allocSize)
				default:
					colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
				}
			}
		// NOTE: if you're adding an implementation of a new aggregate
		// function, make sure to account for the memory under that struct in
		// its constructor.
		default:
			freshAllocator = true
			switch aggKind {
			case HashAggKind:
				funcAllocs[i] = newDefaultHashAggAlloc(
					ctx, args.Allocator, args.Constructors[i], args.EvalCtx, inputArgsConverter,
					len(aggFn.ColIdx), args.ConstArguments[i], args.OutputTypes[i], allocSize,
				)
			case OrderedAggKind:
				funcAllocs[i] = newDefaultOrderedAggAlloc(
					ctx, args.Allocator, args.Constructors[i], args.EvalCtx, inputArgsConverter,
					len(aggFn.ColIdx), args.ConstArguments[i], args.OutputTypes[i], allocSize,
				)
			case WindowAggKind:
				colexecerror.InternalError(errors.AssertionFailedf("default window aggregate not supported"))
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
			}
			toClose = append(toClose, funcAllocs[i].(colexecop.Closer))
		}
		if err != nil {
			return nil, nil, nil, err
		}
		// All optimized aggregate functions have updated the ordinal variable
		// and have stored their alloc struct in the allocs array.
		if firstOverloadIndex != numOverloadsTotal {
			funcAllocs[i] = allocs[firstOverloadIndex+offset]
			if !freshAllocator {
				// If we're reusing the same allocator as for one of the
				// previous aggregate functions, we want to increment the alloc
				// size accordingly.
				funcAllocs[i].increaseAllocSize(initialAllocSize)
			}
		}
	}
	return &AggregateFuncsAlloc{
		allocator:     args.Allocator,
		allocSize:     allocSize,
		maxAllocSize:  maxAllocSize,
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
		//
		// But first check whether we need to grow allocSize.
		if a.returnFuncs != nil {
			// We're doing the very first allocation when returnFuncs is nil, so
			// we don't change the allocSize then.
			if a.allocSize < a.maxAllocSize {
				// We need to grow the alloc size of both this alloc object and
				// all aggAlloc objects.
				newAllocSize := a.allocSize * 2
				if newAllocSize > a.maxAllocSize {
					newAllocSize = a.maxAllocSize
				}
				delta := newAllocSize - a.allocSize
				a.allocSize = newAllocSize
				// Note that the same agg alloc object can be present multiple
				// times in the aggFuncAllocs slice, and we do want to increase
				// its alloc size every time we see it.
				for _, alloc := range a.aggFuncAllocs {
					alloc.increaseAllocSize(delta)
				}
			}
		}
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

func (a *aggAllocBase) increaseAllocSize(delta int64) {
	a.allocSize += delta
}

// ProcessAggregations processes all aggregate functions specified in
// aggregations.
//
// evalCtx will not be mutated.
func ProcessAggregations(
	ctx context.Context,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	aggregations []execinfrapb.AggregatorSpec_Aggregation,
	inputTypes []*types.T,
) (
	constructors []execagg.AggregateConstructor,
	constArguments []tree.Datums,
	outputTypes []*types.T,
	err error,
) {
	constructors = make([]execagg.AggregateConstructor, len(aggregations))
	constArguments = make([]tree.Datums, len(aggregations))
	outputTypes = make([]*types.T, len(aggregations))
	pAlloc := execagg.MakeParamTypesAllocator(aggregations)
	for i, aggFn := range aggregations {
		constructors[i], constArguments[i], outputTypes[i], err = execagg.GetAggregateConstructor(
			ctx, evalCtx, semaCtx, &aggFn, inputTypes, &pAlloc,
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
