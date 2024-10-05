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
		execinfrapb.Sum,
		execinfrapb.SumInt,
		execinfrapb.ConcatAgg,
		execinfrapb.CountRows,
		execinfrapb.Count,
		execinfrapb.Min,
		execinfrapb.Max,
		execinfrapb.BoolAnd,
		execinfrapb.BoolOr:
		return true
	default:
		return false
	}
}

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
	SetOutput(vec coldata.Vec)

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
	Compute(vecs []coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int)

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
	vec coldata.Vec
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

func (o *orderedAggregateFuncBase) SetOutput(vec coldata.Vec) {
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
	vec coldata.Vec
	// nulls is the nulls vector of the output vector of this function.
	nulls *coldata.Nulls
}

func (h *unorderedAggregateFuncBase) Init(_ []bool) {}

func (h *unorderedAggregateFuncBase) SetOutput(vec coldata.Vec) {
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
	allocSize int64,
	aggKind AggKind,
) (*AggregateFuncsAlloc, *colconv.VecToDatumConverter, colexecop.Closers, error) {
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
	for i, aggFn := range aggregations {
		var err error
		switch aggFn.Func {
		case execinfrapb.AnyNotNull:
			switch aggKind {
			case HashAggKind:
				funcAllocs[i], err = newAnyNotNullHashAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			case OrderedAggKind:
				funcAllocs[i], err = newAnyNotNullOrderedAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			case WindowAggKind:
				colexecerror.InternalError(errors.AssertionFailedf("anyNotNull window aggregate not supported"))
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
			}
		case execinfrapb.Avg:
			switch aggKind {
			case HashAggKind:
				funcAllocs[i], err = newAvgHashAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			case OrderedAggKind:
				funcAllocs[i], err = newAvgOrderedAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			case WindowAggKind:
				funcAllocs[i], err = newAvgWindowAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
			}
		case execinfrapb.Sum:
			switch aggKind {
			case HashAggKind:
				funcAllocs[i], err = newSumHashAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			case OrderedAggKind:
				funcAllocs[i], err = newSumOrderedAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			case WindowAggKind:
				funcAllocs[i], err = newSumWindowAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
			}
		case execinfrapb.SumInt:
			switch aggKind {
			case HashAggKind:
				funcAllocs[i], err = newSumIntHashAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			case OrderedAggKind:
				funcAllocs[i], err = newSumIntOrderedAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			case WindowAggKind:
				funcAllocs[i], err = newSumIntWindowAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
			}
		case execinfrapb.ConcatAgg:
			switch aggKind {
			case HashAggKind:
				funcAllocs[i] = newConcatHashAggAlloc(args.Allocator, allocSize)
			case OrderedAggKind:
				funcAllocs[i] = newConcatOrderedAggAlloc(args.Allocator, allocSize)
			case WindowAggKind:
				funcAllocs[i] = newConcatWindowAggAlloc(args.Allocator, allocSize)
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
			}
		case execinfrapb.CountRows:
			switch aggKind {
			case HashAggKind:
				funcAllocs[i] = newCountRowsHashAggAlloc(args.Allocator, allocSize)
			case OrderedAggKind:
				funcAllocs[i] = newCountRowsOrderedAggAlloc(args.Allocator, allocSize)
			case WindowAggKind:
				funcAllocs[i] = newCountRowsWindowAggAlloc(args.Allocator, allocSize)
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
			}
		case execinfrapb.Count:
			switch aggKind {
			case HashAggKind:
				funcAllocs[i] = newCountHashAggAlloc(args.Allocator, allocSize)
			case OrderedAggKind:
				funcAllocs[i] = newCountOrderedAggAlloc(args.Allocator, allocSize)
			case WindowAggKind:
				funcAllocs[i] = newCountWindowAggAlloc(args.Allocator, allocSize)
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
			}
		case execinfrapb.Min:
			switch aggKind {
			case HashAggKind:
				funcAllocs[i] = newMinHashAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			case OrderedAggKind:
				funcAllocs[i] = newMinOrderedAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			case WindowAggKind:
				funcAllocs[i] = newMinWindowAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
			}
		case execinfrapb.Max:
			switch aggKind {
			case HashAggKind:
				funcAllocs[i] = newMaxHashAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			case OrderedAggKind:
				funcAllocs[i] = newMaxOrderedAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			case WindowAggKind:
				funcAllocs[i] = newMaxWindowAggAlloc(args.Allocator, args.InputTypes[aggFn.ColIdx[0]], allocSize)
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
			}
		case execinfrapb.BoolAnd:
			switch aggKind {
			case HashAggKind:
				funcAllocs[i] = newBoolAndHashAggAlloc(args.Allocator, allocSize)
			case OrderedAggKind:
				funcAllocs[i] = newBoolAndOrderedAggAlloc(args.Allocator, allocSize)
			case WindowAggKind:
				funcAllocs[i] = newBoolAndWindowAggAlloc(args.Allocator, allocSize)
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
			}
		case execinfrapb.BoolOr:
			switch aggKind {
			case HashAggKind:
				funcAllocs[i] = newBoolOrHashAggAlloc(args.Allocator, allocSize)
			case OrderedAggKind:
				funcAllocs[i] = newBoolOrOrderedAggAlloc(args.Allocator, allocSize)
			case WindowAggKind:
				funcAllocs[i] = newBoolOrWindowAggAlloc(args.Allocator, allocSize)
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected agg kind"))
			}
		// NOTE: if you're adding an implementation of a new aggregate
		// function, make sure to account for the memory under that struct in
		// its constructor.
		default:
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
	}
	return &AggregateFuncsAlloc{
		allocator:     args.Allocator,
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
	for i, aggFn := range aggregations {
		constructors[i], constArguments[i], outputTypes[i], err = execagg.GetAggregateConstructor(
			ctx, evalCtx, semaCtx, &aggFn, inputTypes,
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
