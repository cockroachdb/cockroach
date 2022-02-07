// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
//go:build execgen_template
// +build execgen_template

//
// This file is the execgen template for ordered_synchronizer.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"container/heap"
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// {{/*
// Declarations to make the template compile properly.

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// */}}

// OrderedSynchronizer receives rows from multiple inputs and produces a single
// stream of rows, ordered according to a set of columns. The rows in each input
// stream are assumed to be ordered according to the same set of columns.
type OrderedSynchronizer struct {
	colexecop.InitHelper
	span *tracing.Span

	accountingHelper      colmem.SetAccountingHelper
	memoryLimit           int64
	inputs                []colexecargs.OpWithMetaInfo
	ordering              colinfo.ColumnOrdering
	typs                  []*types.T
	canonicalTypeFamilies []types.Family

	// inputBatches stores the current batch for each input.
	inputBatches []coldata.Batch
	// inputIndices stores the current index into each input batch.
	inputIndices []int
	// heap is a min heap which stores indices into inputBatches. The "current
	// value" of ith input batch is the tuple at inputIndices[i] position of
	// inputBatches[i] batch. If an input is fully exhausted, it will be removed
	// from heap.
	heap []int
	// comparators stores one comparator per ordering column.
	comparators []vecComparator
	// maxCapacity if non-zero indicates the target capacity of the output
	// batch. It is set when, after setting a row, we realize that the output
	// batch has exceeded the memory limit.
	maxCapacity int
	output      coldata.Batch
	outVecs     coldata.TypedVecs
}

var (
	_ colexecop.Operator = &OrderedSynchronizer{}
	_ colexecop.Closer   = &OrderedSynchronizer{}
)

// ChildCount implements the execinfrapb.OpNode interface.
func (o *OrderedSynchronizer) ChildCount(verbose bool) int {
	return len(o.inputs)
}

// Child implements the execinfrapb.OpNode interface.
func (o *OrderedSynchronizer) Child(nth int, verbose bool) execinfra.OpNode {
	return o.inputs[nth].Root
}

// NewOrderedSynchronizer creates a new OrderedSynchronizer.
// - memoryLimit will limit the size of batches produced by the synchronizer.
func NewOrderedSynchronizer(
	allocator *colmem.Allocator,
	memoryLimit int64,
	inputs []colexecargs.OpWithMetaInfo,
	typs []*types.T,
	ordering colinfo.ColumnOrdering,
) *OrderedSynchronizer {
	os := &OrderedSynchronizer{
		memoryLimit:           memoryLimit,
		inputs:                inputs,
		ordering:              ordering,
		typs:                  typs,
		canonicalTypeFamilies: typeconv.ToCanonicalTypeFamilies(typs),
	}
	os.accountingHelper.Init(allocator, typs)
	return os
}

// Next is part of the Operator interface.
func (o *OrderedSynchronizer) Next() coldata.Batch {
	if o.inputBatches == nil {
		o.inputBatches = make([]coldata.Batch, len(o.inputs))
		o.heap = make([]int, 0, len(o.inputs))
		for i := range o.inputs {
			o.inputBatches[i] = o.inputs[i].Root.Next()
			o.updateComparators(i)
			if o.inputBatches[i].Length() > 0 {
				o.heap = append(o.heap, i)
			}
		}
		heap.Init(o)
	}
	o.resetOutput()
	outputIdx := 0
	for outputIdx < o.output.Capacity() && (o.maxCapacity == 0 || outputIdx < o.maxCapacity) {
		if o.Len() == 0 {
			// All inputs exhausted.
			break
		}

		minBatch := o.heap[0]
		// Copy the min row into the output.
		batch := o.inputBatches[minBatch]
		srcRowIdx := o.inputIndices[minBatch]
		if sel := batch.Selection(); sel != nil {
			srcRowIdx = sel[srcRowIdx]
		}
		for i := range o.typs {
			vec := batch.ColVec(i)
			if vec.Nulls().MaybeHasNulls() && vec.Nulls().NullAt(srcRowIdx) {
				o.outVecs.Nulls[i].SetNull(outputIdx)
			} else {
				switch o.canonicalTypeFamilies[i] {
				// {{range .}}
				case _CANONICAL_TYPE_FAMILY:
					switch o.typs[i].Width() {
					// {{range .WidthOverloads}}
					case _TYPE_WIDTH:
						srcCol := vec._TYPE()
						outCol := o.outVecs._TYPECols[o.outVecs.ColsMap[i]]
						v := srcCol.Get(srcRowIdx)
						outCol.Set(outputIdx, v)
						// {{end}}
					}
					// {{end}}
				default:
					colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", o.typs[i].String()))
				}
			}
		}

		// Advance the input batch, fetching a new batch if necessary.
		if o.inputIndices[minBatch]+1 < o.inputBatches[minBatch].Length() {
			o.inputIndices[minBatch]++
		} else {
			o.inputBatches[minBatch] = o.inputs[minBatch].Root.Next()
			o.inputIndices[minBatch] = 0
			o.updateComparators(minBatch)
		}
		if o.inputBatches[minBatch].Length() == 0 {
			heap.Remove(o, 0)
		} else {
			heap.Fix(o, 0)
		}

		// Account for the memory of the row we have just set.
		o.accountingHelper.AccountForSet(outputIdx)
		outputIdx++
		if o.maxCapacity == 0 && o.accountingHelper.Allocator.Used() >= o.memoryLimit {
			o.maxCapacity = outputIdx
		}
	}

	o.output.SetLength(outputIdx)
	return o.output
}

func (o *OrderedSynchronizer) resetOutput() {
	var reallocated bool
	o.output, reallocated = o.accountingHelper.ResetMaybeReallocate(
		o.typs, o.output, 1 /* minDesiredCapacity */, o.memoryLimit,
	)
	if reallocated {
		o.outVecs.SetBatch(o.output)
	}
}

// Init is part of the Operator interface.
func (o *OrderedSynchronizer) Init(ctx context.Context) {
	if !o.InitHelper.Init(ctx) {
		return
	}
	o.Ctx, o.span = execinfra.ProcessorSpan(o.Ctx, "ordered sync")
	o.inputIndices = make([]int, len(o.inputs))
	for i := range o.inputs {
		o.inputs[i].Root.Init(o.Ctx)
	}
	o.comparators = make([]vecComparator, len(o.ordering))
	for i := range o.ordering {
		typ := o.typs[o.ordering[i].ColIdx]
		o.comparators[i] = GetVecComparator(typ, len(o.inputs))
	}
}

func (o *OrderedSynchronizer) DrainMeta() []execinfrapb.ProducerMetadata {
	var bufferedMeta []execinfrapb.ProducerMetadata
	if o.span != nil {
		for i := range o.inputs {
			for _, stats := range o.inputs[i].StatsCollectors {
				o.span.RecordStructured(stats.GetStats())
			}
		}
		if meta := execinfra.GetTraceDataAsMetadata(o.span); meta != nil {
			bufferedMeta = append(bufferedMeta, *meta)
		}
	}
	for _, input := range o.inputs {
		bufferedMeta = append(bufferedMeta, input.MetadataSources.DrainMeta()...)
	}
	return bufferedMeta
}

func (o *OrderedSynchronizer) Close(context.Context) error {
	// Note that we're using the context of the synchronizer rather than the
	// argument of Close() because the synchronizer derives its own tracing
	// span.
	ctx := o.EnsureCtx()
	o.accountingHelper.Release()
	var lastErr error
	for _, input := range o.inputs {
		if err := input.ToClose.Close(ctx); err != nil {
			lastErr = err
		}
	}
	if o.span != nil {
		o.span.Finish()
	}
	*o = OrderedSynchronizer{}
	return lastErr
}

func (o *OrderedSynchronizer) compareRow(batchIdx1 int, batchIdx2 int) int {
	batch1 := o.inputBatches[batchIdx1]
	batch2 := o.inputBatches[batchIdx2]
	valIdx1 := o.inputIndices[batchIdx1]
	valIdx2 := o.inputIndices[batchIdx2]
	if sel := batch1.Selection(); sel != nil {
		valIdx1 = sel[valIdx1]
	}
	if sel := batch2.Selection(); sel != nil {
		valIdx2 = sel[valIdx2]
	}
	for i := range o.ordering {
		info := o.ordering[i]
		res := o.comparators[i].compare(batchIdx1, batchIdx2, valIdx1, valIdx2)
		if res != 0 {
			switch d := info.Direction; d {
			case encoding.Ascending:
				return res
			case encoding.Descending:
				return -res
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected direction value %d", d))
			}
		}
	}
	return 0
}

// updateComparators should be run whenever a new batch is fetched. It updates
// all the relevant vectors in o.comparators.
func (o *OrderedSynchronizer) updateComparators(batchIdx int) {
	batch := o.inputBatches[batchIdx]
	if batch.Length() == 0 {
		return
	}
	for i := range o.ordering {
		vec := batch.ColVec(o.ordering[i].ColIdx)
		o.comparators[i].setVec(batchIdx, vec)
	}
}

// Len is part of heap.Interface and is only meant to be used internally.
func (o *OrderedSynchronizer) Len() int {
	return len(o.heap)
}

// Less is part of heap.Interface and is only meant to be used internally.
func (o *OrderedSynchronizer) Less(i, j int) bool {
	return o.compareRow(o.heap[i], o.heap[j]) < 0
}

// Swap is part of heap.Interface and is only meant to be used internally.
func (o *OrderedSynchronizer) Swap(i, j int) {
	o.heap[i], o.heap[j] = o.heap[j], o.heap[i]
}

// Push is part of heap.Interface and is only meant to be used internally.
func (o *OrderedSynchronizer) Push(x interface{}) {
	o.heap = append(o.heap, x.(int))
}

// Pop is part of heap.Interface and is only meant to be used internally.
func (o *OrderedSynchronizer) Pop() interface{} {
	x := o.heap[len(o.heap)-1]
	o.heap = o.heap[:len(o.heap)-1]
	return x
}
