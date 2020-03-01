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
// +build execgen_template
//
// This file is the execgen template for orderedsynchronizer.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	// */}}
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// {{/*
// Declarations to make the template compile properly.

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// _GOTYPESLICE is the template Go type slice variable for this operator. It
// will be replaced by the Go slice representation for each type in coltypes.T, for
// example []int64 for coltypes.Int64.
type _GOTYPESLICE interface{}

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// */}}

// OrderedSynchronizer receives rows from multiple inputs and produces a single
// stream of rows, ordered according to a set of columns. The rows in each input
// stream are assumed to be ordered according to the same set of columns.
type OrderedSynchronizer struct {
	allocator   *Allocator
	inputs      []Operator
	ordering    sqlbase.ColumnOrdering
	columnTypes []coltypes.T

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
	output      coldata.Batch
	outNulls    []*coldata.Nulls
	// In order to reduce the number of interface conversions, we will get access
	// to the underlying slice for the output vectors and will use them directly.
	// {{range .}}
	out_TYPECols []_GOTYPESLICE
	// {{end}}
	// outColsMap contains the positions of the corresponding vectors in the
	// slice for the same types. For example, if we have an output batch with
	// types = [Int64, Int64, Bool, Bytes, Bool, Int64], then outColsMap will be
	//                      [0, 1, 0, 0, 1, 2]
	//                       ^  ^  ^  ^  ^  ^
	//                       |  |  |  |  |  |
	//                       |  |  |  |  |  3rd among all Int64's
	//                       |  |  |  |  2nd among all Bool's
	//                       |  |  |  1st among all Bytes's
	//                       |  |  1st among all Bool's
	//                       |  2nd among all Int64's
	//                       1st among all Int64's
	outColsMap []int
}

var _ Operator = &OrderedSynchronizer{}

// ChildCount implements the execinfrapb.OpNode interface.
func (o *OrderedSynchronizer) ChildCount(verbose bool) int {
	return len(o.inputs)
}

// Child implements the execinfrapb.OpNode interface.
func (o *OrderedSynchronizer) Child(nth int, verbose bool) execinfra.OpNode {
	return o.inputs[nth]
}

// NewOrderedSynchronizer creates a new OrderedSynchronizer.
func NewOrderedSynchronizer(
	allocator *Allocator, inputs []Operator, typs []coltypes.T, ordering sqlbase.ColumnOrdering,
) *OrderedSynchronizer {
	return &OrderedSynchronizer{
		allocator:   allocator,
		inputs:      inputs,
		ordering:    ordering,
		columnTypes: typs,
	}
}

// Next is part of the Operator interface.
func (o *OrderedSynchronizer) Next(ctx context.Context) coldata.Batch {
	if o.inputBatches == nil {
		o.inputBatches = make([]coldata.Batch, len(o.inputs))
		o.heap = make([]int, 0, len(o.inputs))
		for i := range o.inputs {
			o.inputBatches[i] = o.inputs[i].Next(ctx)
			o.updateComparators(i)
			if o.inputBatches[i].Length() > 0 {
				o.heap = append(o.heap, i)
			}
		}
		heap.Init(o)
	}
	o.output.ResetInternalBatch()
	outputIdx := 0
	o.allocator.PerformOperation(o.output.ColVecs(), func() {
		for outputIdx < coldata.BatchSize() {
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
			for i, physType := range o.columnTypes {
				vec := batch.ColVec(i)
				if vec.Nulls().MaybeHasNulls() && vec.Nulls().NullAt(srcRowIdx) {
					o.outNulls[i].SetNull(outputIdx)
				} else {
					switch physType {
					// {{range .}}
					case _TYPES_T:
						srcCol := vec._TYPE()
						outCol := o.out_TYPECols[o.outColsMap[i]]
						v := execgen.UNSAFEGET(srcCol, srcRowIdx)
						execgen.SET(outCol, outputIdx, v)
					// {{end}}
					default:
						execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", physType))
					}
				}
			}

			// Advance the input batch, fetching a new batch if necessary.
			if o.inputIndices[minBatch]+1 < o.inputBatches[minBatch].Length() {
				o.inputIndices[minBatch]++
			} else {
				o.inputBatches[minBatch] = o.inputs[minBatch].Next(ctx)
				o.inputIndices[minBatch] = 0
				o.updateComparators(minBatch)
			}
			if o.inputBatches[minBatch].Length() == 0 {
				heap.Remove(o, 0)
			} else {
				heap.Fix(o, 0)
			}

			outputIdx++
		}
	})

	o.output.SetLength(outputIdx)
	return o.output
}

// Init is part of the Operator interface.
func (o *OrderedSynchronizer) Init() {
	o.inputIndices = make([]int, len(o.inputs))
	o.output = o.allocator.NewMemBatch(o.columnTypes)
	o.outNulls = make([]*coldata.Nulls, len(o.columnTypes))
	o.outColsMap = make([]int, len(o.columnTypes))
	for i, outVec := range o.output.ColVecs() {
		o.outNulls[i] = outVec.Nulls()
		switch o.columnTypes[i] {
		// {{range .}}
		case _TYPES_T:
			o.outColsMap[i] = len(o.out_TYPECols)
			o.out_TYPECols = append(o.out_TYPECols, outVec._TYPE())
		// {{end}}
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", o.columnTypes[i]))
		}
	}
	for i := range o.inputs {
		o.inputs[i].Init()
	}
	o.comparators = make([]vecComparator, len(o.ordering))
	for i := range o.ordering {
		typ := o.columnTypes[o.ordering[i].ColIdx]
		o.comparators[i] = GetVecComparator(typ, len(o.inputs))
	}
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
				execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected direction value %d", d))
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
