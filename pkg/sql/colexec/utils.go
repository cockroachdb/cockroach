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
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

var (
	zeroBoolColumn   = make([]bool, coldata.MaxBatchSize)
	zeroIntColumn    = make([]int, coldata.MaxBatchSize)
	zeroUint64Column = make([]uint64, coldata.MaxBatchSize)
)

// makeWindowIntoBatch updates windowedBatch so that it provides a "window"
// into inputBatch starting at tuple index startIdx. It handles selection
// vectors on inputBatch as well (in which case windowedBatch will also have a
// "windowed" selection vector).
func makeWindowIntoBatch(
	windowedBatch, inputBatch coldata.Batch, startIdx int, inputTypes []*types.T,
) {
	inputBatchLen := inputBatch.Length()
	windowStart := startIdx
	windowEnd := inputBatchLen
	if sel := inputBatch.Selection(); sel != nil {
		// We have a selection vector on the input batch, and in order to avoid
		// deselecting (i.e. moving the data over), we will provide an adjusted
		// selection vector to the windowed batch as well.
		windowedBatch.SetSelection(true)
		windowIntoSel := sel[startIdx:inputBatchLen]
		copy(windowedBatch.Selection(), windowIntoSel)
		maxSelIdx := 0
		for _, selIdx := range windowIntoSel {
			if selIdx > maxSelIdx {
				maxSelIdx = selIdx
			}
		}
		windowStart = 0
		windowEnd = maxSelIdx + 1
	} else {
		windowedBatch.SetSelection(false)
	}
	for i := range inputTypes {
		window := inputBatch.ColVec(i).Window(windowStart, windowEnd)
		windowedBatch.ReplaceCol(window, i)
	}
	windowedBatch.SetLength(inputBatchLen - startIdx)
}

func newPartitionerToOperator(
	allocator *colmem.Allocator,
	types []*types.T,
	partitioner colcontainer.PartitionedQueue,
	partitionIdx int,
) *partitionerToOperator {
	return &partitionerToOperator{
		allocator:    allocator,
		types:        types,
		partitioner:  partitioner,
		partitionIdx: partitionIdx,
	}
}

// partitionerToOperator is an Operator that Dequeue's from the corresponding
// partition on every call to Next. It is a converter from filled in
// PartitionedQueue to Operator.
type partitionerToOperator struct {
	colexecbase.ZeroInputNode
	NonExplainable

	allocator    *colmem.Allocator
	types        []*types.T
	partitioner  colcontainer.PartitionedQueue
	partitionIdx int
	batch        coldata.Batch
}

var _ colexecbase.Operator = &partitionerToOperator{}

func (p *partitionerToOperator) Init() {
	// We will be dequeuing the batches from disk into this batch, so we need to
	// have enough capacity to support the batches of any size.
	p.batch = p.allocator.NewMemBatchWithFixedCapacity(p.types, coldata.BatchSize())
}

func (p *partitionerToOperator) Next(ctx context.Context) coldata.Batch {
	if err := p.partitioner.Dequeue(ctx, p.partitionIdx, p.batch); err != nil {
		colexecerror.InternalError(err)
	}
	return p.batch
}

// newAppendOnlyBufferedBatch returns a new appendOnlyBufferedBatch that has
// initial zero capacity and could grow arbitrarily large with append() method.
// It is intended to be used by the operators that need to buffer unknown
// number of tuples.
// colsToStore indicates the positions of columns to actually store in this
// batch. All columns are stored if colsToStore is nil, but when it is non-nil,
// then columns with positions not present in colsToStore will remain
// zero-capacity vectors.
// TODO(yuzefovich): consider whether it is beneficial to start out with
// non-zero capacity.
func newAppendOnlyBufferedBatch(
	allocator *colmem.Allocator, typs []*types.T, colsToStore []int,
) *appendOnlyBufferedBatch {
	if colsToStore == nil {
		colsToStore = make([]int, len(typs))
		for i := range colsToStore {
			colsToStore[i] = i
		}
	}
	batch := allocator.NewMemBatchWithFixedCapacity(typs, 0 /* capacity */)
	return &appendOnlyBufferedBatch{
		Batch:       batch,
		colVecs:     batch.ColVecs(),
		colsToStore: colsToStore,
	}
}

// appendOnlyBufferedBatch is a wrapper around coldata.Batch that should be
// used by operators that buffer many tuples into a single batch by appending
// to it. It stores the length of the batch separately and intercepts calls to
// Length() and SetLength() in order to avoid updating offsets on vectors of
// types.Bytes type - which would result in a quadratic behavior - because
// it is not necessary since coldata.Vec.Append maintains the correct offsets.
//
// Note: "appendOnly" in the name indicates that the tuples should *only* be
// appended to the vectors (which can be done via explicit Vec.Append calls or
// using utility append() method); however, this batch prohibits appending and
// replacing of the vectors themselves.
type appendOnlyBufferedBatch struct {
	coldata.Batch

	length      int
	colVecs     []coldata.Vec
	colsToStore []int
	// sel is the selection vector on this batch. Note that it is stored
	// separately from embedded coldata.Batch because we need to be able to
	// support a vector of an arbitrary length.
	sel []int
}

var _ coldata.Batch = &appendOnlyBufferedBatch{}

func (b *appendOnlyBufferedBatch) Length() int {
	return b.length
}

func (b *appendOnlyBufferedBatch) SetLength(n int) {
	b.length = n
}

func (b *appendOnlyBufferedBatch) ColVec(i int) coldata.Vec {
	return b.colVecs[i]
}

func (b *appendOnlyBufferedBatch) ColVecs() []coldata.Vec {
	return b.colVecs
}

func (b *appendOnlyBufferedBatch) Selection() []int {
	if b.Batch.Selection() != nil {
		return b.sel
	}
	return nil
}

func (b *appendOnlyBufferedBatch) SetSelection(useSel bool) {
	b.Batch.SetSelection(useSel)
	if useSel {
		// Make sure that selection vector is of the appropriate length.
		if cap(b.sel) < b.length {
			b.sel = make([]int, b.length)
		} else {
			b.sel = b.sel[:b.length]
		}
	}
}

func (b *appendOnlyBufferedBatch) AppendCol(coldata.Vec) {
	colexecerror.InternalError(errors.AssertionFailedf("AppendCol is prohibited on appendOnlyBufferedBatch"))
}

func (b *appendOnlyBufferedBatch) ReplaceCol(coldata.Vec, int) {
	colexecerror.InternalError(errors.AssertionFailedf("ReplaceCol is prohibited on appendOnlyBufferedBatch"))
}

// append is a helper method that appends all tuples with indices in range
// [startIdx, endIdx) from batch (paying attention to the selection vector)
// into b.
// NOTE: this does *not* perform memory accounting.
// NOTE: batch must be of non-zero length.
func (b *appendOnlyBufferedBatch) append(batch coldata.Batch, startIdx, endIdx int) {
	for _, colIdx := range b.colsToStore {
		b.colVecs[colIdx].Append(
			coldata.SliceArgs{
				Src:         batch.ColVec(colIdx),
				Sel:         batch.Selection(),
				DestIdx:     b.length,
				SrcStartIdx: startIdx,
				SrcEndIdx:   endIdx,
			},
		)
	}
	b.length += endIdx - startIdx
}

// maybeAllocate* methods make sure that the passed in array is allocated, of
// the desired length and zeroed out.
func maybeAllocateUint64Array(array []uint64, length int) []uint64 {
	if cap(array) < length {
		return make([]uint64, length)
	}
	array = array[:length]
	for n := 0; n < length; n += copy(array[n:], zeroUint64Column) {
	}
	return array
}

func maybeAllocateBoolArray(array []bool, length int) []bool {
	if cap(array) < length {
		return make([]bool, length)
	}
	array = array[:length]
	for n := 0; n < length; n += copy(array[n:], zeroBoolColumn) {
	}
	return array
}

// maybeAllocateLimitedUint64Array is an optimized version of
// maybeAllocateUint64Array that can *only* be used when length is at most
// coldata.MaxBatchSize.
func maybeAllocateLimitedUint64Array(array []uint64, length int) []uint64 {
	if cap(array) < length {
		return make([]uint64, length)
	}
	array = array[:length]
	copy(array, zeroUint64Column)
	return array
}

// maybeAllocateLimitedBoolArray is an optimized version of
// maybeAllocateBool64Array that can *only* be used when length is at most
// coldata.MaxBatchSize.
func maybeAllocateLimitedBoolArray(array []bool, length int) []bool {
	if cap(array) < length {
		return make([]bool, length)
	}
	array = array[:length]
	copy(array, zeroBoolColumn)
	return array
}

func makeOrdering(cols []uint32) []execinfrapb.Ordering_Column {
	res := make([]execinfrapb.Ordering_Column, len(cols))
	for i, colIdx := range cols {
		res[i].ColIdx = colIdx
	}
	return res
}
