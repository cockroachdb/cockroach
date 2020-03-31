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

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

var (
	zeroBoolColumn   = make([]bool, coldata.MaxBatchSize)
	zeroIntColumn    = make([]int, coldata.MaxBatchSize)
	zeroUint64Column = make([]uint64, coldata.MaxBatchSize)

	zeroDecimalValue  apd.Decimal
	zeroFloat64Value  float64
	zeroInt16Value    int16
	zeroInt32Value    int32
	zeroInt64Value    int64
	zeroIntervalValue duration.Duration
)

// decimalOverloadScratch is a utility struct that helps us avoid allocations
// of temporary decimals on every overloaded operation with them. In order for
// the templates to see it correctly, a local variable named `scratch` of this
// type must be declared before the inlined overloaded code.
type decimalOverloadScratch struct {
	tmpDec1, tmpDec2 apd.Decimal
}

// CopyBatch copies the original batch and returns that copy. However, note that
// the underlying capacity might be different (a new batch is created only with
// capacity original.Length()).
func CopyBatch(allocator *Allocator, original coldata.Batch) coldata.Batch {
	typs := make([]coltypes.T, original.Width())
	for i, vec := range original.ColVecs() {
		typs[i] = vec.Type()
	}
	b := allocator.NewMemBatchWithSize(typs, original.Length())
	b.SetLength(original.Length())
	allocator.PerformOperation(b.ColVecs(), func() {
		for colIdx, col := range original.ColVecs() {
			b.ColVec(colIdx).Copy(coldata.CopySliceArgs{
				SliceArgs: coldata.SliceArgs{
					ColType:   typs[colIdx],
					Src:       col,
					SrcEndIdx: original.Length(),
				},
			})
		}
	})
	return b
}

// makeWindowIntoBatch updates windowedBatch so that it provides a "window"
// into inputBatch starting at tuple index startIdx. It handles selection
// vectors on inputBatch as well (in which case windowedBatch will also have a
// "windowed" selection vector).
func makeWindowIntoBatch(
	windowedBatch, inputBatch coldata.Batch, startIdx int, inputTypes []coltypes.T,
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
	for i, typ := range inputTypes {
		window := inputBatch.ColVec(i).Window(typ, windowStart, windowEnd)
		windowedBatch.ReplaceCol(window, i)
	}
	windowedBatch.SetLength(inputBatchLen - startIdx)
}

func newPartitionerToOperator(
	allocator *Allocator,
	types []coltypes.T,
	partitioner colcontainer.PartitionedQueue,
	partitionIdx int,
) *partitionerToOperator {
	return &partitionerToOperator{
		partitioner:  partitioner,
		partitionIdx: partitionIdx,
		batch:        allocator.NewMemBatchNoCols(types, 0 /* size */),
	}
}

// partitionerToOperator is an Operator that Dequeue's from the corresponding
// partition on every call to Next. It is a converter from filled in
// PartitionedQueue to Operator.
type partitionerToOperator struct {
	ZeroInputNode
	NonExplainable

	partitioner  colcontainer.PartitionedQueue
	partitionIdx int
	batch        coldata.Batch
}

var _ Operator = &partitionerToOperator{}

func (p *partitionerToOperator) Init() {}

func (p *partitionerToOperator) Next(ctx context.Context) coldata.Batch {
	if err := p.partitioner.Dequeue(ctx, p.partitionIdx, p.batch); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return p.batch
}

func newAppendOnlyBufferedBatch(
	allocator *Allocator, typs []coltypes.T, initialSize int,
) *appendOnlyBufferedBatch {
	batch := allocator.NewMemBatchWithSize(typs, initialSize)
	return &appendOnlyBufferedBatch{
		Batch:   batch,
		colVecs: batch.ColVecs(),
		typs:    typs,
	}
}

// appendOnlyBufferedBatch is a wrapper around coldata.Batch that should be
// used by operators that buffer many tuples into a single batch by appending
// to it. It stores the length of the batch separately and intercepts calls to
// Length() and SetLength() in order to avoid updating offsets on vectors of
// coltypes.Bytes type - which would result in a quadratic behavior - because
// it is not necessary since coldata.Vec.Append maintains the correct offsets.
//
// Note: "appendOnly" in the name indicates that the tuples should *only* be
// appended to the vectors (which can be done via explicit Vec.Append calls or
// using utility append() method); however, this batch prohibits appending and
// replacing of the vectors themselves.
type appendOnlyBufferedBatch struct {
	coldata.Batch

	length  int
	colVecs []coldata.Vec
	typs    []coltypes.T
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

func (b *appendOnlyBufferedBatch) AppendCol(coldata.Vec) {
	execerror.VectorizedInternalPanic("AppendCol is prohibited on appendOnlyBufferedBatch")
}

func (b *appendOnlyBufferedBatch) ReplaceCol(coldata.Vec, int) {
	execerror.VectorizedInternalPanic("ReplaceCol is prohibited on appendOnlyBufferedBatch")
}

// append is a helper method that appends all tuples with indices in range
// [startIdx, endIdx) from batch (paying attention to the selection vector)
// into b.
// NOTE: this does *not* perform memory accounting.
func (b *appendOnlyBufferedBatch) append(batch coldata.Batch, startIdx, endIdx int) {
	for i, colVec := range b.colVecs {
		colVec.Append(
			coldata.SliceArgs{
				ColType:     b.typs[i],
				Src:         batch.ColVec(i),
				Sel:         batch.Selection(),
				DestIdx:     b.length,
				SrcStartIdx: startIdx,
				SrcEndIdx:   endIdx,
			},
		)
	}
	b.length += endIdx - startIdx
}
