// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecutils

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// MakeWindowIntoBatch updates windowedBatch so that it provides a "window"
// into inputBatch starting at tuple index startIdx. It handles selection
// vectors on inputBatch as well (in which case windowedBatch will also have a
// "windowed" selection vector).
func MakeWindowIntoBatch(
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

// NewAppendOnlyBufferedBatch returns a new AppendOnlyBufferedBatch that has
// initial zero capacity and could grow arbitrarily large with append() method.
// It is intended to be used by the operators that need to buffer unknown
// number of tuples.
// colsToStore indicates the positions of columns to actually store in this
// batch. All columns are stored if colsToStore is nil, but when it is non-nil,
// then columns with positions not present in colsToStore will remain
// zero-capacity vectors.
// TODO(yuzefovich): consider whether it is beneficial to start out with
// non-zero capacity.
func NewAppendOnlyBufferedBatch(
	allocator *colmem.Allocator, typs []*types.T, colsToStore []int,
) *AppendOnlyBufferedBatch {
	if colsToStore == nil {
		colsToStore = make([]int, len(typs))
		for i := range colsToStore {
			colsToStore[i] = i
		}
	}
	batch := allocator.NewMemBatchWithFixedCapacity(typs, 0 /* capacity */)
	return &AppendOnlyBufferedBatch{
		batch:       batch,
		colVecs:     batch.ColVecs(),
		colsToStore: colsToStore,
	}
}

// AppendOnlyBufferedBatch is a wrapper around coldata.Batch that should be
// used by operators that buffer many tuples into a single batch by appending
// to it. It stores the length of the batch separately and intercepts calls to
// Length() and SetLength() in order to avoid updating offsets on vectors of
// types.Bytes type - which would result in a quadratic behavior - because
// it is not necessary since coldata.Vec.Append maintains the correct offsets.
//
// Note: "AppendOnly" in the name indicates that the tuples should *only* be
// appended to the vectors (which can be done via explicit Vec.Append calls or
// using utility append() method); however, this batch prohibits appending and
// replacing of the vectors themselves.
type AppendOnlyBufferedBatch struct {
	// We intentionally do not simply embed the batch so that we have to think
	// through the implementation of each method of coldata.Batch interface.
	batch coldata.Batch

	length      int
	colVecs     []coldata.Vec
	colsToStore []int
	// sel is the selection vector on this batch. Note that it is stored
	// separately from embedded coldata.Batch because we need to be able to
	// support a vector of an arbitrary length.
	sel []int
}

var _ coldata.Batch = &AppendOnlyBufferedBatch{}

// Length implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) Length() int {
	return b.length
}

// SetLength implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) SetLength(n int) {
	b.length = n
}

// Capacity implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) Capacity() int {
	// We assume that the AppendOnlyBufferedBatch has unlimited capacity, so all
	// callers should use that assumption instead of calling Capacity().
	colexecerror.InternalError(errors.AssertionFailedf("Capacity is prohibited on AppendOnlyBufferedBatch"))
	// This code is unreachable, but the compiler cannot infer that.
	return 0
}

// Width implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) Width() int {
	return b.batch.Width()
}

// ColVec implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) ColVec(i int) coldata.Vec {
	return b.colVecs[i]
}

// ColVecs implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) ColVecs() []coldata.Vec {
	return b.colVecs
}

// Selection implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) Selection() []int {
	if b.batch.Selection() != nil {
		return b.sel
	}
	return nil
}

// SetSelection implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) SetSelection(useSel bool) {
	b.batch.SetSelection(useSel)
	if useSel {
		// Make sure that selection vector is of the appropriate length.
		if cap(b.sel) < b.length {
			b.sel = make([]int, b.length)
		} else {
			b.sel = b.sel[:b.length]
		}
	}
}

// AppendCol implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) AppendCol(coldata.Vec) {
	colexecerror.InternalError(errors.AssertionFailedf("AppendCol is prohibited on AppendOnlyBufferedBatch"))
}

// ReplaceCol implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) ReplaceCol(coldata.Vec, int) {
	colexecerror.InternalError(errors.AssertionFailedf("ReplaceCol is prohibited on AppendOnlyBufferedBatch"))
}

// Reset implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) Reset([]*types.T, int, coldata.ColumnFactory) {
	colexecerror.InternalError(errors.AssertionFailedf("Reset is prohibited on AppendOnlyBufferedBatch"))
}

// ResetInternalBatch implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) ResetInternalBatch() {
	b.SetLength(0 /* n */)
	b.batch.ResetInternalBatch()
}

// String implements the coldata.Batch interface.
func (b *AppendOnlyBufferedBatch) String() string {
	// String should not be used in the fast paths, so we will set the length on
	// the wrapped batch (which might be a bit expensive but is ok).
	b.batch.SetLength(b.length)
	return b.batch.String()
}

// AppendTuples is a helper method that appends all tuples with indices in range
// [startIdx, endIdx) from batch (paying attention to the selection vector)
// into b.
// NOTE: this does *not* perform memory accounting.
// NOTE: batch must be of non-zero length.
func (b *AppendOnlyBufferedBatch) AppendTuples(batch coldata.Batch, startIdx, endIdx int) {
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

// MaybeAllocateUint64Array makes sure that the passed in array is allocated, of
// the desired length and zeroed out.
func MaybeAllocateUint64Array(array []uint64, length int) []uint64 {
	if cap(array) < length {
		return make([]uint64, length)
	}
	array = array[:length]
	for n := 0; n < length; n += copy(array[n:], ZeroUint64Column) {
	}
	return array
}

// MaybeAllocateBoolArray makes sure that the passed in array is allocated, of
// the desired length and zeroed out.
func MaybeAllocateBoolArray(array []bool, length int) []bool {
	if cap(array) < length {
		return make([]bool, length)
	}
	array = array[:length]
	for n := 0; n < length; n += copy(array[n:], ZeroBoolColumn) {
	}
	return array
}

// MaybeAllocateLimitedUint64Array is an optimized version of
// MaybeAllocateUint64Array that can *only* be used when length is at most
// coldata.MaxBatchSize.
func MaybeAllocateLimitedUint64Array(array []uint64, length int) []uint64 {
	if cap(array) < length {
		return make([]uint64, length)
	}
	array = array[:length]
	copy(array, ZeroUint64Column)
	return array
}

// MaybeAllocateLimitedBoolArray is an optimized version of
// maybeAllocateBool64Array that can *only* be used when length is at most
// coldata.MaxBatchSize.
func MaybeAllocateLimitedBoolArray(array []bool, length int) []bool {
	if cap(array) < length {
		return make([]bool, length)
	}
	array = array[:length]
	copy(array, ZeroBoolColumn)
	return array
}

var (
	// ZeroBoolColumn is zeroed out boolean slice of coldata.MaxBatchSize
	// length.
	ZeroBoolColumn = make([]bool, coldata.MaxBatchSize)
	// ZeroUint64Column is zeroed out uint64 slice of coldata.MaxBatchSize
	// length.
	ZeroUint64Column = make([]uint64, coldata.MaxBatchSize)
)

// HandleErrorFromDiskQueue takes in non-nil error emitted by colcontainer.Queue
// or colcontainer.PartitionedDiskQueue implementations and propagates it
// throughout the vectorized engine.
func HandleErrorFromDiskQueue(err error) {
	if sqlerrors.IsDiskFullError(err) {
		// We don't want to annotate the disk full error, so we propagate it
		// as expected one.
		colexecerror.ExpectedError(err)
	} else {
		colexecerror.InternalError(err)
	}
}

// EnsureSelectionVectorLength returns an int slice that is guaranteed to have
// the specified length. old is reused if possible but is *not* zeroed out.
func EnsureSelectionVectorLength(old []int, length int) []int {
	if cap(old) >= length {
		return old[:length]
	}
	return make([]int, length)
}
