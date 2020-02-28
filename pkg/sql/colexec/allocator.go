// Copyright 2019 The Cockroach Authors.
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
	"fmt"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// Allocator is a memory management tool for vectorized components. It provides
// new batches (and appends to existing ones) within a fixed memory budget. If
// the budget is exceeded, it will panic with an error.
//
// In the future this can also be used to pool coldata.Vec allocations.
type Allocator struct {
	ctx context.Context
	acc *mon.BoundAccount
}

func selVectorSize(capacity int) int64 {
	return int64(capacity * sizeOfUint16)
}

func getVecMemoryFootprint(vec coldata.Vec) int64 {
	if vec.Type() == coltypes.Bytes {
		return int64(vec.Bytes().Size())
	}
	return int64(estimateBatchSizeBytes([]coltypes.T{vec.Type()}, vec.Capacity()))
}

func getVecsMemoryFootprint(vecs []coldata.Vec) int64 {
	var size int64
	for _, dest := range vecs {
		size += getVecMemoryFootprint(dest)
	}
	return size
}

// NewAllocator constructs a new Allocator instance.
func NewAllocator(ctx context.Context, acc *mon.BoundAccount) *Allocator {
	return &Allocator{ctx: ctx, acc: acc}
}

// NewMemBatch allocates a new in-memory coldata.Batch.
func (a *Allocator) NewMemBatch(types []coltypes.T) coldata.Batch {
	return a.NewMemBatchWithSize(types, int(coldata.BatchSize()))
}

// NewMemBatchWithSize allocates a new in-memory coldata.Batch with the given
// column size.
func (a *Allocator) NewMemBatchWithSize(types []coltypes.T, size int) coldata.Batch {
	estimatedMemoryUsage := selVectorSize(size) + int64(estimateBatchSizeBytes(types, size))
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return coldata.NewMemBatchWithSize(types, size)
}

// RetainBatch adds the size of the batch to the memory account. This shouldn't
// need to be used regularly, since most memory accounting necessary is done
// through PerformOperation. Use this if you want to explicitly manage the
// memory accounted for.
// NOTE: when calculating memory footprint, this method looks at the capacities
// of the vectors and does *not* pay attention to the length of the batch.
func (a *Allocator) RetainBatch(b coldata.Batch) {
	if b == coldata.ZeroBatch {
		// coldata.ZeroBatch takes up no space but also doesn't support the change
		// of the selection vector, so we need to handle it separately.
		return
	}
	// We need to get the capacity of the internal selection vector, even if b
	// currently doesn't use it, so we set selection to true and will reset
	// below.
	usesSel := b.Selection() != nil
	b.SetSelection(true)
	if err := a.acc.Grow(a.ctx, selVectorSize(cap(b.Selection()))+getVecsMemoryFootprint(b.ColVecs())); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	b.SetSelection(usesSel)
}

// ReleaseBatch releases the size of the batch from the memory account. This
// shouldn't need to be used regularly, since all accounts are closed by
// Flow.Cleanup. Use this if you want to explicitly manage the memory used. An
// example of a use case is releasing a batch before writing it to disk.
// NOTE: when calculating memory footprint, this method looks at the capacities
// of the vectors and does *not* pay attention to the length of the batch.
func (a *Allocator) ReleaseBatch(b coldata.Batch) {
	if b == coldata.ZeroBatch {
		// coldata.ZeroBatch takes up no space but also doesn't support the change
		// of the selection vector, so we need to handle it separately.
		return
	}
	// We need to get the capacity of the internal selection vector, even if b
	// currently doesn't use it, so we set selection to true and will reset
	// below.
	usesSel := b.Selection() != nil
	b.SetSelection(true)
	batchMemSize := selVectorSize(cap(b.Selection())) + getVecsMemoryFootprint(b.ColVecs())
	if batchMemSize > a.acc.Used() {
		// It appears to be possible that our estimation of the batch size when
		// allocating a new batch is lower than our current estimate (maybe because
		// we append to flat bytes), so reduce batchMemSize to clear out the
		// account in such scenario.
		// TODO(yuzefovich): I tried to debug to figure out why this happens to no
		// success. See #45425.
		batchMemSize = a.acc.Used()
	}
	a.acc.Shrink(a.ctx, batchMemSize)
	b.SetSelection(usesSel)
}

// NewMemColumn returns a new coldata.Vec, initialized with a length.
func (a *Allocator) NewMemColumn(t coltypes.T, n int) coldata.Vec {
	estimatedMemoryUsage := int64(estimateBatchSizeBytes([]coltypes.T{t}, n))
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return coldata.NewMemColumn(t, n)
}

// MaybeAddColumn might add a newly allocated coldata.Vec of the given type to
// b at position colIdx. It will do so if either
// 1. the width of the batch is not greater than colIdx, or
// 2. there is already an "unknown" vector in position colIdx in the batch.
// If the first condition is true, then "unknown" vectors of zero length will
// be appended to the batch before appending the requested column.
// If the second condition is true, then the "unknown" column is replaced with
// the newly created typed column.
// NOTE: b must be non-zero length batch.
func (a *Allocator) MaybeAddColumn(b coldata.Batch, t coltypes.T, colIdx int) {
	if b.Length() == 0 {
		execerror.VectorizedInternalPanic("trying to add a column to zero length batch")
	}
	if b.Width() > colIdx && b.ColVec(colIdx).Type() != coltypes.Unhandled {
		// Neither of the two conditions mentioned in the comment above are true,
		// so there is nothing to do.
		return
	}
	for b.Width() < colIdx {
		b.AppendCol(a.NewMemColumn(coltypes.Unhandled, 0))
	}
	estimatedMemoryUsage := int64(estimateBatchSizeBytes([]coltypes.T{t}, int(coldata.BatchSize())))
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	col := a.NewMemColumn(t, int(coldata.BatchSize()))
	if b.Width() == colIdx {
		b.AppendCol(col)
	} else {
		b.ReplaceCol(col, colIdx)
	}
}

// PerformOperation executes 'operation' (that somehow modifies 'destVecs') and
// updates the memory account accordingly.
// NOTE: if some columnar vectors are not modified, they should not be included
// in 'destVecs' to reduce the performance hit of memory accounting.
func (a *Allocator) PerformOperation(destVecs []coldata.Vec, operation func()) {
	before := getVecsMemoryFootprint(destVecs)
	// To simplify the accounting, we perform the operation first and then will
	// update the memory account. The minor "drift" in accounting that is
	// caused by this approach is ok.
	operation()
	after := getVecsMemoryFootprint(destVecs)

	delta := after - before
	if delta >= 0 {
		if err := a.acc.Grow(a.ctx, delta); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
	} else {
		a.acc.Shrink(a.ctx, -delta)
	}
}

// Used returns the number of bytes currently allocated through this allocator.
func (a *Allocator) Used() int64 {
	return a.acc.Used()
}

// Clear clears up the memory account of the allocator.
// WARNING: usage of this method is *not* compatible with using
// PerformOperation. Use this only in combination with RetainBatch /
// ReleaseBatch.
func (a *Allocator) Clear() {
	a.acc.Shrink(a.ctx, a.acc.Used())
}

const (
	sizeOfBool     = int(unsafe.Sizeof(true))
	sizeOfInt16    = int(unsafe.Sizeof(int16(0)))
	sizeOfInt32    = int(unsafe.Sizeof(int32(0)))
	sizeOfInt64    = int(unsafe.Sizeof(int64(0)))
	sizeOfFloat64  = int(unsafe.Sizeof(float64(0)))
	sizeOfTime     = int(unsafe.Sizeof(time.Time{}))
	sizeOfDuration = int(unsafe.Sizeof(duration.Duration{}))
	sizeOfUint16   = int(unsafe.Sizeof(uint16(0)))
)

// sizeOfBatchSizeSelVector is the size (in bytes) of a selection vector of
// coldata.BatchSize() length.
var sizeOfBatchSizeSelVector = int(coldata.BatchSize()) * sizeOfUint16

// estimateBatchSizeBytes returns an estimated amount of bytes needed to
// store a batch in memory that has column types vecTypes.
// WARNING: This only is correct for fixed width types, and returns an
// estimate for non fixed width coltypes. In future it might be possible to
// remove the need for estimation by specifying batch sizes in terms of bytes.
func estimateBatchSizeBytes(vecTypes []coltypes.T, batchLength int) int {
	// acc represents the number of bytes to represent a row in the batch.
	acc := 0
	for _, t := range vecTypes {
		switch t {
		case coltypes.Bool:
			acc += sizeOfBool
		case coltypes.Bytes:
			// For byte arrays, we initially allocate BytesInitialAllocationFactor
			// number of bytes (plus an int32 for the offset) for each row, so we use
			// the sum of two values as the estimate. However, later, the exact
			// memory footprint will be used: whenever a modification of Bytes takes
			// place, the Allocator will measure the old footprint and the updated
			// one and will update the memory account accordingly.
			acc += coldata.BytesInitialAllocationFactor + sizeOfInt32
		case coltypes.Int16:
			acc += sizeOfInt16
		case coltypes.Int32:
			acc += sizeOfInt32
		case coltypes.Int64:
			acc += sizeOfInt64
		case coltypes.Float64:
			acc += sizeOfFloat64
		case coltypes.Decimal:
			// Similar to byte arrays, we can't tell how much space is used
			// to hold the arbitrary precision decimal objects.
			acc += 50
		case coltypes.Timestamp:
			// time.Time consists of two 64 bit integers and a pointer to
			// time.Location. We will only account for this 3 bytes without paying
			// attention to the full time.Location struct. The reason is that it is
			// likely that time.Location's are cached and are shared among all the
			// timestamps, so if we were to include that in the estimation, we would
			// significantly overestimate.
			// TODO(yuzefovich): figure out whether the caching does take place.
			acc += sizeOfTime
		case coltypes.Interval:
			acc += sizeOfDuration
		case coltypes.Unhandled:
			// Placeholder coldata.Vecs of unknown types are allowed.
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %s", t))
		}
	}
	return acc * batchLength
}
