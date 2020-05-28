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
	"github.com/cockroachdb/errors"
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
	return int64(capacity * sizeOfInt)
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

// getProportionalBatchMemSize returns the memory size of the batch that is
// proportional to given 'length'. This method returns the estimated memory
// footprint *only* of the first 'length' tuples in 'b'.
func getProportionalBatchMemSize(b coldata.Batch, length int64) int64 {
	usesSel := b.Selection() != nil
	b.SetSelection(true)
	selCapacity := cap(b.Selection())
	b.SetSelection(usesSel)
	proportionalBatchMemSize := int64(0)
	if selCapacity > 0 {
		proportionalBatchMemSize = selVectorSize(selCapacity) * length / int64(selCapacity)
	}
	for _, vec := range b.ColVecs() {
		if vec.Type() == coltypes.Bytes {
			proportionalBatchMemSize += int64(vec.Bytes().ProportionalSize(length))
		} else {
			proportionalBatchMemSize += getVecMemoryFootprint(vec) * length / int64(vec.Capacity())
		}
	}
	return proportionalBatchMemSize
}

// NewAllocator constructs a new Allocator instance.
func NewAllocator(ctx context.Context, acc *mon.BoundAccount) *Allocator {
	return &Allocator{ctx: ctx, acc: acc}
}

// NewMemBatch allocates a new in-memory coldata.Batch.
func (a *Allocator) NewMemBatch(types []coltypes.T) coldata.Batch {
	return a.NewMemBatchWithSize(types, coldata.BatchSize())
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

// NewMemBatchNoCols creates a "skeleton" of new in-memory coldata.Batch. It
// allocates memory for the selection vector but does *not* allocate any memory
// for the column vectors - those will have to be added separately.
func (a *Allocator) NewMemBatchNoCols(types []coltypes.T, size int) coldata.Batch {
	estimatedMemoryUsage := selVectorSize(size)
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return coldata.NewMemBatchNoCols(types, size)
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
	a.ReleaseMemory(batchMemSize)
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

// maybeAppendColumn might append a newly allocated coldata.Vec of the given
// type to b at position colIdx. Behavior of the function depends on how colIdx
// compares to the width of b:
// 1. if colIdx < b.Width(), then we expect that correctly-typed vector is
// already present in position colIdx. If that's not the case, we will panic.
// 2. if colIdx == b.Width(), then we will append a newly allocated coldata.Vec
// of the given type.
// 3. if colIdx > b.Width(), then we will panic because such condition
// indicates an error in setting up vector type enforcers during the planning
// stage.
// NOTE: b must be non-zero length batch.
func (a *Allocator) maybeAppendColumn(b coldata.Batch, t coltypes.T, colIdx int) {
	if b.Length() == 0 {
		execerror.VectorizedInternalPanic("trying to add a column to zero length batch")
	}
	width := b.Width()
	if colIdx < width {
		switch presentType := b.ColVec(colIdx).Type(); presentType {
		case t:
			// We already have the vector of the desired type in place.
			if presentType == coltypes.Bytes {
				// Flat bytes vector needs to be reset before the vector can be
				// reused.
				b.ColVec(colIdx).Bytes().Reset()
			}
			return
		default:
			// We have a vector with an unexpected type, so we panic.
			execerror.VectorizedInternalPanic(errors.Errorf(
				"trying to add a column of %s type at index %d but %s vector already present",
				t, colIdx, presentType,
			))
		}
	} else if colIdx > width {
		// We have a batch of unexpected width which indicates an error in the
		// planning stage.
		execerror.VectorizedInternalPanic(errors.Errorf(
			"trying to add a column of %s type at index %d but batch has width %d",
			t, colIdx, width,
		))
	}
	estimatedMemoryUsage := int64(estimateBatchSizeBytes([]coltypes.T{t}, coldata.BatchSize()))
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	b.AppendCol(a.NewMemColumn(t, coldata.BatchSize()))
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

	a.AdjustMemoryUsage(after - before)
}

// Used returns the number of bytes currently allocated through this allocator.
func (a *Allocator) Used() int64 {
	return a.acc.Used()
}

// AdjustMemoryUsage adjusts the number of bytes currently allocated through
// this allocator by delta bytes (which can be both positive or negative).
func (a *Allocator) AdjustMemoryUsage(delta int64) {
	if delta > 0 {
		if err := a.acc.Grow(a.ctx, delta); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
	} else {
		a.ReleaseMemory(-delta)
	}
}

// ReleaseMemory reduces the number of bytes currently allocated through this
// allocator by (at most) size bytes. size must be non-negative.
func (a *Allocator) ReleaseMemory(size int64) {
	if size < 0 {
		execerror.VectorizedInternalPanic(fmt.Sprintf("unexpectedly negative size in ReleaseMemory: %d", size))
	}
	if size > a.acc.Used() {
		size = a.acc.Used()
	}
	a.acc.Shrink(a.ctx, size)
}

const (
	sizeOfBool     = int(unsafe.Sizeof(true))
	sizeOfInt      = int(unsafe.Sizeof(int(0)))
	sizeOfInt16    = int(unsafe.Sizeof(int16(0)))
	sizeOfInt32    = int(unsafe.Sizeof(int32(0)))
	sizeOfInt64    = int(unsafe.Sizeof(int64(0)))
	sizeOfFloat64  = int(unsafe.Sizeof(float64(0)))
	sizeOfTime     = int(unsafe.Sizeof(time.Time{}))
	sizeOfDuration = int(unsafe.Sizeof(duration.Duration{}))
)

// sizeOfBatchSizeSelVector is the size (in bytes) of a selection vector of
// coldata.BatchSize() length.
var sizeOfBatchSizeSelVector = coldata.BatchSize() * sizeOfInt

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
