// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colmem

import (
	"context"
	"time"
	"unsafe"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	ctx     context.Context
	acc     *mon.BoundAccount
	factory coldata.ColumnFactory
}

func selVectorSize(capacity int) int64 {
	return int64(capacity * sizeOfInt)
}

func getVecMemoryFootprint(vec coldata.Vec) int64 {
	switch vec.CanonicalTypeFamily() {
	case types.BytesFamily:
		return int64(vec.Bytes().Size())
	case types.DecimalFamily:
		return int64(sizeOfDecimals(vec.Decimal()))
	case typeconv.DatumVecCanonicalTypeFamily:
		return int64(vec.Datum().Size())
	}
	return int64(EstimateBatchSizeBytes([]*types.T{vec.Type()}, vec.Capacity()))
}

func getVecsMemoryFootprint(vecs []coldata.Vec) int64 {
	var size int64
	for _, dest := range vecs {
		size += getVecMemoryFootprint(dest)
	}
	return size
}

// GetProportionalBatchMemSize returns the memory size of the batch that is
// proportional to given 'length'. This method returns the estimated memory
// footprint *only* of the first 'length' tuples in 'b'.
func GetProportionalBatchMemSize(b coldata.Batch, length int64) int64 {
	usesSel := b.Selection() != nil
	b.SetSelection(true)
	selCapacity := cap(b.Selection())
	b.SetSelection(usesSel)
	proportionalBatchMemSize := int64(0)
	if selCapacity > 0 {
		proportionalBatchMemSize = selVectorSize(selCapacity) * length / int64(selCapacity)
	}
	for _, vec := range b.ColVecs() {
		if vec.CanonicalTypeFamily() == types.BytesFamily {
			proportionalBatchMemSize += int64(vec.Bytes().ProportionalSize(length))
		} else {
			proportionalBatchMemSize += getVecMemoryFootprint(vec) * length / int64(vec.Capacity())
		}
	}
	return proportionalBatchMemSize
}

// NewAllocator constructs a new Allocator instance.
func NewAllocator(
	ctx context.Context, acc *mon.BoundAccount, factory coldata.ColumnFactory,
) *Allocator {
	return &Allocator{
		ctx:     ctx,
		acc:     acc,
		factory: factory,
	}
}

// NewMemBatchWithFixedCapacity allocates a new in-memory coldata.Batch with the
// given vector capacity.
// Note: consider whether you want the dynamic batch size behavior (in which
// case you should be using ResetMaybeReallocate).
func (a *Allocator) NewMemBatchWithFixedCapacity(typs []*types.T, capacity int) coldata.Batch {
	estimatedMemoryUsage := selVectorSize(capacity) + int64(EstimateBatchSizeBytes(typs, capacity))
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		colexecerror.InternalError(err)
	}
	return coldata.NewMemBatchWithCapacity(typs, capacity, a.factory)
}

// NewMemBatchWithMaxCapacity is a convenience shortcut of
// NewMemBatchWithFixedCapacity with capacity=coldata.BatchSize() and should
// only be used in tests (this is enforced by a linter).
func (a *Allocator) NewMemBatchWithMaxCapacity(typs []*types.T) coldata.Batch {
	return a.NewMemBatchWithFixedCapacity(typs, coldata.BatchSize())
}

// NewMemBatchNoCols creates a "skeleton" of new in-memory coldata.Batch. It
// allocates memory for the selection vector but does *not* allocate any memory
// for the column vectors - those will have to be added separately.
func (a *Allocator) NewMemBatchNoCols(typs []*types.T, capacity int) coldata.Batch {
	estimatedMemoryUsage := selVectorSize(capacity)
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		colexecerror.InternalError(err)
	}
	return coldata.NewMemBatchNoCols(typs, capacity)
}

// ResetMaybeReallocate returns a batch that is guaranteed to be in a "reset"
// state (meaning it is ready to be used) and to have the capacity of at least
// minCapacity. The method will grow the allocated capacity of the batch
// exponentially (possibly incurring a reallocation), until the batch reaches
// coldata.BatchSize().
// NOTE: if the reallocation occurs, then the memory under the old batch is
// released, so it is expected that the caller will lose the references to the
// old batch.
// Note: the method assumes that minCapacity is at least 1 and will "truncate"
// minCapacity if it is larger than coldata.BatchSize().
func (a *Allocator) ResetMaybeReallocate(
	typs []*types.T, oldBatch coldata.Batch, minCapacity int,
) (newBatch coldata.Batch, reallocated bool) {
	if minCapacity < 1 {
		colexecerror.InternalError(errors.AssertionFailedf("invalid minCapacity %d", minCapacity))
	}
	if minCapacity > coldata.BatchSize() {
		minCapacity = coldata.BatchSize()
	}
	reallocated = true
	if oldBatch == nil {
		newBatch = a.NewMemBatchWithFixedCapacity(typs, minCapacity)
	} else if oldBatch.Capacity() < coldata.BatchSize() {
		a.ReleaseBatch(oldBatch)
		newCapacity := oldBatch.Capacity() * 2
		if newCapacity < minCapacity {
			newCapacity = minCapacity
		}
		if newCapacity > coldata.BatchSize() {
			newCapacity = coldata.BatchSize()
		}
		newBatch = a.NewMemBatchWithFixedCapacity(typs, newCapacity)
	} else {
		reallocated = false
		oldBatch.ResetInternalBatch()
		newBatch = oldBatch
	}
	return newBatch, reallocated
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
		colexecerror.InternalError(err)
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

// NewMemColumn returns a new coldata.Vec of the desired capacity.
// NOTE: consider whether you should be using MaybeAppendColumn,
// NewMemBatchWith*, or ResetMaybeReallocate methods.
func (a *Allocator) NewMemColumn(t *types.T, capacity int) coldata.Vec {
	estimatedMemoryUsage := int64(EstimateBatchSizeBytes([]*types.T{t}, capacity))
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		colexecerror.InternalError(err)
	}
	return coldata.NewMemColumn(t, capacity, a.factory)
}

// MaybeAppendColumn might append a newly allocated coldata.Vec of the given
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
func (a *Allocator) MaybeAppendColumn(b coldata.Batch, t *types.T, colIdx int) {
	if b.Length() == 0 {
		colexecerror.InternalError(errors.AssertionFailedf("trying to add a column to zero length batch"))
	}
	width := b.Width()
	if colIdx < width {
		presentVec := b.ColVec(colIdx)
		presentType := presentVec.Type()
		if presentType.Identical(t) {
			// We already have the vector of the desired type in place.
			if presentVec.CanonicalTypeFamily() == types.BytesFamily {
				// Flat bytes vector needs to be reset before the vector can be
				// reused.
				presentVec.Bytes().Reset()
			}
			return
		}
		if presentType.Family() == types.UnknownFamily {
			// We already have an unknown vector in place. If this is expected,
			// then it will not be accessed and we're good; if this is not
			// expected, then an error will occur later.
			return
		}
		// We have a vector with an unexpected type, so we panic.
		colexecerror.InternalError(errors.AssertionFailedf(
			"trying to add a column of %s type at index %d but %s vector already present",
			t, colIdx, presentType,
		))
	} else if colIdx > width {
		// We have a batch of unexpected width which indicates an error in the
		// planning stage.
		colexecerror.InternalError(errors.AssertionFailedf(
			"trying to add a column of %s type at index %d but batch has width %d",
			t, colIdx, width,
		))
	}
	estimatedMemoryUsage := int64(EstimateBatchSizeBytes([]*types.T{t}, b.Capacity()))
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		colexecerror.InternalError(err)
	}
	b.AppendCol(a.NewMemColumn(t, b.Capacity()))
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
			colexecerror.InternalError(err)
		}
	} else {
		a.ReleaseMemory(-delta)
	}
}

// ReleaseMemory reduces the number of bytes currently allocated through this
// allocator by (at most) size bytes. size must be non-negative.
func (a *Allocator) ReleaseMemory(size int64) {
	if size < 0 {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly negative size in ReleaseMemory: %d", size))
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
	sizeOfDatum    = int(unsafe.Sizeof(tree.Datum(nil)))
	sizeOfDecimal  = unsafe.Sizeof(apd.Decimal{})
)

func sizeOfDecimals(decimals coldata.Decimals) uintptr {
	var size uintptr
	for _, d := range decimals {
		size += tree.SizeOfDecimal(d)
	}
	size += uintptr(cap(decimals)-len(decimals)) * sizeOfDecimal
	return size
}

// SizeOfBatchSizeSelVector is the size (in bytes) of a selection vector of
// coldata.BatchSize() length.
var SizeOfBatchSizeSelVector = coldata.BatchSize() * sizeOfInt

// EstimateBatchSizeBytes returns an estimated amount of bytes needed to
// store a batch in memory that has column types vecTypes.
// WARNING: This only is correct for fixed width types, and returns an
// estimate for non fixed width types. In future it might be possible to
// remove the need for estimation by specifying batch sizes in terms of bytes.
func EstimateBatchSizeBytes(vecTypes []*types.T, batchLength int) int {
	if batchLength == 0 {
		return 0
	}
	// acc represents the number of bytes to represent a row in the batch
	// (excluding any Bytes vectors, those are tracked separately).
	acc := 0
	numBytesVectors := 0
	for _, t := range vecTypes {
		switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
		case types.BoolFamily:
			acc += sizeOfBool
		case types.BytesFamily:
			numBytesVectors++
		case types.IntFamily:
			switch t.Width() {
			case 16:
				acc += sizeOfInt16
			case 32:
				acc += sizeOfInt32
			default:
				acc += sizeOfInt64
			}
		case types.FloatFamily:
			acc += sizeOfFloat64
		case types.DecimalFamily:
			// Similar to byte arrays, we can't tell how much space is used
			// to hold the arbitrary precision decimal objects.
			acc += 50
		case types.TimestampTZFamily:
			// time.Time consists of two 64 bit integers and a pointer to
			// time.Location. We will only account for this 3 bytes without paying
			// attention to the full time.Location struct. The reason is that it is
			// likely that time.Location's are cached and are shared among all the
			// timestamps, so if we were to include that in the estimation, we would
			// significantly overestimate.
			// TODO(yuzefovich): figure out whether the caching does take place.
			acc += sizeOfTime
		case types.IntervalFamily:
			acc += sizeOfDuration
		case typeconv.DatumVecCanonicalTypeFamily:
			// In datum vec we need to account for memory underlying the struct
			// that is the implementation of tree.Datum interface (for example,
			// tree.DBoolFalse) as well as for the overhead of storing that
			// implementation in the slice of tree.Datums. Note that if t is of
			// variable size, the memory will be properly accounted in
			// getVecMemoryFootprint.
			// Note: keep the calculation here in line with datumVec.Size.
			implementationSize, _ := tree.DatumTypeSize(t)
			acc += int(implementationSize) + sizeOfDatum
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", t))
		}
	}
	// For byte arrays, we initially allocate BytesInitialAllocationFactor
	// number of bytes (plus an int32 for the offset) for each row, so we use
	// the sum of two values as the estimate. However, later, the exact
	// memory footprint will be used: whenever a modification of Bytes takes
	// place, the Allocator will measure the old footprint and the updated
	// one and will update the memory account accordingly. We also account for
	// the overhead and for the additional offset value that are needed for
	// Bytes vectors (to be in line with coldata.Bytes.Size() method).
	bytesVectorsSize := numBytesVectors * (int(coldata.FlatBytesOverhead) +
		coldata.BytesInitialAllocationFactor*batchLength + sizeOfInt32*(batchLength+1))
	return acc*batchLength + bytesVectorsSize
}
