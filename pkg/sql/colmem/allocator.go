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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// TODO(yuzefovich): audit all Operators to make sure that all static
// (internal) memory is accounted for.

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
	if vec == nil {
		return 0
	}
	switch vec.CanonicalTypeFamily() {
	case types.BytesFamily:
		return int64(vec.Bytes().Size())
	case types.DecimalFamily:
		return int64(sizeOfDecimals(vec.Decimal(), 0 /* startIdx */))
	case types.JsonFamily:
		return int64(vec.JSON().Size())
	case typeconv.DatumVecCanonicalTypeFamily:
		return int64(vec.Datum().Size(0 /* startIdx */))
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

// GetBatchMemSize returns the total memory footprint of the batch.
func GetBatchMemSize(b coldata.Batch) int64 {
	if b == nil || b == coldata.ZeroBatch {
		return 0
	}
	// We need to get the capacity of the internal selection vector, even if b
	// currently doesn't use it, so we set selection to true and will reset
	// below.
	usesSel := b.Selection() != nil
	b.SetSelection(true)
	memUsage := selVectorSize(cap(b.Selection())) + getVecsMemoryFootprint(b.ColVecs())
	b.SetSelection(usesSel)
	return memUsage
}

// GetProportionalBatchMemSize returns the memory size of the batch that is
// proportional to given 'length'. This method returns the estimated memory
// footprint *only* of the first 'length' tuples in 'b'.
func GetProportionalBatchMemSize(b coldata.Batch, length int64) int64 {
	if length == 0 {
		return 0
	}
	usesSel := b.Selection() != nil
	b.SetSelection(true)
	selCapacity := cap(b.Selection())
	b.SetSelection(usesSel)
	proportionalBatchMemSize := int64(0)
	if selCapacity > 0 {
		proportionalBatchMemSize = selVectorSize(selCapacity) * length / int64(selCapacity)
	}
	for _, vec := range b.ColVecs() {
		if vec.IsBytesLike() {
			proportionalBatchMemSize += int64(coldata.ProportionalSize(vec, length))
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
// coldata.BatchSize() in capacity or maxBatchMemSize in the memory footprint.
// NOTE: if the reallocation occurs, then the memory under the old batch is
// released, so it is expected that the caller will lose the references to the
// old batch.
// Note: the method assumes that minCapacity is at least 0 and will clamp
// minCapacity to be between 1 and coldata.BatchSize() inclusive.
// TODO(yuzefovich): change the contract so that maxBatchMemSize takes priority
// over minCapacity.
func (a *Allocator) ResetMaybeReallocate(
	typs []*types.T, oldBatch coldata.Batch, minCapacity int, maxBatchMemSize int64,
) (newBatch coldata.Batch, reallocated bool) {
	if minCapacity < 0 {
		colexecerror.InternalError(errors.AssertionFailedf("invalid minCapacity %d", minCapacity))
	} else if minCapacity == 0 {
		minCapacity = 1
	} else if minCapacity > coldata.BatchSize() {
		minCapacity = coldata.BatchSize()
	}
	reallocated = true
	if oldBatch == nil {
		newBatch = a.NewMemBatchWithFixedCapacity(typs, minCapacity)
	} else {
		// If old batch is already of the largest capacity, we will reuse it.
		useOldBatch := oldBatch.Capacity() == coldata.BatchSize()
		// Avoid calculating the memory footprint if possible.
		var oldBatchMemSize int64
		if !useOldBatch {
			// Check if the old batch already reached the maximum memory size,
			// and use it if so. Note that we must check that the old batch has
			// enough capacity too.
			oldBatchMemSize = GetBatchMemSize(oldBatch)
			useOldBatch = oldBatchMemSize >= maxBatchMemSize && oldBatch.Capacity() >= minCapacity
		}
		if useOldBatch {
			reallocated = false
			oldBatch.ResetInternalBatch()
			newBatch = oldBatch
		} else {
			a.ReleaseMemory(oldBatchMemSize)
			newCapacity := oldBatch.Capacity() * 2
			if newCapacity < minCapacity {
				newCapacity = minCapacity
			}
			if newCapacity > coldata.BatchSize() {
				newCapacity = coldata.BatchSize()
			}
			newBatch = a.NewMemBatchWithFixedCapacity(typs, newCapacity)
		}
	}
	return newBatch, reallocated
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
	desiredCapacity := b.Capacity()
	if desiredCapacity == 0 {
		// In some cases (like when we have a windowed batch), the capacity
		// might be set to zero, yet we want to make sure that the vectors have
		// enough space to accommodate the length of the batch.
		desiredCapacity = b.Length()
	}
	if colIdx < width {
		presentVec := b.ColVec(colIdx)
		presentType := presentVec.Type()
		if presentType.Family() == types.UnknownFamily {
			// We already have an unknown vector in place. If this is expected,
			// then it will not be accessed and we're good; if this is not
			// expected, then an error will occur later.
			return
		}
		if presentType.Identical(t) {
			// We already have the vector of the desired type in place.
			if presentVec.Capacity() < desiredCapacity {
				// Unfortunately, the present vector is not of sufficient
				// capacity, so we need to replace it.
				oldMemUsage := getVecMemoryFootprint(presentVec)
				newEstimatedMemoryUsage := int64(EstimateBatchSizeBytes([]*types.T{t}, desiredCapacity))
				if err := a.acc.Grow(a.ctx, newEstimatedMemoryUsage-oldMemUsage); err != nil {
					colexecerror.InternalError(err)
				}
				b.ReplaceCol(a.NewMemColumn(t, desiredCapacity), colIdx)
				return
			}
			if presentVec.IsBytesLike() {
				// Flat bytes vector needs to be reset before the vector can be
				// reused.
				coldata.Reset(presentVec)
			}
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
	estimatedMemoryUsage := int64(EstimateBatchSizeBytes([]*types.T{t}, desiredCapacity))
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		colexecerror.InternalError(err)
	}
	b.AppendCol(a.NewMemColumn(t, desiredCapacity))
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

// PerformAppend is used to account for memory usage during calls to
// AppendBufferedBatch.AppendTuples. It is more efficient than PerformOperation
// for appending to Decimal column types since the expensive portion of the cost
// calculation only needs to be performed for the newly appended elements.
func (a *Allocator) PerformAppend(batch coldata.Batch, operation func()) {
	prevLength := batch.Length()
	var before int64
	for _, dest := range batch.ColVecs() {
		switch dest.CanonicalTypeFamily() {
		case types.DecimalFamily:
			// Don't add the size of the existing decimals to the 'before' cost, since
			// they are guaranteed not to be modified by an append operation.
			before += int64(sizeOfDecimals(dest.Decimal(), prevLength))
		case typeconv.DatumVecCanonicalTypeFamily:
			before += int64(dest.Datum().Size(prevLength))
		default:
			before += getVecMemoryFootprint(dest)
		}
	}
	operation()
	var after int64
	for _, dest := range batch.ColVecs() {
		switch dest.CanonicalTypeFamily() {
		case types.DecimalFamily:
			after += int64(sizeOfDecimals(dest.Decimal(), prevLength))
		case typeconv.DatumVecCanonicalTypeFamily:
			after += int64(dest.Datum().Size(prevLength))
		default:
			after += getVecMemoryFootprint(dest)
		}
	}
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

// sizeOfDecimals returns the size of the given decimals slice. It only accounts
// for the size of the decimal objects starting from the given index. For that
// reason, sizeOfDecimals is relatively cheap when startIdx >= length, and
// expensive when startIdx < length (with a maximum at startIdx = 0).
func sizeOfDecimals(decimals coldata.Decimals, startIdx int) uintptr {
	if startIdx >= cap(decimals) {
		return 0
	}
	if startIdx >= len(decimals) {
		return uintptr(cap(decimals)-startIdx) * sizeOfDecimal
	}
	if startIdx < 0 {
		startIdx = 0
	}
	// Account for the allocated memory beyond the length of the slice.
	size := uintptr(cap(decimals)-len(decimals)) * sizeOfDecimal
	for i := startIdx; i < decimals.Len(); i++ {
		size += tree.SizeOfDecimal(&decimals[i])
	}
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
	// We will track Uuid vectors separately because they use smaller initial
	// allocation factor.
	numUUIDVectors := 0
	for _, t := range vecTypes {
		switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
		case types.BoolFamily:
			acc += sizeOfBool
		case types.BytesFamily:
			if t.Family() == types.UuidFamily {
				numUUIDVectors++
			} else {
				numBytesVectors++
			}
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
		case types.JsonFamily:
			numBytesVectors++
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
	// For byte arrays, we initially allocate a constant number of bytes (plus
	// an int32 for the offset) for each row, so we use the sum of two values as
	// the estimate. However, later, the exact memory footprint will be used:
	// whenever a modification of Bytes takes place, the Allocator will measure
	// the old footprint and the updated one and will update the memory account
	// accordingly. We also account for the overhead and for the additional
	// offset value that are needed for Bytes vectors (to be in line with
	// coldata.Bytes.Size() method).
	var bytesVectorsSize int
	// Add the overhead.
	bytesVectorsSize += (numBytesVectors + numUUIDVectors) * (int(coldata.FlatBytesOverhead))
	// Add the data for both Bytes and Uuids.
	bytesVectorsSize += (numBytesVectors*coldata.BytesInitialAllocationFactor + numUUIDVectors*uuid.Size) * batchLength
	// Add the offsets.
	bytesVectorsSize += (numBytesVectors + numUUIDVectors) * sizeOfInt32 * (batchLength + 1)
	return acc*batchLength + bytesVectorsSize
}
