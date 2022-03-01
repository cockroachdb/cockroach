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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
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
	return int64(capacity) * memsize.Int
}

func getVecMemoryFootprint(vec coldata.Vec) int64 {
	if vec == nil {
		return 0
	}
	switch vec.CanonicalTypeFamily() {
	case types.BytesFamily:
		return vec.Bytes().Size()
	case types.DecimalFamily:
		return sizeOfDecimals(vec.Decimal(), 0 /* startIdx */)
	case types.JsonFamily:
		return vec.JSON().Size()
	case typeconv.DatumVecCanonicalTypeFamily:
		return vec.Datum().Size(0 /* startIdx */)
	}
	return EstimateBatchSizeBytes([]*types.T{vec.Type()}, vec.Capacity())
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
			proportionalBatchMemSize += coldata.ProportionalSize(vec, length)
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
	estimatedMemoryUsage := selVectorSize(capacity) + EstimateBatchSizeBytes(typs, capacity)
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
// 1. minDesiredCapacity is a hint about the capacity of the returned batch
// (subject to the memory limit).
//
// The method will grow the allocated capacity of the batch exponentially
// (possibly incurring a reallocation), until the batch reaches
// coldata.BatchSize() in capacity or maxBatchMemSize in the memory footprint.
//
// NOTE: if the reallocation occurs, then the memory under the old batch is
// released, so it is expected that the caller will lose the references to the
// old batch.
// Note: the method assumes that minDesiredCapacity is at least 0 and will clamp
// minDesiredCapacity to be between 1 and coldata.BatchSize() inclusive.
func (a *Allocator) ResetMaybeReallocate(
	typs []*types.T, oldBatch coldata.Batch, minDesiredCapacity int, maxBatchMemSize int64,
) (newBatch coldata.Batch, reallocated bool) {
	if minDesiredCapacity < 0 {
		colexecerror.InternalError(errors.AssertionFailedf("invalid minDesiredCapacity %d", minDesiredCapacity))
	} else if minDesiredCapacity == 0 {
		minDesiredCapacity = 1
	} else if minDesiredCapacity > coldata.BatchSize() {
		minDesiredCapacity = coldata.BatchSize()
	}
	reallocated = true
	if oldBatch == nil {
		newBatch = a.NewMemBatchWithFixedCapacity(typs, minDesiredCapacity)
	} else {
		// If old batch is already of the largest capacity, we will reuse it.
		useOldBatch := oldBatch.Capacity() == coldata.BatchSize()
		// Avoid calculating the memory footprint if possible.
		var oldBatchMemSize int64
		if !useOldBatch {
			// Check if the old batch already reached the maximum memory size,
			// and use it if so.
			oldBatchMemSize = GetBatchMemSize(oldBatch)
			useOldBatch = oldBatchMemSize >= maxBatchMemSize
		}
		if useOldBatch {
			reallocated = false
			a.ReleaseMemory(oldBatch.ResetInternalBatch())
			newBatch = oldBatch
		} else {
			a.ReleaseMemory(oldBatchMemSize)
			newCapacity := oldBatch.Capacity() * 2
			if newCapacity < minDesiredCapacity {
				newCapacity = minDesiredCapacity
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
	estimatedMemoryUsage := EstimateBatchSizeBytes([]*types.T{t}, capacity)
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
				newEstimatedMemoryUsage := EstimateBatchSizeBytes([]*types.T{t}, desiredCapacity)
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
			} else if presentVec.CanonicalTypeFamily() == typeconv.DatumVecCanonicalTypeFamily {
				a.ReleaseMemory(presentVec.Datum().Reset())
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
	estimatedMemoryUsage := EstimateBatchSizeBytes([]*types.T{t}, desiredCapacity)
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
			before += sizeOfDecimals(dest.Decimal(), prevLength)
		case typeconv.DatumVecCanonicalTypeFamily:
			before += dest.Datum().Size(prevLength)
		default:
			before += getVecMemoryFootprint(dest)
		}
	}
	operation()
	var after int64
	for _, dest := range batch.ColVecs() {
		switch dest.CanonicalTypeFamily() {
		case types.DecimalFamily:
			after += sizeOfDecimals(dest.Decimal(), prevLength)
		case typeconv.DatumVecCanonicalTypeFamily:
			after += dest.Datum().Size(prevLength)
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
	} else if delta < 0 {
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

// sizeOfDecimals returns the size of the given decimals slice. It only accounts
// for the size of the decimal objects starting from the given index. For that
// reason, sizeOfDecimals is relatively cheap when startIdx >= length, and
// expensive when startIdx < length (with a maximum at startIdx = 0).
func sizeOfDecimals(decimals coldata.Decimals, startIdx int) int64 {
	if startIdx >= cap(decimals) {
		return 0
	}
	if startIdx >= len(decimals) {
		return int64(cap(decimals)-startIdx) * memsize.Decimal
	}
	if startIdx < 0 {
		startIdx = 0
	}
	// Account for the allocated memory beyond the length of the slice.
	size := int64(cap(decimals)-len(decimals)) * memsize.Decimal
	for i := startIdx; i < decimals.Len(); i++ {
		size += int64(decimals[i].Size())
	}
	return size
}

// SizeOfBatchSizeSelVector is the size (in bytes) of a selection vector of
// coldata.BatchSize() length.
var SizeOfBatchSizeSelVector = int64(coldata.BatchSize()) * memsize.Int

// EstimateBatchSizeBytes returns an estimated amount of bytes needed to
// store a batch in memory that has column types vecTypes.
// WARNING: This only is correct for fixed width types, and returns an
// estimate for non fixed width types. In future it might be possible to
// remove the need for estimation by specifying batch sizes in terms of bytes.
func EstimateBatchSizeBytes(vecTypes []*types.T, batchLength int) int64 {
	if batchLength == 0 {
		return 0
	}
	// acc represents the number of bytes to represent a row in the batch
	// (excluding any Bytes vectors, those are tracked separately).
	var acc int64
	numBytesVectors := 0
	// We will track Uuid vectors separately because they use smaller initial
	// allocation factor.
	numUUIDVectors := 0
	for _, t := range vecTypes {
		switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
		case types.BytesFamily:
			if t.Family() == types.UuidFamily {
				numUUIDVectors++
			} else {
				numBytesVectors++
			}
		case types.DecimalFamily:
			// Similar to byte arrays, we can't tell how much space is used
			// to hold the arbitrary precision decimal objects because they
			// can contain a variable-length portion. However, most values
			// (those with a coefficient which can fit in a uint128) do not
			// contain any indirection and are stored entirely inline, so we
			// use the flat struct size as an estimate.
			acc += memsize.Decimal
		case types.JsonFamily:
			numBytesVectors++
		case typeconv.DatumVecCanonicalTypeFamily:
			// Initially, only []tree.Datum slice is allocated for the
			// datum-backed vectors right away, so that's what we're including
			// in the estimate. Later on, once the actual values are set, they
			// will be accounted for properly.
			acc += memsize.DatumOverhead
		case
			types.BoolFamily,
			types.IntFamily,
			types.FloatFamily,
			types.TimestampTZFamily,
			types.IntervalFamily:
			// Types that have a statically known size.
			acc += GetFixedSizeTypeSize(t)
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
	var bytesVectorsSize int64
	// Add the overhead.
	bytesVectorsSize += int64(numBytesVectors+numUUIDVectors) * coldata.FlatBytesOverhead
	// Add the data for both Bytes and Uuids.
	bytesVectorsSize += int64(numBytesVectors*coldata.BytesInitialAllocationFactor+numUUIDVectors*uuid.Size) * int64(batchLength)
	// Add the offsets.
	bytesVectorsSize += int64(numBytesVectors+numUUIDVectors) * memsize.Int32 * int64(batchLength+1)
	return acc*int64(batchLength) + bytesVectorsSize
}

// GetFixedSizeTypeSize returns the size of a type that is not variable in size;
// e.g. its size is known statically.
func GetFixedSizeTypeSize(t *types.T) (size int64) {
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	case types.BoolFamily:
		size = memsize.Bool
	case types.IntFamily:
		switch t.Width() {
		case 16:
			size = memsize.Int16
		case 32:
			size = memsize.Int32
		default:
			size = memsize.Int64
		}
	case types.FloatFamily:
		size = memsize.Float64
	case types.TimestampTZFamily:
		// time.Time consists of two 64 bit integers and a pointer to
		// time.Location. We will only account for this 3 bytes without paying
		// attention to the full time.Location struct. The reason is that it is
		// likely that time.Location's are cached and are shared among all the
		// timestamps, so if we were to include that in the estimation, we would
		// significantly overestimate.
		// TODO(yuzefovich): figure out whether the caching does take place.
		size = memsize.Time
	case types.IntervalFamily:
		size = memsize.Duration
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", t))
	}
	return size
}

// SetAccountingHelper is a utility struct that should be used by callers that
// only perform "set" operations on the coldata.Batch (i.e. neither copies nor
// appends). It encapsulates the logic for performing the memory accounting for
// these sets.
// NOTE: it works under the assumption that only a single coldata.Batch is being
// used.
type SetAccountingHelper struct {
	Allocator *Allocator

	// allFixedLength indicates that we're working with the type schema of only
	// fixed-length elements.
	allFixedLength bool

	// bytesLikeVecIdxs stores the indices of all bytes-like vectors.
	bytesLikeVecIdxs util.FastIntSet
	// bytesLikeVectors stores all actual bytes-like vectors. It is updated
	// every time a new batch is allocated.
	bytesLikeVectors []*coldata.Bytes
	// prevBytesLikeTotalSize tracks the total size of the bytes-like vectors
	// that we have already accounted for.
	prevBytesLikeTotalSize int64

	// decimalVecIdxs stores the indices of all decimal vectors.
	decimalVecIdxs util.FastIntSet
	// decimalVecs stores all decimal vectors. They are updated every time a new
	// batch is allocated.
	decimalVecs []coldata.Decimals
	// decimalSizes stores the amount of space we have accounted for for the
	// corresponding decimal values in the corresponding row of the last batch
	// that the helper has touched. This is necessary to track because when the
	// batch is reset, the vectors still have references to the old decimals, so
	// we need to adjust the accounting only by the delta. Similarly, once a new
	// batch is allocated, we need to track the estimate that we have already
	// accounted for.
	//
	// Note that because ResetMaybeReallocate caps the capacity of the batch at
	// coldata.BatchSize(), this slice will never exceed coldata.BatchSize() in
	// size, and we choose to ignore it for the purposes of memory accounting.
	decimalSizes []int64

	// varLenDatumVecIdxs stores the indices of all datum-backed vectors with
	// variable-length values.
	varLenDatumVecIdxs util.FastIntSet
	// varLenDatumVecs stores all variable-sized datum-backed vectors. They are
	// updated every time a new batch is allocated.
	varLenDatumVecs []coldata.DatumVec
}

// Init initializes the helper.
func (h *SetAccountingHelper) Init(allocator *Allocator, typs []*types.T) {
	h.Allocator = allocator

	for vecIdx, typ := range typs {
		switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
		case types.BytesFamily, types.JsonFamily:
			h.bytesLikeVecIdxs.Add(vecIdx)
		case types.DecimalFamily:
			h.decimalVecIdxs.Add(vecIdx)
		case typeconv.DatumVecCanonicalTypeFamily:
			h.varLenDatumVecIdxs.Add(vecIdx)
		}
	}

	h.allFixedLength = h.bytesLikeVecIdxs.Empty() && h.decimalVecIdxs.Empty() && h.varLenDatumVecIdxs.Empty()
	h.bytesLikeVectors = make([]*coldata.Bytes, h.bytesLikeVecIdxs.Len())
	h.decimalVecs = make([]coldata.Decimals, h.decimalVecIdxs.Len())
	h.varLenDatumVecs = make([]coldata.DatumVec, h.varLenDatumVecIdxs.Len())
}

func (h *SetAccountingHelper) getBytesLikeTotalSize() int64 {
	var bytesLikeTotalSize int64
	for _, b := range h.bytesLikeVectors {
		bytesLikeTotalSize += b.Size()
	}
	return bytesLikeTotalSize
}

// ResetMaybeReallocate is a light wrapper on top of
// Allocator.ResetMaybeReallocate (and thus has the same contract) with an
// additional logic for memory tracking purposes.
func (h *SetAccountingHelper) ResetMaybeReallocate(
	typs []*types.T, oldBatch coldata.Batch, minCapacity int, maxBatchMemSize int64,
) (newBatch coldata.Batch, reallocated bool) {
	newBatch, reallocated = h.Allocator.ResetMaybeReallocate(
		typs, oldBatch, minCapacity, maxBatchMemSize,
	)
	if reallocated && !h.allFixedLength {
		// Allocator.ResetMaybeReallocate has released the precise memory
		// footprint of the old batch and has accounted for the estimated
		// footprint of the new batch. This means that we need to update our
		// internal memory tracking state to those estimates.
		//
		// Note that the loops below have type switches, but that is acceptable
		// given that a batch is reallocated limited number of times throughout
		// the lifetime of the helper's user (namely, at most
		// log2(coldata.BatchSize())+1 (=11 by default) times since we double
		// the capacity until coldata.BatchSize()).
		vecs := newBatch.ColVecs()
		if !h.bytesLikeVecIdxs.Empty() {
			h.bytesLikeVectors = h.bytesLikeVectors[:0]
			for vecIdx, ok := h.bytesLikeVecIdxs.Next(0); ok; vecIdx, ok = h.bytesLikeVecIdxs.Next(vecIdx + 1) {
				if vecs[vecIdx].CanonicalTypeFamily() == types.BytesFamily {
					h.bytesLikeVectors = append(h.bytesLikeVectors, vecs[vecIdx].Bytes())
				} else {
					h.bytesLikeVectors = append(h.bytesLikeVectors, &vecs[vecIdx].JSON().Bytes)
				}
			}
			h.prevBytesLikeTotalSize = h.getBytesLikeTotalSize()
		}
		if !h.decimalVecIdxs.Empty() {
			h.decimalVecs = h.decimalVecs[:0]
			for vecIdx, ok := h.decimalVecIdxs.Next(0); ok; vecIdx, ok = h.decimalVecIdxs.Next(vecIdx + 1) {
				h.decimalVecs = append(h.decimalVecs, vecs[vecIdx].Decimal())
			}
			h.decimalSizes = make([]int64, newBatch.Capacity())
			for i := range h.decimalSizes {
				// In EstimateBatchSizeBytes, memsize.Decimal has already been
				// accounted for for each decimal value, so we multiple that by
				// the number of decimal vectors to get already included
				// footprint of all decimal values in a single row.
				h.decimalSizes[i] = int64(len(h.decimalVecs)) * memsize.Decimal
			}
		}
		if !h.varLenDatumVecIdxs.Empty() {
			h.varLenDatumVecs = h.varLenDatumVecs[:0]
			for vecIdx, ok := h.varLenDatumVecIdxs.Next(0); ok; vecIdx, ok = h.varLenDatumVecIdxs.Next(vecIdx + 1) {
				h.varLenDatumVecs = append(h.varLenDatumVecs, vecs[vecIdx].Datum())
			}
		}
	}
	return newBatch, reallocated
}

// AccountForSet updates the Allocator according to the new variable length
// values in the row rowIdx in the batch that was returned by the last call to
// ResetMaybeReallocate.
func (h *SetAccountingHelper) AccountForSet(rowIdx int) {
	if h.allFixedLength {
		// All vectors are of fixed-length and are already correctly accounted
		// for.
		return
	}

	if len(h.bytesLikeVectors) > 0 {
		newBytesLikeTotalSize := h.getBytesLikeTotalSize()
		h.Allocator.AdjustMemoryUsage(newBytesLikeTotalSize - h.prevBytesLikeTotalSize)
		h.prevBytesLikeTotalSize = newBytesLikeTotalSize
	}

	if !h.decimalVecIdxs.Empty() {
		var newDecimalSizes int64
		for _, decimalVec := range h.decimalVecs {
			d := decimalVec.Get(rowIdx)
			newDecimalSizes += int64(d.Size())
		}
		h.Allocator.AdjustMemoryUsage(newDecimalSizes - h.decimalSizes[rowIdx])
		h.decimalSizes[rowIdx] = newDecimalSizes
	}

	if !h.varLenDatumVecIdxs.Empty() {
		var newVarLengthDatumSize int64
		for _, datumVec := range h.varLenDatumVecs {
			datumSize := datumVec.Get(rowIdx).(tree.Datum).Size()
			// Note that we're ignoring the overhead of tree.Datum because it
			// was already included in EstimateBatchSizeBytes.
			newVarLengthDatumSize += int64(datumSize)
		}
		h.Allocator.AdjustMemoryUsage(newVarLengthDatumSize)
	}
}

// Release releases all of the resources so that they can be garbage collected.
// It should be called once the caller is done with batch manipulation.
func (h *SetAccountingHelper) Release() {
	*h = SetAccountingHelper{}
}
