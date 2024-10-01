// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colmem

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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
	ctx context.Context
	acc *mon.BoundAccount
	// unlimitedAcc might be nil and is only used in some cases when the
	// allocation is denied by acc.
	unlimitedAcc *mon.BoundAccount
	factory      coldata.ColumnFactory
}

// SelVectorSize returns the memory usage of the selection vector of the given
// capacity.
func SelVectorSize(capacity int) int64 {
	return int64(capacity) * memsize.Int
}

func getVecMemoryFootprint(vec *coldata.Vec) int64 {
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

func getVecsMemoryFootprint(vecs []*coldata.Vec) int64 {
	var size int64
	for _, dest := range vecs {
		size += getVecMemoryFootprint(dest)
	}
	return size
}

func init() {
	coldata.GetBatchMemSize = GetBatchMemSize
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
	memUsage := SelVectorSize(cap(b.Selection())) + getVecsMemoryFootprint(b.ColVecs())
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
		proportionalBatchMemSize = SelVectorSize(selCapacity) * length / int64(selCapacity)
	}
	for _, vec := range b.ColVecs() {
		switch vec.CanonicalTypeFamily() {
		case types.BytesFamily, types.JsonFamily:
			proportionalBatchMemSize += coldata.ProportionalSize(vec, length)
		default:
			proportionalBatchMemSize += getVecMemoryFootprint(vec) * length / int64(vec.Capacity())
		}
	}
	return proportionalBatchMemSize
}

// NewAllocator constructs a new Allocator instance with an unlimited memory
// account.
func NewAllocator(
	ctx context.Context, unlimitedAcc *mon.BoundAccount, factory coldata.ColumnFactory,
) *Allocator {
	return &Allocator{
		ctx:     ctx,
		acc:     unlimitedAcc,
		factory: factory,
	}
}

// NewLimitedAllocator constructs a new Allocator instance which works with a
// limited memory account. The unlimited memory account is optional, and it'll
// be used only for the allocations that are denied by the limited memory
// account when using Allocator.PerformAppend, Allocator.PerformOperation, and
// SetAccountingHelper.AccountForSet as well as
// Allocator.AdjustMemoryUsageAfterAllocation.
func NewLimitedAllocator(
	ctx context.Context, limitedAcc, unlimitedAcc *mon.BoundAccount, factory coldata.ColumnFactory,
) *Allocator {
	return &Allocator{
		ctx:          ctx,
		acc:          limitedAcc,
		unlimitedAcc: unlimitedAcc,
		factory:      factory,
	}
}

// NewMemBatchWithFixedCapacity allocates a new in-memory coldata.Batch with the
// given vector capacity.
// Note: consider whether you want the dynamic batch size behavior (in which
// case you should be using ResetMaybeReallocate).
func (a *Allocator) NewMemBatchWithFixedCapacity(typs []*types.T, capacity int) coldata.Batch {
	estimatedMemoryUsage := SelVectorSize(capacity) + EstimateBatchSizeBytes(typs, capacity)
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
	estimatedMemoryUsage := SelVectorSize(capacity)
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		colexecerror.InternalError(err)
	}
	return coldata.NewMemBatchNoCols(typs, capacity)
}

// truncateToMemoryLimit returns the largest batch capacity that is still within
// the memory limit for the given type schema. The returned value is at most
// minDesiredCapacity and at least 1.
func truncateToMemoryLimit(minDesiredCapacity int, maxBatchMemSize int64, typs []*types.T) int {
	if maxBatchMemSize == noMemLimit {
		// If there is no memory limit, then we don't reduce the ask.
		return minDesiredCapacity
	}
	// If we have a memory limit, then make sure that it is sufficient for the
	// desired capacity, if not, reduce the ask.
	estimatedMemoryUsage := SelVectorSize(minDesiredCapacity) + EstimateBatchSizeBytes(typs, minDesiredCapacity)
	if estimatedMemoryUsage > maxBatchMemSize {
		// Perform the binary search to find the maximum allowed capacity.
		l, r := 1, minDesiredCapacity // [l, r)
		for l+1 < r {
			m := (l + r) / 2
			if SelVectorSize(m)+EstimateBatchSizeBytes(typs, m) > maxBatchMemSize {
				r = m
			} else {
				l = m
			}
		}
		minDesiredCapacity = l
	}
	return minDesiredCapacity
}

// growCapacity grows the capacity exponentially or up to minDesiredCapacity
// (whichever is larger) without exceeding maxBatchSize.
func growCapacity(oldCapacity int, minDesiredCapacity int, maxBatchSize int) int {
	newCapacity := oldCapacity * 2
	if newCapacity < minDesiredCapacity {
		newCapacity = minDesiredCapacity
	}
	if newCapacity > maxBatchSize {
		newCapacity = maxBatchSize
	}
	return newCapacity
}

// resetMaybeReallocate returns a batch that is guaranteed to be in a "reset"
// state (meaning it is ready to be used) and to have the capacity of at least
// 1. minDesiredCapacity is a hint about the capacity of the returned batch
// (subject to the memory limit).
//
// The method will grow the allocated capacity of the batch exponentially
// (possibly incurring a reallocation), until the batch reaches
// maxBatchSize in capacity or maxBatchMemSize in the memory footprint if
// desiredCapacitySufficient is false. When that parameter is true and the
// capacity of old batch is at least minDesiredCapacity, then the old batch is
// reused.
//
// oldBatchReachedMemSize is true IFF we calculated the memory footprint of the
// non-nil old batch and it reached maxBatchMemSize. The calculation only occurs
// if desiredCapacitySufficient is false or the old batch has the capacity less
// that minDesiredCapacity. If oldBatchReachedMemSize is true, then the old
// batch is reused (the converse is not necessarily true).
//
// If alwaysReallocate=true is used, then the old batch is never reused and a
// new one is always allocated.
//
// NOTE: if the reallocation occurs, then the memory under the old batch is
// released, so it is expected that the caller will lose the references to the
// old batch.
// Note: the method assumes that minDesiredCapacity is at least 0 and will clamp
// minDesiredCapacity to be between 1 and maxBatchSize inclusive.
func (a *Allocator) resetMaybeReallocate(
	typs []*types.T,
	oldBatch coldata.Batch,
	minDesiredCapacity int,
	maxBatchSize int,
	maxBatchMemSize int64,
	desiredCapacitySufficient bool,
	alwaysReallocate bool,
) (newBatch coldata.Batch, reallocated bool, oldBatchReachedMemSize bool) {
	if minDesiredCapacity < 0 {
		colexecerror.InternalError(errors.AssertionFailedf("invalid minDesiredCapacity %d", minDesiredCapacity))
	} else if minDesiredCapacity == 0 {
		minDesiredCapacity = 1
	} else if minDesiredCapacity > maxBatchSize {
		minDesiredCapacity = maxBatchSize
	}
	reallocated = true
	if oldBatch == nil {
		minDesiredCapacity = truncateToMemoryLimit(minDesiredCapacity, maxBatchMemSize, typs)
		newBatch = a.NewMemBatchWithFixedCapacity(typs, minDesiredCapacity)
	} else {
		oldCapacity := oldBatch.Capacity()
		var useOldBatch bool
		// Avoid calculating the memory footprint if possible.
		var oldBatchMemSize int64
		if oldCapacity == maxBatchSize {
			// If old batch is already of the largest capacity, we will reuse
			// it.
			useOldBatch = true
		} else {
			// Check that if we were to grow the capacity and allocate a new
			// batch, the new batch would still not exceed the limit.
			if estimatedMaxCapacity := truncateToMemoryLimit(
				growCapacity(oldCapacity, minDesiredCapacity, maxBatchSize), maxBatchMemSize, typs,
			); estimatedMaxCapacity < minDesiredCapacity {
				// Reduce the ask according to the estimated maximum. Note that
				// we do not set desiredCapacitySufficient to false since this
				// is the largest capacity we can allocate, so it doesn't matter
				// that the caller wanted more (similar to what we do with
				// clamping at coldata.BatchSize() above).
				minDesiredCapacity = estimatedMaxCapacity
				if estimatedMaxCapacity < int(float64(oldCapacity)*1.1) {
					// If we cannot grow the capacity of the old batch by more
					// than 10%, we might as well just reuse the old batch.
					minDesiredCapacity = oldCapacity
					desiredCapacitySufficient = true
				}
			}
			if desiredCapacitySufficient && oldCapacity >= minDesiredCapacity {
				// If the old batch already satisfies the desired capacity which
				// is sufficient, we will reuse it.
				useOldBatch = true
			} else {
				// Check if the old batch already reached the maximum memory
				// size, and use it if so.
				oldBatchMemSize = GetBatchMemSize(oldBatch)
				oldBatchReachedMemSize = oldBatchMemSize >= maxBatchMemSize
				useOldBatch = oldBatchReachedMemSize
			}
		}
		// If we want to use the old batch, but the batch reuse is not allowed,
		// we won't use the old one.
		if useOldBatch && alwaysReallocate {
			useOldBatch = false
			// Make sure that we get the footprint of the old batch so that it
			// can be correctly released from the allocator (it is the caller's
			// responsibility to track the memory usage of all previous
			// batches).
			if oldBatchMemSize == 0 {
				oldBatchMemSize = GetBatchMemSize(oldBatch)
				oldBatchReachedMemSize = oldBatchMemSize >= maxBatchMemSize
			}
		}
		if useOldBatch {
			reallocated = false
			oldBatch.ResetInternalBatch()
			newBatch = oldBatch
		} else {
			a.ReleaseMemory(oldBatchMemSize)
			newCapacity := growCapacity(oldCapacity, minDesiredCapacity, maxBatchSize)
			newCapacity = truncateToMemoryLimit(newCapacity, maxBatchMemSize, typs)
			newBatch = a.NewMemBatchWithFixedCapacity(typs, newCapacity)
		}
	}
	return newBatch, reallocated, oldBatchReachedMemSize
}

const noMemLimit = math.MaxInt64

// ResetMaybeReallocateNoMemLimit is the same as resetMaybeReallocate when
// MaxInt64 is used as the maxBatchMemSize argument and the desired capacity is
// sufficient. This should be used by the callers that know exactly the capacity
// they need and have no control over that number. It is guaranteed that the
// returned batch has the capacity of at least requiredCapacity (clamped to
// [1, coldata.BatchSize()] range).
func (a *Allocator) ResetMaybeReallocateNoMemLimit(
	typs []*types.T, oldBatch coldata.Batch, requiredCapacity int,
) (newBatch coldata.Batch, reallocated bool) {
	newBatch, reallocated, _ = a.resetMaybeReallocate(
		typs, oldBatch, requiredCapacity, coldata.BatchSize() /* maxBatchSize */, noMemLimit,
		true /* desiredCapacitySufficient */, false, /* alwaysReallocate */
	)
	return newBatch, reallocated
}

// NewVec returns a new coldata.Vec of the desired capacity.
// NOTE: consider whether you should be using MaybeAppendColumn,
// NewMemBatchWith*, or ResetMaybeReallocate methods.
func (a *Allocator) NewVec(t *types.T, capacity int) *coldata.Vec {
	estimatedMemoryUsage := EstimateBatchSizeBytes([]*types.T{t}, capacity)
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		colexecerror.InternalError(err)
	}
	return coldata.NewVec(t, capacity, a.factory)
}

// MaybeAppendColumn might append a newly allocated coldata.Vec of the given
// type to b at position colIdx. The vector is guaranteed to be in a "reset"
// state when this function returns (meaning that no nulls are set,
// coldata.Bytes.Reset is called if applicable, etc).
//
// Behavior of the function depends on how colIdx compares to the width of b:
// 1. if colIdx < b.Width(), then we expect that correctly-typed vector is
// already present in position colIdx. If that's not the case, we will panic.
// Nulls are unset on the vector.
// 2. if colIdx == b.Width(), then we will append a newly allocated coldata.Vec
// of the given type.
// 3. if colIdx > b.Width(), then we will panic because such condition
// indicates an error in setting up vector type enforcers during the planning
// stage.
//
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
				b.ReplaceCol(a.NewVec(t, desiredCapacity), colIdx)
				return
			}
			coldata.ResetIfBytesLike(presentVec)
			if presentVec.MaybeHasNulls() {
				presentVec.Nulls().UnsetNulls()
			}
			return
		}
		// We have a vector with an unexpected type, so we panic.
		colexecerror.InternalError(errors.AssertionFailedf(
			"trying to add a column of %s type at index %d but %s vector already present",
			t.SQLStringForError(), colIdx, presentType.SQLStringForError(),
		))
	} else if colIdx > width {
		// We have a batch of unexpected width which indicates an error in the
		// planning stage.
		colexecerror.InternalError(errors.AssertionFailedf(
			"trying to add a column of %s type at index %d but batch has width %d",
			t.SQLStringForError(), colIdx, width,
		))
	}
	estimatedMemoryUsage := EstimateBatchSizeBytes([]*types.T{t}, desiredCapacity)
	if err := a.acc.Grow(a.ctx, estimatedMemoryUsage); err != nil {
		colexecerror.InternalError(err)
	}
	b.AppendCol(a.NewVec(t, desiredCapacity))
}

// PerformOperation executes 'operation' (that somehow modifies 'destVecs') and
// updates the memory account accordingly.
// NOTE: if some columnar vectors are not modified, they should not be included
// in 'destVecs' to reduce the performance hit of memory accounting.
func (a *Allocator) PerformOperation(destVecs []*coldata.Vec, operation func()) {
	before := getVecsMemoryFootprint(destVecs)
	// To simplify the accounting, we perform the operation first and then will
	// update the memory account. The minor "drift" in accounting that is
	// caused by this approach is ok.
	operation()
	after := getVecsMemoryFootprint(destVecs)

	a.AdjustMemoryUsageAfterAllocation(after - before)
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
	a.AdjustMemoryUsageAfterAllocation(after - before)
}

// Used returns the number of bytes currently allocated through this allocator.
func (a *Allocator) Used() int64 {
	return a.acc.Used()
}

// Acc returns the memory account of the Allocator.
func (a *Allocator) Acc() *mon.BoundAccount {
	return a.acc
}

// adjustMemoryUsage adjusts the number of bytes currently allocated through
// this allocator by delta bytes (which can be both positive or negative).
//
// If:
//   - afterAllocation is true,
//   - the allocator was created via NewLimitedAllocator with a non-nil unlimited
//     memory account,
//   - the positive delta allocation is denied by the limited memory account,
//
// then the unlimited account is grown by delta. The memory error is still
// thrown.
func (a *Allocator) adjustMemoryUsage(delta int64, afterAllocation bool) {
	if delta > 0 {
		if err := a.acc.Grow(a.ctx, delta); err != nil {
			// If we were given a separate unlimited account and the adjustment
			// is performed after the allocation has already occurred, then grow
			// the unlimited account.
			if a.unlimitedAcc != nil && afterAllocation {
				if newErr := a.unlimitedAcc.Grow(a.ctx, delta); newErr != nil {
					// Prefer the error from the unlimited account since it
					// indicates that --max-sql-memory pool has been used up.
					colexecerror.InternalError(newErr)
				}
			}
			colexecerror.InternalError(err)
		}
	} else if delta < 0 {
		a.ReleaseMemory(-delta)
	}
}

// AdjustMemoryUsage adjusts the number of bytes currently allocated through
// this allocator by delta bytes (which can be both positive or negative).
func (a *Allocator) AdjustMemoryUsage(delta int64) {
	a.adjustMemoryUsage(delta, false /* afterAllocation */)
}

// AdjustMemoryUsageAfterAllocation is similar to AdjustMemoryUsage with a
// difference that if 1) the allocator was created via NewLimitedAllocator, and
// 2) the allocation is denied by the limited memory account, then the unlimited
// account will be grown. The memory error is still thrown. It should be used
// whenever the caller has already incurred an allocation of delta bytes, and it
// is desirable to account for that allocation against some budget.
func (a *Allocator) AdjustMemoryUsageAfterAllocation(delta int64) {
	a.adjustMemoryUsage(delta, true /* afterAllocation */)
}

// ReleaseMemory reduces the number of bytes currently allocated through this
// allocator by (at most) size bytes. size must be non-negative.
func (a *Allocator) ReleaseMemory(size int64) {
	if size < 0 {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly negative size in ReleaseMemory: %d", size))
	} else if size == 0 {
		return
	}
	if size > a.acc.Used() {
		size = a.acc.Used()
	}
	a.acc.Shrink(a.ctx, size)
}

// ReleaseAll releases all of the reservations from the allocator. The usage of
// this method implies that the memory account of the allocator is not shared
// with any other component.
func (a *Allocator) ReleaseAll() {
	a.ReleaseMemory(a.Used())
	if a.unlimitedAcc != nil {
		a.unlimitedAcc.Shrink(a.ctx, a.unlimitedAcc.Used())
	}
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
	for _, t := range vecTypes {
		switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
		case types.BytesFamily, types.JsonFamily:
			numBytesVectors++
		case types.DecimalFamily:
			// Similar to byte arrays, we can't tell how much space is used
			// to hold the arbitrary precision decimal objects because they
			// can contain a variable-length portion. However, most values
			// (those with a coefficient which can fit in a uint128) do not
			// contain any indirection and are stored entirely inline, so we
			// use the flat struct size as an estimate.
			acc += memsize.Decimal
		case typeconv.DatumVecCanonicalTypeFamily:
			// In datum vec we need to account for memory underlying the struct
			// that is the implementation of tree.Datum interface (for example,
			// tree.DBoolFalse) as well as for the overhead of storing that
			// implementation in the slice of tree.Datums. Note that if t is of
			// variable size, the memory will be properly accounted in
			// getVecMemoryFootprint.
			// Note: keep the calculation here in line with datumVec.Size.
			implementationSize, _ := tree.DatumTypeSize(t)
			acc += int64(implementationSize) + memsize.DatumOverhead
		case
			types.BoolFamily,
			types.IntFamily,
			types.FloatFamily,
			types.TimestampTZFamily,
			types.IntervalFamily:
			// Types that have a statically known size.
			acc += GetFixedSizeTypeSize(t)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", t.SQLStringForError()))
		}
	}
	// For byte arrays, we initially allocate a constant number of bytes for
	// each row (namely coldata.ElementSize). However, later, the exact memory
	// footprint will be used: whenever a modification of Bytes takes place, the
	// Allocator will measure the old footprint and the updated one and will
	// update the memory account accordingly.
	bytesVectorsSize := int64(numBytesVectors) * (coldata.FlatBytesOverhead + int64(batchLength)*coldata.ElementSize)
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
		colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", t.SQLStringForError()))
	}
	return size
}

// AccountingHelper is a helper that provides a reasonable heuristic for
// reallocating batches with ResetMaybeReallocate() function.
//
// The heuristic is as follows:
//   - the first time a batch exceeds the memory limit, its capacity is memorized,
//     and from now on that capacity will determine the upper bound on the
//     capacities of the batches allocated through the helper;
//   - if at any point in time a batch exceeds the memory limit by at least a
//     factor of two, then that batch is discarded, and the capacity will never
//     exceed half of the capacity of the discarded batch;
//   - if the memory limit is not reached, then the behavior of the dynamic growth
//     of the capacity provided by Allocator.resetMaybeReallocate is still
//     applicable (i.e. the capacities will grow exponentially until
//     coldata.BatchSize()).
//
// NOTE: it works under the assumption that only a single coldata.Batch is being
// used.
type AccountingHelper struct {
	allocator *Allocator
	// maxBatchSize determines the maximum size of the batches produced by the
	// helper in rows (coldata.BatchSize() by default).
	maxBatchSize int
	// memoryLimit determines the maximum memory footprint of the batch.
	memoryLimit int64
	// maxCapacity if non-zero indicates the target capacity of the batch. It is
	// set once the batch exceeds the memory limit. It will be reduced even
	// further if the batch significantly exceeds the memory limit.
	// TODO(yuzefovich): consider growing the maxCapacity after the number of
	// "successes" (a batch of maxCapacity not reaching the memory limit)
	// reaches some threshold.
	maxCapacity int
	// alwaysReallocate, if set, indicates that a new batch must be returned on
	// each ResetMaybeReallocate call. At the moment, it can only be set by the
	// SetAccountingHelper.
	alwaysReallocate bool
}

// discardBatch returns true if the batch with the given memory footprint has
// exceeded the limit by too much and should be discarded.
func (h *AccountingHelper) discardBatch(batchMemSize int64) bool {
	// We use the division instead of multiplication to avoid issues with the
	// overflow.
	return batchMemSize/2 >= h.memoryLimit
}

// Init initializes the helper. The allocator can be shared with other
// components.
func (h *AccountingHelper) Init(allocator *Allocator, memoryLimit int64) {
	h.allocator = allocator
	h.maxBatchSize = coldata.BatchSize()
	if memoryLimit == 1 {
		// The memory limit of 1 most likely indicates that we are in a "force
		// disk spilling" scenario, but the helper should ignore that, so we
		// override it to the default value of the distsql_workmem variable.
		memoryLimit = 64 << 20 /* 64 MiB */
	}
	h.memoryLimit = memoryLimit
}

// ResetMaybeReallocate returns a batch that is guaranteed to be in a "reset"
// state (meaning it is ready to be used) and to have the capacity of at least
// 1.
//
// The method will grow the allocated capacity of the batch exponentially
// (possibly incurring a reallocation), until the batch reaches
// coldata.BatchSize() in capacity or the target memory limit (specified in
// Init()) in the memory footprint. If the limit is exceeded by at least a
// factor of two, then the old batch is discarded, and the new batch will be
// allocated of at most half of the capacity (the capacity will never increase
// from that point).
//
// - tuplesToBeSet, if positive, indicates the total number of tuples that are
// yet to be set. Zero and negative values are ignored.
//
// NOTE: if the reallocation occurs, then the memory under the old batch is
// released, so it is expected that the caller will lose the references to the
// old batch.
func (h *AccountingHelper) ResetMaybeReallocate(
	typs []*types.T, oldBatch coldata.Batch, tuplesToBeSet int,
) (newBatch coldata.Batch, reallocated bool) {
	if oldBatch != nil {
		// First, do a quick check whether the allocator as a whole has exceeded
		// the limit by too much. (The allocator here is allowed to be shared
		// with other components, thus, we cannot ask it directly for the batch
		// mem size, yet the allocator can provide a useful upper bound.)
		if batchMemSizeUpperBound := h.allocator.Used(); h.discardBatch(batchMemSizeUpperBound) {
			// Now check whether the precise footprint of the batch is too much.
			if batchMemSize := GetBatchMemSize(oldBatch); h.discardBatch(batchMemSize) {
				// The old batch has exceeded the memory limit by too much, so
				// we release it and will allocate a new one that is at most
				// half of the capacity.
				newMaxCapacity := (oldBatch.Capacity() + 1) / 2 // round up
				if h.maxCapacity == 0 || newMaxCapacity < h.maxCapacity {
					h.maxCapacity = newMaxCapacity
				}
				h.allocator.ReleaseMemory(batchMemSize)
				oldBatch = nil
			}
		}
	}
	// Ignore the negative values.
	if tuplesToBeSet < 0 {
		tuplesToBeSet = 0
	}
	// By default, assume that the number of tuples to be set is sufficient and
	// ask for it. If that number is unknown, we'll rely on the
	// Allocator.resetMaybeReallocate method to provide the dynamically-growing
	// batches.
	minDesiredCapacity := tuplesToBeSet
	desiredCapacitySufficient := tuplesToBeSet > 0
	if h.maxCapacity > 0 && (h.maxCapacity <= tuplesToBeSet || tuplesToBeSet == 0) {
		// If we have already exceeded the max capacity, and
		// - that capacity doesn't exceed the number of tuples to be set, or
		// - the number of tuples to be set is unknown,
		// then we'll use that max capacity and tell the allocator to not try
		// allocating larger batch.
		minDesiredCapacity = h.maxCapacity
		desiredCapacitySufficient = true
	}
	var oldBatchReachedMemSize bool
	newBatch, reallocated, oldBatchReachedMemSize = h.allocator.resetMaybeReallocate(
		typs, oldBatch, minDesiredCapacity, h.maxBatchSize, h.memoryLimit, desiredCapacitySufficient, h.alwaysReallocate,
	)
	if oldBatchReachedMemSize && h.maxCapacity == 0 {
		// The old batch has just reached the memory size for the first time, so
		// we memorize the maximum capacity. Note that this is not strictly
		// necessary to do (since Allocator.resetMaybeReallocate would never
		// allocate a new batch from now on), but it makes things more clear and
		// allows us to avoid computing the memory size of the batch on each
		// call.
		h.maxCapacity = oldBatch.Capacity()
	} else if reallocated && GetBatchMemSize(newBatch) >= h.memoryLimit {
		// A new batch has just been allocated and it exceeds the memory limit,
		// so we memorize its capacity to use from now on. Notably, this will
		// also ensure that the SetAccountingHelper will use the full capacity
		// of this batch when variable-width types are present.
		if buildutil.CrdbTestBuild {
			if batchMemSize := GetBatchMemSize(newBatch); h.discardBatch(batchMemSize) && newBatch.Capacity() > 1 {
				colexecerror.InternalError(errors.AssertionFailedf(
					"newly-allocated batch of capacity %d should be discarded right away: "+
						"memory limit %d, batch mem size %d", newBatch.Capacity(), h.memoryLimit, batchMemSize,
				))
			}
		}
		h.maxCapacity = newBatch.Capacity()
	}
	return newBatch, reallocated
}

// SetAccountingHelper is a utility struct that should be used by callers that
// only perform "set" operations on the coldata.Batch (i.e. neither copies nor
// appends). It encapsulates the logic for performing the memory accounting for
// these sets.
// NOTE: it works under the assumption that only the last coldata.Batch returned
// by ResetMaybeReallocate is being modified by the caller.
type SetAccountingHelper struct {
	helper AccountingHelper

	// curCapacity is the capacity of the last batch returned by
	// ResetMaybeReallocate.
	curCapacity int

	// allFixedLength indicates that we're working with the type schema of only
	// fixed-length elements.
	allFixedLength bool

	// bytesLikeVecIdxs stores the indices of all bytes-like vectors.
	bytesLikeVecIdxs intsets.Fast
	// bytesLikeVectors stores all actual bytes-like vectors. It is updated
	// every time a new batch is allocated.
	bytesLikeVectors []*coldata.Bytes
	// prevBytesLikeTotalSize tracks the total size of the bytes-like vectors
	// that we have already accounted for.
	prevBytesLikeTotalSize int64

	// varSizeVecIdxs stores the indices of all vectors with variable sized
	// values except for the bytes-like ones.
	varSizeVecIdxs intsets.Fast
	// decimalVecs and datumVecs store all decimal and datum-backed vectors,
	// respectively. They are updated every time a new batch is allocated.
	decimalVecs []coldata.Decimals
	datumVecs   []coldata.DatumVec
	// varSizeDatumSizes stores the amount of space we have accounted for for
	// the corresponding "row" of variable length values in the last batch that
	// the helper has touched. This is necessary to track because when the batch
	// is reset, the vectors still have references to the old datums, so we need
	// to adjust the accounting only by the delta. Similarly, once a new batch
	// is allocated, we need to track the estimate that we have already
	// accounted for.
	//
	// Note that because ResetMaybeReallocate caps the capacity of the batch at
	// coldata.BatchSize(), this slice will never exceed coldata.BatchSize() in
	// size, and we choose to ignore it for the purposes of memory accounting.
	varSizeDatumSizes []int64
	// varSizeEstimatePerRow is the total estimated size of single values from
	// varSizeVecIdxs vectors which is accounted for by EstimateBatchSizeBytes.
	// It serves as the initial value for varSizeDatumSizes values.
	varSizeEstimatePerRow int64
}

// Init initializes the helper. The allocator must **not** be shared with any
// other component.
// - alwaysReallocate indicates whether a fresh batch must be returned on each
// ResetMaybeReallocate call. If this option is used, the SetAccountingHelper
// releases the memory of the previous batch from its accounting (in other words
// only the last batch returned by ResetMaybeReallocate is accounted for by the
// helper), so it is the caller's responsibility to track the memory usage of
// all batches except for the last one (should the caller choose to keep them).
func (h *SetAccountingHelper) Init(
	allocator *Allocator, memoryLimit int64, typs []*types.T, alwaysReallocate bool,
) {
	h.helper.Init(allocator, memoryLimit)
	h.helper.alwaysReallocate = alwaysReallocate

	numDecimalVecs := 0
	for vecIdx, typ := range typs {
		switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
		case types.BytesFamily, types.JsonFamily:
			h.bytesLikeVecIdxs.Add(vecIdx)
		case types.DecimalFamily:
			h.varSizeVecIdxs.Add(vecIdx)
			h.varSizeEstimatePerRow += memsize.Decimal
			numDecimalVecs++
		case typeconv.DatumVecCanonicalTypeFamily:
			estimate, isVarlen := tree.DatumTypeSize(typ)
			if isVarlen {
				h.varSizeVecIdxs.Add(vecIdx)
				h.varSizeEstimatePerRow += int64(estimate) + memsize.DatumOverhead
			}
		}
	}

	h.allFixedLength = h.bytesLikeVecIdxs.Empty() && h.varSizeVecIdxs.Empty()
	h.bytesLikeVectors = make([]*coldata.Bytes, h.bytesLikeVecIdxs.Len())
	h.decimalVecs = make([]coldata.Decimals, numDecimalVecs)
	h.datumVecs = make([]coldata.DatumVec, h.varSizeVecIdxs.Len()-numDecimalVecs)
}

// SetMaxBatchSize use this to get more or less than the coldata.BatchSize()
// default.
func (h *SetAccountingHelper) SetMaxBatchSize(maxBatchSize int) {
	h.helper.maxBatchSize = maxBatchSize
}

func (h *SetAccountingHelper) getBytesLikeTotalSize() int64 {
	var bytesLikeTotalSize int64
	for _, b := range h.bytesLikeVectors {
		bytesLikeTotalSize += b.Size()
	}
	return bytesLikeTotalSize
}

// ResetMaybeReallocate is a light wrapper on top of
// AccountingHelper.ResetMaybeReallocate (and thus has the same contract) with
// an additional logic for memory tracking purposes.
// - tuplesToBeSet, if positive, indicates the total number of tuples that are
// yet to be set. Zero and negative values are ignored.
func (h *SetAccountingHelper) ResetMaybeReallocate(
	typs []*types.T, oldBatch coldata.Batch, tuplesToBeSet int,
) (newBatch coldata.Batch, reallocated bool) {
	newBatch, reallocated = h.helper.ResetMaybeReallocate(typs, oldBatch, tuplesToBeSet)
	h.curCapacity = newBatch.Capacity()
	if reallocated && !h.allFixedLength {
		// Allocator.resetMaybeReallocate has released the precise memory
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
				switch vecs[vecIdx].CanonicalTypeFamily() {
				case types.BytesFamily:
					h.bytesLikeVectors = append(h.bytesLikeVectors, vecs[vecIdx].Bytes())
				case types.JsonFamily:
					h.bytesLikeVectors = append(h.bytesLikeVectors, &vecs[vecIdx].JSON().Bytes)
				default:
					colexecerror.InternalError(
						errors.AssertionFailedf(
							"unexpected bytes-like type: %s", typs[vecIdx].SQLStringForError(),
						),
					)
				}
			}
			h.prevBytesLikeTotalSize = h.getBytesLikeTotalSize()
		}
		if !h.varSizeVecIdxs.Empty() {
			h.decimalVecs = h.decimalVecs[:0]
			h.datumVecs = h.datumVecs[:0]
			for vecIdx, ok := h.varSizeVecIdxs.Next(0); ok; vecIdx, ok = h.varSizeVecIdxs.Next(vecIdx + 1) {
				if vecs[vecIdx].CanonicalTypeFamily() == types.DecimalFamily {
					h.decimalVecs = append(h.decimalVecs, vecs[vecIdx].Decimal())
				} else {
					h.datumVecs = append(h.datumVecs, vecs[vecIdx].Datum())
				}
			}
			if cap(h.varSizeDatumSizes) < newBatch.Capacity() {
				h.varSizeDatumSizes = make([]int64, newBatch.Capacity())
			} else {
				h.varSizeDatumSizes = h.varSizeDatumSizes[:newBatch.Capacity()]
			}
			for i := range h.varSizeDatumSizes {
				h.varSizeDatumSizes[i] = h.varSizeEstimatePerRow
			}
		}
	}
	return newBatch, reallocated
}

// AccountForSet updates the Allocator according to the new variable length
// values in the row rowIdx in the batch that was returned by the last call to
// ResetMaybeReallocate. It returns a boolean indicating whether the batch is
// done (i.e. no more rows should be set on it before it is reset).
func (h *SetAccountingHelper) AccountForSet(rowIdx int) (batchDone bool) {
	// The batch is done if we've just set the last row that the batch has the
	// capacity for.
	batchDone = h.curCapacity == rowIdx+1
	if h.allFixedLength {
		// All vectors are of fixed-length and are already correctly accounted
		// for. We also utilize the whole capacity since setting extra rows
		// incurs no additional memory usage.
		return batchDone
	}

	if len(h.bytesLikeVectors) > 0 {
		newBytesLikeTotalSize := h.getBytesLikeTotalSize()
		h.helper.allocator.AdjustMemoryUsageAfterAllocation(newBytesLikeTotalSize - h.prevBytesLikeTotalSize)
		h.prevBytesLikeTotalSize = newBytesLikeTotalSize
	}

	if !h.varSizeVecIdxs.Empty() {
		var newVarLengthDatumSize int64
		for _, decimalVec := range h.decimalVecs {
			d := decimalVec.Get(rowIdx)
			newVarLengthDatumSize += int64(d.Size())
		}
		for _, datumVec := range h.datumVecs {
			datumSize := datumVec.Get(rowIdx).(tree.Datum).Size()
			newVarLengthDatumSize += int64(datumSize) + memsize.DatumOverhead
		}
		h.helper.allocator.AdjustMemoryUsageAfterAllocation(newVarLengthDatumSize - h.varSizeDatumSizes[rowIdx])
		h.varSizeDatumSizes[rowIdx] = newVarLengthDatumSize
	}

	// The allocator is not shared with any other components, so we can just use
	// it directly to get the memory footprint of the batch.
	batchMemSize := h.helper.allocator.Used()
	if (h.helper.maxCapacity == 0 && batchMemSize >= h.helper.memoryLimit) || h.helper.discardBatch(batchMemSize) {
		// This is either
		// - the first time we exceeded the memory limit, or
		// - the batch has just significantly exceeded the memory limit, so
		// we update the memorized capacity. If it's the latter, then on the
		// following call to ResetMaybeReallocate, the batch will be discarded.
		h.helper.maxCapacity = rowIdx + 1
	}
	if h.helper.maxCapacity > 0 && h.helper.maxCapacity == rowIdx+1 {
		// The batch is done if we've exceeded the memory limit, and we've just
		// set the last row according to the memorized capacity.
		batchDone = true
	}
	return batchDone
}

// TestingUpdateMemoryLimit sets the new memory limit as well as resets the
// memorized max capacity. It should only be used in tests.
func (h *SetAccountingHelper) TestingUpdateMemoryLimit(memoryLimit int64) {
	h.helper.memoryLimit = memoryLimit
	h.helper.maxCapacity = 0
}

// ReleaseMemory releases all of the memory that is currently registered with
// the helper.
func (h *SetAccountingHelper) ReleaseMemory() {
	if h.helper.allocator != nil {
		// Protect from the cases when Release() has already been called.
		h.helper.allocator.ReleaseAll()
	}
}

// Release releases all of the resources so that they can be garbage collected.
// It should be called once the caller is done with batch manipulation.
func (h *SetAccountingHelper) Release() {
	*h = SetAccountingHelper{}
}
