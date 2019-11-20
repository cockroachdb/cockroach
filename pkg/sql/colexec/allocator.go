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
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

var zeroBatch = coldata.NewMemBatchWithSize(nil /* types */, 0 /* size */)

func init() {
	zeroBatch.SetLength(0)
}

// Allocator is a memory management tool for vectorized components. It provides
// new batches (and appends to existing ones) within a fixed memory budget. If
// the budget is exceeded, it will panic with an error.
//
// In the future this can also be used to pool coldata.Vec allocations.
type Allocator struct {
	ctx context.Context
	acc *mon.BoundAccount
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
	selVectorSize := size * sizeOfUint16
	estimatedStaticMemoryUsage := int64(estimateBatchSizeBytes(types, size) + selVectorSize)
	if err := a.acc.Grow(a.ctx, estimatedStaticMemoryUsage); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return coldata.NewMemBatchWithSize(types, size)
}

// NewMemColumn returns a new coldata.Vec, initialized with a length.
func (a *Allocator) NewMemColumn(t coltypes.T, n int) coldata.Vec {
	estimatedStaticMemoryUsage := int64(estimateBatchSizeBytes([]coltypes.T{t}, n))
	if err := a.acc.Grow(a.ctx, estimatedStaticMemoryUsage); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return coldata.NewMemColumn(t, n)
}

// AppendColumn appends a newly allocated coldata.Vec of the given type to b.
func (a *Allocator) AppendColumn(b coldata.Batch, t coltypes.T) {
	estimatedStaticMemoryUsage := int64(estimateBatchSizeBytes([]coltypes.T{t}, int(coldata.BatchSize())))
	if err := a.acc.Grow(a.ctx, estimatedStaticMemoryUsage); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	col := a.NewMemColumn(t, int(coldata.BatchSize()))
	b.AppendCol(col)
}

// Append appends elements of a source coldata.Vec into dest according to
// coldata.SliceArgs.
func (a *Allocator) Append(dest coldata.Vec, args coldata.SliceArgs) {
	var delta int64
	if dest.Type() == coltypes.Bytes {
		destBytes, srcBytes := dest.Bytes(), args.Src.Bytes()
		delta -= int64(destBytes.Slice(int(args.DestIdx), destBytes.Len()).Size()) - int64(coldata.FlatBytesOverhead)
		if args.Sel != nil {
			for idx := args.SrcStartIdx; idx < args.SrcEndIdx; idx++ {
				start := int(args.Sel[idx])
				end := start + 1
				delta += int64(srcBytes.Slice(start, end).Size()) - int64(coldata.FlatBytesOverhead)
			}
		} else {
			delta += int64(srcBytes.Slice(int(args.SrcStartIdx), int(args.SrcEndIdx)).Size()) - int64(coldata.FlatBytesOverhead)
		}
	} else {
		delta -= int64(estimateBatchSizeBytes([]coltypes.T{dest.Type()}, dest.Length()-int(args.DestIdx)))
		delta += int64(estimateBatchSizeBytes([]coltypes.T{dest.Type()}, int(args.SrcEndIdx-args.SrcStartIdx)))
	}
	if err := a.acc.Grow(a.ctx, delta); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	dest.Append(args)
}

// TODO(yuzefovich): Vec.Copy and execgen.SET need to also go through the
// Allocator.

const (
	sizeOfBool    = int(unsafe.Sizeof(true))
	sizeOfInt16   = int(unsafe.Sizeof(int16(0)))
	sizeOfInt32   = int(unsafe.Sizeof(int32(0)))
	sizeOfInt64   = int(unsafe.Sizeof(int64(0)))
	sizeOfFloat64 = int(unsafe.Sizeof(float64(0)))
	sizeOfTime    = int(unsafe.Sizeof(time.Time{}))
	sizeOfUint16  = int(unsafe.Sizeof(uint16(0)))
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
			// TODO(yuzefovich): we actually do not allocate any memory for b.data
			// when calling NewBytes(), so remove this estimation once execgen.SET is
			// handled by the Allocator.

			// We don't know without looking at the data in a batch to see how
			// much space each byte array takes up. Use some default value as a
			// heuristic right now.
			acc += 100
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
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %s", t))
		}
	}
	return acc * batchLength
}
