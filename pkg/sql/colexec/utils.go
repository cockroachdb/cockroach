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
	"fmt"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

var (
	zeroBoolColumn   = make([]bool, coldata.MaxBatchSize)
	zeroIntColumn    = make([]int, coldata.MaxBatchSize)
	zeroUint64Column = make([]uint64, coldata.MaxBatchSize)

	zeroDecimalValue  apd.Decimal
	zeroFloat64Value  float64
	zeroInt64Value    int64
	zeroIntervalValue duration.Duration
)

// overloadHelper is a utility struct that helps us avoid allocations
// of temporary decimals on every overloaded operation with them as well as
// plumbs through other useful information. In order for the templates to see
// it correctly, a local variable named `_overloadHelper` of this type must be declared
// before the inlined overloaded code.
type overloadHelper struct {
	tmpDec1, tmpDec2 apd.Decimal
	binFn            *tree.BinOp
	evalCtx          *tree.EvalContext
}

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
		partitioner:  partitioner,
		partitionIdx: partitionIdx,
		batch:        allocator.NewMemBatch(types),
	}
}

// partitionerToOperator is an Operator that Dequeue's from the corresponding
// partition on every call to Next. It is a converter from filled in
// PartitionedQueue to Operator.
type partitionerToOperator struct {
	colexecbase.ZeroInputNode
	NonExplainable

	partitioner  colcontainer.PartitionedQueue
	partitionIdx int
	batch        coldata.Batch
}

var _ colexecbase.Operator = &partitionerToOperator{}

func (p *partitionerToOperator) Init() {}

func (p *partitionerToOperator) Next(ctx context.Context) coldata.Batch {
	if err := p.partitioner.Dequeue(ctx, p.partitionIdx, p.batch); err != nil {
		colexecerror.InternalError(err)
	}
	return p.batch
}

func newAppendOnlyBufferedBatch(
	allocator *colmem.Allocator, typs []*types.T, initialSize int,
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
// types.Bytes type - which would result in a quadratic behavior - because
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
	typs    []*types.T
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
	colexecerror.InternalError("AppendCol is prohibited on appendOnlyBufferedBatch")
}

func (b *appendOnlyBufferedBatch) ReplaceCol(coldata.Vec, int) {
	colexecerror.InternalError("ReplaceCol is prohibited on appendOnlyBufferedBatch")
}

// append is a helper method that appends all tuples with indices in range
// [startIdx, endIdx) from batch (paying attention to the selection vector)
// into b.
// NOTE: this does *not* perform memory accounting.
func (b *appendOnlyBufferedBatch) append(batch coldata.Batch, startIdx, endIdx int) {
	for i, colVec := range b.colVecs {
		colVec.Append(
			coldata.SliceArgs{
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

// GetDatumToPhysicalFn returns a function for converting a datum of the given
// ColumnType to the corresponding Go type. Note that the signature of the
// return function doesn't contain an error since we assume that the conversion
// must succeed. If for some reason it fails, a panic will be emitted and will
// be caught by the panic-catcher mechanism of the vectorized engine and will
// be propagated as an error accordingly.
func GetDatumToPhysicalFn(ct *types.T) func(tree.Datum) interface{} {
	switch ct.Family() {
	case types.BoolFamily:
		return func(datum tree.Datum) interface{} {
			return bool(*datum.(*tree.DBool))
		}
	case types.BytesFamily:
		return func(datum tree.Datum) interface{} {
			return encoding.UnsafeConvertStringToBytes(string(*datum.(*tree.DBytes)))
		}
	case types.IntFamily:
		switch ct.Width() {
		case 16:
			return func(datum tree.Datum) interface{} {
				return int16(*datum.(*tree.DInt))
			}
		case 32:
			return func(datum tree.Datum) interface{} {
				return int32(*datum.(*tree.DInt))
			}
		case 0, 64:
			return func(datum tree.Datum) interface{} {
				return int64(*datum.(*tree.DInt))
			}
		}
		colexecerror.InternalError(fmt.Sprintf("unhandled INT width %d", ct.Width()))
	case types.DateFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DDate).UnixEpochDaysWithOrig()
		}
	case types.FloatFamily:
		return func(datum tree.Datum) interface{} {
			return float64(*datum.(*tree.DFloat))
		}
	case types.OidFamily:
		return func(datum tree.Datum) interface{} {
			return int64(datum.(*tree.DOid).DInt)
		}
	case types.StringFamily:
		return func(datum tree.Datum) interface{} {
			// Handle other STRING-related OID types, like oid.T_name.
			wrapper, ok := datum.(*tree.DOidWrapper)
			if ok {
				datum = wrapper.Wrapped
			}
			return encoding.UnsafeConvertStringToBytes(string(*datum.(*tree.DString)))
		}
	case types.DecimalFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DDecimal).Decimal
		}
	case types.UuidFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DUuid).UUID.GetBytesMut()
		}
	case types.TimestampFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DTimestamp).Time
		}
	case types.TimestampTZFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DTimestampTZ).Time
		}
	case types.IntervalFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DInterval).Duration
		}

	// Types backed by tree.Datums.
	case types.CollatedStringFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DCollatedString)
		}
	case types.UnknownFamily:
		return func(datum tree.Datum) interface{} {
			return tree.DNull
		}
	case types.ArrayFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DArray)
		}
	case types.INetFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DIPAddr)
		}
	case types.TimeFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DTime)
		}
	case types.JsonFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DJSON)
		}
	case types.TimeTZFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DTimeTZ)
		}
	case types.TupleFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DTuple)
		}
	case types.BitFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DBitArray)
		}
	case types.GeometryFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DGeometry)
		}
	case types.GeographyFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DGeography)
		}
	case types.EnumFamily:
		return func(datum tree.Datum) interface{} {
			return datum.(*tree.DEnum)
		}
	}
	colexecerror.InternalError(fmt.Sprintf("unexpectedly unhandled type %s", ct.DebugString()))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
