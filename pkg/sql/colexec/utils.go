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
	"reflect"

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
	"github.com/cockroachdb/errors"
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
// ColumnType to the corresponding Go type.
func GetDatumToPhysicalFn(ct *types.T) func(tree.Datum) (interface{}, error) {
	switch ct.Family() {
	case types.BoolFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBool)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBool, found %s", reflect.TypeOf(datum))
			}
			return bool(*d), nil
		}
	case types.BytesFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBytes)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBytes, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case types.IntFamily:
		switch ct.Width() {
		case 16:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int16(*d), nil
			}
		case 32:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int32(*d), nil
			}
		case 0, 64:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int64(*d), nil
			}
		}
		colexecerror.InternalError(fmt.Sprintf("unhandled INT width %d", ct.Width()))
	case types.DateFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDate)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDate, found %s", reflect.TypeOf(datum))
			}
			return d.UnixEpochDaysWithOrig(), nil
		}
	case types.FloatFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DFloat)
			if !ok {
				return nil, errors.Errorf("expected *tree.DFloat, found %s", reflect.TypeOf(datum))
			}
			return float64(*d), nil
		}
	case types.OidFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DOid)
			if !ok {
				return nil, errors.Errorf("expected *tree.DOid, found %s", reflect.TypeOf(datum))
			}
			return int64(d.DInt), nil
		}
	case types.StringFamily:
		return func(datum tree.Datum) (interface{}, error) {
			// Handle other STRING-related OID types, like oid.T_name.
			wrapper, ok := datum.(*tree.DOidWrapper)
			if ok {
				datum = wrapper.Wrapped
			}

			d, ok := datum.(*tree.DString)
			if !ok {
				return nil, errors.Errorf("expected *tree.DString, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case types.DecimalFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDecimal)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDecimal, found %s", reflect.TypeOf(datum))
			}
			return d.Decimal, nil
		}
	case types.UuidFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DUuid)
			if !ok {
				return nil, errors.Errorf("expected *tree.DUuid, found %s", reflect.TypeOf(datum))
			}
			return d.UUID.GetBytesMut(), nil
		}
	case types.TimestampFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DTimestamp)
			if !ok {
				return nil, errors.Errorf("expected *tree.DTimestamp, found %s", reflect.TypeOf(datum))
			}
			return d.Time, nil
		}
	case types.TimestampTZFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DTimestampTZ)
			if !ok {
				return nil, errors.Errorf("expected *tree.DTimestampTZ, found %s", reflect.TypeOf(datum))
			}
			return d.Time, nil
		}
	case types.IntervalFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DInterval)
			if !ok {
				return nil, errors.Errorf("expected *tree.DInterval, found %s", reflect.TypeOf(datum))
			}
			return d.Duration, nil
		}

	// Types backed by tree.Datums.
	case types.CollatedStringFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DCollatedString)
			if !ok {
				return nil, errors.Errorf("expected *tree.DCollatedString, found %s", reflect.TypeOf(datum))
			}
			return d, nil
		}
	case types.UnknownFamily:
		return func(datum tree.Datum) (interface{}, error) {
			if datum != tree.DNull {
				return nil, errors.Errorf("unexpectedly datum is not tree.DNull for types.UnknownFamily: %+v", datum)
			}
			return tree.DNull, nil
		}
	case types.ArrayFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DArray)
			if !ok {
				return nil, errors.Errorf("expected *tree.DArray, found %s", reflect.TypeOf(datum))
			}
			return d, nil
		}
	case types.INetFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DIPAddr)
			if !ok {
				return nil, errors.Errorf("expected *tree.DIPAddr, found %s", reflect.TypeOf(datum))
			}
			return d, nil
		}
	case types.TimeFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DTime)
			if !ok {
				return nil, errors.Errorf("expected *tree.DTime, found %s", reflect.TypeOf(datum))
			}
			return d, nil
		}
	case types.JsonFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DJSON)
			if !ok {
				return nil, errors.Errorf("expected *tree.DJSON, found %s", reflect.TypeOf(datum))
			}
			return d, nil
		}
	case types.TimeTZFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DTimeTZ)
			if !ok {
				return nil, errors.Errorf("expected *tree.DTimeTZ, found %s", reflect.TypeOf(datum))
			}
			return d, nil
		}
	case types.TupleFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DTuple)
			if !ok {
				return nil, errors.Errorf("expected *tree.DTuple, found %s", reflect.TypeOf(datum))
			}
			return d, nil
		}
	case types.BitFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBitArray)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBitArray, found %s", reflect.TypeOf(datum))
			}
			return d, nil
		}
	case types.GeometryFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DGeometry)
			if !ok {
				return nil, errors.Errorf("expected *tree.DGeometry, found %s", reflect.TypeOf(datum))
			}
			return d, nil
		}
	case types.GeographyFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DGeography)
			if !ok {
				return nil, errors.Errorf("expected *tree.DGeography, found %s", reflect.TypeOf(datum))
			}
			return d, nil
		}
	case types.EnumFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DEnum)
			if !ok {
				return nil, errors.Errorf("expected *tree.DEnum, found %s", reflect.TypeOf(datum))
			}
			return d, nil
		}
	}
	colexecerror.InternalError(fmt.Sprintf("unexpectedly unhandled type %s", ct.DebugString()))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
