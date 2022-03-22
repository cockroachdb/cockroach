// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
//go:build execgen_template
// +build execgen_template

//
// This file is the execgen template for range_offset_handler.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexecwindow

import (
	"context"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ apd.Context
	_ = coldataext.CompareDatum
	_ duration.Duration
	_ pgdate.Date
)

// {{/*

// Declarations to make the template compile properly.
const _OFFSET_BOUND = execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING
const _ORDER_DIRECTION = false
const _IS_START = false
const _TYPE_FAMILY = types.UnknownFamily
const _TYPE_WIDTH = 0

type _OFFSET_GOTYPE interface{}
type _CMP_GOTYPE interface{}

// _VALUE_BY_OFFSET is the template function for assigning the first input to
// the result of the second input + or - the third input, depending on the type
// of bound and direction of the ordering column.
func _VALUE_BY_OFFSET(_, _, _ interface{}) {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// _ASSIGN_CMP is the template function for assigning the first input to the
// result of a comparison between the second and third inputs.
func _ASSIGN_CMP(_, _, _, _ interface{}) {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

// rangeOffsetHandler is a utility operator used to retrieve the location of the
// start or end bound for each row when in RANGE mode with an offset.
type rangeOffsetHandler interface {
	// startPartition resets the rangeOffsetHandler for use on the current
	// partition.
	startPartition(storedCols *colexecutils.SpillingBuffer, peersColIdx, ordColIdx int)

	// getIdx provides an updated bound index for the current row given the
	// location of the last bound index. getIdx should only be called for the
	// first row of each peer group.
	getIdx(ctx context.Context, currRow, lastIdx int) int

	// close releases references to prevent memory leaks.
	close()
}

func newRangeOffsetHandler(
	evalCtx *tree.EvalContext,
	datumAlloc *tree.DatumAlloc,
	bound *execinfrapb.WindowerSpec_Frame_Bound,
	ordColType *types.T,
	ordColAsc, isStart bool,
) rangeOffsetHandler {
	switch bound.BoundType {
	// {{range .}}
	case _OFFSET_BOUND:
		switch isStart {
		// {{range .Bounds}}
		case _IS_START:
			switch ordColAsc {
			// {{range .Directions}}
			case _ORDER_DIRECTION:
				switch ordColType.Family() {
				// {{range .TypeFamilies}}
				case _TYPE_FAMILY:
					switch ordColType.Width() {
					// {{range .WidthOverloads}}
					case _TYPE_WIDTH:
						op := &_OP_STRING{
							offset: decodeOffset(datumAlloc, ordColType, bound.TypedOffset).(_OFFSET_GOTYPE),
						}
						// {{if eq .VecMethod "Datum"}}
						// {{if .BinOpIsPlus}}
						binOp, _, _ := tree.WindowFrameRangeOps{}.LookupImpl(
							ordColType, getOffsetType(ordColType))
						// {{else}}
						_, binOp, _ := tree.WindowFrameRangeOps{}.LookupImpl(
							ordColType, getOffsetType(ordColType))
						// {{end}}
						op.overloadHelper = colexecbase.BinaryOverloadHelper{BinFn: binOp.Fn, EvalCtx: evalCtx}
						// {{end}}
						return op
						// {{end}}
					}
					// {{end}}
				}
				// {{end}}
			}
			// {{end}}
		}
		// {{end}}
	}
	colexecerror.InternalError(errors.AssertionFailedf("unsupported range offset"))
	return nil
}

// rangeOffsetHandlerBase extracts common fields and methods of the
// rangeOffsetHandler utility operators.
type rangeOffsetHandlerBase struct {
	storedCols  *colexecutils.SpillingBuffer
	ordColIdx   int
	peersColIdx int
}

// {{range .}}
// {{range .Bounds}}
// {{range .Directions}}
// {{range .TypeFamilies}}
// {{range .WidthOverloads}}

// _OP_STRING is a utility operator used to retrieve the location of
// the start or end bound for each row when in RANGE mode with an offset.
type _OP_STRING struct {
	rangeOffsetHandlerBase
	// {{if eq .VecMethod "Datum"}}
	overloadHelper colexecbase.BinaryOverloadHelper
	// {{end}}
	offset _OFFSET_GOTYPE
}

var _ rangeOffsetHandler = &_OP_STRING{}

// getIdx provides an updated bound index for the current row given the
// location of the last bound index. It is called for the first row of each
// peer group. For example:
//
//    ord col
//    -------
//       1
//       2
//       2
//       3
//
//   currRow: 1
//   lastIdx: 0
//   offset:  1
//
// Assume we are calculating the end index for an ascending column. In this
// case, the value at the current row is '2' and the offset is '1' unit. So,
// getIdx will advance from the last end index (which is '0') until it reaches
// the first row for which the value is greater than 2 + 1 = 3, or the end of
// the partition, whichever comes first. In this case, the returned index would
// be '4' to indicate that the end index is the end of the partition.
func (h *_OP_STRING) getIdx(ctx context.Context, currRow, lastIdx int) (idx int) {
	// {{if eq .VecMethod "Datum"}}
	// {{/*
	//     In order to inline the templated code of the binary overloads
	//     operating on datums, we need to have a `_overloadHelper` local
	//     variable of type `colexecbase.BinaryOverloadHelper`. This is
	//     necessary when dealing with Time and TimeTZ columns since they aren't
	//     yet handled natively.
	// */}}
	_overloadHelper := h.overloadHelper
	// {{end}}

	if lastIdx >= h.storedCols.Length() {
		return lastIdx
	}

	var vec coldata.Vec
	var vecIdx, n int
	vec, vecIdx, _ = h.storedCols.GetVecWithTuple(ctx, h.ordColIdx, currRow)

	// When the order column is null, the offset is ignored. The start index is
	// the first null value, and the end index is the end of the null group. All
	// null rows have the same start and end indices.
	if vec.Nulls().MaybeHasNulls() && vec.Nulls().NullAt(vecIdx) {
		// Since this function is only called for the first row of each peer group,
		// this is the first null row.
		idx = currRow
		// {{if not .IsStart}}
		// We need to scan beyond the last null to retrieve the end index.
		idx++
		for {
			if idx >= h.storedCols.Length() {
				break
			}
			vec, vecIdx, n = h.storedCols.GetVecWithTuple(ctx, h.peersColIdx, idx)
			peersCol := vec.Bool()
			_, _ = peersCol[vecIdx], peersCol[n-1]
			for ; vecIdx < n; vecIdx++ {
				//gcassert:bce
				if peersCol[vecIdx] {
					return idx
				}
				idx++
			}
		}
		// {{end}}
		return idx
	}

	// The current row is not null. Retrieve the value that is equal to that at
	// the current row plus (minus if descending) 'offset' logical units.
	var (
		seekVal   _CMP_GOTYPE
		cmpResult int
	)
	col := vec.TemplateType()
	currRowVal := col.Get(vecIdx)
	_VALUE_BY_OFFSET(seekVal, currRowVal, h.offset)

	// Pick up where the last index left off, since the start and indexes of each
	// successive window frame are non-decreasing as we increment the current row.
	// (This follows from the fact that offsets are required to be constants).
	idx = lastIdx

	// {{if .IsOrdColAsc}}
	// If the ordering column is null at lastIdx, we must have just moved past the
	// end of the null rows (we already checked that the current row isn't null).
	// The index is only updated when a new peer group is encountered, and
	// non-null and null rows are never part of the same frame. So, we have to
	// iterate past any leading nulls.
	// {{/* Only do this for an ascending column because nulls order first. */}}
	vec, vecIdx, _ = h.storedCols.GetVecWithTuple(ctx, h.ordColIdx, idx)
	if vec.Nulls().MaybeHasNulls() && vec.Nulls().NullAt(vecIdx) {
		idx++
		var foundNonNull bool
		for {
			if idx >= h.storedCols.Length() {
				return idx
			}
			vec, vecIdx, n = h.storedCols.GetVecWithTuple(ctx, h.peersColIdx, idx)
			peersCol := vec.Bool()
			_, _ = peersCol[vecIdx], peersCol[n-1]
			for ; vecIdx < n; vecIdx++ {
				//gcassert:bce
				if peersCol[vecIdx] {
					// We have scanned past the null rows. We can't return yet, since we
					// may still need to continue scanning past non-null rows.
					foundNonNull = true
					break
				}
				idx++
			}
			if foundNonNull {
				break
			}
		}
	}
	// {{end}}

	// Scan to the next peer group and then compare to the value indicated by
	// the offset. If the comparison fails, scan again to the next peer group
	// and repeat.
	var peersVec coldata.Vec
	for {
		if idx >= h.storedCols.Length() {
			break
		}
		peersVec, vecIdx, n = h.storedCols.GetVecWithTuple(ctx, h.peersColIdx, idx)
		vec, _, _ = h.storedCols.GetVecWithTuple(ctx, h.ordColIdx, idx)
		peersCol := peersVec.Bool()
		col = vec.TemplateType()
		_, _ = peersCol[vecIdx], peersCol[n-1]
		for ; vecIdx < n; vecIdx++ {
			//gcassert:bce
			if peersCol[vecIdx] {
				// {{if not .IsOrdColAsc}}
				// Nulls and non-nulls are never part of the same window frame, which
				// means that this index must be the end of the current frame.
				// {{/*
				// We only have to check for nulls when the column is descending because
				// nulls order first and we only scan forward from the last index.
				// */}}
				if vec.Nulls().MaybeHasNulls() && vec.Nulls().NullAt(vecIdx) {
					return idx
				}
				// {{end}}
				cmpVal := col.Get(vecIdx)
				_ASSIGN_CMP(cmpResult, cmpVal, seekVal, col)
				// {{if .IsOrdColAsc}}
				// {{if .IsStart}}
				if cmpResult >= 0 {
					return idx
				}
				// {{else}}
				if cmpResult > 0 {
					return idx
				}
				// {{end}}
				// {{else}}
				// {{if .IsStart}}
				if cmpResult <= 0 {
					return idx
				}
				// {{else}}
				if cmpResult < 0 {
					return idx
				}
				// {{end}}
				// {{end}}
			}
			idx++
		}
	}
	return idx
}

func (h *_OP_STRING) close() {
	*h = _OP_STRING{}
}

// {{end}}
// {{end}}
// {{end}}
// {{end}}
// {{end}}

// startPartition resets the rangeOffsetHandler for use on the current
// partition.
func (b *rangeOffsetHandlerBase) startPartition(
	storedCols *colexecutils.SpillingBuffer, peersColIdx, ordColIdx int,
) {
	b.storedCols = storedCols
	b.peersColIdx = peersColIdx
	b.ordColIdx = ordColIdx
}

// decodeOffset decodes the given encoded offset into the given type.
func decodeOffset(
	datumAlloc *tree.DatumAlloc, orderColType *types.T, typedOffset []byte,
) interface{} {
	offsetType := getOffsetType(orderColType)
	datum, err := execinfra.DecodeDatum(datumAlloc, offsetType, typedOffset)
	if err != nil {
		colexecerror.InternalError(err)
	}
	switch typeconv.TypeFamilyToCanonicalTypeFamily(orderColType.Family()) {
	case typeconv.DatumVecCanonicalTypeFamily:
		return datum
	}
	typeConverter := colconv.GetDatumToPhysicalFn(offsetType)
	return typeConverter(datum)
}

// getOffsetType returns the offset type that corresponds to the given ordering
// column type.
func getOffsetType(orderColType *types.T) *types.T {
	if types.IsDateTimeType(orderColType) {
		return types.Interval
	}
	return orderColType
}
