// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

import (
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// IndexedRow is a row with a corresponding index.
type IndexedRow struct {
	Idx int
	Row Datums
}

// WindowFrameRun contains the runtime state of window frame during calculations.
type WindowFrameRun struct {
	// constant for all calls to WindowFunc.Add
	Rows             []IndexedRow
	ArgIdxStart      int                // the index which arguments to the window function begin
	ArgCount         int                // the number of window function arguments
	Frame            *WindowFrame       // If non-nil, Frame represents the frame specification of this window. If nil, default frame is used.
	StartBoundOffset int                // non-negative logical or physical offset from the current row for the start of the frame.
	EndBoundOffset   int                // non-negative logical or physical offset from the current row for the end of the frame.
	Direction        encoding.Direction // If set, Direction indicates how the column is ordered. It is only required for RANGE mode.

	// changes for each row (each call to WindowFunc.Add)
	RowIdx int // the current row index

	// changes for each peer group
	FirstPeerIdx int // the first index in the current peer group
	PeerRowCount int // the number of rows in the current peer group
}

// getValueByOffset returns a datum calculated as the value of the current row
// plus offset (logical) and an error if encountered.
func (wfr WindowFrameRun) getValueByOffset(offset int) (Datum, error) {
	switch v := wfr.valueAt(wfr.RowIdx).(type) {
	case *DInt:
		return NewDInt(*v + DInt(offset)), nil
	case *DFloat:
		return NewDFloat(*v + DFloat(offset)), nil
	case *DDecimal:
		value := &DDecimal{}
		value.SetCoefficient(int64(offset))
		_, err := ExactCtx.Add(&value.Decimal, &v.Decimal, &value.Decimal)
		return value, err
	default:
		return DNull, pgerror.NewErrorf(pgerror.CodeWindowingError, "non-numerical column provided for windowing with logical offset in RANGE mode")
	}
}

// FrameStartIdx returns the index of starting row in the frame (which is the first to be included).
func (wfr WindowFrameRun) FrameStartIdx(evalCtx *EvalContext) int {
	if wfr.Frame == nil {
		return 0
	}
	switch wfr.Frame.Mode {
	case RANGE:
		switch wfr.Frame.Bounds.StartBound.BoundType {
		case UnboundedPreceding:
			return 0
		case ValuePreceding:
			value, err := wfr.getValueByOffset(-wfr.StartBoundOffset)
			if err != nil {
				panic(fmt.Sprintf("error received: %v", err))
			}
			// We use binary search on [0, wfr.RowIdx) interval to find the first row
			// whose value is greater or equal to 'value'. If such row is not found,
			// then Search will correctly return wfr.RowIdx.
			return sort.Search(wfr.RowIdx, func(i int) bool { return wfr.valueAt(i).Compare(evalCtx, value) >= 0 })
		case CurrentRow:
			// Spec: in RANGE mode CURRENT ROW means that the frame starts with the current row's first peer.
			return wfr.FirstPeerIdx
		case ValueFollowing:
			value, err := wfr.getValueByOffset(wfr.StartBoundOffset)
			if err != nil {
				panic(fmt.Sprintf("error received: %v", err))
			}
			// We use binary search on [wfr.RowIdx, wfr.PartitionSize()) interval to find the first row
			// whose value is greater or equal to 'value'.
			idx := sort.Search(wfr.PartitionSize(), func(i int) bool { return wfr.valueAt(i).Compare(evalCtx, value) >= 0 })
			if idx < wfr.RowIdx {
				// This case is possible when offset == 0.
				// ValueFollowing cannot offset backward.
				idx = wfr.RowIdx
			}
			return idx
		default:
			panic("unexpected WindowFrameBoundType in RANGE mode")
		}
	case ROWS:
		switch wfr.Frame.Bounds.StartBound.BoundType {
		case UnboundedPreceding:
			return 0
		case ValuePreceding:
			idx := wfr.RowIdx - wfr.StartBoundOffset
			if idx < 0 {
				idx = 0
			}
			return idx
		case CurrentRow:
			return wfr.RowIdx
		case ValueFollowing:
			idx := wfr.RowIdx + wfr.StartBoundOffset
			if idx >= wfr.PartitionSize() {
				idx = wfr.unboundedFollowing()
			}
			return idx
		default:
			panic("unexpected WindowFrameBoundType in ROWS mode")
		}
	default:
		panic("unexpected WindowFrameMode")
	}
}

// FrameEndIdx returns the index of the first row after the frame.
func (wfr WindowFrameRun) FrameEndIdx(evalCtx *EvalContext) int {
	if wfr.Frame == nil {
		return wfr.DefaultFrameSize()
	}
	switch wfr.Frame.Mode {
	case RANGE:
		if wfr.Frame.Bounds.EndBound == nil {
			// We're using default value of CURRENT ROW when EndBound is omitted.
			// Spec: in RANGE mode CURRENT ROW means that the frame ends with the current row's last peer.
			return wfr.DefaultFrameSize()
		}
		switch wfr.Frame.Bounds.EndBound.BoundType {
		case ValuePreceding:
			value, err := wfr.getValueByOffset(-wfr.EndBoundOffset)
			if err != nil {
				panic(fmt.Sprintf("error received: %v", err))
			}
			// We use binary search on [0, wfr.RowIdx] interval to find the first row
			// whose value is greater than 'value'. If such row is not found,
			// then Search will correctly return wfr.RowIdx+1.
			return sort.Search(wfr.RowIdx+1, func(i int) bool { return wfr.valueAt(i).Compare(evalCtx, value) > 0 })
		case CurrentRow:
			return wfr.DefaultFrameSize()
		case ValueFollowing:
			value, err := wfr.getValueByOffset(wfr.EndBoundOffset)
			if err != nil {
				panic(fmt.Sprintf("error received: %v", err))
			}
			// We use binary search on [wfr.RowIdx, wfr.PartitionSize()) interval to find the first row
			// whose value is greater than 'value'.
			return sort.Search(wfr.PartitionSize(), func(i int) bool { return wfr.valueAt(i).Compare(evalCtx, value) > 0 })
		case UnboundedFollowing:
			return wfr.unboundedFollowing()
		default:
			panic("unexpected WindowFrameBoundType in RANGE mode")
		}
	case ROWS:
		if wfr.Frame.Bounds.EndBound == nil {
			// We're using default value of CURRENT ROW when EndBound is omitted.
			return wfr.RowIdx + 1
		}
		switch wfr.Frame.Bounds.EndBound.BoundType {
		case ValuePreceding:
			idx := wfr.RowIdx - wfr.EndBoundOffset + 1
			if idx < 0 {
				idx = 0
			}
			return idx
		case CurrentRow:
			return wfr.RowIdx + 1
		case ValueFollowing:
			idx := wfr.RowIdx + wfr.EndBoundOffset + 1
			if idx >= wfr.PartitionSize() {
				idx = wfr.unboundedFollowing()
			}
			return idx
		case UnboundedFollowing:
			return wfr.unboundedFollowing()
		default:
			panic("unexpected WindowFrameBoundType in ROWS mode")
		}
	default:
		panic("unexpected WindowFrameMode")
	}
}

// FrameSize returns the number of rows in the current frame.
func (wfr WindowFrameRun) FrameSize(evalCtx *EvalContext) int {
	if wfr.Frame == nil {
		return wfr.DefaultFrameSize()
	}
	size := wfr.FrameEndIdx(evalCtx) - wfr.FrameStartIdx(evalCtx)
	if size <= 0 {
		size = 0
	}
	return size
}

// Rank returns the rank of the current row.
func (wfr WindowFrameRun) Rank() int {
	return wfr.RowIdx + 1
}

// PartitionSize returns the number of rows in the current partition.
func (wfr WindowFrameRun) PartitionSize() int {
	return len(wfr.Rows)
}

// unboundedFollowing returns the index of the "first row beyond" the partition
// so that current frame contains all the rows till the end of the partition.
func (wfr WindowFrameRun) unboundedFollowing() int {
	return wfr.PartitionSize()
}

// DefaultFrameSize returns the size of default window frame which contains
// the rows from the start of the partition through the last peer of the current row.
func (wfr WindowFrameRun) DefaultFrameSize() int {
	return wfr.FirstPeerIdx + wfr.PeerRowCount
}

// FirstInPeerGroup returns if the current row is the first in its peer group.
func (wfr WindowFrameRun) FirstInPeerGroup() bool {
	return wfr.RowIdx == wfr.FirstPeerIdx
}

// Args returns the current argument set in the window frame.
func (wfr WindowFrameRun) Args() Datums {
	return wfr.ArgsWithRowOffset(0)
}

// ArgsWithRowOffset returns the argument set at the given offset in the window frame.
func (wfr WindowFrameRun) ArgsWithRowOffset(offset int) Datums {
	return wfr.Rows[wfr.RowIdx+offset].Row[wfr.ArgIdxStart : wfr.ArgIdxStart+wfr.ArgCount]
}

// ArgsByRowIdx returns the argument set of the row at idx.
func (wfr WindowFrameRun) ArgsByRowIdx(idx int) Datums {
	return wfr.Rows[idx].Row[wfr.ArgIdxStart : wfr.ArgIdxStart+wfr.ArgCount]
}

// valueAt returns the first value of the row at idx.
func (wfr WindowFrameRun) valueAt(idx int) Datum {
	if len(wfr.Args()) == 0 {
		return DNull
	}
	return wfr.ArgsByRowIdx(idx)[0]
}

// RequiresAscendingOrdering returns whether we need rows to be ordered by the column
// on which windowing takes place. We require this only in RANGE mode with either
// 'value' PRECEDING or 'value' FOLLOWING for at least one of the bounds.
func (wfr WindowFrameRun) RequiresAscendingOrdering() bool {
	return wfr.Frame != nil && wfr.Frame.Mode == RANGE &&
		(wfr.Frame.Bounds.StartBound.OffsetExpr != nil || (wfr.Frame.Bounds.EndBound != nil && wfr.Frame.Bounds.EndBound.OffsetExpr != nil))
}

// WindowFunc performs a computation on each row using data from a provided WindowFrameRun.
type WindowFunc interface {
	// Compute computes the window function for the provided window frame, given the
	// current state of WindowFunc. The method should be called sequentially for every
	// row in a partition in turn with the desired ordering of the WindowFunc. This is
	// because there is an implicit carried dependency between each row and all those
	// that have come before it (like in an AggregateFunc). As such, this approach does
	// not present any exploitable associativity/commutativity for optimization.
	Compute(context.Context, *EvalContext, *WindowFrameRun) (Datum, error)

	// Close allows the window function to free any memory it requested during execution,
	// such as during the execution of an aggregation like CONCAT_AGG or ARRAY_AGG.
	Close(context.Context, *EvalContext)
}
