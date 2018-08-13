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
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
)

// IndexedRows are rows with the corresponding indices.
type IndexedRows interface {
	Len() int                  // returns number of rows
	GetRow(idx int) IndexedRow // returns a row at the given index
}

// IndexedRow is a row with a corresponding index.
type IndexedRow interface {
	GetIdx() int                           // returns index of the row
	GetDatum(idx int) Datum                // returns a datum at the given index
	GetDatums(startIdx, endIdx int) Datums // returns datums at indices [startIdx, endIdx)
}

// WindowFrameRun contains the runtime state of window frame during calculations.
type WindowFrameRun struct {
	// constant for all calls to WindowFunc.Add
	Rows             IndexedRows
	ArgIdxStart      int          // the index which arguments to the window function begin
	ArgCount         int          // the number of window function arguments
	Frame            *WindowFrame // If non-nil, Frame represents the frame specification of this window. If nil, default frame is used.
	StartBoundOffset Datum
	EndBoundOffset   Datum
	FilterColIdx     int
	OrdColIdx        int                // Column over which rows are ordered within the partition. It is only required in RANGE mode.
	OrdDirection     encoding.Direction // Direction of the ordering over OrdColIdx.

	// changes for each row (each call to WindowFunc.Add)
	RowIdx int // the current row index

	// changes for each peer group
	FirstPeerIdx int // the first index in the current peer group
	PeerRowCount int // the number of rows in the current peer group
}

// getValueByOffset returns a datum calculated as the value of the current row
// in the column over which rows are ordered plus/minus logic offset, and an
// error if encountered. It should be used only in RANGE mode.
func (wfr WindowFrameRun) getValueByOffset(offset Datum, negative bool) (Datum, error) {
	if wfr.OrdDirection == encoding.Descending {
		// If rows are in descending order, we want to perform the "opposite"
		// addition/subtraction to default ascending order.
		negative = !negative
	}
	var ok bool
	switch v := wfr.valueAt(wfr.RowIdx).(type) {
	case *DInt:
		o := MustBeDInt(offset)
		if negative {
			return NewDInt(*v - o), nil
		}
		return NewDInt(*v + o), nil
	case *DFloat:
		var o *DFloat
		if o, ok = offset.(*DFloat); !ok {
			return DNull, pgerror.NewErrorf(pgerror.CodeWindowingError, "expected FLOAT offset in RANGE mode")
		}
		if negative {
			return NewDFloat(*v - *o), nil
		}
		return NewDFloat(*v + *o), nil
	case *DDecimal:
		var o *DDecimal
		if o, ok = offset.(*DDecimal); !ok {
			return DNull, pgerror.NewErrorf(pgerror.CodeWindowingError, "expected DECIMAL offset in RANGE mode")
		}
		value := &DDecimal{}
		var err error
		if negative {
			_, err = ExactCtx.Sub(&value.Decimal, &v.Decimal, &o.Decimal)
		} else {
			_, err = ExactCtx.Add(&value.Decimal, &v.Decimal, &o.Decimal)
		}
		return value, err
	case *DDate:
		var o *DInterval
		if o, ok = offset.(*DInterval); !ok {
			return DNull, pgerror.NewErrorf(pgerror.CodeWindowingError, "expected INTERVAL offset in RANGE mode")
		}
		if negative {
			return NewDDate(*v - DDate(o.Days)), nil
		}
		return NewDDate(*v + DDate(o.Days)), nil
	case *DTime:
		var o *DInterval
		if o, ok = offset.(*DInterval); !ok {
			return DNull, pgerror.NewErrorf(pgerror.CodeWindowingError, "expected INTERVAL offset in RANGE mode")
		}
		t := timeofday.TimeOfDay(*v)
		if negative {
			return MakeDTime(t.Add(o.Duration.Mul(-1))), nil
		}
		return MakeDTime(t.Add(o.Duration)), nil
	case *DTimestamp:
		var o *DInterval
		if o, ok = offset.(*DInterval); !ok {
			return DNull, pgerror.NewErrorf(pgerror.CodeWindowingError, "expected INTERVAL offset in RANGE mode")
		}
		if negative {
			return MakeDTimestamp(duration.Add(v.Time, o.Duration.Mul(-1)), time.Microsecond), nil
		}
		return MakeDTimestamp(duration.Add(v.Time, o.Duration), time.Microsecond), nil
	case *DTimestampTZ:
		var o *DInterval
		if o, ok = offset.(*DInterval); !ok {
			return DNull, pgerror.NewErrorf(pgerror.CodeWindowingError, "expected INTERVAL offset in RANGE mode")
		}
		if negative {
			return MakeDTimestampTZ(duration.Add(v.Time, o.Duration.Mul(-1)), time.Microsecond), nil
		}
		return MakeDTimestampTZ(duration.Add(v.Time, o.Duration), time.Microsecond), nil
	case *DInterval:
		var o *DInterval
		if o, ok = offset.(*DInterval); !ok {
			return DNull, pgerror.NewErrorf(pgerror.CodeWindowingError, "expected INTERVAL offset in RANGE mode")
		}
		if negative {
			return &DInterval{Duration: v.Duration.Sub(o.Duration)}, nil
		}
		return &DInterval{Duration: v.Duration.Add(o.Duration)}, nil
	default:
		return DNull, pgerror.NewErrorf(pgerror.CodeWindowingError, "given logical offset cannot be combined with ordering column")
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
		case OffsetPreceding:
			value, err := wfr.getValueByOffset(wfr.StartBoundOffset, true)
			if err != nil {
				panic(fmt.Sprintf("error received: %v", err))
			}
			if wfr.OrdDirection == encoding.Descending {
				// We use binary search on [0, wfr.RowIdx) interval to find the first row
				// whose value is smaller or equal to 'value'. If such row is not found,
				// then Search will correctly return wfr.RowIdx.
				return sort.Search(wfr.RowIdx, func(i int) bool { return wfr.valueAt(i).Compare(evalCtx, value) <= 0 })
			}
			// We use binary search on [0, wfr.RowIdx) interval to find the first row
			// whose value is greater or equal to 'value'. If such row is not found,
			// then Search will correctly return wfr.RowIdx.
			return sort.Search(wfr.RowIdx, func(i int) bool { return wfr.valueAt(i).Compare(evalCtx, value) >= 0 })
		case CurrentRow:
			// Spec: in RANGE mode CURRENT ROW means that the frame starts with the current row's first peer.
			return wfr.FirstPeerIdx
		case OffsetFollowing:
			value, err := wfr.getValueByOffset(wfr.StartBoundOffset, false)
			if err != nil {
				panic(fmt.Sprintf("error received: %v", err))
			}
			if wfr.OrdDirection == encoding.Descending {
				// We use binary search on [wfr.RowIdx, wfr.PartitionSize()) interval
				// to find the first row whose value is smaller or equal to 'value'.
				return wfr.RowIdx + sort.Search(wfr.PartitionSize()-wfr.RowIdx, func(i int) bool { return wfr.valueAt(i+wfr.RowIdx).Compare(evalCtx, value) <= 0 })
			}
			// We use binary search on [wfr.RowIdx, wfr.PartitionSize()) interval
			// to find the first row whose value is greater or equal to 'value'.
			return wfr.RowIdx + sort.Search(wfr.PartitionSize()-wfr.RowIdx, func(i int) bool { return wfr.valueAt(i+wfr.RowIdx).Compare(evalCtx, value) >= 0 })
		default:
			panic("unexpected WindowFrameBoundType in RANGE mode")
		}
	case ROWS:
		switch wfr.Frame.Bounds.StartBound.BoundType {
		case UnboundedPreceding:
			return 0
		case OffsetPreceding:
			offset := MustBeDInt(wfr.StartBoundOffset)
			idx := wfr.RowIdx - int(offset)
			if idx < 0 {
				idx = 0
			}
			return idx
		case CurrentRow:
			return wfr.RowIdx
		case OffsetFollowing:
			offset := MustBeDInt(wfr.StartBoundOffset)
			idx := wfr.RowIdx + int(offset)
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

// IsDefaultFrame returns whether a frame equivalent to the default frame
// is being used (default is RANGE UNBOUNDED PRECEDING).
func (wfr WindowFrameRun) IsDefaultFrame() bool {
	if wfr.Frame == nil {
		return true
	}
	if wfr.Frame.Bounds.StartBound.BoundType == UnboundedPreceding {
		return wfr.Frame.Bounds.EndBound == nil || wfr.Frame.Bounds.EndBound.BoundType == CurrentRow
	}
	return false
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
		case OffsetPreceding:
			value, err := wfr.getValueByOffset(wfr.EndBoundOffset, true)
			if err != nil {
				panic(fmt.Sprintf("error received: %v", err))
			}
			if wfr.OrdDirection == encoding.Descending {
				// We use binary search on [0, wfr.RowIdx] interval to find the first row
				// whose value is smaller than 'value'. If such row is not found,
				// then Search will correctly return wfr.RowIdx+1.
				return sort.Search(wfr.RowIdx+1, func(i int) bool { return wfr.valueAt(i).Compare(evalCtx, value) < 0 })
			}
			// We use binary search on [0, wfr.RowIdx] interval to find the first row
			// whose value is greater than 'value'. If such row is not found,
			// then Search will correctly return wfr.RowIdx+1.
			return sort.Search(wfr.RowIdx+1, func(i int) bool { return wfr.valueAt(i).Compare(evalCtx, value) > 0 })
		case CurrentRow:
			// Spec: in RANGE mode CURRENT ROW means that the frame end with the current row's last peer.
			return wfr.FirstPeerIdx + wfr.PeerRowCount
		case OffsetFollowing:
			value, err := wfr.getValueByOffset(wfr.EndBoundOffset, false)
			if err != nil {
				panic(fmt.Sprintf("error received: %v", err))
			}
			if wfr.OrdDirection == encoding.Descending {
				// We use binary search on [wfr.RowIdx, wfr.PartitionSize()) interval
				// to find the first row whose value is smaller than 'value'.
				return wfr.RowIdx + sort.Search(wfr.PartitionSize()-wfr.RowIdx, func(i int) bool { return wfr.valueAt(i+wfr.RowIdx).Compare(evalCtx, value) < 0 })
			}
			// We use binary search on [wfr.RowIdx, wfr.PartitionSize()) interval
			// to find the first row whose value is greater than 'value'.
			return wfr.RowIdx + sort.Search(wfr.PartitionSize()-wfr.RowIdx, func(i int) bool { return wfr.valueAt(i+wfr.RowIdx).Compare(evalCtx, value) > 0 })
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
		case OffsetPreceding:
			offset := MustBeDInt(wfr.EndBoundOffset)
			idx := wfr.RowIdx - int(offset) + 1
			if idx < 0 {
				idx = 0
			}
			return idx
		case CurrentRow:
			return wfr.RowIdx + 1
		case OffsetFollowing:
			offset := MustBeDInt(wfr.EndBoundOffset)
			idx := wfr.RowIdx + int(offset) + 1
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
	return wfr.Rows.Len()
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
	return wfr.Rows.GetRow(wfr.RowIdx+offset).GetDatums(wfr.ArgIdxStart, wfr.ArgIdxStart+wfr.ArgCount)
}

// ArgsByRowIdx returns the argument set of the row at idx.
func (wfr WindowFrameRun) ArgsByRowIdx(idx int) Datums {
	return wfr.Rows.GetRow(idx).GetDatums(wfr.ArgIdxStart, wfr.ArgIdxStart+wfr.ArgCount)
}

// valueAt returns the first argument of the window function at the row idx.
func (wfr WindowFrameRun) valueAt(idx int) Datum {
	return wfr.Rows.GetRow(idx).GetDatum(wfr.OrdColIdx)
}

// RequiresOrdering returns whether we require ordering on a single column. We
// do only in RANGE mode with 'value' PRECEDING/FOLLOWING types of bounds.
func (wfr WindowFrameRun) RequiresOrdering() bool {
	return wfr.Frame.Mode == RANGE &&
		((wfr.Frame.Bounds.StartBound.BoundType == OffsetPreceding ||
			wfr.Frame.Bounds.StartBound.BoundType == OffsetFollowing) ||
			(wfr.Frame.Bounds.EndBound != nil && (wfr.Frame.Bounds.EndBound.BoundType == OffsetPreceding ||
				wfr.Frame.Bounds.EndBound.BoundType == OffsetFollowing)))
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
