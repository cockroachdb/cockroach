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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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
	PlusOp, MinusOp  *BinOp             // Binary operators for addition and subtraction required only in RANGE mode.
	PeerHelper       PeerGroupsIndicesHelper

	// changes for each peer group
	CurRowPeerGroupNum int // the number of the current row's peer group

	// changes for each row (each call to WindowFunc.Add)
	RowIdx int // the current row index
}

// WindowFrameRangeOps allows for looking up an implementation of binary
// operators necessary for RANGE mode of framing.
type WindowFrameRangeOps struct{}

// LookupImpl looks up implementation of Plus and Minus binary operators for
// provided left and right types and returns them along with a boolean which
// indicates whether lookup is successful.
func (o WindowFrameRangeOps) LookupImpl(left, right types.T) (*BinOp, *BinOp, bool) {
	plusOverloads, minusOverloads := BinOps[Plus], BinOps[Minus]
	plusOp, found := plusOverloads.lookupImpl(left, right)
	if !found {
		return nil, nil, false
	}
	minusOp, found := minusOverloads.lookupImpl(left, right)
	if !found {
		return nil, nil, false
	}
	return plusOp, minusOp, true
}

// getValueByOffset returns a datum calculated as the value of the current row
// in the column over which rows are ordered plus/minus logic offset, and an
// error if encountered. It should be used only in RANGE mode.
func (wfr *WindowFrameRun) getValueByOffset(
	evalCtx *EvalContext, offset Datum, negative bool,
) (Datum, error) {
	if wfr.OrdDirection == encoding.Descending {
		// If rows are in descending order, we want to perform the "opposite"
		// addition/subtraction to default ascending order.
		negative = !negative
	}
	var binOp *BinOp
	if negative {
		binOp = wfr.MinusOp
	} else {
		binOp = wfr.PlusOp
	}
	return binOp.Fn(evalCtx, wfr.valueAt(wfr.RowIdx), offset)
}

// FrameStartIdx returns the index of starting row in the frame (which is the first to be included).
func (wfr *WindowFrameRun) FrameStartIdx(evalCtx *EvalContext) int {
	if wfr.Frame == nil {
		return 0
	}
	switch wfr.Frame.Mode {
	case RANGE:
		switch wfr.Frame.Bounds.StartBound.BoundType {
		case UnboundedPreceding:
			return 0
		case OffsetPreceding:
			value, err := wfr.getValueByOffset(evalCtx, wfr.StartBoundOffset, true /* negative */)
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
			return wfr.PeerHelper.GetFirstPeerIdx(wfr.CurRowPeerGroupNum)
		case OffsetFollowing:
			value, err := wfr.getValueByOffset(evalCtx, wfr.StartBoundOffset, false /* negative */)
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
	case GROUPS:
		switch wfr.Frame.Bounds.StartBound.BoundType {
		case UnboundedPreceding:
			return 0
		case OffsetPreceding:
			offset := MustBeDInt(wfr.StartBoundOffset)
			peerGroupNum := wfr.CurRowPeerGroupNum - int(offset)
			if peerGroupNum < 0 {
				peerGroupNum = 0
			}
			return wfr.PeerHelper.GetFirstPeerIdx(peerGroupNum)
		case CurrentRow:
			// Spec: in GROUPS mode CURRENT ROW means that the frame starts with the current row's first peer.
			return wfr.PeerHelper.GetFirstPeerIdx(wfr.CurRowPeerGroupNum)
		case OffsetFollowing:
			offset := MustBeDInt(wfr.StartBoundOffset)
			peerGroupNum := wfr.CurRowPeerGroupNum + int(offset)
			lastPeerGroupNum := wfr.PeerHelper.GetLastPeerGroupNum()
			if peerGroupNum > lastPeerGroupNum {
				// peerGroupNum is out of bounds, so we return the index of the first
				// row after the partition.
				return wfr.unboundedFollowing()
			}
			return wfr.PeerHelper.GetFirstPeerIdx(peerGroupNum)
		default:
			panic("unexpected WindowFrameBoundType in GROUPS mode")
		}
	default:
		panic("unexpected WindowFrameMode")
	}
}

// IsDefaultFrame returns whether a frame equivalent to the default frame
// is being used (default is RANGE UNBOUNDED PRECEDING).
func (wfr *WindowFrameRun) IsDefaultFrame() bool {
	if wfr.Frame == nil {
		return true
	}
	if wfr.Frame.Bounds.StartBound.BoundType == UnboundedPreceding {
		return wfr.Frame.Bounds.EndBound == nil || wfr.Frame.Bounds.EndBound.BoundType == CurrentRow
	}
	return false
}

// FrameEndIdx returns the index of the first row after the frame.
func (wfr *WindowFrameRun) FrameEndIdx(evalCtx *EvalContext) int {
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
			value, err := wfr.getValueByOffset(evalCtx, wfr.EndBoundOffset, true /* negative */)
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
			return wfr.DefaultFrameSize()
		case OffsetFollowing:
			value, err := wfr.getValueByOffset(evalCtx, wfr.EndBoundOffset, false /* negative */)
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
	case GROUPS:
		if wfr.Frame.Bounds.EndBound == nil {
			// We're using default value of CURRENT ROW when EndBound is omitted.
			// Spec: in GROUPS mode CURRENT ROW means that the frame ends with the current row's last peer.
			return wfr.DefaultFrameSize()
		}
		switch wfr.Frame.Bounds.EndBound.BoundType {
		case OffsetPreceding:
			offset := MustBeDInt(wfr.EndBoundOffset)
			peerGroupNum := wfr.CurRowPeerGroupNum - int(offset)
			if peerGroupNum < 0 {
				// EndBound's peer group is "outside" of the partition.
				return 0
			}
			return wfr.PeerHelper.GetFirstPeerIdx(peerGroupNum) + wfr.PeerHelper.GetRowCount(peerGroupNum)
		case CurrentRow:
			return wfr.DefaultFrameSize()
		case OffsetFollowing:
			offset := MustBeDInt(wfr.EndBoundOffset)
			peerGroupNum := wfr.CurRowPeerGroupNum + int(offset)
			lastPeerGroupNum := wfr.PeerHelper.GetLastPeerGroupNum()
			if peerGroupNum > lastPeerGroupNum {
				// peerGroupNum is out of bounds, so we return the index of the first
				// row after the partition.
				return wfr.unboundedFollowing()
			}
			return wfr.PeerHelper.GetFirstPeerIdx(peerGroupNum) + wfr.PeerHelper.GetRowCount(peerGroupNum)
		case UnboundedFollowing:
			return wfr.unboundedFollowing()
		default:
			panic("unexpected WindowFrameBoundType in GROUPS mode")
		}
	default:
		panic("unexpected WindowFrameMode")
	}
}

// FrameSize returns the number of rows in the current frame.
func (wfr *WindowFrameRun) FrameSize(evalCtx *EvalContext) int {
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
func (wfr *WindowFrameRun) Rank() int {
	return wfr.RowIdx + 1
}

// PartitionSize returns the number of rows in the current partition.
func (wfr *WindowFrameRun) PartitionSize() int {
	return wfr.Rows.Len()
}

// unboundedFollowing returns the index of the "first row beyond" the partition
// so that current frame contains all the rows till the end of the partition.
func (wfr *WindowFrameRun) unboundedFollowing() int {
	return wfr.PartitionSize()
}

// DefaultFrameSize returns the size of default window frame which contains
// the rows from the start of the partition through the last peer of the current row.
func (wfr *WindowFrameRun) DefaultFrameSize() int {
	return wfr.PeerHelper.GetFirstPeerIdx(wfr.CurRowPeerGroupNum) + wfr.PeerHelper.GetRowCount(wfr.CurRowPeerGroupNum)
}

// FirstInPeerGroup returns if the current row is the first in its peer group.
func (wfr *WindowFrameRun) FirstInPeerGroup() bool {
	return wfr.RowIdx == wfr.PeerHelper.GetFirstPeerIdx(wfr.CurRowPeerGroupNum)
}

// Args returns the current argument set in the window frame.
func (wfr *WindowFrameRun) Args() Datums {
	return wfr.ArgsWithRowOffset(0)
}

// ArgsWithRowOffset returns the argument set at the given offset in the window frame.
func (wfr *WindowFrameRun) ArgsWithRowOffset(offset int) Datums {
	return wfr.Rows.GetRow(wfr.RowIdx+offset).GetDatums(wfr.ArgIdxStart, wfr.ArgIdxStart+wfr.ArgCount)
}

// ArgsByRowIdx returns the argument set of the row at idx.
func (wfr *WindowFrameRun) ArgsByRowIdx(idx int) Datums {
	return wfr.Rows.GetRow(idx).GetDatums(wfr.ArgIdxStart, wfr.ArgIdxStart+wfr.ArgCount)
}

// valueAt returns the first argument of the window function at the row idx.
func (wfr *WindowFrameRun) valueAt(idx int) Datum {
	return wfr.Rows.GetRow(idx).GetDatum(wfr.OrdColIdx)
}

// RangeModeWithOffsets returns whether the frame is in RANGE mode with at least
// one of the bounds containing an offset.
func (wfr *WindowFrameRun) RangeModeWithOffsets() bool {
	return wfr.Frame.Mode == RANGE && wfr.Frame.Bounds.HasOffset()
}

// WindowFunc performs a computation on each row using data from a provided *WindowFrameRun.
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
