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
)

// IndexedRow is a row with a corresponding index.
type IndexedRow struct {
	Idx int
	Row Datums
}

// WindowFrameMode indicates which mode of framing is used.
type WindowFrameMode int

const (
	// RANGE is the mode of specifying frame in terms of logical range (e.g. 100 units cheaper).
	RANGE WindowFrameMode = iota
	// ROWS is the mode of specifying frame in terms of physical offsets (e.g. 1 row before etc).
	ROWS
)

// WindowFrameBoundType indicates which type of boundary is used.
type WindowFrameBoundType int

const (
	// UnboundedPreceding represents UNBOUNDED PRECEDING type of boundary.
	UnboundedPreceding WindowFrameBoundType = iota
	// ValuePreceding represents 'value' PRECEDING type of boundary.
	ValuePreceding
	// CurrentRow represents CURRENT ROW type of boundary.
	CurrentRow
	// ValueFollowing represents 'value' FOLLOWING type of boundary.
	ValueFollowing
	// UnboundedFollowing represents UNBOUNDED FOLLOWING type of boundary.
	UnboundedFollowing
)

// WindowFrameBound specifies the offset and the type of boundary.
type WindowFrameBound struct {
	BoundType  WindowFrameBoundType
	Offset     int
	OffsetExpr Expr
}

// WindowFrameBounds specifies boundaries of the window frame.
// The row at StartBound is included whereas the row at EndBound is not.
type WindowFrameBounds struct {
	StartBound *WindowFrameBound
	EndBound   *WindowFrameBound
}

// WindowFrame is a view into a subset of data over which calculations are made.
type WindowFrame struct {
	// constant for all calls to WindowFunc.Add
	Rows        []IndexedRow
	ArgIdxStart int                // the index which arguments to the window function begin
	ArgCount    int                // the number of window function arguments
	Mode        WindowFrameMode    // the mode of framing being used
	Bounds      *WindowFrameBounds // the bounds of the frame

	// changes for each row (each call to WindowFunc.Add)
	RowIdx int // the current row index

	// changes for each peer group
	FirstPeerIdx int // the first index in the current peer group
	PeerRowCount int // the number of rows in the current peer group
}

func (boundary *WindowFrameBound) write(ctx *FmtCtx) {
	switch boundary.BoundType {
	case UnboundedPreceding:
		ctx.WriteString("UNBOUNDED PRECEDING")
	case ValuePreceding:
		ctx.WriteString(fmt.Sprintf("%v PRECEDING", boundary.OffsetExpr))
	case CurrentRow:
		ctx.WriteString("CURRENT ROW")
	case ValueFollowing:
		ctx.WriteString(fmt.Sprintf("%v FOLLOWING", boundary.OffsetExpr))
	case UnboundedFollowing:
		ctx.WriteString("UNBOUNDED FOLLOWING")
	}
}

// Format implements the NodeFormatter interface.
func (wf *WindowFrame) Format(ctx *FmtCtx) {
	switch wf.Mode {
	case RANGE:
		ctx.WriteString("RANGE ")
	case ROWS:
		ctx.WriteString("ROWS ")
	}
	if wf.Bounds.EndBound != nil {
		ctx.WriteString("BETWEEN ")
		wf.Bounds.StartBound.write(ctx)
		ctx.WriteString(" AND ")
		wf.Bounds.EndBound.write(ctx)
	} else {
		wf.Bounds.StartBound.write(ctx)
	}
}

// Copy returns a deep copy of wf.
func (wf *WindowFrame) Copy() *WindowFrame {
	frameCopy := *wf
	if wf.Bounds != nil {
		startBoundCopy := *wf.Bounds.StartBound
		frameCopy.Bounds = &WindowFrameBounds{&startBoundCopy, nil}
		if wf.Bounds.EndBound != nil {
			endBoundCopy := *wf.Bounds.EndBound
			frameCopy.Bounds.EndBound = &endBoundCopy
		}
	}
	return &frameCopy
}

var _ NodeFormatter = &WindowFrame{}

// Rank returns the rank of the current row.
func (wf WindowFrame) Rank() int {
	return wf.RowIdx + 1
}

// PartitionSize returns the number of rows in the current partition.
func (wf WindowFrame) PartitionSize() int {
	return len(wf.Rows)
}

func (wf WindowFrame) unboundedFollowing() int {
	return wf.PartitionSize()
}

// FrameStartIdx returns the index of starting row in the frame (which is the first to be included).
func (wf WindowFrame) FrameStartIdx() int {
	if wf.Bounds == nil {
		return 0
	}
	//fmt.Printf("in window_funcs.go: getting start idx on %+v\n", wf.Bounds.StartBound)
	switch wf.Mode {
	case RANGE:
		switch wf.Bounds.StartBound.BoundType {
		case UnboundedPreceding:
			return 0
		case ValuePreceding:
			// TODO(yuzefovich): Currently, it is not supported, and this case should not be reached.
			return 0
		case CurrentRow:
			// Spec: in RANGE mode CURRENT ROW means that the frame starts with the current row's first peer.
			return wf.FirstPeerIdx
		case ValueFollowing:
			// TODO(yuzefovich): Currently, it is not supported, and this case should not be reached.
			return 0
		default:
			// It should never be reached.
			return 0
		}
	case ROWS:
		switch wf.Bounds.StartBound.BoundType {
		case UnboundedPreceding:
			return 0
		case ValuePreceding:
			idx := wf.RowIdx - wf.Bounds.StartBound.Offset
			if idx < 0 {
				idx = 0
			}
			return idx
		case CurrentRow:
			return wf.RowIdx
		case ValueFollowing:
			idx := wf.RowIdx + wf.Bounds.StartBound.Offset
			if idx >= wf.PartitionSize() {
				idx = wf.unboundedFollowing()
			}
			return idx
		default:
			// It should never be reached.
			return 0
		}
	default:
		// It should never be reached.
		return 0
	}
}

// FrameEndIdx returns the index of the first row after the frame.
func (wf WindowFrame) FrameEndIdx() int {
	if wf.Bounds == nil {
		return wf.DefaultFrameSize()
	}
	switch wf.Mode {
	case RANGE:
		if wf.Bounds.EndBound == nil {
			// We're using default value of CURRENT ROW when EndBound is omitted.
			// Spec: in RANGE mode CURRENT ROW means that the frame ends with the current row's last peer.
			return wf.DefaultFrameSize()
		}
		switch wf.Bounds.EndBound.BoundType {
		case ValuePreceding:
			// TODO(yuzefovich): Currently, it is not supported, and this case should not be reached.
			return wf.DefaultFrameSize()
		case CurrentRow:
			return wf.DefaultFrameSize()
		case ValueFollowing:
			// TODO(yuzefovich): Currently, it is not supported, and this case should not be reached.
			return wf.DefaultFrameSize()
		case UnboundedFollowing:
			return wf.unboundedFollowing()
		default:
			// It should never be reached.
			return wf.DefaultFrameSize()
		}
	case ROWS:
		if wf.Bounds.EndBound == nil {
			// We're using default value of CURRENT ROW when EndBound is omitted.
			return wf.RowIdx + 1
		}
		switch wf.Bounds.EndBound.BoundType {
		case ValuePreceding:
			idx := wf.RowIdx - wf.Bounds.EndBound.Offset + 1
			if idx < 0 {
				idx = 0
			}
			return idx
		case CurrentRow:
			return wf.RowIdx + 1
		case ValueFollowing:
			idx := wf.RowIdx + wf.Bounds.EndBound.Offset + 1
			if idx >= wf.PartitionSize() {
				idx = wf.unboundedFollowing()
			}
			return idx
		case UnboundedFollowing:
			return wf.unboundedFollowing()
		default:
			// It should never be reached.
			return wf.RowIdx + 1
		}
	default:
		// It should never be reached.
		return wf.DefaultFrameSize()
	}
}

// FrameSize returns the number of rows in the current frame.
func (wf WindowFrame) FrameSize() int {
	if wf.Bounds == nil {
		return wf.DefaultFrameSize()
	}
	size := wf.FrameEndIdx() - wf.FrameStartIdx()
	if size <= 0 {
		size = 0
	}
	return size
}

// DefaultFrameSize returns the size of default window frame which contains
// the rows from the start of the partition through the last peer of the current row.
func (wf WindowFrame) DefaultFrameSize() int {
	return wf.FirstPeerIdx + wf.PeerRowCount
}

// FirstInPeerGroup returns if the current row is the first in its peer group.
func (wf WindowFrame) FirstInPeerGroup() bool {
	return wf.RowIdx == wf.FirstPeerIdx
}

// Args returns the current argument set in the window frame.
func (wf WindowFrame) Args() Datums {
	return wf.ArgsWithRowOffset(0)
}

// ArgsWithRowOffset returns the argument set at the given offset in the window frame.
func (wf WindowFrame) ArgsWithRowOffset(offset int) Datums {
	return wf.Rows[wf.RowIdx+offset].Row[wf.ArgIdxStart : wf.ArgIdxStart+wf.ArgCount]
}

// ArgsByRowIdx returns the argument set of the row at idx.
func (wf WindowFrame) ArgsByRowIdx(idx int) Datums {
	return wf.Rows[idx].Row[wf.ArgIdxStart : wf.ArgIdxStart+wf.ArgCount]
}

// WindowFunc performs a computation on each row using data from a provided WindowFrame.
type WindowFunc interface {
	// Compute computes the window function for the provided window frame, given the
	// current state of WindowFunc. The method should be called sequentially for every
	// row in a partition in turn with the desired ordering of the WindowFunc. This is
	// because there is an implicit carried dependency between each row and all those
	// that have come before it (like in an AggregateFunc). As such, this approach does
	// not present any exploitable associativity/commutativity for optimization.
	Compute(context.Context, *EvalContext, WindowFrame) (Datum, error)

	// Close allows the window function to free any memory it requested during execution,
	// such as during the execution of an aggregation like CONCAT_AGG or ARRAY_AGG.
	Close(context.Context, *EvalContext)
}
