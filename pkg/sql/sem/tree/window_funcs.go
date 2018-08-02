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

// PeerGroup represents a single peer group.
type PeerGroup struct {
	StartIdx int
	RowCount int
}

// PeerGroupsInitialSize defines initial size of PeerGroups slice.
const PeerGroupsInitialSize = 8

// WindowFrameRun contains the runtime state of window frame during calculations.
type WindowFrameRun struct {
	// constant for all calls to WindowFunc.Add
	Rows                 IndexedRows
	ArgIdxStart          int          // the index which arguments to the window function begin
	ArgCount             int          // the number of window function arguments
	Frame                *WindowFrame // If non-nil, Frame represents the frame specification of this window. If nil, default frame is used.
	StartBoundOffset     Datum
	EndBoundOffset       Datum
	PeerGroupNumByRowIdx []int        // indicates the number of the peer group a row belongs to, counting from 0
	PeerGroups           []*PeerGroup // all peer groups in the partition

	// changes for each row (each call to WindowFunc.Add)
	RowIdx int // the current row index
}

// FrameStartIdx returns the index of starting row in the frame (which is the first to be included).
func (wfr WindowFrameRun) FrameStartIdx() int {
	if wfr.Frame == nil {
		return 0
	}
	switch wfr.Frame.Mode {
	case RANGE:
		switch wfr.Frame.Bounds.StartBound.BoundType {
		case UnboundedPreceding:
			return 0
		case ValuePreceding:
			// TODO(yuzefovich): Currently, it is not supported, and this case should not be reached.
			panic("unsupported WindowFrameBoundType in RANGE mode")
		case CurrentRow:
			// Spec: in RANGE mode CURRENT ROW means that the frame starts with the current row's first peer.
			return wfr.PeerGroups[wfr.PeerGroupNumByRowIdx[wfr.RowIdx]].StartIdx
		case ValueFollowing:
			// TODO(yuzefovich): Currently, it is not supported, and this case should not be reached.
			panic("unsupported WindowFrameBoundType in RANGE mode")
		default:
			panic("unexpected WindowFrameBoundType in RANGE mode")
		}
	case ROWS:
		switch wfr.Frame.Bounds.StartBound.BoundType {
		case UnboundedPreceding:
			return 0
		case ValuePreceding:
			offset := MustBeDInt(wfr.StartBoundOffset)
			idx := wfr.RowIdx - int(offset)
			if idx < 0 {
				idx = 0
			}
			return idx
		case CurrentRow:
			return wfr.RowIdx
		case ValueFollowing:
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
		case ValuePreceding:
			offset := MustBeDInt(wfr.StartBoundOffset)
			peerGroupNum := wfr.PeerGroupNumByRowIdx[wfr.RowIdx] - int(offset)
			if peerGroupNum < 0 {
				peerGroupNum = 0
			}
			return wfr.PeerGroups[peerGroupNum].StartIdx
		case CurrentRow:
			// Spec: in GROUPS mode CURRENT ROW means that the frame starts with the current row's first peer.
			return wfr.PeerGroups[wfr.PeerGroupNumByRowIdx[wfr.RowIdx]].StartIdx
		case ValueFollowing:
			offset := MustBeDInt(wfr.StartBoundOffset)
			peerGroupNum := wfr.PeerGroupNumByRowIdx[wfr.RowIdx] + int(offset)
			lastPeerGroupNum := wfr.PeerGroupNumByRowIdx[wfr.PartitionSize()-1]
			if peerGroupNum > lastPeerGroupNum {
				// peerGroupNum is out of bounds, so we return the index of the first
				// row after the partition.
				return wfr.unboundedFollowing()
			}
			return wfr.PeerGroups[peerGroupNum].StartIdx
		default:
			panic("unexpected WindowFrameBoundType in GROUPS mode")
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
		return !wfr.NonDefaultFrameExclusion() &&
			(wfr.Frame.Bounds.EndBound == nil || wfr.Frame.Bounds.EndBound.BoundType == CurrentRow)
	}
	return false
}

// FrameEndIdx returns the index of the first row after the frame.
func (wfr WindowFrameRun) FrameEndIdx() int {
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
			// TODO(yuzefovich): Currently, it is not supported, and this case should not be reached.
			panic("unsupported WindowFrameBoundType in RANGE mode")
		case CurrentRow:
			return wfr.DefaultFrameSize()
		case ValueFollowing:
			// TODO(yuzefovich): Currently, it is not supported, and this case should not be reached.
			panic("unsupported WindowFrameBoundType in RANGE mode")
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
			offset := MustBeDInt(wfr.EndBoundOffset)
			idx := wfr.RowIdx - int(offset) + 1
			if idx < 0 {
				idx = 0
			}
			return idx
		case CurrentRow:
			return wfr.RowIdx + 1
		case ValueFollowing:
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
		case ValuePreceding:
			offset := MustBeDInt(wfr.EndBoundOffset)
			peerGroupNum := wfr.PeerGroupNumByRowIdx[wfr.RowIdx] - int(offset)
			if peerGroupNum < 0 {
				// EndBound's peer group is "outside" of the partition.
				return 0
			}
			return wfr.PeerGroups[peerGroupNum].StartIdx + wfr.PeerGroups[peerGroupNum].RowCount
		case CurrentRow:
			return wfr.DefaultFrameSize()
		case ValueFollowing:
			offset := MustBeDInt(wfr.EndBoundOffset)
			peerGroupNum := wfr.PeerGroupNumByRowIdx[wfr.RowIdx] + int(offset)
			lastPeerGroupNum := wfr.PeerGroupNumByRowIdx[wfr.PartitionSize()-1]
			if peerGroupNum > lastPeerGroupNum {
				// peerGroupNum is out of bounds, so we return the index of the first
				// row after the partition.
				return wfr.unboundedFollowing()
			}
			return wfr.PeerGroups[peerGroupNum].StartIdx + wfr.PeerGroups[peerGroupNum].RowCount
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
func (wfr WindowFrameRun) FrameSize() int {
	if wfr.Frame == nil {
		return wfr.DefaultFrameSize()
	}
	if wfr.NonDefaultFrameExclusion() {
		size := 0
		for i := wfr.FrameStartIdx(); i < wfr.FrameEndIdx(); i++ {
			if !wfr.IsRowExcluded(i) {
				size++
			}
		}
		return size
	}
	size := wfr.FrameEndIdx() - wfr.FrameStartIdx()
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
	peerGroup := wfr.PeerGroups[wfr.PeerGroupNumByRowIdx[wfr.RowIdx]]
	return peerGroup.StartIdx + peerGroup.RowCount
}

// FirstInPeerGroup returns if the current row is the first in its peer group.
func (wfr WindowFrameRun) FirstInPeerGroup() bool {
	return wfr.RowIdx == wfr.PeerGroups[wfr.PeerGroupNumByRowIdx[wfr.RowIdx]].StartIdx
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

// NonDefaultFrameExclusion returns true if optional frame exclusion is present
// and is not ExcludeNoOthers (which is equivalent to omitting the clause).
func (wfr WindowFrameRun) NonDefaultFrameExclusion() bool {
	return wfr.Frame != nil && wfr.Frame.Exclusion != nil && *wfr.Frame.Exclusion != ExcludeNoOthers
}

// IsRowExcluded returns whether the row at index idx should be excluded from
// the window frame of the current row.
func (wfr WindowFrameRun) IsRowExcluded(idx int) bool {
	if wfr.Frame == nil || wfr.Frame.Exclusion == nil {
		// By default, no rows are excluded.
		return false
	}
	switch *wfr.Frame.Exclusion {
	case ExcludeCurrentRow:
		return idx == wfr.RowIdx
	case ExcludeGroup:
		return wfr.PeerGroupNumByRowIdx[idx] == wfr.PeerGroupNumByRowIdx[wfr.RowIdx]
	case ExcludeTies:
		return wfr.PeerGroupNumByRowIdx[idx] == wfr.PeerGroupNumByRowIdx[wfr.RowIdx] && idx != wfr.RowIdx
	case ExcludeNoOthers:
		return false
	default:
		panic("unexpected WindowFrameExclusion")
	}
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
