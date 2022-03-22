// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// IndexedRows are rows with the corresponding indices.
type IndexedRows interface {
	Len() int                                                // returns number of rows
	GetRow(ctx context.Context, idx int) (IndexedRow, error) // returns a row at the given index or an error
}

// IndexedRow is a row with a corresponding index.
type IndexedRow interface {
	GetIdx() int                                    // returns index of the row
	GetDatum(idx int) (Datum, error)                // returns a datum at the given index
	GetDatums(startIdx, endIdx int) (Datums, error) // returns datums at indices [startIdx, endIdx)
}

// WindowFrameRun contains the runtime state of window frame during calculations.
type WindowFrameRun struct {
	// constant for all calls to WindowFunc.Add
	Rows             IndexedRows
	ArgsIdxs         []uint32     // indices of the arguments to the window function
	Frame            *WindowFrame // If non-nil, Frame represents the frame specification of this window. If nil, default frame is used.
	StartBoundOffset Datum
	EndBoundOffset   Datum
	FilterColIdx     int
	OrdColIdx        int                // Column over which rows are ordered within the partition. It is only required in RANGE mode.
	OrdDirection     encoding.Direction // Direction of the ordering over OrdColIdx.
	PlusOp, MinusOp  *BinOp             // Binary operators for addition and subtraction required only in RANGE mode.
	PeerHelper       PeerGroupsIndicesHelper

	// Any error that occurred within methods that cannot return an error (like
	// within a closure that is passed into sort.Search()).
	err error

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
func (o WindowFrameRangeOps) LookupImpl(left, right *types.T) (*BinOp, *BinOp, bool) {
	plusOverloads, minusOverloads := BinOps[treebin.Plus], BinOps[treebin.Minus]
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
	ctx context.Context, evalCtx *EvalContext, offset Datum, negative bool,
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
	value, err := wfr.valueAt(ctx, wfr.RowIdx)
	if err != nil {
		return nil, err
	}
	if value == DNull {
		return DNull, nil
	}
	return binOp.Fn(evalCtx, value, offset)
}

// FrameStartIdx returns the index of starting row in the frame (which is the first to be included).
func (wfr *WindowFrameRun) FrameStartIdx(ctx context.Context, evalCtx *EvalContext) (int, error) {
	if wfr.Frame == nil {
		return 0, nil
	}
	switch wfr.Frame.Mode {
	case treewindow.RANGE:
		switch wfr.Frame.Bounds.StartBound.BoundType {
		case treewindow.UnboundedPreceding:
			return 0, nil
		case treewindow.OffsetPreceding:
			value, err := wfr.getValueByOffset(ctx, evalCtx, wfr.StartBoundOffset, true /* negative */)
			if err != nil {
				return 0, err
			}
			if wfr.OrdDirection == encoding.Descending {
				// We use binary search on [0, wfr.RowIdx) interval to find the first row
				// whose value is smaller or equal to 'value'. If such row is not found,
				// then Search will correctly return wfr.RowIdx.
				return sort.Search(wfr.RowIdx, func(i int) bool {
					if wfr.err != nil {
						return false
					}
					valueAt, err := wfr.valueAt(ctx, i)
					if err != nil {
						wfr.err = err
						return false
					}
					cmp, err := compareForWindow(evalCtx, valueAt, value)
					if err != nil {
						wfr.err = err
						return false
					}
					return cmp <= 0
				}), wfr.err
			}
			// We use binary search on [0, wfr.RowIdx) interval to find the first row
			// whose value is greater or equal to 'value'. If such row is not found,
			// then Search will correctly return wfr.RowIdx.
			return sort.Search(wfr.RowIdx, func(i int) bool {
				if wfr.err != nil {
					return false
				}
				valueAt, err := wfr.valueAt(ctx, i)
				if err != nil {
					wfr.err = err
					return false
				}
				cmp, err := compareForWindow(evalCtx, valueAt, value)
				if err != nil {
					wfr.err = err
					return false
				}
				return cmp >= 0
			}), wfr.err
		case treewindow.CurrentRow:
			// Spec: in RANGE mode CURRENT ROW means that the frame starts with the current row's first peer.
			return wfr.PeerHelper.GetFirstPeerIdx(wfr.CurRowPeerGroupNum), nil
		case treewindow.OffsetFollowing:
			value, err := wfr.getValueByOffset(ctx, evalCtx, wfr.StartBoundOffset, false /* negative */)
			if err != nil {
				return 0, err
			}
			if wfr.OrdDirection == encoding.Descending {
				// We use binary search on [0, wfr.PartitionSize()) interval to find
				// the first row whose value is smaller or equal to 'value'.
				return sort.Search(wfr.PartitionSize(), func(i int) bool {
					if wfr.err != nil {
						return false
					}
					valueAt, err := wfr.valueAt(ctx, i)
					if err != nil {
						wfr.err = err
						return false
					}
					cmp, err := compareForWindow(evalCtx, valueAt, value)
					if err != nil {
						wfr.err = err
						return false
					}
					return cmp <= 0
				}), wfr.err
			}
			// We use binary search on [0, wfr.PartitionSize()) interval to find the
			// first row whose value is greater or equal to 'value'.
			return sort.Search(wfr.PartitionSize(), func(i int) bool {
				if wfr.err != nil {
					return false
				}
				valueAt, err := wfr.valueAt(ctx, i)
				if err != nil {
					wfr.err = err
					return false
				}
				cmp, err := compareForWindow(evalCtx, valueAt, value)
				if err != nil {
					wfr.err = err
					return false
				}
				return cmp >= 0
			}), wfr.err
		default:
			return 0, errors.AssertionFailedf(
				"unexpected WindowFrameBoundType in RANGE mode: %d",
				redact.Safe(wfr.Frame.Bounds.StartBound.BoundType))
		}
	case treewindow.ROWS:
		switch wfr.Frame.Bounds.StartBound.BoundType {
		case treewindow.UnboundedPreceding:
			return 0, nil
		case treewindow.OffsetPreceding:
			offset := MustBeDInt(wfr.StartBoundOffset)
			idx := wfr.RowIdx - int(offset)
			if idx < 0 {
				idx = 0
			}
			return idx, nil
		case treewindow.CurrentRow:
			return wfr.RowIdx, nil
		case treewindow.OffsetFollowing:
			offset := MustBeDInt(wfr.StartBoundOffset)
			idx := wfr.RowIdx + int(offset)
			if idx >= wfr.PartitionSize() || int(offset) >= wfr.PartitionSize() {
				// The second part of the condition protects us from an integer
				// overflow when offset is very large.
				idx = wfr.unboundedFollowing()
			}
			return idx, nil
		default:
			return 0, errors.AssertionFailedf(
				"unexpected WindowFrameBoundType in ROWS mode: %d",
				redact.Safe(wfr.Frame.Bounds.StartBound.BoundType))
		}
	case treewindow.GROUPS:
		switch wfr.Frame.Bounds.StartBound.BoundType {
		case treewindow.UnboundedPreceding:
			return 0, nil
		case treewindow.OffsetPreceding:
			offset := MustBeDInt(wfr.StartBoundOffset)
			peerGroupNum := wfr.CurRowPeerGroupNum - int(offset)
			if peerGroupNum < 0 {
				peerGroupNum = 0
			}
			return wfr.PeerHelper.GetFirstPeerIdx(peerGroupNum), nil
		case treewindow.CurrentRow:
			// Spec: in GROUPS mode CURRENT ROW means that the frame starts with the current row's first peer.
			return wfr.PeerHelper.GetFirstPeerIdx(wfr.CurRowPeerGroupNum), nil
		case treewindow.OffsetFollowing:
			offset := MustBeDInt(wfr.StartBoundOffset)
			peerGroupNum := wfr.CurRowPeerGroupNum + int(offset)
			lastPeerGroupNum := wfr.PeerHelper.GetLastPeerGroupNum()
			if peerGroupNum > lastPeerGroupNum || peerGroupNum < 0 {
				// peerGroupNum is out of bounds, so we return the index of the first
				// row after the partition.
				return wfr.unboundedFollowing(), nil
			}
			return wfr.PeerHelper.GetFirstPeerIdx(peerGroupNum), nil
		default:
			return 0, errors.AssertionFailedf(
				"unexpected WindowFrameBoundType in GROUPS mode: %d",
				redact.Safe(wfr.Frame.Bounds.StartBound.BoundType))
		}
	default:
		return 0, errors.AssertionFailedf("unexpected WindowFrameMode: %d", wfr.Frame.Mode)
	}
}

// IsDefaultFrame returns whether a frame equivalent to the default frame
// is being used (default is RANGE UNBOUNDED PRECEDING).
func (f *WindowFrame) IsDefaultFrame() bool {
	if f == nil {
		return true
	}
	if f.Bounds.StartBound.BoundType == treewindow.UnboundedPreceding {
		return f.DefaultFrameExclusion() && f.Mode == treewindow.RANGE &&
			(f.Bounds.EndBound == nil || f.Bounds.EndBound.BoundType == treewindow.CurrentRow)
	}
	return false
}

// DefaultFrameExclusion returns true if optional frame exclusion is omitted.
func (f *WindowFrame) DefaultFrameExclusion() bool {
	return f == nil || f.Exclusion == treewindow.NoExclusion
}

// FrameEndIdx returns the index of the first row after the frame.
func (wfr *WindowFrameRun) FrameEndIdx(ctx context.Context, evalCtx *EvalContext) (int, error) {
	if wfr.Frame == nil {
		return wfr.DefaultFrameSize(), nil
	}
	switch wfr.Frame.Mode {
	case treewindow.RANGE:
		if wfr.Frame.Bounds.EndBound == nil {
			// We're using default value of CURRENT ROW when EndBound is omitted.
			// Spec: in RANGE mode CURRENT ROW means that the frame ends with the current row's last peer.
			return wfr.DefaultFrameSize(), nil
		}
		switch wfr.Frame.Bounds.EndBound.BoundType {
		case treewindow.OffsetPreceding:
			value, err := wfr.getValueByOffset(ctx, evalCtx, wfr.EndBoundOffset, true /* negative */)
			if err != nil {
				return 0, err
			}
			if wfr.OrdDirection == encoding.Descending {
				// We use binary search on [0, wfr.PartitionSize()) interval to find
				// the first row whose value is smaller than 'value'. If such row is
				// not found, then Search will correctly return wfr.PartitionSize().
				// Note that searching up to wfr.RowIdx is not correct in case of a
				// zero offset (we need to include all peers of the current row).
				return sort.Search(wfr.PartitionSize(), func(i int) bool {
					if wfr.err != nil {
						return false
					}
					valueAt, err := wfr.valueAt(ctx, i)
					if err != nil {
						wfr.err = err
						return false
					}
					cmp, err := compareForWindow(evalCtx, valueAt, value)
					if err != nil {
						wfr.err = err
						return false
					}
					return cmp < 0
				}), wfr.err
			}
			// We use binary search on [0, wfr.PartitionSize()) interval to find
			// the first row whose value is smaller than 'value'. If such row is
			// not found, then Search will correctly return wfr.PartitionSize().
			// Note that searching up to wfr.RowIdx is not correct in case of a
			// zero offset (we need to include all peers of the current row).
			return sort.Search(wfr.PartitionSize(), func(i int) bool {
				if wfr.err != nil {
					return false
				}
				valueAt, err := wfr.valueAt(ctx, i)
				if err != nil {
					wfr.err = err
					return false
				}
				cmp, err := compareForWindow(evalCtx, valueAt, value)
				if err != nil {
					wfr.err = err
					return false
				}
				return cmp > 0
			}), wfr.err
		case treewindow.CurrentRow:
			// Spec: in RANGE mode CURRENT ROW means that the frame end with the current row's last peer.
			return wfr.DefaultFrameSize(), nil
		case treewindow.OffsetFollowing:
			value, err := wfr.getValueByOffset(ctx, evalCtx, wfr.EndBoundOffset, false /* negative */)
			if err != nil {
				return 0, err
			}
			if wfr.OrdDirection == encoding.Descending {
				// We use binary search on [0, wfr.PartitionSize()) interval to find
				// the first row whose value is smaller than 'value'.
				return sort.Search(wfr.PartitionSize(), func(i int) bool {
					if wfr.err != nil {
						return false
					}
					valueAt, err := wfr.valueAt(ctx, i)
					if err != nil {
						wfr.err = err
						return false
					}
					cmp, err := compareForWindow(evalCtx, valueAt, value)
					if err != nil {
						wfr.err = err
						return false
					}
					return cmp < 0
				}), wfr.err
			}
			// We use binary search on [0, wfr.PartitionSize()) interval to find
			// the first row whose value is smaller than 'value'.
			return sort.Search(wfr.PartitionSize(), func(i int) bool {
				if wfr.err != nil {
					return false
				}
				valueAt, err := wfr.valueAt(ctx, i)
				if err != nil {
					wfr.err = err
					return false
				}
				cmp, err := compareForWindow(evalCtx, valueAt, value)
				if err != nil {
					wfr.err = err
					return false
				}
				return cmp > 0
			}), wfr.err
		case treewindow.UnboundedFollowing:
			return wfr.unboundedFollowing(), nil
		default:
			return 0, errors.AssertionFailedf(
				"unexpected WindowFrameBoundType in RANGE mode: %d",
				redact.Safe(wfr.Frame.Bounds.EndBound.BoundType))
		}
	case treewindow.ROWS:
		if wfr.Frame.Bounds.EndBound == nil {
			// We're using default value of CURRENT ROW when EndBound is omitted.
			return wfr.RowIdx + 1, nil
		}
		switch wfr.Frame.Bounds.EndBound.BoundType {
		case treewindow.OffsetPreceding:
			offset := MustBeDInt(wfr.EndBoundOffset)
			idx := wfr.RowIdx - int(offset) + 1
			if idx < 0 {
				idx = 0
			}
			return idx, nil
		case treewindow.CurrentRow:
			return wfr.RowIdx + 1, nil
		case treewindow.OffsetFollowing:
			offset := MustBeDInt(wfr.EndBoundOffset)
			idx := wfr.RowIdx + int(offset) + 1
			if idx >= wfr.PartitionSize() || int(offset) >= wfr.PartitionSize() {
				// The second part of the condition protects us from an integer
				// overflow when offset is very large.
				idx = wfr.unboundedFollowing()
			}
			return idx, nil
		case treewindow.UnboundedFollowing:
			return wfr.unboundedFollowing(), nil
		default:
			return 0, errors.AssertionFailedf(
				"unexpected WindowFrameBoundType in ROWS mode: %d",
				redact.Safe(wfr.Frame.Bounds.EndBound.BoundType))
		}
	case treewindow.GROUPS:
		if wfr.Frame.Bounds.EndBound == nil {
			// We're using default value of CURRENT ROW when EndBound is omitted.
			// Spec: in GROUPS mode CURRENT ROW means that the frame ends with the current row's last peer.
			return wfr.DefaultFrameSize(), nil
		}
		switch wfr.Frame.Bounds.EndBound.BoundType {
		case treewindow.OffsetPreceding:
			offset := MustBeDInt(wfr.EndBoundOffset)
			peerGroupNum := wfr.CurRowPeerGroupNum - int(offset)
			if peerGroupNum < wfr.PeerHelper.headPeerGroupNum {
				// EndBound's peer group is "outside" of the partition.
				return 0, nil
			}
			return wfr.PeerHelper.GetFirstPeerIdx(peerGroupNum) + wfr.PeerHelper.GetRowCount(peerGroupNum), nil
		case treewindow.CurrentRow:
			return wfr.DefaultFrameSize(), nil
		case treewindow.OffsetFollowing:
			offset := MustBeDInt(wfr.EndBoundOffset)
			peerGroupNum := wfr.CurRowPeerGroupNum + int(offset)
			lastPeerGroupNum := wfr.PeerHelper.GetLastPeerGroupNum()
			if peerGroupNum > lastPeerGroupNum || peerGroupNum < 0 {
				// peerGroupNum is out of bounds, so we return the index of the first
				// row after the partition.
				return wfr.unboundedFollowing(), nil
			}
			return wfr.PeerHelper.GetFirstPeerIdx(peerGroupNum) + wfr.PeerHelper.GetRowCount(peerGroupNum), nil
		case treewindow.UnboundedFollowing:
			return wfr.unboundedFollowing(), nil
		default:
			return 0, errors.AssertionFailedf(
				"unexpected WindowFrameBoundType in GROUPS mode: %d",
				redact.Safe(wfr.Frame.Bounds.EndBound.BoundType))
		}
	default:
		return 0, errors.AssertionFailedf(
			"unexpected WindowFrameMode: %d", redact.Safe(wfr.Frame.Mode))
	}
}

// FrameSize returns the number of rows in the current frame (taking into
// account - if present - a filter and a frame exclusion).
func (wfr *WindowFrameRun) FrameSize(ctx context.Context, evalCtx *EvalContext) (int, error) {
	if wfr.Frame == nil {
		return wfr.DefaultFrameSize(), nil
	}
	frameEndIdx, err := wfr.FrameEndIdx(ctx, evalCtx)
	if err != nil {
		return 0, err
	}
	frameStartIdx, err := wfr.FrameStartIdx(ctx, evalCtx)
	if err != nil {
		return 0, err
	}
	size := frameEndIdx - frameStartIdx
	if !wfr.noFilter() || !wfr.Frame.DefaultFrameExclusion() {
		size = 0
		for idx := frameStartIdx; idx < frameEndIdx; idx++ {
			if skipped, err := wfr.IsRowSkipped(ctx, idx); err != nil {
				return 0, err
			} else if skipped {
				continue
			}
			size++
		}
	}
	if size <= 0 {
		size = 0
	}
	return size, nil
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
func (wfr *WindowFrameRun) Args(ctx context.Context) (Datums, error) {
	return wfr.ArgsWithRowOffset(ctx, 0)
}

// ArgsWithRowOffset returns the argument set at the given offset in the window frame.
func (wfr *WindowFrameRun) ArgsWithRowOffset(ctx context.Context, offset int) (Datums, error) {
	return wfr.ArgsByRowIdx(ctx, wfr.RowIdx+offset)
}

// ArgsByRowIdx returns the argument set of the row at idx.
func (wfr *WindowFrameRun) ArgsByRowIdx(ctx context.Context, idx int) (Datums, error) {
	row, err := wfr.Rows.GetRow(ctx, idx)
	if err != nil {
		return nil, err
	}
	datums := make(Datums, len(wfr.ArgsIdxs))
	for i, argIdx := range wfr.ArgsIdxs {
		datums[i], err = row.GetDatum(int(argIdx))
		if err != nil {
			return nil, err
		}
	}
	return datums, nil
}

// valueAt returns the first argument of the window function at the row idx.
func (wfr *WindowFrameRun) valueAt(ctx context.Context, idx int) (Datum, error) {
	row, err := wfr.Rows.GetRow(ctx, idx)
	if err != nil {
		return nil, err
	}
	return row.GetDatum(wfr.OrdColIdx)
}

// RangeModeWithOffsets returns whether the frame is in RANGE mode with at least
// one of the bounds containing an offset.
func (wfr *WindowFrameRun) RangeModeWithOffsets() bool {
	return wfr.Frame.Mode == treewindow.RANGE && wfr.Frame.Bounds.HasOffset()
}

// FullPartitionIsInWindow checks whether we have such a window frame that all
// rows of the partition are inside of the window for each of the rows.
func (wfr *WindowFrameRun) FullPartitionIsInWindow() bool {
	// Note that we do not need to check whether a filter is present because
	// application of the filter to a row does not depend on the position of the
	// row or whether it is inside of the window frame.
	if wfr.Frame == nil || !wfr.Frame.DefaultFrameExclusion() {
		return false
	}
	if wfr.Frame.Bounds.EndBound == nil {
		// If the end bound is omitted, it is CURRENT ROW (the default value) which
		// doesn't guarantee full partition in the window for all rows.
		return false
	}
	// precedingConfirmed and followingConfirmed indicate whether, for every row,
	// all preceding and following, respectively, rows are always in the window.
	precedingConfirmed := wfr.Frame.Bounds.StartBound.BoundType == treewindow.UnboundedPreceding
	followingConfirmed := wfr.Frame.Bounds.EndBound.BoundType == treewindow.UnboundedFollowing
	if wfr.Frame.Mode == treewindow.ROWS || wfr.Frame.Mode == treewindow.GROUPS {
		// Every peer group in GROUPS mode always contains at least one row, so
		// treating GROUPS as ROWS here is a subset of the cases when we should
		// return true.
		if wfr.Frame.Bounds.StartBound.BoundType == treewindow.OffsetPreceding {
			// Both ROWS and GROUPS have an offset of integer type, so this type
			// conversion is safe.
			startOffset := wfr.StartBoundOffset.(*DInt)
			// The idea of this conditional is that to confirm that all preceding
			// rows will always be in the window, we only need to look at the last
			// row: if startOffset is at least as large as the number of rows in the
			// partition before the last one, then it will be true for the first to
			// last, second to last, etc.
			precedingConfirmed = precedingConfirmed || *startOffset >= DInt(wfr.Rows.Len()-1)
		}
		if wfr.Frame.Bounds.EndBound.BoundType == treewindow.OffsetFollowing {
			// Both ROWS and GROUPS have an offset of integer type, so this type
			// conversion is safe.
			endOffset := wfr.EndBoundOffset.(*DInt)
			// The idea of this conditional is that to confirm that all following
			// rows will always be in the window, we only need to look at the first
			// row: if endOffset is at least as large as the number of rows in the
			// partition after the first one, then it will be true for the second,
			// third, etc rows as well.
			followingConfirmed = followingConfirmed || *endOffset >= DInt(wfr.Rows.Len()-1)
		}
	}
	return precedingConfirmed && followingConfirmed
}

// noFilter returns whether a filter is present.
func (wfr *WindowFrameRun) noFilter() bool {
	return wfr.FilterColIdx == NoColumnIdx
}

// isRowExcluded returns whether the row at index idx should be excluded from
// the window frame of the current row.
func (wfr *WindowFrameRun) isRowExcluded(idx int) (bool, error) {
	if wfr.Frame.DefaultFrameExclusion() {
		// By default, no rows are excluded.
		return false, nil
	}
	switch wfr.Frame.Exclusion {
	case treewindow.ExcludeCurrentRow:
		return idx == wfr.RowIdx, nil
	case treewindow.ExcludeGroup:
		curRowFirstPeerIdx := wfr.PeerHelper.GetFirstPeerIdx(wfr.CurRowPeerGroupNum)
		curRowPeerGroupRowCount := wfr.PeerHelper.GetRowCount(wfr.CurRowPeerGroupNum)
		return curRowFirstPeerIdx <= idx && idx < curRowFirstPeerIdx+curRowPeerGroupRowCount, nil
	case treewindow.ExcludeTies:
		curRowFirstPeerIdx := wfr.PeerHelper.GetFirstPeerIdx(wfr.CurRowPeerGroupNum)
		curRowPeerGroupRowCount := wfr.PeerHelper.GetRowCount(wfr.CurRowPeerGroupNum)
		return curRowFirstPeerIdx <= idx && idx < curRowFirstPeerIdx+curRowPeerGroupRowCount && idx != wfr.RowIdx, nil
	default:
		return false, errors.AssertionFailedf("unexpected WindowFrameExclusion")
	}
}

// IsRowSkipped returns whether a row at index idx is skipped from the window
// frame (it can either be filtered out according to the filter clause or
// excluded according to the frame exclusion clause) and any error if it
// occurs.
func (wfr *WindowFrameRun) IsRowSkipped(ctx context.Context, idx int) (bool, error) {
	if !wfr.noFilter() {
		row, err := wfr.Rows.GetRow(ctx, idx)
		if err != nil {
			return false, err
		}
		d, err := row.GetDatum(wfr.FilterColIdx)
		if err != nil {
			return false, err
		}
		if d != DBoolTrue {
			// Row idx is filtered out from the window frame, so it is skipped.
			return true, nil
		}
	}
	// If a row is excluded from the window frame, it is skipped.
	return wfr.isRowExcluded(idx)
}

// compareForWindow wraps the Datum Compare method so that casts can be
// performed up front. This allows us to return an expected error in the event
// of an invalid comparison, rather than panicking.
func compareForWindow(evalCtx *EvalContext, left, right Datum) (int, error) {
	if types.IsDateTimeType(left.ResolvedType()) && left.ResolvedType() != types.Interval {
		// Datetime values (other than Intervals) are converted to timestamps for
		// comparison. Note that the right side never needs to be casted.
		ts, err := timeFromDatumForComparison(evalCtx, left)
		if err != nil {
			return 0, err
		}
		left, err = MakeDTimestampTZ(ts, time.Microsecond)
		if err != nil {
			return 0, err
		}
	}
	return left.Compare(evalCtx, right), nil
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

	// Reset resets the window function which allows for reusing it when
	// computing over different partitions.
	Reset(context.Context)

	// Close allows the window function to free any memory it requested during execution,
	// such as during the execution of an aggregation like CONCAT_AGG or ARRAY_AGG.
	Close(context.Context, *EvalContext)
}
