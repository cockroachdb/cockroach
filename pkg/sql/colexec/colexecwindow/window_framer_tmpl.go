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
// +build execgen_template
//
// This file is the execgen template for window_framer.eg.go. It's formatted in
// a special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecwindow

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// {{/*

// Declarations to make the template compile properly.
const _FRAME_MODE = execinfrapb.WindowerSpec_Frame_RANGE
const _START_BOUND = execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING
const _END_BOUND = execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING
const _EXCLUSION_CASE = false

// */}}

// windowFramer is used to calculate the window frame for each row in a
// partition.
type windowFramer interface {
	// getColsToStore is called upon initialization of the windowFramer operators
	// in order to add to the list of columns to store in the SpillingBuffer owned
	// by the parent operator.
	//
	// getColsToStore adds column indices to the given list when the windowFramer
	// operator needs access to values in a column for each partition (for
	// example, the peer groups column). If a column to be stored is already
	// present in the list, a duplicate entry will not be created.
	getColsToStore(oldColsToStore []int) (colsToStore []int)

	// startPartition is called before each partition is processed. It initializes
	// the windowFramer operators with the size of the partition and stored
	// columns. It also resets any fields used for window frame calculation.
	startPartition(ctx context.Context, partitionSize int, storedCols *colexecutils.SpillingBuffer)

	// next should be called for each row in the partition. It calculates the
	// window frame for the current row (starting from the zeroth) and then
	// increments the currentRow field. next should not be called beyond the end
	// of the partition - otherwise, undefined behavior will result.
	next(ctx context.Context)

	// frameStartIdx returns the index of the first row in the window frame for
	// the current row. If no such row exists, frameStartIndex returns -1.
	frameStartIdx() int

	// frameEndIdx returns the index of the last row in the window frame for
	// the current row. If no such row exists, frameEndIdx returns -1.
	frameEndIdx() int

	// frameNthIdx returns the index of the nth row (starting from one) in the
	// window frame for the current row. If no such row exists, frameNthIdx
	// returns -1. frameNthIdx expects n to be greater than zero - undefined
	// behavior will result otherwise.
	frameNthIdx(n int) int

	// close should always be called upon closing of the parent operator. It
	// releases all references to enable garbage collection.
	close()
}

func newWindowFramer(
	frame *execinfrapb.WindowerSpec_Frame,
	peersColIdx int,
	ordering execinfrapb.Ordering,
	datumAlloc *rowenc.DatumAlloc,
) windowFramer {
	startBound := frame.Bounds.Start
	endBound := frame.Bounds.End
	switch frame.Mode {
	// {{range .}}
	case _FRAME_MODE:
		switch startBound.BoundType {
		// {{range .StartBoundTypes}}
		case _START_BOUND:
			switch endBound.BoundType {
			// {{range .EndBoundTypes}}
			case _END_BOUND:
				switch frame.Exclusion != execinfrapb.WindowerSpec_Frame_NO_EXCLUSION {
				// {{range .Overloads}}
				case _EXCLUSION_CASE:
					// {{if (.StartHasOffset)}}
					if int(startBound.IntOffset) < 0 {
						colexecerror.ExpectedError(errors.Errorf("window frame offset out of range"))
					}
					// {{end}}
					// {{if (.EndHasOffset)}}
					if int(endBound.IntOffset) < 0 {
						colexecerror.ExpectedError(errors.Errorf("window frame offset out of range"))
					}
					// {{end}}
					return &_OP_STRING{
						windowFramerBase: windowFramerBase{
							peersColIdx: peersColIdx,
							unordered:   len(ordering.Columns) == 0,
							exclusion:   frame.Exclusion,
							// {{if and (.RangeMode) (.HasOffsets)}}
							ordColIdx: int(ordering.Columns[0].ColIdx),
							ordColAsc: ordering.Columns[0].Direction == execinfrapb.Ordering_Column_ASC,
							// {{else}}
							ordColIdx: -1,
							// {{end}}
							// {{if (.StartHasOffset)}}
							startOffset: int(startBound.IntOffset),
							// {{end}}
							// {{if (.EndHasOffset)}}
							endOffset: int(endBound.IntOffset),
							// {{end}}
						},
					}
					// {{end}}
				}
				// {{end}}
			}
			// {{end}}
		}
		// {{end}}
	}
	colexecerror.InternalError(errors.AssertionFailedf("unsupported window framer type"))
	return nil
}

// windowFramerBase extracts common fields and methods for window framer
// operators.
type windowFramerBase struct {
	// storedCols stores the columns for which any row in the partition may be
	// accessed during evaluation of the window frame. The window framer operators
	// are not responsible for handling the SpillingBuffer other than to release
	// the reference after close is called.
	storedCols *colexecutils.SpillingBuffer

	partitionSize int // The total number of tuples in the partition.
	currentRow    int // The index of the current row.
	currentGroup  int // The index of the current peer group.
	startIdx      int // Inclusive start of the window frame ignoring exclusion.
	endIdx        int // Exclusive end of the window frame ignoring exclusion.
	exStartIdx    int // Inclusive start index of the excluded rows (if any).
	exEndIdx      int // Exclusive end index of the excluded rows.
	startOffset   int // Input value when the start bound has an offset.
	endOffset     int // Input value when the end bound has an offset.

	exclusion   execinfrapb.WindowerSpec_Frame_Exclusion
	unordered   bool
	ordColAsc   bool
	ordColIdx   int
	peersColIdx int
}

// {{range .}}
// {{range .StartBoundTypes}}
// {{range .EndBoundTypes}}
// {{range .Overloads}}

type _OP_STRING struct {
	windowFramerBase
}

var _ windowFramer = &_OP_STRING{}

// startPartition prepares the window framer to begin iterating through a new
// partition.
func (f *_OP_STRING) startPartition(
	ctx context.Context, partitionSize int, storedCols *colexecutils.SpillingBuffer,
) {
	f.partitionSize = partitionSize
	f.storedCols = storedCols
	f.currentRow = 0
	f.startIdx = 0
	f.endIdx = 0
	// {{if and (.GroupsMode) (or .StartOffsetPreceding .EndOffsetPreceding)}}
	f.currentGroup = 0
	// {{end}}
	// {{if .Exclude}}
	f.exStartIdx = 0
	f.exEndIdx = 0
	// {{end}}
	// {{if and (.GroupsMode) (.StartOffsetFollowing)}}
	// f.startIdx must be advanced by 'startOffset' peer groups.
	f.startIdx = f.incrementPeerGroup(ctx, f.startIdx, f.startOffset)
	// {{end}}
	// {{if and (.GroupsMode) (.EndOffsetFollowing)}}
	// f.endIdx must be advanced by 'endOffset' peer groups.
	f.endIdx = f.incrementPeerGroup(ctx, f.endIdx, f.endOffset)
	// {{end}}
}

// Next is called for each row in the partition. It calculates the window frame
// for the current row and then advances to the next row.
func (f *_OP_STRING) next(ctx context.Context) {
	// {{if or (and .GroupsMode (.HasOffsets)) (and (not .RowsMode) (or .StartCurrentRow .EndCurrentRow))}}
	currRowIsGroupStart := f.isFirstPeer(ctx, f.currentRow)
	// {{else if .Exclude}}
	var currRowIsGroupStart bool
	if !f.excludeCurrRow() {
		currRowIsGroupStart = f.isFirstPeer(ctx, f.currentRow)
	}
	// {{end}}
	// Handle the start bound.
	// {{if .StartUnboundedPreceding}}
	f.startIdx = 0
	// {{end}}
	// {{if .StartOffsetPreceding}}
	// {{if .RowsMode}}
	f.startIdx = f.currentRow - f.startOffset
	if f.startIdx < 0 {
		f.startIdx = 0
	}
	// {{else if .GroupsMode}}
	if currRowIsGroupStart && f.currentGroup > f.startOffset {
		// Advance the start index to the start of the next peer group.
		f.startIdx = f.incrementPeerGroup(ctx, f.startIdx, 1 /* groups */)
	}
	// {{else if .RangeMode}}
	// TODO
	// {{end}}
	// {{end}}
	// {{if .StartCurrentRow}}
	// {{if .RowsMode}}
	f.startIdx = f.currentRow
	// {{else if or (.GroupsMode) (.RangeMode)}}
	if currRowIsGroupStart {
		f.startIdx = f.currentRow
	}
	// {{end}}
	// {{end}}
	// {{if .StartOffsetFollowing}}
	// {{if .RowsMode}}
	f.startIdx = f.currentRow + f.startOffset
	if f.startIdx > f.partitionSize {
		f.startIdx = f.partitionSize
	}
	// {{else if .GroupsMode}}
	if currRowIsGroupStart && f.currentRow > 0 {
		// The start index will already have been advanced by f.startOffset rows, so
		// we only need to advance to the next adjacent peers group whenever the
		// currentRow pointer enters a new peers group.
		f.startIdx = f.incrementPeerGroup(ctx, f.startIdx, 1 /* groups */)
	}
	// {{else if .RangeMode}}
	// TODO
	// {{end}}
	// {{end}}
	// Handle the end bound.
	// {{if .EndOffsetPreceding}}
	// {{if .RowsMode}}
	f.endIdx = f.currentRow - f.endOffset
	if f.endIdx < 0 {
		f.endIdx = 0
	}
	// {{else if .GroupsMode}}
	if currRowIsGroupStart && f.currentGroup >= f.endOffset {
		// Advance the end index to the end of the next peer group.
		f.endIdx = f.incrementPeerGroup(ctx, f.endIdx, 1 /* groups */)
	}
	// {{else if .RangeMode}}
	// TODO
	// {{end}}
	// {{end}}
	// {{if .EndCurrentRow}}
	// {{if .RowsMode}}
	f.endIdx = f.currentRow + 1
	// {{else if or (.GroupsMode) (.RangeMode)}}
	if currRowIsGroupStart {
		f.endIdx = f.incrementPeerGroup(ctx, f.endIdx, 1 /* groups */)
	}
	// {{end}}
	// {{end}}
	// {{if .EndOffsetFollowing}}
	// {{if .RowsMode}}
	f.endIdx = f.currentRow + f.startOffset + 1
	if f.endIdx > f.partitionSize {
		f.endIdx = f.partitionSize
	}
	// {{else if .GroupsMode}}
	if currRowIsGroupStart {
		// The end index will already have been advanced by f.startOffset rows, so
		// we only need to advance to the next adjacent peers group whenever the
		// currentRow pointer enters a new peers group.
		f.endIdx = f.incrementPeerGroup(ctx, f.endIdx, 1 /* groups */)
	}
	// {{else if .RangeMode}}
	// TODO
	// {{end}}
	// {{end}}
	// {{if .EndUnboundedFollowing}}
	f.endIdx = f.partitionSize
	// {{end}}
	// {{if .Exclude}}
	// Handle the exclusion clause.
	if f.excludeCurrRow() {
		f.exStartIdx = f.currentRow
		f.exEndIdx = f.currentRow + 1
	} else if currRowIsGroupStart {
		// The current row is the start of a new peers group. We need to update the
		// exclusion indices.
		f.exStartIdx, f.exEndIdx = f.currentRow, f.currentRow
		f.exEndIdx = f.incrementPeerGroup(ctx, f.exEndIdx, 1 /* groups */)
	}
	// {{end}}
	// {{if and (.GroupsMode) (or .StartOffsetPreceding .EndOffsetPreceding)}}
	if f.currentRow > 0 && currRowIsGroupStart {
		f.currentGroup++
	}
	// {{end}}
	f.currentRow++
}

func (f *_OP_STRING) frameStartIdx() (idx int) {
	if f.startIdx >= f.endIdx {
		// The window frame is empty, so there is no start index.
		return -1
	}
	idx = f.startIdx
	// {{if .Exclude}}
	if f.exStartIdx <= idx && f.exEndIdx > idx {
		// The excluded rows overlap with the start of the frame, so we will have to
		// adjust the index.
		if f.excludeGroup() || f.excludeCurrRow() {
			idx = f.exEndIdx
		} else {
			idx = f.currentRow
		}
	}
	// {{end}}
	if idx < f.startIdx || idx >= f.endIdx {
		// The window frame is empty, so there is no start index.
		idx = -1
	}
	return idx
}

func (f *_OP_STRING) frameEndIdx() (idx int) {
	if f.startIdx >= f.endIdx {
		// The window frame is empty, so there is no end index.
		return -1
	}
	idx = f.endIdx - 1
	// {{if .Exclude}}
	if f.exStartIdx <= idx && f.exEndIdx > idx {
		// The excluded rows overlap with the end of the frame, so we will have to
		// adjust the index.
		if f.excludeGroup() || f.excludeCurrRow() {
			idx = f.exStartIdx - 1
		} else {
			idx = f.currentRow
		}
	}
	// {{end}}
	if idx < f.startIdx || idx >= f.endIdx {
		// The window frame is empty, so there is no end index.
		idx = -1
	}
	return idx
}

func (f *_OP_STRING) frameNthIdx(n int) (idx int) {
	if f.startIdx >= f.endIdx {
		// The window frame is empty, so there is no nth index.
		return -1
	}
	// Subtract from n to make it a zero-based index.
	n = n - 1
	idx = f.startIdx + n
	// {{if .Exclude}}
	if f.exStartIdx <= idx {
		// The index is located beyond the excluded rows.
		offset := f.exEndIdx - f.exStartIdx
		if f.exStartIdx < f.startIdx {
			// Don't include rows beyond startIdx in the offset.
			offset -= f.startIdx - f.exStartIdx
		}
		idx += offset
		if f.excludeTies() {
			// The current row is not excluded, so we have incremented the index too
			// far.
			idx--
			if idx < f.exEndIdx {
				idx = f.currentRow
			}
		}
	}
	// {{end}}
	if idx < f.startIdx || idx >= f.endIdx {
		// The requested index is out of range for this window frame.
		idx = -1
	}
	return idx
}

// {{end}}
// {{end}}
// {{end}}
// {{end}}

// incrementPeerGroup increments the given index by 'groups' peer groups,
// returning the resulting index. If the given offset is greater than the
// remaining number of groups, the returned index will be equal to the size of
// the partition.
func (b *windowFramerBase) incrementPeerGroup(ctx context.Context, index, groups int) int {
	for {
		if groups <= 0 || index >= b.partitionSize {
			break
		}
		groups--
		index++
		if b.isFirstPeer(ctx, index) {
			// We have reached the start of the next peer group (the end of the
			// current one).
			break
		}
	}
	return index
}

func (b *windowFramerBase) isFirstPeer(ctx context.Context, idx int) bool {
	if idx == 0 {
		// The first row in the partition is always the first in its peer group.
		return true
	}
	if b.unordered {
		// All rows are peers, so only the first is the first in its peer group.
		return false
	}
	batch, batchIdx := b.storedCols.GetBatchWithTuple(ctx, idx)
	return batch.ColVec(b.peersColIdx).Bool()[batchIdx]
}

// getColsToStore appends to the given slice of column indices whatever columns
// to which the window framer will need access. getColsToStore also remaps the
// corresponding fields in the window framer to refer to ordinal positions
// within the colsToStore slice rather than within the input batches.
func (b *windowFramerBase) getColsToStore(oldColsToStore []int) (colsToStore []int) {
	colsToStore = oldColsToStore
	storeCol := func(colIdx int) int {
		for i := range colsToStore {
			if colsToStore[i] == colIdx {
				// The column is already present in colsToStore. Do not store any column
				// more than once.
				return i
			}
		}
		colsToStore = append(colsToStore, colIdx)
		return len(colsToStore) - 1
	}
	if b.peersColIdx != -1 {
		b.peersColIdx = storeCol(b.peersColIdx)
	}
	if b.ordColIdx != -1 {
		b.ordColIdx = storeCol(b.ordColIdx)
	}
	return colsToStore
}

func decodeOffset(
	datumAlloc *rowenc.DatumAlloc, offsetType *types.T, typedOffset []byte,
) interface{} {
	datum, rem, err := rowenc.DecodeTableValue(datumAlloc, offsetType, typedOffset)
	if err != nil {
		colexecerror.InternalError(errors.NewAssertionErrorWithWrappedErrf(err,
			"error decoding %d bytes", errors.Safe(len(typedOffset))))
	}
	if len(rem) != 0 {
		colexecerror.InternalError(errors.AssertionFailedf(
			"%d trailing bytes in encoded value", errors.Safe(len(rem))))
	}
	typeConverter := colconv.GetDatumToPhysicalFn(offsetType)
	return typeConverter(datum)
}

func (b *windowFramerBase) excludeCurrRow() bool {
	return b.exclusion == execinfrapb.WindowerSpec_Frame_EXCLUDE_CURRENT_ROW
}

func (b *windowFramerBase) excludeGroup() bool {
	return b.exclusion == execinfrapb.WindowerSpec_Frame_EXCLUDE_GROUP
}

func (b *windowFramerBase) excludeTies() bool {
	return b.exclusion == execinfrapb.WindowerSpec_Frame_EXCLUDE_TIES
}

func (b *windowFramerBase) close() {
	b.storedCols = nil
}
