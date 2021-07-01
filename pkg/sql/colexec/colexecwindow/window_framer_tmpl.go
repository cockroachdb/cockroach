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

	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// {{/*

// Declarations to make the template compile properly.
const _FRAME_MODE = execinfrapb.WindowerSpec_Frame_ROWS
const _START_BOUND = execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING
const _END_BOUND = execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING

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
	// columns. It also resets any fields used for window frame calculation. It
	// must be called before calling next.
	startPartition(ctx context.Context, partitionSize int, storedCols *colexecutils.SpillingBuffer)

	// next should be called for each row in the partition. It advances to the
	// next row and then calculates the window frame for that row. (windowFramer
	// keeps track internally of the index of the current row starting from zero).
	// next should not be called beyond the end of the partition - otherwise,
	// undefined behavior will result.
	next(ctx context.Context)

	// frameFirstIdx returns the index of the first row in the window frame for
	// the current row. If no such row exists, frameFirstIdx returns -1.
	frameFirstIdx() int

	// frameLastIdx returns the index of the last row in the window frame for
	// the current row. If no such row exists, frameLastIdx returns -1. Note that
	// this index is inclusive.
	frameLastIdx() int

	// frameNthIdx returns the index of the nth row (starting from one) in the
	// window frame for the current row. If no such row exists, frameNthIdx
	// returns -1.
	frameNthIdx(n int) int

	// close should always be called upon closing of the parent operator. It
	// releases all references to enable garbage collection.
	close()
}

func newWindowFramer(frame *execinfrapb.WindowerSpec_Frame, peersColIdx int) windowFramer {
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
				op := &_OP_STRING{
					windowFramerBase: windowFramerBase{
						peersColIdx: peersColIdx,
					},
				}
				// {{if .StartHasOffset}}
				op.startOffset = int(startBound.IntOffset)
				// {{end}}
				// {{if .EndHasOffset}}
				op.endOffset = int(endBound.IntOffset)
				// {{end}}
				return op
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
	currentGroup  int // The index of the current peer group, used by GROUPS mode.

	// startIdx and endIdx are bounds on the rows specified by the window frame
	// before considering any exclusion clause. excludeStartIdx and excludeEndIdx
	// are bounds on the rows specified by the exclusion clause (possibly none).
	// These fields are used to calculate the row indexes that are part of the
	// window frame.
	startIdx        int // Inclusive start of the window frame ignoring exclusion.
	endIdx          int // Exclusive end of the window frame ignoring exclusion.
	excludeStartIdx int // Inclusive start index of excluded rows.
	excludeEndIdx   int // Exclusive end index of excluded rows.

	peersColIdx int // Indicates the beginning of each peer group. Can be unset.

	// startOffset and endOffset store the integer offsets for ROWS and GROUPS
	// modes with OFFSET PRECEDING or OFFSET FOLLOWING.
	startOffset, endOffset int
}

// {{range .}}
// {{range .StartBoundTypes}}
// {{range .EndBoundTypes}}

type _OP_STRING struct {
	windowFramerBase
}

var _ windowFramer = &_OP_STRING{}

// startPartition prepares the window framer to begin iterating through a new
// partition.
func (f *_OP_STRING) startPartition(
	ctx context.Context, partitionSize int, storedCols *colexecutils.SpillingBuffer,
) {
	f.windowFramerBase.startPartition(partitionSize, storedCols)
	// {{if and .GroupsMode .OffsetPreceding}}
	f.currentGroup = 0
	// {{end}}
	// {{if and .GroupsMode .StartOffsetFollowing}}
	// f.startIdx must be advanced by 'startOffset' peer groups.
	f.startIdx = f.incrementPeerGroup(ctx, f.startIdx, f.startOffset)
	// {{end}}
	// {{if and .GroupsMode .EndOffsetFollowing}}
	// f.endIdx must be advanced by 'endOffset' peer groups.
	f.endIdx = f.incrementPeerGroup(ctx, f.endIdx, f.endOffset)
	// {{end}}
}

// next is called for each row in the partition. It advances to the next row and
// then calculates the window frame for the current row. next should not be
// called beyond the end of the partition, or undefined behavior may result.
func (f *_OP_STRING) next(ctx context.Context) {
	f.currentRow++
	// {{if and .GroupsMode (not .BothUnbounded)}}
	// {{/*
	// We need to keep track of whether the current row is the first of its peer
	// group in GROUPS mode when one of the bounds is not a variant of UNBOUNDED.
	// The information is used to trigger updates on the start and end indexes
	// whenever we have advanced to a new peer group.
	// */}}
	currRowIsGroupStart := f.isFirstPeer(ctx, f.currentRow)
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
	// {{end}}
	// {{end}}
	// {{if .StartCurrentRow}}
	// {{if .RowsMode}}
	f.startIdx = f.currentRow
	// {{else if .GroupsMode}}
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
	// {{end}}
	// {{end}}
	// Handle the end bound.
	// {{if .EndOffsetPreceding}}
	// {{if .RowsMode}}
	f.endIdx = f.currentRow - f.endOffset + 1
	if f.endIdx < 0 {
		f.endIdx = 0
	}
	// {{else if .GroupsMode}}
	if currRowIsGroupStart && f.currentGroup >= f.endOffset {
		// Advance the end index to the end of the next peer group.
		f.endIdx = f.incrementPeerGroup(ctx, f.endIdx, 1 /* groups */)
	}
	// {{end}}
	// {{end}}
	// {{if .EndCurrentRow}}
	// {{if .RowsMode}}
	f.endIdx = f.currentRow + 1
	// {{else if .GroupsMode}}
	if currRowIsGroupStart {
		f.endIdx = f.incrementPeerGroup(ctx, f.endIdx, 1 /* groups */)
	}
	// {{end}}
	// {{end}}
	// {{if .EndOffsetFollowing}}
	// {{if .RowsMode}}
	f.endIdx = f.currentRow + f.endOffset + 1
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
	// {{end}}
	// {{end}}
	// {{if .EndUnboundedFollowing}}
	f.endIdx = f.partitionSize
	// {{end}}
	// {{if and .GroupsMode .OffsetPreceding}}
	if currRowIsGroupStart {
		f.currentGroup++
	}
	// {{end}}
}

func (f *_OP_STRING) close() {
	*f = _OP_STRING{}
}

// {{end}}
// {{end}}
// {{end}}

// frameFirstIdx returns the index of the first row in the window frame for
// the current row. If no such row exists, frameFirstIdx returns -1.
func (b *windowFramerBase) frameFirstIdx() (idx int) {
	if b.startIdx >= b.endIdx {
		// The window frame is empty, so there is no start index.
		return -1
	}
	return b.startIdx
}

// frameLastIdx returns the index of the last row in the window frame for
// the current row. If no such row exists, frameLastIdx returns -1.
func (b *windowFramerBase) frameLastIdx() (idx int) {
	if b.startIdx >= b.endIdx {
		// The window frame is empty, so there is no end index.
		return -1
	}
	return b.endIdx - 1
}

// frameNthIdx returns the index of the nth row (starting from one) in the
// window frame for the current row. If no such row exists, frameNthIdx
// returns -1.
func (b *windowFramerBase) frameNthIdx(n int) (idx int) {
	if b.startIdx >= b.endIdx {
		// The window frame is empty, so there is no nth index.
		return -1
	}
	// Subtract from n to make it a zero-based index.
	n = n - 1
	idx = b.startIdx + n
	if idx < 0 || idx >= b.endIdx {
		// The requested index is out of range for this window frame.
		idx = -1
	}
	return idx
}

// startPartition is called before each partition is processed. It initializes
// the windowFramer operators with the size of the partition and stored
// columns. It also resets any fields used for window frame calculation.
func (b *windowFramerBase) startPartition(
	partitionSize int, storedCols *colexecutils.SpillingBuffer,
) {
	// Initialize currentRow to -1 to ensure that the first call to next advances
	// currentRow to zero.
	b.currentRow = -1
	b.partitionSize = partitionSize
	b.storedCols = storedCols
	b.startIdx = 0
	b.endIdx = 0
}

// incrementPeerGroup increments the given index by 'groups' peer groups,
// returning the resulting index. If the given offset is greater than the
// remaining number of groups, the returned index will be equal to the size of
// the partition.
func (b *windowFramerBase) incrementPeerGroup(ctx context.Context, index, groups int) int {
	for {
		if groups <= 0 {
			break
		}
		index++
		if index >= b.partitionSize {
			index = b.partitionSize
			break
		}
		if b.isFirstPeer(ctx, index) {
			// We have reached the start of the next peer group (the end of the
			// current one).
			groups--
		}
	}
	return index
}

func (b *windowFramerBase) isFirstPeer(ctx context.Context, idx int) bool {
	if idx == 0 {
		// The first row in the partition is always the first in its peer group.
		return true
	}
	if b.peersColIdx == tree.NoColumnIdx {
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
	if b.peersColIdx != tree.NoColumnIdx {
		b.peersColIdx = storeCol(b.peersColIdx)
	}
	return colsToStore
}
