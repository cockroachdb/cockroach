// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// {{/*

// Declarations to make the template compile properly.
const _FRAME_MODE = execinfrapb.WindowerSpec_Frame_ROWS
const _START_BOUND = execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING
const _END_BOUND = execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING
const _EXCLUDES_ROWS = false

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
	// this index is inclusive so that operators that need the last index don't
	// need to have logic to detect when the frame is empty (say, because of the
	// exclusion clause).
	frameLastIdx() int

	// frameNthIdx returns the index of the nth row (starting from one to mirror
	// the nth_value window function) in the window frame for the current row. If
	// no such row exists, frameNthIdx returns -1.
	frameNthIdx(n int) int

	// frameIntervals returns a series of intervals that describes the set of all
	// rows that are part of the frame for the current row. Note that there are at
	// most three intervals - this case can occur when EXCLUDE TIES is used.
	// frameIntervals is used to compute aggregate functions over a window. The
	// returned intervals cannot be modified.
	frameIntervals() []windowInterval

	// slidingWindowIntervals returns a pair of interval sets that describes the
	// rows that should be added to the current aggregation, and those which
	// should be removed from the current aggregation. It is used to implement the
	// sliding window optimization for aggregate window functions. toAdd specifies
	// the rows that should be accumulated in the current aggregation, and
	// toRemove specifies those which should be removed.
	slidingWindowIntervals() (toAdd, toRemove []windowInterval)

	// close should always be called upon closing of the parent operator. It
	// releases all references to enable garbage collection.
	close()
}

func newWindowFramer(
	evalCtx *eval.Context,
	frame *execinfrapb.WindowerSpec_Frame,
	ordering *execinfrapb.Ordering,
	inputTypes []*types.T,
	peersColIdx int,
) windowFramer {
	startBound := frame.Bounds.Start
	endBound := frame.Bounds.End
	exclude := frame.Exclusion != execinfrapb.WindowerSpec_Frame_NO_EXCLUSION
	switch frame.Mode {
	// {{range .}}
	case _FRAME_MODE:
		switch startBound.BoundType {
		// {{range .StartBoundTypes}}
		case _START_BOUND:
			switch endBound.BoundType {
			// {{range .EndBoundTypes}}
			case _END_BOUND:
				switch exclude {
				// {{range .ExcludeInfos}}
				case _EXCLUDES_ROWS:
					op := &_OP_STRING{
						windowFramerBase: windowFramerBase{
							peersColIdx: peersColIdx,
							ordColIdx:   tree.NoColumnIdx,
							exclusion:   frame.Exclusion,
						},
					}
					op.handleOffsets(evalCtx, frame, ordering, inputTypes)
					return op
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
// operators. Window framer operators keep track of the start and end bounds of
// the window frame for the current row. The operators use the window frame for
// the previous row as a starting point for calculating the window frame for the
// current row. This takes advantage of the fact that window frame bounds are
// always non-decreasing from row to row.
//
// Consider the case of ROWS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING. The
// start of the frame will always be the first row, and the end will be three
// rows after the current one, which will always advance as the current row is
// advanced. Similar logic holds for the other framing modes and bounds.
// Excluded rows have the same non-decreasing property, and so are handled
// similarly.
type windowFramerBase struct {
	// storedCols stores the columns for which any row in the partition may be
	// accessed during evaluation of the window frame. The window framer operators
	// are not responsible for handling the SpillingBuffer other than to release
	// the reference after close is called.
	storedCols *colexecutils.SpillingBuffer

	partitionSize int // The total number of tuples in the partition.
	currentRow    int // The index of the current row.

	// currentGroup is the index of the current peer group, used by GROUPS mode.
	// It is only used when one or both bounds is of type OFFSET PRECEDING, to
	// ensure that the frame boundaries aren't advanced until the current row is
	// at least OFFSET groups beyond the start of the partition.
	currentGroup int

	startIdx int // Inclusive start of the window frame ignoring exclusion.
	endIdx   int // Exclusive end of the window frame ignoring exclusion.

	excludeStartIdx int // Inclusive start of rows specified by EXCLUDE clause.
	excludeEndIdx   int // Exclusive end of rows specified by EXCLUDE clause

	peersColIdx int // Indicates the beginning of each peer group. Can be unset.
	ordColIdx   int // The single ordering column for RANGE mode with offset.

	// startOffset and endOffset store the integer offsets for ROWS and GROUPS
	// modes with OFFSET PRECEDING or OFFSET FOLLOWING.
	startOffset, endOffset int

	// startHandler and endHandler are used to calculate the start and end indexes
	// in RANGE mode with OFFSET PRECEDING or OFFSET FOLLOWING.
	startHandler, endHandler rangeOffsetHandler

	// datumAlloc is used to decode the offsets in RANGE mode. It is initialized
	// lazily.
	datumAlloc *tree.DatumAlloc

	exclusion execinfrapb.WindowerSpec_Frame_Exclusion

	// intervals is a small (at most length 3) slice that is used during
	// aggregation computation.
	intervals       []windowInterval
	intervalsAreSet bool

	// prevIntervals, toAdd, and toRemove are used to calculate the intervals
	// for calculating aggregate window functions using the sliding window
	// optimization.
	prevIntervals, toAdd, toRemove []windowInterval
}

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
		return -1
	}
	return idx
}

// frameIntervals returns a series of intervals that describes the set of all
// rows that are part of the frame for the current row. Note that there are at
// most three intervals - this case can occur when EXCLUDE TIES is used.
// frameIntervals is used to compute aggregate functions over a window.
func (b *windowFramerBase) frameIntervals() []windowInterval {
	if b.intervalsAreSet {
		return b.intervals
	}
	b.intervalsAreSet = true
	b.intervals = b.intervals[:0]
	if b.startIdx >= b.endIdx {
		return b.intervals
	}
	b.intervals = append(b.intervals, windowInterval{start: b.startIdx, end: b.endIdx})
	return b.intervals
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
	if b.ordColIdx != tree.NoColumnIdx {
		b.ordColIdx = storeCol(b.ordColIdx)
	}
	return colsToStore
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
	b.intervals = b.intervals[:0]
	b.prevIntervals = b.prevIntervals[:0]
	b.toAdd = b.toAdd[:0]
	b.toRemove = b.toRemove[:0]
}

// incrementPeerGroup increments the given index by 'groups' peer groups,
// returning the resulting index. If the given offset is greater than the
// remaining number of groups, the returned index will be equal to the size of
// the partition.
func (b *windowFramerBase) incrementPeerGroup(ctx context.Context, index, groups int) int {
	if groups <= 0 {
		return index
	}
	if b.peersColIdx == tree.NoColumnIdx || index >= b.partitionSize {
		// With no ORDER BY clause, the entire partition is a single peer group.
		return b.partitionSize
	}
	// We have to iterate to the beginning of the next peer group.
	index++
	for {
		if index >= b.partitionSize {
			return b.partitionSize
		}
		vec, vecIdx, n := b.storedCols.GetVecWithTuple(ctx, b.peersColIdx, index)
		peersCol := vec.Bool()
		_, _ = peersCol[vecIdx], peersCol[n-1]
		for ; vecIdx < n; vecIdx++ {
			//gcassert:bce
			if peersCol[vecIdx] {
				// We have reached the start of the next peer group (the end of the
				// current one).
				groups--
				if groups <= 0 {
					return index
				}
			}
			index++
		}
	}
}

// isFirstPeer returns true if the row at the given index into the current
// partition is the first in its peer group.
func (b *windowFramerBase) isFirstPeer(ctx context.Context, idx int) bool {
	if idx == 0 {
		// The first row in the partition is always the first in its peer group.
		return true
	}
	if b.peersColIdx == tree.NoColumnIdx {
		// All rows are peers, so only the first is the first in its peer group.
		return false
	}
	vec, vecIdx, _ := b.storedCols.GetVecWithTuple(ctx, b.peersColIdx, idx)
	return vec.Bool()[vecIdx]
}

// handleOffsets populates the offset fields of the window framer operator, if
// one or both bounds are OFFSET PRECEDING or OFFSET FOLLOWING.
func (b *windowFramerBase) handleOffsets(
	evalCtx *eval.Context,
	frame *execinfrapb.WindowerSpec_Frame,
	ordering *execinfrapb.Ordering,
	inputTypes []*types.T,
) {
	startBound, endBound := &frame.Bounds.Start, frame.Bounds.End
	startHasOffset := startBound.BoundType == execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING ||
		startBound.BoundType == execinfrapb.WindowerSpec_Frame_OFFSET_FOLLOWING
	endHasOffset := endBound.BoundType == execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING ||
		endBound.BoundType == execinfrapb.WindowerSpec_Frame_OFFSET_FOLLOWING
	if !(startHasOffset || endHasOffset) {
		return
	}

	if frame.Mode != execinfrapb.WindowerSpec_Frame_RANGE {
		// For ROWS or GROUPS mode, the offset is of type int64 and the ordering
		// column(s) is not stored.
		if startHasOffset {
			b.startOffset = int(startBound.IntOffset)
		}
		if endHasOffset {
			b.endOffset = int(endBound.IntOffset)
		}
		return
	}

	// For RANGE mode with an offset, there is a single ordering column. The
	// offset type depends on the ordering column type. In addition, the ordering
	// column must be stored.
	if len(ordering.Columns) != 1 {
		colexecerror.InternalError(
			errors.AssertionFailedf("expected exactly one ordering column for RANGE mode with offset"))
	}
	// Only initialize the DatumAlloc field when we know we will need it.
	b.datumAlloc = &tree.DatumAlloc{}
	b.ordColIdx = int(ordering.Columns[0].ColIdx)
	ordColType := inputTypes[b.ordColIdx]
	ordColAsc := ordering.Columns[0].Direction == execinfrapb.Ordering_Column_ASC
	if startHasOffset {
		b.startHandler = newRangeOffsetHandler(
			evalCtx, b.datumAlloc, startBound, ordColType, ordColAsc, true, /* isStart */
		)
	}
	if endHasOffset {
		b.endHandler = newRangeOffsetHandler(
			evalCtx, b.datumAlloc, endBound, ordColType, ordColAsc, false, /* isStart */
		)
	}
}

// handleExcludeForNext updates the start and end indices for rows excluded by
// the window frame's exclusion clause.
func (b *windowFramerBase) handleExcludeForNext(ctx context.Context, currRowIsGroupStart bool) {
	if b.excludeCurrRow() {
		b.excludeStartIdx = b.currentRow
		b.excludeEndIdx = b.excludeStartIdx + 1
	} else {
		// Note that EXCLUDE GROUP and EXCLUDE TIES are handled in the same way here
		// because they have the same start and end bounds. EXCLUDE TIES does not
		// actually exclude the current row, but that is handled later (by
		// handleExcludeForFirstIdx, handleExcludeForLastIdx, etc.).
		if currRowIsGroupStart {
			// Only update the exclude indices upon entering a new peer group.
			b.excludeStartIdx = b.excludeEndIdx
			b.excludeEndIdx = b.incrementPeerGroup(ctx, b.excludeEndIdx, 1 /* groups */)
		}
	}
}

// handleExcludeForFirstIdx adjusts the given 'first index' to account for the
// exclusion clause.
func (b *windowFramerBase) handleExcludeForFirstIdx(idx int) int {
	if idx == -1 {
		return idx
	}
	// Note that EXCLUDE GROUP and EXCLUDE CURRENT ROW are handled the same way
	// here because they can both be described simply by the interval
	// [excludeStartIdx, excludeEndIdx).
	if b.excludeStartIdx <= idx && b.excludeEndIdx > idx {
		if b.excludeTies() && b.currentRow >= b.startIdx && b.currentRow < b.endIdx {
			return b.currentRow
		}
		if b.excludeEndIdx >= b.endIdx {
			// All rows are excluded.
			return -1
		}
		return b.excludeEndIdx
	}
	return idx
}

// handleExcludeForLastIdx adjusts the given 'last index' to account for the
// exclusion clause.
func (b *windowFramerBase) handleExcludeForLastIdx(idx int) int {
	if idx == -1 {
		return idx
	}
	// Note that EXCLUDE GROUP and EXCLUDE CURRENT ROW are handled the same way
	// here because they can both be described simply by the interval
	// [excludeStartIdx, excludeEndIdx).
	if b.excludeStartIdx <= idx && b.excludeEndIdx > idx {
		if b.excludeTies() && b.currentRow >= b.startIdx && b.currentRow < b.endIdx {
			return b.currentRow
		}
		if b.excludeStartIdx <= b.startIdx {
			// All rows are excluded.
			return -1
		}
		return b.excludeStartIdx - 1
	}
	return idx
}

// handleExcludeForNthIdx adjusts the given 'nth index' to account for the
// exclusion clause.
func (b *windowFramerBase) handleExcludeForNthIdx(idx int) int {
	if idx == -1 {
		return idx
	}
	// Retrieve the rows that are actually excluded - those that are within
	// [startIdx, endIdx) in addition to being specified by the EXCLUDE clause.
	// Note that EXCLUDE GROUP and EXCLUDE CURRENT ROW are handled the same way
	// here because they can both be described simply by the interval
	// [excludeStartIdx, excludeEndIdx).
	excludedRowsStart := b.excludeStartIdx
	if excludedRowsStart < b.startIdx {
		excludedRowsStart = b.startIdx
	}
	excludedRowsEnd := b.excludeEndIdx
	if excludedRowsEnd > b.endIdx {
		excludedRowsEnd = b.endIdx
	}
	if excludedRowsStart < excludedRowsEnd && idx >= excludedRowsStart {
		if b.excludeTies() && b.currentRow >= b.startIdx && b.currentRow < b.endIdx {
			if idx == excludedRowsStart {
				return b.currentRow
			}
			idx--
		}
		idx += excludedRowsEnd - excludedRowsStart
		if idx >= b.endIdx {
			// The nth index doesn't exist.
			return -1
		}
	}
	return idx
}

func (b *windowFramerBase) excludeCurrRow() bool {
	return b.exclusion == execinfrapb.WindowerSpec_Frame_EXCLUDE_CURRENT_ROW
}

func (b *windowFramerBase) excludeTies() bool {
	return b.exclusion == execinfrapb.WindowerSpec_Frame_EXCLUDE_TIES
}

// getSlidingWindowIntervals is a helper function used to calculate the sets of
// rows that are a part of the current window frame, but not the previous one,
// and rows that were a part of the previous window frame, but not the current
// one. getSlidingWindowIntervals expects the intervals stored in currIntervals
// and prevIntervals to be non-overlapping and increasing, and guarantees the
// same invariants for the output intervals.
func getSlidingWindowIntervals(
	currIntervals, prevIntervals, toAdd, toRemove []windowInterval,
) ([]windowInterval, []windowInterval) {
	toAdd, toRemove = toAdd[:0], toRemove[:0]
	var prevIdx, currIdx int
	var prev, curr windowInterval
	setPrev, setCurr := true, true
	for {
		// We need to find the set difference currIntervals \ prevIntervals (toAdd)
		// and the set difference prevIntervals \ currIntervals (toRemove). To do
		// this, take advantage of the fact that both sets of intervals are in
		// ascending order, similar to merging sorted lists. Maintain indices into
		// each list, and iterate whichever index has the 'smaller' interval
		// (e.g. whichever ends first). The portions of the intervals that overlap
		// are ignored, while those that don't are added to one of the 'toAdd' and
		// 'toRemove' sets.
		if prevIdx >= len(prevIntervals) {
			// None of the remaining intervals in the current frame were part of the
			// previous frame.
			if !setCurr {
				// The remaining interval stored in curr still hasn't been handled.
				toAdd = append(toAdd, curr)
				currIdx++
			}
			if currIdx < len(currIntervals) {
				toAdd = append(toAdd, currIntervals[currIdx:]...)
			}
			break
		}
		if currIdx >= len(currIntervals) {
			// None of the remaining intervals in the previous frame are part of the
			// current frame.
			if !setPrev {
				// The remaining interval stored in prev still hasn't been handled.
				toRemove = append(toRemove, prev)
				prevIdx++
			}
			if prevIdx < len(prevIntervals) {
				toRemove = append(toRemove, prevIntervals[prevIdx:]...)
			}
			break
		}
		if setPrev {
			prev = prevIntervals[prevIdx]
			setPrev = false
		}
		if setCurr {
			curr = currIntervals[currIdx]
			setCurr = false
		}
		if prev == curr {
			// This interval has not changed from the previous frame.
			prevIdx++
			currIdx++
			setPrev, setCurr = true, true
			continue
		}
		if prev.start >= curr.end {
			// The intervals do not overlap, and the curr interval did not exist in
			// the previous window frame.
			toAdd = append(toAdd, curr)
			currIdx++
			setCurr = true
			continue
		}
		if curr.start >= prev.end {
			// The intervals do not overlap, and the prev interval existed in the
			// previous window frame, but not the current one.
			toRemove = append(toRemove, prev)
			prevIdx++
			setPrev = true
			continue
		}
		// The intervals overlap but are not equal.
		if curr.start < prev.start {
			// curr starts before prev. Add the prefix of curr to 'toAdd'. Advance the
			// start of curr to the start of prev to reflect that the prefix has
			// already been processed.
			toAdd = append(toAdd, windowInterval{start: curr.start, end: prev.start})
			curr.start = prev.start
		} else if prev.start < curr.start {
			// prev starts before curr. Add the prefix of prev to 'toRemove'. Advance
			// the start of prev to the start of curr to reflect that the prefix has
			// already been processed.
			toRemove = append(toRemove, windowInterval{start: prev.start, end: curr.start})
			prev.start = curr.start
		}
		if curr.end > prev.end {
			// prev ends before curr. Set the start of curr to the end of prev to
			// indicate that prev has been processed.
			curr.start = prev.end
			prevIdx++
			setPrev = true
		} else if prev.end > curr.end {
			// curr ends before prev. Set the start of prev to the end of curr to
			// indicate that curr has been processed.
			prev.start = curr.end
			currIdx++
			setCurr = true
		} else {
			// prev and curr end at the same index. The prefix of whichever one starts
			// first has already been handled.
			prevIdx++
			currIdx++
			setPrev, setCurr = true, true
		}
	}
	return toAdd, toRemove
}

// {{range .}}
// {{range .StartBoundTypes}}
// {{range .EndBoundTypes}}
// {{range .ExcludeInfos}}

type _OP_STRING struct {
	windowFramerBase
}

var _ windowFramer = &_OP_STRING{}

// startPartition prepares the window framer to begin iterating through a new
// partition. It must be called before calling next.
func (f *_OP_STRING) startPartition(
	ctx context.Context, partitionSize int, storedCols *colexecutils.SpillingBuffer,
) {
	f.windowFramerBase.startPartition(partitionSize, storedCols)
	// {{if and .GroupsMode .OffsetPreceding}}
	f.currentGroup = 0
	// {{end}}
	// {{/*
	// In GROUPS mode, 'offset' peer groups need to be processed upfront because
	// they are within the frame of the first row when one or both bounds are of
	// type OFFSET FOLLOWING.
	// */}}
	// {{if and .GroupsMode .StartOffsetFollowing}}
	// f.startIdx must be advanced by 'startOffset' peer groups.
	f.startIdx = f.incrementPeerGroup(ctx, f.startIdx, f.startOffset)
	// {{end}}
	// {{if and .GroupsMode .EndOffsetFollowing}}
	// f.endIdx must be advanced by 'endOffset' peer groups.
	f.endIdx = f.incrementPeerGroup(ctx, f.endIdx, f.endOffset)
	// {{end}}
	// {{/*
	// In RANGE mode, rows which are within 'offset' logical units of the current
	// row on the ordering column are within the frame of the first row, so they
	// need to be processed up front.
	// */}}
	// {{if and .RangeMode .StartHasOffset}}
	f.startHandler.startPartition(storedCols, f.peersColIdx, f.ordColIdx)
	// {{end}}
	// {{if and .RangeMode .EndHasOffset}}
	f.endHandler.startPartition(storedCols, f.peersColIdx, f.ordColIdx)
	// {{end}}
	// {{if .Exclude}}
	f.excludeStartIdx = 0
	f.excludeEndIdx = 0
	// {{end}}
}

// next is called for each row in the partition. It advances to the next row and
// then calculates the window frame for the current row. next should not be
// called beyond the end of the partition, or undefined behavior may result.
func (f *_OP_STRING) next(ctx context.Context) {
	f.currentRow++
	// {{if or (and (or .GroupsMode .RangeMode) (not .BothUnbounded)) .Exclude}}
	// {{/*
	// We need to keep track of whether the current row is the first of its peer
	// group in GROUPS or RANGE mode when one of the bounds is not a variant of
	// UNBOUNDED. The information is used to trigger updates on the start and end
	// indexes whenever the current row has advanced to a new peer group.
	// EXCLUDE GROUP and EXCLUDE TIES also require this information regardless of
	// the frame mode and bounds.
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
	// {{else if .RangeMode}}
	if currRowIsGroupStart {
		f.startIdx = f.startHandler.getIdx(ctx, f.currentRow, f.startIdx)
	}
	// {{end}}
	// {{end}}
	// {{if .StartCurrentRow}}
	// {{if .RowsMode}}
	f.startIdx = f.currentRow
	// {{else if or .GroupsMode .RangeMode}}
	if currRowIsGroupStart {
		f.startIdx = f.currentRow
	}
	// {{end}}
	// {{end}}
	// {{if .StartOffsetFollowing}}
	// {{if .RowsMode}}
	f.startIdx = f.currentRow + f.startOffset
	if f.startIdx > f.partitionSize || f.startOffset >= f.partitionSize {
		// The second part of the condition protects us from an integer
		// overflow when offset is very large.
		f.startIdx = f.partitionSize
	}
	// {{else if .GroupsMode}}
	if currRowIsGroupStart && f.currentRow > 0 {
		// The start index will already have been advanced by f.startOffset peer
		// groups, so we only need to advance to the next adjacent peers group
		// whenever the currentRow pointer enters a new peers group.
		f.startIdx = f.incrementPeerGroup(ctx, f.startIdx, 1 /* groups */)
	}
	// {{else if .RangeMode}}
	if currRowIsGroupStart {
		f.startIdx = f.startHandler.getIdx(ctx, f.currentRow, f.startIdx)
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
		// Advance the end index to the start of the next peer group.
		f.endIdx = f.incrementPeerGroup(ctx, f.endIdx, 1 /* groups */)
	}
	// {{else if .RangeMode}}
	if currRowIsGroupStart {
		f.endIdx = f.endHandler.getIdx(ctx, f.currentRow, f.endIdx)
	}
	// {{end}}
	// {{end}}
	// {{if .EndCurrentRow}}
	// {{if .RowsMode}}
	f.endIdx = f.currentRow + 1
	// {{else if or .GroupsMode .RangeMode}}
	if currRowIsGroupStart {
		f.endIdx = f.incrementPeerGroup(ctx, f.endIdx, 1 /* groups */)
	}
	// {{end}}
	// {{end}}
	// {{if .EndOffsetFollowing}}
	// {{if .RowsMode}}
	f.endIdx = f.currentRow + f.endOffset + 1
	if f.endIdx > f.partitionSize || f.endOffset >= f.partitionSize {
		// The second part of the condition protects us from an integer
		// overflow when offset is very large.
		f.endIdx = f.partitionSize
	}
	// {{else if .GroupsMode}}
	if currRowIsGroupStart {
		// The end index will already have been advanced by f.startOffset peer
		// groups, so we only need to advance to the next adjacent peers group
		// whenever the currentRow pointer enters a new peers group.
		f.endIdx = f.incrementPeerGroup(ctx, f.endIdx, 1 /* groups */)
	}
	// {{else if .RangeMode}}
	if currRowIsGroupStart {
		f.endIdx = f.endHandler.getIdx(ctx, f.currentRow, f.endIdx)
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
	// {{if .Exclude}}
	// Handle exclusion clause.
	f.handleExcludeForNext(ctx, currRowIsGroupStart)
	// {{end}}
	f.prevIntervals = append(f.prevIntervals[:0], f.intervals...)
	f.intervalsAreSet = false
}

func (f *_OP_STRING) close() {
	// {{if and .RangeMode .StartHasOffset}}
	f.startHandler.close()
	// {{end}}
	// {{if and .RangeMode .EndHasOffset}}
	f.endHandler.close()
	// {{end}}
	*f = _OP_STRING{}
}

// slidingWindowIntervals returns a pair of interval sets that describes the
// rows that should be added to the current aggregation, and those which
// should be removed from the current aggregation. It is used to implement the
// sliding window optimization for aggregate window functions.
func (f *_OP_STRING) slidingWindowIntervals() (toAdd, toRemove []windowInterval) {
	f.toAdd, f.toRemove = f.toAdd[:0], f.toRemove[:0]
	f.frameIntervals()
	f.toAdd, f.toRemove = getSlidingWindowIntervals(f.intervals, f.prevIntervals, f.toAdd, f.toRemove)
	return f.toAdd, f.toRemove
}

// {{if .Exclude}}

// frameFirstIdx returns the index of the first row in the window frame for
// the current row. If no such row exists, frameFirstIdx returns -1.
func (f *_OP_STRING) frameFirstIdx() (idx int) {
	idx = f.windowFramerBase.frameFirstIdx()
	return f.handleExcludeForFirstIdx(idx)
}

// frameLastIdx returns the index of the last row in the window frame for
// the current row. If no such row exists, frameLastIdx returns -1.
func (f *_OP_STRING) frameLastIdx() (idx int) {
	idx = f.windowFramerBase.frameLastIdx()
	return f.handleExcludeForLastIdx(idx)
}

// frameNthIdx returns the index of the nth row (starting from one) in the
// window frame for the current row. If no such row exists, frameNthIdx
// returns -1.
func (f *_OP_STRING) frameNthIdx(n int) (idx int) {
	idx = f.windowFramerBase.frameNthIdx(n)
	return f.handleExcludeForNthIdx(idx)
}

// frameIntervals returns a series of intervals that describes the set of all
// rows that are part of the frame for the current row. Note that there are at
// most three intervals - this case can occur when EXCLUDE TIES is used.
// frameIntervals is used to compute aggregate functions over a window.
func (f *_OP_STRING) frameIntervals() []windowInterval {
	if f.startIdx >= f.endIdx {
		f.intervals = f.intervals[:0]
		return f.intervals
	}
	if f.excludeStartIdx >= f.endIdx || f.excludeEndIdx <= f.startIdx {
		// No rows excluded.
		return f.windowFramerBase.frameIntervals()
	}
	f.intervals = f.intervals[:0]
	if f.excludeStartIdx > f.startIdx {
		f.intervals = append(f.intervals, windowInterval{start: f.startIdx, end: f.excludeStartIdx})
	}
	if f.excludeTies() && f.currentRow >= f.startIdx && f.currentRow < f.endIdx {
		f.intervals = append(f.intervals, windowInterval{start: f.currentRow, end: f.currentRow + 1})
	}
	if f.excludeEndIdx < f.endIdx {
		f.intervals = append(f.intervals, windowInterval{start: f.excludeEndIdx, end: f.endIdx})
	}
	return f.intervals
}

// {{end}}
// {{end}}
// {{end}}
// {{end}}
// {{end}}
