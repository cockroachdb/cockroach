// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
)

// circularGroupsBuffer is a struct designed to store the groups' slices for a
// given column. We know that there is a maximum number of possible groups per
// batch, so we can cap the buffer and make it circular.
type circularGroupsBuffer struct {
	// startIdx indicates which index the next group to be processed is stored
	// at.
	startIdx int
	// endIdx indicates the first empty slot within the buffer.
	endIdx int
	// nextColStartIdx is the index of the first group that belongs to the next
	// column.
	nextColStartIdx int
	cap             int

	leftGroups  []group
	rightGroups []group
}

func makeGroupsBuffer(cap int) circularGroupsBuffer {
	return circularGroupsBuffer{
		cap: cap,
		// Allocate twice the amount of space needed so that no additional
		// allocations are needed to make the resulting slice contiguous.
		leftGroups:  make([]group, cap*2),
		rightGroups: make([]group, cap*2),
	}
}

// reset sets the circular buffer state to groups that produce the maximal
// cross product, i.e. one maximal group on each side.
func (b *circularGroupsBuffer) reset(lStartIdx int, lEndIdx int, rStartIdx int, rEndIdx int) {
	b.startIdx = 0
	b.endIdx = 1
	b.nextColStartIdx = 1

	b.leftGroups[0] = group{lStartIdx, lEndIdx, 1, 0, false, false}
	b.rightGroups[0] = group{rStartIdx, rEndIdx, 1, 0, false, false}
}

// nextGroupInCol returns whether or not there exists a next group in the
// current column, and sets the parameters to be the left and right groups
// corresponding to the next values in the buffer.
func (b *circularGroupsBuffer) nextGroupInCol(lGroup *group, rGroup *group) bool {
	if b.startIdx == b.nextColStartIdx {
		return false
	}
	idx := b.startIdx
	b.startIdx++

	if b.startIdx >= b.cap {
		b.startIdx -= b.cap
	}

	*lGroup = b.leftGroups[idx]
	*rGroup = b.rightGroups[idx]
	return true
}

// isLastGroupInCol returns whether the last group obtained via nextGroupInCol
// from the buffer is the last one for the column.
func (b *circularGroupsBuffer) isLastGroupInCol() bool {
	return b.startIdx == b.nextColStartIdx
}

// addGroupsToNextCol appends a left and right group to the buffer. In an
// iteration of a column, these values are either processed in the next
// equality column or used to build the cross product.
func (b *circularGroupsBuffer) addGroupsToNextCol(
	curLIdx int, lRunLength int, curRIdx int, rRunLength int,
) {
	b.leftGroups[b.endIdx] = group{
		rowStartIdx: curLIdx,
		rowEndIdx:   curLIdx + lRunLength,
		numRepeats:  rRunLength,
		toBuild:     lRunLength * rRunLength,
	}
	b.rightGroups[b.endIdx] = group{
		rowStartIdx: curRIdx,
		rowEndIdx:   curRIdx + rRunLength,
		numRepeats:  lRunLength,
		toBuild:     lRunLength * rRunLength,
	}
	b.endIdx++

	// Modulus on every step is more expensive than this check.
	if b.endIdx >= b.cap {
		b.endIdx -= b.cap
	}
}

// addLeftUnmatchedGroup adds a left and right group to the buffer that
// correspond to an unmatched row from the left side in the case of LEFT OUTER
// JOIN or LEFT ANTI JOIN.
func (b *circularGroupsBuffer) addLeftUnmatchedGroup(curLIdx int, curRIdx int) {
	b.leftGroups[b.endIdx] = group{
		rowStartIdx: curLIdx,
		rowEndIdx:   curLIdx + 1,
		numRepeats:  1,
		toBuild:     1,
		nullGroup:   false,
		unmatched:   true,
	}
	b.rightGroups[b.endIdx] = group{
		rowStartIdx: curRIdx,
		rowEndIdx:   curRIdx + 1,
		numRepeats:  1,
		toBuild:     1,
		nullGroup:   true,
		unmatched:   false,
	}
	b.endIdx++

	// Modulus on every step is more expensive than this check.
	if b.endIdx >= b.cap {
		b.endIdx -= b.cap
	}
}

// addRightOuterGroup adds a left and right group to the buffer that correspond
// to an unmatched row from the right side in the case of RIGHT OUTER JOIN.
func (b *circularGroupsBuffer) addRightOuterGroup(curLIdx int, curRIdx int) {
	b.leftGroups[b.endIdx] = group{
		rowStartIdx: curLIdx,
		rowEndIdx:   curLIdx + 1,
		numRepeats:  1,
		toBuild:     1,
		nullGroup:   true,
		unmatched:   false,
	}
	b.rightGroups[b.endIdx] = group{
		rowStartIdx: curRIdx,
		rowEndIdx:   curRIdx + 1,
		numRepeats:  1,
		toBuild:     1,
		nullGroup:   false,
		unmatched:   true,
	}
	b.endIdx++

	// Modulus on every step is more expensive than this check.
	if b.endIdx >= b.cap {
		b.endIdx -= b.cap
	}
}

// addLeftSemiGroup adds a left group to the buffer that corresponds to a run
// of tuples from the left side that all have a match on the right side. This
// should only be called after processing the last equality column, and this
// group will be used by the builder next. Note that we're not adding a right
// group here since tuples from the right are not outputted in LEFT SEMI JOIN.
func (b *circularGroupsBuffer) addLeftSemiGroup(curLIdx int, lRunLength int) {
	b.leftGroups[b.endIdx] = group{
		rowStartIdx: curLIdx,
		rowEndIdx:   curLIdx + lRunLength,
		numRepeats:  1,
		toBuild:     lRunLength,
	}
	b.endIdx++

	// Modulus on every step is more expensive than this check.
	if b.endIdx >= b.cap {
		b.endIdx -= b.cap
	}
}

// finishedCol is used to notify the circular buffer to update the indices
// representing the "window" of available values for the next column.
func (b *circularGroupsBuffer) finishedCol() {
	b.startIdx = b.nextColStartIdx
	b.nextColStartIdx = b.endIdx
}

// getGroups returns left and right slices of groups that are contiguous, which
// is a useful simplification for the build phase.
func (b *circularGroupsBuffer) getGroups() ([]group, []group) {
	startIdx := b.startIdx
	endIdx := b.endIdx
	leftGroups, rightGroups := b.leftGroups, b.rightGroups

	if endIdx < startIdx {
		copy(leftGroups[b.cap:], leftGroups[:endIdx])
		copy(rightGroups[b.cap:], rightGroups[:endIdx])
		endIdx += b.cap
	}

	return leftGroups[startIdx:endIdx], rightGroups[startIdx:endIdx]
}

func newMJBufferedGroup(types []coltypes.T) *mjBufferedGroup {
	bg := &mjBufferedGroup{
		colVecs: make([]coldata.Vec, len(types)),
	}
	for i, t := range types {
		bg.colVecs[i] = coldata.NewMemColumn(t, int(coldata.BatchSize()))
	}
	return bg
}

// mjBufferedGroup is a custom implementation of coldata.Batch interface (only
// a subset of methods is implemented) which stores the length as uint64. This
// allows for plugging it into the builder through the common interface.
type mjBufferedGroup struct {
	colVecs []coldata.Vec
	length  uint64
	// needToReset indicates whether the buffered group should be reset on the
	// call to reset().
	needToReset bool
}

var _ coldata.Batch = &mjBufferedGroup{}

func (bg *mjBufferedGroup) Length() uint16 {
	execerror.VectorizedInternalPanic("Length() should not be called on mjBufferedGroup; instead, " +
		"length field should be accessed directly")
	// This code is unreachable, but the compiler cannot infer that.
	return 0
}

func (bg *mjBufferedGroup) SetLength(uint16) {
	execerror.VectorizedInternalPanic("SetLength(uint16) should not be called on mjBufferedGroup;" +
		"instead, length field should be accessed directly")
}

func (bg *mjBufferedGroup) Width() int {
	return len(bg.colVecs)
}

func (bg *mjBufferedGroup) ColVec(i int) coldata.Vec {
	return bg.colVecs[i]
}

func (bg *mjBufferedGroup) ColVecs() []coldata.Vec {
	return bg.colVecs
}

// Selection is not implemented because the tuples should only be appended to
// mjBufferedGroup, and Append does the deselection step.
func (bg *mjBufferedGroup) Selection() []uint16 {
	return nil
}

// SetSelection is not implemented because the tuples should only be appended
// to mjBufferedGroup, and Append does the deselection step.
func (bg *mjBufferedGroup) SetSelection(bool) {
	execerror.VectorizedInternalPanic("SetSelection(bool) should not be called on mjBufferedGroup")
}

// AppendCol is not implemented because mjBufferedGroup is only initialized
// when the column schema is known.
func (bg *mjBufferedGroup) AppendCol(coltypes.T) {
	execerror.VectorizedInternalPanic("AppendCol(coltypes.T) should not be called on mjBufferedGroup")
}

// Reset is not implemented because mjBufferedGroup is not reused with
// different column schemas at the moment.
func (bg *mjBufferedGroup) Reset(types []coltypes.T, length int) {
	execerror.VectorizedInternalPanic("Reset([]coltypes.T, int) should not be called on mjBufferedGroup")
}

// ResetInternalBatch is not implemented because mjBufferedGroup is not meant
// to be used by any operator other than the merge joiner, and it should be
// using reset() instead.
func (bg *mjBufferedGroup) ResetInternalBatch() {
	execerror.VectorizedInternalPanic("ResetInternalBatch() should not be called on mjBufferedGroup")
}

// reset resets the state of the buffered group so that we can reuse the
// underlying memory.
func (bg *mjBufferedGroup) reset() {
	bg.length = 0
	bg.needToReset = false
	for _, colVec := range bg.colVecs {
		// We do not need to reset the column vectors because those will be just
		// written over, but we do need to reset the nulls.
		colVec.Nulls().UnsetNulls()
		if colVec.Type() == coltypes.Bytes {
			// Bytes type is the only exception to the comment above.
			colVec.Bytes().Reset()
		}
	}
}
