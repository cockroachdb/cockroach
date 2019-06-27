// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

// circularGroupsBuffer is a struct design to store the groups slice
// for a given column. We know that there is a maximum number of possible
// groups per batch, so we can cap the buffer and make it circular.
type circularGroupsBuffer struct {
	bufferStartIdx     int
	bufferEndIdx       int
	bufferCap          int
	bufferEndIdxForCol int

	leftGroups  []group
	rightGroups []group
}

func makeGroupsBuffer(bufferCap int) circularGroupsBuffer {
	return circularGroupsBuffer{
		bufferCap: bufferCap,
		// Allocate twice the amount of space needed so that no additional
		// allocations are needed to make the resulting slice contiguous.
		leftGroups:  make([]group, bufferCap*2),
		rightGroups: make([]group, bufferCap*2),
	}
}

// reset sets the circular buffer state to groups that produce the maximal
// cross product, ie one maximal group on each side.
func (b *circularGroupsBuffer) reset(lIdx int, lLength int, rIdx int, rLength int) {
	b.bufferStartIdx = 0
	b.bufferEndIdx = 1
	b.bufferEndIdxForCol = 1

	b.leftGroups[0] = group{lIdx, lLength, 1, 0, false, false}
	b.rightGroups[0] = group{rIdx, rLength, 1, 0, false, false}
}

// nextGroupInCol returns whether or not there exists a next group in the current
// column, and sets the parameters to be the left and right groups corresponding
// to the next values in the buffer.
func (b *circularGroupsBuffer) nextGroupInCol(lGroup *group, rGroup *group) bool {
	if b.bufferStartIdx == b.bufferEndIdxForCol {
		return false
	}
	idx := b.bufferStartIdx
	b.bufferStartIdx++

	if b.bufferStartIdx >= b.bufferCap {
		b.bufferStartIdx -= b.bufferCap
	}

	*lGroup = b.leftGroups[idx]
	*rGroup = b.rightGroups[idx]
	return true
}

// isLastGroupInCol returns whether the last group obtained via nextGroupInCol
// from the buffer is the last one for the column.
func (b *circularGroupsBuffer) isLastGroupInCol() bool {
	return b.bufferStartIdx == b.bufferEndIdxForCol
}

// addGroupsToNextCol appends a left and right group to the buffer. In an iteration
// of a column, these values are either processed in the next equality column, or
// used to build the cross product.
func (b *circularGroupsBuffer) addGroupsToNextCol(
	curLIdx int, lRunLength int, curRIdx int, rRunLength int,
) {
	b.leftGroups[b.bufferEndIdx] = group{
		rowStartIdx: curLIdx,
		rowEndIdx:   curLIdx + lRunLength,
		numRepeats:  rRunLength,
		toBuild:     lRunLength * rRunLength,
	}
	b.rightGroups[b.bufferEndIdx] = group{
		rowStartIdx: curRIdx,
		rowEndIdx:   curRIdx + rRunLength,
		numRepeats:  lRunLength,
		toBuild:     lRunLength * rRunLength,
	}
	b.bufferEndIdx++

	// Modulus on every step is more expensive than this check.
	if b.bufferEndIdx >= b.bufferCap {
		b.bufferEndIdx -= b.bufferCap
	}
}

// addLeftOuterGroup adds a left and right group to the buffer that correspond
// to an unmatched row from the left side in the case of LEFT OUTER JOIN.
func (b *circularGroupsBuffer) addLeftOuterGroup(curLIdx int, curRIdx int) {
	b.leftGroups[b.bufferEndIdx] = group{
		rowStartIdx: curLIdx,
		rowEndIdx:   curLIdx + 1,
		numRepeats:  1,
		toBuild:     1,
		nullGroup:   false,
		unmatched:   true,
	}
	b.rightGroups[b.bufferEndIdx] = group{
		rowStartIdx: curRIdx,
		rowEndIdx:   curRIdx + 1,
		numRepeats:  1,
		toBuild:     1,
		nullGroup:   true,
		unmatched:   false,
	}
	b.bufferEndIdx++

	// Modulus on every step is more expensive than this check.
	if b.bufferEndIdx >= b.bufferCap {
		b.bufferEndIdx -= b.bufferCap
	}
}

// finishedCol is used to notify the circular buffer to update the indices representing
// the "window" of available values for the next column.
func (b *circularGroupsBuffer) finishedCol() {
	b.bufferStartIdx = b.bufferEndIdxForCol
	b.bufferEndIdxForCol = b.bufferEndIdx
}

// getLGroups wraps getGroups for the groups on the left side.
func (b *circularGroupsBuffer) getLGroups() []group {
	return b.getGroups(b.leftGroups)
}

// getRGroups wraps getGroups for the groups on the right side.
func (b *circularGroupsBuffer) getRGroups() []group {
	return b.getGroups(b.rightGroups)
}

// getGroups returns a []group that is contiguous, which is a useful simplification
// for the build phase.
func (b *circularGroupsBuffer) getGroups(groups []group) []group {
	startIdx := b.bufferStartIdx
	endIdx := b.bufferEndIdx

	if endIdx < startIdx {
		copy(groups[b.bufferCap:], groups[:endIdx])
		endIdx += b.bufferCap
	}

	return groups[startIdx:endIdx]
}

// getBufferLen returns the length of the buffer, ie the length of the "window".
func (b *circularGroupsBuffer) getBufferLen() int {
	return (b.bufferEndIdx - b.bufferStartIdx + b.bufferCap) % b.bufferCap
}
