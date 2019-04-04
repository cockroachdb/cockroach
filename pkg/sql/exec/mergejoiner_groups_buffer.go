// Copyright 2019 The Cockroach Authors.
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

	b.leftGroups[0] = group{lIdx, lLength, 1, 0}
	b.rightGroups[0] = group{rIdx, rLength, 1, 0}
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
