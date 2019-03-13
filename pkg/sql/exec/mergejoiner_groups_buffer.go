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

type groupsBuffer struct {
	bufferStartIdx     int
	bufferEndIdx       int
	bufferCap          int
	bufferEndIdxForCol int

	leftGroups        []group
	rightGroups       []group
	leftReturnGroups  []group
	rightReturnGroups []group
}

func newGroupsBuffer(bufferCap int) groupsBuffer {
	b := groupsBuffer{
		bufferCap:         bufferCap,
		leftGroups:        make([]group, bufferCap),
		rightGroups:       make([]group, bufferCap),
		leftReturnGroups:  make([]group, bufferCap),
		rightReturnGroups: make([]group, bufferCap),
	}
	return b
}

func (b *groupsBuffer) reset(lIdx int, lLength int, rIdx int, rLength int) {
	b.bufferStartIdx = 0
	b.bufferEndIdx = 1
	b.bufferEndIdxForCol = 1

	b.leftGroups[0] = group{lIdx, lLength, 1}
	b.rightGroups[0] = group{rIdx, rLength, 1}
}

func (b *groupsBuffer) nextGroupInCol(lGroup *group, rGroup *group) bool {
	if b.bufferStartIdx >= b.bufferEndIdxForCol {
		return false
	}
	idx := b.bufferStartIdx % b.bufferCap
	b.bufferStartIdx++

	if b.bufferStartIdx >= b.bufferCap {
		b.bufferStartIdx -= b.bufferCap
	}

	*lGroup = b.leftGroups[idx]
	*rGroup = b.rightGroups[idx]
	return true
}

func (b *groupsBuffer) addGroupsToNextCol(
	curLIdx int, lRunLength int, curRIdx int, rRunLength int,
) {
	b.leftGroups[b.bufferEndIdx] = group{curLIdx, curLIdx + lRunLength, rRunLength}
	b.rightGroups[b.bufferEndIdx] = group{curRIdx, curRIdx + rRunLength, lRunLength}
	b.bufferEndIdx++

	if b.bufferEndIdx >= b.bufferCap {
		b.bufferEndIdx -= b.bufferCap
	}
}

func (b *groupsBuffer) finishedCol() {
	b.bufferStartIdx = b.bufferEndIdxForCol
	b.bufferEndIdxForCol = b.bufferEndIdx
}

func (b *groupsBuffer) getLGroups() []group {
	return b.getGroups(b.leftGroups, b.leftReturnGroups)
}

func (b *groupsBuffer) getRGroups() []group {
	return b.getGroups(b.rightGroups, b.rightReturnGroups)
}

func (b *groupsBuffer) getGroups(groups []group, returnGroups []group) []group {
	startIdx := b.bufferStartIdx % b.bufferCap
	endIdx := b.bufferEndIdx % b.bufferCap

	if endIdx < startIdx {
		copy(returnGroups, groups[startIdx:b.bufferCap])
		start := b.bufferCap - startIdx
		copy(returnGroups[start:], groups[:endIdx])
		return returnGroups
	}

	return groups[startIdx:endIdx]
}

func (b *groupsBuffer) getBufferLen() int {
	return b.bufferEndIdx - b.bufferStartIdx
}
