// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecjoin

import "github.com/cockroachdb/cockroach/pkg/util"

// circularGroupsBuffer is a struct designed to store the groups' slices for a
// given column. It starts out small and will grow dynamically if necessary
// until pre-computed maximum capacity (since we know that there is a maximum
// number of possible groups per batch).
type circularGroupsBuffer struct {
	// startIdx indicates which index the next group to be processed is stored
	// at.
	startIdx int
	// endIdx indicates the first empty slot within the buffer.
	endIdx int
	// nextColStartIdx is the index of the first group that belongs to the next
	// column.
	nextColStartIdx int
	// numGroupsInBuffer tracks the number of groups that are currently in the
	// buffer which includes both of the current column being processed as well
	// as the next one.
	numGroupsInBuffer int
	// capacity is the limit on the number of groups that current
	// {left,right}Groups slices can support. numGroupsInBuffer will never reach
	// this number, instead, we'll allocate bigger slices and increase capacity
	// accordingly.
	capacity int
	// size is the number used to calculate the current values of capacity and
	// the groups slices' lengths. It will be doubling when reallocating.
	size int
	// maxSize determines the maximum value for size variable that we'll ever
	// need (determined by the batch size).
	maxSize int

	leftGroups  []group
	rightGroups []group
}

// groupsBufferInitialSize determines the size used in initial allocations of
// the slices of the circularGroupsBuffer.
var groupsBufferInitialSize = util.ConstantWithMetamorphicTestRange(
	"merge-joiner-groups-buffer",
	8,  /* defaultValue */
	1,  /* min */
	16, /* max */
)

func makeGroupsBuffer(batchSize int) circularGroupsBuffer {
	return circularGroupsBuffer{
		capacity:    getGroupsBufferCapacity(groupsBufferInitialSize),
		size:        groupsBufferInitialSize,
		maxSize:     batchSize,
		leftGroups:  make([]group, getGroupsSlicesLen(groupsBufferInitialSize)),
		rightGroups: make([]group, getGroupsSlicesLen(groupsBufferInitialSize)),
	}
}

// getGroupsBufferCapacity returns capacity value for circularGroupsBuffer for
// the given size.
//
// The maximum number of possible groups per batch is achieved with FULL OUTER
// JOIN when no rows have matches, so there will be exactly size x 2 groups.
// We add an additional element to the capacity in order to be able to
// distinguish between an "empty" (no groups) and a "full" (2*size groups)
// buffers.
func getGroupsBufferCapacity(size int) int {
	return 2*size + 1
}

// getGroupsSlicesLen returns the length of {left,right}Groups slices for
// circularGroupsBuffer for the given size.
//
// Since we have a circular buffer, it is possible for groups to wrap when
// capacity is reached. Consider an example when size = 3 and startIdx = 6 when
// maximum number of groups is present:
//   buffer = [1, 2, 3, 4, 5, x, 0]
//  (integers denote different groups and 'x' stands for a garbage).
// When getGroups() is called, for ease of usage we need to return the buffer
// "flattened out", and in order to reduce allocation, we actually reserve
// 4*size. In the example above we will copy the buffer as:
//   buffer = [1, 2, 3, 4, 5, x, 0, 1, 2, 3, 4, 5]
// and will return buffer[6:12] when getGroups is called.
// The calculations for why 4*size is sufficient:
// - the largest position in which the first group can be placed is 2*size
//   (capacity field enforces that)
// - the largest number of groups to copy from the "physical" start of the
//   buffer is 2*size-1
// - adding those two numbers we arrive at 4*size.
func getGroupsSlicesLen(size int) int {
	return 4 * size
}

// reset sets the circular buffer state to groups that produce the maximal
// cross product, i.e. one maximal group on each side.
func (b *circularGroupsBuffer) reset(lStartIdx int, lEndIdx int, rStartIdx int, rEndIdx int) {
	b.startIdx = 0
	b.endIdx = 1
	b.nextColStartIdx = 1
	b.numGroupsInBuffer = 1

	b.leftGroups[0] = group{lStartIdx, lEndIdx, 1, 0, false, false}
	b.rightGroups[0] = group{rStartIdx, rEndIdx, 1, 0, false, false}
}

func (b *circularGroupsBuffer) advanceStart() {
	b.numGroupsInBuffer--
	b.startIdx++
	// Modulus on every step is more expensive than this check.
	if b.startIdx >= b.capacity {
		b.startIdx -= b.capacity
	}
}

func (b *circularGroupsBuffer) advanceEnd() {
	b.numGroupsInBuffer++
	b.endIdx++
	// Modulus on every step is more expensive than this check.
	if b.endIdx >= b.capacity {
		b.endIdx -= b.capacity
	}
}

// nextGroupInCol returns whether or not there exists a next group in the
// current column, and sets the parameters to be the left and right groups
// corresponding to the next values in the buffer.
func (b *circularGroupsBuffer) nextGroupInCol(lGroup *group, rGroup *group) bool {
	if b.startIdx == b.nextColStartIdx {
		return false
	}
	*lGroup = b.leftGroups[b.startIdx]
	*rGroup = b.rightGroups[b.startIdx]
	b.advanceStart()
	return true
}

// isLastGroupInCol returns whether the last group obtained via nextGroupInCol
// from the buffer is the last one for the column.
func (b *circularGroupsBuffer) isLastGroupInCol() bool {
	return b.startIdx == b.nextColStartIdx
}

// ensureCapacityForNewGroup makes sure that groups slices have enough space to
// add another group to the buffer, reallocating the slices if necessary.
func (b *circularGroupsBuffer) ensureCapacityForNewGroup() {
	if b.size == b.maxSize || b.numGroupsInBuffer+1 < b.capacity {
		// We either have already allocated the slices with maximally possible
		// required capacity or we still have enough space for one more group.
		return
	}
	newSize := b.size * 2
	if newSize > b.maxSize {
		newSize = b.maxSize
	}
	newLeftGroups := make([]group, getGroupsSlicesLen(newSize))
	newRightGroups := make([]group, getGroupsSlicesLen(newSize))
	// Note that this if block is never reached when startIdx == endIdx (because
	// that would indicate an empty buffer and we would have enough capacity
	// given our initialization in makeGroupsBuffer).
	if b.startIdx <= b.endIdx {
		// Current groups are contiguous in the slices, so copying them over is
		// simple.
		copy(newLeftGroups, b.leftGroups[b.startIdx:b.endIdx])
		copy(newRightGroups, b.rightGroups[b.startIdx:b.endIdx])
		b.nextColStartIdx -= b.startIdx
	} else {
		// Current groups are wrapped after position b.capacity-1. Namely, if we
		// have size = 3, capacity = 7, we might have the following:
		//   buffer = [1, 2, 0', 1', 2', x, 0]                               (1)
		// where startIdx = 6, endIdx = 5, nextColStartIdx = 2, so we need to
		// copy over with the adjustments so that the resulting state is
		//   buffer = [0, 1, 2, 0', 1', 2', x]
		// where startIdx = 0, endIdx = 6, nextColStartIdx = 3.
		//
		// First, copy over the start of the buffer (which is currently at the
		// end of the old slices) into the beginning of the new slices. In the
		// example above, we're copying [0].
		copy(newLeftGroups, b.leftGroups[b.startIdx:b.capacity])
		copy(newRightGroups, b.rightGroups[b.startIdx:b.capacity])
		// If non-empty, copy over the end of the buffer (which is currently at
		// the beginning of the old slices). In the example above, we're copying
		// [1, 2, 0', 1', 2'].
		if b.endIdx > 0 {
			offset := b.capacity - b.startIdx
			copy(newLeftGroups[offset:], b.leftGroups[:b.endIdx])
			copy(newRightGroups[offset:], b.rightGroups[:b.endIdx])
		}
		// Now update b.nextColStartIdx. There are two cases:
		// 1. it was in the part we copied first. In such scenario we need to
		//    shift the index towards the beginning by the same offset as when
		//    we were copying (by b.startIdx).
		//    For example, consider the following, size = 3, capacity = 7
		//      buffer = [2', x, 0, 1, 2, 0', 1']
		//    with startIdx = 2, endIdx = 1, nextColStartIdx = 5. We need to
		//    decrement nextColStartIdx by 2.
		// 2. it was in the part we copied second. In such scenario we need to
		//    shift the index towards the end by the same offset as when we were
		//    copying (by b.capacity-b.startIdx). Consider the example (1)
		//    above: we need to increment nextColStartIdx by 1.
		if b.nextColStartIdx >= b.startIdx {
			b.nextColStartIdx -= b.startIdx
		} else {
			b.nextColStartIdx += b.capacity - b.startIdx
		}
	}
	b.startIdx = 0
	b.endIdx = b.numGroupsInBuffer
	b.capacity = getGroupsBufferCapacity(newSize)
	b.size = newSize
	b.leftGroups = newLeftGroups
	b.rightGroups = newRightGroups
}

// addGroupsToNextCol appends a left and right group to the buffer. In an
// iteration of a column, these values are either processed in the next
// equality column or used to build the cross product.
func (b *circularGroupsBuffer) addGroupsToNextCol(
	curLIdx int, lRunLength int, curRIdx int, rRunLength int,
) {
	b.ensureCapacityForNewGroup()
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
	b.advanceEnd()
}

// addLeftUnmatchedGroup adds a left and right group to the buffer that
// correspond to an unmatched row from the left side in the case of LEFT OUTER,
// LEFT ANTI, or EXCEPT ALL joins.
func (b *circularGroupsBuffer) addLeftUnmatchedGroup(curLIdx int, curRIdx int) {
	b.ensureCapacityForNewGroup()
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
	b.advanceEnd()
}

// addRightUnmatchedGroup adds a left and right group to the buffer that
// correspond to an unmatched row from the right side in the case of RIGHT
// OUTER and RIGHT ANTI joins.
func (b *circularGroupsBuffer) addRightUnmatchedGroup(curLIdx int, curRIdx int) {
	b.ensureCapacityForNewGroup()
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
	b.advanceEnd()
}

// addLeftSemiGroup adds a left group to the buffer that corresponds to a run
// of tuples from the left side that all have a match on the right side. This
// should only be called after processing the last equality column, and this
// group will be used by the builder next. Note that we're not adding a right
// group here since tuples from the right are not outputted in LEFT SEMI nor
// INTERSECT ALL joins.
func (b *circularGroupsBuffer) addLeftSemiGroup(curLIdx int, lRunLength int) {
	b.ensureCapacityForNewGroup()
	b.leftGroups[b.endIdx] = group{
		rowStartIdx: curLIdx,
		rowEndIdx:   curLIdx + lRunLength,
		numRepeats:  1,
		toBuild:     lRunLength,
	}
	b.advanceEnd()
}

// addRightSemiGroup adds a right group to the buffer that corresponds to a run
// of tuples from the right side that all have a match on the left side. This
// should only be called after processing the last equality column, and this
// group will be used by the builder next. Note that we're not adding a left
// group here since tuples from the left are not outputted in RIGHT SEMI joins.
func (b *circularGroupsBuffer) addRightSemiGroup(curRIdx int, rRunLength int) {
	b.ensureCapacityForNewGroup()
	b.rightGroups[b.endIdx] = group{
		rowStartIdx: curRIdx,
		rowEndIdx:   curRIdx + rRunLength,
		numRepeats:  1,
		toBuild:     rRunLength,
	}
	b.advanceEnd()
}

// finishedCol is used to notify the circular buffer to update the indices
// representing the "window" of available values for the next column.
func (b *circularGroupsBuffer) finishedCol() {
	b.startIdx = b.nextColStartIdx
	b.nextColStartIdx = b.endIdx
	b.numGroupsInBuffer = b.endIdx - b.startIdx
	if b.numGroupsInBuffer < 0 {
		b.numGroupsInBuffer += b.capacity
	}
}

// getGroups returns left and right slices of groups that are contiguous, which
// is a useful simplification for the build phase.
func (b *circularGroupsBuffer) getGroups() ([]group, []group) {
	startIdx := b.startIdx
	endIdx := b.endIdx
	leftGroups, rightGroups := b.leftGroups, b.rightGroups

	if endIdx < startIdx {
		copy(leftGroups[b.capacity:], leftGroups[:endIdx])
		copy(rightGroups[b.capacity:], rightGroups[:endIdx])
		endIdx += b.capacity
	}

	return leftGroups[startIdx:endIdx], rightGroups[startIdx:endIdx]
}
