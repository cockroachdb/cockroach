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

func makeGroupsBuffer(batchSize int) circularGroupsBuffer {
	return circularGroupsBuffer{
		// The maximum number of possible groups per batch is achieved with FULL
		// OUTER JOIN when no rows have matches, so there will be exactly
		// batchSize x 2 groups. We add an additional element to the capacity in
		// order to be able to distinguish between an "empty" (no groups) and a
		// "full" (2*batchSize groups) buffers.
		cap: 2*batchSize + 1,
		// Since we have a circular buffer, it is possible for groups to wrap when
		// cap is reached. Consider an example when batchSize = 3 and startIdx = 6
		// when maximum number of groups is present:
		// buffer = [1, 2, 3, 4, 5, x, 0] (integers denote different groups and 'x'
		// stands for a garbage).
		// When getGroups() is called, for ease of usage we need to return the
		// buffer "flattened out", and in order to reduce allocation, we actually
		// reserve 4*batchSize. In the example above we will copy the buffer as:
		// buffer = [1, 2, 3, 4, 5, x, 0, 1, 2, 3, 4, 5]
		// and will return buffer[6:12] when getGroups is called.
		// The calculations for why 4*batchSize is sufficient:
		// - the largest position in which the first group can be placed is
		//   2*batchSize (cap field enforces that)
		// - the largest number of groups to copy from the "physical" start of the
		//   buffer is 2*batchSize-1
		// - adding those two numbers we arrive at 4*batchSize.
		leftGroups:  make([]group, 4*batchSize),
		rightGroups: make([]group, 4*batchSize),
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
// correspond to an unmatched row from the left side in the case of LEFT OUTER,
// LEFT ANTI, or EXCEPT ALL joins.
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
// group here since tuples from the right are not outputted in LEFT SEMI nor
// INTERSECT ALL joins.
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
