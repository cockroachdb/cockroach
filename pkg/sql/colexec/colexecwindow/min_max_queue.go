// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecwindow

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/errors"
)

var errEmptyGetFirst = errors.AssertionFailedf("getting first from empty minMaxQueue")

func newMinMaxQueue(maxLength int) minMaxQueue {
	return minMaxQueue{maxLength: maxLength, empty: true}
}

// minMaxQueue buffers uint32 values that are used by the min and max window
// functions to handle the case where the window frame for the current row need
// not include every row that was in the previous frame. minMaxQueue optimizes
// for operations on the start and end of the buffer, such as removing the last
// n values.
type minMaxQueue struct {
	buffer []uint32 // an increasing circular buffer of values
	head   int      // the index of the front of the buffer
	tail   int      // the index of the first position after the end of the buffer

	// Indicates whether the buffer is empty. Necessary to distinguish between an
	// empty buffer and a buffer that uses all of its capacity.
	empty bool

	// The maximum size to which the minMaxQueue can grow. If reached, no new
	// elements can be added to the minMaxQueue until some have been removed.
	maxLength int
}

// len returns the number of elements in the minMaxQueue.
// gcassert:inline
func (q *minMaxQueue) len() int {
	if q.empty {
		return 0
	}
	if q.head < q.tail {
		return q.tail - q.head
	}
	return cap(q.buffer) + q.tail - q.head
}

// isEmpty returns true if the minMaxQueue has no elements.
// gcassert:inline
func (q *minMaxQueue) isEmpty() bool {
	return q.empty
}

// get returns the element at position pos in the minMaxQueue (zero-based).
// gcassert:inline
func (q *minMaxQueue) get(pos int) uint32 {
	return q.buffer[(pos+q.head)%cap(q.buffer)]
}

// getFirst returns the element at the start of the minMaxQueue.
// gcassert:inline
func (q *minMaxQueue) getFirst() uint32 {
	if q.empty {
		colexecerror.InternalError(errEmptyGetFirst)
	}
	return q.buffer[q.head]
}

// getLast returns the element at the end of the minMaxQueue.
// gcassert:inline
func (q *minMaxQueue) getLast() uint32 {
	return q.buffer[(cap(q.buffer)+q.tail-1)%cap(q.buffer)]
}

// addLast adds element to the end of the minMaxQueue and doubles it's
// underlying slice if necessary, subject to the max length limit. If the
// minMaxQueue has already reached the maximum length, addLast returns true,
// otherwise false.
func (q *minMaxQueue) addLast(element uint32) (reachedLimit bool) {
	if q.maybeGrow() {
		return true
	}
	q.buffer[q.tail] = element
	q.tail = (q.tail + 1) % cap(q.buffer)
	q.empty = false
	return false
}

// removeLast removes a single element from the end of the minMaxQueue.
func (q *minMaxQueue) removeLast() {
	if q.empty {
		colexecerror.InternalError(errors.AssertionFailedf("removing last from empty ring buffer"))
	}
	lastPos := (cap(q.buffer) + q.tail - 1) % cap(q.buffer)
	q.tail = lastPos
	if q.tail == q.head {
		q.empty = true
	}
}

// removeAllBefore removes from the minMaxQueue all values in the range
// [0, val).
func (q *minMaxQueue) removeAllBefore(val uint32) {
	if q.empty {
		return
	}
	var idx int
	length := q.len()
	for ; idx < length && q.get(idx) < val; idx++ {
	}
	if idx == length {
		q.empty = true
	}
	q.head = (q.head + idx) % cap(q.buffer)
}

func (q *minMaxQueue) grow(n int) {
	newBuffer := make([]uint32, n)
	if q.head < q.tail {
		copy(newBuffer[:q.len()], q.buffer[q.head:q.tail])
	} else {
		copy(newBuffer[:cap(q.buffer)-q.head], q.buffer[q.head:])
		copy(newBuffer[cap(q.buffer)-q.head:q.len()], q.buffer[:q.tail])
	}
	q.head = 0
	q.tail = cap(q.buffer)
	q.buffer = newBuffer
}

// maybeGrow attempts to double the size of the minMaxQueue, capped at
// maxLength. If the minMaxQueue has already reached maxLength, returns true.
func (q *minMaxQueue) maybeGrow() (reachedLimit bool) {
	if q.len() != cap(q.buffer) {
		return false
	}
	if q.len() == q.maxLength {
		return true
	}
	n := 2 * cap(q.buffer)
	if n == 0 {
		n = 1
	}
	if n > q.maxLength {
		n = q.maxLength
	}
	q.grow(n)
	return false
}

// Reset makes minMaxQueue treat its underlying memory as if it were empty. This
// allows for reusing the same memory again without explicitly removing old
// elements.
// gcassert:inline
func (q *minMaxQueue) reset() {
	q.head = 0
	q.tail = 0
	q.empty = true
}

// close releases the minMaxQueue's underlying memory.
func (q *minMaxQueue) close() {
	q.buffer = nil
}
