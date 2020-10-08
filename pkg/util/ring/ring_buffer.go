// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ring

// Buffer is a deque maintained over a ring buffer.
//
// Note: it is backed by a slice (unlike container/ring which is backed by a
// linked list).
type Buffer struct {
	buffer []interface{}
	head   int // the index of the front of the buffer
	tail   int // the index of the first position after the end of the buffer

	// Indicates whether the buffer is empty. Necessary to distinguish
	// between an empty buffer and a buffer that uses all of its capacity.
	nonEmpty bool
}

// Len returns the number of elements in the Buffer.
func (r *Buffer) Len() int {
	if !r.nonEmpty {
		return 0
	}
	if r.head < r.tail {
		return r.tail - r.head
	} else if r.head == r.tail {
		return cap(r.buffer)
	} else {
		return cap(r.buffer) + r.tail - r.head
	}
}

// Cap returns the capacity of the Buffer.
func (r *Buffer) Cap() int {
	return cap(r.buffer)
}

// Get returns an element at position pos in the Buffer (zero-based).
func (r *Buffer) Get(pos int) interface{} {
	if !r.nonEmpty || pos < 0 || pos >= r.Len() {
		panic("index out of bounds")
	}
	return r.buffer[(pos+r.head)%cap(r.buffer)]
}

// GetFirst returns an element at the front of the Buffer.
func (r *Buffer) GetFirst() interface{} {
	if !r.nonEmpty {
		panic("getting first from empty ring buffer")
	}
	return r.buffer[r.head]
}

// GetLast returns an element at the front of the Buffer.
func (r *Buffer) GetLast() interface{} {
	if !r.nonEmpty {
		panic("getting last from empty ring buffer")
	}
	return r.buffer[(cap(r.buffer)+r.tail-1)%cap(r.buffer)]
}

func (r *Buffer) grow(n int) {
	newBuffer := make([]interface{}, n)
	if r.head < r.tail {
		copy(newBuffer[:r.Len()], r.buffer[r.head:r.tail])
	} else {
		copy(newBuffer[:cap(r.buffer)-r.head], r.buffer[r.head:])
		copy(newBuffer[cap(r.buffer)-r.head:r.Len()], r.buffer[:r.tail])
	}
	r.head = 0
	r.tail = cap(r.buffer)
	r.buffer = newBuffer
}

func (r *Buffer) maybeGrow() {
	if r.Len() != cap(r.buffer) {
		return
	}
	n := 2 * cap(r.buffer)
	if n == 0 {
		n = 1
	}
	r.grow(n)
}

// AddFirst add element to the front of the Buffer and doubles it's underlying
// slice if necessary.
func (r *Buffer) AddFirst(element interface{}) {
	r.maybeGrow()
	r.head = (cap(r.buffer) + r.head - 1) % cap(r.buffer)
	r.buffer[r.head] = element
	r.nonEmpty = true
}

// AddLast adds element to the end of the Buffer and doubles it's underlying
// slice if necessary.
func (r *Buffer) AddLast(element interface{}) {
	r.maybeGrow()
	r.buffer[r.tail] = element
	r.tail = (r.tail + 1) % cap(r.buffer)
	r.nonEmpty = true
}

// RemoveFirst removes a single element from the front of the Buffer.
func (r *Buffer) RemoveFirst() {
	if r.Len() == 0 {
		panic("removing first from empty ring buffer")
	}
	r.buffer[r.head] = nil
	r.head = (r.head + 1) % cap(r.buffer)
	if r.head == r.tail {
		r.nonEmpty = false
	}
}

// RemoveLast removes a single element from the end of the Buffer.
func (r *Buffer) RemoveLast() {
	if r.Len() == 0 {
		panic("removing last from empty ring buffer")
	}
	lastPos := (cap(r.buffer) + r.tail - 1) % cap(r.buffer)
	r.buffer[lastPos] = nil
	r.tail = lastPos
	if r.tail == r.head {
		r.nonEmpty = false
	}
}

// Reserve reserves the provided number of elemnets in the Buffer. It is an
// error to reserve a size less than the Buffer's current length.
func (r *Buffer) Reserve(n int) {
	if n < r.Len() {
		panic("reserving fewer elements than current length")
	} else if n > cap(r.buffer) {
		r.grow(n)
	}
}

// Reset makes Buffer treat its underlying memory as if it were empty. This
// allows for reusing the same memory again without explicitly removing old
// elements.
func (r *Buffer) Reset() {
	r.head = 0
	r.tail = 0
	r.nonEmpty = false
}
