// Copyright 2018 The Cockroach Authors.
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

package ring

const bufferInitialSize = 8

// Buffer is a deque maintained over a ring buffer. Note: it is backed by
// a slice (unlike container/ring one that is backed by a linked list).
type Buffer struct {
	buffer []interface{}
	head   int // the index of the front of the deque.
	tail   int // the index of the first position right after the end of the deque.

	// indicates whether the deque is empty, necessary to distinguish
	// between an empty deque and a deque that uses all of its capacity.
	nonEmpty bool
}

// Len returns the number of elements in the deque.
func (r Buffer) Len() int {
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

// AddFirst add element to the front of the deque
// and doubles it's underlying slice if necessary.
func (r *Buffer) AddFirst(element interface{}) {
	if cap(r.buffer) == 0 {
		r.buffer = make([]interface{}, bufferInitialSize)
		r.buffer[0] = element
		r.tail = 1
	} else {
		if r.Len() == cap(r.buffer) {
			newBuffer := make([]interface{}, 2*cap(r.buffer))
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
		r.head = (cap(r.buffer) + r.head - 1) % cap(r.buffer)
		r.buffer[r.head] = element
	}
	r.nonEmpty = true
}

// AddLast adds element to the end of the deque
// and doubles it's underlying slice if necessary.
func (r *Buffer) AddLast(element interface{}) {
	if cap(r.buffer) == 0 {
		r.buffer = make([]interface{}, bufferInitialSize)
		r.buffer[0] = element
		r.tail = 1
	} else {
		if r.Len() == cap(r.buffer) {
			newBuffer := make([]interface{}, 2*cap(r.buffer))
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
		r.buffer[r.tail] = element
		r.tail = (r.tail + 1) % cap(r.buffer)
	}
	r.nonEmpty = true
}

// Get returns an element at position pos in the deque (zero-based).
func (r Buffer) Get(pos int) interface{} {
	if !r.nonEmpty || pos < 0 || pos >= r.Len() {
		panic("unexpected behavior: index out of bounds")
	}
	return r.buffer[(pos+r.head)%cap(r.buffer)]
}

// GetFirst returns an element at the front of the deque.
func (r Buffer) GetFirst() interface{} {
	if !r.nonEmpty {
		panic("unexpected behavior: getting first from empty deque")
	}
	return r.buffer[r.head]
}

// GetLast returns an element at the front of the deque.
func (r Buffer) GetLast() interface{} {
	if !r.nonEmpty {
		panic("unexpected behavior: getting last from empty deque")
	}
	return r.buffer[(cap(r.buffer)+r.tail-1)%cap(r.buffer)]
}

// RemoveFirst removes a single element from the front of the deque.
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

// RemoveLast removes a single element from the end of the deque.
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

// Reset makes Buffer treat its underlying memory as if it were empty. This
// allows for reusing the same memory again without explicitly removing old
// elements.
func (r *Buffer) Reset() {
	r.head = 0
	r.tail = 0
	r.nonEmpty = false
}
