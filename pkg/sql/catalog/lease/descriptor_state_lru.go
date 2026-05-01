// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

// lruEntry holds the prev/next pointers for the LRU
// doubly-linked list embedded in each descriptorState.
type lruEntry struct {
	prev *descriptorState
	next *descriptorState
}

// descriptorStateLRU is a doubly-linked list of descriptorState objects.
// The list is ordered from most recently used (head) to least recently used (tail).
// All operations are O(1). The list is protected by Manager.mu.
type descriptorStateLRU struct {
	root descriptorState
}

// init initializes the LRU list.
func (l *descriptorStateLRU) init() {
	l.root.next = &l.root
	l.root.prev = &l.root
}

// tail returns the least recently used entry, or nil if empty.
func (l *descriptorStateLRU) tail() *descriptorState {
	if l.root.prev == &l.root {
		return nil
	}
	return l.root.prev
}

// prev returns the entry before e, or nil if e is the tail.
func (l *descriptorStateLRU) prev(e *descriptorState) *descriptorState {
	if e.prev == &l.root {
		return nil
	}
	return e.prev
}

// pushFront moves or inserts e to the front (most recently used)
// of the list. If e is already in the list, it is moved; otherwise
// it is inserted.
func (l *descriptorStateLRU) pushFront(e *descriptorState) {
	if e == l.root.next {
		return // already at front
	}
	if e.next != nil {
		// Already in the list; remove first.
		l.remove(e)
	}
	h := l.root.next
	e.next = h
	e.prev = &l.root
	h.prev = e
	l.root.next = e
}

// remove removes e from the list. e must be in the list.
func (l *descriptorStateLRU) remove(e *descriptorState) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil
	e.prev = nil
}
