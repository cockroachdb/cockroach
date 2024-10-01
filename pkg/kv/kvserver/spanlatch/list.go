// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanlatch

// latchList is a double-linked circular list of *latch elements.
type latchList struct {
	root latch
	len  int
}

func (ll *latchList) front() *latch {
	if ll.len == 0 {
		return nil
	}
	return ll.root.next
}

func (ll *latchList) lazyInit() {
	if ll.root.next == nil {
		ll.root.next = &ll.root
		ll.root.prev = &ll.root
	}
}

func (ll *latchList) pushBack(la *latch) {
	ll.lazyInit()
	at := ll.root.prev
	n := at.next
	at.next = la
	la.prev = at
	la.next = n
	n.prev = la
	ll.len++
}

func (ll *latchList) remove(la *latch) {
	la.prev.next = la.next
	la.next.prev = la.prev
	la.next = nil // avoid memory leaks
	la.prev = nil // avoid memory leaks
	ll.len--
}
