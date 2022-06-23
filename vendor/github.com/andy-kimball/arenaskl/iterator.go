/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2017 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package arenaskl

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

type splice struct {
	prev *node
	next *node
}

func (s *splice) init(prev, next *node) {
	s.prev = prev
	s.next = next
}

// Iterator is an iterator over the skiplist object. Call Init to associate a
// skiplist with the iterator. The current state of the iterator can be cloned
// by simply value copying the struct. All iterator methods are thread-safe.
type Iterator struct {
	list  *Skiplist
	arena *Arena
	nd    *node
	value uint64
}

// Init associates the iterator with a skiplist and resets all state.
func (it *Iterator) Init(list *Skiplist) {
	it.list = list
	it.arena = list.arena
	it.nd = nil
	it.value = 0
}

// Valid returns true iff the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool { return it.nd != nil }

// Key returns the key at the current position.
func (it *Iterator) Key() []byte {
	return it.nd.getKey(it.arena)
}

// Value returns the value at the current position.
func (it *Iterator) Value() []byte {
	valOffset, valSize := decodeValue(it.value)
	return it.arena.GetBytes(valOffset, uint32(valSize))
}

// Meta returns the metadata at the current position.
func (it *Iterator) Meta() uint16 {
	return decodeMeta(it.value)
}

// Next advances to the next position. If there are no following nodes, then
// Valid() will be false after this call.
func (it *Iterator) Next() {
	next := it.list.getNext(it.nd, 0)
	it.setNode(next, false)
}

// Prev moves to the previous position. If there are no previous nodes, then
// Valid() will be false after this call.
func (it *Iterator) Prev() {
	prev := it.list.getPrev(it.nd, 0)
	it.setNode(prev, true)
}

// Seek searches for the record with the given key. If it is present in the
// skiplist, then Seek positions the iterator on that record and returns true.
// If the record is not present, then Seek positions the iterator on the
// following node (if it exists) and returns false.
func (it *Iterator) Seek(key []byte) (found bool) {
	var next *node
	_, next, found = it.seekForBaseSplice(key)
	present := it.setNode(next, false)
	return found && present
}

// SeekForPrev searches for the record with the given key. If it is present in
// the skiplist, then SeekForPrev positions the iterator on that record and
// returns true. If the record is not present, then SeekForPrev positions the
// iterator on the preceding node (if it exists) and returns false.
func (it *Iterator) SeekForPrev(key []byte) (found bool) {
	var prev, next *node
	prev, next, found = it.seekForBaseSplice(key)

	var present bool
	if found {
		present = it.setNode(next, true)
	} else {
		present = it.setNode(prev, true)
	}

	return found && present
}

// Add creates a new key/value record if it does not yet exist and positions the
// iterator on it. If the record already exists, then Add positions the iterator
// on the most current value and returns ErrRecordExists. If there isn't enough
// room in the arena, then Add returns ErrArenaFull.
func (it *Iterator) Add(key []byte, val []byte, meta uint16) error {
	var spl [maxHeight]splice
	if it.seekForSplice(key, &spl) {
		// Found a matching node, but handle case where it's been deleted.
		return it.setValueIfDeleted(spl[0].next, val, meta)
	}

	if it.list.testing {
		// Add delay to make it easier to test race between this thread
		// and another thread that sees the intermediate state between
		// finding the splice and using it.
		runtime.Gosched()
	}

	nd, height, err := it.list.newNode(key, val, meta)
	if err != nil {
		return err
	}

	value := nd.value
	ndOffset := it.arena.GetPointerOffset(unsafe.Pointer(nd))

	// We always insert from the base level and up. After you add a node in base
	// level, we cannot create a node in the level above because it would have
	// discovered the node in the base level.
	var found bool
	for i := 0; i < int(height); i++ {
		prev := spl[i].prev
		next := spl[i].next

		if prev == nil {
			// New node increased the height of the skiplist, so assume that the
			// new level has not yet been populated.
			if next != nil {
				panic("next is expected to be nil, since prev is nil")
			}

			prev = it.list.head
			next = it.list.tail
		}

		// +----------------+     +------------+     +----------------+
		// |      prev      |     |     nd     |     |      next      |
		// | prevNextOffset |---->|            |     |                |
		// |                |<----| prevOffset |     |                |
		// |                |     | nextOffset |---->|                |
		// |                |     |            |<----| nextPrevOffset |
		// +----------------+     +------------+     +----------------+
		//
		// 1. Initialize prevOffset and nextOffset to point to prev and next.
		// 2. CAS prevNextOffset to repoint from next to nd.
		// 3. CAS nextPrevOffset to repoint from prev to nd.
		for {
			prevOffset := it.arena.GetPointerOffset(unsafe.Pointer(prev))
			nextOffset := it.arena.GetPointerOffset(unsafe.Pointer(next))
			nd.tower[i].init(prevOffset, nextOffset)

			// Check whether next has an updated link to prev. If it does not,
			// that can mean one of two things:
			//   1. The thread that added the next node hasn't yet had a chance
			//      to add the prev link (but will shortly).
			//   2. Another thread has added a new node between prev and next.
			nextPrevOffset := next.prevOffset(i)
			if nextPrevOffset != prevOffset {
				// Determine whether #1 or #2 is true by checking whether prev
				// is still pointing to next. As long as the atomic operations
				// have at least acquire/release semantics (no need for
				// sequential consistency), this works, as it is equivalent to
				// the "publication safety" pattern.
				prevNextOffset := prev.nextOffset(i)
				if prevNextOffset == nextOffset {
					// Ok, case #1 is true, so help the other thread along by
					// updating the next node's prev link.
					next.casPrevOffset(i, nextPrevOffset, prevOffset)
				}
			}

			if prev.casNextOffset(i, nextOffset, ndOffset) {
				// Managed to insert nd between prev and next, so update the next
				// node's prev link and go to the next level.
				if it.list.testing {
					// Add delay to make it easier to test race between this thread
					// and another thread that sees the intermediate state between
					// setting next and setting prev.
					runtime.Gosched()
				}

				next.casPrevOffset(i, prevOffset, ndOffset)
				break
			}

			// CAS failed. We need to recompute prev and next. It is unlikely to
			// be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev
			// and next.
			prev, next, found = it.list.findSpliceForLevel(key, i, prev)
			if found {
				if i != 0 {
					panic("how can another thread have inserted a node at a non-base level?")
				}

				return it.setValueIfDeleted(next, val, meta)
			}
		}
	}

	it.value = value
	it.nd = nd
	return nil
}

// Set updates the value of the current iteration record if it has not been
// updated or deleted since iterating or seeking to it. If the record has been
// updated, then Set positions the iterator on the most current value and
// returns ErrRecordUpdated. If the record has been deleted, then Set keeps
// the iterator positioned on the current record with the current value and
// returns ErrRecordDeleted.
func (it *Iterator) Set(val []byte, meta uint16) error {
	new, err := it.list.allocVal(val, meta)
	if err != nil {
		return err
	}

	return it.trySetValue(new)
}

// SetMeta updates the meta value of the current iteration record if it has not
// been updated or deleted since iterating or seeking to it. If the record has
// been updated, then SetMeta positions the iterator on the most current value
// and returns ErrRecordUpdated. If the record has been deleted, then SetMeta
// keeps the iterator positioned on the current record with the current value
// and returns ErrRecordDeleted.
func (it *Iterator) SetMeta(meta uint16) error {
	// Try to reuse the same value bytes. Do this only in the case where meta
	// is increasing, in order to avoid cases where the meta is changed, then
	// changed back to the original value, which would make it impossible to
	// detect updates had occurred in the interim.
	if meta > decodeMeta(it.value) {
		valOffset, valSize := decodeValue(it.value)
		new := encodeValue(valOffset, valSize, meta)
		return it.trySetValue(new)
	}

	return it.Set(it.Value(), meta)
}

// Delete marks the current iterator record as deleted from the store if it
// has not been updated since iterating or seeking to it. If the record has
// been updated, then Delete positions the iterator on the most current value
// and returns ErrRecordUpdated. If the record is deleted, then Delete positions
// the iterator on the next record.
func (it *Iterator) Delete() error {
	if !atomic.CompareAndSwapUint64(&it.nd.value, it.value, deletedVal) {
		if it.setNode(it.nd, false) {
			return ErrRecordUpdated
		}

		return nil
	}

	// Deletion succeeded, so position iterator on next non-deleted node.
	next := it.list.getNext(it.nd, 0)
	it.setNode(next, false)
	return nil
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) SeekToFirst() {
	it.setNode(it.list.getNext(it.list.head, 0), false)
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) SeekToLast() {
	it.setNode(it.list.getPrev(it.list.tail, 0), true)
}

func (it *Iterator) setNode(nd *node, reverse bool) bool {
	var value uint64

	success := true
	for nd != nil {
		// Skip past deleted nodes.
		value = atomic.LoadUint64(&nd.value)
		if value != deletedVal {
			break
		}

		success = false

		if reverse {
			nd = it.list.getPrev(nd, 0)
		} else {
			nd = it.list.getNext(nd, 0)
		}
	}

	it.value = value
	it.nd = nd
	return success
}

func (it *Iterator) trySetValue(new uint64) error {
	if !atomic.CompareAndSwapUint64(&it.nd.value, it.value, new) {
		old := atomic.LoadUint64(&it.nd.value)
		if old == deletedVal {
			return ErrRecordDeleted
		}

		it.value = old
		return ErrRecordUpdated
	}

	it.value = new
	return nil
}

func (it *Iterator) setValueIfDeleted(nd *node, val []byte, meta uint16) error {
	var new uint64
	var err error

	for {
		old := atomic.LoadUint64(&nd.value)

		if old != deletedVal {
			it.value = old
			it.nd = nd
			return ErrRecordExists
		}

		if new == 0 {
			new, err = it.list.allocVal(val, meta)
			if err != nil {
				return err
			}
		}

		if atomic.CompareAndSwapUint64(&nd.value, old, new) {
			break
		}
	}

	it.value = new
	it.nd = nd
	return err
}

func (it *Iterator) seekForSplice(key []byte, spl *[maxHeight]splice) (found bool) {
	var prev, next *node

	level := int(it.list.Height() - 1)
	prev = it.list.head

	for {
		prev, next, found = it.list.findSpliceForLevel(key, level, prev)
		if next == nil {
			next = it.list.tail
		}

		spl[level].init(prev, next)

		if level == 0 {
			break
		}

		level--
	}

	return
}

func (it *Iterator) seekForBaseSplice(key []byte) (prev, next *node, found bool) {
	level := int(it.list.Height() - 1)

	prev = it.list.head
	for {
		prev, next, found = it.list.findSpliceForLevel(key, level, prev)

		if found {
			break
		}

		if level == 0 {
			break
		}

		level--
	}

	return
}

// IsSameArray returns true if the slices are the same length and the array
// underlying the two slices is the same. Always returns false for empty arrays.
func isSameArray(val1, val2 []byte) bool {
	if len(val1) == len(val2) && len(val1) > 0 {
		return &val1[0] == &val2[0]
	}

	return false
}
