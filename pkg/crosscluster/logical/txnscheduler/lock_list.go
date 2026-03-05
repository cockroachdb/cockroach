// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnscheduler

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

// readLocksPerBlock is the number of read locks that can be stored in a single
// lockBlock before a new block must be allocated.
const readLocksPerBlock = 8

// lockTable is a pool of `lockBlock` structs. It has two main uses:
// 1. It lets us allocate the lockBlocks as a single contiguous array. This is
// cache friendly and lets us use i32 "pointers" in the lock -> list hash map.
// 2. It tracks which lockBlocks are currently free.
type lockTable struct {
	// locks is a slab of allocated lockBlocks.
	locks []lockBlock

	// freeList is used as a stack. So we prefer to reuse lock blocks that are
	// likely to be in the cache.
	freeList []lockEntryIndex
}

// lockEntryIndex is an index into lockTable.locks.
type lockEntryIndex int32

// lockList is an unrolled linked list used to track read locks. A write lock
// must follow every read that preceded it, so the maximum number of read locks
// we need to hold onto is bound only by the garbage collection that occurs
// when we advance the event horizon.
type lockList struct {
	// head is the first block in the linked list.
	head lockEntryIndex
	// tail is the last block in the linked list.
	tail lockEntryIndex
}

// lockBlock is a link in an unrolled linked list of read locks.
type lockBlock struct {
	writeLock hlc.Timestamp
	readAlloc [readLocksPerBlock]hlc.Timestamp
	readLocks int8
	next      lockEntryIndex
}

func makeLockTable(size int32) lockTable {
	locks := lockTable{
		freeList: make([]lockEntryIndex, size),
		locks:    make([]lockBlock, size),
	}
	for i := range locks.freeList {
		locks.freeList[i] = lockEntryIndex(i)
	}
	return locks
}

func (lt *lockTable) allocateBlock() lockEntryIndex {
	next := lt.freeList[len(lt.freeList)-1]
	lt.freeList = lt.freeList[:len(lt.freeList)-1]
	return next
}

func (lt *lockTable) allocateList() lockList {
	entry := lt.allocateBlock()
	return lockList{
		head: entry,
		tail: entry,
	}
}

func (lt *lockTable) appendWriteDependency(
	list lockList, dependencies []hlc.Timestamp,
) []hlc.Timestamp {
	next := list.head
	entry := &lt.locks[next]

	if !entry.writeLock.IsEmpty() {
		dependencies = append(dependencies, entry.writeLock)
	}
	for {
		for i := int8(0); i < entry.readLocks; i++ {
			dependencies = append(dependencies, entry.readAlloc[i])
		}
		if next == list.tail {
			return dependencies
		}
		next = entry.next
		entry = &lt.locks[next]
	}
}

func (lt *lockTable) appendReadDependency(
	list lockList, dependencies []hlc.Timestamp,
) []hlc.Timestamp {
	head := &lt.locks[list.head]
	if !head.writeLock.IsEmpty() {
		return append(dependencies, head.writeLock)
	}
	return dependencies
}

func (lt *lockTable) removeReadLock(list lockList, lock hlc.Timestamp) (lockList, bool) {
	head := &lt.locks[list.head]
	// Locks are removed in hlc order, so if the lock is present it is the first
	// lock in the block. The lock may not be present if a write lock caused it
	// to be cleared.
	if head.readAlloc[0] != lock {
		return list, true
	}

	head.readLocks--
	if head.readLocks == 0 && list.head == list.tail {
		lt.free(list.head)
		return lockList{}, false
	}
	if head.readLocks == 0 {
		// NOTE: we are assuming there is no write lock here because if the write
		// lock came before the read lock, the read lock would have been cleared
		// when the write lock was recorded.
		nextHead := head.next
		lt.free(list.head)
		list.head = nextHead
		return list, true
	}

	copy(head.readAlloc[0:], head.readAlloc[1:head.readLocks+1])
	head.readAlloc[head.readLocks] = hlc.Timestamp{}

	return list, true
}

func (lt *lockTable) free(entry lockEntryIndex) {
	lt.locks[entry].clear()
	lt.freeList = append(lt.freeList, entry)
}

func (lt *lockTable) removeWriteLock(list lockList, lock hlc.Timestamp) (lockList, bool) {
	head := &lt.locks[list.head]
	if head.writeLock != lock {
		return list, true
	}

	head.writeLock = hlc.Timestamp{}
	if head.readLocks == 0 {
		lt.free(list.head)
		return lockList{}, false
	}

	return list, true
}

func (lt *lockTable) recordWriteLock(list lockList, lock hlc.Timestamp) lockList {
	head := &lt.locks[list.head]

	// If there is more than one block of read locks, we need to free all but the
	// first block.
	if list.head != list.tail {
		toFree := head.next
		for {
			next := lt.locks[toFree].next
			lt.free(toFree)
			if toFree != list.tail {
				toFree = next
			} else {
				break
			}
		}
	}

	head.clear()
	head.writeLock = lock
	return lockList{
		head: list.head,
		tail: list.head,
	}
}

func (lt *lockTable) recordReadLock(list lockList, lock hlc.Timestamp) lockList {
	tail := &lt.locks[list.tail]
	if tail.readLocks < readLocksPerBlock {
		tail.readAlloc[tail.readLocks] = lock
		tail.readLocks++
	} else {
		list.tail = lt.allocateBlock()
		tail.next = list.tail
		tail = &lt.locks[list.tail]
		tail.readAlloc[0] = lock
		tail.readLocks = 1
	}
	return list
}

func (lt *lockTable) availableCapacity() int {
	return len(lt.freeList)
}

func (l *lockBlock) clear() {
	*l = lockBlock{}
}
