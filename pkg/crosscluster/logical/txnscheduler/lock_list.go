package scheduler

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

// TODO we do actually need to continue to track write locks when there are
// read locks.

func makeLockTable(lockCount int32) lockTable {
	locks := lockTable{
		freeList: make([]lockEntryIndex, lockCount),
		locks:    make([]lockBlock, lockCount),
	}
	for i := int32(0); i < lockCount; i++ {
		locks.freeList[i] = lockEntryIndex(i)
	}
	return locks
}

type lockTable struct {
	locks    []lockBlock
	freeList []lockEntryIndex
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

func (lt *lockTable) addWriteDependency(
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

func (lt *lockTable) addReadDependency(
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
	// lock in the block.
	if head.readAlloc[0] != lock {
		return list, true
	}

	head.readLocks--
	if head.readLocks == 0 && list.head == list.tail {
		lt.free(list.head)
		return lockList{}, false
	}
	if head.readLocks == 0 {
		nextHead := head.next
		lt.locks[nextHead].writeLock = head.writeLock
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

func (lt *lockTable) setWriteLock(list lockList, lock hlc.Timestamp) lockList {
	head := &lt.locks[list.head]

	// If this is already a write lock, just update it.
	if !head.writeLock.IsEmpty() {
		head.writeLock = lock
		return list
	}

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

func (lt *lockTable) addReadLock(list lockList, lock hlc.Timestamp) lockList {
	tail := &lt.locks[list.tail]
	if tail.readLocks < 8 {
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

type lockList struct {
	head lockEntryIndex
	tail lockEntryIndex
}

type lockBlock struct {
	writeLock hlc.Timestamp
	readAlloc [8]hlc.Timestamp
	readLocks int8
	next      lockEntryIndex
}

func (l *lockBlock) clear() {
	l.writeLock = hlc.Timestamp{}
	l.readLocks = 0
	l.readAlloc = [8]hlc.Timestamp{}
}
