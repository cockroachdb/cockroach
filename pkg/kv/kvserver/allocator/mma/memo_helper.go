// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

// clearableMemoMap is a generic map suited to the needs of various memos used
// in the allocator. The key is any type that implements the mapKey interface
// (e.g. storeIDPostingList). There is no removal from the map since the memo
// is built up during an allocator round and then cleared for the next round.
// The key-value pairs are stored in a mapEntry, which the caller is
// responsible for initializing. The mapEntrySlice and its mapEntries are
// reused (via the mapEntrySlicePool), since the contents of mapEntry may be
// allocation heavy.
type clearableMemoMap[K mapKey, T mapEntry] struct {
	entryAlloc mapEntryAllocator[T]
	slicePool  mapEntrySlicePool[T]
	entryMap   map[uint64]*mapEntrySlice[T]
}

// mapKey is the key of the map.
type mapKey interface {
	hash() uint64
	// TODO(sumeer): we want the parameter to be the concrete type that
	// implements mapKey instead of needing to do a type assertion in the method
	// implementation. Is that possible with Go generics?
	isEqual(b mapKey) bool
}

// mapEntry bundles together the key and value. The map implementation does
// not care about the value since it is the caller's responsibility to
// initialize.
type mapEntry interface {
	mapKey
	// clear prepares the entry for reuse.
	//
	// TODO(sumeer): we want to call clear on the element of the mapEntrySlice,
	// which means we need a pointer receiver. So callers need to implement
	// mapEntry using a pointer type, which is a pain, since it requires them to
	// pass a mapEntryAllocator to create a new struct whose pointer implements
	// mapEntry.
	clear()
}

// mapEntryAllocator returns a new non-nil mapEntry if the parameter is nil.
// It should directly allocate from the Go memory allocator, i.e., it does not
// need to do any pooling. The peculiar ensureNonNilMapEntry is needed since
// the caller can't do x == nil, where x is an object of parameter T with type
// constraint mapEntry ("mismatched types T and untyped nil"), or x == T(nil)
// ("cannot convert nil to type T").
type mapEntryAllocator[T mapEntry] interface {
	ensureNonNilMapEntry(T) T
}

// mapEntrySlice is the value in the underlying Golang map to allow for hash
// collisions. These will typically have a single entry. Since the memory of
// these slices is reused, we use this simple approach to handle collisions,
// instead of some form of open addressing scheme.
type mapEntrySlice[T mapEntry] struct {
	entries []T
}

// mapEntrySlicePool provides memory reuse. The methods can be directly backed
// by a sync.Pool, since the caller already does the clearing needed for
// reuse.
type mapEntrySlicePool[T mapEntry] interface {
	newEntry() *mapEntrySlice[T]
	releaseEntry(slice *mapEntrySlice[T])
}

func newClearableMapMemo[K mapKey, T mapEntry](
	entryAlloc mapEntryAllocator[T], slicePool mapEntrySlicePool[T],
) *clearableMemoMap[K, T] {
	return &clearableMemoMap[K, T]{
		entryAlloc: entryAlloc,
		slicePool:  slicePool,
		entryMap:   map[uint64]*mapEntrySlice[T]{},
	}
}

// get looks up k in the map and if found returns the entry and true, or
// inserts an empty entry and returns that entry and false. The caller must
// initialize that empty entry, including the key, so that subsequent calls to
// isEqual can do meaningful computation. This assumes there is no error case
// where the caller will not be able to populate the entry, which is true for
// the current use cases. If needed we can add less efficient getIfFound and
// put methods for use cases that need the capability to handle errors.
func (cmm *clearableMemoMap[K, T]) get(k mapKey) (entry T, ok bool) {
	h := k.hash()
	var entrySlice *mapEntrySlice[T]
	entrySlice, ok = cmm.entryMap[h]
	if ok {
		for i := range entrySlice.entries {
			if entrySlice.entries[i].isEqual(k) {
				return entrySlice.entries[i], true
			}
		}
	} else {
		entrySlice = cmm.slicePool.newEntry()
		entrySlice.entries = entrySlice.entries[:0]
		cmm.entryMap[h] = entrySlice
	}
	n := len(entrySlice.entries)
	if cap(entrySlice.entries) > n {
		entrySlice.entries = entrySlice.entries[:n+1]
	} else {
		size := (n + 1) * 2
		entries := make([]T, n+1, size)
		copy(entries, entrySlice.entries)
		entrySlice.entries = entries
	}
	entrySlice.entries[n] = cmm.entryAlloc.ensureNonNilMapEntry(entrySlice.entries[n])
	return entrySlice.entries[n], false
}

func (cmm *clearableMemoMap[K, T]) clear() {
	for k, v := range cmm.entryMap {
		for i := range v.entries {
			v.entries[i].clear()
		}
		v.entries = v.entries[:0]
		cmm.slicePool.releaseEntry(v)
		delete(cmm.entryMap, k)
	}
}

func (cmm *clearableMemoMap[K, T]) lenForTesting() int {
	var n int
	for _, v := range cmm.entryMap {
		n += len(v.entries)
	}
	return n
}
