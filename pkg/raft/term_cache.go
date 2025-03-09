// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raft

// TermCache represents a compressed representation of the suffix of a raftLog.
// It is used to determine the term of a raft entry at a specific index.
//
// The TermCache conforms to raft safety properties. It also provides the
// following guarantees:
//
//  1. cache[i].Index < entries[i+1].Index,
//  2. cache[i].term < entries[i+1].Term.
//  3. cache[0] is always populated.
//  4. Except for the first entry cache[0], all entries are term flips.
//
// Which we can use the above invariants to determine the term of a
// raft entry if given an index that is greater than cache[0].index.
//
// Users of this struct can assume the invariants hold true.
type TermCache struct {
	// cache is a fixed size slice storing term flip entryIDs.
	// Special case: cache[0] may not be a term flip entry,
	// but is always populated.
	cache []entryID
	// maxSize is the maximum size of the term cache.
	maxSize uint64
}

// NewTermCache initializes a TermCache with a fixed size and the first entry.
func NewTermCache(size uint64, eID entryID) *TermCache {
	tc := &TermCache{
		cache:   make([]entryID, 0, size),
		maxSize: size,
	}
	tc.resetWithFirst(eID)
	return tc
}

// Term returns the entry term based on the given entry index.
// Returns error if not in the term cache.
func (tc *TermCache) Term(index uint64) (term uint64, found bool) {
	if len(tc.cache) == 0 {
		panic("term cache should not be empty when calling term")
	}

	for i := len(tc.cache) - 1; i >= 0; i-- {
		if index >= tc.cache[i].index {
			return tc.cache[i].term, true
		}
	}

	return 0, false
}

// ScanAppend appends a slice of raftLog to the TermCache.
// If overwriting, it will truncate the cache first
// and append the new entries.
func (tc *TermCache) ScanAppend(entriesSlice LogSlice) {
	entries := entriesSlice.Entries()

	if len(tc.cache) == 0 {
		panic("Term cache should not be empty when appending entries in batch")
	}

	tc.truncateFrom(entriesSlice.prev)

	for _, eID := range entries {
		tc.append(entryID{eID.Term, eID.Index})
	}
}

// truncateFrom clears all entries from the term cache with index greater than
// eID. If eID is lower than the first entry index, the whole term cache
// is reinitialized with the passed in entry.
func (tc *TermCache) truncateFrom(eID entryID) {
	if eID.index < tc.firstEntry().index {
		tc.resetWithFirst(eID)
		return
	}

	for i := len(tc.cache) - 1; i >= 1; i-- {
		if eID.index >= tc.cache[i].index {
			tc.cache = tc.cache[:i+1]
			return
		}
	}
}

// append adds a new entryID to the cache.
// If the cache is full, the oldest entryID is removed.
func (tc *TermCache) append(eID entryID) {
	if eID.term == tc.lastEntry().term {
		return
	}
	// the newEntry has a higher term than the last entry,
	// remove the first entry if full then append.
	if uint64(len(tc.cache)) == tc.maxSize {
		// Shift elements left by one using copy (no-alloc)
		copy(tc.cache, tc.cache[1:])
		// Reuse the last slot for the new entry
		tc.cache = tc.cache[:len(tc.cache)-1]
	}

	tc.cache = append(tc.cache, eID)
}

func (tc *TermCache) lastEntry() entryID {
	return tc.cache[len(tc.cache)-1]
}

func (tc *TermCache) firstEntry() entryID {
	return tc.cache[0]
}

// resetWithFirst clears the term cache and adds the first entry.
func (tc *TermCache) resetWithFirst(eID entryID) {
	tc.wipe()
	tc.cache = append(tc.cache, eID)
}

// reset clears the term cache and resets the lastIndex to 0.
func (tc *TermCache) wipe() {
	tc.cache = tc.cache[:0]
}
