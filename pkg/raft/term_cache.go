// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raft

// termCache is a compressed representation of entryIDs of a raft log suffix.
// It may cover entries in both stable and unstable parts of the log.
//
// The termCache conforms to raft safety properties. It also provides the
// following guarantees:
//
//  1. termCache is consistent with the raftLog.
//     (It observes all appends(overwrite), truncations,
//     and (re-)initializations of the raftLog.)
//  2. Covers entryIDs of a log suffix:
//     [cache[0],raftLog.lastEntry()].
//
// Which we can use the above invariants to determine the term of a
// raft entry by index.
type termCache struct {
	// cache is a fix sized slice storing term flip entryIDs.
	//
	// cache has the following invariants:
	//  1. cache[i].Index < entries[i+1].Index,
	//  2. cache[i].term < entries[i+1].Term.
	//  3. cache[0] is always populated.
	//  4. Except for the first entry cache[0], all entries are term flips.
	//     	NB: cache[0] = raftLog.lastEntry on initialization
	//
	// Users of this struct can assume the invariants always hold true.
	//
	// We can use the above invariants to determine the term of a raft entry
	// if given the entry index:
	//
	//  entry.index >= cache[0].index && entry.index <= raftLog.lastIndex
	cache []entryID
	// maxSize is the maximum size of the term cache.
	maxSize uint64
}

// NewTermCache initializes a termCache with a fixed size and the last entry.
//
// last should be the last entry in the raftLog when calling this function.
func NewTermCache(size uint64, last entryID) termCache {
	tc := termCache{
		cache:   make([]entryID, 0, size),
		maxSize: size,
	}
	tc.reset(last)
	return tc
}

// term returns the term of the log entry at the given index, or false if the
// index is not covered by the term cache.
//
// It is the caller's responsibility to ensure:
//
//	raftLog.firstIndex() <= index <= raftLog.lastIndex().
func (tc *termCache) term(index uint64) (uint64, bool) {
	for i := len(tc.cache) - 1; i >= 0; i-- {
		if index >= tc.cache[i].index {
			return tc.cache[i].term, true
		}
	}
	return 0, false
}

// truncateAndAppend appends a slice of raftLog to the termCache.
// If overwriting, it will truncate the cache first
// and append the new entries.
func (tc *termCache) truncateAndAppend(ls LogSlice) {
	tc.truncateFrom(ls.prev)
	for _, entry := range ls.Entries() {
		tc.append(entryID{entry.Term, entry.Index})
	}
}

// truncateFrom clears all entries from the term cache with index greater than
// entry. If entry.index is lower than cache[0].index, the whole term cache
// is reinitialized with the passed in entry.
func (tc *termCache) truncateFrom(entry entryID) {
	if entry.index >= tc.lastEntry().index {
		return
	}
	for i := len(tc.cache) - 2; i >= 0; i-- {
		if entry.index >= tc.cache[i].index {
			// Full slice expression to specify the max capacity of the slice.
			// A subsequent attempt of appending to the slice
			// will cause a reallocation.
			// This allows for copy-on-write which preserves the
			// integrity of any previously taken snapshot.
			tc.cache = tc.cache[: i+1 : i+1]
			return
		}
	}
	tc.reset(entry)
}

// append adds a new entryID to the cache.
// If the cache is full, the oldest entryID is removed.
func (tc *termCache) append(entry entryID) {
	if entry.term == tc.lastEntry().term {
		return
	}
	if len(tc.cache) >= int(tc.maxSize) {
		tc.cache = tc.cache[1:]
	}
	tc.cache = append(tc.cache, entry)
}

func (tc *termCache) lastEntry() entryID {
	return tc.cache[len(tc.cache)-1]
}

func (tc *termCache) firstEntry() entryID {
	return tc.cache[0]
}

// reset clears the term cache and adds the last entry.
func (tc *termCache) reset(last entryID) {
	// reallocate the cache to the max size,
	// any previous snapshot taken on the term cache is unmodified
	tc.cache = make([]entryID, 0, tc.maxSize)
	tc.cache = append(tc.cache, last)
}
