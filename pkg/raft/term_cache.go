// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raft

// termCache is a compressed representation of entryIDs of a raft log suffix.
// It may cover entries in both stable and unstable parts of the log.
//
// The termCache provides the following guarantees:
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
	tc.cache = append(tc.cache, last)
	return tc
}

// term returns the term of the log entry at the given index, or false if the
// index is not covered by the term cache.
//
// Requires: storage.Compacted() <= index <= raftLog.lastIndex().
func (tc *termCache) term(index uint64) (uint64, bool) {
	for i := len(tc.cache) - 1; i >= 0; i-- {
		if index >= tc.cache[i].index {
			return tc.cache[i].term, true
		}
	}
	return 0, false
}

// truncateAndAppend appends a log slice to the termCache.
// The appendage has been validated by the caller, and mirrors the log append.
//
// If the append slice overwrites a suffix of the raft log,the term cache suffix
// is overwritten accordingly.
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
	last := len(tc.cache) - 1 // Invariant: last >= 0
	// NB: entry.index > last never happens.
	if entry.index >= tc.cache[last].index {
		return
	}
	for i := last - 1; i >= 0; i-- {
		if entry.index >= tc.cache[i].index {
			// Use the full slice expression to protect potential copies of this slice
			// from subsequent appends. This copy-on-write approach preserves
			// integrity of term cache copies that may be in use by other goroutines
			// concurrently, e.g. in LogSnapshot.
			tc.cache = tc.cache[: i+1 : i+1]
			return
		}
	}
	// the appendage overwrites the entire suffix of
	// the raft log covered by term cache
	tc.reset(entry)
}

// append adds a new entryID to the cache.
// If the cache is full, the oldest entryID is removed.
func (tc *termCache) append(entry entryID) {
	if entry.term == tc.last().term {
		return
	}
	if len(tc.cache) >= int(tc.maxSize) {
		tc.cache = tc.cache[1:]
	}
	tc.cache = append(tc.cache, entry)
}

func (tc *termCache) last() entryID {
	return tc.cache[len(tc.cache)-1]
}

func (tc *termCache) first() entryID {
	return tc.cache[0]
}

// reset clears the term cache and adds the last entry.
func (tc *termCache) reset(last entryID) {
	// any previous snapshot taken on the term cache is unmodified
	tc.cache = append(tc.cache[len(tc.cache):], last)
}
