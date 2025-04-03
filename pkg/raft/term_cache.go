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
//  1. It is consistent with the raftLog. It observes all appends, overwrites,
//     and (re-)initializations of the log, and is updated accordingly.
//  2. Covers entryIDs of a log suffix: [cache[0].index, raftLog.lastIndex()].
//  3. Copies of termCache are immutable.
//
// The termCache is used to determine the term of a raft entry by its index.
type termCache struct {
	// cache is a limited-size list of term change positions in the raft log.
	//
	// cache has the following invariants:
	// Two important invariants to call out first, in relation to the raft log:
	//  1.	raftLog.term(index) == cache[i].term,
	// 			for cache[i].index <= index < cache[i+1].index
	//	2. raftLog.term(index) == cache[len-1].term,
	// 			for cache[len-1].index <= index <= lastIndex
	// That is, there are no gaps in this representation.
	// The invariants below mainly follow from these 2, and from properties of
	// raft logs.
	//  3. cache[i].index < cache[i+1].index,
	//  4. cache[i].term < cache[i+1].term.
	//  5. cache[0] is always populated.
	//  6. Except for the first entry cache[0], all entries are term "flips".
	//     	NB: cache[0] = raftLog.lastEntryID on initialization
	//
	// We can use the above invariants to determine the term of a raft entry
	// if given the entry index:
	//
	//	cache[0].index <= entry.index <= raftLog.lastIndex
	cache []entryID
	// maxSize is the maximum size of the term cache.
	maxSize uint64
}

// newTermCache initializes a termCache with a fixed size and the last entry.
//
// last should be the last entry in the raftLog when calling this function.
func newTermCache(size uint64, last entryID) termCache {
	// TODO(hakuuww): decrease the initial size to 2 when termCache starts
	// observing log truncations.
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
// Requires: LogStorage.Compacted() <= index <= raftLog.lastIndex().
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
// If the appended slice overwrites a suffix of the raft log, the term cache
// suffix is overwritten accordingly.
func (tc *termCache) truncateAndAppend(ls LogSlice) {
	tc.truncateFrom(ls.prev)
	if ls.lastEntryID().term == tc.last().term {
		// NB: Common case, there are no new terms in the appended slice.
		return
	}
	for _, entry := range ls.Entries() {
		tc.append(entryID{term: entry.Term, index: entry.Index})
	}
}

// truncateFrom clears all entries from the term cache with index > entry.index.
// If entry.index < cache[0].index, the whole term cache is reinitialized with
// the passed in entry.
func (tc *termCache) truncateFrom(entry entryID) {
	last := len(tc.cache) - 1 // Invariant: last >= 0
	// NB: entry.index > cache[last].index never happens.
	if entry.index >= tc.cache[last].index {
		// NB: it is also true that entry.term == tc.cache[last].term here, by
		// invariant 2.
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
	// The appended slice overwrites the entire suffix of the raft log covered
	// by the term cache.
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
	// NB: any previous copy of the term cache is unmodified.
	tc.cache = append(tc.cache[len(tc.cache):], last)
}
