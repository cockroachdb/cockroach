// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package rafttermcache provides a lightweight representation of a subset of
// raftLog for the purpose of determining the term of a raft entry at an index.

package rafttermcache

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type entryID struct {
	term  kvpb.RaftTerm
	index kvpb.RaftIndex
}

type TermCache struct {
	// 	cache is a fixed size slice sorted by entry.index. Each entry has a term
	// 	associated with it. Signifying "changed to this term at this index"
	// 	Special case: cache[0] may not be a term flip entry. This is due to log
	// 	truncations and overwrite.
	//
	// 	3 important Raft properties to keep in mind (makes all of this possible)
	//
	// 	- Raft’s Log Matching Property (Section 5.3 of Raft Paper), which states:
	// 	If two logs contain an entry with the same index and term, then the logs
	// 	are identical in all entries up through the given index.
	// 	- In any given raftLog at any given time, entry.index are always strictly
	// 	increasing with no gaps in between.
	// 	- In any given raftLog at any given time, entry.term numbers are always
	// 	monotonically increasing and can have gaps in between.
	//
	// 	Other Properties:
	// 	- The cache slice is always fixed size, and contain at least
	// 	one entry.(after proper initialization).
	//
	// 	- The cache entry with the lowest entry id is evicted when a new term flip
	// 	entry is added to the cache and the cache is at capacity.
	//
	// 	- When log entries are applied to the storage engine state machine, we
	// 	truncate the term cache by calling ClearTo().
	//
	// 	- Appends to the term cache are always done in batches by calling
	// 	ScanAppend().
	//
	//
	// 	Valid subset of raftLog example:
	// (index/term)
	// [11/t1, 12/t1, 13/t1, 14/t2, 15/t2, 16/t2, 17/t2, 18/t4, 19/t4, 20/t6]
	//                                                  ^            ^
	// 	                                             term jump    term jump
	//
	// 	Invalid subset of raftLog example:
	// [11/t1, 13/t1, 14/t1, 16/t2,    17/t2, 17/t2, 18/t2, 19/t1, 20/t4]
	// 	      ^                              ^             ^
	// 	index jump               non-increasing index    term decrease
	//
	// 	Detailed explanation of meaning of a cache slice:
	// 	an example term cache could look like:
	//
	// [11/t1, 14/t2, 18/t4, 22/t6]  lastIndex = 25
	//
	// 	What we can conclude from the term cache:
	//
	// 	The raft log at least has entries from index 11 up through to 25,
	// 	no gaps in between
	// 	The subset of the raftLog covered by the term cache looks like:
	// [11/t1, 12/t1, 13/t1, 14/t2, 15/t2, 16/t2, 17/t2, 18/t4,
	// 	19/t4, 20/t4, 21/t4, 22/t6, 23/t6, 24/t6, 25/t6]
	//
	// 	From index 11 to index 14:
	// 	we know the raftLog must be: [11/t1, 12/t1, 13/t1, 14/t2]
	//
	// 	From index 14 to 18, Note (14, t2) , (18, t4) in the raftLog,
	// 	we jumped from term2 to term4.
	// 	Due to the properties of the termCache, we know there is no entries from
	// 	index 14 to 18 that has term 3.
	// 	The raftLog must be: [14/t2, 15/t2, 16/t2, 17/t2, 18/t4]
	// 	Because we update the termCache by examining each new entry.
	// 	We modify the cache by appending to the end if there is a term flip entry.
	// 	We never modify the cache by inserting into the middle.
	//  If the cache is full, we evict the earliest entry.
	// 	So there are no entries with term = 3 in the raftLog.
	//
	// 	Similarly, from index 18 to 22:
	// 	The raftLog must be: [18/t4, 19/t4, 20/t4, 21/t4, 22/t4, 23/t6]
	// 	So there are no entries with term = 5 in the raftLog,
	//
	// 	Term 6 is the last entry in cache.
	// 	Since we also know the lastIndex covered by termCache:
	// 	Therefore from index 22 to 25:
	// 	The raftLog must be: [22/t6, 23/t6, 24/t6, 25/t6]
	// TODO(hakuuww): consider using a more efficient data structure
	//  that doesn't require copying slices (ie. circular buffer)
	//  If we append entries in big batches, and term flips somehow happen
	//  extremely frequently, we will be bottlenecked by the term cache
	//  in the extreme case.
	cache []entryID
	// maxSize is the maximum size of the term cache.
	maxSize uint64
	// lastIndex is the last index known to the TermCache that is in the raftLog.
	// the entry at lastIndex has the same term as TermCache.cache's
	// last entry's term.
	//
	// lastIndex is set to 0 when TermCache is created.
	//
	// lastIndex is first updated with the storage engine's
	// term: r.shMu.lastTermNotDurable and index: r.shMu.lastIndexNotDurable
	// as the first entry. This entry is the last entry in the raftLog's
	// persisted part.
	//
	// in the special case when raftLog is completely empty,
	// (can happen when deploying the database for the first time with no data)
	// the TruncatedState == LastEntryID
	// (the TruncatedState represents the highest applied-to-state machine entry)
	//
	//
	// lastIndex is updated when entries are appended to the TermCache.
	//
	// Invariants (except for on the creation of the TermCache):
	//
	// For callers, lastIndex is always populated (lastIndex >= 0),
	// and there is always at least one entry being covered in the termCache.
	//
	// lastIndex mirrors the last index in the storage part of raftLog.
	lastIndex kvpb.RaftIndex
	// mu is used to protect the term cache from concurrent accesses.
	// Which can happen for example when the following is called concurrently:
	// - logstore.LoadTerm() in replicaLogStorage.termLocked(),
	// - ScanAppend() in logstore/storeEntriesAndCommitBatch().
	// TODO(hakuuww): using a RWMutex is one way, and can guarantee safeness.
	//  But maybe we can just lock replica.mu externally when calling ScanAppend()
	//  and avoid using locks when calling LoadTerm().
	mu syncutil.RWMutex
}

// errInvalidEntryID is returned when the supplied entryID
// is invalid for the operation. Indicates inconsistent log entries.
var errInvalidEntryID = errors.New("invalid entry ID")

// NewTermCache initializes a TermCache with a fixed maxSize.
func NewTermCache(size uint64) *TermCache {
	return &TermCache{
		cache:     make([]entryID, 0, size),
		maxSize:   size,
		lastIndex: 0,
	}
}

// Term returns the entry term based on the given entry index.
// Returns error if not in the term cache.
func (tc *TermCache) Term(
	ctx context.Context, index kvpb.RaftIndex,
) (term kvpb.RaftTerm, found bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	if len(tc.cache) == 0 {
		log.Fatalf(ctx, "term cache is empty when calling Term(), should not happen")
		return 0, false
	}

	if index < tc.firstEntry().index ||
		index > tc.lastIndex {
		return 0, false
	}

	// in last term of term cache, index <= tc.lastIndex
	if index >= tc.lastEntry().index {
		return tc.lastEntry().term, true
	}

	for i := len(tc.cache) - 2; i >= 0; i-- {
		if index >= tc.cache[i].index {
			return tc.cache[i].term, true
		}
	}

	return 0, false
}

// ClearTo clears entries from the TermCache with index strictly less than hi.
// If hi is above the lastIndex, the whole term cache is cleared.
// Mirrors the clearTo function in raftentry/cache.go
func (tc *TermCache) ClearTo(hi kvpb.RaftIndex) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if len(tc.cache) == 0 || hi <= tc.firstEntry().index {
		return
	}

	// special cases:
	// keep the last entry in storage
	if hi >= tc.lastIndex {
		tc.resetWithFirstNoLock(tc.lastEntry().term, tc.lastIndex)
		return
	}

	// hi is above last entry's index, but lower or equal to lastIndex
	if hi > tc.lastEntry().index {
		tc.resetWithFirstNoLock(tc.lastEntry().term, hi)
		return
	}

	// only keep the last entry
	if hi == tc.lastEntry().index {
		tc.cache = tc.cache[len(tc.cache)-1:]
		return
	}

	// general cases
	for i := 0; i < len(tc.cache)-1; i++ {
		// hi matches a term flip index
		if hi == tc.cache[i].index {
			tc.cache = tc.cache[i:]
			return
		}

		// Allow the first entry in the term cache to not represent a term flip point
		// cache[0] only tells us entries are in term cache[0].term
		// starting from cache[0].index up to
		// min(cache[i+1].index-1 /* if not nil*/, lastIndex)
		if hi > tc.cache[i].index && hi < tc.cache[i+1].index {
			tc.cache[i].index = hi
			tc.cache = tc.cache[i:]
			return
		}
	}
}

// ScanAppend appends a list of raft entries to the TermCache
// It is the caller's responsibility to ensure entries is a valid raftLog.
func (tc *TermCache) ScanAppend(ctx context.Context, entries []pb.Entry) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if len(entries) == 0 {
		return
	}

	if len(tc.cache) == 0 {
		log.Fatalf(ctx, "Term cache should not be empty when appending entries in batch: \n %v", entries)
		return
	}

	// Reset the term cache accordingly if needed before appending.
	truncIdx := entries[0].Index
	tc.truncateFrom(kvpb.RaftIndex(truncIdx))

	// NB: we don't need to check if entries[0].index == tc.lastIndex + 1
	// since it is guaranteed by calling truncateFrom()
	//
	// TODO(hakuuww): consider appending entries in batch if there is more than
	//  length of cache entries.
	for _, ent := range entries {
		if err := tc.append(entryID{kvpb.RaftTerm(ent.Term), kvpb.RaftIndex(ent.Index)}); errors.Is(err, errInvalidEntryID) {
			// This should never happen.
			log.Fatalf(ctx, "Invalid raftLog detected when trying to append entry %v to TermCache", ent.String())
		}
	}
}

// truncateFrom clears all entries from the term cache with index equal to or
// greater than lo. Note that lo itself may or may not be in the cache.
// If lo is lower than the first entry index, the whole term cache is cleared.
// No overwrite if lo is above the lastIndex.
// Mirrors the truncateFrom function in raftentry/cache.go
func (tc *TermCache) truncateFrom(lo kvpb.RaftIndex) {
	if len(tc.cache) == 0 || lo > tc.lastIndex {
		return
	}

	if lo <= tc.firstEntry().index {
		tc.reset()
		return
	}

	for i := len(tc.cache) - 1; i >= 0; i-- {
		// lo is in between tc.cache[i].index and tc.cache[i+1].index
		if lo > tc.cache[i].index {
			tc.cache = tc.cache[:i+1]
			tc.lastIndex = lo - 1
			return
		}
		// lo matches a term flip index
		if lo == tc.cache[i].index {
			// remove everything starting from (including) this term flip index
			tc.cache = tc.cache[:i]
			if len(tc.cache) == 0 {
				tc.lastIndex = 0
			} else {
				// set lastIndex to be the index one before lo,
				tc.lastIndex = lo - 1
				// invariant after above assignment:
				// tc.lastIndex >= tc.cache[i-1].index
			}
			return
		}
	}
}

// append adds a new entryID to the cache.
// If the cache is full, the oldest entryID is removed.
func (tc *TermCache) append(newEntry entryID) error {
	if len(tc.cache) == 0 {
		tc.cache = append(tc.cache, newEntry)
		tc.lastIndex = tc.firstEntry().index
		return nil
	}

	// the entry index should be strictly increasing
	// the entry term should be increasing
	if newEntry.index <= tc.lastIndex ||
		newEntry.term < tc.lastEntry().term {
		return errInvalidEntryID
	}

	defer func() {
		// update the last entry of the cache
		tc.lastIndex = newEntry.index
	}()

	// if the term is the same as the last entry, update the last entry's index
	if newEntry.term == tc.lastEntry().term {
		return nil
	}

	// the newEntry has a higher term than the last entry
	// remove the first entry if the cache is full
	if uint64(len(tc.cache)) == tc.maxSize {
		// Shift elements left by one using copy
		copy(tc.cache, tc.cache[1:])
		// Reuse the last slot for the new entry
		tc.cache = tc.cache[:len(tc.cache)-1]
	}

	tc.cache = append(tc.cache, newEntry)

	return nil
}

// lastEntry returns the entryId symbolizing the latest term flip
// according to the term cache.
// The unstable log may have more up-to-date term flips.
// This is not the last index covered by the term cache.
func (tc *TermCache) lastEntry() entryID {
	return tc.cache[len(tc.cache)-1]
}

// firstEntry returns the entryId symbolizing the first term flip
// known to the term cache.
func (tc *TermCache) firstEntry() entryID {
	return tc.cache[0]
}

// ResetWithFirst clears the term cache and adds the first entry.
func (tc *TermCache) ResetWithFirst(term kvpb.RaftTerm, index kvpb.RaftIndex) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.reset()
	_ = tc.append(entryID{term, index})
}

// resetWithFirstNoLock is like ResetWithFirst but does not hold the lock.
func (tc *TermCache) resetWithFirstNoLock(term kvpb.RaftTerm, index kvpb.RaftIndex) {
	tc.reset()
	_ = tc.append(entryID{term, index})
}

// reset clears the term cache and resets the lastIndex to 0.
func (tc *TermCache) reset() {
	tc.cache = tc.cache[:0]
	tc.lastIndex = 0
}
