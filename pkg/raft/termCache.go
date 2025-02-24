// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/errors"
)

type TermCache struct {
	cache []entryID
	// lastIndex is the last index known to the TermCache that is in the raftLog
	// the entry at lastIndex has the same term as TermCache.cache's
	// last element's term
	// lastIndex == 0 means TermCache is empty
	lastIndex uint64
	// max size of the term cache slice
	maxSize uint64
}

// ErrInvalidEntryID is returned when the supplied entryID
// is invalid for the operation.
var ErrInvalidEntryID = errors.New("invalid entry ID")

// ErrUnavailableInTermCache is returned when the term is unavailable in  cache.
// It can potentially still be found in a lower level cache (raft entry cache)
var ErrUnavailableInTermCache = errors.New("term not available")

// ErrTermCacheEmpty is returned when the term cache is empty
var ErrTermCacheEmpty = errors.New("termCache is empty")

// NewTermCache initializes a TermCache with a fixed maxSize.
func NewTermCache(size uint64) *TermCache {
	return &TermCache{
		cache:     make([]entryID, 0, size),
		maxSize:   size,
		lastIndex: 0,
	}
}

// truncateFrom clears all entries from the termCache with index equal to or
// greater than lo. Note that lo itself may or may not be in the cache.
// If lo is lower than the first entry index, the whole term cache is cleared.
// Mirrors the truncateFrom function in raftentry/cache.go
func (tc *TermCache) truncateFrom(lo uint64) error {
	if len(tc.cache) == 0 || lo > tc.lastIndex {
		return nil
	}

	if lo <= tc.firstEntry().index {
		tc.reset()
		return nil
	}

	for i := len(tc.cache) - 1; i >= 0; i-- {
		// lo is in between tc.cache[i].index and tc.cache[i+1].index
		if lo > tc.cache[i].index {
			tc.cache = tc.cache[:i+1]
			tc.lastIndex = lo - 1
			return nil
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
			return nil
		}
	}
	return nil
}

// ClearTo clears entries from the TermCache with index strictly less than hi.
// If hi is above the lastIndex, the whole term cache is cleared.
// Mirrors the clearTo function in raftentry/cache.go
func (tc *TermCache) ClearTo(hi uint64) error {
	if len(tc.cache) == 0 || hi <= tc.firstEntry().index {
		return nil
	}

	// special cases:
	if hi > tc.lastIndex {
		tc.reset()
		return nil
	}

	// hi is above last entry's index, but lower or equal to lastIndex
	if hi > tc.lastEntry().index {
		tc.ResetWithFirst(tc.lastEntry().term, hi)
		return nil
	}

	// only keep the last entry
	if hi == tc.lastEntry().index {
		tc.cache = tc.cache[len(tc.cache)-1:]
		return nil
	}

	// general cases
	for i := 0; i < len(tc.cache)-1; i++ {
		// hi matches a term flip index
		if hi == tc.cache[i].index {
			tc.cache = tc.cache[i:]
			return nil
		}

		// Allow the first entry in the termCache to not represent a term flip point
		// cache[0] only tells us entries are in term cache[0].term
		// starting from cache[0].index up to
		// min(cache[i+1].index-1 /* if not nil*/, lastIndex)
		if hi > tc.cache[i].index && hi < tc.cache[i+1].index {
			tc.cache = tc.cache[i:]
			tc.cache[i].index = hi
			return nil
		}
	}
	return nil
}

// ScanAppend appends a list of raft entries to the TermCache
// It is the caller's responsibility to ensure entries is a valid raftLog.
func (tc *TermCache) ScanAppend(entries []pb.Entry, truncate bool) error {
	if len(entries) == 0 {
		return nil
	}

	if truncate {
		truncIdx := entries[0].Index
		_ = tc.truncateFrom(truncIdx)
	}

	for _, ent := range entries {
		if err := tc.Append(entryID{ent.Term, ent.Index}); errors.Is(err, ErrInvalidEntryID) {
			continue
		}
	}
	return nil
}

// Append adds a new entryID to the cache.
// If the cache is full, the oldest entryID is removed.
func (tc *TermCache) Append(newEntry entryID) error {
	if len(tc.cache) == 0 {
		tc.cache = append(tc.cache, newEntry)
		tc.lastIndex = tc.firstEntry().index
		return nil
	}

	// the entry index should be strictly increasing
	// the entry term should be increasing
	if newEntry.index <= tc.lastIndex ||
		newEntry.term < tc.lastEntry().term {
		return ErrInvalidEntryID
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
		tc.cache = tc.cache[1:]
	}

	tc.cache = append(tc.cache, newEntry)
	return nil
}

// Term returns the entry term based on the given entry index.
// Returns error if not in the termCache.
func (tc *TermCache) Term(index uint64) (term uint64, err error) {
	if len(tc.cache) == 0 {
		return 0, ErrTermCacheEmpty
	}

	if index < tc.firstEntry().index ||
		index > tc.lastIndex {
		return 0, ErrUnavailableInTermCache
	}

	// in last term of termCache, index <= tc.lastIndex
	if index >= tc.lastEntry().index {
		return tc.lastEntry().term, nil
	}

	for i := len(tc.cache) - 2; i >= 0; i-- {
		if index >= tc.cache[i].index {
			return tc.cache[i].term, nil
		}
	}

	return 0, ErrUnavailableInTermCache
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
func (tc *TermCache) ResetWithFirst(term uint64, index uint64) {
	tc.reset()
	tc.cache = append(tc.cache, entryID{term, index})
}

// reset clears the term cache and resets the lastIndex to 0.
func (tc *TermCache) reset() {
	tc.cache = tc.cache[:0]
	tc.lastIndex = 0
}
