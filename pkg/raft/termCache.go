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
	"fmt"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/errors"
)

type TermCache struct {
	cache []entryID
	// lastIndex is the last index know to the TermCache that is in the raftLog
	// the entry at lastIndex has the same term as TermCache.cache's
	// last element's term
	lastIndex uint64
	maxSize   uint64
}

// ErrInvalidEntryID is returned when the supplied entryID
// is invalid for the operation.
var ErrInvalidEntryID = errors.New("invalid entry ID")

// ErrUnavailableInTermCache is returned when the term is unavailable in  cache.
// It can potentially still be found in a lower level cache (raft entry cache)
var ErrUnavailableInTermCache = errors.New("term not available")

// ErrInconsistent is returned when the term is not consistent with the raftLog
// Upon receiving this error, the caller shouldn't look further down the stack
var ErrInconsistent = errors.New("provided entry to termCache is inconsistent with raftLog")

var ErrTermCacheEmpty = errors.New("termCache is empty")

// NewTermCache initializes a TermCache with a fixed maxSize.
func NewTermCache(size uint64) *TermCache {
	fmt.Println("new term cache")
	return &TermCache{
		cache:     make([]entryID, 0, size),
		maxSize:   size,
		lastIndex: 0,
	}
}

// ScanAppend this is bad, we are scanning linearly through a
// potentially very large array
func (tc *TermCache) ScanAppend(entries []pb.Entry) error {
	fmt.Println("scan append")
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
	fmt.Println("Append()")
	if len(tc.cache) == 0 {
		tc.cache = append(tc.cache, newEntry)
		return nil
	}

	// the entry index should be strictly increasing
	// the entry term should be increasing
	if newEntry.index <= tc.lastIndex ||
		newEntry.term < tc.getLastFlipEntry().term {
		return ErrInvalidEntryID
	}

	defer func() {
		// update the last entry of the cache
		tc.lastIndex = newEntry.index
	}()

	// if the term is the same as the last entry, update the last entry's index
	if newEntry.term == tc.getLastFlipEntry().term {
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

// Match returns whether the entryID is in the TermCache.
// If it is in the termCache, then it is in the raftLog.
func (tc *TermCache) Match(argEntryId entryID) (bool, error) {
	fmt.Println("Match()")
	if len(tc.cache) == 0 {
		return false, ErrTermCacheEmpty
	}

	if argEntryId.index < tc.cache[0].index ||
		argEntryId.index > tc.lastIndex ||
		argEntryId.term < tc.cache[0].term ||
		argEntryId.term > tc.getLastFlipEntry().term {
		return false, ErrUnavailableInTermCache
	}

	if argEntryId.term == tc.getLastFlipEntry().term {
		// redundant comparison here
		if argEntryId.index >= tc.getLastFlipEntry().index && argEntryId.index <= tc.lastIndex {
			return true, nil
		}
		return false, ErrInconsistent
	}

	// tc.cache[i].index < tc.cache[i+1].index is equivalent to
	// tc.cache[i].index <= tc.cache[i+1].index-1
	for i := 0; i < len(tc.cache)-1; i++ {
		if argEntryId.term == tc.cache[i].term &&
			argEntryId.index >= tc.cache[i].index &&
			argEntryId.index < tc.cache[i+1].index {
			return true, nil
		}
	}

	return false, ErrInconsistent
}

// Term returns the entry term based on the given entry index
// Returns error if not in the termCache
func (tc *TermCache) Term(index uint64) (term uint64, err error) {
	fmt.Println("Term()")
	if len(tc.cache) == 0 {
		return 0, ErrTermCacheEmpty
	}

	if index < tc.getFirstTermCacheEntry().index ||
		index > tc.lastIndex {
		return 0, ErrUnavailableInTermCache
	}

	// in last term of termCache
	if index >= tc.getLastFlipEntry().index && index <= tc.lastIndex {
		return tc.getLastFlipEntry().term, nil
	}

	for i := 0; i < len(tc.cache)-1; i++ {
		if index >= tc.cache[i].index && index < tc.cache[i+1].index {
			return tc.cache[i].term, nil
		}
	}

	// shouldn't get here
	return 0, ErrUnavailableInTermCache
}

// getLastFlipEntry returns the entryId symbolizing the latest term flip
// according to the termCache.
func (tc *TermCache) getLastFlipEntry() entryID {
	return tc.cache[len(tc.cache)-1]
}

func (tc *TermCache) getFirstTermCacheEntry() entryID {
	return tc.cache[0]
}

// truncate
// no need for truncate for now, assume the caller doesn't query term cache for
// compacted entries
