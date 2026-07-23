// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessiondatapb

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SequenceCacheNodeEntry embeds the existing SequenceCacheEntry struct that
// session-level sequence caching currently utilizes, and also includes a mutex.
type SequenceCacheNodeEntry struct {
	entry SequenceCacheEntry
	mu    syncutil.Mutex
}

// SequenceCacheNode stores sequence values at a per-node level
// The cache stores sequence values that have already been created in KV
// and are available to be given out as sequence numbers. Values for sequences
// are keyed by the descpb.ID of each sequence. These IDs are represented as
// uint32 to prevent an import cycle with the descpb package.
type SequenceCacheNode struct {
	cache map[catid.DescID]*SequenceCacheNodeEntry
	mu    syncutil.RWMutex
}

// NewSequenceCacheNode initializes a new SequenceCacheNode.
func NewSequenceCacheNode() *SequenceCacheNode {
	return &SequenceCacheNode{
		cache: map[catid.DescID]*SequenceCacheNodeEntry{},
	}
}

// NextValue fetches the next value in the sequence cache. If the values in the cache have all been
// given out or if the descriptor version has changed, then fetchNextValues() is used to repopulate the cache.
func (sc *SequenceCacheNode) NextValue(
	seqID catid.DescID, clientVersion uint32, fetchNextValues func() (int64, int64, int64, error),
) (int64, error) {
	sc.mu.RLock()
	cacheEntry, found := sc.cache[seqID]
	sc.mu.RUnlock()

	createSequenceCacheNodeEntry := func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		// There is a hazard that multiple threads could add the entry, so check if it exists again with the writer lock
		cacheEntry, found = sc.cache[seqID]
		if !found {
			cacheEntry = &SequenceCacheNodeEntry{}
			sc.cache[seqID] = cacheEntry
		}
	}
	// Create entry for this sequence ID if there are no existing entries.
	if !found {
		createSequenceCacheNodeEntry()
	}
	cacheEntry.mu.Lock()
	defer cacheEntry.mu.Unlock()

	if cacheEntry.entry.NumValues > 0 && cacheEntry.entry.CachedVersion == clientVersion {
		cacheEntry.entry.CurrentValue += cacheEntry.entry.Increment
		cacheEntry.entry.NumValues--
		return cacheEntry.entry.CurrentValue - cacheEntry.entry.Increment, nil
	}

	currentValue, increment, numValues, err := fetchNextValues()
	if err != nil {
		return 0, err
	}

	// One value must be returned, and the rest of the values are stored.
	val := currentValue
	cacheEntry.entry.CurrentValue = currentValue + increment
	cacheEntry.entry.Increment = increment
	cacheEntry.entry.NumValues = numValues - 1
	cacheEntry.entry.CachedVersion = clientVersion
	return val, nil

}
