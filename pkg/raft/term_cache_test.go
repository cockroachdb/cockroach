// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raft

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	"github.com/stretchr/testify/require"
)

func TestTermCacheInitialization(t *testing.T) {
	tc := NewTermCache(8, entryID{term: 5, index: 10})

	// Verify basic properties after initialization
	require.Len(t, tc.cache, 1)
	require.Equal(t, entryID{term: 5, index: 10}, tc.first())
	require.Equal(t, uint64(10), tc.cache[0].index)
	require.Equal(t, 8, cap(tc.cache))
}

func TestTermCacheTerm(t *testing.T) {
	// Create a term cache with initial entry at term=5, index=10
	initialID := entryID{term: 5, index: 10}
	tc := NewTermCache(4, initialID)

	// Create a log slice with entries that have different term patterns
	// This creates entries at indices 11-14 with terms [5,6,6,7]
	leadSlice := initialID.append(5, 6, 6, 7)
	tc.truncateAndAppend(leadSlice.LogSlice)
	tc.checkInvariants(t)

	// Test term lookup for various indices
	for _, tt := range []struct {
		name     string
		index    uint64
		wantTerm uint64
		wantOK   bool
	}{
		{name: "below cache", index: 9, wantTerm: 0, wantOK: false},
		{name: "initial entry", index: 10, wantTerm: 5, wantOK: true},
		{name: "same term continuation", index: 11, wantTerm: 5, wantOK: true},
		{name: "term flip", index: 12, wantTerm: 6, wantOK: true},
		{name: "same term", index: 13, wantTerm: 6, wantOK: true},
		{name: "another term flip", index: 14, wantTerm: 7, wantOK: true},
		// NB: Although index 15 is not inserted to the cache,
		// we still return term = 7. Callers should make sure
		// that index <= lastIndex when calling term(index).
		{name: "beyond cache", index: 15, wantTerm: 7, wantOK: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			term, ok := tc.term(tt.index)
			require.Equal(t, tt.wantTerm, term, "index %d", tt.index)
			require.Equal(t, tt.wantOK, ok, "index %d", tt.index)
		})
	}
}

func TestTermCacheLogSnapshot(t *testing.T) {
	initLeadSlice := entryID{term: 0, index: 0}.append(1, 2, 3, 4, 5, 6)
	initEntries := initLeadSlice.LogSlice.entries

	s := NewMemoryStorage()
	raftLog := newLog(s, raftlogger.DiscardLogger)
	// hardcode the maxSize to 4 for testing
	raftLog.termCache.maxSize = 4
	raftLog.append(initLeadSlice)
	origCap := cap(raftLog.termCache.cache)

	// Preserve the log snapshot which includes a reference to term cache.
	snap := raftLog.snap(s)
	// where the termCache starts to cover in the initEntries.
	offset := len(initEntries) - int(raftLog.termCache.maxSize)
	// The term cache must be immutable regardless of mutations on the raftLog.
	check := func() {
		// Check that the term cache referenced by the snapshot
		// has not been mutated by appends.
		for _, ent := range initEntries[offset:] {
			term, found := snap.termCache.term(ent.Index)
			require.True(t, found)
			require.Equal(t, ent.Term, term)
			require.Equal(t, origCap, cap(snap.termCache.cache))
		}
	}
	check()
	require.True(t, raftLog.append(entryID{term: 6, index: 6}.append(6, 6, 6, 6))) // regular append, no term flip
	check()
	require.True(t, raftLog.maybeAppend(entryID{term: 4, index: 4}.append(7, 8, 9, 10))) // partial overwrite
	check()
	require.True(t, raftLog.maybeAppend(entryID{term: 1, index: 1}.append(11, 12, 13, 14))) // complete overwrite
	check()
}

func TestTermCachePopulatedTruncateAndAppend(t *testing.T) {
	for _, tt := range []struct {
		cacheSize uint64
		// Initial log slice to populate cache.
		init LogSlice
		// Log slice to append after initial population.
		app LogSlice
		// Checking for term look-ups at index.
		// want.prev.index should reference the index one below the cache.first(),
		// which the term cache no longer cover during check
		// looking up cache.term(want.entries[i].index) should return want.entries[i].term, true
		want LogSlice
	}{
		// Test the first batch of append after initialization.
		{
			// Evicts oldest entries when full.
			cacheSize: 4,
			// Initial: [t6/12, t7/14, t8/16, t9/17]
			init: entryID{term: 5, index: 10}.append(5, 6, 6, 7, 7, 8, 9).LogSlice,
			// Append: dummy
			app: entryID{term: 9, index: 17}.append().LogSlice,
			// Cached: [t6/12, t6/13, t7/14, t7/15, t8/16, t9/17]
			want: entryID{term: 0, index: 11}.append(6, 6, 7, 7, 8, 9).LogSlice,
		},
		{
			// Appends small amounts of term flips, not full, no evict.
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12]
			init: entryID{term: 5, index: 10}.append(6, 7).LogSlice,
			// Append: dummy
			app: entryID{term: 7, index: 13}.append().LogSlice,
			// Cached: [t5/10, t6/11, t7/12, t7/13]
			want: entryID{term: 0, index: 9}.append(5, 6, 7, 7).LogSlice,
		},
		// Test various scenarios of append after initial append.
		{
			// Append to half-filled cache.
			cacheSize: 4,
			// Initial: [t5/10, t5/11, t6/12, t6/13]
			init: entryID{term: 5, index: 10}.append(5, 6, 6).LogSlice,
			// Append: [t6/13, t7/14, t8/15]
			app: entryID{term: 6, index: 13}.append(7, 8).LogSlice,
			// Cached: [t5/10, t5/11, t6/12, t6/13, t7/14, t8/15]
			want: entryID{term: 0, index: 9}.append(5, 5, 6, 6, 7, 8).LogSlice,
		},
		{
			// Cause eviction, no overwrite.
			cacheSize: 4,
			// Initial: [t5/10, t6/11]
			init: entryID{term: 5, index: 10}.append(6).LogSlice,
			// Append: [t6/11, t7/12, t8/13, t9/14]
			app: entryID{term: 6, index: 11}.append(7, 8, 9).LogSlice,
			// Cached: [t6/11, t7/12, t8/13, t9/14]
			want: entryID{term: 0, index: 10}.append(6, 7, 8, 9).LogSlice,
		},
		{
			// Overwriting with new entries.
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t5/10, t11/11, t12/12, t13/13]
			app: entryID{term: 5, index: 10}.append(11, 12, 13).LogSlice,
			// Cached: [t5/10, t11/11, t12/12, t13/13]
			want: entryID{term: 0, index: 9}.append(5, 11, 12, 13).LogSlice,
		},
		{
			// Repeated term appends won't modify the term cache.
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t8/13, t8/14, t8/15, ... many repeating terms]
			app: entryID{term: 8, index: 13}.append(8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8).LogSlice,
			// Cached: [t5/10, t6/11, t7/12, t8/13, t8/14, t8/15]
			want: entryID{term: 0, index: 9}.append(5, 6, 7, 8, 8, 8).LogSlice,
		},
		{
			// Overwrite test case.
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t5/10, t7/11, t8/12, t9/13, t10/14, t11/15, t12/16, t13/17, t13/18, ...]
			app: entryID{term: 5, index: 10}.append(7, 8, 9, 10, 11, 12, 13, 13, 13, 13, 13, 13).LogSlice,
			// Cached: [t10/14, t11/15, t12/16, t13/17, t13/18, ...]
			want: entryID{term: 0, index: 13}.append(10, 11, 12, 13, 13, 13, 13, 13, 13).LogSlice,
		},
		{
			// Extreme but possible, overwrite with LogSlice.prev and entry.
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t4/9, t10/10]
			app: entryID{term: 4, index: 9}.append(10).LogSlice,
			// Cached: [t4/9, t10/10]
			want: entryID{term: 0, index: 8}.append(4, 10).LogSlice,
		},
		{
			// Overwrite with just one entry.
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t5/10, t10/11]
			app: entryID{term: 5, index: 10}.append(10).LogSlice,
			// Cached: [t5/10, t10/11]
			want: entryID{term: 0, index: 9}.append(5, 10).LogSlice,
		},
		{
			// Multiple gaps in between, test term.
			cacheSize: 4,
			// Initial: [t5/10, t6/11]
			init: entryID{term: 5, index: 10}.append(6).LogSlice,
			// Append: [t7/12, t7/13, t7/14, t9/15, t9/16, t9/17, t10/18, t10/19, t10/20]
			app: entryID{term: 6, index: 11}.append(7, 7, 7, 9, 9, 9, 10, 10, 10).LogSlice,
			// Cached: [t6/11, t7/12, t7/13, t7/14, t9/15, t9/16, t9/17, t10/18, t10/19, t10/20]
			want: entryID{term: 0, index: 10}.append(6, 7, 7, 7, 9, 9, 9, 10, 10, 10).LogSlice,
		},
	} {
		t.Run("", func(t *testing.T) {
			// Initialize cache with the first entry of init
			cache := NewTermCache(tt.cacheSize, tt.init.prev)
			cache.checkInvariants(t)
			// First append to establish initial state
			cache.truncateAndAppend(tt.init)
			cache.checkInvariants(t)
			// Second append with new entries
			cache.truncateAndAppend(tt.app)
			cache.checkInvariants(t)

			// tt.want.prev.index represents one index below what is covered by cache
			// Anything at or below prev should also return found == false
			_, found := cache.term(tt.want.prev.index)
			require.False(t, found, "Looking up for term below the cache should result in not found")

			for _, expTermAtIdx := range tt.want.entries {
				actualTerm, found := cache.term(expTermAtIdx.Index)
				require.True(t, found, "Should find term at index %d", expTermAtIdx.Index)
				require.Equal(t, expTermAtIdx.Term, actualTerm, "unexpected term for index %d", expTermAtIdx.Index)
			}

			// Explicit check that looking up a few indices above the last entry appended
			// to cache returns the same term as the last appended entry's term.
			// Assuming want.last to be the last entryID appended to cache.
			last := len(tt.want.entries) - 1
			for i := tt.want.entries[last].Index; i < 3; i++ {
				term, found := cache.term(i)
				require.True(t, found)
				require.Equal(t, tt.want.entries[last].Term, term,
					"Should get same term as the last term as want.last.term: %d", tt.want.entries[last].Term)
			}
		})
	}
}

// TestTermCacheRandomInsertMemorySliceCapacity inserts randomly generated
// logSlices and test for term cache invariants.
func TestTermCacheRandomInsertMemorySliceCapacity(t *testing.T) {
	// Test with different cache sizes
	cacheSizes := []uint64{1, 2, 4, 8}
	const iterations = 500 // Number of random operations to perform

	for _, cacheSize := range cacheSizes {
		t.Run(fmt.Sprintf("cacheSize-%d", cacheSize), func(t *testing.T) {
			// Initialize cache with random starting point
			initialID := entryID{term: 50, index: 100}
			cache := NewTermCache(cacheSize, initialID)
			// Perform random operations
			for i := 0; i < iterations; i++ {
				// Get current boundaries of the cache
				first := cache.first()
				last := cache.last()

				// Generate a random LogSlice
				avgLength := int(cacheSize * 50)
				randomSlice := generateRandomLogSlice(t, first, last, avgLength)

				// Apply the operation
				cache.truncateAndAppend(randomSlice)
				cache.checkInvariants(t)
			}
			// // Final verification
			// t.Logf("Initial cache capacity: %d, MaxCapacityReached during run: %d,  "+
			// 	"Final cache state - cap: %d, len: %d, first: %v, last: %v",
			// 	cacheSize, maxCapacityReached,
			// 	cap(cache.cache), len(cache.cache), cache.first(), cache.last())
		})
	}
}

func (tc *termCache) checkInvariants(t *testing.T) {
	// check termCache is non-empty
	require.NotEmptyf(t, tc.cache, "termCache should never be empty")
	// check we haven't grown past 2x maxLength
	require.LessOrEqual(t, cap(tc.cache), int(tc.maxSize*2))
	// check termCache has increasing entries and terms
	for i := 1; i < len(tc.cache); i++ {
		require.Greater(t, tc.cache[i].index, tc.cache[i-1].index)
		require.Greater(t, tc.cache[i].term, tc.cache[i-1].term)
	}
}

// generateRandomLogSlice creates a LogSlice with randomly generated entries for testing term cache behavior.
// It simulates three scenarios based on random chance:
//  1. Regular append: starts after endID
//  2. Partial overwrite: starts between startID and endID
//  3. Complete overwrite: starts before startID
//
// Parameters:
//   - startID, endID: Define the range boundaries in the current term cache
//   - averageLength: Controls entry count, generating between 1 and 2*averageLength entries
//
// The function ensures all generated entries maintain required term ordering invariants.
func generateRandomLogSlice(t *testing.T, startID, endID entryID, averageLength int) (lg LogSlice) {
	require.False(t, startID.index > endID.index || startID.term > endID.term,
		"startID must be less than endID")
	require.False(t, startID.index == 0 || startID.term == 0,
		"startID must be less than endID")

	// Determine append type by random chance
	appendType := rand.Intn(3) // 0: regular, 1: partial overwrite, 2: complete overwrite

	prevID := endID
	switch appendType {
	case 0: // Regular append (prev == end)
	case 1: // Partial overwrite (prev between start and end)
		offset := rand.Int63n(int64(endID.index - startID.index + 1))
		prevID = entryID{term: endID.term, index: startID.index + uint64(offset)}
	case 2: // Complete overwrite (prev < start)
		prevID = entryID{term: startID.term - 1, index: startID.index - 1}
	}

	entryCount := rand.Intn(averageLength*2) + 1

	// Generate terms with possible term flips
	terms := make([]uint64, entryCount)
	currentTerm := prevID.term

	for i := range terms {
		// 50% chance of a term flip
		if rand.Float32() < 0.5 {
			currentTerm++
		}
		terms[i] = currentTerm
	}

	return prevID.append(terms...).LogSlice
}
