// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raft

import (
	"fmt"
	"math/rand"
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func TestTermCacheInitialization(t *testing.T) {
	tc := newTermCache(8, entryID{term: 5, index: 10})
	tc.checkInvariants(t)

	// Verify basic properties after initialization
	require.Len(t, tc.cache, 1)
	require.Equal(t, entryID{term: 5, index: 10}, tc.first())
	require.Equal(t, uint64(10), tc.cache[0].index)
	require.Equal(t, 8, cap(tc.cache))
}

// TestTermCacheTruncateAndAppend verifies the copy-on-write behavior of the
// term cache. In all scenarios of new appends, a previously taken copy of the
// term cache should be immutable.
func TestTermCacheCopyImmutability(t *testing.T) {
	tc := newTermCache(4, entryID{term: 5, index: 10})
	tc.truncateAndAppend(entryID{term: 5, index: 10}.append(5, 6, 6, 7, 7, 8, 9).LogSlice)
	cacheCopy := tc
	deep := make([]entryID, len(tc.cache))
	copy(deep, tc.cache)

	// The term cache must be immutable regardless of mutations on the raftLog.
	check := func() {
		require.Equal(t, deep, cacheCopy.cache)
	}
	check()
	// regular append
	tc.truncateAndAppend(entryID{term: 9, index: 17}.append(9, 10, 10, 10).LogSlice)
	check()
	// partial overwrite
	tc.truncateAndAppend(entryID{term: 10, index: 19}.append(15, 15, 16, 16).LogSlice)
	check()
	// complete overwrite
	tc.truncateAndAppend(entryID{term: 4, index: 4}.append(4, 4, 5, 5, 6).LogSlice)
	check()
	// long append with term changes causing eviction
	tc.truncateAndAppend(entryID{term: 4, index: 4}.append(4, 5, 6, 7, 8, 9, 10, 11).LogSlice)
	check()
	tc.reset(entryID{term: 100, index: 100})
	check()
}

func TestTermCachePopulatedTruncateAndAppend(t *testing.T) {
	for _, tt := range []struct {
		cacheSize uint64
		// Initial log slice to populate cache.
		init LogSlice
		// Log slice to append after initial population.
		app LogSlice
		// want is the list of entries that should be covered by the term cache.
		// Entries below should be not found. For indices above, the term cache
		// should return the last term.
		want []pb.Entry
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
			want: index(12).terms(6, 6, 7, 7, 8, 9),
		}, {
			// Appends small amounts of term flips, not full, no evict.
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12]
			init: entryID{term: 5, index: 10}.append(6, 7).LogSlice,
			// Append: dummy
			app: entryID{term: 7, index: 12}.append().LogSlice,
			// Cached: [t5/10, t6/11, t7/12]
			want: index(10).terms(5, 6, 7),
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
			want: index(10).terms(5, 5, 6, 6, 7, 8),
		}, {
			// Cause eviction, no overwrite.
			cacheSize: 4,
			// Initial: [t5/10, t6/11]
			init: entryID{term: 5, index: 10}.append(6).LogSlice,
			// Append: [t6/11, t7/12, t8/13, t9/14]
			app: entryID{term: 6, index: 11}.append(7, 8, 9).LogSlice,
			// Cached: [t6/11, t7/12, t8/13, t9/14]
			want: index(11).terms(6, 7, 8, 9),
		}, {
			// Overwriting with new entries.
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t5/10, t11/11, t12/12, t13/13]
			app: entryID{term: 5, index: 10}.append(11, 12, 13).LogSlice,
			// Cached: [t5/10, t11/11, t12/12, t13/13]
			want: index(10).terms(5, 11, 12, 13),
		}, {
			// Repeated term appends won't modify the term cache.
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t8/13, t8/14, t8/15, ... many repeating terms]
			app: entryID{term: 8, index: 13}.append(8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8).LogSlice,
			// Cached: [t5/10, t6/11, t7/12, t8/13, t8/14, t8/15]
			want: index(10).terms(5, 6, 7, 8, 8, 8),
		}, {
			// Overwrite test case.
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t5/10, t7/11, t8/12, t9/13, t10/14, t11/15, t12/16, t13/17, t13/18, ...]
			app: entryID{term: 5, index: 10}.append(7, 8, 9, 10, 11, 12, 13, 13, 13, 13, 13, 13).LogSlice,
			// Cached: [t10/14, t11/15, t12/16, t13/17, t13/18, ...]
			want: index(14).terms(10, 11, 12, 13, 13, 13, 13, 13, 13),
		}, {
			// Extreme but possible, overwrite with LogSlice.prev and entry.
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t4/9, t10/10]
			app: entryID{term: 4, index: 9}.append(10).LogSlice,
			// Cached: [t4/9, t10/10]
			want: index(9).terms(4, 10),
		}, {
			// Overwrite with just one entry.
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t5/10, t10/11]
			app: entryID{term: 5, index: 10}.append(10).LogSlice,
			// Cached: [t5/10, t10/11]
			want: index(10).terms(5, 10),
		}, {
			// Multiple "term gaps" in between, test term.
			cacheSize: 4,
			// Initial: [t5/10, t6/11]
			init: entryID{term: 5, index: 10}.append(6).LogSlice,
			// Append: [t7/12, t7/13, t7/14, t9/15, t9/16, t9/17, t10/18, t10/19, t10/20]
			app: entryID{term: 6, index: 11}.append(7, 7, 7, 9, 9, 9, 10, 10, 10).LogSlice,
			// Cached: [t6/11, t7/12, t7/13, t7/14, t9/15, t9/16, t9/17, t10/18, t10/19, t10/20]
			want: index(11).terms(6, 7, 7, 7, 9, 9, 9, 10, 10, 10),
		}, {
			// Overwrite in the middle of the cache.
			cacheSize: 4,
			// Initial: [t5/10, t5/11, t5/12, t6/13, t6/14, t7/15]
			init: entryID{term: 5, index: 10}.append(5, 5, 6, 6, 7).LogSlice,
			// Append: [t8/13, t8/14, t9/15]
			app: entryID{term: 6, index: 13}.append(8, 8, 9).LogSlice,
			// Cached: [t5/10, t5/11, t5/12, t6/13, t8/14, t8/15, t9/16]
			want: index(10).terms(5, 5, 5, 6, 8, 8, 9),
		}, {
			// Overwrite in the middle of the cache.
			cacheSize: 4,
			// Initial: [t5/10, t5/11, t5/12, t6/13, t6/14, t7/15]
			init: entryID{term: 5, index: 10}.append(5, 5, 6, 6, 7).LogSlice,
			// Append: [t8/13, t8/14, t9/15]
			app: entryID{term: 6, index: 14}.append(8, 8, 9).LogSlice,
			// Cached: [t5/10, t5/11, t5/12, t6/13, t6/14, t8/15, t8/16, t9/17]
			want: index(10).terms(5, 5, 5, 6, 6, 8, 8, 9),
		},
	} {
		t.Run("", func(t *testing.T) {
			// Initialize cache with the first entry of init
			cache := newTermCache(tt.cacheSize, tt.init.prev)
			cache.checkInvariants(t)
			// First append to establish initial state
			cache.truncateAndAppend(tt.init)
			cache.checkInvariants(t)
			// Second append with new entries
			cache.truncateAndAppend(tt.app)
			cache.checkInvariants(t)

			// Anything below the first covered index should return found == false.
			_, found := cache.term(tt.want[0].Index - 1)
			require.False(t, found, "Looking up for term below the cache should result in not found")

			for _, entry := range tt.want {
				term, found := cache.term(entry.Index)
				require.True(t, found, "Should find term at index %d", entry.Index)
				require.Equal(t, entry.Term, term, "unexpected term for index %d", entry.Index)
			}

			// Explicit check that looking up a few indices above the last entry
			// appended to cache returns the same term as the last appended entry's
			// term.
			lastEnt := tt.want[len(tt.want)-1]
			for i, end := lastEnt.Index, lastEnt.Index+3; i < end; i++ {
				term, found := cache.term(i)
				require.True(t, found)
				require.Equal(t, lastEnt.Term, term,
					"Should get same term as the last term as want.last.term: %d", lastEnt.Term)
			}
		})
	}
}

// TestTermCacheRandomTruncateAndAppends inserts randomly generated log slices
// and tests for term cache invariants.
func TestTermCacheRandomTruncateAndAppends(t *testing.T) {
	const iterations = 500 // Number of random operations to perform

	// Test with different cache sizes
	for _, cacheSize := range []uint64{1, 2, 3, 4, 8} {
		t.Run(fmt.Sprintf("cacheSize-%d", cacheSize), func(t *testing.T) {
			// Initialize the cache with some starting point.
			cache := newTermCache(cacheSize, entryID{term: 50, index: 100})
			cache.checkInvariants(t)
			// Perform random operations.
			for i := 0; i < iterations; i++ {
				maxTermFlips := int(cacheSize * 2)
				ls := generateRandomLogSlice(t, cache.first(), cache.last(), maxTermFlips)
				cache.truncateAndAppend(ls)
				cache.checkInvariants(t)
			}
		})
	}
}

func (tc *termCache) checkInvariants(t testing.TB) {
	t.Helper()
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

// generateRandomLogSlice creates a LogSlice with randomly generated entries for
// testing term cache behaviors.
//
// It simulates three scenarios based on equal random probability:
//  1. Regular append: starts after endID
//  2. Partial overwrite: starts between startID and endID
//  3. Complete overwrite: starts before startID
//
// Parameters:
//   - startID, endID: Define the boundaries of a raft log (subset).
//   - maxTermFlips: Controls the number of term flips in the returned LogSlice.
//
// The returned entries maintain required term ordering invariants.
func generateRandomLogSlice(t *testing.T, startID, endID entryID, maxTermFlips int) LogSlice {
	require.False(t, startID.index > endID.index || startID.term > endID.term,
		"startID must be less than endID")
	require.False(t, startID.index == 0 || startID.term == 0,
		"startID must be less than endID")

	// Determine append type by random chance.
	appendType := rand.Intn(3)

	prevID := endID
	switch appendType {
	case 0: // Regular append (prev == end)
	case 1: // Partial overwrite (prev between start and end)
		offset := rand.Int63n(int64(endID.index - startID.index + 1))
		prevID = entryID{term: endID.term, index: startID.index + uint64(offset)}
	case 2: // Complete overwrite (prev < start)
		prevID = entryID{term: startID.term - 1, index: startID.index - 1}
	}

	// Generate terms with possible term flips.
	var terms []uint64
	currentTerm := prevID.term
	for i, numTerms := 0, rand.Intn(maxTermFlips)+1; i < numTerms; i++ {
		for entries := rand.Intn(10) + 1; entries > 0; entries-- {
			terms = append(terms, currentTerm)
		}
		currentTerm++
	}
	return prevID.append(terms...).LogSlice
}
