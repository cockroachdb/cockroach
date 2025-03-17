// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raft

import (
	"fmt"
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestTermCacheInitialization(t *testing.T) {
	tc := NewTermCache(8, entryID{term: 5, index: 10})

	// Verify basic properties after initialization
	require.Len(t, tc.cache, 1)
	require.Equal(t, entryID{term: 5, index: 10}, tc.firstEntry())
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

func TestTermCachePopulatedTruncateAndAppend(t *testing.T) {
	testCases := []struct {
		name      string
		cacheSize uint64
		init      LogSlice // Initial logslice to populate cache
		app       LogSlice // LogSlice to append after initial population
		want      LogSlice
	}{
		// Test The first batch of append after initialization.
		{
			name:      "evicts oldest entries when full",
			cacheSize: 4,
			// Initial: [t6/12, t7/14, t8/16, t9/17]
			init: entryID{term: 5, index: 10}.append(5, 6, 6, 7, 7, 8, 9).LogSlice,
			// Append: dummy
			app: entryID{term: 9, index: 17}.append().LogSlice,
			// Test case: [t6/12, t6/13, t7/14, t7/15, t8/16, t9/17, t9/18]
			want: entryID{term: 0, index: 11}.append(6, 6, 7, 7, 8, 9, 9).LogSlice,
		},
		{
			name:      "appends small amounts of term flips, not full, no evict",
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12]
			init: entryID{term: 5, index: 10}.append(6, 7).LogSlice,
			// Append: dummy
			app: entryID{term: 7, index: 13}.append().LogSlice,
			// Test case: [t6/11, t6/12, t7/13, t7/14, t8/15, t9/16, t9/17]
			want: entryID{term: 0, index: 9}.append(5, 6, 7, 7).LogSlice,
		},
		// Test various scenarios of append after initial append.
		{
			name:      "append to half-filled cache",
			cacheSize: 4,
			// Initial: [t5/10, t5/11, t6/12, t6/13]
			init: entryID{term: 5, index: 10}.append(5, 6, 6).LogSlice,
			// Append: [t6/13, t7/14, t8/15]
			app: entryID{term: 6, index: 13}.append(7, 8).LogSlice,
			// Test case: [t5/10, t5/11, t6/12, t6/13, t7/14, t8/15, t8/16]
			want: entryID{term: 0, index: 9}.append(5, 5, 6, 6, 7, 8, 8).LogSlice,
		},
		{
			name:      "cause eviction, no overwrite",
			cacheSize: 4,
			// Initial: [t5/10, t6/11]
			init: entryID{term: 5, index: 10}.append(6).LogSlice,
			// Append: [t6/11, t7/12, t8/13, t9/14]
			app: entryID{term: 6, index: 11}.append(7, 8, 9).LogSlice,
			// Test case: [t6/11, t7/12, t8/13, t9/14, t9/15, t9/16]
			want: entryID{term: 0, index: 10}.append(6, 7, 8, 9, 9, 9).LogSlice,
		},
		{
			name:      "overwriting with new entries",
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t5/10, t11/11, t12/12, t13/13]
			app: entryID{term: 5, index: 10}.append(11, 12, 13).LogSlice,
			// Test case: [t5/10, t11/11, t12/12, t13/13, t13/14, t13/15]
			want: entryID{term: 0, index: 9}.append(5, 11, 12, 13, 13, 13).LogSlice,
		},
		{
			name:      "repeated term appends won't modify the term cache",
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t8/13, t8/14, t8/15, ... many repeating terms]
			app: entryID{term: 8, index: 13}.append(8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8).LogSlice,
			// Test case: [t6/11, t7/12, t8/13, t9/14, t9/15, t9/16]
			want: entryID{term: 0, index: 9}.append(5, 6, 7, 8, 8, 8).LogSlice,
		},
		{
			name:      "overwrite test case",
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t5/10, t7/11, t8/12, ... t13/17, t13/18, ...]
			app: entryID{term: 5, index: 10}.append(7, 8, 9, 10, 11, 12, 13, 13, 13, 13, 13, 13).LogSlice,
			// Test case: [t10/14, t11/15, t12/16, t13/17, t13/18, ...]
			want: entryID{term: 0, index: 13}.append(10, 11, 12, 13, 13, 13, 13, 13, 13, 13, 13).LogSlice,
		},
		{
			name:      "extreme but possible, overwrite with LogSlice.prev and entry",
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t4/9, t10/10]
			app: entryID{term: 4, index: 9}.append(10).LogSlice,
			// Test case: [t4/9, t10/10, t10/11, t10/12]
			want: entryID{term: 0, index: 8}.append(4, 10, 10).LogSlice,
		},
		{
			name:      "overwrite with just one entry",
			cacheSize: 4,
			// Initial: [t5/10, t6/11, t7/12, t8/13]
			init: entryID{term: 5, index: 10}.append(6, 7, 8).LogSlice,
			// Append: [t5/10, t10/11]
			app: entryID{term: 5, index: 10}.append(10).LogSlice,
			// Test case: [t5/10, t10/11, t10/12, t10/13]
			want: entryID{term: 0, index: 9}.append(5, 10, 10, 10).LogSlice,
		},
		{
			name:      "multiple gaps in between, test term",
			cacheSize: 4,
			// Initial: [t5/10, t6/11]
			init: entryID{term: 5, index: 10}.append(6).LogSlice,
			// Append: [t7/12, t7/13, t7/14, t9/15, t9/16, t9/17, t10/18, t10/19, t10/20]
			app: entryID{term: 6, index: 11}.append(7, 7, 7, 9, 9, 9, 10, 10, 10).LogSlice,
			// Test case: [t6/11, t7/12, t7/13, t7/14, t9/15, t9/16, t9/17, t10/18, t10/19, t10/20]
			want: entryID{term: 6, index: 10}.append(6, 7, 7, 7, 9, 9, 9, 10, 10, 10).LogSlice,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize cache with the first entry of init
			cache := NewTermCache(tc.cacheSize, tc.init.prev)
			cache.checkInvariants(t)
			// First append to establish initial state
			cache.truncateAndAppend(tc.init)
			cache.checkInvariants(t)
			// Second append with new entries
			cache.truncateAndAppend(tc.app)
			cache.checkInvariants(t)

			func(t *testing.T) {
				_, found := cache.term(tc.want.prev.index)
				require.False(t, found, "Looking up for term below the cache should result in not found")

				for _, expTermAtIdx := range tc.want.entries {
					actualTerm, _ := cache.term(expTermAtIdx.Index)
					require.Equal(t, expTermAtIdx.Term, actualTerm, "unexpected term for index %d", expTermAtIdx.Index)
				}
			}(t)
		})
	}
}

// TestTermCacheRandomInsertMemorySliceCapacity inserts randomly generated
// logSlices and test that the cache capacity never grows to more than 2x initial size
func TestTermCacheRandomInsertMemorySliceCapacity(t *testing.T) {
	// Test with different cache sizes
	cacheSizes := []uint64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}
	iterations := 500 // Number of random operations to perform

	for _, cacheSize := range cacheSizes {
		t.Run(fmt.Sprintf("cacheSize-%d", cacheSize), func(t *testing.T) {
			// Initialize cache with random starting point
			initialID := entryID{term: 600, index: 600}
			cache := NewTermCache(cacheSize, initialID)
			maxCapacityReached := 0
			// Perform random operations
			for i := 0; i < iterations; i++ {
				// Get current boundaries of the cache
				first := cache.firstEntry()
				last := cache.lastEntry()

				// Generate a random LogSlice
				avgLength := cacheSize * 10
				randomSlice := generateRandomLogSlice(first, last, int(avgLength))

				// Apply the operation
				cache.truncateAndAppend(randomSlice)
				cache.checkInvariants(t)

				maxCapacityReached = max(maxCapacityReached, cap(cache.cache))
				// Verify capacity hasn't grown indefinitely
				require.LessOrEqual(t, float64(cap(cache.cache)), float64(cacheSize)*2,
					"capacity should not grow indefinitely after iteration %d", i)
			}

			// Final verification
			t.Logf("Initial cache capacity: %d, MaxCapacityReached during run: %d,  "+
				"Final cache state - cap: %d, len: %d, first: %v, last: %v",
				cacheSize, maxCapacityReached,
				cap(cache.cache), len(cache.cache), cache.firstEntry(), cache.lastEntry())
		})
	}
}

// The benchmark tests show that a ring buffer is not necessary.
// Copying everything in a small fixed size slice and shifting it over
// by 1 index is fast enough (probably optimized internally by go compiler).
//
// When the termcachesize is relatively small(10),
// the performance difference between
// every entry is a termflip vs a termflip occurs every 30 entries is only 5%
func BenchmarkTermCacheScanAppendConstantTermFlips(b *testing.B) {
	// Create entries with term flips for each entry
	createEntries := func(count int, startIndex uint64, startTerm uint64) []pb.Entry {
		entries := make([]pb.Entry, count)
		for i := 0; i < count; i++ {
			entries[i] = pb.Entry{
				Index: startIndex + uint64(i),
				Term:  startTerm + uint64(i), // Each entry has a new term
			}
		}
		return entries
	}

	benchSizes := []int{10000, 100000, 1000000}
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("entries-%d", size), func(b *testing.B) {
			initialEntries := createEntries(2, 1, 1)
			entries := createEntries(size, 3, 3)
			initialSlice := LogSlice{prev: entryID{term: 5, index: 10}, entries: initialEntries}
			appendSlice := LogSlice{prev: entryID{term: 5 + 1, index: 10 + 1}, entries: entries}

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				cache := NewTermCache(100, entryID{1, 1})
				cache.truncateAndAppend(initialSlice)
				b.StartTimer()
				// This is the operation we're benchmarking
				cache.truncateAndAppend(appendSlice)
			}
		})
	}
}

func BenchmarkTermCacheScanAppendFewTermFlips(b *testing.B) {
	// Create entries with few term flips
	createEntriesWithFewFlips := func(count int, startIndex uint64, startTerm uint64) []pb.Entry {
		entries := make([]pb.Entry, count)
		currentTerm := startTerm
		oneOverTermFlipFreq := 30

		for i := 0; i < count; i++ {
			// Only flip term every 30 entries
			if i > 0 && i%oneOverTermFlipFreq == 0 {
				currentTerm++
			}
			entries[i] = pb.Entry{
				Index: startIndex + uint64(i),
				Term:  currentTerm,
			}
		}
		return entries
	}

	benchSizes := []int{10000, 100000, 1000000}
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("entries-%d", size), func(b *testing.B) {
			initialEntries := createEntriesWithFewFlips(2, 1, 1)
			initialSlice := LogSlice{prev: entryID{term: 5, index: 10}, entries: initialEntries}
			entries := createEntriesWithFewFlips(size, 3, 3)
			appendSlice := LogSlice{prev: entryID{term: 5 + 1, index: 10 + 1}, entries: entries}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				cache := NewTermCache(100, entryID{1, 1})
				cache.truncateAndAppend(initialSlice)
				b.StartTimer()

				// This is the operation we're benchmarking
				cache.truncateAndAppend(appendSlice)
			}
		})
	}
}

func (tc *termCache) checkInvariants(t *testing.T) {
	// check termCache is non-empty
	require.NotEqual(t, 0, len(tc.cache), "termCache should never be empty")

	// check termCache has increasing entries and terms
	for i := 1; i < len(tc.cache); i++ {
		if tc.cache[i].index <= tc.cache[i-1].index {
			t.Errorf("termCache index is not strictly increasing: %d <= %d", tc.cache[i].index, tc.cache[i-1].index)
		}
		if tc.cache[i].term < tc.cache[i-1].term {
			t.Errorf("termCache term is not monotonically increasing: %d < %d", tc.cache[i].term, tc.cache[i-1].term)
		}
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
func generateRandomLogSlice(startID, endID entryID, averageLength int) (lg LogSlice) {
	if startID.index > endID.index || startID.term > endID.term {
		panic("startID must be less than endID")
	}
	if startID.index == 0 || startID.term == 0 {
		panic("startID must be higher than zero")
	}
	// Determine append type by random chance
	appendType := rand.Intn(3) // 0: regular, 1: partial overwrite, 2: complete overwrite

	var prevID entryID
	switch appendType {
	case 0: // Regular append (prev == end)
		prevID = endID
	case 1: // Partial overwrite (prev between start and end)
		if startID.index == endID.index {
			prevID = endID
		}
		if startID.index < endID.index {
			randomIdx := rand.Intn(int(endID.index - startID.index))
			prevID = entryID{term: endID.term, index: startID.index + uint64(randomIdx)}
		}
	case 2: // Complete overwrite (prev < start)
		prevID = entryID{term: startID.term - 1, index: startID.index - 1}
	}

	entryCount := 1
	if averageLength > 1 {
		entryCount = rand.Intn(averageLength*2) + 1
	}

	// Generate terms with possible term flips
	terms := make([]uint64, entryCount)
	currentTerm := prevID.term + 1 // Start with term greater than prev

	for i := range terms {
		// 90% chance of a term flip
		if i > 0 && rand.Float32() < 0.5 {
			currentTerm++
		}
		terms[i] = currentTerm
	}

	ls := prevID.append(terms...).LogSlice

	return ls
}
