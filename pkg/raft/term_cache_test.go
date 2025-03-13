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
)

// Test constants, the test cases are hardcoded based on current test constants.
var TestInitialTerm = uint64(5)
var TestInitialIndex = uint64(10)

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
	testCases := []struct {
		index    uint64
		wantTerm uint64
		wantOK   bool
	}{
		{9, 0, false}, // below cache
		{10, 5, true}, // initial entry
		{11, 5, true}, // same term continuation
		{12, 6, true}, // term flip
		{13, 6, true}, // same term
		{14, 7, true}, // another term flip
		// NB: Although index 15 is not inserted to the cache,
		// we still return term = 7. Callers should make sure
		// that index <= lastIndex when calling term(index).
		{15, 7, true}, // beyond cache
	}

	for _, tcase := range testCases {
		term, ok := tc.term(tcase.index)
		require.Equal(t, tcase.wantTerm, term, "index %d", tcase.index)
		require.Equal(t, tcase.wantOK, ok, "index %d", tcase.index)
	}
}

func TestTermCachePopulatedTruncateAndAppend(t *testing.T) {
	testCases := []struct {
		name            string
		cacheSize       uint64
		initialID       entryID
		initTerms       []uint64 // Initial terms to populate cache
		appendInitialID entryID
		appendTerms     []uint64 // Terms to append after initial population
		want            struct {
			cacheLen             int
			first                entryID
			last                 entryID
			indexAndExpectedTerm [][2]uint64 // Checks to verify terms at specific indices
		}
	}{
		{
			name:      "append to half-filled cache",
			cacheSize: 4,
			initialID: entryID{term: 5, index: 10},
			// Initial append fills the cache:
			// [t5/10, t6/12, t6/13]
			initTerms:       []uint64{5, 6, 6},
			appendInitialID: entryID{term: 6, index: 13},
			// Second append:
			// [t7/14, t8/15]
			appendTerms: []uint64{7, 8},
			want: struct {
				cacheLen             int
				first                entryID
				last                 entryID
				indexAndExpectedTerm [][2]uint64
			}{
				cacheLen: 4,
				first:    entryID{term: 5, index: 10}, // First entry
				last:     entryID{term: 8, index: 15}, // New last entry
			},
		},
		{
			name:      "cause eviction, no overwrite",
			cacheSize: 4,
			initialID: entryID{term: 5, index: 10},
			// Initial entries only fill half: [t5/10, t6/11]
			initTerms:       []uint64{6},
			appendInitialID: entryID{term: 6, index: 11},
			// Append more: [t7/12, t8/13, t9/14]
			appendTerms: []uint64{7, 8, 9},
			// Final term cache state: [t6/11, t7/12, t8/13, t9/14]
			want: struct {
				cacheLen             int
				first                entryID
				last                 entryID
				indexAndExpectedTerm [][2]uint64
			}{
				cacheLen: 4,                           // Now full
				first:    entryID{term: 6, index: 11}, // Nothing evicted yet
				last:     entryID{term: 9, index: 14}, // New last entry
			},
		},
		{
			name:      "overwriting with new entries",
			cacheSize: 4,
			initialID: entryID{term: 5, index: 10},
			// Initial entries: [t5/10, t6/11, t7/12, t8/13], already full
			initTerms:       []uint64{6, 7, 8},
			appendInitialID: entryID{term: 5, index: 10},
			// Many term flips: [t11/11, t12/12, t13/13]
			// We want to overwrite the original termcache, starting at t6/11
			appendTerms: []uint64{11, 12, 13},
			// Final term cache state: [t5/10, t11/11, t12/12, t13/13]
			want: struct {
				cacheLen             int
				first                entryID
				last                 entryID
				indexAndExpectedTerm [][2]uint64
			}{
				cacheLen: 4,                            // At maximum
				first:    entryID{term: 5, index: 10},  // First entry evicted
				last:     entryID{term: 13, index: 13}, // New last entry
				indexAndExpectedTerm: [][2]uint64{
					{10, 5},
					{11, 11},
					{12, 12},
					{13, 13},
				},
			},
		},
		{
			name:      "repeated term appends won't modify the term cache",
			initialID: entryID{term: 5, index: 10},
			cacheSize: 4,
			// Initial entries: [t6/11, t7/12, t8/13]
			initTerms:       []uint64{6, 7, 8},
			appendInitialID: entryID{term: 8, index: 13},
			// Append with repeating terms: [t8/14, t8,15, t8/16, t8/17,......]
			appendTerms: []uint64{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8},
			// Final term cache state: [t5/10, t6/11, t7/12, t8/13]
			want: struct {
				cacheLen             int
				first                entryID
				last                 entryID
				indexAndExpectedTerm [][2]uint64
			}{
				cacheLen: 4,                           // Size is still same
				first:    entryID{term: 5, index: 10}, // Nothing evicted
				last:     entryID{term: 8, index: 13},
				indexAndExpectedTerm: [][2]uint64{
					{10, 5},
					{11, 6},
					{12, 7},
					{13, 8}, {14, 8}, {15, 8}},
			},
		},
		{
			name:      "overwrite test case",
			cacheSize: 4,
			initialID: entryID{term: 5, index: 10},
			// Initial entries: [t6/11, t7/12, t8/13]
			initTerms: []uint64{6, 7, 8},
			// We now have a full cache: [t5/10, t6/11, t7/12, t8/13]
			appendInitialID: entryID{term: 5, index: 10},
			// Append with repeating terms: [t7/11, t8,12, t9/13, t10/14, t11/15, t12/16, t13/17, t13/18, t13/19, t13/20...]
			appendTerms: []uint64{7, 8, 9, 10, 11, 12, 13, 13, 13, 13, 13, 13},
			// Final term cache state: [t10/14, t11/15, t12/16, t13/17]
			want: struct {
				cacheLen             int
				first                entryID
				last                 entryID
				indexAndExpectedTerm [][2]uint64
			}{
				cacheLen: 4,
				first:    entryID{term: 10, index: 14},
				last:     entryID{term: 13, index: 17},
				indexAndExpectedTerm: [][2]uint64{
					{14, 10},
					{15, 11},
					{16, 12},
					{17, 13}, {18, 13},
				},
			},
		},
		{
			name:      "extreme but possible, overwrite with LogSlice.prev, and an entry",
			cacheSize: 4,
			initialID: entryID{term: 5, index: 10},
			// Initial entries: [t6/11, t7/12, t8/13]
			initTerms: []uint64{6, 7, 8},
			// We now have a full cache: [t5/10, t6/11, t7/12, t8/13]
			appendInitialID: entryID{term: 4, index: 9},
			appendTerms:     []uint64{10},
			// Final term cache state: [t4/9, t10/10]
			want: struct {
				cacheLen             int
				first                entryID
				last                 entryID
				indexAndExpectedTerm [][2]uint64
			}{
				cacheLen: 2,
				first:    entryID{term: 4, index: 9},   // LogSlice.prev is set as the first entry
				last:     entryID{term: 10, index: 10}, // New last entry, which is what we appended
				indexAndExpectedTerm: [][2]uint64{
					{8, 0},
					{9, 4},
					{10, 10}, {11, 10}, {12, 10},
				},
			},
		},
		{
			name:      "overwrite with just one entry",
			cacheSize: 4,
			initialID: entryID{term: 5, index: 10},
			// Initial entries: [t6/11, t7/12, t8/13]
			initTerms: []uint64{6, 7, 8},
			// We now have a full cache: [t5/10, t6/11, t7/12, t8/13]
			appendInitialID: entryID{term: 5, index: 10},
			appendTerms:     []uint64{10},
			// Final term cache state: [t5/10, t10/11]
			want: struct {
				cacheLen             int
				first                entryID
				last                 entryID
				indexAndExpectedTerm [][2]uint64
			}{
				cacheLen: 2,
				first:    entryID{term: 5, index: 10},  // LogSlice.prev is set as the first entry
				last:     entryID{term: 10, index: 11}, // New last entry, which is what we appended
				indexAndExpectedTerm: [][2]uint64{
					{8, 0}, {9, 0},
					{10, 5},
					{11, 10}, {12, 10}},
			},
		},
		{
			name:      "multiple gaps in between, test term",
			cacheSize: 4,
			initialID: entryID{term: 5, index: 10},
			// Initial entries only fill half: [t5/10, t6/11]
			initTerms:       []uint64{6},
			appendInitialID: entryID{term: 6, index: 11},
			// Append more: [t7/12, t8/13, t9/14]
			appendTerms: []uint64{7, 7, 7, 9, 9, 9, 10, 10, 10},
			// Final term cache state: [t6/11, t7/12, t9/15, t10/18]
			want: struct {
				cacheLen             int
				first                entryID
				last                 entryID
				indexAndExpectedTerm [][2]uint64
			}{
				cacheLen: 4,
				first:    entryID{term: 6, index: 11},
				last:     entryID{term: 10, index: 18},
				indexAndExpectedTerm: [][2]uint64{
					{10, 0},
					{11, 6},
					{12, 7}, {13, 7}, {14, 7},
					{15, 9}, {16, 9},
					{17, 9}, {18, 10},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize cache with size 4
			cache := NewTermCache(tc.cacheSize, tc.initialID)

			// First append to establish initial state
			initialSlice := tc.initialID.append(tc.initTerms...)
			cache.truncateAndAppend(initialSlice.LogSlice)

			// Second append with new entries
			appendSlice := tc.appendInitialID.append(tc.appendTerms...)
			cache.truncateAndAppend(appendSlice.LogSlice)

			// Verify final state
			require.Equal(t, tc.want.cacheLen, len(cache.cache))
			require.Equal(t, tc.want.first, cache.firstEntry())
			require.Equal(t, tc.want.last, cache.lastEntry())

			// Verify capacity hasn't increased
			require.Equal(t, tc.cacheSize, uint64(cap(cache.cache)),
				"capacity should remain at initial size")

			checkTerm(t, &cache, tc.want.indexAndExpectedTerm)
		})
	}
}

func TestTermCacheInitialTruncateAndAppend(t *testing.T) {
	testCases := []struct {
		name      string
		initialID entryID
		terms     []uint64
		want      struct {
			cacheLen int
			first    entryID
			last     entryID
		}
	}{
		{
			name:      "evicts oldest entries when full",
			initialID: entryID{term: 5, index: 10},
			// append [t5/11, t6/12, t6/13, t7/14, t7/15, t8/16, t9/17]
			terms: []uint64{5, 6, 6, 7, 7, 8, 9},
			want: struct {
				cacheLen int
				first    entryID
				last     entryID
			}{
				cacheLen: 4,                           // Maximum size
				first:    entryID{term: 6, index: 12}, // Oldest entries evicted
				last:     entryID{term: 9, index: 17}, // Last entry
			},
		},
		{
			name:      "appends small amounts of term flips, not full, no evict",
			initialID: entryID{term: 5, index: 10},
			// append [t6/11, t7/12]
			terms: []uint64{6, 7},
			want: struct {
				cacheLen int
				first    entryID
				last     entryID
			}{
				cacheLen: 3,                           // We are not full yet
				first:    entryID{term: 5, index: 10}, // Initial entry
				last:     entryID{term: 7, index: 12}, // Last entry
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize cache with size 4
			cache := NewTermCache(4, tc.initialID)

			// Append entries using the helper
			slice := tc.initialID.append(tc.terms...)
			cache.truncateAndAppend(slice.LogSlice)

			// Verify results
			require.Equal(t, tc.want.cacheLen, len(cache.cache))
			require.Equal(t, tc.want.first, cache.firstEntry())
			require.Equal(t, tc.want.last, cache.lastEntry())

			// Verify capacity hasn't increased
			require.Equal(t, 4, cap(cache.cache),
				"capacity should remain at initial size")
		})
	}
}

func TestNeverEmpty(t *testing.T) {
	tc := NewTermCache(8, entryID{5, 10})

	// Test operations that could potentially empty the cache
	operations := []struct {
		name string
		fn   func()
	}{
		{"truncateFrom low index", func() { tc.truncateFrom(entryID{4, 9}) }},
		{"truncateFrom overflow index", func() { tc.truncateFrom(entryID{1000, 10}) }},
		{"truncateFrom lower index than initial", func() { tc.truncateFrom(entryID{3, 8}) }},
		{"truncateAndAppend entries with higher indices", func() {
			terms := make([]uint64, 20)
			for i := range terms {
				terms[i] = uint64(10 + i)
			}
			initialID := entryID{term: 9, index: 19}
			slice := initialID.append(terms...)
			tc.truncateAndAppend(slice.LogSlice)
		}},
	}

	for _, op := range operations {
		// Reset before each operation
		tc = NewTermCache(8, entryID{5, 10})
		op.fn()

		if len(tc.cache) == 0 {
			t.Errorf("operation %q emptied the cache", op.name)
		}
	}
}

// Benchmarks
// After using the benchmark tests, a ring buffer is not necessary.
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
			initialSlice := LogSlice{prev: entryID{term: TestInitialTerm, index: TestInitialIndex}, entries: initialEntries}
			appendSlice := LogSlice{prev: entryID{term: TestInitialTerm + 1, index: TestInitialIndex + 1}, entries: entries}

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
			initialSlice := LogSlice{prev: entryID{term: TestInitialTerm, index: TestInitialIndex}, entries: initialEntries}
			entries := createEntriesWithFewFlips(size, 3, 3)
			appendSlice := LogSlice{prev: entryID{term: TestInitialTerm + 1, index: TestInitialIndex + 1}, entries: entries}

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

// checkTerm is a helper for checking the term given an entry index.
// Pass in indexAndExpectedTerms := [][2]uint64{{index, expectedTerm}, ...}
// if expectedTerm is zero, that means we expect the term to not be found.
func checkTerm(t *testing.T, tc *termCache, indexAndExpectedTerms [][2]uint64) {
	for _, pairs := range indexAndExpectedTerms {
		index, expectedTerm := pairs[0], pairs[1]
		term, ok := tc.term(index)
		if expectedTerm != 0 {
			require.True(t, ok, "index %d not found in term cache", index)
		}
		require.Equal(t, expectedTerm, term, "unexpected term for index %d", index)
	}
}
