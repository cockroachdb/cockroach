// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raft

import (
	"fmt"
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

// Test constants, the test cases are hardcoded based on current test constants.
var TestTermCacheSize = 8
var TestInitialTerm = uint64(5)
var TestInitialIndex = uint64(10)

func TestTermCacheInitialization(t *testing.T) {
	testCases := []struct {
		name          string
		term          uint64
		index         uint64
		expectedLen   int
		expectedCap   int
		expectedIndex uint64
	}{
		{
			name:          "basic initialization",
			term:          TestInitialTerm,
			index:         TestInitialIndex,
			expectedLen:   1,
			expectedCap:   TestTermCacheSize,
			expectedIndex: TestInitialIndex,
		},
	}
	// Test case where capacity is less than the number of entries
	testCases = append(testCases, struct {
		name          string
		term          uint64
		index         uint64
		expectedLen   int
		expectedCap   int
		expectedIndex uint64
	}{
		name:          "initialization with more entries than capacity",
		term:          TestInitialTerm,
		index:         TestInitialIndex + 100,
		expectedLen:   1,
		expectedCap:   TestTermCacheSize,
		expectedIndex: TestInitialIndex + 100,
	})

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cache := NewTermCache(uint64(TestTermCacheSize), entryID{testCase.term, testCase.index})

			// After initialization, the cache should not be empty
			if len(cache.cache) == 0 {
				t.Errorf("expected non-empty term cache after initialization")
			}

			// Check the capacity didn't exceed what we specified
			if cap(cache.cache) != testCase.expectedCap {
				t.Errorf("expected capacity of %d, got %d", testCase.expectedCap, cap(cache.cache))
			}

			// Check the first entry
			if cache.firstEntry().term != testCase.term || cache.firstEntry().index != testCase.index {
				t.Errorf("expected first entry to be (term: %d, index: %d), got (term: %d, index: %d)",
					testCase.term, testCase.index, cache.firstEntry().term, cache.firstEntry().index)
			}
		})
	}
}

func TestTermCacheTerm(t *testing.T) {
	testCases := []struct {
		name          string
		initialTerm   uint64
		initialIndex  uint64
		entries       []pb.Entry
		queries       []uint64
		expectedTerms []uint64
		expectedOK    []bool
	}{
		{
			name:         "basic term lookups",
			initialTerm:  TestInitialTerm,
			initialIndex: TestInitialIndex,
			entries: []pb.Entry{
				{Index: TestInitialIndex + 1, Term: TestInitialTerm},     // same term continuation
				{Index: TestInitialIndex + 2, Term: TestInitialTerm + 1}, // term flip
				{Index: TestInitialIndex + 3, Term: TestInitialTerm + 1}, // same term continuation
				{Index: TestInitialIndex + 4, Term: TestInitialTerm + 2}, // term flip
			},
			queries: []uint64{
				TestInitialIndex,       // Initial entry
				TestInitialIndex - 1,   // Below cache
				TestInitialIndex + 300, // Above cache - should return term at lastIndex
				TestInitialIndex + 1,   // Same term
				TestInitialIndex + 2,   // Term flip
				TestInitialIndex + 3,
				TestInitialIndex + 4, // Another term flip
			},
			expectedTerms: []uint64{
				TestInitialTerm,
				0,
				TestInitialTerm + 2, // Return term at lastIndex for query above lastIndex
				TestInitialTerm,
				TestInitialTerm + 1,
				TestInitialTerm + 1,
				TestInitialTerm + 2,
			},
			expectedOK: []bool{
				true,
				false,
				true, // Should succeed for index above lastIndex
				true,
				true,
				true,
				true,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cache := NewTermCache(uint64(TestTermCacheSize), entryID{testCase.initialTerm, testCase.initialIndex})

			entriesLogSlice := LogSlice{
				prev: entryID{
					term:  testCase.entries[0].Term - 1,
					index: testCase.entries[0].Index - 1,
				},
				entries: testCase.entries,
			}

			if len(testCase.entries) > 0 {
				cache.ScanAppend(entriesLogSlice)
			}

			for i, query := range testCase.queries {
				term, ok := cache.Term(query)

				// Check if term was found
				if ok != testCase.expectedOK[i] {
					t.Errorf("query %d: expected ok=%v, got ok=%v", i, testCase.expectedOK[i], ok)
				}

				// Check term if it was found
				if ok && term != testCase.expectedTerms[i] {
					t.Errorf("query %d: expected term %d for index %d, got %d",
						i, testCase.expectedTerms[i], query, term)
				}
			}
		})
	}
}

func TestTermCacheScanAppend(t *testing.T) {
	testCases := []struct {
		name                string
		initialTerm         uint64
		initialIndex        uint64
		entries             []pb.Entry
		expectedLen         int
		expectedLastTerm    uint64
		expectedLastIndex   uint64
		expectedFirstTerm   uint64
		expectedFirstIndex  uint64
		expectedMaxCapacity int
	}{
		{
			name:         "append multiple term flips",
			initialTerm:  TestInitialTerm,
			initialIndex: TestInitialIndex,
			entries: []pb.Entry{
				{Index: TestInitialIndex + 1, Term: TestInitialTerm},
				{Index: TestInitialIndex + 2, Term: TestInitialTerm + 1},
				{Index: TestInitialIndex + 3, Term: TestInitialTerm + 1},
				{Index: TestInitialIndex + 4, Term: TestInitialTerm + 2},
				{Index: TestInitialIndex + 5, Term: TestInitialTerm + 2},
				{Index: TestInitialIndex + 6, Term: TestInitialTerm + 2},
				{Index: TestInitialIndex + 7, Term: TestInitialTerm + 3},
				{Index: TestInitialIndex + 8, Term: TestInitialTerm + 4},
				{Index: TestInitialIndex + 9, Term: TestInitialTerm + 5},
				{Index: TestInitialIndex + 10, Term: TestInitialTerm + 8},
				{Index: TestInitialIndex + 11, Term: TestInitialTerm + 9},
				{Index: TestInitialIndex + 12, Term: TestInitialTerm + 10},
				{Index: TestInitialIndex + 13, Term: TestInitialTerm + 11},
			},
			expectedLen:         TestTermCacheSize,
			expectedLastTerm:    TestInitialTerm + 11,
			expectedLastIndex:   TestInitialIndex + 13,
			expectedFirstTerm:   TestInitialTerm + 2,
			expectedFirstIndex:  TestInitialIndex + 4,
			expectedMaxCapacity: TestTermCacheSize,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cache := NewTermCache(uint64(TestTermCacheSize), entryID{testCase.initialTerm, testCase.initialIndex})

			entriesLogSlice := LogSlice{
				prev: entryID{
					term:  testCase.entries[0].Term - 1,
					index: testCase.entries[0].Index - 1,
				},
				entries: testCase.entries,
			}

			cache.ScanAppend(entriesLogSlice)

			// Check the cache size
			if len(cache.cache) != testCase.expectedLen {
				t.Errorf("expected %d entries in cache after ScanAppend, got %d",
					testCase.expectedLen, len(cache.cache))
			}

			// Check capacity didn't exceed initial
			if cap(cache.cache) > testCase.expectedMaxCapacity {
				t.Errorf("capacity exceeded initial size, meaning realloc on slice was used: %d", cap(cache.cache))
			}

			// Check that entries were properly evicted when capacity was reached
			firstEntryTerm, firstEntryIndex := cache.firstEntry().term, cache.firstEntry().index
			if firstEntryTerm != testCase.expectedFirstTerm || firstEntryIndex != testCase.expectedFirstIndex {
				t.Errorf("expected first entry after eviction to be (%d, %d), got (%d, %d)",
					testCase.expectedFirstTerm, testCase.expectedFirstIndex,
					firstEntryTerm, firstEntryIndex)
			}
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
		{"ScanAppend many entries with higher index and term", func() {
			entries := make([]pb.Entry, 20)
			for i := range entries {
				entries[i] = pb.Entry{
					Index: uint64(20 + i),
					Term:  uint64(10 + i/3), // Term flips every 3 entries
				}
			}
			entriesSlice := LogSlice{term: 100, prev: entryID{term: 9, index: 19}, entries: entries}
			tc.ScanAppend(entriesSlice)
		}},
		{"ScanAppend many entries", func() {
			entries := make([]pb.Entry, 20)
			for i := range entries {
				entries[i] = pb.Entry{
					Index: uint64(9 + i),
					Term:  uint64(4 + i/3), // Term flips every 3 entries
				}
			}
			entriesSlice := LogSlice{term: 100, prev: entryID{term: 3, index: 8}, entries: entries}
			tc.ScanAppend(entriesSlice)
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
			initialSlice := LogSlice{term: 0, prev: entryID{term: TestInitialTerm, index: TestInitialIndex}, entries: initialEntries}
			appendSlice := LogSlice{term: 0, prev: entryID{term: TestInitialTerm + 1, index: TestInitialIndex + 1}, entries: entries}

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				cache := NewTermCache(100, entryID{1, 1})
				cache.ScanAppend(initialSlice)
				b.StartTimer()
				// This is the operation we're benchmarking
				cache.ScanAppend(appendSlice)
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
			initialSlice := LogSlice{term: 0, prev: entryID{term: TestInitialTerm, index: TestInitialIndex}, entries: initialEntries}
			entries := createEntriesWithFewFlips(size, 3, 3)
			appendSlice := LogSlice{term: 0, prev: entryID{term: TestInitialTerm + 1, index: TestInitialIndex + 1}, entries: entries}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				cache := NewTermCache(100, entryID{1, 1})
				cache.ScanAppend(initialSlice)
				b.StartTimer()

				// This is the operation we're benchmarking
				cache.ScanAppend(appendSlice)
			}
		})
	}
}
