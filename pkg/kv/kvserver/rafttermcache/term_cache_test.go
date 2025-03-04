// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rafttermcache

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

// Test constants, the test cases are hardcoded based on current test constants.
var TestTermCacheSize = 8
var TestInitialTerm = kvpb.RaftTerm(5)
var TestInitialIndex = kvpb.RaftIndex(10)

func TestTermCacheInitialization(t *testing.T) {
	testCases := []struct {
		name          string
		term          kvpb.RaftTerm
		index         kvpb.RaftIndex
		expectedLen   int
		expectedCap   int
		expectedIndex kvpb.RaftIndex
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

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cache := NewTermCache(uint64(TestTermCacheSize))
			cache.ResetWithFirst(testCase.term, testCase.index)

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

			// Check lastIndex
			if cache.lastIndex != testCase.expectedIndex {
				t.Errorf("expected lastIndex to be %d, got %d", testCase.expectedIndex, cache.lastIndex)
			}
		})
	}
}

func TestTermCacheTerm(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name          string
		initialTerm   kvpb.RaftTerm
		initialIndex  kvpb.RaftIndex
		entries       []pb.Entry
		queries       []kvpb.RaftIndex
		expectedTerms []kvpb.RaftTerm
		expectedOK    []bool
	}{
		{
			name:         "basic term lookups",
			initialTerm:  TestInitialTerm,
			initialIndex: TestInitialIndex,
			entries: []pb.Entry{
				{Index: uint64(TestInitialIndex) + 1, Term: uint64(TestInitialTerm)},     // same term continuation
				{Index: uint64(TestInitialIndex) + 2, Term: uint64(TestInitialTerm) + 1}, // term flip
				{Index: uint64(TestInitialIndex) + 3, Term: uint64(TestInitialTerm) + 1}, // same term continuation
				{Index: uint64(TestInitialIndex) + 4, Term: uint64(TestInitialTerm) + 2}, // term flip
			},
			queries: []kvpb.RaftIndex{
				TestInitialIndex,       // Initial entry
				TestInitialIndex - 1,   // Below cache
				TestInitialIndex + 300, // Above cache
				TestInitialIndex + 1,   // Same term
				TestInitialIndex + 2,   // Term flip
				TestInitialIndex + 3,
				TestInitialIndex + 4, // Another term flip
			},
			expectedTerms: []kvpb.RaftTerm{
				TestInitialTerm,
				0,
				0,
				TestInitialTerm,
				TestInitialTerm + 1,
				TestInitialTerm + 1,
				TestInitialTerm + 2,
			},
			expectedOK: []bool{
				true,
				false,
				false,
				true,
				true,
				true,
				true,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cache := NewTermCache(uint64(TestTermCacheSize))
			cache.ResetWithFirst(testCase.initialTerm, testCase.initialIndex)

			if len(testCase.entries) > 0 {
				cache.ScanAppend(ctx, testCase.entries)
			}

			for i, query := range testCase.queries {
				term, ok := cache.Term(ctx, query)

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

func TestTermCacheTermAfterFull(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name          string
		initialTerm   kvpb.RaftTerm
		initialIndex  kvpb.RaftIndex
		entries       []pb.Entry
		queries       []kvpb.RaftIndex
		expectedTerms []kvpb.RaftTerm
		expectedOK    []bool
	}{
		{
			name:         "basic term lookups",
			initialTerm:  TestInitialTerm,
			initialIndex: TestInitialIndex,
			entries: []pb.Entry{
				{Index: uint64(TestInitialIndex) + 1, Term: uint64(TestInitialTerm)},      // same term continuation
				{Index: uint64(TestInitialIndex) + 2, Term: uint64(TestInitialTerm) + 1},  // term flip
				{Index: uint64(TestInitialIndex) + 3, Term: uint64(TestInitialTerm) + 1},  // same term continuation
				{Index: uint64(TestInitialIndex) + 4, Term: uint64(TestInitialTerm) + 2},  // term flip
				{Index: uint64(TestInitialIndex) + 5, Term: uint64(TestInitialTerm) + 3},  // term flip
				{Index: uint64(TestInitialIndex) + 6, Term: uint64(TestInitialTerm) + 4},  // term flip
				{Index: uint64(TestInitialIndex) + 7, Term: uint64(TestInitialTerm) + 5},  // term flip
				{Index: uint64(TestInitialIndex) + 8, Term: uint64(TestInitialTerm) + 6},  // term flip
				{Index: uint64(TestInitialIndex) + 9, Term: uint64(TestInitialTerm) + 7},  // term flip
				{Index: uint64(TestInitialIndex) + 10, Term: uint64(TestInitialTerm) + 8}, // term flip
			},
			queries: []kvpb.RaftIndex{
				TestInitialIndex,       // Initial entry
				TestInitialIndex - 1,   // Below cache
				TestInitialIndex + 300, // Above cache
				TestInitialIndex + 1,   // Same term
				TestInitialIndex + 2,   // Term flip
				TestInitialIndex + 3,   // Another term flip
				TestInitialIndex + 4,   // Another term flip
				TestInitialIndex + 5,   // ...
				TestInitialIndex + 6,
				TestInitialIndex + 7,
				TestInitialIndex + 8,
				TestInitialIndex + 9,
				TestInitialIndex + 10,
			},
			expectedTerms: []kvpb.RaftTerm{
				0,
				0,
				0,
				TestInitialTerm,
				TestInitialTerm + 1,
				TestInitialTerm + 1,
				TestInitialTerm + 2,
				TestInitialTerm + 3,
				TestInitialTerm + 4,
				TestInitialTerm + 5,
				TestInitialTerm + 6,
				TestInitialTerm + 7,
				TestInitialTerm + 8,
			},
			expectedOK: []bool{
				false,
				false,
				false,
				false,
				true,
				true,
				true,
				true,
				true,
				true,
				true,
				true,
				true,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cache := NewTermCache(uint64(TestTermCacheSize))
			cache.ResetWithFirst(testCase.initialTerm, testCase.initialIndex)

			if len(testCase.entries) > 0 {
				cache.ScanAppend(ctx, testCase.entries)
			}

			for i, query := range testCase.queries {
				term, ok := cache.Term(ctx, query)

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

func TestTermCacheClearTo(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name                string
		initialTerm         kvpb.RaftTerm
		initialIndex        kvpb.RaftIndex
		entries             []pb.Entry
		clearIndex          kvpb.RaftIndex
		expectedLen         int
		expectedFirstTerm   kvpb.RaftTerm
		expectedFirstIndex  kvpb.RaftIndex
		expectedMaxCapacity int
	}{
		{
			name:         "clear to middle",
			initialTerm:  TestInitialTerm,
			initialIndex: TestInitialIndex,
			entries: []pb.Entry{
				{Index: 11, Term: uint64(TestInitialTerm)},
				{Index: 12, Term: uint64(TestInitialTerm) + 1},
				{Index: 13, Term: uint64(TestInitialTerm) + 1},
				{Index: 14, Term: uint64(TestInitialTerm) + 2},
				{Index: 15, Term: uint64(TestInitialTerm) + 3},
			},
			clearIndex:          12,
			expectedLen:         3,
			expectedFirstTerm:   TestInitialTerm + 1,
			expectedFirstIndex:  12,
			expectedMaxCapacity: TestTermCacheSize,
		},
		{
			name:         "clear beyond lastIndex",
			initialTerm:  TestInitialTerm,
			initialIndex: TestInitialIndex,
			entries: []pb.Entry{
				{Index: 11, Term: uint64(TestInitialTerm) + 3},
				{Index: 12, Term: uint64(TestInitialTerm) + 4},
			},
			clearIndex:          20,
			expectedLen:         1,
			expectedFirstTerm:   TestInitialTerm + 4,
			expectedFirstIndex:  12,
			expectedMaxCapacity: TestTermCacheSize,
		},
		{
			name:         "clear below lastIndex",
			initialTerm:  TestInitialTerm,
			initialIndex: TestInitialIndex,
			entries: []pb.Entry{
				{Index: 11, Term: uint64(TestInitialTerm) + 3},
				{Index: 12, Term: uint64(TestInitialTerm) + 4},
				{Index: 13, Term: uint64(TestInitialTerm) + 5},
				{Index: 14, Term: uint64(TestInitialTerm) + 6},
				{Index: 15, Term: uint64(TestInitialTerm) + 7},
			},
			clearIndex:          TestInitialIndex - 1,
			expectedLen:         6,
			expectedFirstTerm:   TestInitialTerm,
			expectedFirstIndex:  TestInitialIndex,
			expectedMaxCapacity: TestTermCacheSize,
		},
		{
			name:         "clear at first entry index",
			initialTerm:  TestInitialTerm,
			initialIndex: TestInitialIndex,
			entries: []pb.Entry{
				{Index: 11, Term: uint64(TestInitialTerm) + 3},
				{Index: 12, Term: uint64(TestInitialTerm) + 4},
				{Index: 13, Term: uint64(TestInitialTerm) + 5},
				{Index: 14, Term: uint64(TestInitialTerm) + 6},
				{Index: 15, Term: uint64(TestInitialTerm) + 7},
			},
			clearIndex:          TestInitialIndex,
			expectedLen:         6,
			expectedFirstTerm:   TestInitialTerm,
			expectedFirstIndex:  TestInitialIndex,
			expectedMaxCapacity: TestTermCacheSize,
		},
		{
			name:         "clear at last entry index",
			initialTerm:  TestInitialTerm,
			initialIndex: TestInitialIndex,
			entries: []pb.Entry{
				{Index: 11, Term: uint64(TestInitialTerm) + 3},
				{Index: 12, Term: uint64(TestInitialTerm) + 4},
				{Index: 13, Term: uint64(TestInitialTerm) + 5},
				{Index: 14, Term: uint64(TestInitialTerm) + 6},
				{Index: 15, Term: uint64(TestInitialTerm) + 7},
			},
			clearIndex:          15,
			expectedLen:         1,
			expectedFirstTerm:   TestInitialTerm + 7,
			expectedFirstIndex:  15,
			expectedMaxCapacity: TestTermCacheSize,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cache := NewTermCache(uint64(TestTermCacheSize))
			cache.ResetWithFirst(testCase.initialTerm, testCase.initialIndex)

			if len(testCase.entries) > 0 {
				cache.ScanAppend(ctx, testCase.entries)
			}

			// Clear to index
			cache.ClearTo(testCase.clearIndex)

			// Cache should not be empty
			if len(cache.cache) == 0 {
				t.Errorf("cache is empty after ClearTo")
			}

			// Check cache length
			if len(cache.cache) != testCase.expectedLen {
				t.Errorf("expected %d entries in cache after ClearTo, got %d",
					testCase.expectedLen, len(cache.cache))
			}

			// Check first entry values
			if cache.firstEntry().term != testCase.expectedFirstTerm ||
				cache.firstEntry().index != testCase.expectedFirstIndex {
				t.Errorf("expected first entry (%d, %d), got (%d, %d)",
					testCase.expectedFirstTerm, testCase.expectedFirstIndex,
					cache.firstEntry().term, cache.firstEntry().index)
			}

			// Check capacity didn't change
			if cap(cache.cache) > testCase.expectedMaxCapacity {
				t.Errorf("capacity exceeded initial size: %d", cap(cache.cache))
			}
		})
	}
}

func TestTermCacheScanAppend(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name                string
		initialTerm         kvpb.RaftTerm
		initialIndex        kvpb.RaftIndex
		entries             []pb.Entry
		expectedLen         int
		expectedLastTerm    kvpb.RaftTerm
		expectedLastIndex   kvpb.RaftIndex
		expectedFirstTerm   kvpb.RaftTerm
		expectedFirstIndex  kvpb.RaftIndex
		expectedMaxCapacity int
	}{
		{
			name:         "append multiple term flips",
			initialTerm:  TestInitialTerm,
			initialIndex: TestInitialIndex,
			entries: []pb.Entry{
				{Index: uint64(TestInitialIndex) + 1, Term: uint64(TestInitialTerm)},
				{Index: uint64(TestInitialIndex) + 2, Term: uint64(TestInitialTerm) + 1},
				{Index: uint64(TestInitialIndex) + 3, Term: uint64(TestInitialTerm) + 1},
				{Index: uint64(TestInitialIndex) + 4, Term: uint64(TestInitialTerm) + 2}, // flip
				{Index: uint64(TestInitialIndex) + 5, Term: uint64(TestInitialTerm) + 2},
				{Index: uint64(TestInitialIndex) + 6, Term: uint64(TestInitialTerm) + 2},
				{Index: uint64(TestInitialIndex) + 7, Term: uint64(TestInitialTerm) + 3},   // flip
				{Index: uint64(TestInitialIndex) + 8, Term: uint64(TestInitialTerm) + 4},   // flip
				{Index: uint64(TestInitialIndex) + 9, Term: uint64(TestInitialTerm) + 5},   // flip
				{Index: uint64(TestInitialIndex) + 10, Term: uint64(TestInitialTerm) + 8},  // flip
				{Index: uint64(TestInitialIndex) + 11, Term: uint64(TestInitialTerm) + 9},  // flip
				{Index: uint64(TestInitialIndex) + 12, Term: uint64(TestInitialTerm) + 10}, // flip
				{Index: uint64(TestInitialIndex) + 13, Term: uint64(TestInitialTerm) + 11}, // flip
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
			cache := NewTermCache(uint64(TestTermCacheSize))
			cache.ResetWithFirst(testCase.initialTerm, testCase.initialIndex)

			cache.ScanAppend(ctx, testCase.entries)

			// Check the cache size
			if len(cache.cache) != testCase.expectedLen {
				t.Errorf("expected %d entries in cache after ScanAppend, got %d",
					testCase.expectedLen, len(cache.cache))
			}

			// Check the last entry
			if cache.lastEntry().term != testCase.expectedLastTerm ||
				cache.lastEntry().index != testCase.expectedLastIndex {
				t.Errorf("expected last entry (%d, %d), got (%d, %d)",
					testCase.expectedLastTerm, testCase.expectedLastIndex,
					cache.lastEntry().term, cache.lastEntry().index)
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
	ctx := context.Background()
	tc := NewTermCache(8)
	tc.ResetWithFirst(5, 10)

	// Test operations that could potentially empty the cache
	operations := []struct {
		name string
		fn   func()
	}{
		{"ClearTo low index", func() { tc.ClearTo(5) }},
		{"ClearTo overflow index", func() { tc.ClearTo(100) }},
		{"ClearTo lower index than initial", func() { tc.ClearTo(4) }},
		{"ScanAppend many entries with higher index and term", func() {
			entries := make([]pb.Entry, 20)
			for i := range entries {
				entries[i] = pb.Entry{
					Index: uint64(20 + i),
					Term:  uint64(10 + i/3), // Term flips every 3 entries
				}
			}
			tc.ScanAppend(ctx, entries)
		}},
		{"ScanAppend many entries", func() {
			entries := make([]pb.Entry, 20)
			for i := range entries {
				entries[i] = pb.Entry{
					Index: uint64(9 + i),
					Term:  uint64(4 + i/3), // Term flips every 3 entries
				}
			}
			tc.ScanAppend(ctx, entries)
		}},
	}

	for _, op := range operations {
		tc.ResetWithFirst(5, 10) // Reset before each operation
		op.fn()

		if len(tc.cache) == 0 {
			t.Errorf("operation %q emptied the cache", op.name)
		}
	}
}

// Extreme case: when we are appending to the term cache by
// calling ScanAppend(), if the term flips too often,
// we will see linear increase in delay.
// Adopting a ring buffer can bring the
// complexity down to by a factor of cache size.
// But after further benchmarking, it seems that copying a small slice
// (<20) would not be the bottleneck even if every entry we append is
// a term flip. So no need to use a ring buffer.
func BenchmarkTermCacheScanAppendConstantTermFlips(b *testing.B) {
	ctx := context.Background()
	// Create entries with term flips for each entry
	createEntries := func(count int, startIndex kvpb.RaftIndex, startTerm kvpb.RaftTerm) []pb.Entry {
		entries := make([]pb.Entry, count)
		for i := 0; i < count; i++ {
			entries[i] = pb.Entry{
				Index: uint64(startIndex) + uint64(i),
				Term:  uint64(startTerm) + uint64(i), // Each entry has a new term
			}
		}
		return entries
	}

	benchSizes := []int{10000, 100000, 1000000}
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("entries-%d", size), func(b *testing.B) {
			initialEntries := createEntries(2, 1, 1)
			entries := createEntries(size, 3, 3)
			cache := NewTermCache(100) // big cache to test for the overhead of copying

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				cache.ResetWithFirst(1, 1)
				cache.ScanAppend(ctx, initialEntries)
				b.StartTimer()
				// This is the operation we're benchmarking
				cache.ScanAppend(ctx, entries)
			}
		})
	}
}

// With infrequent term flips which is the practical case,
// there are no problems with the current implementation.
func BenchmarkTermCacheScanAppendFewTermFlips(b *testing.B) {
	ctx := context.Background()

	// Create entries with few term flips
	createEntriesWithFewFlips := func(count int, startIndex kvpb.RaftIndex, startTerm kvpb.RaftTerm) []pb.Entry {
		entries := make([]pb.Entry, count)
		currentTerm := startTerm
		oneOverTermFlipFreq := 30

		for i := 0; i < count; i++ {
			// Only flip term every 30 entries
			if i > 0 && i%oneOverTermFlipFreq == 0 {
				currentTerm++
			}
			entries[i] = pb.Entry{
				Index: uint64(startIndex) + uint64(i),
				Term:  uint64(currentTerm),
			}
		}
		return entries
	}

	benchSizes := []int{10000, 100000, 1000000}
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("entries-%d", size), func(b *testing.B) {
			initialEntries := createEntriesWithFewFlips(2, 1, 1)
			entries := createEntriesWithFewFlips(size, 3, 3)
			cache := NewTermCache(100) // big cache to test for the overhead of copying

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				cache.ResetWithFirst(1, 1)
				cache.ScanAppend(ctx, initialEntries)
				b.StartTimer()

				// This is the operation we're benchmarking
				cache.ScanAppend(ctx, entries)
			}
		})
	}
}
