// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSearchResult(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	r1 := SearchResult{
		QuerySquaredDistance: 1,
		ErrorBound:           0.5,
		CentroidDistance:     10,
		ParentPartitionKey:   100,
		ChildKey:             ChildKey{KeyBytes: []byte{10}},
	}
	r2 := SearchResult{
		QuerySquaredDistance: 2,
		ErrorBound:           0,
		CentroidDistance:     20,
		ParentPartitionKey:   200,
		ChildKey:             ChildKey{KeyBytes: []byte{20}},
	}
	r3 := SearchResult{
		QuerySquaredDistance: 2,
		ErrorBound:           1,
		CentroidDistance:     20,
		ParentPartitionKey:   200,
		ChildKey:             ChildKey{KeyBytes: []byte{30}},
	}
	r4 := SearchResult{
		QuerySquaredDistance: 2,
		ErrorBound:           1,
		CentroidDistance:     30,
		ParentPartitionKey:   300,
		ChildKey:             ChildKey{KeyBytes: []byte{40}},
	}
	r5 := SearchResult{
		QuerySquaredDistance: 4,
		ErrorBound:           1,
		CentroidDistance:     30,
		ParentPartitionKey:   300,
		ChildKey:             ChildKey{KeyBytes: []byte{40}},
	}

	// MaybeCloser.
	require.True(t, r1.MaybeCloser(&r1))
	require.True(t, r1.MaybeCloser(&r2))
	require.False(t, r2.MaybeCloser(&r1))
	require.True(t, r3.MaybeCloser(&r1))
	require.True(t, r5.MaybeCloser(&r4))

	// Compare.
	require.Equal(t, 0, r1.Compare(&r1))
	require.Equal(t, -1, r1.Compare(&r2))
	require.Equal(t, 1, r2.Compare(&r1))

	require.Equal(t, -1, r2.Compare(&r3))
	require.Equal(t, 1, r3.Compare(&r2))

	require.Equal(t, -1, r3.Compare(&r4))
	require.Equal(t, 1, r4.Compare(&r3))
}

func TestSearchStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var stats SearchStats
	stats.SearchedPartition(LeafLevel, 10)
	stats.SearchedPartition(LeafLevel+1, 10)
	stats.Add(&SearchStats{
		PartitionCount:           3,
		QuantizedVectorCount:     30,
		QuantizedLeafVectorCount: 15,
		FullVectorCount:          5,
	})
	require.Equal(t, 5, stats.PartitionCount)
	require.Equal(t, 50, stats.QuantizedVectorCount)
	require.Equal(t, 25, stats.QuantizedLeafVectorCount)
	require.Equal(t, 5, stats.FullVectorCount)
}

func TestSearchSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Empty.
	searchSet := SearchSet{MaxResults: 3, MaxExtraResults: 7}
	require.Nil(t, searchSet.PopResults())

	// Exceed max results, outside of error bounds.
	result1 := SearchResult{
		QuerySquaredDistance: 3, ErrorBound: 0.5, CentroidDistance: 10, ParentPartitionKey: 100, ChildKey: ChildKey{KeyBytes: []byte{10}}}
	result2 := SearchResult{
		QuerySquaredDistance: 6, ErrorBound: 1, CentroidDistance: 20, ParentPartitionKey: 200, ChildKey: ChildKey{KeyBytes: []byte{20}}}
	result3 := SearchResult{
		QuerySquaredDistance: 1, ErrorBound: 0.5, CentroidDistance: 30, ParentPartitionKey: 300, ChildKey: ChildKey{KeyBytes: []byte{30}}}
	result4 := SearchResult{
		QuerySquaredDistance: 4, ErrorBound: 0.5, CentroidDistance: 40, ParentPartitionKey: 400, ChildKey: ChildKey{KeyBytes: []byte{40}}}
	searchSet.Add(&result1)
	searchSet.Add(&result2)
	searchSet.Add(&result3)
	searchSet.Add(&result4)
	require.Equal(t, SearchResults{result3, result1, result4}, searchSet.PopResults())

	// Exceed max results, but within error bounds.
	result5 := SearchResult{
		QuerySquaredDistance: 6, ErrorBound: 1.5, CentroidDistance: 50, ParentPartitionKey: 500, ChildKey: ChildKey{KeyBytes: []byte{50}}}
	result6 := SearchResult{
		QuerySquaredDistance: 5, ErrorBound: 1, CentroidDistance: 60, ParentPartitionKey: 600, ChildKey: ChildKey{KeyBytes: []byte{60}}}
	searchSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6})
	require.Equal(t, SearchResults{result3, result1, result4, result6, result5}, searchSet.PopResults())

	// Don't allow extra results.
	otherSet := SearchSet{MaxResults: 3}
	otherSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6})
	require.Equal(t, SearchResults{result3, result1, result4}, otherSet.PopResults())

	// Add better results that invalidate farther candidates.
	result7 := SearchResult{
		QuerySquaredDistance: 4, ErrorBound: 1.5, CentroidDistance: 70, ParentPartitionKey: 700, ChildKey: ChildKey{KeyBytes: []byte{70}}}
	searchSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6, result7})
	require.Equal(t, SearchResults{result3, result1, result4, result7, result6, result5}, searchSet.PopResults())

	result8 := SearchResult{
		QuerySquaredDistance: 0.5, ErrorBound: 0.5, CentroidDistance: 80, ParentPartitionKey: 800, ChildKey: ChildKey{KeyBytes: []byte{80}}}
	searchSet.AddAll(SearchResults{result1, result2, result3, result4})
	searchSet.AddAll(SearchResults{result5, result6, result7, result8})
	require.Equal(t, SearchResults{result8, result3, result1, result4, result7}, searchSet.PopResults())

	// Allow one extra result.
	otherSet.MaxExtraResults = 1
	otherSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6, result7})
	require.Equal(t, SearchResults{result3, result1, result4, result7}, otherSet.PopResults())

	// Ignore results without a matching primary key.
	otherSet = SearchSet{MaxResults: 2, MatchKey: []byte{60}}
	otherSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6, result7})
	require.Equal(t, SearchResults{result6}, otherSet.PopResults())
}
