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

	result1 := SearchResult{
		QuerySquaredDistance: 3, ErrorBound: 0.5, CentroidDistance: 10, ParentPartitionKey: 100, ChildKey: ChildKey{KeyBytes: []byte{10}}}
	result2 := SearchResult{
		QuerySquaredDistance: 6, ErrorBound: 1, CentroidDistance: 20, ParentPartitionKey: 200, ChildKey: ChildKey{KeyBytes: []byte{20}}}
	result3 := SearchResult{
		QuerySquaredDistance: 1, ErrorBound: 0.5, CentroidDistance: 30, ParentPartitionKey: 300, ChildKey: ChildKey{KeyBytes: []byte{30}}}
	result4 := SearchResult{
		QuerySquaredDistance: 4, ErrorBound: 0.5, CentroidDistance: 40, ParentPartitionKey: 400, ChildKey: ChildKey{KeyBytes: []byte{40}}}
	result5 := SearchResult{
		QuerySquaredDistance: 6, ErrorBound: 1.5, CentroidDistance: 50, ParentPartitionKey: 500, ChildKey: ChildKey{KeyBytes: []byte{50}}}
	result6 := SearchResult{
		QuerySquaredDistance: 5, ErrorBound: 1, CentroidDistance: 60, ParentPartitionKey: 600, ChildKey: ChildKey{KeyBytes: []byte{60}}}
	result7 := SearchResult{
		QuerySquaredDistance: 4, ErrorBound: 1.5, CentroidDistance: 70, ParentPartitionKey: 700, ChildKey: ChildKey{KeyBytes: []byte{70}}}
	result8 := SearchResult{
		QuerySquaredDistance: 0.5, ErrorBound: 0.5, CentroidDistance: 80, ParentPartitionKey: 800, ChildKey: ChildKey{KeyBytes: []byte{80}}}

	t.Run("empty set", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 3, MaxExtraResults: 7}
		require.Equal(t, 0, searchSet.Count())
		require.Nil(t, searchSet.PopResults())
	})

	t.Run("exceed max results, outside of error bounds", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 3, MaxExtraResults: 7}
		searchSet.Add(&result1)
		searchSet.Add(&result2)
		searchSet.Add(&result3)
		searchSet.Add(&result4)
		require.Equal(t, 4, searchSet.Count())
		require.Equal(t, SearchResults{result3, result1, result4}, searchSet.PopResults())
	})

	t.Run("exceed max results, but within error bounds", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 3, MaxExtraResults: 7}
		searchSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6})
		require.Equal(t, SearchResults{result3, result1, result4, result6}, searchSet.PopResults())
	})

	t.Run("don't allow extra results", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 3}
		searchSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6})
		require.Equal(t, SearchResults{result3, result1, result4}, searchSet.PopResults())
	})

	t.Run("add better results that invalidate farther candidates", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 3, MaxExtraResults: 7}
		searchSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6, result7})
		require.Equal(t, SearchResults{result3, result1, result4, result7, result6}, searchSet.PopResults())
		searchSet.Clear()
		searchSet.AddAll(SearchResults{result1, result2, result3, result4})
		searchSet.AddAll(SearchResults{result5, result6, result7, result8})
		require.Equal(t, SearchResults{result8, result3, result1, result4, result7}, searchSet.PopResults())
	})

	t.Run("allow one extra result", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 3, MaxExtraResults: 1}
		searchSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6, result7})
		require.Equal(t, SearchResults{result3, result1, result4, result7}, searchSet.PopResults())
	})

	t.Run("ignore duplicate results", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 3, MaxExtraResults: 7}
		searchSet.AddAll(SearchResults{result1, result2, result1, result3, result4, result1, result5, result6, result7, result1})
		require.Equal(t, SearchResults{result3, result1, result4, result7, result6}, searchSet.PopResults())
	})

	t.Run("ignore results without a matching primary key", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 2, MatchKey: []byte{60}}
		searchSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6, result7})
		require.Equal(t, SearchResults{result6}, searchSet.PopResults())
	})

	t.Run("test RemoveByParent", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 4, MaxExtraResults: 2}
		searchSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6})
		searchSet.RemoveByParent(100)
		require.Equal(t, SearchResults{result3, result4, result6, result2, result5}, searchSet.PopResults())
		searchSet.Clear()
		searchSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6})
		searchSet.RemoveByParent(200)
		require.Equal(t, SearchResults{result3, result1, result4, result6, result5}, searchSet.PopResults())
	})

	t.Run("trigger pruning", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 2, MaxExtraResults: 1}
		searchSet.pruningThreshold = 5
		searchSet.AddAll(SearchResults{result1, result2, result3, result4, result5, result6})
		require.Equal(t, 4, searchSet.Count())
		require.Equal(t, SearchResults{result3, result1, result4}, searchSet.PopResults())
	})

	t.Run("call PopResults repeatedly, with duplicates present", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 2, MaxExtraResults: 1}
		searchSet.AddAll(SearchResults{result1, result2, result1, result3, result4, result1, result5, result6, result7, result1, result8})
		require.Equal(t, SearchResults{result8, result3}, searchSet.PopResults())
		require.Equal(t, SearchResults{result1, result4, result7}, searchSet.PopResults())
		require.Equal(t, SearchResults{result6, result2, result5}, searchSet.PopResults())

		// Duplicates are ignored until Clear is called.
		searchSet.AddAll(SearchResults{result1, result1})
		require.Equal(t, SearchResults{}, searchSet.PopResults())
		searchSet.Clear()
		searchSet.AddAll(SearchResults{result1, result1})
		require.Equal(t, SearchResults{result1}, searchSet.PopResults())
	})

	t.Run("call PopBestResult repeatedly, with duplicates present", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 3, MaxExtraResults: 7}
		searchSet.AddAll(SearchResults{result1, result2, result1, result3, result4, result1, result5, result6, result7, result1, result8})
		require.Equal(t, &result8, searchSet.PopBestResult())
		require.Equal(t, &result3, searchSet.PopBestResult())
		require.Equal(t, &result1, searchSet.PopBestResult())
		require.Equal(t, &result4, searchSet.PopBestResult())
		require.Equal(t, &result7, searchSet.PopBestResult())
		require.Equal(t, &result6, searchSet.PopBestResult())
		require.Equal(t, &result2, searchSet.PopBestResult())
		require.Equal(t, &result5, searchSet.PopBestResult())

		// Duplicates are ignored until Clear is called.
		searchSet.AddAll(SearchResults{result1, result5, result3, result8})
		require.Equal(t, SearchResults{}, searchSet.PopResults())
		searchSet.Clear()
		searchSet.AddAll(SearchResults{result1, result5, result3, result8})
		require.Equal(t, SearchResults{result8, result3, result1}, searchSet.PopResults())
	})

	t.Run("test AddSet with no MatchKey", func(t *testing.T) {
		// set1 has results 1 to 4.
		set1 := SearchSet{MaxResults: 3, MaxExtraResults: 1}
		set1.AddAll(SearchResults{result1, result2, result1, result3, result4})

		// set2 has results 5 to 8, with duplicate results 1 and 2.
		set2 := SearchSet{MaxResults: 2, MaxExtraResults: 1}
		set2.AddAll(SearchResults{result1, result5, result6, result2, result7, result1, result8})

		// Add set1 to set2 and check that both sets have expected candidates.
		set2.AddSet(&set1)
		require.Equal(t, 5, set1.Count())
		require.Equal(t, 12, set2.Count())
		require.Equal(t, SearchResults{result3, result1, result4}, set1.PopResults())
		require.Equal(t, SearchResults{result2}, set1.PopResults())
		require.Equal(t, SearchResults{result8, result3}, set2.PopResults())
		require.Equal(t, SearchResults{result1, result4, result7}, set2.PopResults())
		require.Equal(t, SearchResults{result6, result2, result5}, set2.PopResults())

		// Add empty set.
		set1 = SearchSet{MaxResults: 3, MaxExtraResults: 1}
		set1.AddAll(SearchResults{result1, result2, result1, result3, result4})
		set2 = SearchSet{MaxResults: 2, MaxExtraResults: 1}
		set1.AddSet(&set2)
		require.Equal(t, 5, set1.Count())
	})

	t.Run("test AddSet with MatchKey", func(t *testing.T) {
		set1 := SearchSet{MaxResults: 1, MatchKey: []byte{60}}
		set1.AddAll(SearchResults{result6})
		set2 := SearchSet{MaxResults: 2, MaxExtraResults: 1}
		set2.AddAll(SearchResults{result6, result2, result6, result5})
		set1.AddSet(&set2)
		require.Equal(t, 3, set1.Count())
		require.Equal(t, 4, set2.Count())
	})

	t.Run("test FindBestDistances", func(t *testing.T) {
		searchSet := SearchSet{MaxResults: 2, MaxExtraResults: 1}
		searchSet.AddAll(SearchResults{
			result1, result2, result3, result4, result1, result5, result6, result7, result1, result8})

		var distances [20]float64
		require.Equal(t, []float64{}, searchSet.FindBestDistances(distances[:0]))
		require.Equal(t, []float64{0.5}, searchSet.FindBestDistances(distances[:1]))
		require.Equal(t, []float64{0.5, 3, 1, 3}, searchSet.FindBestDistances(distances[:4]))
		require.Equal(t, []float64{3, 3, 1, 4, 3, 0.5, 5, 4}, searchSet.FindBestDistances(distances[:8]))
		require.Equal(t, []float64{3, 6, 1, 4, 3, 6, 5, 4, 3, 0.5}, searchSet.FindBestDistances(distances[:12]))
	})
}
