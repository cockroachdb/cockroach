// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// SearchResults is a list of search results from the search set.
type SearchResults []SearchResult

// Sort re-orders the results in-place, by their distance.
func (s *SearchResults) Sort() {
	results := searchResultHeap(*s)
	if len(results) > 1 {
		// Sort the results in-place.
		sort.Slice(results, func(i int, j int) bool {
			return !results.Less(i, j)
		})
	}
}

// SearchResult contains a set of results from searching partitions for data
// vectors that are nearest to a query vector.
type SearchResult struct {
	// QuerySquaredDistance is the estimated squared distance of the data vector
	// from the query vector.
	QuerySquaredDistance float32
	// ErrorBound captures the uncertainty of the distance estimate, which is
	// highly likely to fall within QuerySquaredDistance +- ErrorBound.
	ErrorBound float32
	// CentroidDistance is the (non-squared) exact distance of the data vector
	// from its partition's centroid.
	CentroidDistance float32
	// ParentPartitionKey is the key of the parent of the partition that contains
	// the data vector.
	ParentPartitionKey PartitionKey
	// ChildKey is the primary key of the data vector if it's in a leaf
	// partition, or the key of its partition if it's in a root/branch partition.
	ChildKey ChildKey
	// Vector is the original, full-size data vector. This is nil by default,
	// and is only set after an optional reranking step.
	Vector vector.T
}

// MaybeCloser returns true if this result's data vector may be closer (or the
// same distance) to the query vector than the given result's data vector.
func (r *SearchResult) MaybeCloser(r2 *SearchResult) bool {
	return r.QuerySquaredDistance-r.ErrorBound <= r2.QuerySquaredDistance+r2.ErrorBound
}

// searchResultHeap implements heap.Interface for a min-heap of SearchResult
// It compares using negative distances, since SearchSet needs to discard
// results with the highest distances.
type searchResultHeap []SearchResult

// Len implements heap.Interface.
func (h searchResultHeap) Len() int { return len(h) }

// Less implements heap.Interface.
func (h searchResultHeap) Less(i, j int) bool {
	distance1 := h[i].QuerySquaredDistance
	distance2 := h[j].QuerySquaredDistance
	if -distance1 < -distance2 {
		return true
	}
	if distance1 == distance2 && h[i].ErrorBound < h[j].ErrorBound {
		// If distance is equal, lower error bound sorts first.
		return true
	}
	return false
}

// Swap implements heap.Interface.
func (h searchResultHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push implements heap.Interface.
func (h *searchResultHeap) Push(x *SearchResult) {
	*h = append(*h, *x)
}

// Pop implements heap.Interface.
func (h *searchResultHeap) Pop() *SearchResult {
	old := *h
	n := len(old)
	item := &old[n-1]
	*h = old[0 : n-1]
	return item
}

// SearchStats tracks useful information about searches, such as how many
// vectors and partitions were scanned.
type SearchStats struct {
	// PartitionCount is the number of partitions that were scanned for nearby
	// vectors.
	PartitionCount int
	// QuantizedVectorCount is the total number of quantized vectors that were
	// scanned in the search for the closest vectors.
	QuantizedVectorCount int
	// QuantizedLeafVectorCount is the total number of quantized leaf vectors
	// (i.e. those in leaf partitions) that were scanned in the search for the
	// closest vectors.
	QuantizedLeafVectorCount int
	// FullVectorCount is the total number of original, full-size vectors that
	// were evaluated during the reranking phase.
	FullVectorCount int
}

// SearchedPartition records statistics resulting from the scan of a partition
// at the given level.
func (ss *SearchStats) SearchedPartition(level Level, quantizedVectorCount int) {
	ss.PartitionCount++
	ss.QuantizedVectorCount += quantizedVectorCount
	if level == LeafLevel {
		ss.QuantizedLeafVectorCount += quantizedVectorCount
	}
}

// Add updates these statistics by adding the given statistics.
func (ss *SearchStats) Add(other *SearchStats) {
	ss.PartitionCount += other.PartitionCount
	ss.QuantizedVectorCount += other.QuantizedVectorCount
	ss.QuantizedLeafVectorCount += other.QuantizedLeafVectorCount
	ss.FullVectorCount += other.FullVectorCount
}

// SearchSet incrementally maintains the nearest candidates that result from
// searching a partition for the data vectors that are nearest to a query
// vector. Candidates are repeatedly added via the Add methods and the Pop
// methods return a sorted list of the best candidates.
type SearchSet struct {
	// MaxResults is the maximum number of candidates that will be returned by
	// PopResults.
	MaxResults int

	// MaxExtraResults is the maximum number of candidates that will be returned
	// by PopExtraResults. These results are within the error threshold of being
	// among the best results.
	MaxExtraResults int

	// Stats tracks useful information about the search, such as how many vectors
	// and partitions were scanned.
	Stats SearchStats

	result       SearchResult
	results      searchResultHeap
	extraResults searchResultHeap
}

// Add includes a new candidate in the search set. If set limits have been
// reached, then the candidate with the farthest distance will be discarded.
func (ss *SearchSet) Add(candidate *SearchResult) {
	// Fast path where no pruning is necessary.
	if len(ss.results) < ss.MaxResults {
		heap.Push[*SearchResult](&ss.results, candidate)
		return
	}

	// Candidate needs to be inserted into the results list. In this case, a
	// previous candidate may need to go the extra results list.
	if candidate.QuerySquaredDistance <= ss.results[0].QuerySquaredDistance {
		heap.Push(&ss.results, candidate)
		candidate = heap.Pop[*SearchResult](&ss.results)
	}

	if ss.MaxExtraResults == 0 {
		return
	}

	if candidate.MaybeCloser(&ss.results[0]) {
		// The result could still be a match, since it's within the error bound
		// of the farthest remaining distance. Add it to the extra results heap.
		heap.Push(&ss.extraResults, candidate)
		if len(ss.extraResults) > ss.MaxExtraResults {
			// Respect max extra results.
			heap.Pop[*SearchResult](&ss.extraResults)
		}
	}

	// Ensure that extra results within the error bound are still within it.
	// NB: This potentially discards results it should not, as the heaps are
	// sorted by distance rather than by distance +- error bounds. However,
	// the error bounds are not exact anyway, so the impact should be minimal.
	for len(ss.extraResults) > 0 && !ss.extraResults[0].MaybeCloser(&ss.results[0]) {
		heap.Pop[*SearchResult](&ss.extraResults)
	}
}

// AddAll includes a set of candidates in the search set.
func (ss *SearchSet) AddAll(candidates SearchResults) {
	for i := range candidates {
		ss.Add(&candidates[i])
	}
}

// PopResults removes all results from the set and returns them in order of
// their distance.
func (ss *SearchSet) PopResults() SearchResults {
	results := ss.PopUnsortedResults()
	results.Sort()
	return results
}

// PopUnsortedResults removes the nearest candidates by distance from the set
// and returns them in unsorted order.
func (ss *SearchSet) PopUnsortedResults() SearchResults {
	popped := append(ss.results, ss.extraResults...)
	ss.results = nil
	ss.extraResults = nil
	return SearchResults(popped)
}
