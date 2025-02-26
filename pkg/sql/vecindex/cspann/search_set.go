// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"bytes"
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
	// highly likely to fall within QuerySquaredDistance ± ErrorBound.
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
	// and is only set when SearchOptions.SkipRerank is false or
	// SearchOptions.ReturnVectors is true.
	Vector vector.T
	// ValueBytes are the opaque bytes stored alongside the quantized vector.
	// Depending on the store, this could be empty, or it could contain
	// information associated with the vector, such as STORING columns.
	ValueBytes []byte
}

// MaybeCloser returns true if this result's data vector may be closer (or the
// same distance) to the query vector than the given result's data vector.
func (r *SearchResult) MaybeCloser(r2 *SearchResult) bool {
	return r.QuerySquaredDistance-r.ErrorBound <= r2.QuerySquaredDistance+r2.ErrorBound
}

// Compare returns an integer comparing two search results. The result is zero
// if the two are equal, -1 if this result is less than the other, and +1 if
// this result is greater than the other. Results are ordered by distance. If
// distances are equal, then the highest error bound breaks ties. If error
// bounds are equal, then the child key breaks ties.
func (r *SearchResult) Compare(r2 *SearchResult) int {
	// Equivalent to the following, but with lazy comparisons:
	//
	//  return cmp.Or(
	//  	cmp.Compare(r.QuerySquaredDistance, r2.QuerySquaredDistance),
	//  	cmp.Compare(r.ErrorBound, r2.ErrorBound),
	//  	r.ChildKey.Compare(r2.ChildKey),
	//  )
	//

	// Compare distances.
	distance1 := r.QuerySquaredDistance
	distance2 := r2.QuerySquaredDistance
	if distance1 < distance2 {
		return -1
	} else if distance1 > distance2 {
		return 1
	}

	// Compare error bounds.
	errorBound1 := r.ErrorBound
	errorBound2 := r2.ErrorBound
	if errorBound1 < errorBound2 {
		return -1
	} else if errorBound1 > errorBound2 {
		return 1
	}

	// Compare child keys.
	return r.ChildKey.Compare(r2.ChildKey)
}

// searchResultHeap implements heap.Interface for a max-heap of SearchResult.
// Since the heap data structure is a min-heap, Less inverts its result to make
// it into a max-heap. This is needed so that SearchSet can discard results
// with the highest distances.
type searchResultHeap SearchResults

// Len implements heap.Interface.
func (h searchResultHeap) Len() int { return len(h) }

// Less implements heap.Interface.
func (h searchResultHeap) Less(i, j int) bool {
	// Flip the result of comparison in order to turn the min-heap into a
	// max-heap.
	return h[i].Compare(&h[j]) > 0
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
	// MaxResults specifies the max number of search results to return. Although
	// these are the best results, the search process may retrieve additional
	// candidates for reranking and statistics (up to MaxExtraResults) that are
	// within the error threshold of being among the best results.
	MaxResults int

	// MaxExtraResults is the maximum number of additional candidates (beyond
	// MaxResults) that will be returned by the search process for reranking and
	// statistics. These results are within the error threshold of being among the
	// best results.
	MaxExtraResults int

	// MatchKey, if non-nil, filters out all search candidates that do not have a
	// matching primary key.
	MatchKey KeyBytes

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
	if ss.MatchKey != nil && !bytes.Equal(ss.MatchKey, candidate.ChildKey.KeyBytes) {
		// Filter out candidates without a matching primary key.
		return
	}

	// Fast path where no pruning is necessary.
	if len(ss.results) < ss.MaxResults {
		heap.Push(&ss.results, candidate)
		return
	}

	// Candidate needs to be inserted into the results list. In this case, a
	// previous candidate may need to go in the extra results list.
	if candidate.QuerySquaredDistance <= ss.results[0].QuerySquaredDistance {
		heap.Push(&ss.results, candidate)
		candidate = heap.Pop(&ss.results)
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
			heap.Pop(&ss.extraResults)
		}
	}

	// Ensure that extra results within the error bound are still within it.
	// NB: This potentially discards results it should not, as the heaps are
	// sorted by distance rather than by distance ± error bounds. However, the
	// error bounds are not exact anyway, so the impact should be minimal.
	for len(ss.extraResults) > 0 && !ss.extraResults[0].MaybeCloser(&ss.results[0]) {
		heap.Pop(&ss.extraResults)
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
