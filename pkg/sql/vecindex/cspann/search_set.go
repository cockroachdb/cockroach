// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"bytes"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// SearchResults is a list of search results from the search set.
type SearchResults []SearchResult

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
	// NOTE: If this is an interior centroid vector, then it is randomized.
	// Otherwise, it's an original, un-randomized leaf vector.
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
// distances are equal, then the lowest error bound breaks ties. If error
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

// searchResultHeap implements heap.Interface for a min-heap of SearchResult.
// However, its methods inverts indexes such that the minimum element is the
// last element of the heap rather than the first. This is convenient, since it
// means that as we pop results from the heap, they're naturally placed at the
// beginning of the search results slice, sorted by ascending distance.
type searchResultHeap SearchResults

// Len implements heap.Interface.
func (h searchResultHeap) Len() int { return len(h) }

// Less implements heap.Interface. It inverts the "i" and "j" indexes so that
// the heap will place the min element at the end of the slice rather than the
// beginning.
func (h searchResultHeap) Less(i, j int) bool {
	n := len(h) - 1
	return h[n-i].Compare(&h[n-j]) < 0
}

// Swap implements heap.Interface. It inverts the "i" and "j" indexes so that
// the heap will place the min element at the end of the slice rather than the
// beginning.
func (h searchResultHeap) Swap(i, j int) {
	n := len(h) - 1
	i = n - i
	j = n - j
	h[i], h[j] = h[j], h[i]
}

// Push implements heap.Interface. However, it should never be called.
func (h *searchResultHeap) Push(x *SearchResult) {
	panic(errors.AssertionFailedf("Push should never be called"))
}

// Pop implements heap.Interface. However, it simply returns nil, since we never
// need the popped result. It also does not reduce the size of the slice, since
// that's done in SearchSet.pruneCandidates.
func (h *searchResultHeap) Pop() *SearchResult {
	return nil
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
// methods return a sorted list of the best candidates. Candidates are
// de-duplicated, such that no two results will have the same child key.
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

	// deDuper is used to de-duplicate search results by their child keys.
	deDuper childKeyDeDup
	// pruningThreshold records how many results can be added to the search set
	// before a pruning pass happens, where the best results are retained and
	// worse results are discarded.
	pruningThreshold int
	// candidates is the slice of search results added so far to the set, in
	// unsorted order. Periodically, these are pruned so that its length never
	// exceeds the pruning threshold.
	candidates searchResultHeap
	// tempResult is used to avoid heap allocations when searching partitions.
	tempResult SearchResult
}

// RemoveByParent removes all results that are in the given parent partition.
func (ss *SearchSet) RemoveByParent(parentPartitionKey PartitionKey) {
	// Walk over candidates and remove any that have the specified parent
	// partition key.
	i := 0
	for i < len(ss.candidates) {
		if ss.candidates[i].ParentPartitionKey == parentPartitionKey {
			// Remove the result by replacing it with the last candidate and
			// reducing the length of the candidates slice by one.
			n := len(ss.candidates) - 1
			ss.candidates[i] = ss.candidates[n]
			ss.candidates = ss.candidates[:n]
			continue
		}
		i++
	}
}

// Add includes a new candidate in the search set.
func (ss *SearchSet) Add(candidate *SearchResult) {
	if ss.MatchKey != nil && !bytes.Equal(ss.MatchKey, candidate.ChildKey.KeyBytes) {
		// Filter out candidates without a matching primary key.
		return
	}

	if ss.candidates == nil {
		ss.candidates = make(searchResultHeap, 0, ss.MaxResults)
	}
	ss.candidates = append(ss.candidates, *candidate)

	// If the length of the candidates slice has reached the pruning threshold,
	// then prune results with the highest distances.
	if ss.pruningThreshold == 0 {
		ss.pruningThreshold = max(20, (ss.MaxResults+ss.MaxExtraResults)*2)
	}
	if len(ss.candidates) >= ss.pruningThreshold {
		ss.pruneCandidates()
	}
}

// AddAll includes a set of candidates in the search set.
func (ss *SearchSet) AddAll(candidates SearchResults) {
	ss.candidates = slices.Grow(ss.candidates, len(candidates))
	for i := range candidates {
		ss.Add(&candidates[i])
	}
}

// PopResults removes all results from the set and returns them in sorted order,
// by distance.
func (ss *SearchSet) PopResults() SearchResults {
	ss.pruneCandidates()
	popped := ss.candidates
	ss.candidates = nil
	return SearchResults(popped)
}

// pruneCandidates keeps the best candidates, up to the configured maximum
// number of results, and arranges them in sorted order in the candidates slice.
// Candidates that are farther away (within error bounds) are discarded. Pruning
// happens only after a sizeable number of candidates have been added to the
// search set.
//
// Pruning in batches is O(N + K + K*logN), where N is the number of candidates
// and K is the number of best results (always fewer than MaxResults +
// MaxExtraResults). Compare that to pruning as results are added to the search
// set, which has complexity O(N + N + N*logN). The savings come from being able
// to short-circuit results that are outside the best distance bounds. For
// example, there's no need to perform expensive duplicate detection if a
// result's distance could never be among the best results.
//
// NOTE: After calling this, the length of the candidates slice will always be
// <= MaxResults + MaxExtraResults. Results will be ordered by their
// QuerySquaredDistance field in that slice.
func (ss *SearchSet) pruneCandidates() {
	candidates := ss.candidates
	totalCount := 0
	nonDupCount := 0
	var thresholdCandidate *SearchResult

	ss.deDuper.Init(ss.MaxResults * 2)
	heap.Init(&candidates)
	for len(candidates) > 0 && nonDupCount < ss.MaxResults+ss.MaxExtraResults {
		// Pop will copy the candidate with the smallest distance to candidates[0].
		heap.Pop(&candidates)

		if thresholdCandidate != nil && !candidates[0].MaybeCloser(thresholdCandidate) {
			// Remaining candidates are not likely to have distance <= the threshold
			// candidate, so prune all remaining candidates.
			break
		}

		// Check for duplicate child keys.
		if ss.deDuper.TryAdd(candidates[0].ChildKey) {
			// Not a duplicate. If we found previous duplicates, then need to
			// "close the gap" in the slice by copying the non-duplicate candidate
			// to its correct position.
			if nonDupCount != totalCount {
				ss.candidates[nonDupCount] = candidates[0]
			}
			nonDupCount++

			// Once we have MaxResults candidates, then we need to start looking
			// for MaxExtraResults candidates. A candidate is only eligible to be
			// an extra result if it could be among the best results, according to
			// its error bounds.
			if thresholdCandidate == nil && nonDupCount >= ss.MaxResults {
				thresholdCandidate = &ss.candidates[nonDupCount-1]
			}
		}
		totalCount++
		candidates = candidates[1:]
	}

	ss.candidates = ss.candidates[:nonDupCount]
}
