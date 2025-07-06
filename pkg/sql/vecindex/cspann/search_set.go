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
	// QueryDistance is the estimated distance of the data vector from the query
	// vector. The quantizer used by the index determines the distance metric.
	QueryDistance float32
	// ErrorBound captures the uncertainty of the distance estimate, which is
	// highly likely to fall within QueryDistance ± ErrorBound.
	ErrorBound float32
	// CentroidDistance is the exact distance of the result vector from its
	// centroid, according to the distance metric in use (e.g. L2Squared or
	// Cosine).
	// NOTE: This is only returned if the SearchSet.IncludeCentroidDistances is
	// set to true.
	CentroidDistance float32
	// ParentPartitionKey is the key of the parent of the partition that contains
	// the data vector.
	ParentPartitionKey PartitionKey
	// ChildKey is the primary key of the data vector if it's in a leaf
	// partition, or the key of its partition if it's in a root/branch partition.
	ChildKey ChildKey
	// Vector is the original, full-size data vector. This is nil by default,
	// and is only set when SearchOptions.SkipRerank is false.
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
	return r.QueryDistance-r.ErrorBound <= r2.QueryDistance+r2.ErrorBound
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
	distance1 := r.QueryDistance
	distance2 := r2.QueryDistance
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
// vector. Candidates are repeatedly added via the Add methods and then the Pop
// methods returns a sorted list of the best candidates. Candidates are
// de-duplicated, such that no two results will have the same child key.
type SearchSet struct {
	// MaxResults specifies the max number of best search results to return from
	// a call to the Pop methods.
	MaxResults int

	// MaxExtraResults is the maximum number of additional candidates (beyond
	// MaxResults) that will be returned by the Pop methods. These results are
	// within the error threshold of being among the best results and are used
	// for reranking.
	MaxExtraResults int

	// MatchKey, if non-nil, filters out all search candidates that do not have a
	// matching primary key.
	MatchKey KeyBytes

	// ExcludedPartitions specifies which partitions to skip during search.
	// Vectors in any of these partitions will not be added to the set.
	ExcludedPartitions []PartitionKey

	// IncludeCentroidDistances indicates that search results need to have their
	// CentroidDistance field set. This records the vector's distance from the
	// centroid of its partition.
	IncludeCentroidDistances bool

	// Stats tracks useful information about the search, such as how many vectors
	// and partitions were scanned.
	Stats SearchStats

	// deDuper is used to de-duplicate search results by their child keys.
	deDuper ChildKeyDeDup
	// pruningThreshold records how many results can be added to the search set
	// before a pruning pass happens, where the best results are retained and
	// worse results are discarded.
	pruningThreshold int
	// candidates is the slice of search results added so far to the set, in
	// unsorted order. Periodically, these are pruned so that its length never
	// exceeds the pruning threshold.
	candidates searchResultHeap
	// heapified is true if heap.Init has been called on the candidates so that
	// the heap invariants hold.
	heapified bool
	// tempResult is used to avoid heap allocations when searching partitions.
	tempResult SearchResult
}

// Init sets up the search set for use. While it's not necessary to call this
// when the search set has been newly allocated (i.e. with zero'd memory), this
// method is useful for re-initializing it after previous usage has left it in
// an undefined state.
func (ss *SearchSet) Init() {
	ss.deDuper.Clear()
	*ss = SearchSet{
		deDuper:    ss.deDuper,
		candidates: ss.candidates[:0],
	}
}

// Count returns the number of search candidates in the set.
// NOTE: This can be greater than MaxResults + MaxExtraResults in the case where
// pruning has not yet taken place.
func (ss *SearchSet) Count() int {
	return len(ss.candidates)
}

// Clear removes all candidates from the set, but does not otherwise disturb
// other settings.
func (ss *SearchSet) Clear() {
	ss.candidates = ss.candidates[:0]
	ss.deDuper.Clear()
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
	ss.heapified = false
}

// Add includes a new candidate in the search set.
func (ss *SearchSet) Add(candidate *SearchResult) {
	if ss.MatchKey != nil && !bytes.Equal(ss.MatchKey, candidate.ChildKey.KeyBytes) {
		// Filter out candidates without a matching primary key.
		return
	}

	// Skip vectors in excluded partitions.
	if ss.ExcludedPartitions != nil {
		if slices.Contains(ss.ExcludedPartitions, candidate.ParentPartitionKey) {
			return
		}
	}

	if ss.candidates == nil {
		// Pre-allocate some capacity for candidates.
		ss.candidates = make(searchResultHeap, 0, 16)
	}
	ss.candidates = append(ss.candidates, *candidate)
	ss.heapified = false
	ss.checkPruneCandidates()
}

// AddAll includes a set of candidates in the search set.
func (ss *SearchSet) AddAll(candidates SearchResults) {
	ss.candidates = slices.Grow(ss.candidates, len(candidates))
	for i := range candidates {
		ss.Add(&candidates[i])
	}
}

// AddSet copies all candidates from the given search set to this search set.
func (ss *SearchSet) AddSet(searchSet *SearchSet) {
	if searchSet.Count() == 0 {
		return
	}
	ss.candidates = slices.Grow(ss.candidates, len(searchSet.candidates))
	if ss.MatchKey != nil || ss.ExcludedPartitions != nil {
		// Add each candidate individually in order to check the match key.
		ss.AddAll(SearchResults(searchSet.candidates))
	} else {
		// Append entire candidates slice.
		ss.candidates = append(ss.candidates, searchSet.candidates...)
		ss.heapified = false
		ss.checkPruneCandidates()
	}
}

// FindBestDistances sets the input "distances" slice to the QueryDistance
// values for the closest candidate search results. It returns the "distances"
// slice, possibly truncated if there are not enough search results to fill it.
// The returned distances are unsorted and duplicates are not filtered.
func (ss *SearchSet) FindBestDistances(distances []float64) []float64 {
	// Can't return more distances than there are candidates.
	n := len(ss.candidates)
	k := min(len(distances), n)

	// Copy all distances to the output slice. Track the max distance.
	var maxDistance float32
	maxOffset := -1
	for i := range k {
		distance := ss.candidates[i].QueryDistance
		if maxOffset == -1 || distance > maxDistance {
			maxDistance = distance
			maxOffset = i
		}
		distances[i] = float64(distance)
	}

	// For each remaining candidate, if its distance is smaller than the largest
	// in our result so far, replace that largest one.
	for i := k; i < n; i++ {
		distance := ss.candidates[i].QueryDistance
		if distance < maxDistance {
			// Find the largest distance in our current result.
			distances[maxOffset] = float64(distance)
			maxDistance64 := distances[0]
			maxOffset = 0
			for j := 1; j < k; j++ {
				if distances[j] > maxDistance64 {
					maxDistance64 = distances[j]
					maxOffset = j
				}
			}
			maxDistance = float32(maxDistance64)
		}
	}

	return distances[:k]
}

// PopResults removes the best results from the set, returning them in sorted
// order, by distance. The number of best results is always <= ss.MaxResults +
// ss.MaxExtraResults. This method may be called repeatedly to get additional
// results that may be cached in the set. As long as the Add methods are not
// called between calls to Pop methods, duplicates will never be returned.
func (ss *SearchSet) PopResults() SearchResults {
	// findBestCandidates moves the best candidates to the beginning of the
	// ss.candidates list, so return those and preserve remaining candidates for
	// any future calls to PopResults. Discard any duplicates by reslicing after
	// both best and duplicate candidates. Note that any remaining candidates are
	// still in heap order.
	count, dups := ss.findBestCandidates(ss.MaxResults, ss.MaxExtraResults)
	popped := SearchResults(ss.candidates[:count])
	ss.candidates = ss.candidates[count+dups:]
	return popped
}

// PopBestResult removes the best result from the set and returns it. This
// method may be called repeatedly to get additional results that may be cached
// in the set. As long as the Add methods are not called between calls to Pop
// methods, duplicates will never be returned.
func (ss *SearchSet) PopBestResult() *SearchResult {
	count, dups := ss.findBestCandidates(1, 0)
	if count == 0 {
		return nil
	}
	popped := &ss.candidates[0]
	ss.candidates = ss.candidates[count+dups:]
	return popped
}

// checkPruneCandidates checks if the length of the candidates slice has reached
// the pruning threshold. If so, it prunes results with the highest distances.
func (ss *SearchSet) checkPruneCandidates() {
	if ss.pruningThreshold == 0 {
		ss.pruningThreshold = max(1024, (ss.MaxResults+ss.MaxExtraResults)*2)
	}
	if len(ss.candidates) >= ss.pruningThreshold {
		ss.pruneCandidates()
	}
}

// pruneCandidates reduces the number of search candidates to ss.MaxResults +
// ss.MaxExtraResults, retaining only the best candidates by distance.
// Candidates that are farther away are discarded. Pruning happens only after a
// sizeable number of candidates have been added to the search set.
func (ss *SearchSet) pruneCandidates() {
	// The best candidates are moved to the beginning of the ss.candidates slice,
	// so this amounts to truncating the slice.
	count, _ := ss.findBestCandidates(ss.MaxResults, ss.MaxExtraResults)
	ss.candidates = ss.candidates[:count]

	// The candidates are not in heap order. Also clear the de-duper, to release
	// the memory from discarded candidates.
	ss.heapified = false
	ss.deDuper.Clear()
}

// findBestCandidates finds the best candidates by distance, up to the specified
// maximum number of results, and returns their count. The best candidates are
// moved to the front of the candidates slice in sorted order.
// findBestCandidates also returns the number of duplicate candidates it found.
// These are moved just beyond the best candidates.
//
// Finding in batches is O(N + K + K*logN), where N is the total number of
// candidates and K is the specified maximum number of results. Compare that to
// pruning as results are added to the search set, which has complexity
// O(N + N + N*logN). The savings come from being able to short-circuit results
// that are outside the best distance bounds. For example, there's no need to
// perform expensive duplicate detection if a result's distance could never be
// among the best results.
func (ss *SearchSet) findBestCandidates(maxResults, extraResults int) (count, dups int) {
	candidates := ss.candidates
	totalCount := 0
	nonDupCount := 0
	var thresholdCandidate *SearchResult

	if ss.deDuper.Count() == 0 {
		ss.deDuper.Init(maxResults + extraResults)
	}

	if !ss.heapified {
		heap.Init(&candidates)
		ss.heapified = true
	}

	for len(candidates) > 0 && nonDupCount < maxResults+extraResults {
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

			// Once we have maxResults candidates, then we need to start looking
			// for maxExtraResults candidates. A candidate is only eligible to be
			// an extra result if it could be among the best results, according to
			// its error bounds.
			if extraResults > 0 && thresholdCandidate == nil && nonDupCount >= maxResults {
				thresholdCandidate = &ss.candidates[nonDupCount-1]
			}
		}
		totalCount++
		candidates = candidates[1:]
	}

	return nonDupCount, totalCount - nonDupCount
}
