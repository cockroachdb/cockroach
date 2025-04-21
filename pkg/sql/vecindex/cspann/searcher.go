// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"
	"math"

	"github.com/cockroachdb/errors"
)

// searcher contains the core search logic for a K-means tree. It begins at the
// root and proceeds downwards, breadth-first. At each level of the tree, it
// searches the subset of partitions that have centroids nearest to the query
// vector. Using estimated distance calculations, the search finds the nearest
// quantized data vectors within these partitions. If at an interior level,
// these data vectors are the quantized representation of centroids in the next
// level down, and the search continues there. If at the leaf level, then these
// data vectors are the quantized representation of the original vectors that
// were inserted into the tree. The original, full-size vectors can be fetched
// from the primary index and used to re-rank candidate search results.
//
// searcher offers a pull iterator model that returns results in batches sorted
// by distance, with duplicates removed. However, this is only a partial order;
// while vectors in earlier batches tend to have lower distances than those in
// later batches, the distances across batches can overlap and duplicates can
// appear across batches. Batches can be of different sizes, even empty.
//
// To use the searcher, the caller should first call Init with a target search
// set. Then, the caller repeatedly calls Next to get the next batch. The best
// results from the batch are added to the search set and can be retrieved by
// repeated calls to the search set's Pop methods. The size of each batch is a
// function of the search beam size. The search set always retains enough
// results to respect the MaxResults and MaxExtraResults settings (i.e. enough
// for at least one call to PopResults), but often is able to keep more.
type searcher struct {
	// idx points back to the vector indexing instance.
	idx *Index
	// idxCtx contains context information for this search, such as the vector to
	// search for, which K-means tree is being searched, what level of the tree
	// is being searched, and so on.
	idxCtx *Context
	// searchSet contains the results produced by the search. All results from
	// each batch are copied into the set, subject to its retention policy.
	searchSet *SearchSet
	// levels contains one level searcher for each level in the tree search.
	levels []levelSearcher
	// levelStorage statically allocates up to 5 level searchers, to avoid memory
	// allocation in common cases.
	levelStorage [5]levelSearcher
}

// Init sets up the searcher for iteration. It reuses memory where possible.
func (s *searcher) Init(idx *Index, idxCtx *Context, searchSet *SearchSet) {
	*s = searcher{
		idx:       idx,
		idxCtx:    idxCtx,
		searchSet: searchSet,
		levels:    s.levels[:0], // Preserve any existing slice memory.
	}
	if s.levels == nil {
		s.levels = s.levelStorage[:0]
	}
}

// Next gets the next batch of results and adds them to the target search set.
// It returns false if there are no more batches available. It will update
// search stats and also rerank vectors (if requested by idxCtx).
func (s *searcher) Next(ctx context.Context) (ok bool, err error) {
	if len(s.levels) == 0 {
		root := &s.levelStorage[0]
		root.Init(s.idx, s.idxCtx, nil /* parent */, &s.searchSet.Stats)

		// Return enough search results to ensure that:
		// 1. The number of results requested by the caller is respected.
		// 2. There are enough samples for calculating stats.
		// 3. There are enough results for adaptive querying to dynamically expand
		//    the beam size (up to 2x the base beam size).
		root.SearchSet().MaxResults = max(
			s.searchSet.MaxResults, s.idx.options.QualitySamples, s.idxCtx.options.BaseBeamSize*2)
		root.SearchSet().MaxExtraResults = s.searchSet.MaxExtraResults

		// Search the root patition in order to discover its level.
		err = root.SearchRoot(ctx)
		if err != nil {
			return ok, err
		}

		if s.idxCtx.level > root.Level() {
			// Next must have been called by an insert operation. This code path
			// should only be hit when the insert needs to go into the root partition.
			if root.Level() != s.idxCtx.level-1 {
				panic(errors.AssertionFailedf("caller passed invalid level %d", s.idxCtx.level))
			}
			s.searchSet.Add(&SearchResult{
				ChildKey: ChildKey{PartitionKey: RootKey},
			})
			// Ensure that if Next() is called again, it will return false.
			s.levels = ensureSliceLen(s.levels, 1)
			s.levels[0] = *root
			return root.NextBatch(ctx)
		}

		// Set up remainder of searchers now that we know the root's level.
		n := int(root.Level()-s.idxCtx.level) + 1
		s.levels = ensureSliceLen(s.levels, n)
		s.levels[0] = *root
		for i := 1; i < n; i++ {
			var maxResults, maxExtraResults int
			var matchKey KeyBytes
			if i == n-1 {
				// This is the last level to be searched, so ensure that:
				// 1. The number of results requested by the caller is respected.
				// 2. There are enough samples for re-ranking to work well, even if
				//    there are deleted vectors.
				// 3. There are enough samples for calculating stats.
				maxResults = s.searchSet.MaxResults
				maxExtraResults = s.searchSet.MaxExtraResults
				if !s.idxCtx.options.SkipRerank {
					maxResults = int(math.Ceil(float64(maxResults) * DeletedMultiplier))
					maxExtraResults = maxResults * RerankMultiplier
				}
				if s.idxCtx.level != LeafLevel && s.idxCtx.options.UpdateStats {
					maxResults = max(maxResults, s.idx.options.QualitySamples)
				}
				matchKey = s.searchSet.MatchKey
			} else {
				// This is an interior level, so ensure that:
				// 1. There are enough samples for calculating stats.
				// 2. There are enough results for adaptive querying to dynamically
				//    expand the beam size (up to 2x the base beam size).
				maxResults = max(s.idx.options.QualitySamples, s.idxCtx.options.BaseBeamSize*2)
			}

			s.levels[i].Init(s.idx, s.idxCtx, &s.levels[i-1], &s.searchSet.Stats)
			searchSet := s.levels[i].SearchSet()
			searchSet.MaxResults = maxResults
			searchSet.MaxExtraResults = maxExtraResults
			searchSet.MatchKey = matchKey
		}
	}

	// Get next batch of results from the searcher at the lowest level.
	lastSearcher := &s.levels[len(s.levels)-1]
	ok, err = lastSearcher.NextBatch(ctx)
	if err != nil || !ok {
		// Error or no more batches to get.
		return ok, err
	}

	// If stats need to be updated for a non-leaf level, then do so now. This is
	// typically called in the insert case, where the last search level is
	// the second level rather than at the leaf level. It's important to compute
	// stats at the second level so that stats will be available for later
	// leaf-level searches.
	// TODO(andyk): If we decide to turn on-reranking for inserts, we should
	// consider performing stats computation with popped results rather than
	// doing a separate scan over all results via FindBestDistances.
	if s.idxCtx.options.UpdateStats && lastSearcher.Level() != LeafLevel {
		if lastSearcher.SearchSet().Count() >= s.idx.options.QualitySamples {
			samples := s.idxCtx.tempQualitySamples[:s.idx.options.QualitySamples]
			lastSearcher.SearchSet().FindBestDistances(samples)
			s.idx.stats.ReportSearch(lastSearcher.Level(), samples, true /* updateStats */)
		}
	}

	if !s.idxCtx.options.SkipRerank {
		// Re-rank search results with full vectors.
		results := lastSearcher.SearchSet().PopResults()
		s.searchSet.Stats.FullVectorCount += len(results)
		results, err = s.idx.rerankSearchResults(ctx, s.idxCtx, results)
		if err != nil {
			return false, err
		}

		// Add the re-ranked results into the result search set.
		s.searchSet.AddAll(results)
	} else {
		// No-ranking needed, so copy all candidates from the batch into the result
		// search set, with no pruning.
		s.searchSet.AddSet(lastSearcher.SearchSet())
	}
	lastSearcher.SearchSet().Clear()

	return true, nil
}

// levelSearcher searches a single level of the K-means tree. If it's the root
// level, then it directly searches the root partition. Otherwise, it pulls
// result partitions from the parent level and searches them (in parallel). The
// breadth of the search is based on the configured BaseBeamSize, modified
// according to the adaptive seach algorithm.
//
// To use, call the Init method to configure the searcher. For the root-level
// searcher, the SearchRoot must first be called to bootstrap search. Then,
// repeatedly call the NextBatch method to fetch batches. Call Pop methods on
// the SearchSet for this level to get search results.
type levelSearcher struct {
	// idx points back to the vector indexing instance.
	idx *Index
	// idxCtx contains context information for this search, such as the vector to
	// search for, which K-means tree is being searched, what level of the tree
	// is being searched, and so on.
	idxCtx *Context
	// parent points to the searcher for the parent level of the K-means tree.
	// It is nil if this is the root-level searcher.
	parent *levelSearcher
	// stats points to search stats that should be updated as the search runs.
	stats *SearchStats
	// level is the K-means tree level being searched. This is undefined for the
	// root level until SearchRoot is called.
	level Level
	// searchSet contains the results produced by the search. All results from
	// each batch are copied into the set, subject to the retention policy that
	// was configured by Init.
	searchSet SearchSet
	// parentResults caches search results returned by the searcher for the parent
	// level (only non-nil if parent != nil).
	parentResults SearchResults
	// beamSize defines the breadth of the search - how many partitions are
	// searched in parallel at a given level of the tree. This is calculated
	// during the first batch, based on the adaptive search algorithm.
	beamSize int

	tempResults [2]SearchResult
}

// Init sets up the level searcher for iteration. It reuses memory where
// possible.
func (s *levelSearcher) Init(
	idx *Index, idxCtx *Context, parent *levelSearcher, stats *SearchStats,
) {
	*s = levelSearcher{
		idx:       idx,
		idxCtx:    idxCtx,
		parent:    parent,
		stats:     stats,
		searchSet: s.searchSet, // Preserve existing searchSet memory.
	}
	if parent != nil {
		if parent.Level() == InvalidLevel {
			panic(errors.AssertionFailedf("parent level cannot be InvalidLevel"))
		}
		s.level = parent.Level() - 1
	}

	s.searchSet.Clear()
}

// SearchSet returns the target search set to which results are copied for each
// batch. This is cleared each time NextBatch is called.
func (s *levelSearcher) SearchSet() *SearchSet {
	return &s.searchSet
}

// Level is the K-means tree level being searched. This is undefined for the
// root level until SearchRoot is called.
func (s *levelSearcher) Level() Level {
	return s.level
}

// SearchRoot searches the root partition for results and determines its level.
// This must be called before calling NextBatch for the root level searcher.
func (s *levelSearcher) SearchRoot(ctx context.Context) error {
	var err error
	s.tempResults[0] = SearchResult{ChildKey: ChildKey{PartitionKey: RootKey}}
	s.level, err = s.searchChildPartitions(ctx, s.tempResults[:1])
	return err
}

// NextBatch searches the next batch of partitions for this level of the K-means
// tree, adding all results to the target search set. It returns false if there
// are no more batches available.
func (s *levelSearcher) NextBatch(ctx context.Context) (ok bool, err error) {
	firstBatch := s.beamSize == 0

	// Get the next batch of partitions from the parent.
	if s.parent == nil {
		// No parent, so we must be searching the root level.
		if !firstBatch {
			// Only one batch at the root, so return false.
			return false, nil
		}

		// SearchRoot should have already been called.
		if s.level == InvalidLevel {
			return false, errors.AssertionFailedf(
				"expected SearchRoot to be called for the root level before NextBatch")
		}
		s.beamSize = 1
		return true, nil
	}

	// Reset the search set for each batch. While results in the new batch will
	// not contain duplicates and will be returned in sorted order, they may
	// overlap with the previous batch, in terms of ordering and duplicates.
	s.searchSet.Clear()

	if firstBatch {
		ok, err := s.parent.NextBatch(ctx)
		if err != nil || !ok {
			return ok, err
		}
		s.parentResults = s.parent.SearchSet().PopResults()
	} else if len(s.parentResults) < s.beamSize {
		// Get more results from parent to try and fill the beam size.
		parentResults := s.parent.SearchSet().PopResults()
		if len(parentResults) == 0 {
			// Get next batch of results from parent.
			ok, err := s.parent.NextBatch(ctx)
			if err != nil {
				return false, err
			}
			if !ok && len(s.parentResults) == 0 {
				// Only exit if there are no more results to process.
				return false, nil
			}
			parentResults = s.parent.SearchSet().PopResults()
		}
		if len(s.parentResults) == 0 {
			s.parentResults = parentResults
		} else {
			s.parentResults = append(s.parentResults, parentResults...)
		}
	}

	if firstBatch {
		// Compute the Z-score of the parent results if there are enough samples.
		// Otherwise, use the default Z-score of 0.
		var zscore float64
		if len(s.parentResults) >= s.idx.options.QualitySamples {
			for i := range s.idx.options.QualitySamples {
				s.idxCtx.tempQualitySamples[i] = float64(s.parentResults[i].QuerySquaredDistance)
			}
			samples := s.idxCtx.tempQualitySamples[:s.idx.options.QualitySamples]
			zscore = s.idx.stats.ReportSearch(s.parent.Level(), samples, s.idxCtx.options.UpdateStats)
		}

		// Calculate beam size for this level.
		s.beamSize = s.idxCtx.options.BaseBeamSize
		if !s.idx.options.DisableAdaptiveSearch {
			// Look at variance in result distances to calculate the beam size for
			// the next level. The less variance there is, the larger the beam size.
			// The intuition is that the closer the distances are to one another, the
			// more densely packed are the vectors, and the more partitions they're
			// likely to be spread across.
			tempBeamSize := float64(s.beamSize) * math.Pow(2, -zscore)
			tempBeamSize = max(min(tempBeamSize, float64(s.beamSize)*2), float64(s.beamSize)/2)

			if s.level > LeafLevel {
				// Use progressively smaller beam size for higher levels, since
				// each contains exponentially fewer partitions.
				tempBeamSize /= math.Pow(2, float64(s.level-LeafLevel))
			}

			s.beamSize = int(math.Ceil(tempBeamSize))
		}
		s.beamSize = max(s.beamSize, 1)
	}

	// Search up to s.beamSize child partitions.
	parentResults := s.parentResults[:min(s.beamSize, len(s.parentResults))]
	s.parentResults = s.parentResults[len(parentResults):]

	_, err = s.searchChildPartitions(ctx, parentResults)
	return true, err
}

// searchChildPartitions searches for nearest neighbors to the query vector in
// the set of partitions referenced by the given parent search results. It adds
// the closest matches to the target search set.
func (s *levelSearcher) searchChildPartitions(
	ctx context.Context, parentResults SearchResults,
) (Level, error) {
	if len(parentResults) == 0 {
		// No partitions to search, short-circuit.
		return InvalidLevel, nil
	}

	s.idxCtx.tempToSearch = ensureSliceLen(s.idxCtx.tempToSearch, len(parentResults))
	for i := range parentResults {
		// If this is an Insert or SearchForInsert operation, then do not scan
		// leaf vectors. Insert operations never need leaf vectors and scanning
		// them only increases the contention footprint. This is only needed when
		// we're searching the root partition, as we do not know its level until
		// we read its metadata record. By contrast, we know whether a non-root
		// partition is a leaf partition because we know the level of its parent.
		partitionKey := parentResults[i].ChildKey.PartitionKey
		s.idxCtx.tempToSearch[i] = PartitionToSearch{
			Key:                partitionKey,
			ExcludeLeafVectors: s.idxCtx.forInsert && partitionKey == RootKey,
		}
	}

	// Search all partitions in parallel.
	err := s.idxCtx.txn.SearchPartitions(
		ctx, s.idxCtx.treeKey, s.idxCtx.tempToSearch, s.idxCtx.randomized, &s.searchSet)
	if err != nil {
		return InvalidLevel, errors.Wrapf(err, "searching level %d", s.level)
	}

	// Process each searched partition.
	level := s.level
	for i := range s.idxCtx.tempToSearch {
		toSearch := s.idxCtx.tempToSearch[i]
		count := toSearch.Count
		partitionKey := toSearch.Key

		// Determine the search level. Level is InvalidLevel if the searched
		// partition is missing. That can happen when the partition has been
		// deleted from the tree by split or merge operations and there still
		// exist dangling references to it.
		if level == InvalidLevel {
			level = toSearch.Level
		} else if toSearch.Level != InvalidLevel && toSearch.Level != level {
			return InvalidLevel, errors.AssertionFailedf("level is %d, but was %d for a previous search",
				toSearch.Level, level)
		}
		s.stats.SearchedPartition(level, count)

		// If searching for vector to delete, skip partitions that are in a state
		// that does not allow add and remove operations. This is not possible to
		// do here for the insert case, because we do not actually search the
		// partition in which to insert; we only search its parent and never get
		// the metadata for the insert partition itself.
		// TODO(andyk): This should probably be checked in the Store, perhaps by
		// passing a "forUpdate" parameter to SearchPartitions, so that the Store
		// doesn't even add vectors from partitions that do not allow updates.
		if s.idxCtx.forDelete && s.idxCtx.level == level {
			if !s.idxCtx.tempToSearch[i].StateDetails.State.AllowAddOrRemove() {
				s.searchSet.RemoveByParent(partitionKey)
			}
		}

		// Enqueue background fixup if a split or merge operation needs to be
		// started or continued after stalling.
		state := s.idxCtx.tempToSearch[i].StateDetails
		needSplit := count > s.idx.options.MaxPartitionSize
		needSplit = needSplit || state.MaybeSplitStalled(s.idx.options.StalledOpTimeout())
		needMerge := count < s.idx.options.MinPartitionSize && partitionKey != RootKey
		needMerge = needMerge || state.MaybeMergeStalled(s.idx.options.StalledOpTimeout())
		if needSplit {
			s.idx.fixups.AddSplit(ctx, s.idxCtx.treeKey,
				parentResults[i].ParentPartitionKey, partitionKey, false /* singleStep */)
		} else if needMerge {
			s.idx.fixups.AddMerge(ctx, s.idxCtx.treeKey,
				parentResults[i].ParentPartitionKey, partitionKey, false /* singleStep */)
		}
	}

	// If the root partition is in a non-ready state, then we need to check
	// whether its target partitions also need to be searched. Newly inserted
	// vectors can get forwarded to target partitions during a split, and we
	// need to make sure we can find them.
	if len(parentResults) != 1 || parentResults[0].ParentPartitionKey != InvalidKey {
		return level, nil
	}
	rootState := s.idxCtx.tempToSearch[0].StateDetails
	if rootState.State == ReadyState {
		return level, nil
	}

	// This is the root partition in a non-ready state. Check whether target
	// positions also need to be searched. This can happen when the root
	// partition is in the middle of a split.
	s.tempResults[0] = SearchResult{
		ParentPartitionKey: RootKey,
		ChildKey:           ChildKey{PartitionKey: rootState.Target1},
	}
	s.tempResults[1] = SearchResult{
		ParentPartitionKey: RootKey,
		ChildKey:           ChildKey{PartitionKey: rootState.Target2},
	}

	if rootState.State == DrainingForSplitState {
		// In the DrainingForSplit state, the target partitions are still at
		// the same level as the root partition, so merge their contents into
		// the search set (which will remove any duplicates).
		s.idxCtx.tempToSearch = ensureSliceCap(s.idxCtx.tempToSearch, 2)
		level, err = s.searchChildPartitions(ctx, s.tempResults[:2])
		if err != nil {
			return InvalidLevel, err
		}
	} else if rootState.State == AddingLevelState {
		// In the AddingLevel state, the target partitions should be treated as
		// children of the root partition. Add their partition keys to the search
		// set.
		// NOTE: QuerySquaredDistance is zero for both partitions. This should have
		// almost no impact on accuracy. If we decide to handle this edge case, we
		// can use GetFullVectors to fetch the sub-partition centroids in order to
		// compute distances.
		s.searchSet.Add(&s.tempResults[0])
		s.searchSet.Add(&s.tempResults[1])
	}

	return level, nil
}
